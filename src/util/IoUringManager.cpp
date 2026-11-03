// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/IoUringManager.h"

#include <absl/strings/str_cat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <utility>

#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

//______________________________________________________________________________
void SyncIoPolicy::readFullyOrThrow(int fd, char* targetBuffer, size_t numBytes,
                                    uint64_t fileOffset) {
  // `pread` reads up to `numBytes` bytes from file descriptor `fd` at offset
  // `fileOffset` (from the start of the file) into `targetBuffer`. The file
  // offset is not changed. On success, it returns the number of bytes read (0
  // indicates end of file); on error it returns -1 and sets `errno`. See
  // https://man7.org/linux/man-pages/man2/pread.2.html for more details.
  const ssize_t numBytesRead =
      pread(fd, targetBuffer, numBytes, static_cast<off_t>(fileOffset));

  if (numBytesRead < 0) {
    AD_THROW("pread failed in readFullyOrThrow");
  }
  // A result smaller than requested (a partial read, or 0 at end of file) means
  // we read fewer bytes than expected, which we treat as an error.
  if (static_cast<size_t>(numBytesRead) != numBytes) {
    AD_THROW("read fewer bytes than requested in readFullyOrThrow");
  }
}

//______________________________________________________________________________
void SyncIoPolicy::addBatch(int fd,
                            ql::span<const size_t> numBytesToReadPerRequest,
                            ql::span<const uint64_t> fileOffsetPerRequest,
                            ql::span<char*> targetBufferPerRequest,
                            [[maybe_unused]] BatchHandle handle) const {
  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    SyncIoPolicy::readFullyOrThrow(fd, targetBuf, numBytesToRead, fileOffset);
  }
}

#ifdef QLEVER_HAS_IO_URING

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize)
    : IoUringPolicy(ringSize, LiburingCalls{}) {}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize, LiburingCalls liburingCalls)
    : ringSize_(ringSize), liburingCalls_(std::move(liburingCalls)) {
  setUpRing();
}

//______________________________________________________________________________
void IoUringPolicy::setUpRing() {
  // Set up the submission and completion queues, shared between this process
  // and the kernel, with (at least) `ringSize_` submission slots in the
  // submission queue. liburing rounds the requested size up to a power of two,
  // so the actual ring may be larger than `ringSize`; `ringSize_` is therefore
  // a conservative (lower) bound for the "ring full" check below. See
  // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
  // details.
  int ret = io_uring_queue_init(ringSize_, &ring_, /*flags=*/0);
  if (ret < 0) {
    AD_THROW("io_uring_queue_init failed in IoUringManager");
  }
  ringIsSetUp_ = true;
}

//______________________________________________________________________________
IoUringPolicy::~IoUringPolicy() {
  // A ring that is not set up has no read in flight.
  if (!ringIsSetUp_) {
    return;
  }
  if (numInFlightReadRequests_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with " << numInFlightReadRequests_
                << " read request(s) still in flight; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  // Reap the outstanding completions before tearing down the ring, so the
  // kernel is no longer writing into any target buffer once we return. We
  // deliberately do not call `drainOneCqe` here: it can throw, and a
  // destructor must not throw. An interrupted wait is retried; we stop only if
  // `io_uring_wait_cqe` fails otherwise, to avoid spinning forever (it would
  // not decrement the in-flight count).
  while (numInFlightReadRequests_ > 0) {
    io_uring_cqe* cqe = nullptr;
    const int ret = liburingCalls_.waitCqe(&ring_, &cqe);
    if (ret == -EINTR) {
      continue;
    }
    if (ret < 0) {
      break;
    }
    io_uring_cqe_seen(&ring_, cqe);
    --numInFlightReadRequests_;
  }
  io_uring_queue_exit(&ring_);
}

//______________________________________________________________________________
void IoUringPolicy::addBatch(int fd,
                             ql::span<const size_t> numBytesToReadPerRequest,
                             ql::span<const uint64_t> fileOffsetPerRequest,
                             ql::span<char*> targetBufferPerRequest,
                             BatchHandle handle) {
  const size_t numReadRequestsToPerform = numBytesToReadPerRequest.size();

  if (numReadRequestsToPerform == 0) {
    return;
  }
  if (!ringIsSetUp_) {
    setUpRing();
  }
  // Count all reads of the batch from the start, so that the batch cannot be
  // erased by a completion while its later reads are still being prepared.
  batches_[handle] = Batch{numReadRequestsToPerform};

  size_t numberOfPreparedReads = 0;
  try {
    for (const auto& [numBytesToRead, fileOffset, targetBuf] :
         ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                              targetBufferPerRequest)) {
      // The ring has no free slot, so make room: submit what we have prepared
      // so far and block until enough completions have been drained.
      if (numInFlightReadRequests_ >= ringSize_) {
        // Flush the SQEs prepared so far to the kernel so the kernel can start
        // servicing them. Their completions will free up submission slots.
        submitPreparedReads();
        while (numInFlightReadRequests_ >= ringSize_) {
          drainOneCqe();
        }
      }

      // Record the read's metadata (the batch it belongs to and how many bytes
      // it should read) under a unique request id. This is done before an SQE
      // is claimed, so that a failing allocation leaves no SQE behind.
      const uint64_t requestId = nextRequestIdToAssign_;
      inFlightReadsByRequestId_[requestId] =
          InFlightRead{handle, numBytesToRead};

      // Claim the next free SQE. The check above guarantees a slot is
      // available, so `io_uring_get_sqe` must not return `nullptr` here.
      io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
      if (sqe == nullptr) {
        inFlightReadsByRequestId_.erase(requestId);
        AD_FAIL();
      }

      // Record the read's parameters in the SQE (this only sets the SQE's
      // fields; the request is not handed to the kernel until a later
      // `io_uring_submit`). io_uring copies the request id (the SQE's
      // `user_data`) verbatim into the matching completion, so `drainOneCqe`
      // can recover it.
      io_uring_prep_read(sqe, fd, targetBuf,
                         static_cast<unsigned>(numBytesToRead),
                         static_cast<__u64>(fileOffset));
      io_uring_sqe_set_data64(sqe, requestId);
      ++nextRequestIdToAssign_;
      numInFlightReadRequests_++;
      ++numberOfPreparedReads;
    }
    // Flush the remaining prepared SQEs to the kernel (the loop above only
    // submits when the submission queue is full, so the last group of SQEs has
    // not yet been submitted).
    submitPreparedReads();
  } catch (...) {
    abandonBatch(handle, numReadRequestsToPerform - numberOfPreparedReads);
    throw;
  }
}

//______________________________________________________________________________
void IoUringPolicy::submitPreparedReads() {
  // `io_uring_sq_ready` is the number of prepared SQEs that the kernel has not
  // yet taken. A submit may take only some of them, then the next one hands
  // the rest to the kernel.
  while (io_uring_sq_ready(&ring_) > 0) {
    const int ret = liburingCalls_.submit(&ring_);
    if (ret > 0 || ret == -EINTR) {
      continue;
    }
    AD_THROW(absl::StrCat(
        "io_uring_submit failed in IoUringPolicy: ",
        ret == 0 ? "the kernel took no SQE" : std::strerror(-ret)));
  }
}

//______________________________________________________________________________
void IoUringPolicy::abandonBatch(BatchHandle handle,
                                 size_t numberOfUnpreparedReads) {
  // The kernel takes the SQEs in the order in which they were prepared, so the
  // SQEs that it has not taken are the most recently prepared ones. They all
  // belong to this batch, because every `addBatch` that returns has handed all
  // of its SQEs to the kernel. They will never complete, so forget them; the
  // ring is torn down below, so that they are never submitted either.
  const size_t numberOfUnsubmittedReads = io_uring_sq_ready(&ring_);
  for (uint64_t requestId = nextRequestIdToAssign_ - numberOfUnsubmittedReads;
       requestId < nextRequestIdToAssign_; ++requestId) {
    inFlightReadsByRequestId_.erase(requestId);
  }
  numInFlightReadRequests_ -= numberOfUnsubmittedReads;
  auto it = batches_.find(handle);
  AD_CORRECTNESS_CHECK(it != batches_.end());
  it->second.numberOfInFlightReads -=
      numberOfUnpreparedReads + numberOfUnsubmittedReads;

  // Reap the reads of this batch that reached the kernel. The batch's failure
  // (if any) is dropped: the exception of `addBatch` replaces it.
  drainBatch(handle);
  batches_.erase(handle);

  if (numberOfUnsubmittedReads > 0) {
    // Tearing down the ring discards the SQEs that were not submitted. The
    // kernel must first be done with the reads of the other batches that it
    // has taken, so they are reaped here; their failures stay recorded for the
    // `wait()` of their batch.
    while (numInFlightReadRequests_ > 0) {
      drainOneCqe();
    }
    io_uring_queue_exit(&ring_);
    ring_ = io_uring{};
    ringIsSetUp_ = false;
  }
}

//______________________________________________________________________________
void IoUringPolicy::drainBatch(BatchHandle handle) {
  // `drainOneCqe` may erase the batch, so look it up anew after each CQE.
  while (true) {
    auto it = batches_.find(handle);
    if (it == batches_.end() || it->second.numberOfInFlightReads == 0) {
      return;
    }
    drainOneCqe();
  }
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  // Drain completions until no read of this batch is in flight, also if one of
  // them failed: the caller frees the target buffers when it unwinds from the
  // exception below, so the kernel must be done with all of them first.
  drainBatch(handle);

  // An entry that is left is a batch with a failed read.
  auto it = batches_.find(handle);
  if (it == batches_.end()) {
    return;
  }
  AD_CORRECTNESS_CHECK(it->second.firstFailedRead.has_value());
  const FailedRead failedRead = it->second.firstFailedRead.value();
  batches_.erase(it);
  // `cqe->res` < 0 is `-errno`.
  if (failedRead.result < 0) {
    AD_THROW(absl::StrCat("I/O error in IoUringPolicy read operation: ",
                          std::strerror(-failedRead.result)));
  }
  // A result smaller than requested (a partial read, or 0 at end of file) means
  // we read fewer bytes than expected, which we treat as an error.
  AD_THROW(absl::StrCat("read fewer bytes than requested in IoUringPolicy (",
                        failedRead.result, " of ",
                        failedRead.numberOfRequestedBytes, " bytes)"));
}

//______________________________________________________________________________
void ad_utility::IoUringPolicy::drainOneCqe() {
  // Block until at least one completion queue entry (CQE) is available. A wait
  // that a signal interrupts is retried.
  io_uring_cqe* cqe = nullptr;
  int ret = liburingCalls_.waitCqe(&ring_, &cqe);
  while (ret == -EINTR) {
    ret = liburingCalls_.waitCqe(&ring_, &cqe);
  }
  if (ret < 0) {
    AD_THROW(absl::StrCat("io_uring_wait_cqe failed in IoUringPolicy: ",
                          std::strerror(-ret)));
  }

  // Recover the read's result (`cqe->res`) and the request id we stored in the
  // SQE, then consume the CQE so its slot is freed.
  const int numBytesRead = cqe->res;
  const uint64_t requestId = io_uring_cqe_get_data64(cqe);
  io_uring_cqe_seen(&ring_, cqe);
  numInFlightReadRequests_--;

  // Every reaped CQE corresponds to exactly one in-flight read whose id we
  // inserted in `addBatch`, so the entry must be present.
  auto reqIt = inFlightReadsByRequestId_.find(requestId);
  AD_CORRECTNESS_CHECK(reqIt != inFlightReadsByRequestId_.end());
  const InFlightRead inFlightRead = reqIt->second;
  inFlightReadsByRequestId_.erase(reqIt);

  // Attribute the completion to its batch and decrement that batch's in-flight
  // count. The entry must still be present here: the read we are processing
  // belongs to this batch and was outstanding, so the batch's count was at
  // least one and it had not yet been erased.
  auto it = batches_.find(inFlightRead.batchHandle);
  AD_CORRECTNESS_CHECK(it != batches_.end());
  Batch& batch = it->second;
  // A read fails if `cqe->res` < 0 (it is `-errno`) or if it read fewer bytes
  // than requested (a partial read, or 0 at end of file). The first failure is
  // recorded and thrown by the `wait()` of the batch, but only after all of
  // its reads have completed.
  const bool readFailed =
      numBytesRead < 0 ||
      static_cast<size_t>(numBytesRead) != inFlightRead.expectedNumBytes;
  if (readFailed && !batch.firstFailedRead.has_value()) {
    batch.firstFailedRead =
        FailedRead{numBytesRead, inFlightRead.expectedNumBytes};
  }
  // Erase the batch once its last read completes, unless a read failed: then
  // its `wait()` erases it when it throws the failure.
  if (--batch.numberOfInFlightReads == 0 &&
      !batch.firstFailedRead.has_value()) {
    batches_.erase(it);
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
