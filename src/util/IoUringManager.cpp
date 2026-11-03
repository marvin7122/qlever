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

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <string>
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
IoUringPolicy::IoUringPolicy(unsigned ringSize) : ringSize_(ringSize) {
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
}

//______________________________________________________________________________
// Drain the outstanding reads, so that the kernel no longer writes into any
// target buffer, then tear down the ring.
IoUringPolicy::~IoUringPolicy() {
  if (!failedBatches_.empty()) {
    AD_LOG_WARN << "IoUringPolicy destroyed with " << failedBatches_.size()
                << " failed batch(es) that were never `wait()`ed; one of the "
                   "failures is: "
                << failedBatches_.begin()->second << "\n";
  }
  if (numOutstandingReadRequests_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with "
                << numOutstandingReadRequests_
                << " read request(s) still outstanding; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  // Reap the outstanding completions before tearing down the ring, so the
  // kernel is no longer writing into any target buffer once we return. We
  // deliberately do not call `reapWave` here: it throws if `io_uring_wait_cqes`
  // fails and on its correctness checks, and a destructor must not throw.
  // (Failed reads do not throw there; `processCqe` only records them.) We also
  // stop if `io_uring_wait_cqe` fails, to avoid spinning forever (it would not
  // decrement the outstanding count).
  while (numOutstandingReadRequests_ > 0) {
    io_uring_cqe* cqe = nullptr;
    if (io_uring_wait_cqe(&ring_, &cqe) < 0) {
      break;
    }
    io_uring_cqe_seen(&ring_, cqe);
    --numOutstandingReadRequests_;
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
  numOutstandingReadRequestsPerBatch_[handle] = numReadRequestsToPerform;

  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    // The ring has no free slot, so make room: submit what we have prepared so
    // far and block until enough completions have been drained.
    if (numOutstandingReadRequests_ >= ringSize_) {
      // Flush the SQEs prepared so far to the kernel so the kernel can start
      // servicing them. Their completions will free up submission slots.
      // Wait for a wave of completions instead of one, and reap every ready
      // CQE, so a large batch refills the ring in waves rather than one SQE
      // per reaped CQE.
      io_uring_submit(&ring_);
      while (numOutstandingReadRequests_ >= ringSize_) {
        reapWave(static_cast<unsigned>(
            std::min<size_t>(REAP_WAVE, numOutstandingReadRequests_)));
      }
    }

    // Claim the next free SQE. The check above guarantees a slot is available,
    // so `io_uring_get_sqe` must not return `nullptr` here.
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);

    // Record the read's parameters in the SQE (this only sets the SQE's fields;
    // the request is not handed to the kernel until a later `io_uring_submit`).
    io_uring_prep_read(sqe, fd, targetBuf,
                       static_cast<unsigned>(numBytesToRead),
                       static_cast<__u64>(fileOffset));

    // Tag the SQE with a unique request id and record its metadata (the batch
    // it belongs to and how many bytes it should read). io_uring copies the
    // request id (the SQE's `user_data`) verbatim into the matching completion,
    // so `processCqe` can recover it.
    const uint64_t requestId = nextRequestIdToAssign_++;
    outstandingReadsByRequestId_[requestId] =
        OutstandingRead{handle, numBytesToRead};
    io_uring_sqe_set_data64(sqe, requestId);
    numOutstandingReadRequests_++;
  }
  // Flush the remaining prepared SQEs to the kernel (the loop above only
  // submits when the submission queue is full, so the last group of SQEs has
  // not yet been submitted).
  io_uring_submit(&ring_);
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  // Drain completions until this batch is gone. `processCqe` erases a batch as
  // soon as its last read completes, so a present entry always still has
  // outstanding reads. Waiting for up to `REAP_WAVE` CQEs never waits longer
  // than this batch needs: it cannot finish before its own remaining reads
  // complete, and any CQE (also of other batches) counts towards the wave.
  // The iterator is looked up anew in every round, because `reapWave` may erase
  // the entry.
  while (true) {
    auto it = numOutstandingReadRequestsPerBatch_.find(handle);
    if (it == numOutstandingReadRequestsPerBatch_.end()) {
      break;
    }
    reapWave(static_cast<unsigned>(std::min<size_t>(REAP_WAVE, it->second)));
  }
  // Throw only now that every read of the batch is reaped, so the kernel no
  // longer writes into its buffers.
  if (auto failed = failedBatches_.find(handle);
      failed != failedBatches_.end()) {
    std::string errorMessage = std::move(failed->second);
    failedBatches_.erase(failed);
    AD_THROW(std::move(errorMessage));
  }
}

//______________________________________________________________________________
void IoUringPolicy::reapWave(unsigned waveSize) {
  AD_CORRECTNESS_CHECK(waveSize > 0);
  AD_CORRECTNESS_CHECK(waveSize <= numOutstandingReadRequests_);

  // Block until `waveSize` completion queue entries (CQEs) are ready. This
  // typically costs one `io_uring_enter` for the whole wave. A signal ends the
  // wait early: with an empty completion queue, `io_uring_wait_cqes` returns
  // `-EINTR` and we wait again; with at least one ready CQE, it returns 0
  // although fewer than `waveSize` CQEs may be ready (the kernel then reports
  // success, see `io_cqring_wait`), and we reap those.
  io_uring_cqe* cqe = nullptr;
  int ret = 0;
  do {
    ret = io_uring_wait_cqes(&ring_, &cqe, waveSize, nullptr, nullptr);
  } while (ret == -EINTR);
  if (ret < 0) {
    AD_THROW(absl::StrCat("io_uring_wait_cqes failed in IoUringPolicy: ",
                          std::strerror(-ret)));
  }

  // Reap every ready CQE in chunks. `io_uring_peek_batch_cqe` does not block;
  // `io_uring_cq_advance` releases a whole chunk with one CQ-head update
  // instead of one `io_uring_cqe_seen` per CQE. The result and the request id
  // of each CQE are copied out and the chunk is released before any of them is
  // applied to the bookkeeping, and every CQE of the chunk is applied even if
  // `processCqe` throws for one of them (its correctness checks, or the
  // allocation of a failure message): a CQE that stayed in the completion
  // queue would be processed a second time by the next drain, and a CQE that
  // left the queue without being applied would leave the outstanding counts
  // too high, so that the next `wait()` or the destructor would block forever.
  // The first exception is rethrown once every ready CQE is applied.

  // The two fields of a CQE that `processCqe` needs.
  struct CompletedRead {
    int numBytesRead;
    uint64_t requestId;
  };
  std::array<io_uring_cqe*, REAP_CHUNK> cqes{};
  std::array<CompletedRead, REAP_CHUNK> completedReadsBuffer{};
  size_t numReaped = 0;
  std::exception_ptr firstException;
  while (true) {
    const unsigned n = io_uring_peek_batch_cqe(
        &ring_, cqes.data(), static_cast<unsigned>(cqes.size()));
    if (n == 0) {
      break;
    }
    const ql::span<io_uring_cqe* const> readyCqes(cqes.data(), n);
    const ql::span<CompletedRead> completedReads(completedReadsBuffer.data(),
                                                 n);
    ql::ranges::transform(
        readyCqes, completedReads.begin(), [](const io_uring_cqe* cqe) {
          // Recover the id via the 64-bit `user_data` field, see `addBatch`.
          return CompletedRead{cqe->res, io_uring_cqe_get_data64(cqe)};
        });
    io_uring_cq_advance(&ring_, n);
    for (const auto& [numBytesRead, requestId] : completedReads) {
      try {
        processCqe(numBytesRead, requestId);
      } catch (...) {
        if (!firstException) {
          firstException = std::current_exception();
        }
      }
    }
    numReaped += n;
  }
  if (firstException) {
    std::rethrow_exception(std::move(firstException));
  }
  // `io_uring_wait_cqes` returned 0, so at least one CQE was ready, and the
  // loop above reaps every ready CQE.
  AD_CORRECTNESS_CHECK(numReaped > 0);
}

//______________________________________________________________________________
void IoUringPolicy::processCqe(int numBytesRead, uint64_t requestId) {
  // Every reaped CQE corresponds to exactly one outstanding read whose id we
  // inserted in `addBatch`, so the entry must be present, and so must the
  // entry of its batch. Both lookups and checks happen before any count is
  // changed, so that a failing check leaves the bookkeeping consistent.
  auto outstandingReadIt = outstandingReadsByRequestId_.find(requestId);
  AD_CORRECTNESS_CHECK(outstandingReadIt != outstandingReadsByRequestId_.end());
  const OutstandingRead outstandingRead = outstandingReadIt->second;
  auto it =
      numOutstandingReadRequestsPerBatch_.find(outstandingRead.batchHandle);
  AD_CORRECTNESS_CHECK(it != numOutstandingReadRequestsPerBatch_.end());

  // Attribute the completion to its batch and decrement that batch's
  // outstanding count, erasing the batch once its last read is reaped. This
  // happens also for a failed read, so a batch whose read failed does not stay
  // outstanding.
  --numOutstandingReadRequests_;
  outstandingReadsByRequestId_.erase(outstandingReadIt);
  if (--it->second == 0) {
    numOutstandingReadRequestsPerBatch_.erase(it);
  }

  // `cqe->res` < 0 is `-errno`. A result smaller than requested (a partial
  // read, or 0 at end of file) means we read fewer bytes than expected, which
  // we treat as an error. Keep the first failure of the batch; `wait()` of that
  // batch throws it.
  if (failedBatches_.contains(outstandingRead.batchHandle)) {
    return;
  }
  if (numBytesRead < 0) {
    failedBatches_.emplace(
        outstandingRead.batchHandle,
        absl::StrCat("I/O error in IoUringPolicy read operation for request ",
                     requestId, ": ", std::strerror(-numBytesRead)));
  } else if (static_cast<size_t>(numBytesRead) !=
             outstandingRead.expectedNumBytes) {
    failedBatches_.emplace(
        outstandingRead.batchHandle,
        absl::StrCat("read fewer bytes than requested in IoUringPolicy for "
                     "request ",
                     requestId, ": expected ", outstandingRead.expectedNumBytes,
                     " bytes, got ", numBytesRead));
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
