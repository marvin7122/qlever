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
#include <chrono>
#include <climits>
#include <cstring>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Log.h"

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
// `liburing.h` pulls in the kernel header `<linux/fs.h>`, which defines a macro
// `BLOCK_SIZE`, a very common identifier. QLever does not use the macro, so we
// get rid of it again.
#undef BLOCK_SIZE
#endif

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

namespace {
// Called when the kernel may hold reads that the policy cannot wait for (a
// failed wait, or a submit that reports more SQEs than were queued). Those
// reads can still write into their target buffers, so neither throwing (the
// callers would free the buffers while unwinding) nor tearing down the ring is
// safe.
[[noreturn]] void terminateBecauseRingIsUnusable(
    std::string_view reason) noexcept {
  // Only to `std::cerr`, flushed by `std::endl`: `std::terminate` does not
  // flush the streams.
  std::cerr << "IoUringPolicy: " << reason
            << ". The kernel may still write into the buffers of submitted "
               "reads, so the process cannot continue safely. Terminating"
            << std::endl;
  std::terminate();
}

// `IoUringInterface` via liburing.
class LiburingRing final : public IoUringInterface,
                           public ad_utility::NoCopyNoMove {
 public:
  explicit LiburingRing(unsigned ringSize) {
    // Set up the submission and completion queues, shared between this
    // process and the kernel, with (at least) `ringSize` submission slots in
    // the submission queue and twice as many completion slots. liburing
    // rounds the requested size up to a power of two, so the actual ring may
    // be larger than `ringSize`; `ringSize` is therefore a conservative
    // (lower) bound for the "ring full" check of `IoUringPolicy`. See
    // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
    // details.
    if (io_uring_queue_init(ringSize, &ring_, /*flags=*/0) < 0) {
      AD_THROW("io_uring_queue_init failed in IoUringManager");
    }
  }

  ~LiburingRing() override { io_uring_queue_exit(&ring_); }

  bool prepareRead(int fd, char* buffer, size_t numberOfBytes, uint64_t offset,
                   uint64_t requestId) override {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      return false;
    }
    // Record the read's parameters in the SQE (this only sets the SQE's
    // fields; the request is not handed to the kernel until a later
    // `io_uring_submit`). io_uring copies the SQE's `user_data` verbatim into
    // the matching completion, so the request id can be recovered from it.
    io_uring_prep_read(sqe, fd, buffer, static_cast<unsigned>(numberOfBytes),
                       static_cast<__u64>(offset));
    io_uring_sqe_set_data64(sqe, requestId);
    return true;
  }

  int submit() override { return io_uring_submit(&ring_); }

  int waitForCompletions(unsigned minComplete) override {
    io_uring_cqe* cqe = nullptr;
    return io_uring_wait_cqes(&ring_, &cqe, minComplete, nullptr, nullptr);
  }

  size_t popCompletions(ql::span<Completion> completions) override {
    // `io_uring_peek_batch_cqe` does not block; `io_uring_cq_advance`
    // releases all of them with one CQ-head update instead of one
    // `io_uring_cqe_seen` per CQE. The fields are copied out before the
    // release, after which the kernel may reuse the slots.
    std::array<io_uring_cqe*, MAX_COMPLETIONS_PER_POP> cqes{};
    const unsigned numberOfReady = io_uring_peek_batch_cqe(
        &ring_, cqes.data(),
        static_cast<unsigned>(std::min(completions.size(), cqes.size())));
    for (size_t i = 0; i < numberOfReady; ++i) {
      completions[i] =
          Completion{io_uring_cqe_get_data64(cqes[i]), cqes[i]->res};
    }
    io_uring_cq_advance(&ring_, numberOfReady);
    return numberOfReady;
  }

  size_t cancelQueued(uint64_t noOpRequestId) override {
    // Without `IORING_SETUP_SQPOLL`, the kernel reads SQEs only inside our own
    // `io_uring_enter` calls, so the SQEs it has not taken can safely be
    // rewritten between two calls. The queued SQEs are the
    // `io_uring_sq_ready` positions from the kernel's head on, and liburing
    // hands out the SQE of position `p` as `sqes[p & mask]`.
    AD_CORRECTNESS_CHECK((ring_.flags & IORING_SETUP_SQPOLL) == 0);
    const unsigned mask = *ring_.sq.kring_mask;
    const unsigned head = io_uring_smp_load_acquire(ring_.sq.khead);
    const unsigned numberOfQueued = io_uring_sq_ready(&ring_);
    for (unsigned i = 0; i < numberOfQueued; ++i) {
      io_uring_sqe* sqe = &ring_.sq.sqes[(head + i) & mask];
      io_uring_prep_nop(sqe);
      io_uring_sqe_set_data64(sqe, noOpRequestId);
    }
    return numberOfQueued;
  }

  void backOff() override {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

 private:
  io_uring ring_{};
};
}  // namespace

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize)
    : IoUringPolicy(ringSize, std::make_unique<LiburingRing>(ringSize)) {}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize,
                             std::unique_ptr<IoUringInterface> ring)
    : ring_(std::move(ring)), ringSize_(ringSize) {
  AD_CONTRACT_CHECK(ring_ != nullptr,
                    "An IoUringPolicy needs a ring to run on.");
  AD_CONTRACT_CHECK(ringSize_ > 0, "An IoUringPolicy needs a ring size > 0.");
}

//______________________________________________________________________________
// Reap the outstanding reads, so that the kernel no longer writes into any
// target buffer, then tear down the ring (in the destructor of `ring_`).
IoUringPolicy::~IoUringPolicy() {
  if (numberOfOutstandingReads() > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with " << numberOfOutstandingReads()
                << " read request(s) still outstanding; all batches should be "
                   "`wait()`ed before destroying the policy. Reaping them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  reapAllOutstandingReads();
  // Logged after the reaping, which can record more failures.
  const auto isFailed = [](const auto& entry) {
    return entry.second.firstFailure.has_value();
  };
  const auto numberOfFailedBatches = static_cast<size_t>(
      std::count_if(batches_.begin(), batches_.end(), isFailed));
  if (numberOfFailedBatches > 0) {
    ad_utility::ignoreExceptionIfThrows([&] {
      AD_LOG_WARN << "IoUringPolicy destroyed with " << numberOfFailedBatches
                  << " failed batch(es) that were never `wait()`ed; one of the "
                     "failures is: "
                  << std::find_if(batches_.begin(), batches_.end(), isFailed)
                         ->second.firstFailure->message()
                  << "\n";
    });
  }
}

//______________________________________________________________________________
std::string IoUringPolicy::ReadFailure::message() const {
  if (result < 0) {
    return absl::StrCat(
        "I/O error in IoUringPolicy read operation for request ", requestId,
        ": ", std::strerror(-result));
  }
  return absl::StrCat(
      "read fewer bytes than requested in IoUringPolicy for request ",
      requestId, ": expected ", expectedNumberOfBytes, " bytes, got ", result);
}

//______________________________________________________________________________
void IoUringPolicy::addBatch(int fd,
                             ql::span<const size_t> numBytesToReadPerRequest,
                             ql::span<const uint64_t> fileOffsetPerRequest,
                             ql::span<char*> targetBufferPerRequest,
                             BatchHandle handle) {
  AD_CONTRACT_CHECK(!batches_.contains(handle),
                    "A batch handle must not be reused while its batch has "
                    "outstanding reads or a failure that was not yet thrown.");
  if (numBytesToReadPerRequest.empty()) {
    return;
  }

  try {
    for (const auto& [numBytesToRead, fileOffset, targetBuf] :
         ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                              targetBufferPerRequest)) {
      // The ring has no free slot, so make room: submit what we have prepared
      // so far, then reap completions (in waves, see `reapWave`) until a slot
      // is free.
      if (numberOfRequestsInRing() >= ringSize_) {
        submitQueuedReads();
        while (numberOfRequestsInRing() >= ringSize_) {
          reapWave(numberOfRequestsInKernel());
        }
      }
      recordAndPrepareRead(fd, targetBuf, numBytesToRead, fileOffset, handle);
    }
    // Submit the remaining prepared reads (the loop above only submits when
    // the ring is full).
    submitQueuedReads();
  } catch (...) {
    // Some reads of this batch may be in the kernel. The caller frees their
    // buffers when it unwinds, so reap them first. The caller never gets the
    // handle, so the batch (and any failure of it) is forgotten.
    reapAllOutstandingReads();
    batches_.erase(handle);
    throw;
  }
}

//______________________________________________________________________________
void IoUringPolicy::recordAndPrepareRead(int fd, char* targetBuffer,
                                         size_t numberOfBytesToRead,
                                         uint64_t fileOffset,
                                         BatchHandle handle) {
  // Record the read (its batch and how many bytes it should read) under a
  // fresh request id first, so that an SQE is only prepared for a read that is
  // fully recorded: a prepared SQE is submitted with the next submit.
  const uint64_t requestId = nextRequestIdToAssign_;
  outstandingReadsByRequestId_.emplace(
      requestId, OutstandingRead{handle, numberOfBytesToRead});
  // Undo the recording; `BatchState` exists then, and has no failure, because
  // `addBatch` checked that the handle is new.
  auto forgetRead = [this, requestId, handle]() noexcept {
    outstandingReadsByRequestId_.erase(requestId);
    if (auto it = batches_.find(handle);
        it != batches_.end() && --it->second.numberOfOutstandingReads == 0 &&
        !it->second.firstFailure.has_value()) {
      batches_.erase(it);
    }
  };
  try {
    ++batches_[handle].numberOfOutstandingReads;
  } catch (...) {
    outstandingReadsByRequestId_.erase(requestId);
    throw;
  }
  // `addBatch` keeps fewer than `ringSize_` requests in the ring here, so a
  // free SQE is available.
  if (!ring_->prepareRead(fd, targetBuffer, numberOfBytesToRead, fileOffset,
                          requestId)) {
    forgetRead();
    AD_THROW("no free SQE although the ring is not full");
  }
  ++nextRequestIdToAssign_;
}

//______________________________________________________________________________
void IoUringPolicy::submitQueuedReads() {
  // The queued no-ops are submitted, too: they occupy ring slots until the
  // kernel has taken and completed them.
  size_t numberOfRetries = 0;
  while (numberOfQueuedReads() + numberOfNoOpsQueued_ > 0) {
    const int result = ring_->submit();
    if (result > 0) {
      // The kernel takes the SQEs in queue order, and the queued no-ops
      // precede the queued reads.
      const auto numberOfTaken = static_cast<size_t>(result);
      if (numberOfTaken > numberOfNoOpsQueued_ + numberOfQueuedReads()) {
        terminateBecauseRingIsUnusable(
            "io_uring_submit reported more SQEs than were queued");
      }
      const size_t numberOfTakenNoOps =
          std::min(numberOfTaken, numberOfNoOpsQueued_);
      numberOfNoOpsQueued_ -= numberOfTakenNoOps;
      numberOfNoOpsInKernel_ += numberOfTakenNoOps;
      numberOfReadsInKernel_ += numberOfTaken - numberOfTakenNoOps;
      numberOfRetries = 0;
      continue;
    }
    // Retry an interrupted submit, like an interrupted wait in `reapWave`.
    if (result == -EINTR) {
      continue;
    }
    // With `EAGAIN`, `EBUSY` or `ENOMEM` (or no SQE taken) the kernel could not
    // allocate the requests for now. Completed requests free some, so wait for
    // one if there is any.
    const bool isTransient = result == 0 || result == -EAGAIN ||
                             result == -EBUSY || result == -ENOMEM;
    if (isTransient && numberOfRetries < MAX_SUBMIT_RETRIES) {
      ++numberOfRetries;
      if (numberOfRequestsInKernel() > 0) {
        reapWave(1);
      } else {
        ring_->backOff();
      }
      continue;
    }
    cancelQueuedReads();
    AD_THROW(absl::StrCat(
        "io_uring_submit failed in IoUringPolicy: ",
        result == 0 ? "the kernel took no SQE" : std::strerror(-result),
        " (after ", numberOfRetries, " retries)"));
  }
}

//______________________________________________________________________________
void IoUringPolicy::cancelQueuedReads() noexcept {
  const size_t numberOfQueued = numberOfQueuedReads();
  if (numberOfQueued == 0) {
    return;
  }
  const size_t numberOfRewritten = ring_->cancelQueued(NO_OP_REQUEST_ID);
  if (numberOfRewritten != numberOfNoOpsQueued_ + numberOfQueued) {
    terminateBecauseRingIsUnusable(
        "the queued SQEs do not match the queued reads and no-ops");
  }
  numberOfNoOpsQueued_ = numberOfRewritten;
  // The kernel takes the SQEs in the order in which they were prepared, so the
  // queued reads are the most recently prepared ones. Forget them; their
  // batch is the one that `addBatch` is adding, which forgets it, too.
  for (uint64_t requestId = nextRequestIdToAssign_ - numberOfQueued;
       requestId < nextRequestIdToAssign_; ++requestId) {
    auto it = outstandingReadsByRequestId_.find(requestId);
    if (it == outstandingReadsByRequestId_.end()) {
      terminateBecauseRingIsUnusable(
          "a queued read is not among the outstanding reads");
    }
    if (auto batchIt = batches_.find(it->second.batchHandle);
        batchIt != batches_.end()) {
      --batchIt->second.numberOfOutstandingReads;
    }
    outstandingReadsByRequestId_.erase(it);
  }
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  // Reap completions until no read of this batch is outstanding. Outside of
  // `addBatch` they are all in the kernel. Waiting for up to `REAP_WAVE` CQEs
  // never waits longer than this batch needs: it cannot finish before its own
  // remaining reads complete, and any CQE (also of other batches) counts
  // towards the wave. The iterator is looked up anew in every round, because
  // `reapWave` may erase the entry.
  try {
    while (true) {
      auto it = batches_.find(handle);
      if (it == batches_.end() || it->second.numberOfOutstandingReads == 0) {
        break;
      }
      reapWave(it->second.numberOfOutstandingReads);
    }
  } catch (...) {
    // The caller frees the buffers of this batch when it unwinds. It got an
    // exception for this handle, so the batch (and any failure of it) is
    // forgotten.
    reapAllOutstandingReads();
    batches_.erase(handle);
    throw;
  }
  // Throw only now that every read of the batch is reaped, so the kernel no
  // longer writes into its buffers.
  if (auto it = batches_.find(handle); it != batches_.end()) {
    const std::optional<ReadFailure> failure = it->second.firstFailure;
    batches_.erase(it);
    if (failure.has_value()) {
      AD_THROW(failure->message());
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::reapAllOutstandingReads() noexcept {
  // Queued reads never reach the kernel, so they need not be submitted.
  cancelQueuedReads();
  // Every round reaps at least one CQE or terminates (a failed wait). A round
  // that throws without reaping one would repeat forever, so terminate then.
  while (numberOfReadsInKernel_ > 0) {
    const size_t numberOfReapedBefore = numberOfReapedCompletions_;
    try {
      reapWave(numberOfRequestsInKernel());
    } catch (...) {
      // The caller rethrows its own first error; this one is a consequence.
      if (numberOfReapedCompletions_ == numberOfReapedBefore) {
        terminateBecauseRingIsUnusable(
            "reaping the outstanding reads failed without progress");
      }
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::reapWave(size_t numberOfRequestsToComplete) {
  AD_CORRECTNESS_CHECK(numberOfRequestsToComplete > 0,
                       "a wave must wait for at least one completion");
  AD_CORRECTNESS_CHECK(numberOfRequestsToComplete <= numberOfRequestsInKernel(),
                       "a wave cannot wait for more requests than are in the "
                       "kernel, or it would block forever");
  std::array<IoUringInterface::Completion, REAP_CHUNK> completions{};
  size_t numberOfReady = ring_->popCompletions(completions);

  // Only if no CQE is ready, block until a wave of CQEs is ready. This
  // typically costs one `io_uring_enter` for the whole wave. A signal ends the
  // wait early: with an empty completion queue, `io_uring_wait_cqes` returns
  // `-EINTR` and we wait again; with at least one ready CQE, it returns 0
  // although fewer than the wave may be ready (the kernel then reports
  // success, see `io_cqring_wait`), and we reap those. Any other failure leaves
  // reads in the kernel that we cannot wait for, so we terminate.
  if (numberOfReady == 0) {
    const auto waveSize = static_cast<unsigned>(
        std::min<size_t>(REAP_WAVE, numberOfRequestsToComplete));
    int result = 0;
    do {
      result = ring_->waitForCompletions(waveSize);
    } while (result == -EINTR);
    if (result < 0) {
      terminateBecauseRingIsUnusable(
          absl::StrCat("io_uring_wait_cqes failed: ", std::strerror(-result)));
    }
    numberOfReady = ring_->popCompletions(completions);
    // The wait returned 0, so at least one CQE is ready.
    AD_CORRECTNESS_CHECK(numberOfReady > 0,
                         "`io_uring_wait_cqes` returned 0 without a ready CQE");
  }

  // Reap every ready CQE in chunks of up to `REAP_CHUNK`. Every CQE of a chunk
  // is applied even if `processCompletion` throws for one of them (a failed
  // correctness check): a CQE that left the queue without being applied would
  // leave the outstanding counts too high, so that the next `wait()` or the
  // destructor would block forever. The first exception is rethrown once every
  // ready CQE is applied.
  std::exception_ptr firstException;
  while (numberOfReady > 0) {
    numberOfReapedCompletions_ += numberOfReady;
    for (const auto& completion : ql::span<const IoUringInterface::Completion>(
             completions.data(), numberOfReady)) {
      try {
        processCompletion(completion);
      } catch (...) {
        if (!firstException) {
          firstException = std::current_exception();
        }
      }
    }
    numberOfReady = ring_->popCompletions(completions);
  }
  if (firstException) {
    std::rethrow_exception(std::move(firstException));
  }
}

//______________________________________________________________________________
void IoUringPolicy::processCompletion(
    const IoUringInterface::Completion& completion) {
  const auto& [requestId, result] = completion;
  if (requestId == NO_OP_REQUEST_ID) {
    AD_CORRECTNESS_CHECK(numberOfNoOpsInKernel_ > 0,
                         "a no-op completed that the kernel never took");
    --numberOfNoOpsInKernel_;
    return;
  }
  // Every other CQE corresponds to exactly one read in the kernel whose id we
  // recorded in `recordAndPrepareRead`.
  auto readIt = outstandingReadsByRequestId_.find(requestId);
  AD_CORRECTNESS_CHECK(
      readIt != outstandingReadsByRequestId_.end(),
      "a CQE carries the id of a read that is not outstanding");
  AD_CORRECTNESS_CHECK(numberOfReadsInKernel_ > 0,
                       "a read completed that the kernel never took");
  const OutstandingRead read = readIt->second;
  outstandingReadsByRequestId_.erase(readIt);
  --numberOfReadsInKernel_;

  // Every outstanding read has a batch entry, see `recordAndPrepareRead`.
  auto batchIt = batches_.find(read.batchHandle);
  AD_CORRECTNESS_CHECK(
      batchIt != batches_.end() && batchIt->second.numberOfOutstandingReads > 0,
      "an outstanding read belongs to a batch that has no "
      "outstanding reads");
  BatchState& batch = batchIt->second;
  --batch.numberOfOutstandingReads;
  // `result` < 0 is `-errno`. A result smaller than requested (a partial read,
  // or 0 at end of file) means we read fewer bytes than expected, which we
  // treat as an error. Keep the first failure of the batch; its `wait()`
  // throws it.
  const bool isFailed =
      result < 0 || static_cast<size_t>(result) != read.expectedNumberOfBytes;
  if (isFailed && !batch.firstFailure.has_value()) {
    batch.firstFailure =
        ReadFailure{requestId, result, read.expectedNumberOfBytes};
  }
  if (batch.numberOfOutstandingReads == 0 && !batch.firstFailure.has_value()) {
    batches_.erase(batchIt);
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
