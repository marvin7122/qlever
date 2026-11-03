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
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>

#include "util/Exception.h"
#include "util/ExceptionHandling.h"
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

namespace {
// The text of the `errno` value `errorNumber`. Unlike `std::strerror`,
// thread-safe.
std::string errorText(int errorNumber) {
  return std::generic_category().message(errorNumber);
}

// `IoUringInterface` via liburing. Neither copyable nor movable: it owns a
// kernel ring.
class LiburingRing final : public IoUringInterface, public NoCopyNoMove {
 public:
  // Set up a ring with (at least) `ringSize` SQEs. Throws if
  // `io_uring_queue_init` fails.
  explicit LiburingRing(unsigned ringSize) {
    // Set up the submission and completion queues, shared between this
    // process and the kernel, with `ringSize` submission slots (rounded up to
    // a power of two) and twice as many completion slots. See
    // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
    // details.
    const int result = io_uring_queue_init(ringSize, &ring_, /*flags=*/0);
    if (result < 0) {
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
    // This only fills in the SQE; the kernel sees it at the next
    // `io_uring_submit`. io_uring copies the SQE's `user_data` verbatim into
    // the matching completion.
    io_uring_prep_read(sqe, fd, buffer, static_cast<unsigned>(numberOfBytes),
                       static_cast<__u64>(offset));
    io_uring_sqe_set_data64(sqe, requestId);
    return true;
  }

  size_t numberOfQueued() const noexcept override {
    return io_uring_sq_ready(&ring_);
  }

  // `*kring_entries` exists in every liburing version, `ring_entries` only
  // since 2.3.
  size_t sqCapacity() const noexcept override {
    return *ring_.sq.kring_entries;
  }
  size_t cqCapacity() const noexcept override {
    return *ring_.cq.kring_entries;
  }

  int submit() override { return io_uring_submit(&ring_); }

  int waitForCompletions(unsigned minComplete) override {
    io_uring_cqe* cqe = nullptr;
    return io_uring_wait_cqes(&ring_, &cqe, minComplete, nullptr, nullptr);
  }

  size_t popCompletions(ql::span<Completion> completions) override {
    // `io_uring_peek_cqe` does not block. Each completion is copied before
    // `io_uring_cqe_seen` releases its CQ slot.
    size_t numberOfPopped = 0;
    io_uring_cqe* cqe = nullptr;
    while (numberOfPopped < completions.size() &&
           io_uring_peek_cqe(&ring_, &cqe) == 0) {
      completions[numberOfPopped++] =
          Completion{io_uring_cqe_get_data64(cqe), cqe->res};
      io_uring_cqe_seen(&ring_, cqe);
    }
    return numberOfPopped;
  }

  std::vector<uint64_t> cancelQueued(uint64_t noOpRequestId) override {
    // liburing consumes completions with this id itself.
    AD_CORRECTNESS_CHECK(noOpRequestId != LIBURING_UDATA_TIMEOUT,
                         "liburing consumes the completions of "
                         "LIBURING_UDATA_TIMEOUT itself, so a no-op with this "
                         "id would never be reaped.");
    // Without `IORING_SETUP_SQPOLL`, the kernel reads SQEs only inside our own
    // `io_uring_enter` calls, so the SQEs it has not taken can safely be
    // rewritten between two calls.
    AD_CORRECTNESS_CHECK((ring_.flags & IORING_SETUP_SQPOLL) == 0,
                         "Rewriting queued SQEs is only safe if the kernel "
                         "reads SQEs only inside io_uring_enter (no SQPOLL).");
    // The queued SQEs are the `io_uring_sq_ready` positions from the kernel's
    // head on, and liburing hands out the SQE of position `p` as
    // `sqes[p & mask]` (`io_uring_get_sqe`, in every version). The SQ array is
    // not used: liburing <= 2.2 fills it only when it flushes the SQEs to the
    // kernel. `*kring_mask` exists in every version, `ring_mask` only since
    // 2.3.
    const unsigned mask = *ring_.sq.kring_mask;
    const unsigned head = io_uring_smp_load_acquire(ring_.sq.khead);
    const unsigned numberOfReady = io_uring_sq_ready(&ring_);
    std::vector<uint64_t> cancelled;
    for (const unsigned i : ql::views::iota(0u, numberOfReady)) {
      io_uring_sqe* sqe = &ring_.sq.sqes[(head + i) & mask];
      if (sqe->user_data != noOpRequestId) {
        cancelled.push_back(sqe->user_data);
      }
      io_uring_prep_nop(sqe);
      io_uring_sqe_set_data64(sqe, noOpRequestId);
    }
    return cancelled;
  }

  void backOff() override {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

 private:
  io_uring ring_{};
};
}  // namespace

//______________________________________________________________________________
std::unique_ptr<IoUringInterface> makeLiburingRing(unsigned ringSize) {
  return std::make_unique<LiburingRing>(ringSize);
}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize,
                             BatchManagerParameters parameters)
    : IoUringPolicy(makeLiburingRing(ringSize), parameters) {}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(std::unique_ptr<IoUringInterface> ring,
                             BatchManagerParameters parameters)
    : ring_{std::move(ring)},
      // `addBatch` keeps at most `ringSize_` requests in the ring, counting
      // those that are queued, in the kernel, or completed but not reaped. So
      // each of them has a free SQE, and at most `ringSize_` completions are
      // ever pending.
      ringSize_{ring_ != nullptr ? ring_->sqCapacity() : 0},
      maxSubmitRetries_{parameters.maxSubmitRetries} {
  AD_CONTRACT_CHECK(ring_ != nullptr,
                    "An IoUringPolicy needs a ring to run on.");
  AD_CONTRACT_CHECK(ringSize_ > 0,
                    "The ring of an IoUringPolicy needs an SQE.");
  AD_CONTRACT_CHECK(ring_->cqCapacity() >= ringSize_,
                    "The ring of an IoUringPolicy needs at least as many CQEs "
                    "as SQEs, or completions could be dropped.");
  AD_CONTRACT_CHECK(maxSubmitRetries_ >= 1,
                    "An IoUringPolicy must retry a failed submit at least "
                    "once, or a single transient failure cancels reads.");
}

//______________________________________________________________________________
IoUringPolicy::~IoUringPolicy() {
  if (numberOfInFlightReads_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with " << numberOfInFlightReads_
                << " read request(s) still in flight; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  // Reap the outstanding completions before tearing down the ring (in the
  // destructor of `ring_`), so the kernel is no longer writing into any target
  // buffer once we return. Between two batches no read is queued (`addBatch`
  // submits or cancels all of them), so every in-flight read is in the kernel.
  // The no-ops that are still queued are not submitted here; they refer to no
  // buffer. The failures of batches that were never waited for are dropped.
  ad_utility::terminateIfThrows(
      [this]() {
        while (numberOfRequestsInKernel() > 0) {
          reapWave(numberOfRequestsInKernel());
        }
      },
      "The destructor of IoUringPolicy must not throw while the kernel may "
      "hold reads");
}

//______________________________________________________________________________
void IoUringPolicy::addBatch(
    int fd, ql::span<const size_t> numberOfBytesToReadPerRequest,
    ql::span<const uint64_t> fileOffsetPerRequest,
    ql::span<char*> targetBufferPerRequest, BatchHandle handle) {
  const size_t numberOfReads = numberOfBytesToReadPerRequest.size();

  if (numberOfReads == 0) {
    return;
  }
  // Allocate before the first read is handed to the kernel, so that running
  // out of memory here throws instead of terminating.
  inFlightReadsByRequestId_.reserve(inFlightReadsByRequestId_.size() +
                                    numberOfReads);
  batches_[handle] = Batch{numberOfReads};

  std::optional<std::string> submitFailure;
  // Nothing below throws, except if a correctness check fails or memory runs
  // out. The bookkeeping can then no longer tell which reads the kernel still
  // holds, so the exception must not reach the caller, who would free the
  // buffers.
  ad_utility::terminateIfThrows(
      [&]() {
        size_t numberOfPreparedReads = 0;
        for (const auto& [numberOfBytesToRead, fileOffset, targetBuf] :
             ::ranges::views::zip(numberOfBytesToReadPerRequest,
                                  fileOffsetPerRequest,
                                  targetBufferPerRequest)) {
          // The ring has no free slot, so make room: submit what we have
          // prepared so far and reap completions until a slot is free. The
          // reaping waits for a wave of completions instead of one, so that a
          // large batch refills the ring in waves rather than one SQE per
          // reaped CQE.
          if (ringIsFull()) {
            submitFailure = submitQueued();
            if (submitFailure.has_value()) {
              break;
            }
            while (ringIsFull()) {
              reapWave(numberOfRequestsInKernel());
            }
          }

          // Tag the read with a unique request id and record its metadata
          // (the batch it belongs to and how many bytes it should read).
          // io_uring copies the request id verbatim into the matching
          // completion, so `processCompletion` can recover it.
          const uint64_t requestId = nextRequestIdToAssign_++;
          inFlightReadsByRequestId_[requestId] =
              InFlightRead{handle, numberOfBytesToRead};

          // Prepare the read in the next free SQE (the request is not handed
          // to the kernel until a later submit). The check above guarantees
          // a free SQE.
          AD_CORRECTNESS_CHECK(ring_->prepareRead(
              fd, targetBuf, numberOfBytesToRead, fileOffset, requestId));
          ++numberOfInFlightReads_;
          ++numberOfQueuedReads_;
          ++numberOfPreparedReads;
        }
        // Flush the remaining prepared SQEs to the kernel (the loop above only
        // submits when the submission queue is full, so the last group of
        // SQEs has not yet been submitted).
        if (!submitFailure.has_value()) {
          submitFailure = submitQueued();
        }
        if (submitFailure.has_value()) {
          // The reads that were not prepared will never complete. Then reap the
          // reads of the batch that the kernel holds (the cancelled ones were
          // already forgotten).
          batches_.at(handle).numberOfInFlightReads -=
              numberOfReads - numberOfPreparedReads;
          drainBatch(handle);
        }
      },
      "IoUringPolicy::addBatch must not throw while the kernel may hold "
      "reads");
  if (submitFailure.has_value()) {
    // The caller never gets the handle, so the batch's read failures (if any)
    // are dropped.
    batches_.erase(handle);
    AD_THROW(std::move(submitFailure).value());
  }
}

//______________________________________________________________________________
std::optional<std::string> IoUringPolicy::submitQueued() {
  // `submit` returns the number of SQEs that the kernel took, or `-errno`. The
  // SQEs it did not take stay queued. `EINTR` is retried at once. `EAGAIN`
  // means that the kernel could not allocate memory for the requests, and so
  // does `ENOMEM` (not in io_uring_enter(2), but returned when the kernel sets
  // up the context of a thread that submits to the ring for the first time);
  // they, and a submit that takes no SQE, are retried after a back-off. A
  // completion that is ready frees no memory (the kernel frees a request when
  // it posts its completion), so reaping instead of backing off would not help.
  // Any other error means that the ring is broken. In every case in which the
  // submit fails for good, the queued reads are cancelled, and the caller
  // reaps the reads that the kernel took; that wait terminates if the ring
  // cannot even be waited on. See
  // https://man7.org/linux/man-pages/man2/io_uring_enter.2.html
  unsigned numberOfRetries = 0;
  while (numberOfQueuedReads_ + numberOfNoOpsQueued_ > 0) {
    const int result = ring_->submit();
    if (result > 0) {
      onSqesTaken(static_cast<size_t>(result));
      // Only a failure that persists cancels the reads.
      numberOfRetries = 0;
      continue;
    }
    if (result == -EINTR) {
      continue;
    }
    const bool isTransient =
        result == 0 || result == -EAGAIN || result == -ENOMEM;
    if (!isTransient || numberOfRetries == maxSubmitRetries_) {
      cancelQueuedReads();
      // Only the no-ops of the cancelled reads are left in the queue.
      AD_CORRECTNESS_CHECK(ring_->numberOfQueued() == numberOfNoOpsQueued_,
                           "After cancelling, only no-ops may be queued.");
      return absl::StrCat(
          "io_uring_submit failed in IoUringPolicy: ",
          result == 0 ? "no SQE was taken" : errorText(-result),
          isTransient ? absl::StrCat(" (after ", maxSubmitRetries_, " retries)")
                      : "");
    }
    ++numberOfRetries;
    ring_->backOff();
  }
  // The kernel took every queued SQE. Checked against the ring, so that a
  // broken count of queued SQEs stops here instead of making a later wait
  // block forever.
  AD_CORRECTNESS_CHECK(ring_->numberOfQueued() == 0,
                       "A successful submit must leave nothing queued.");
  return std::nullopt;
}

//______________________________________________________________________________
void IoUringPolicy::onSqesTaken(size_t numberOfTakenSqes) {
  // The kernel takes the SQEs in queue order, and the queued no-ops precede
  // the queued reads.
  const size_t numberOfTakenNoOps =
      std::min(numberOfTakenSqes, numberOfNoOpsQueued_);
  const size_t numberOfTakenReads = numberOfTakenSqes - numberOfTakenNoOps;
  AD_CORRECTNESS_CHECK(numberOfTakenReads <= numberOfQueuedReads_,
                       "The kernel cannot take more SQEs than are queued.");
  numberOfNoOpsQueued_ -= numberOfTakenNoOps;
  numberOfNoOpsInKernel_ += numberOfTakenNoOps;
  numberOfQueuedReads_ -= numberOfTakenReads;
  AD_CORRECTNESS_CHECK(
      ring_->numberOfQueued() == numberOfNoOpsQueued_ + numberOfQueuedReads_,
      "The SQEs in the ring's queue must be exactly the queued no-ops and the "
      "queued reads.");
}

//______________________________________________________________________________
void IoUringPolicy::cancelQueuedReads() {
  // The no-ops stay queued and are handed to the kernel by the next submit;
  // their completions are reaped and ignored.
  const std::vector<uint64_t> cancelled = ring_->cancelQueued(NO_OP_REQUEST_ID);
  AD_CORRECTNESS_CHECK(cancelled.size() == numberOfQueuedReads_,
                       "Every queued read, and no other read, must have been "
                       "rewritten into a no-op.");
  for (const uint64_t requestId : cancelled) {
    auto it = inFlightReadsByRequestId_.find(requestId);
    AD_CORRECTNESS_CHECK(it != inFlightReadsByRequestId_.end(),
                         "Only a read that is in flight can be cancelled.");
    auto batchIt = batches_.find(it->second.batchHandle);
    AD_CORRECTNESS_CHECK(batchIt != batches_.end());
    --batchIt->second.numberOfInFlightReads;
    inFlightReadsByRequestId_.erase(it);
  }
  numberOfInFlightReads_ -= cancelled.size();
  numberOfQueuedReads_ = 0;
  numberOfNoOpsQueued_ += cancelled.size();
}

//______________________________________________________________________________
void IoUringPolicy::drainBatch(BatchHandle handle) {
  // `reapWave` may erase the batch, so look it up anew after each wave. A wave
  // for at most the batch's remaining reads never waits longer than the batch
  // needs: the batch cannot finish before they complete, and every CQE (also of
  // other batches) counts towards the wave. They are all in the kernel: a
  // batch keeps no read queued once `addBatch` returns or cancels them.
  while (true) {
    auto it = batches_.find(handle);
    if (it == batches_.end() || it->second.numberOfInFlightReads == 0) {
      return;
    }
    reapWave(it->second.numberOfInFlightReads);
  }
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  // Drain completions until no read of this batch is in flight, also if one of
  // them failed: the caller frees the target buffers when it unwinds from the
  // exception below, so the kernel must be done with all of them first.
  std::optional<ReadFailure> failure;
  ad_utility::terminateIfThrows(
      [&]() {
        drainBatch(handle);
        // An entry that is left belongs to a batch with a failed read.
        auto it = batches_.find(handle);
        if (it != batches_.end()) {
          AD_CORRECTNESS_CHECK(it->second.firstFailure.has_value());
          failure = it->second.firstFailure;
          batches_.erase(it);
        }
      },
      "IoUringPolicy::wait must not throw while the kernel may hold reads");
  if (!failure.has_value()) {
    return;
  }
  std::visit(
      [](const auto& readFailure) {
        using T = std::decay_t<decltype(readFailure)>;
        if constexpr (std::is_same_v<T, ReadError>) {
          AD_THROW(absl::StrCat("I/O error in IoUringPolicy read operation: ",
                                errorText(readFailure.errorNumber)));
        } else {
          AD_THROW(
              absl::StrCat("read fewer bytes than requested in IoUringPolicy (",
                           readFailure.numberOfBytesRead, " of ",
                           readFailure.numberOfRequestedBytes, " bytes)"));
        }
      },
      failure.value());
}

//______________________________________________________________________________
void IoUringPolicy::reapWave(size_t numberOfRequestsToComplete) {
  // A wait for more completions than requests in the kernel would block
  // forever.
  AD_CORRECTNESS_CHECK(numberOfRequestsToComplete > 0);
  AD_CORRECTNESS_CHECK(numberOfRequestsToComplete <=
                       numberOfRequestsInKernel());
  std::array<IoUringInterface::Completion, REAP_CHUNK> completions{};
  size_t numberOfReady = ring_->popCompletions(completions);

  // Only if no completion queue entry (CQE) is ready, block until a wave of
  // them is ready, which typically costs one `io_uring_enter` for the whole
  // wave. A signal ends the wait early: with an empty completion queue, the
  // wait returns `-EINTR` and is retried. With a ready CQE, the kernel reports
  // success (see `io_cqring_wait`); liburing then waits once more, and if a
  // signal also ends that wait, it returns 0 with fewer CQEs than the wave,
  // which are reaped. If waiting fails otherwise, the kernel may still write
  // into the buffers of the reads that it holds, and there is no way to learn
  // when it is done, so neither throwing (the callers would free the buffers
  // while unwinding) nor tearing down the ring is safe.
  if (numberOfReady == 0) {
    const auto waveSize = static_cast<unsigned>(
        std::min<size_t>(REAP_WAVE, numberOfRequestsToComplete));
    int result = 0;
    do {
      result = ring_->waitForCompletions(waveSize);
    } while (result == -EINTR);
    if (result < 0) {
      const std::string message = absl::StrCat(
          "io_uring_wait_cqes failed in IoUringPolicy: ", errorText(-result),
          ". The kernel may still write into the buffers of submitted reads, "
          "so the process cannot continue safely. Terminating");
      // `std::endl` on purpose: `std::terminate` does not flush the streams,
      // so without the flush the reason for the termination could be lost.
      AD_LOG_ERROR << message << std::endl;
      std::cerr << message << std::endl;
      std::terminate();
    }
    numberOfReady = ring_->popCompletions(completions);
    // The wait returned 0, so at least one CQE is ready.
    AD_CORRECTNESS_CHECK(numberOfReady > 0);
  }

  // Reap every ready CQE, in chunks of up to `REAP_CHUNK`.
  while (numberOfReady > 0) {
    for (const auto& completion : ql::span<const IoUringInterface::Completion>(
             completions.data(), numberOfReady)) {
      processCompletion(completion);
    }
    numberOfReady = ring_->popCompletions(completions);
  }
}

//______________________________________________________________________________
void IoUringPolicy::processCompletion(
    const IoUringInterface::Completion& completion) {
  if (completion.requestId == NO_OP_REQUEST_ID) {
    --numberOfNoOpsInKernel_;
    return;
  }
  const int numberOfBytesRead = completion.result;
  --numberOfInFlightReads_;

  // Every reaped CQE corresponds to exactly one in-flight read whose id we
  // inserted in `addBatch`, so the entry must be present.
  auto reqIt = inFlightReadsByRequestId_.find(completion.requestId);
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
  // `res` < 0 is `-errno`. A result smaller than requested (a partial read, or
  // 0 at end of file) means we read fewer bytes than requested, which we treat
  // as an error. Only the first failure of a batch is kept, and the `wait()` of
  // the batch throws it once all of the batch's reads completed.
  if (!batch.firstFailure.has_value()) {
    if (numberOfBytesRead < 0) {
      batch.firstFailure = ReadError{-numberOfBytesRead};
    } else if (static_cast<size_t>(numberOfBytesRead) !=
               inFlightRead.numberOfRequestedBytes) {
      batch.firstFailure = ShortRead{static_cast<size_t>(numberOfBytesRead),
                                     inFlightRead.numberOfRequestedBytes};
    }
  }
  if (--batch.numberOfInFlightReads == 0 && !batch.firstFailure.has_value()) {
    batches_.erase(it);
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
