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
#include <string_view>
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

namespace detail {
//______________________________________________________________________________
std::string errorText(int errorNumber) {
  // Unlike `std::strerror`, thread-safe.
  return std::generic_category().message(errorNumber);
}

//______________________________________________________________________________
void terminateBecauseRingIsUnusable(std::string_view operation,
                                    int errorNumber) noexcept {
  const std::string message = absl::StrCat(
      operation, " failed in IoUringPolicy: ", errorText(errorNumber),
      ". The kernel may still write into the buffers of submitted reads, so "
      "the process cannot continue safely. Terminating");
  // `std::endl` on purpose: `std::terminate` does not flush the streams, so
  // without the flush the reason for the termination could be lost.
  AD_LOG_ERROR << message << std::endl;
  std::cerr << message << std::endl;
  std::terminate();
}
}  // namespace detail

namespace {
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

  size_t numberOfQueued() const override { return io_uring_sq_ready(&ring_); }

  // `*kring_entries` exists in every liburing version, `ring_entries` only
  // since 2.3.
  size_t sqCapacity() const override { return *ring_.sq.kring_entries; }
  size_t cqCapacity() const override { return *ring_.cq.kring_entries; }

  int submit() override { return io_uring_submit(&ring_); }

  int waitForCompletions(unsigned minComplete) override {
    io_uring_cqe* cqe = nullptr;
    return io_uring_wait_cqes(&ring_, &cqe, minComplete, nullptr, nullptr);
  }

  size_t popCompletions(ql::span<Completion> completions) override {
    // `io_uring_peek_batch_cqe` does not block; `io_uring_cq_advance` releases
    // all of them with one CQ-head update. They are copied first, so nothing
    // that happens with the results can leave a released CQE unprocessed or a
    // processed one in the queue.
    std::array<io_uring_cqe*, 64> cqes{};
    const unsigned numberOfReady = io_uring_peek_batch_cqe(
        &ring_, cqes.data(),
        static_cast<unsigned>(std::min(cqes.size(), completions.size())));
    for (unsigned i : ql::views::iota(0u, numberOfReady)) {
      completions[i] =
          Completion{io_uring_cqe_get_data64(cqes[i]), cqes[i]->res};
    }
    io_uring_cq_advance(&ring_, numberOfReady);
    return numberOfReady;
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
IoUringPolicy::IoUringPolicy(unsigned ringSize)
    : IoUringPolicy(makeLiburingRing(ringSize)) {}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(std::unique_ptr<IoUringInterface> ring)
    : ring_{std::move(ring)} {
  AD_CONTRACT_CHECK(ring_ != nullptr,
                    "An IoUringPolicy needs a ring to run on.");
  // `addBatch` keeps at most `ringSize_` reads in the ring, counting those that
  // are queued, in the kernel, or completed but not reaped. So each of them
  // has a free SQE, and at most `ringSize_` completions are ever pending.
  ringSize_ = ring_->sqCapacity();
  AD_CONTRACT_CHECK(ringSize_ > 0,
                    "The ring of an IoUringPolicy needs an SQE.");
  AD_CONTRACT_CHECK(ring_->cqCapacity() >= ringSize_,
                    "The ring of an IoUringPolicy needs at least as many CQEs "
                    "as SQEs, or completions could be dropped.");
}

//______________________________________________________________________________
IoUringPolicy::~IoUringPolicy() {
  if (numInFlightReadRequests_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with " << numInFlightReadRequests_
                << " read request(s) still in flight; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  // Reap the outstanding completions before tearing down the ring (in the
  // destructor of `ring_`), so the kernel is no longer writing into any target
  // buffer once we return. Between two batches no read is queued (`addBatch`
  // submits or cancels all of them), so every in-flight read is in the kernel.
  // Queued no-ops are never handed to the kernel; they refer to no buffer.
  // The failures of batches that were never waited for are dropped.
  ad_utility::terminateIfThrows(
      [this]() {
        while (numberOfRequestsInKernel() > 0) {
          drainOneCqe();
        }
      },
      "The destructor of IoUringPolicy must not throw while the kernel may "
      "hold reads");
}

//______________________________________________________________________________
size_t IoUringPolicy::numberOfReadsInKernel() const {
  // The queued SQEs are the queued no-ops and the queued reads; every other
  // in-flight read is in the kernel.
  return numInFlightReadRequests_ -
         (ring_->numberOfQueued() - numberOfNoOpsQueued_);
}

//______________________________________________________________________________
size_t IoUringPolicy::numberOfRequestsInKernel() const {
  return numberOfReadsInKernel() + numberOfNoOpsInKernel_;
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
  batches_[handle] = Batch{numReadRequestsToPerform};

  std::optional<std::string> submitFailure;
  // Nothing below throws, except if a correctness check fails or memory runs
  // out. The bookkeeping can then no longer tell which reads the kernel still
  // holds, so the exception must not reach the caller, who would free the
  // buffers.
  ad_utility::terminateIfThrows(
      [&]() {
        size_t numberOfPreparedReads = 0;
        for (const auto& [numBytesToRead, fileOffset, targetBuf] :
             ::ranges::views::zip(numBytesToReadPerRequest,
                                  fileOffsetPerRequest,
                                  targetBufferPerRequest)) {
          // The ring has no free slot, so make room: submit what we have
          // prepared so far and block until enough completions have been
          // drained.
          if (numInFlightReadRequests_ + numberOfNoOpsQueued_ +
                  numberOfNoOpsInKernel_ >=
              ringSize_) {
            submitFailure = submitQueued();
            if (submitFailure.has_value()) {
              break;
            }
            while (numInFlightReadRequests_ + numberOfNoOpsInKernel_ >=
                   ringSize_) {
              drainOneCqe();
            }
          }

          // Tag the read with a unique request id and record its metadata
          // (the batch it belongs to and how many bytes it should read).
          // io_uring copies the request id verbatim into the matching
          // completion, so `drainOneCqe` can recover it.
          const uint64_t requestId = nextRequestIdToAssign_++;
          inFlightReadsByRequestId_[requestId] =
              InFlightRead{handle, numBytesToRead};

          // Prepare the read in the next free SQE (the request is not handed
          // to the kernel until a later submit). The check above guarantees
          // a free SQE.
          AD_CORRECTNESS_CHECK(ring_->prepareRead(fd, targetBuf, numBytesToRead,
                                                  fileOffset, requestId));
          numInFlightReadRequests_++;
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
              numReadRequestsToPerform - numberOfPreparedReads;
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
  // SQEs it did not take stay queued. Of the errors listed in
  // io_uring_enter(2), only `EINTR` and `EAGAIN` are expected for this ring (no
  // setup flags, and never more requests than CQ slots); the others mean that
  // the ring is broken. See
  // https://man7.org/linux/man-pages/man2/io_uring_enter.2.html
  unsigned numberOfRetries = 0;
  while (ring_->numberOfQueued() > 0) {
    const int result = ring_->submit();
    if (result > 0) {
      // The kernel takes the queued no-ops first, they precede the reads.
      const size_t numberOfTakenNoOps =
          std::min(static_cast<size_t>(result), numberOfNoOpsQueued_);
      numberOfNoOpsQueued_ -= numberOfTakenNoOps;
      numberOfNoOpsInKernel_ += numberOfTakenNoOps;
      AD_CORRECTNESS_CHECK(
          numberOfNoOpsQueued_ <= ring_->numberOfQueued(),
          "The queued SQEs must include every queued no-op; the kernel takes "
          "them before any queued read.");
      // Only an `EAGAIN` that persists cancels the reads.
      numberOfRetries = 0;
      continue;
    }
    if (result == -EINTR) {
      continue;
    }
    // A submit that takes nothing although SQEs are queued did not make
    // progress either, so it is retried like `EAGAIN`.
    if (result != 0 && result != -EAGAIN) {
      // The ring is unusable. That forces termination only while the kernel
      // holds reads (it may still write into their buffers, and they cannot be
      // reaped safely). Otherwise, the queued reads were never handed to the
      // kernel and are cancelled.
      if (numberOfReadsInKernel() > 0) {
        detail::terminateBecauseRingIsUnusable("io_uring_submit", -result);
      }
      cancelQueuedReads();
      return absl::StrCat("io_uring_submit failed in IoUringPolicy: ",
                          detail::errorText(-result));
    }
    if (numberOfRetries == MAX_SUBMIT_RETRIES) {
      cancelQueuedReads();
      return absl::StrCat(
          "io_uring_submit failed in IoUringPolicy: ",
          result == 0 ? "no SQE was taken" : detail::errorText(-result),
          " (after ", MAX_SUBMIT_RETRIES, " retries)");
    }
    ++numberOfRetries;
    // With `EAGAIN` the kernel could not allocate memory for the requests.
    // Completed requests free some, so wait for one if there is any.
    if (numberOfRequestsInKernel() > 0) {
      drainOneCqe();
    } else {
      ring_->backOff();
    }
  }
  return std::nullopt;
}

//______________________________________________________________________________
void IoUringPolicy::cancelQueuedReads() {
  // The no-ops stay queued and are handed to the kernel by the next submit;
  // their completions are reaped and ignored.
  const std::vector<uint64_t> cancelled = ring_->cancelQueued(NO_OP_REQUEST_ID);
  for (const uint64_t requestId : cancelled) {
    auto it = inFlightReadsByRequestId_.find(requestId);
    AD_CORRECTNESS_CHECK(it != inFlightReadsByRequestId_.end(),
                         "Only a read that is in flight can be cancelled.");
    auto batchIt = batches_.find(it->second.batchHandle);
    AD_CORRECTNESS_CHECK(batchIt != batches_.end());
    --batchIt->second.numberOfInFlightReads;
    inFlightReadsByRequestId_.erase(it);
  }
  numInFlightReadRequests_ -= cancelled.size();
  numberOfNoOpsQueued_ += cancelled.size();
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
                                detail::errorText(readFailure.errorNumber)));
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
void ad_utility::IoUringPolicy::drainOneCqe() {
  // A wait for a completion when the kernel holds no request would block
  // forever.
  AD_CORRECTNESS_CHECK(numberOfRequestsInKernel() > 0);
  // Block until at least one completion queue entry (CQE) is available. A wait
  // that a signal interrupts is retried. If waiting fails otherwise, the
  // kernel may still write into the buffers of the reads that it holds, and
  // there is no way to learn when it is done.
  int ret = 0;
  do {
    ret = ring_->waitForCompletions(1);
  } while (ret == -EINTR);
  if (ret < 0) {
    detail::terminateBecauseRingIsUnusable("io_uring_wait_cqe", -ret);
  }

  // Recover the read's result and the request id we stored in the SQE, and
  // consume the CQE so its slot is freed.
  IoUringInterface::Completion completion{};
  AD_CORRECTNESS_CHECK(ring_->popCompletions({&completion, 1}) == 1);
  if (completion.requestId == NO_OP_REQUEST_ID) {
    --numberOfNoOpsInKernel_;
    return;
  }
  const int numBytesRead = completion.result;
  const uint64_t requestId = completion.requestId;
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
  // `cqe->res` < 0 is `-errno`. A result smaller than requested (a partial
  // read, or 0 at end of file) means we read fewer bytes than expected, which
  // we treat as an error. Only the first failure of a batch is kept, and the
  // `wait()` of the batch throws it once all of the batch's reads completed.
  if (!batch.firstFailure.has_value()) {
    if (numBytesRead < 0) {
      batch.firstFailure = ReadError{-numBytesRead};
    } else if (static_cast<size_t>(numBytesRead) !=
               inFlightRead.expectedNumBytes) {
      batch.firstFailure = ShortRead{static_cast<size_t>(numBytesRead),
                                     inFlightRead.expectedNumBytes};
    }
  }
  if (--batch.numberOfInFlightReads == 0 && !batch.firstFailure.has_value()) {
    batches_.erase(it);
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
