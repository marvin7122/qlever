// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/io_uring/IoUringPolicy.h"

#ifdef QLEVER_HAS_IO_URING

// Only this file includes liburing, so that its macros reach no other
// translation unit.
#include <liburing.h>
// `liburing.h` pulls in the kernel header `<linux/fs.h>`, which defines a macro
// `BLOCK_SIZE`. That is a very common identifier, and a macro of that name
// breaks every header that later declares something with that name (for
// example `boost/sort/common/util/circular_buffer.hpp`, which is pulled in by
// `util/blockSort/BoostSortHeaders.h`). QLever does not use the macro, so we
// simply get rid of it again.
#undef BLOCK_SIZE

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/IoUringManager.h"
#include "util/Log.h"

namespace ad_utility {

namespace {
// Log that `operation` failed with `error` and terminate the process. Used
// where the kernel may still hold reads of a batch while the ring cannot be
// used any more: throwing would let the caller free buffers that the kernel
// still writes into.
[[noreturn]] void terminateBecauseRingIsUnusable(std::string_view operation,
                                                 int error) noexcept {
  const std::string message = absl::StrCat(
      operation, " failed in IoUringPolicy: ", std::strerror(error),
      ". The kernel may still write into the buffers of submitted reads, so "
      "the process cannot continue safely. Terminating");
  // `std::endl` on purpose: `std::terminate` does not flush the streams, so
  // without the flush the reason for the termination could be lost.
  AD_LOG_ERROR << message << std::endl;
  std::cerr << message << std::endl;
  std::terminate();
}

// `IoUringInterface` via liburing. Neither copyable nor movable: it owns a
// kernel ring.
class LiburingRing final : public IoUringInterface, public NoCopyNoMove {
 public:
  // Set up a ring with (at least) `ringSize` SQEs. Throws if
  // `io_uring_queue_init` fails or `probe` does not confirm the operations
  // that `IoUringPolicy` uses.
  LiburingRing(unsigned ringSize, detail::OpcodeProbe probe) {
    // Set up the submission and completion queues, shared between this
    // process and the kernel, with `ringSize` submission slots (rounded up to
    // a power of two) and twice as many completion slots. See
    // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
    // details.
    const int result = io_uring_queue_init(ringSize, &ring_, /*flags=*/0);
    if (result < 0) {
      AD_THROW(
          absl::StrCat("io_uring_queue_init failed: ", std::strerror(-result)));
    }
    const detail::OpcodeSupport support = probe(&ring_);
    if (support != detail::OpcodeSupport::Supported) {
      // The destructor does not run if the constructor throws, so the ring is
      // torn down here.
      io_uring_queue_exit(&ring_);
      AD_THROW(support == detail::OpcodeSupport::ProbeFailed
                   ? "io_uring could not confirm that the kernel supports "
                     "IORING_OP_READ and IORING_OP_NOP: the probe "
                     "(IORING_REGISTER_PROBE) failed, which happens before "
                     "Linux 5.6, if io_uring_register is blocked, or if "
                     "memory is short"
                   : "the kernel does not support IORING_OP_READ or "
                     "IORING_OP_NOP for io_uring (IORING_OP_READ exists since "
                     "Linux 5.6)");
    }
  }

  // `IoUringPolicy` leaves no read in the ring between batches, only no-ops,
  // so no read can write into a buffer after the ring is torn down.
  ~LiburingRing() override { io_uring_queue_exit(&ring_); }

  bool prepareRead(int fd, char* buffer, size_t numberOfBytes, uint64_t offset,
                   uint64_t requestId) override {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      return false;
    }
    // This only fills in the SQE; the kernel sees it at the next
    // `io_uring_submit`. io_uring copies the SQE's `user_data` verbatim into
    // the matching completion. `IoUringPolicy` checks that `numberOfBytes` fits
    // into an `int`, so the cast does not truncate.
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
    std::array<io_uring_cqe*, MAX_COMPLETIONS_PER_POP> cqes{};
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
    // liburing consumes completions with this id itself (see
    // `IoUringPolicy::NO_OP_REQUEST_ID`).
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

namespace detail {
//______________________________________________________________________________
OpcodeSupport probeOpcodes(::io_uring* ring) noexcept {
  io_uring_probe* probe = io_uring_get_probe_ring(ring);
  if (probe == nullptr) {
    return OpcodeSupport::ProbeFailed;
  }
  const bool supported = io_uring_opcode_supported(probe, IORING_OP_READ) &&
                         io_uring_opcode_supported(probe, IORING_OP_NOP);
  io_uring_free_probe(probe);
  return supported ? OpcodeSupport::Supported : OpcodeSupport::OpcodeMissing;
}

}  // namespace detail

//______________________________________________________________________________
std::unique_ptr<IoUringInterface> makeLiburingRing(unsigned ringSize,
                                                   detail::OpcodeProbe probe) {
  return std::make_unique<LiburingRing>(ringSize, probe);
}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize)
    : IoUringPolicy(makeLiburingRing(ringSize)) {}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(std::unique_ptr<IoUringInterface> ring)
    : ring_{std::move(ring)} {
  AD_CONTRACT_CHECK(ring_ != nullptr,
                    "An IoUringPolicy needs a ring to run on.");
  // `run` keeps at most `ringSize_` requests in the ring, counting those that
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
void IoUringPolicy::readBatch(int fd, const BatchOfReads& reads) {
  AD_CONTRACT_CHECK(
      ql::ranges::all_of(reads.numberOfBytes(),
                         [](size_t numberOfBytes) {
                           return numberOfBytes <= MAX_NUMBER_OF_BYTES_PER_READ;
                         }),
      "A read of an IoUringPolicy must fit into the `int` of its completion.");
  if (ring_ == nullptr) {
    SyncIoPolicy{}.readBatch(fd, reads);
    return;
  }
  if (reads.size() == 0) {
    return;
  }
  Batch batch{fd, reads};
  // `run` throws only if a correctness check fails or memory runs out. The
  // bookkeeping can then no longer tell which reads the kernel still holds, so
  // the exception must not reach the caller, who would free the buffers.
  ad_utility::terminateIfThrows([&]() { run(batch); },
                                "IoUringPolicy::readBatch must not throw while "
                                "the kernel may hold reads");
  // The kernel holds no read of the batch any more.
  if (batch.ringError_.has_value()) {
    retireRing(batch.ringError_.value());
  }
  if (batch.readError_.has_value()) {
    AD_THROW(std::move(batch.readError_).value());
  }
  if (batch.cancelReason_.has_value()) {
    // Some reads were cancelled. Reading the whole batch again is simpler
    // than tracking which ones, and the reads that completed write the same
    // bytes again.
    SyncIoPolicy{}.readBatch(fd, reads);
  }
}

//______________________________________________________________________________
void IoUringPolicy::run(Batch& batch) {
  const size_t numberOfReads = batch.reads_.size();
  const auto numberOfBytesPerRead = batch.reads_.numberOfBytes();
  const auto offsets = batch.reads_.offsets();
  const auto buffers = batch.reads_.buffers();
  while (true) {
    // Fill the free ring slots with the next reads, unless no more may be
    // queued. At most `ringSize_` requests are in the ring, so a free SQE is
    // always available and the completion queue (checked by the constructor
    // to have room for `ringSize_` CQEs) cannot overflow.
    while (!batch.stoppedQueueing() && batch.numberOfQueued_ < numberOfReads &&
           batch.numberOfSlots() + numberOfNoOpsQueued_ +
                   numberOfNoOpsInKernel_ <
               ringSize_) {
      const size_t i = batch.numberOfQueued_;
      ReadState& state = batch.states_[i];
      AD_CORRECTNESS_CHECK(state == ReadState::NotQueued,
                           "Reads are queued in index order, so the next read "
                           "to queue has not been queued yet.");
      AD_CORRECTNESS_CHECK(
          ring_->prepareRead(batch.fd_, buffers[i], numberOfBytesPerRead[i],
                             offsets[i], i),
          "Fewer than `ringSize` requests are in the ring, so a free SQE must "
          "exist.");
      state = ReadState::Queued;
      ++batch.numberOfQueued_;
    }
    submit(batch);
    if (batch.ringError_.has_value()) {
      // The kernel holds no read of the batch, at most no-ops, which write
      // nothing. Waiting for them on the broken ring could fail or block, so
      // the ring is torn down without waiting.
      return;
    }
    checkQueue(batch);

    const size_t numberOfRequestsInKernel =
        batch.numberOfReadsInKernel() + numberOfNoOpsInKernel_;
    if (numberOfRequestsInKernel == 0) {
      // The queue is empty or holds only no-ops (`submit` cancels the reads it
      // cannot submit), so the batch is done or has stopped queueing.
      AD_CORRECTNESS_CHECK(
          batch.numberOfDone_ == numberOfReads || batch.stoppedQueueing(),
          "With nothing in the kernel and no read left to queue, every read "
          "must be done, unless the batch failed or its reads were "
          "cancelled.");
      return;
    }
    // Wait for a wave of completions, but never for more requests than the
    // kernel holds.
    reap(batch, static_cast<unsigned>(
                    std::min<size_t>(REAP_WAVE, numberOfRequestsInKernel)));
  }
}

//______________________________________________________________________________
void IoUringPolicy::submit(Batch& batch) {
  // `io_uring_submit` returns the number of SQEs that the kernel took, or
  // `-errno`. The SQEs it did not take stay queued. Of the errors listed in
  // io_uring_enter(2), only `EINTR` and `EAGAIN` are expected for this ring (no
  // setup flags, and never more requests than CQ slots); the others mean that
  // the ring is broken, which is handled below. See
  // https://man7.org/linux/man-pages/man2/io_uring_enter.2.html
  unsigned numberOfRetries = 0;
  while (ring_->numberOfQueued() > 0) {
    const int result = ring_->submit();
    if (result > 0) {
      onTaken(batch, static_cast<size_t>(result));
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
      // A fatal submit error forces termination only while the kernel holds
      // reads of this batch (it may still write into their buffers). If it
      // holds none, the queued reads were never handed to the kernel: cancel
      // them (their SQEs stay queued as no-ops) and mark the ring as broken,
      // so that `readBatch` tears it down and reads with `pread`. No-ops in
      // the kernel write nothing, so they do not force termination either.
      if (batch.numberOfReadsInKernel() == 0) {
        batch.ringError_ =
            absl::StrCat("io_uring_submit failed in IoUringPolicy: ",
                         std::strerror(-result));
        cancelQueuedReads(batch, batch.ringError_.value());
        return;
      }
      terminateBecauseRingIsUnusable("io_uring_submit", -result);
    }
    if (numberOfRetries == MAX_SUBMIT_RETRIES) {
      cancelQueuedReads(
          batch, absl::StrCat(
                     "io_uring_submit failed in IoUringPolicy: ",
                     result == 0 ? "no SQE was taken" : std::strerror(-result),
                     " (after ", MAX_SUBMIT_RETRIES, " retries)"));
      return;
    }
    ++numberOfRetries;
    // With `EAGAIN` the kernel could not allocate memory for the requests.
    // Completed requests free some, so wait for one if there is any.
    if (batch.numberOfReadsInKernel() + numberOfNoOpsInKernel_ > 0) {
      reap(batch, 1);
    } else {
      ring_->backOff();
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::onTaken(Batch& batch, size_t numberOfTakenSqes) {
  // The queued no-ops precede the queued reads: those of earlier batches were
  // queued before this batch started, and cancelling this batch's reads turns
  // every queued read into a no-op and stops queueing reads.
  const size_t numberOfNoOps =
      std::min(numberOfTakenSqes, numberOfNoOpsQueued_);
  numberOfNoOpsQueued_ -= numberOfNoOps;
  numberOfNoOpsInKernel_ += numberOfNoOps;
  for ([[maybe_unused]] size_t taken :
       ql::views::iota(numberOfNoOps, numberOfTakenSqes)) {
    AD_CORRECTNESS_CHECK(batch.numberOfTaken_ < batch.numberOfQueued_,
                         "The kernel cannot take more SQEs than are queued.");
    ReadState& state = batch.states_[batch.numberOfTaken_++];
    AD_CORRECTNESS_CHECK(state == ReadState::Queued,
                         "The kernel takes SQEs in queue order, so the next "
                         "SQE it takes is the next queued read.");
    state = ReadState::InKernel;
  }
}

//______________________________________________________________________________
void IoUringPolicy::cancelQueuedReads(Batch& batch, std::string cancelReason) {
  const std::vector<uint64_t> cancelled = ring_->cancelQueued(NO_OP_REQUEST_ID);
  AD_CORRECTNESS_CHECK(
      cancelled.size() == batch.numberOfQueued_ - batch.numberOfTaken_,
      "Every queued read of the batch, and no other read, must have been "
      "rewritten into a no-op.");
  for (const uint64_t requestId : cancelled) {
    AD_CORRECTNESS_CHECK(
        requestId >= batch.numberOfTaken_ && requestId < batch.numberOfQueued_,
        "The queued reads are those in [numberOfTaken_, "
        "numberOfQueued_).");
    ReadState& state = batch.states_[requestId];
    AD_CORRECTNESS_CHECK(state == ReadState::Queued,
                         "Only a queued read can be cancelled.");
    state = ReadState::Done;
  }
  // The cancelled reads are done; their SQEs stay queued as no-ops.
  batch.numberOfTaken_ = batch.numberOfQueued_;
  batch.numberOfDone_ += cancelled.size();
  numberOfNoOpsQueued_ += cancelled.size();
  if (!batch.cancelReason_.has_value()) {
    batch.cancelReason_ = std::move(cancelReason);
  }
}

//______________________________________________________________________________
void IoUringPolicy::reap(Batch& batch, unsigned minComplete) {
  AD_CORRECTNESS_CHECK(
      minComplete > 0 &&
          minComplete <= batch.numberOfReadsInKernel() + numberOfNoOpsInKernel_,
      "A wait for no completion, or for more completions than requests in "
      "the kernel, would return at once or block forever.");
  int result = 0;
  do {
    result = ring_->waitForCompletions(minComplete);
  } while (result == -EINTR);
  if (result < 0) {
    terminateBecauseRingIsUnusable("io_uring_wait_cqes", -result);
  }

  const auto numberOfBytesPerRead = batch.reads_.numberOfBytes();
  std::array<IoUringInterface::Completion,
             IoUringInterface::MAX_COMPLETIONS_PER_POP>
      completions{};
  while (true) {
    const size_t numberOfReady = ring_->popCompletions(completions);
    if (numberOfReady == 0) {
      return;
    }
    for (const auto& [requestId, readResult] :
         ql::views::take(completions, numberOfReady)) {
      if (requestId == NO_OP_REQUEST_ID) {
        AD_CORRECTNESS_CHECK(
            numberOfNoOpsInKernel_ > 0,
            "A no-op completes only after the kernel took it.");
        --numberOfNoOpsInKernel_;
        continue;
      }
      // A read's request id is its index in the batch.
      AD_CORRECTNESS_CHECK(requestId < batch.numberOfTaken_,
                           "Only a read that the kernel took can complete.");
      ReadState& state = batch.states_[requestId];
      AD_CORRECTNESS_CHECK(state == ReadState::InKernel,
                           "Each read completes exactly once.");
      state = ReadState::Done;
      ++batch.numberOfDone_;
      // `readResult` < 0 is `-errno`. A result smaller than requested (a
      // partial read, or 0 at end of file) means we read fewer bytes than
      // expected, which we treat as an error. Only the first error of a batch
      // is kept.
      const size_t expectedNumberOfBytes = numberOfBytesPerRead[requestId];
      if (batch.readError_.has_value()) {
        continue;
      }
      if (readResult < 0) {
        batch.readError_ = absl::StrCat(
            "I/O error in IoUringPolicy read operation (read ", requestId,
            " of the batch): ", std::strerror(-readResult));
      } else if (static_cast<size_t>(readResult) != expectedNumberOfBytes) {
        batch.readError_ = absl::StrCat(
            "read fewer bytes than requested in IoUringPolicy (read ",
            requestId, " of the batch: ", readResult, " of ",
            expectedNumberOfBytes, ")");
      }
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::checkQueue(const Batch& batch) const {
  AD_CORRECTNESS_CHECK(
      ring_->numberOfQueued() ==
          numberOfNoOpsQueued_ + (batch.numberOfQueued_ - batch.numberOfTaken_),
      "The SQEs in the ring's queue must be exactly the queued no-ops and "
      "the queued reads of the batch.");
}

//______________________________________________________________________________
void IoUringPolicy::retireRing(const std::string& reason) {
  // The kernel holds no read: `run` marks the ring as broken only then.
  ring_.reset();
  numberOfNoOpsQueued_ = 0;
  numberOfNoOpsInKernel_ = 0;
  AD_LOG_WARN << reason << "; this batch manager reads with pread from now on"
              << '\n';
}

}  // namespace ad_utility

#endif  // QLEVER_HAS_IO_URING
