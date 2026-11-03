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
void SyncIoPolicy::readBatch(int fd, const BatchOfReads& reads) const {
  // `BatchOfReads` guarantees equal lengths, so `zip` drops no read.
  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(reads.numberOfBytes(), reads.offsets(),
                            reads.buffers())) {
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
  // `io_uring_queue_init` fails or `probe` does not confirm the operations that
  // `IoUringPolicy` uses.
  LiburingRing(unsigned ringSize, detail::OpcodeProbe probe) {
    // Set up the submission and completion queues, shared between this
    // process and the kernel, with `ringSize` submission slots (rounded up to
    // a power of two) and twice as many completion slots. See
    // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
    // details.
    const int result = io_uring_queue_init(ringSize, &ring_, /*flags=*/0);
    if (result < 0) {
      AD_THROW(
          absl::StrCat("io_uring_queue_init failed: ", errorText(-result)));
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
    // the matching completion. `IoUringPolicy` checks that `numberOfBytes`
    // fits into an `int`, so the cast does not truncate.
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

namespace detail {
//______________________________________________________________________________
OpcodeSupport probeOpcodes(io_uring* ring) noexcept {
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
    : ring_{std::move(ring)},
      // `run` keeps at most `ringSize_` requests in the ring, counting those
      // that are queued, in the kernel, or completed but not reaped. So each of
      // them has a free SQE, and at most `ringSize_` completions are ever
      // pending.
      ringSize_{ring_ != nullptr ? ring_->sqCapacity() : 0} {
  AD_CONTRACT_CHECK(ring_ != nullptr,
                    "An IoUringPolicy needs a ring to run on.");
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
  Batch batch{reads};
  // `run` throws only if a correctness check fails or memory runs out. The
  // bookkeeping can then no longer tell which reads the kernel still holds, so
  // the exception must not reach the caller, who would free the buffers.
  ad_utility::terminateIfThrows(
      [&]() { run(fd, batch); },
      "IoUringPolicy::readBatch must not throw while the kernel may hold "
      "reads");
  // The kernel holds no read of the batch any more.
  if (batch.submitFailure_.has_value() &&
      batch.submitFailure_.value().ringIsBroken) {
    retireRing(batch.submitFailure_.value().message);
  }
  if (batch.firstFailure_.has_value()) {
    std::visit(
        [](const auto& readFailure) {
          using T = std::decay_t<decltype(readFailure)>;
          if constexpr (std::is_same_v<T, ReadError>) {
            AD_THROW(absl::StrCat("I/O error in IoUringPolicy read operation: ",
                                  errorText(readFailure.errorNumber)));
          } else {
            AD_THROW(absl::StrCat(
                "read fewer bytes than requested in IoUringPolicy (",
                readFailure.numberOfBytesRead, " of ",
                readFailure.numberOfRequestedBytes, " bytes)"));
          }
        },
        batch.firstFailure_.value());
  }
  if (batch.submitFailure_.has_value()) {
    // Some reads were cancelled. Reading the whole batch again is simpler
    // than tracking which ones, and the reads that completed write the same
    // bytes again.
    SyncIoPolicy{}.readBatch(fd, reads);
  }
}

//______________________________________________________________________________
void IoUringPolicy::run(int fd, Batch& batch) {
  const auto numberOfBytesPerRead = batch.reads_.numberOfBytes();
  const auto offsets = batch.reads_.offsets();
  const auto buffers = batch.reads_.buffers();
  for (const size_t i : ql::views::iota(size_t{0}, batch.reads_.size())) {
    // After a failed read the batch throws anyway, so no further read is
    // queued.
    if (batch.firstFailure_.has_value()) {
      break;
    }
    // The ring has no free slot, so make room: submit what we have prepared so
    // far and reap completions until a slot is free. The reaping waits for a
    // wave of completions instead of one, so that a large batch refills the
    // ring in waves rather than one SQE per reaped CQE.
    if (numberOfOutstandingReads_ + numberOfNoOpsQueued_ +
            numberOfNoOpsInKernel_ >=
        ringSize_) {
      batch.submitFailure_ = submitQueued();
      if (batch.submitFailure_.has_value()) {
        cancelQueuedReads(batch);
        break;
      }
      while (numberOfOutstandingReads_ + numberOfNoOpsInKernel_ >= ringSize_) {
        reapWave(batch, numberOfRequestsInKernel());
      }
    }
    // A read's request id is its index in the batch. io_uring copies it
    // verbatim into the matching completion, so `processCompletion` can
    // recover the read. The check above guarantees a free SQE.
    AD_CORRECTNESS_CHECK(
        ring_->prepareRead(fd, buffers[i], numberOfBytesPerRead[i], offsets[i],
                           i),
        "Fewer than `ringSize` requests are in the ring, so a free SQE must "
        "exist.");
    ++batch.numberOfPreparedReads_;
    ++numberOfOutstandingReads_;
    ++numberOfQueuedReads_;
  }
  // Flush the remaining prepared SQEs to the kernel (the loop above only
  // submits when the ring is full, so the last group of SQEs has not yet been
  // submitted).
  if (!batch.submitFailure_.has_value()) {
    batch.submitFailure_ = submitQueued();
    if (batch.submitFailure_.has_value()) {
      cancelQueuedReads(batch);
    }
  }
  // Reap the reads that the kernel took, also after a failed submit, because
  // the caller frees the buffers when `readBatch` returns. The no-ops write
  // nothing, so they are not waited for: on a broken ring that wait could fail
  // or block.
  while (numberOfOutstandingReads_ > 0) {
    reapWave(batch, numberOfOutstandingReads_);
  }
}

//______________________________________________________________________________
std::optional<IoUringPolicy::SubmitFailure> IoUringPolicy::submitQueued() {
  // `submit` returns the number of SQEs that the kernel took, or `-errno`. The
  // SQEs it did not take stay queued. `EINTR` is retried at once. `EAGAIN`
  // means that the kernel could not allocate memory for the requests, and so
  // does `ENOMEM` (not in io_uring_enter(2), but returned when the kernel sets
  // up the context of a thread that submits to the ring for the first time);
  // they, and a submit that takes no SQE, are retried after a back-off. A
  // completion that is ready frees no memory (the kernel frees a request when
  // it posts its completion), so reaping instead of backing off would not help.
  // Any other error means that the ring is broken (for example, `EBUSY`
  // reports completions that did not fit into the CQ, which the slot bound
  // rules out). See
  // https://man7.org/linux/man-pages/man2/io_uring_enter.2.html
  unsigned numberOfRetries = 0;
  while (numberOfQueuedReads_ + numberOfNoOpsQueued_ > 0) {
    const int result = ring_->submit();
    if (result > 0) {
      onTaken(static_cast<size_t>(result));
      // Only a failure that persists cancels the reads.
      numberOfRetries = 0;
      continue;
    }
    if (result == -EINTR) {
      continue;
    }
    const bool isTransient =
        result == 0 || result == -EAGAIN || result == -ENOMEM;
    if (!isTransient || numberOfRetries == MAX_SUBMIT_RETRIES) {
      return SubmitFailure{
          absl::StrCat(
              "io_uring_submit failed in IoUringPolicy: ",
              result == 0 ? "no SQE was taken" : errorText(-result),
              isTransient
                  ? absl::StrCat(" (after ", MAX_SUBMIT_RETRIES, " retries)")
                  : ""),
          !isTransient};
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
void IoUringPolicy::onTaken(size_t numberOfTakenSqes) {
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
void IoUringPolicy::cancelQueuedReads(const Batch& batch) {
  // The no-ops stay queued and are handed to the kernel by the next submit;
  // their completions are reaped and ignored. The queued reads are the last
  // `numberOfQueuedReads_` prepared ones, because the kernel takes the SQEs in
  // queue order.
  const std::vector<uint64_t> cancelled = ring_->cancelQueued(NO_OP_REQUEST_ID);
  AD_CORRECTNESS_CHECK(cancelled.size() == numberOfQueuedReads_,
                       "Every queued read, and no other read, must have been "
                       "rewritten into a no-op.");
  for (const uint64_t requestId : cancelled) {
    AD_CORRECTNESS_CHECK(
        requestId >= batch.numberOfPreparedReads_ - numberOfQueuedReads_ &&
            requestId < batch.numberOfPreparedReads_,
        "Only a queued read can be cancelled.");
  }
  numberOfOutstandingReads_ -= cancelled.size();
  numberOfQueuedReads_ = 0;
  numberOfNoOpsQueued_ += cancelled.size();
  AD_CORRECTNESS_CHECK(ring_->numberOfQueued() == numberOfNoOpsQueued_,
                       "After cancelling, only no-ops may be queued.");
}

//______________________________________________________________________________
void IoUringPolicy::reapWave(Batch& batch, size_t numberOfRequestsToComplete) {
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
      processCompletion(batch, completion);
    }
    numberOfReady = ring_->popCompletions(completions);
  }
}

//______________________________________________________________________________
void IoUringPolicy::processCompletion(
    Batch& batch, const IoUringInterface::Completion& completion) {
  if (completion.requestId == NO_OP_REQUEST_ID) {
    AD_CORRECTNESS_CHECK(numberOfNoOpsInKernel_ > 0,
                         "A no-op completes only after the kernel took it.");
    --numberOfNoOpsInKernel_;
    return;
  }
  // A read's request id is its index in the batch, and only a read that the
  // kernel took can complete.
  AD_CORRECTNESS_CHECK(
      completion.requestId < batch.numberOfPreparedReads_ &&
          numberOfOutstandingReads_ > numberOfQueuedReads_,
      "Only a read of the current batch that the kernel took can complete.");
  --numberOfOutstandingReads_;
  // `res` < 0 is `-errno`. A result smaller than requested (a partial read, or
  // 0 at end of file) means we read fewer bytes than requested, which we treat
  // as an error. Only the first failure of a batch is kept, and `readBatch`
  // throws it once all of the batch's reads completed.
  const int numberOfBytesRead = completion.result;
  const size_t numberOfRequestedBytes =
      batch.reads_.numberOfBytes()[completion.requestId];
  if (!batch.firstFailure_.has_value()) {
    if (numberOfBytesRead < 0) {
      batch.firstFailure_ = ReadError{-numberOfBytesRead};
    } else if (static_cast<size_t>(numberOfBytesRead) !=
               numberOfRequestedBytes) {
      batch.firstFailure_ = ShortRead{static_cast<size_t>(numberOfBytesRead),
                                      numberOfRequestedBytes};
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::retireRing(const std::string& reason) {
  // The kernel holds no read of the batch: `run` reaps them all first. The
  // no-ops that are left in the ring write nothing.
  ring_.reset();
  numberOfNoOpsQueued_ = 0;
  numberOfNoOpsInKernel_ = 0;
  AD_LOG_WARN << reason << "; this batch manager reads with pread from now on"
              << '\n';
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
