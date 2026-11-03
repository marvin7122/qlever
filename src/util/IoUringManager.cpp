// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/IoUringManager.h"

// Only this file includes liburing, so that its macros reach no other
// translation unit.
#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
// `liburing.h` pulls in the kernel header `<linux/fs.h>`, which defines a macro
// `BLOCK_SIZE`. That is a very common identifier, and a macro of that name
// breaks every header that later declares something with that name (for
// example `boost/sort/common/util/circular_buffer.hpp`, which is pulled in by
// `util/blockSort/BoostSortHeaders.h`). QLever does not use the macro, so we
// simply get rid of it again.
#undef BLOCK_SIZE
#endif

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
#include "util/NoCopyNoMove.h"
#include "util/PageCacheFirstPolicy.h"
#include "util/io_uring/BatchManager.h"
#include "util/io_uring/IoUringPolicy.h"
#include "util/io_uring/SyncIoPolicy.h"

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
void SyncIoPolicy::readBatch(int fd,
                             ql::span<const size_t> numBytesToReadPerRequest,
                             ql::span<const uint64_t> fileOffsetPerRequest,
                             ql::span<char*> targetBufferPerRequest) const {
  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    SyncIoPolicy::readFullyOrThrow(fd, targetBuf, numBytesToRead, fileOffset);
  }
}

#ifdef QLEVER_HAS_IO_URING

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
  // Set up a ring with `ringSize` SQEs. `explicit`, because a `RingSize` is not
  // a ring. Throws if `io_uring_queue_init` fails or the kernel does not
  // support the operations that `IoUringPolicy` uses.
  explicit LiburingRing(RingSize ringSize) {
    // Set up the submission and completion queues, shared between this
    // process and the kernel, with `ringSize` submission slots and twice as
    // many completion slots. `RingSize` is a power of two, so liburing does
    // not round it up. See
    // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
    // details.
    if (io_uring_queue_init(ringSize.value(), &ring_, /*flags=*/0) < 0) {
      AD_THROW("io_uring_queue_init failed in IoUringManager");
    }
    // The destructor does not run if the constructor throws, so the ring is
    // torn down here.
    if (!supportsReadAndNoOp()) {
      io_uring_queue_exit(&ring_);
      AD_THROW(
          "io_uring does not support IORING_OP_READ and IORING_OP_NOP on this "
          "kernel (Linux 5.6 or newer is needed)");
    }
  }

  // `IoUringPolicy` leaves no read in the ring between batches, only no-ops,
  // so no read can write into a buffer after the ring is torn down.
  ~LiburingRing() override { io_uring_queue_exit(&ring_); }

  bool prepareRead(int fd, char* buffer, size_t numBytes, uint64_t offset,
                   uint64_t requestId) override {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      return false;
    }
    // This only fills in the SQE; the kernel sees it at the next
    // `io_uring_submit`. io_uring copies the SQE's `user_data` verbatim into
    // the matching completion.
    io_uring_prep_read(sqe, fd, buffer, static_cast<unsigned>(numBytes),
                       static_cast<__u64>(offset));
    io_uring_sqe_set_data64(sqe, requestId);
    return true;
  }

  size_t numQueued() const override { return io_uring_sq_ready(&ring_); }

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
    const unsigned numReady = io_uring_peek_batch_cqe(
        &ring_, cqes.data(),
        static_cast<unsigned>(std::min(cqes.size(), completions.size())));
    for (unsigned i : ql::views::iota(0u, numReady)) {
      completions[i] =
          Completion{io_uring_cqe_get_data64(cqes[i]), cqes[i]->res};
    }
    io_uring_cq_advance(&ring_, numReady);
    return numReady;
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
    const unsigned numReady = io_uring_sq_ready(&ring_);
    std::vector<uint64_t> cancelled;
    for (const unsigned i : ql::views::iota(0u, numReady)) {
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

  // Return whether the kernel supports the operations that the ring is used
  // for. `IORING_OP_READ` exists since Linux 5.6; on 5.1 to 5.5,
  // `io_uring_queue_init` succeeds, but every read would complete with
  // `EINVAL`. The probe exists since 5.6 as well, so a failed probe also
  // means that the reads are not supported.
  bool supportsReadAndNoOp() {
    io_uring_probe* probe = io_uring_get_probe_ring(&ring_);
    if (probe == nullptr) {
      return false;
    }
    const bool supported = io_uring_opcode_supported(probe, IORING_OP_READ) &&
                           io_uring_opcode_supported(probe, IORING_OP_NOP);
    io_uring_free_probe(probe);
    return supported;
  }
};
}  // namespace

//______________________________________________________________________________
std::unique_ptr<IoUringInterface> makeLiburingRing(RingSize ringSize) {
  return std::make_unique<LiburingRing>(ringSize);
}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(RingSize ringSize)
    : IoUringPolicy(ringSize, makeLiburingRing(ringSize)) {}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(RingSize ringSize,
                             std::unique_ptr<IoUringInterface> ring)
    : ring_{std::move(ring)}, ringSize_{ringSize} {
  AD_CONTRACT_CHECK(ring_ != nullptr,
                    "An IoUringPolicy needs a ring to run on.");
  // `run` keeps at most `ringSize` requests in the ring, counting those that
  // are queued, in the kernel, or completed but not reaped. So it needs a free
  // SQE for each of them, and at most `ringSize` completions are ever pending.
  AD_CONTRACT_CHECK(ring_->sqCapacity() >= ringSize_.value(),
                    "The ring of an IoUringPolicy needs room for `ringSize` "
                    "SQEs.");
  AD_CONTRACT_CHECK(ring_->cqCapacity() >= ringSize_.value(),
                    "The ring of an IoUringPolicy needs room for `ringSize` "
                    "CQEs, or completions could be dropped.");
}

//______________________________________________________________________________
void IoUringPolicy::readBatch(int fd, ql::span<const size_t> numBytesToRead,
                              ql::span<const uint64_t> offsets,
                              ql::span<char*> buffers) {
  validateSameLength(numBytesToRead, offsets, buffers);
  if (numBytesToRead.empty()) {
    return;
  }
  Batch batch;
  batch.numBytes = numBytesToRead;
  batch.states.assign(numBytesToRead.size(), ReadState::NotQueued);
  // `run` throws only if a correctness check fails or memory runs out. The
  // bookkeeping can then no longer tell which reads the kernel still holds, so
  // the exception must not reach the caller, who would free the buffers.
  ad_utility::terminateIfThrows([&]() { run(fd, offsets, buffers, batch); },
                                "IoUringPolicy::readBatch must not throw while "
                                "the kernel may hold reads");
  if (batch.error.has_value()) {
    AD_THROW(std::move(batch.error).value());
  }
}

//______________________________________________________________________________
void IoUringPolicy::run(int fd, ql::span<const uint64_t> offsets,
                        ql::span<char*> buffers, Batch& batch) {
  const size_t numReads = batch.numBytes.size();
  while (true) {
    // Fill the free ring slots with the next reads, unless the batch has
    // failed (then `readBatch` throws anyway). At most `ringSize_` requests are
    // in the ring, so a free SQE is always available and the completion queue
    // (checked by the constructor to have room for `ringSize_` CQEs) cannot
    // overflow.
    while (!batch.error.has_value() && batch.numQueued < numReads &&
           batch.numSlots() + numNoOpsQueued_ + numNoOpsInKernel_ <
               ringSize_.value()) {
      const size_t i = batch.numQueued;
      AD_CORRECTNESS_CHECK(
          ring_->prepareRead(fd, buffers[i], batch.numBytes[i], offsets[i], i),
          "Fewer than `ringSize` requests are in the ring, so a free SQE must "
          "exist.");
      batch.states[i] = ReadState::Queued;
      ++batch.numQueued;
    }
    submit(batch);
    checkQueue(batch);

    const size_t numInKernel = batch.numReadsInKernel() + numNoOpsInKernel_;
    if (numInKernel == 0) {
      // The queue is empty or holds only no-ops (`submit` cancels the reads it
      // cannot submit), so the batch is done or has failed.
      AD_CORRECTNESS_CHECK(
          batch.numDone == numReads || batch.error.has_value(),
          "With nothing in the kernel and no read left to queue, every read "
          "must be done, unless the batch failed and its reads were "
          "cancelled.");
      return;
    }
    // Wait for a wave of completions, but never for more requests than the
    // kernel holds.
    reap(batch,
         static_cast<unsigned>(std::min<size_t>(REAP_WAVE, numInKernel)));
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
  unsigned numRetries = 0;
  while (ring_->numQueued() > 0) {
    const int ret = ring_->submit();
    if (ret > 0) {
      onTaken(batch, static_cast<size_t>(ret));
      // Only an `EAGAIN` that persists cancels the reads.
      numRetries = 0;
      continue;
    }
    if (ret == -EINTR) {
      continue;
    }
    // A submit that takes nothing although SQEs are queued did not make
    // progress either, so it is retried like `EAGAIN`.
    if (ret != 0 && ret != -EAGAIN) {
      // A fatal submit error only forces termination while the kernel
      // holds reads of this batch (it may still write into their
      // buffers). If it holds none, the queued reads were never handed
      // to the kernel, so the batch can fail gracefully instead: cancel
      // its queued reads (their SQEs stay queued as no-ops) and let
      // `readBatch` throw the error. No-ops in the kernel write nothing,
      // so they do not force termination either.
      if (batch.numReadsInKernel() == 0) {
        cancelQueuedReads(
            batch, absl::StrCat("io_uring_submit failed in IoUringPolicy: ",
                                std::strerror(-ret)));
        return;
      }
      terminateBecauseRingIsUnusable("io_uring_submit", -ret);
    }
    if (numRetries == MAX_SUBMIT_RETRIES) {
      cancelQueuedReads(
          batch,
          absl::StrCat("io_uring_submit failed in IoUringPolicy: ",
                       ret == 0 ? "no SQE was taken" : std::strerror(-ret),
                       " (after ", MAX_SUBMIT_RETRIES, " retries)"));
      return;
    }
    ++numRetries;
    // With `EAGAIN` the kernel could not allocate memory for the requests.
    // Completed requests free some, so wait for one if there is any.
    if (batch.numReadsInKernel() + numNoOpsInKernel_ > 0) {
      reap(batch, 1);
    } else {
      ring_->backOff();
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::onTaken(Batch& batch, size_t numTaken) {
  // The queued no-ops precede the queued reads: those of earlier batches were
  // queued before this batch started, and cancelling this batch's reads turns
  // every queued read into a no-op and stops queueing reads.
  const size_t numNoOps = std::min(numTaken, numNoOpsQueued_);
  numNoOpsQueued_ -= numNoOps;
  numNoOpsInKernel_ += numNoOps;
  for ([[maybe_unused]] size_t taken : ql::views::iota(numNoOps, numTaken)) {
    AD_CORRECTNESS_CHECK(batch.numTaken < batch.numQueued,
                         "The kernel cannot take more SQEs than are queued.");
    ReadState& state = batch.states[batch.numTaken++];
    AD_CORRECTNESS_CHECK(state == ReadState::Queued,
                         "The kernel takes SQEs in queue order, so the next "
                         "SQE it takes is the next queued read.");
    state = ReadState::InKernel;
  }
}

//______________________________________________________________________________
void IoUringPolicy::cancelQueuedReads(Batch& batch, std::string errorMessage) {
  const std::vector<uint64_t> cancelled = ring_->cancelQueued(NO_OP_REQUEST_ID);
  AD_CORRECTNESS_CHECK(cancelled.size() == batch.numQueued - batch.numTaken,
                       "Every queued read of the batch, and no other read, "
                       "must have been rewritten into a no-op.");
  for (const uint64_t requestId : cancelled) {
    AD_CORRECTNESS_CHECK(
        requestId >= batch.numTaken && requestId < batch.numQueued,
        "The queued reads are those in [numTaken, numQueued).");
    ReadState& state = batch.states[requestId];
    AD_CORRECTNESS_CHECK(state == ReadState::Queued,
                         "Only a queued read can be cancelled.");
    state = ReadState::Done;
  }
  // The cancelled reads are done; their SQEs stay queued as no-ops.
  batch.numTaken = batch.numQueued;
  batch.numDone += cancelled.size();
  numNoOpsQueued_ += cancelled.size();
  if (!batch.error.has_value()) {
    batch.error = std::move(errorMessage);
  }
}

//______________________________________________________________________________
void IoUringPolicy::reap(Batch& batch, unsigned minComplete) {
  AD_CORRECTNESS_CHECK(
      minComplete > 0 &&
          minComplete <= batch.numReadsInKernel() + numNoOpsInKernel_,
      "A wait for no completion, or for more completions than requests in "
      "the kernel, would return at once or block forever.");
  int ret = 0;
  do {
    ret = ring_->waitForCompletions(minComplete);
  } while (ret == -EINTR);
  if (ret < 0) {
    terminateBecauseRingIsUnusable("io_uring_wait_cqes", -ret);
  }

  std::array<IoUringInterface::Completion, 64> completions{};
  while (true) {
    const size_t numReady = ring_->popCompletions(completions);
    if (numReady == 0) {
      return;
    }
    for (const auto& [requestId, result] :
         ql::views::take(completions, numReady)) {
      if (requestId == NO_OP_REQUEST_ID) {
        AD_CORRECTNESS_CHECK(
            numNoOpsInKernel_ > 0,
            "A no-op completes only after the kernel took it.");
        --numNoOpsInKernel_;
        continue;
      }
      // A read's request id is its index in the batch.
      AD_CORRECTNESS_CHECK(requestId < batch.numTaken,
                           "Only a read that the kernel took can complete.");
      ReadState& state = batch.states[requestId];
      AD_CORRECTNESS_CHECK(state == ReadState::InKernel,
                           "Each read completes exactly once.");
      state = ReadState::Done;
      ++batch.numDone;
      // `result` < 0 is `-errno`. A result smaller than requested (a partial
      // read, or 0 at end of file) means we read fewer bytes than expected,
      // which we treat as an error. Only the first error of a batch is kept.
      const size_t expectedNumBytes = batch.numBytes[requestId];
      if (batch.error.has_value()) {
        continue;
      }
      if (result < 0) {
        batch.error =
            absl::StrCat("I/O error in IoUringPolicy read operation (read ",
                         requestId, " of the batch): ", std::strerror(-result));
      } else if (static_cast<size_t>(result) != expectedNumBytes) {
        batch.error = absl::StrCat(
            "read fewer bytes than requested in IoUringPolicy (read ",
            requestId, " of the batch: ", result, " of ", expectedNumBytes,
            ")");
      }
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::checkQueue(const Batch& batch) const {
  AD_CORRECTNESS_CHECK(
      ring_->numQueued() ==
          numNoOpsQueued_ + (batch.numQueued - batch.numTaken),
      "The SQEs in the ring's queue must be exactly the queued no-ops and "
      "the queued reads of the batch.");
}

#endif  // QLEVER_HAS_IO_URING

namespace {
//______________________________________________________________________________
// Build a `BatchManager` for `ReadPolicy`, decorated with
// `PageCacheFirstPolicy` if the page-cache fast path is supported, so that
// `makeBatchManager` makes that choice in one place for both backends.
template <typename ReadPolicy>
std::unique_ptr<BatchManagerBase> makeBatchManagerFor(RingSize ringSize) {
  if (pageCacheFastPathIsSupported()) {
    return std::make_unique<BatchManager<PageCacheFirstPolicy<ReadPolicy>>>(
        ringSize);
  }
  return std::make_unique<BatchManager<ReadPolicy>>(ringSize);
}
}  // namespace

//______________________________________________________________________________
std::unique_ptr<BatchManagerBase> makeBatchManager(bool& preferIoUring,
                                                   RingSize ringSize) {
#ifdef QLEVER_HAS_IO_URING
  if (preferIoUring) {
    try {
      return makeBatchManagerFor<IoUringPolicy>(ringSize);
    } catch (const std::exception& e) {
      preferIoUring = false;
      AD_LOG_WARN << "io_uring is compiled in but unavailable at runtime ("
                  << e.what()
                  << "); falling back to synchronous pread for vocabulary "
                     "lookups\n";
    }
  }
#else
  preferIoUring = false;
#endif
  return makeBatchManagerFor<SyncIoPolicy>(ringSize);
}

}  // namespace ad_utility
