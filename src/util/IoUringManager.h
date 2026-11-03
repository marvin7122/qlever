// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IOURINGMANAGER_H
#define QLEVER_SRC_UTIL_IOURINGMANAGER_H

#include <gtest/gtest_prod.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/IoUringManagerConcepts.h"
#include "util/Log.h"
#include "util/NoCopyNoMove.h"
#include "util/PageCacheFirstPolicy.h"
#include "util/io_uring/IoUringInterface.h"

namespace ad_utility {

// Abstract base of `BatchManager` so a pool can hold managers of different
// `ReadPolicy`s behind a unified interface and pick the backend at runtime.
class BatchManagerBase {
 public:
  using BatchHandle = uint64_t;
  virtual ~BatchManagerBase() = default;

  [[nodiscard]] virtual BatchHandle addBatch(int fd,
                                             ql::span<const size_t> numBytes,
                                             ql::span<const uint64_t> offsets,
                                             ql::span<char*> buffers) = 0;

  virtual void wait(BatchHandle handle) = 0;

  // Switch the page-cache fast path of this manager on or off (see
  // `PageCacheFirstPolicy`). Takes effect for the batches added afterwards.
  // Managers without a page-cache fast path ignore it.
  virtual void setPageCacheFastPath([[maybe_unused]] bool enabled) noexcept {}
};

// `BatchManager` owns the batch bookkeeping (minting a `BatchHandle` per batch,
// validating the input spans) and delegates the reads from the underlying
// Vocabulary to the `Policy`, which must satisfy the `ReadPolicy` concept
// above.
template <typename ReadPolicy>
class BatchManager final : public BatchManagerBase {
  static_assert(
      ReadPolicyConcept<ReadPolicy>,
      "BatchManager's ReadPolicy must satisfy the ReadPolicyConcept concept.");
  // The policy is only valid if the policy's handle type matches the base's.
  static_assert(
      std::is_same_v<typename ReadPolicy::BatchHandle,
                     BatchManagerBase::BatchHandle>,
      "ReadPolicy::BatchHandle must match BatchManagerBase::BatchHandle.");

 public:
  using BatchHandle = typename BatchManagerBase::BatchHandle;

  explicit BatchManager(unsigned ringSize = 256) : policy_(ringSize) {}

  BatchManager(const BatchManager&) = delete;
  BatchManager& operator=(const BatchManager&) = delete;

  [[nodiscard]] BatchHandle addBatch(int fd, ql::span<const size_t> numBytes,
                                     ql::span<const uint64_t> offsets,
                                     ql::span<char*> buffers) override {
    validateSameLength(numBytes, offsets, buffers);

    BatchHandle handle = nextBatchHandle_++;

    // Delegate the I/O work to the policy.
    policy_.addBatch(fd, numBytes, offsets, buffers, handle);

    return handle;
  }

  // Block until every read in `handle` has completed.
  void wait(BatchHandle handle) override { policy_.wait(handle); }

  // Forward the switch to the policy if it has one.
  void setPageCacheFastPath([[maybe_unused]] bool enabled) noexcept override {
    if constexpr (CPP_requires_ref(detail::HasPageCacheFastPathSwitch,
                                   ReadPolicy)) {
      policy_.setPageCacheFastPath(enabled);
    }
  }

 private:
  [[no_unique_address]] ReadPolicy policy_;
  BatchHandle nextBatchHandle_ = 0;

  template <typename Span0, typename... Spans>
  static void validateSameLength(const Span0& first, const Spans&... rest) {
    const auto n = ql::ranges::size(first);
    auto valid = ((ql::ranges::size(rest) == n) && ...);
    if (!valid) {
      AD_THROW("spans must have same length");
    }
    return;
  }
};

// Fallback implementation for the `IoUringPolicy` below. Schedules pread calls
// in a synchronous (blocking) manner. Single-threaded use only.
struct SyncIoPolicy {
  using BatchHandle = uint64_t;

  // `ringSize` is ignored; it exists only so the policy is constructible the
  // same way as `IoUringPolicy`.
  //
  // NOTE: GCC rejects `[[maybe_unused]]` on a defaulted parameter; cast to
  // void.
  explicit SyncIoPolicy(unsigned ringSize = 256) { (void)ringSize; }

  ~SyncIoPolicy() = default;
  SyncIoPolicy(const SyncIoPolicy&) = delete;
  SyncIoPolicy& operator=(const SyncIoPolicy&) = delete;

  // Immediately execute a batch of reads synchronously. This blocks the calling
  // thread. Read `i` reads `numBytesToReadPerRequest[i]` bytes from file
  // descriptor `fd`, starting at offset `fileOffsetPerRequest[i]` (from the
  // start of the file), into the buffer starting at
  // `targetBufferPerRequest[i]`. `handle` is unused (the batch completes before
  // `addBatch` returns).
  void addBatch(int fd, ql::span<const size_t> numBytesToReadPerRequest,
                ql::span<const uint64_t> fileOffsetPerRequest,
                ql::span<char*> targetBufferPerRequest,
                BatchHandle handle) const;

  void wait(BatchHandle) const {
    // No-op: `addBatch` already completed all reads synchronously.
  }

  // Read exactly `numBytes` bytes from file descriptor `fd` at `fileOffset`
  // (from the start of the file) into `targetBuffer`. Throws exception if the
  // read fails or returns fewer bytes than requested (a partial read or end of
  // file), since every read must be fully satisfied.
  static void readFullyOrThrow(int fd, char* targetBuffer, size_t numBytes,
                               uint64_t fileOffset);
};

// Persistent io_uring manager that accepts multiple named batches of indices to
// be read from the underlying storage medium, submits all SQEs in `addBatch`
// (blocking if the ring is full), and lets the caller block on a specific batch
// via `wait()`. Single-threaded use only. See https://github.com/axboe/liburing
// for more details.
#ifdef QLEVER_HAS_IO_URING

// Neither copyable nor movable: the kernel writes into the target buffers of
// the reads that this object tracks, and it must reap them before it is gone.
class IoUringPolicy : public ad_utility::NoCopyNoMove {
 public:
  using BatchHandle = uint64_t;

  // The size of a wave: when the ring is full or `wait()` blocks and no
  // completion is ready, wait for up to this many completions, then reap all
  // ready ones in one pass (see `reapWave`). This amortizes `io_uring_enter`
  // and the CQ-head update over the wave. Not tuned: in a sweep of 1 to 128 on
  // an earlier version, no value differed from 8 in wall time beyond noise.
  static constexpr unsigned REAP_WAVE = 8;

  // The number of CQEs that `reapWave` takes out of the completion queue at
  // once; a wave with more ready CQEs is reaped in several chunks.
  static constexpr size_t REAP_CHUNK =
      IoUringInterface::MAX_COMPLETIONS_PER_POP;

  // How often `submitQueuedReads` retries a submit that took no SQE
  // (`EAGAIN`, `EBUSY`, `ENOMEM` or 0) before it gives up.
  static constexpr size_t MAX_SUBMIT_RETRIES = 100;

 private:
  std::unique_ptr<IoUringInterface> ring_;
  unsigned ringSize_;

  // Per-read metadata needed when a completion is reaped: which batch the read
  // belongs to, and how many bytes it was supposed to read (so that reading
  // fewer bytes than expected can be detected).
  struct OutstandingRead {
    BatchHandle batchHandle;
    size_t expectedNumberOfBytes;
  };

  // Monotonically increasing counter that mints a unique request id for each
  // individual read. The id is stored in the SQE's `user_data` and recovered
  // from the matching CQE to look up the read's `OutstandingRead` metadata.
  uint64_t nextRequestIdToAssign_ = 0;

  // The request id of the no-ops into which `cancelQueuedReads` rewrites
  // queued SQEs. Not `UINT64_MAX`, which liburing reserves for its timeouts.
  static constexpr uint64_t NO_OP_REQUEST_ID =
      std::numeric_limits<uint64_t>::max() - 1;

  // The outstanding reads by request id: reads that are prepared (SQE filled
  // in, not yet submitted), in flight (submitted to the kernel, not yet
  // completed), or completed but not yet reaped via a completion queue entry
  // (CQE). An entry is inserted when the read is prepared and erased when its
  // completion is reaped or the read is cancelled.
  ad_utility::HashMap<uint64_t, OutstandingRead> outstandingReadsByRequestId_;

  // The first failed or short read of a batch. Recorded without allocating, so
  // that recording it cannot fail; the message is built in `wait()`.
  struct ReadFailure {
    uint64_t requestId;
    // `-errno`, or the number of bytes read if fewer than expected.
    int result;
    size_t expectedNumberOfBytes;
    std::string message() const;
  };

  // The state of a batch that has outstanding reads or a failure that was not
  // yet thrown. A batch whose last read is reaped without a failure is erased
  // at once; a failed batch stays until its `wait()` throws the failure.
  // `addBatch` and the `wait()` of another batch also reap CQEs, so a failure
  // can be recorded long before the batch's own `wait()`.
  struct BatchState {
    size_t numberOfOutstandingReads = 0;
    std::optional<ReadFailure> firstFailure;
  };
  ad_utility::HashMap<BatchHandle, BatchState> batches_;

  // The outstanding reads that the kernel has taken (in flight, or completed
  // but not reaped). The others are queued: prepared but not yet submitted.
  // Outside of `addBatch`, no read is queued.
  size_t numberOfReadsInKernel_ = 0;

  // SQEs that `cancelQueuedReads` rewrote into no-ops: queued, or taken by the
  // kernel but not reaped. They occupy ring slots and produce CQEs, but refer
  // to no buffer. The queued ones precede every queued read.
  size_t numberOfNoOpsQueued_ = 0;
  size_t numberOfNoOpsInKernel_ = 0;

  // The number of CQEs reaped so far, to detect a round of
  // `reapAllOutstandingReads` that made no progress.
  size_t numberOfReapedCompletions_ = 0;

  size_t numberOfOutstandingReads() const {
    return outstandingReadsByRequestId_.size();
  }
  size_t numberOfQueuedReads() const {
    return numberOfOutstandingReads() - numberOfReadsInKernel_;
  }
  // The requests that produce a CQE that is not reaped yet.
  size_t numberOfRequestsInKernel() const {
    return numberOfReadsInKernel_ + numberOfNoOpsInKernel_;
  }
  // `addBatch` keeps this at most `ringSize_`, so that a free SQE is always
  // available and the completion queue (twice as large as the submission
  // queue) cannot overflow.
  size_t numberOfRequestsInRing() const {
    return numberOfOutstandingReads() + numberOfNoOpsQueued_ +
           numberOfNoOpsInKernel_;
  }

  // Record a read of batch `handle` and prepare its SQE (not yet submitted).
  void recordAndPrepareRead(int fd, char* targetBuffer, size_t numBytesToRead,
                            uint64_t fileOffset, BatchHandle handle);

  // Submit every queued read and no-op. Retries an interrupted submit, and a
  // submit that took no SQE (after reaping a completion, or backing off). If
  // that does not help, or for any other error, cancels the queued reads and
  // throws.
  void submitQueuedReads();

  // Rewrite the queued SQEs into no-ops, and forget the queued reads (they
  // never reach the kernel). The ring stays usable: the no-ops are submitted
  // with the next submit, and their CQEs are skipped.
  void cancelQueuedReads() noexcept;

  // Cancel the queued reads, then reap every read in the kernel, without
  // throwing. Called before an exception leaves `addBatch` or `wait()` and in
  // the destructor, because the callers free the target buffers then.
  void reapAllOutstandingReads() noexcept;

  // Reap every ready CQE. If none is ready, first block until a wave of
  // `min(REAP_WAVE, numberOfRequestsToComplete)` CQEs is ready.
  // `numberOfRequestsToComplete` is the number of requests the caller waits
  // for; it must be > 0 and at most `numberOfRequestsInKernel()`, so that the
  // wave never waits for more completions than will arrive. Reaps at least one
  // CQE, but may reap fewer than the wave if a signal interrupts the wait, so
  // the callers loop until their condition holds. Terminates the process if
  // waiting fails, because the reads in the kernel could then still write into
  // buffers that the callers free. If `processCompletion` throws for a CQE,
  // every other ready CQE is still applied, and the first exception is
  // rethrown afterwards.
  void reapWave(size_t numberOfRequestsToComplete);

  // Apply one completion to the bookkeeping: skip a no-op, otherwise remove
  // the read from the outstanding reads (also a failed one) and record a
  // failed or short read as the batch's failure. Never allocates.
  void processCompletion(const IoUringInterface::Completion& completion);

 public:
  // `ringSize` must be > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringPolicy(unsigned ringSize);
  // Run on `ring` instead of a liburing ring (for tests). `ring` must have
  // room for at least `ringSize` SQEs and `2 * ringSize` CQEs.
  IoUringPolicy(unsigned ringSize, std::unique_ptr<IoUringInterface> ring);
  ~IoUringPolicy();

  // Enqueue a batch of read requests and submit them to the kernel. Blocks the
  // calling thread only when the ring is full, in order to reap completion
  // queue entries and free slots. Read `i` reads `numBytesToRead[i]` bytes
  // from file descriptor `fd`, starting at offset `offsets[i]` (from the start
  // of the file), into the buffer starting at `buffers[i]`. The reads are
  // tracked under `handle`, which can be passed to `wait()` to block until
  // this batch has completed. `handle` must not belong to a batch that has
  // outstanding reads or a failure that was not yet thrown. Throws if the
  // reads cannot be submitted (the ring stays usable for later batches). If
  // `addBatch` throws, every read in the kernel (of every batch) is reaped
  // first, so the kernel no longer writes into the buffers, and the batch is
  // forgotten.
  void addBatch(int fd, ql::span<const size_t> numBytesToRead,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle);

  // Block until every read in the batch that is represented by the `handle` has
  // completed. (The `handle` was submitted along the read requests using
  // `addBatch`.) Throws if a read of this batch failed or was short, after all
  // of its reads are reaped; the failure is reported only once, so a later
  // `wait()` for the same handle returns normally. If anything else throws,
  // every read in the kernel (of every batch) is reaped before the exception
  // leaves `wait()`, and the batch is forgotten. A `wait()` for a handle
  // without outstanding reads (a batch that is already reaped, was empty, or
  // is unknown) returns at once, unless the batch has a failure that was not
  // yet thrown.
  void wait(BatchHandle handle);
};

using BatchIoManager = BatchManager<IoUringPolicy>;
#else
using BatchIoManager = BatchManager<SyncIoPolicy>;
#endif

namespace detail {
// Build a `BatchManager` for `Policy`, decorated with `PageCacheFirstPolicy` if
// the page-cache fast path is supported.
template <typename Policy>
std::unique_ptr<BatchManagerBase> makeBatchManagerFor(unsigned ringSize) {
  static_assert(ReadPolicyConcept<Policy>,
                "makeBatchManagerFor's Policy must satisfy the "
                "ReadPolicyConcept.");
  if (pageCacheFastPathIsSupported()) {
    return std::make_unique<BatchManager<PageCacheFirstPolicy<Policy>>>(
        ringSize);
  }
  return std::make_unique<BatchManager<Policy>>(ringSize);
}
}  // namespace detail

// Build a batch manager. When io_uring is compiled in and the runtime flag
// `preferIoUring` is set, try to build an `IoUringManager`. If its setup
// syscall fails at runtime clear `preferIoUring` and fall back to a
// `SyncIoManager`. Passing the flag by reference makes this probe-once: after
// the first failure, every subsequent call goes straight to the sync manager,
// so we don't repeat a failing syscall. Where `preadv2(RWF_NOWAIT)` is
// available, the manager serves page-cache hits first (`PageCacheFirstPolicy`,
// switched with `BatchManagerBase::setPageCacheFastPath`).
inline std::unique_ptr<BatchManagerBase> makeBatchManager(
    bool& preferIoUring, unsigned ringSize = 256) {
#ifdef QLEVER_HAS_IO_URING
  if (preferIoUring) {
    try {
      return detail::makeBatchManagerFor<IoUringPolicy>(ringSize);
    } catch (const std::exception& e) {
      preferIoUring = false;
      AD_LOG_WARN << "io_uring is compiled in but unavailable at runtime ("
                  << e.what()
                  << "); falling back to synchronous pread for vocabulary "
                     "lookups"
                  << std::endl;
    }
  }
#else
  preferIoUring = false;
#endif
  return detail::makeBatchManagerFor<SyncIoPolicy>(ringSize);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
