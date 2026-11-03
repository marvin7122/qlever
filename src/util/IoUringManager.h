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
#include "util/IoUringInterface.h"
#include "util/IoUringManagerConcepts.h"
#include "util/Log.h"
#include "util/PageCacheFirstPolicy.h"

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

class IoUringPolicy {
 public:
  using BatchHandle = uint64_t;

 private:
  std::unique_ptr<IoUringInterface> ring_;
  unsigned ringSize_;

  // Total number of outstanding reads: reads that are prepared (SQE filled in,
  // not yet submitted), in flight (submitted to the kernel, not yet
  // completed), or completed but not yet reaped via a completion queue entry
  // (CQE). `addBatch` keeps it at most `ringSize_`, so that a free SQE is
  // always available and the completion queue (twice as large as the
  // submission queue) cannot overflow. Invariant: it equals both the sum of
  // the counts in `numOutstandingReadRequestsPerBatch_` and the size of
  // `outstandingReadsByRequestId_`.
  size_t numOutstandingReadRequests_ = 0;

  // The outstanding reads that the kernel has taken (in flight, or completed
  // but not reaped). Only these produce a CQE, so a wave never waits for more
  // of them. The others are queued: prepared but not yet submitted. Outside of
  // `addBatch`, no read is queued.
  size_t numberOfReadsInKernel_ = 0;
  size_t numberOfQueuedReads() const {
    return numOutstandingReadRequests_ - numberOfReadsInKernel_;
  }

  // Set when `io_uring_submit` failed for good. The reads that were queued
  // then are cancelled (failed in the bookkeeping) but stay in the submission
  // queue, so the ring must never submit again: `addBatch` throws from then on.
  std::optional<std::string> submitFailure_;

  // The same outstanding reads as `numOutstandingReadRequests_`, but broken
  // down per batch: maps a batch handle to the number of its reads that have
  // not yet been reaped. An entry is removed as soon as the last read of its
  // batch is reaped: usually in the `wait()` of that batch, but also in
  // `addBatch` or in the `wait()` of another batch, which reap every ready CQE.
  ad_utility::HashMap<BatchHandle, size_t> numOutstandingReadRequestsPerBatch_;

  // Batches with a failed, short, or cancelled read, mapped to the message of
  // their first failure. `addBatch` and the `wait()` of another batch also
  // reap CQEs, so a failure is kept here until the `wait()` of its own batch
  // throws it (and erases it). A batch may still have outstanding reads while
  // its failure is recorded here; the entry of a batch that is never waited for
  // stays until the policy is destroyed.
  ad_utility::HashMap<BatchHandle, std::string> failedBatches_;

  // Per-read metadata needed when a completion is reaped: which batch the read
  // belongs to, and how many bytes it was supposed to read (so that reading
  // fewer bytes than expected can be detected). See
  // `outstandingReadsByRequestId_`.
  struct OutstandingRead {
    BatchHandle batchHandle;
    size_t expectedNumBytes;
  };

  // Monotonically increasing counter that mints a unique request id for each
  // individual read. The id is stored in the SQE's `user_data` and recovered
  // from the matching CQE to look up the read's `OutstandingRead` metadata.
  uint64_t nextRequestIdToAssign_ = 0;

  // Maps a read's request id to its metadata. An entry is inserted when the
  // read is prepared in `addBatch` and erased when its completion is reaped.
  ad_utility::HashMap<uint64_t, OutstandingRead> outstandingReadsByRequestId_;

  // The size of a wave: when the ring is full or `wait()` blocks, wait for up
  // to this many completions, then reap all ready ones in one pass (see
  // `reapWave`). This amortizes `io_uring_enter` and the CQ-head update over
  // the wave.
  static constexpr unsigned REAP_WAVE = 8;

  // The number of CQEs that `reapWave` takes out of the completion queue per
  // `io_uring_peek_batch_cqe` and releases with one `io_uring_cq_advance`; a
  // wave with more ready CQEs is reaped in several chunks.
  static constexpr size_t REAP_CHUNK = 64;

  // How often `submitQueuedReads` retries a submit that took no SQE (`EAGAIN`,
  // `EBUSY` or 0) before it gives up and cancels the queued reads.
  static constexpr size_t MAX_SUBMIT_RETRIES = 100;

  // Record a read of batch `handle` and prepare its SQE (not yet submitted).
  void recordAndPrepareRead(int fd, char* targetBuffer, size_t numBytesToRead,
                            uint64_t fileOffset, BatchHandle handle);

  // Submit every queued read. Retries an interrupted submit, and a submit that
  // took no SQE (after reaping a completion, or backing off); if that does not
  // help, or for any other error, cancels the queued reads (see
  // `cancelQueuedReads`). Afterwards no read is queued. Throws only what
  // `reapWave` throws.
  void submitQueuedReads();

  // Fail every queued read with `errorMessage` in the bookkeeping, and refuse
  // every later batch, because the cancelled SQEs stay in the submission queue.
  void cancelQueuedReads(std::string errorMessage);

  // Submit or cancel the queued reads, then reap every outstanding read,
  // without throwing. Called before an exception leaves `addBatch` or `wait()`
  // and in the destructor, because the callers free the target buffers then.
  void reapAllOutstandingReads() noexcept;

  // Block until a wave of `min(REAP_WAVE, numReadsToComplete)` CQEs is ready,
  // then reap every ready CQE. `numReadsToComplete` is the number of reads the
  // caller waits for (all reads in the kernel in `addBatch`, the remaining
  // reads of the batch in `wait()`). It must be > 0 and at most
  // `numberOfReadsInKernel_`, so that the wave never waits for more
  // completions than will arrive. Reaps at least one CQE, but may reap fewer
  // than the wave if a signal interrupts the wait, so the callers loop until
  // their condition holds. Terminates the process if waiting fails, because
  // the reads in the kernel could then still write into buffers that the
  // callers free. If `processCqe` throws for a CQE, every other ready CQE is
  // still applied to the bookkeeping, and the first exception is rethrown
  // afterwards.
  void reapWave(size_t numReadsToComplete);

  // Apply one completion to the bookkeeping of the outstanding reads. Always
  // updates the counts, also for a failed read, and records a failed or short
  // read in `failedBatches_`. A negative `numBytesRead` is `-errno`;
  // `requestId` must be the id of a read in the kernel.
  void processCqe(int numBytesRead, uint64_t requestId);

  // Remove one read of batch `handle` from the outstanding counts, and the
  // batch once its last read is removed.
  void retireRead(BatchHandle handle);

 public:
  IoUringPolicy(const IoUringPolicy&) = delete;
  IoUringPolicy& operator=(const IoUringPolicy&) = delete;

  // `ringSize` must be > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringPolicy(unsigned ringSize);
  // Run on `ring` instead of a liburing ring (for tests). `ring` must have
  // room for at least `ringSize` SQEs and `2 * ringSize` CQEs.
  IoUringPolicy(unsigned ringSize, std::unique_ptr<IoUringInterface> ring);
  ~IoUringPolicy();

  // Enqueue a batch of read requests and submit them to the kernel. Blocks the
  // calling thread only when the submission queue is full, in order to reap
  // completion queue entries and free slots in the submission queue. Read `i`
  // reads `numBytesToRead[i]` bytes from file descriptor `fd`, starting at
  // offset `offsets[i]` (from the start of the file), into the buffer starting
  // at `buffers[i]`. The reads are tracked under `handle`, which can be passed
  // to `wait()` to block until this batch has completed. `handle` must not be
  // outstanding or have a failure that was not yet thrown. If `addBatch`
  // throws, all outstanding reads (of every batch) are reaped first, so the
  // kernel no longer writes into the buffers. Throws at once if an earlier
  // submit failed for good (see `cancelQueuedReads`).
  void addBatch(int fd, ql::span<const size_t> numBytesToRead,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle);

  // Block until every read in the batch that is represented by the `handle` has
  // completed. (The `handle` was submitted along the read requests using
  // `addBatch`.) Throws if a read of this batch failed or was short, or could
  // not be submitted, after all of its reads are reaped; the failure is
  // reported only once, so a later `wait()` for the same handle returns
  // normally. If anything else throws, all outstanding reads (of every batch)
  // are reaped before the exception leaves `wait()`. A `wait()` for a handle
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
