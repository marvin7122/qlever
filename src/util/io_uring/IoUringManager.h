// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IO_URING_IOURINGMANAGER_H
#define QLEVER_SRC_UTIL_IO_URING_IOURINGMANAGER_H

#include <gtest/gtest_prod.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Log.h"
#include "util/NoCopyNoMove.h"

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

#include "backports/span.h"
#include "util/io_uring/IoUringManagerConcepts.h"
#include "util/io_uring/PageCacheFirstPolicy.h"

namespace ad_utility {

// Abstract base of `BatchManager` so a pool can hold managers of different
// `ReadPolicy`s behind a unified interface and pick the backend at runtime.
class BatchManagerBase {
 public:
  // The id of a batch, unique per manager; the `ReadPolicy` tags the reads of
  // the batch with it.
  using BatchId = uint64_t;
  // What `addBatch` returns and `wait` takes: the id of the batch, and whether
  // any of its reads was still in flight when `addBatch` returned. If none was
  // (all reads were done synchronously or served from the page cache), `wait`
  // returns at once without involving the `ReadPolicy`.
  struct BatchHandle {
    BatchId id_ = 0;
    bool hasReadsInFlight_ = true;
  };
  virtual ~BatchManagerBase() = default;

  [[nodiscard]] virtual BatchHandle addBatch(int fd,
                                             ql::span<const size_t> numBytes,
                                             ql::span<const uint64_t> offsets,
                                             ql::span<char*> buffers) = 0;

  virtual void wait(BatchHandle handle) = 0;
};

// `BatchManager` owns the batch bookkeeping (minting a `BatchHandle` per batch,
// validating the input spans) and delegates the reads from the underlying
// Vocabulary to the `Policy`, which must satisfy `ReadPolicyConcept` (see
// `util/io_uring/IoUringManagerConcepts.h`).
template <typename ReadPolicy>
class BatchManager final : public BatchManagerBase, public NoCopyNoMove {
  static_assert(
      ReadPolicyConcept<ReadPolicy>,
      "BatchManager's ReadPolicy must satisfy the ReadPolicyConcept concept.");
  // The policy is only valid if it tags reads with the base's batch ids.
  static_assert(
      std::is_same_v<typename ReadPolicy::BatchId, BatchManagerBase::BatchId>,
      "ReadPolicy::BatchId must match BatchManagerBase::BatchId.");

 public:
  using BatchHandle = typename BatchManagerBase::BatchHandle;

  // `ringSize` and `policyArgs` are passed on to the constructor of the
  // `ReadPolicy` (for example the `PageCacheRead` of a `PageCacheFirstPolicy`).
  template <typename... PolicyArgs>
  explicit BatchManager(unsigned ringSize = 256, PolicyArgs&&... policyArgs)
      : policy_(ringSize, std::forward<PolicyArgs>(policyArgs)...) {}

  [[nodiscard]] BatchHandle addBatch(int fd, ql::span<const size_t> numBytes,
                                     ql::span<const uint64_t> offsets,
                                     ql::span<char*> buffers) override {
    validateSameLength(numBytes, offsets, buffers);

    const BatchId id = nextBatchId_++;

    // Delegate the I/O work to the policy.
    const bool hasReadsInFlight =
        policy_.addBatch(fd, numBytes, offsets, buffers, id);

    return BatchHandle{id, hasReadsInFlight};
  }

  // Block until every read in `handle` has completed. Nothing to do if no read
  // of the batch was in flight when `addBatch` returned.
  void wait(BatchHandle handle) override {
    if (handle.hasReadsInFlight_) {
      policy_.wait(handle.id_);
    }
  }

 private:
  [[no_unique_address]] ReadPolicy policy_;
  BatchId nextBatchId_ = 0;

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
struct SyncIoPolicy : public NoCopyNoMove {
  using BatchId = uint64_t;

  // `ringSize` is ignored; it exists only so the policy is constructible the
  // same way as `IoUringPolicy`.
  //
  // NOTE: GCC rejects `[[maybe_unused]]` on a defaulted parameter; cast to
  // void.
  explicit SyncIoPolicy(unsigned ringSize = 256) { (void)ringSize; }

  // Immediately execute a batch of reads synchronously. This blocks the calling
  // thread. Read `i` reads `numBytesToReadPerRequest[i]` bytes from file
  // descriptor `fd`, starting at offset `fileOffsetPerRequest[i]` (from the
  // start of the file), into the buffer starting at
  // `targetBufferPerRequest[i]`. `handle` is unused (the batch completes before
  // `addBatch` returns), so it always returns `false` (no read in flight).
  bool addBatch(int fd, ql::span<const size_t> numBytesToReadPerRequest,
                ql::span<const uint64_t> fileOffsetPerRequest,
                ql::span<char*> targetBufferPerRequest, BatchId handle) const;

  void wait(BatchId) const {
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

class IoUringPolicy : public NoCopyNoMove {
 public:
  using BatchId = uint64_t;

 private:
  io_uring ring_{};
  unsigned ringSize_;

  // Total number of reads that occupy a ring slot but have not yet been reaped
  // via a completion queue entry (CQE), i.e. that are prepared or submitted but
  // not yet completed. Used to detect whether the ring is full.
  size_t numInFlightReadRequests_ = 0;

  // The same in-flight reads as `numInFlight_`, but broken down per batch:
  // maps a batch handle to the number of its reads that have not yet completed
  // (are "in flight"). An entry for a batch (identified by `BatchId`) is
  // removed once `wait()` has observed all of its reads complete.
  ad_utility::HashMap<BatchId, size_t> numInFlightReadRequestsPerBatch_;

  // Per-read metadata needed when a completion is reaped: which batch the read
  // belongs to, and how many bytes it was supposed to read (so that reading
  // fewer bytes than expected can be detected). See
  // `inFlightReadsByRequestId_`.
  struct InFlightRead {
    BatchId batchHandle;
    size_t expectedNumBytes;
  };

  // Monotonically increasing counter that mints a unique request id for each
  // individual read. The id is stored in the SQE's `user_data` and recovered
  // from the matching CQE to look up the read's `InFlightRead` metadata.
  uint64_t nextRequestIdToAssign_ = 0;

  // Maps a read's request id to its metadata. An entry is inserted when the
  // read is prepared in `addBatch` and erased when its completion is reaped in
  // `drainOneCqe`.
  ad_utility::HashMap<uint64_t, InFlightRead> inFlightReadsByRequestId_;

  // Wait for one CQE and update the in-flight bookkeeping.
  void drainOneCqe();

 public:
  // `ringSize` must be > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringPolicy(unsigned ringSize);
  ~IoUringPolicy();

  // Enqueue a batch of read requests and submit them to the kernel. Blocks the
  // calling thread only when the submission queue is full, in order to drain
  // completion queue entries and free slots in the submission queue. Read `i`
  // reads `numBytesToRead[i]` bytes from file descriptor `fd`, starting at
  // offset `offsets[i]` (from the start of the file), into the buffer starting
  // at `buffers[i]`. The reads are tracked under `handle`, which can be passed
  // to `wait()` to block until this batch has completed. Return `false` for an
  // empty batch (nothing to wait for) and `true` otherwise.
  bool addBatch(int fd, ql::span<const size_t> numBytesToRead,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchId handle);

  // Block until every read in the batch that is represented by the `handle` has
  // completed. (The `handle` was submitted along the read requests using
  // `addBatch`.) Throws on any I/O error.
  void wait(BatchId handle);
};

using BatchIoManager = BatchManager<IoUringPolicy>;
#else
using BatchIoManager = BatchManager<SyncIoPolicy>;
#endif

// The number of entries of the io_uring ring of a batch manager that
// `makeBatchManager` builds, unless the caller asks for another one.
inline constexpr unsigned defaultBatchManagerRingSize = 256;

namespace detail {
// Build a `BatchManager` for `Policy`, decorated with a `PageCacheFirstPolicy`
// that is constructed with `pageCacheRead` and `isFastPathEnabled`. Where
// `preadv2(RWF_NOWAIT)` is not compiled in or not supported, the decorator
// passes every batch on unchanged.
template <typename Policy>
std::unique_ptr<BatchManagerBase> makeBatchManagerFor(
    unsigned ringSize, PageCacheRead pageCacheRead,
    std::function<bool()> isFastPathEnabled) {
  static_assert(ReadPolicyConcept<Policy>,
                "makeBatchManagerFor's Policy must satisfy the "
                "ReadPolicyConcept.");
  return std::make_unique<BatchManager<PageCacheFirstPolicy<Policy>>>(
      ringSize, std::move(pageCacheRead), std::move(isFastPathEnabled));
}
}  // namespace detail

// Build a batch manager. When io_uring is compiled in and the runtime flag
// `preferIoUring` is set, try to build a manager with an `IoUringPolicy`. If
// its setup syscall fails at runtime clear `preferIoUring` and fall back to a
// `SyncIoPolicy`. Passing the flag by reference makes this probe-once: after
// the first failure, every subsequent call goes straight to the sync manager,
// so we don't repeat a failing syscall. The manager serves page-cache hits
// first (`PageCacheFirstPolicy`) for every batch for which `isFastPathEnabled`
// returns `true`, with `pageCacheRead`, which only tests replace.
inline std::unique_ptr<BatchManagerBase> makeBatchManager(
    bool& preferIoUring, unsigned ringSize = defaultBatchManagerRingSize,
    PageCacheRead pageCacheRead = &systemPageCacheRead,
    std::function<bool()> isFastPathEnabled = [] { return true; }) {
#ifdef QLEVER_HAS_IO_URING
  if (preferIoUring) {
    try {
      return detail::makeBatchManagerFor<IoUringPolicy>(ringSize, pageCacheRead,
                                                        isFastPathEnabled);
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
  return detail::makeBatchManagerFor<SyncIoPolicy>(
      ringSize, std::move(pageCacheRead), std::move(isFastPathEnabled));
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IO_URING_IOURINGMANAGER_H
