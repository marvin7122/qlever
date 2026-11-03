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
#include <unordered_map>
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

namespace ad_utility {

template <typename T>
CPP_requires(ReadPolicy_,
             requires(T& policy, int fd, ql::span<const size_t> numBytes,
                      ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                      typename T::BatchHandle handle)(
                 concepts::constructible_from<T, unsigned>,
                 // Must provide `addBatch` with the following parameters.
                 policy.addBatch(fd, numBytes, offsets, buffers, handle),
                 // Must provide a `wait` method with the following interface.
                 policy.wait(handle)));

// The pluggable I/O backend of `BatchManager`: it specifies how the reads in a
// batch are carried out. See `IoUringPolicy` (asynchronous, via io_uring) and
// `SyncIoPolicy` (blocking `pread` fallback) below. Must additionally be
// constructible from a ringsize (`unsigned`). Also require that `T` must expose
// an unsigned integral `BatchHandle` type.
template <typename T>
CPP_concept ReadPolicyConcept =
    concepts::constructible_from<T, unsigned> &&
    concepts::unsigned_integral<typename T::BatchHandle> &&
    CPP_requires_ref(ReadPolicy_, T);

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

#ifdef QLEVER_HAS_IO_URING

// The operations of an io_uring instance that `IoUringPolicy` uses. An
// interface so that unit tests can run the policy against a simulated ring
// (`test/util/FakeIoUring.h`) that takes only some of the submitted requests,
// fails submits and completes requests in any order.
class IoUringInterface {
 public:
  // A reaped completion: the request id of its SQE and its result (the
  // CQE's `res`): for a read, the number of bytes read on success, or
  // `-errno` on failure.
  struct Completion {
    uint64_t requestId;
    int result;
  };

  // Virtual, because `IoUringPolicy` owns its ring as
  // `std::unique_ptr<IoUringInterface>`.
  virtual ~IoUringInterface() = default;

  // Fill a free SQE with a read of `numberOfBytes` bytes at `offset` of `fd`
  // into `buffer`, tagged with `requestId`. Return false if no SQE is free. An
  // implementation must have a free SQE whenever fewer requests than
  // `sqCapacity()` are queued or in the kernel: `IoUringPolicy` never exceeds
  // that bound and treats a `false` as a broken invariant.
  virtual bool prepareRead(int fd, char* buffer, size_t numberOfBytes,
                           uint64_t offset, uint64_t requestId) = 0;

  // The number of prepared SQEs that the kernel has not taken yet.
  virtual size_t numberOfQueued() const = 0;

  // Return the number of SQEs, and of CQEs, that the ring has room for.
  virtual size_t sqCapacity() const = 0;
  virtual size_t cqCapacity() const = 0;

  // `io_uring_submit`: hand the queued SQEs to the kernel, which takes them in
  // the order in which they were prepared. Return the number of SQEs taken, or
  // `-errno`.
  virtual int submit() = 0;

  // Block until at least `minComplete` completions are ready. Return 0 or
  // `-errno`. Must neither hand queued SQEs to the kernel nor consume
  // completions: `IoUringPolicy` learns which SQEs the kernel took only from
  // `submit`, and counts every completion that `popCompletions` returns.
  virtual int waitForCompletions(unsigned minComplete) = 0;

  // Move up to `completions.size()` ready completions into `completions` and
  // release their CQ slots. Return how many were moved.
  virtual size_t popCompletions(ql::span<Completion> completions) = 0;

  // Rewrite every queued SQE into a no-op tagged with `noOpRequestId`, so that
  // it no longer refers to a buffer. Return the request ids of the rewritten
  // SQEs that were not no-ops already, in queue order.
  virtual std::vector<uint64_t> cancelQueued(uint64_t noOpRequestId) = 0;

  // Wait a moment before a submit that failed with `EAGAIN` is retried
  // without any completion to wait for.
  virtual void backOff() = 0;
};

// Return an `IoUringInterface` backed by liburing with (at least) `ringSize`
// SQEs; liburing rounds the size up to a power of two. Throw if the ring
// cannot be set up, for example because `io_uring_setup` is blocked by
// seccomp, so that `makeBatchManager` falls back to synchronous reads.
std::unique_ptr<IoUringInterface> makeLiburingRing(unsigned ringSize);

// Persistent io_uring manager that accepts multiple named batches of indices to
// be read from the underlying storage medium, submits all SQEs in `addBatch`
// (blocking if the ring is full), and lets the caller block on a specific batch
// via `wait()`. Single-threaded use only. See https://github.com/axboe/liburing
// for more details.
//
// Neither copyable nor movable: the policy owns the ring, whose kernel-side
// state cannot be duplicated, and it lives inside its `BatchManager`.
class IoUringPolicy : public NoCopyNoMove {
 public:
  using BatchHandle = uint64_t;

 private:
  std::unique_ptr<IoUringInterface> ring_;
  // The most reads that the policy keeps in the ring.
  size_t ringSize_;

  // Total number of reads that occupy a ring slot but have not yet been reaped
  // via a completion queue entry (CQE), i.e. that are prepared or submitted but
  // not yet completed. Used to detect whether the ring is full.
  size_t numInFlightReadRequests_ = 0;

  // The same in-flight reads as `numInFlight_`, but broken down per batch:
  // maps a batch handle to the number of its reads that have not yet completed
  // (are "in flight"). An entry for a batch (identified by `BatchHandle`) is
  // removed once `wait()` has observed all of its reads complete.
  ad_utility::HashMap<BatchHandle, size_t> numInFlightReadRequestsPerBatch_;

  // Per-read metadata needed when a completion is reaped: which batch the read
  // belongs to, and how many bytes it was supposed to read (so that reading
  // fewer bytes than expected can be detected). See
  // `inFlightReadsByRequestId_`.
  struct InFlightRead {
    BatchHandle batchHandle;
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
  // Set up a liburing ring with (at least) `ringSize` SQEs. `ringSize` must be
  // > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringPolicy(unsigned ringSize);

  // Run on `ring` instead of a liburing ring, for tests. The policy keeps at
  // most `ring->sqCapacity()` reads in the ring. Throws if `ring` is null, has
  // no SQE, or has fewer CQEs than SQEs.
  explicit IoUringPolicy(std::unique_ptr<IoUringInterface> ring);
  ~IoUringPolicy();

  // Enqueue a batch of read requests and submit them to the kernel. Blocks the
  // calling thread only when the submission queue is full, in order to drain
  // completion queue entries and free slots in the submission queue. Read `i`
  // reads `numBytesToRead[i]` bytes from file descriptor `fd`, starting at
  // offset `offsets[i]` (from the start of the file), into the buffer starting
  // at `buffers[i]`. The reads are tracked under `handle`, which can be passed
  // to `wait()` to block until this batch has completed.
  void addBatch(int fd, ql::span<const size_t> numBytesToRead,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle);

  // Block until every read in the batch that is represented by the `handle` has
  // completed. (The `handle` was submitted along the read requests using
  // `addBatch`.) Throws on any I/O error.
  void wait(BatchHandle handle);
};

using BatchIoManager = BatchManager<IoUringPolicy>;
#else
using BatchIoManager = BatchManager<SyncIoPolicy>;
#endif

// Build a batch manager. When io_uring is compiled in and the runtime flag
// `preferIoUring` is set, try to build an `IoUringManager`. If its setup
// syscall fails at runtime clear `preferIoUring` and fall back to a
// `SyncIoManager`. Passing the flag by reference makes this probe-once: after
// the first failure, every subsequent call goes straight to the sync manager,
// so we don't repeat a failing syscall.
inline std::unique_ptr<BatchManagerBase> makeBatchManager(
    bool& preferIoUring, unsigned ringSize = 256) {
#ifdef QLEVER_HAS_IO_URING
  if (preferIoUring) {
    try {
      return std::make_unique<BatchManager<IoUringPolicy>>(ringSize);
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
  return std::make_unique<BatchManager<SyncIoPolicy>>(ringSize);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
