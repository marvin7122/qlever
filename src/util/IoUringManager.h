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
#include <unordered_map>
#include <variant>
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

// The io_uring calls that `IoUringPolicy` makes, behind an interface, so that
// tests can run the policy on a simulated ring (`test/util/FakeIoUring.h`) and
// inject the failures that a real kernel produces only rarely (partial and
// failed submits, interrupted and failed waits, failed or short reads,
// completions in any order).
class IoUringInterface {
 public:
  // The most completions that `popCompletions` moves per call.
  static constexpr size_t MAX_COMPLETIONS_PER_POP = 64;

  // A reaped completion: the request id of its SQE and its result (the CQE's
  // `res`): the number of bytes read on success, or `-errno` on failure.
  struct Completion {
    uint64_t requestId;
    int result;
  };

  virtual ~IoUringInterface() = default;

  // Fill a free SQE with a read of `numberOfBytes` bytes at `offset` of `fd`
  // into `buffer`, tagged with `requestId`. Return false if no SQE is free.
  // The kernel sees the SQE only at a later `submit`. An implementation must
  // have a free SQE whenever fewer requests than `sqCapacity()` are queued or
  // in the kernel: `IoUringPolicy` never exceeds that bound and treats a
  // `false` as a broken invariant.
  virtual bool prepareRead(int fd, char* buffer, size_t numberOfBytes,
                           uint64_t offset, uint64_t requestId) = 0;

  // The number of prepared SQEs that the kernel has not taken yet.
  virtual size_t numberOfQueued() const = 0;

  // Return the number of SQEs, and of CQEs, that the ring has room for.
  virtual size_t sqCapacity() const = 0;
  virtual size_t cqCapacity() const = 0;

  // `io_uring_submit`: hand the prepared SQEs to the kernel, which takes them
  // in the order in which they were prepared, possibly not all of them. Return
  // the number of SQEs taken, or `-errno`.
  virtual int submit() = 0;

  // `io_uring_wait_cqes`: block until at least `minComplete` completions are
  // ready. Return 0 or `-errno`. Neither submits prepared SQEs nor consumes
  // completions.
  virtual int waitForCompletions(unsigned minComplete) = 0;

  // Move up to `completions.size()` (at most `MAX_COMPLETIONS_PER_POP`) ready
  // completions into `completions` and release their CQ slots. Return how many
  // were moved; 0 if none is ready.
  virtual size_t popCompletions(ql::span<Completion> completions) = 0;

  // Rewrite every prepared SQE that the kernel has not taken yet into a no-op
  // tagged with `noOpRequestId`, so that it no longer refers to a buffer. The
  // no-ops are submitted with the next `submit` and complete with result 0.
  // Return the request ids of the rewritten SQEs that were not no-ops already,
  // in queue order.
  virtual std::vector<uint64_t> cancelQueued(uint64_t noOpRequestId) = 0;

  // Wait a moment before a submit that failed transiently is retried.
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
// A batch's error is thrown only once the kernel holds none of the batch's
// reads, because the caller frees the target buffers when it unwinds:
// - A failed or short read is thrown by the `wait()` of its batch.
// - If `io_uring_submit` keeps failing with `EAGAIN` or `ENOMEM` (the kernel is
//   out of memory for requests), keeps taking no SQE, or fails with any other
//   error, `addBatch` rewrites the queued reads into no-ops, which refer to no
//   buffer, reaps the reads of the batch that the kernel holds, and throws.
// - A failed wait for completions terminates the process: the kernel may
//   still write into the buffers of the reads it holds, and there is no way to
//   learn when it is done.
//
// Neither copyable nor movable: the policy owns the ring, whose kernel-side
// state cannot be duplicated, and it lives inside its `BatchManager`.
class IoUringPolicy : public NoCopyNoMove {
 public:
  using BatchHandle = uint64_t;

  // The size of a wave: when the ring is full or `wait()` blocks and no
  // completion is ready, wait for up to this many completions at once, then
  // reap all ready ones (see `reapWave`). This amortizes the `io_uring_enter`
  // call of the wait over the wave. The value is not tuned.
  static constexpr unsigned REAP_WAVE = 8;

 private:
  // The request id of a no-op (the no-ops of cancelled reads keep this id). The
  // reads get their ids from a counter that starts at 0, so no read gets this
  // id. It is not `uint64_t` max: that is liburing's `LIBURING_UDATA_TIMEOUT`,
  // whose completions liburing consumes itself on kernels before 5.11, so the
  // no-op would never be reaped.
  static constexpr uint64_t NO_OP_REQUEST_ID =
      std::numeric_limits<uint64_t>::max() - 1;

  // How often in a row an `io_uring_submit` that fails with `EAGAIN` or
  // `ENOMEM` (or takes no SQE) is retried, after a back-off, before the queued
  // reads are cancelled.
  static constexpr unsigned MAX_SUBMIT_RETRIES = 16;

  std::unique_ptr<IoUringInterface> ring_;
  // The most requests (reads and no-ops) that the policy keeps in the ring.
  const size_t ringSize_;

  // Total number of reads that occupy a ring slot but have not yet been reaped
  // via a completion queue entry (CQE), i.e. that are prepared or submitted but
  // not yet completed. Used to detect whether the ring is full.
  size_t numberOfInFlightReads_ = 0;
  // Of those, the reads that are prepared but not yet taken by the kernel.
  size_t numberOfQueuedReads_ = 0;

  // No-ops of cancelled reads that are queued or in the kernel. They occupy
  // ring slots until their CQE is reaped. The queued no-ops precede every
  // queued read, because a batch whose reads are cancelled queues no more.
  size_t numberOfNoOpsQueued_ = 0;
  size_t numberOfNoOpsInKernel_ = 0;

  // A read that failed: the kernel reported an error (`res` is `-errno`)...
  struct ReadError {
    int errorNumber;
  };
  // ... or read fewer bytes than requested (a partial read, or 0 at the end of
  // the file).
  struct ShortRead {
    size_t numberOfBytesRead;
    size_t numberOfRequestedBytes;
  };
  using ReadFailure = std::variant<ReadError, ShortRead>;

  // The state of a batch whose reads are in flight, or that has a failed read
  // whose `wait()` has not yet thrown it.
  struct Batch {
    // The reads of this batch that have not yet completed (are "in flight").
    size_t numberOfInFlightReads;
    // The first failed read of the batch. Stored in place, so that recording a
    // failure does not allocate.
    std::optional<ReadFailure> firstFailure = std::nullopt;
  };

  // The same in-flight reads as `numberOfInFlightReads_`, but broken down per
  // batch. The entry of a batch is removed once all of its reads have
  // completed, unless one of them failed: then the `wait()` of the batch
  // removes it when it throws the failure.
  ad_utility::HashMap<BatchHandle, Batch> batches_;

  // Per-read metadata needed when a completion is reaped: which batch the read
  // belongs to, and how many bytes it was supposed to read (so that reading
  // fewer bytes than requested can be detected). See
  // `inFlightReadsByRequestId_`.
  struct InFlightRead {
    BatchHandle batchHandle;
    size_t numberOfRequestedBytes;
  };

  // Monotonically increasing counter that mints a unique request id for each
  // individual read. The id is stored in the SQE's `user_data` and recovered
  // from the matching CQE to look up the read's `InFlightRead` metadata.
  uint64_t nextRequestIdToAssign_ = 0;

  // Maps a read's request id to its metadata. An entry is inserted when the
  // read is prepared in `addBatch` and erased when its completion is reaped in
  // `processCompletion`, or when the read is cancelled.
  ad_utility::HashMap<uint64_t, InFlightRead> inFlightReadsByRequestId_;

  // The requests (reads and no-ops) that the kernel has taken and whose
  // completion has not been reaped yet.
  size_t numberOfRequestsInKernel() const {
    return numberOfInFlightReads_ - numberOfQueuedReads_ +
           numberOfNoOpsInKernel_;
  }

  // The number of CQEs that `reapWave` takes out of the completion queue at
  // once; a wave with more ready CQEs is reaped in several chunks.
  static constexpr size_t REAP_CHUNK =
      IoUringInterface::MAX_COMPLETIONS_PER_POP;

  // Reap every ready CQE and update the in-flight bookkeeping. If none is
  // ready, first block until a wave of `min(REAP_WAVE,
  // numberOfRequestsToComplete)` CQEs is ready. `numberOfRequestsToComplete` is
  // the number of requests the caller waits for; it must be > 0 and at most
  // `numberOfRequestsInKernel()`, so that the wave never waits for more
  // completions than will arrive. Reaps at least one CQE, but possibly fewer
  // than the wave if a signal ends the wait early, so the callers loop until
  // their condition holds. A failed or short read is recorded in its batch, not
  // thrown, so that the caller can reap the batch's other reads first. Retries
  // an interrupted wait and terminates if waiting fails otherwise.
  void reapWave(size_t numberOfRequestsToComplete);

  // Apply one reaped completion to the in-flight bookkeeping (see `reapWave`).
  void processCompletion(const IoUringInterface::Completion& completion);

  // Reap completions until no read of the batch `handle` is in flight.
  void drainBatch(BatchHandle handle);

  // Hand the queued SQEs to the kernel. Retries `EINTR` at once, and retries
  // `EAGAIN` and `ENOMEM` (or a submit that takes no SQE) up to
  // `MAX_SUBMIT_RETRIES` times in a row after a back-off. If the submit fails
  // for good, cancels the queued reads and returns the error.
  std::optional<std::string> submitQueued();

  // The kernel took `numberOfTakenSqes` SQEs: first the queued no-ops, then
  // reads.
  void onTaken(size_t numberOfTakenSqes);

  // Rewrite the queued reads into no-ops and forget them.
  void cancelQueuedReads();

 public:
  // Set up a liburing ring with (at least) `ringSize` SQEs. `ringSize` must be
  // > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringPolicy(unsigned ringSize);

  // Run on `ring` instead of a liburing ring, for tests. The policy keeps at
  // most `ring->sqCapacity()` requests in the ring. Throws if `ring` is null,
  // has no SQE, or has fewer CQEs than SQEs.
  explicit IoUringPolicy(std::unique_ptr<IoUringInterface> ring);
  ~IoUringPolicy();

  // Enqueue a batch of read requests and submit them to the kernel. Blocks the
  // calling thread only when the submission queue is full, in order to drain
  // completion queue entries and free slots in the submission queue. Read `i`
  // reads `numberOfBytesToReadPerRequest[i]` bytes from file descriptor `fd`,
  // starting at offset `fileOffsetPerRequest[i]` (from the start of the file),
  // into the buffer starting at `targetBufferPerRequest[i]`. The reads are
  // tracked under `handle`, which can be passed to `wait()` to block until this
  // batch has completed. Throws if the reads cannot be submitted (see above),
  // but only once the kernel holds none of the batch's reads.
  void addBatch(int fd, ql::span<const size_t> numberOfBytesToReadPerRequest,
                ql::span<const uint64_t> fileOffsetPerRequest,
                ql::span<char*> targetBufferPerRequest, BatchHandle handle);

  // Block until every read in the batch that is represented by the `handle` has
  // completed. (The `handle` was submitted along the read requests using
  // `addBatch`.) Then throw if one of these reads failed or read fewer bytes
  // than requested.
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
