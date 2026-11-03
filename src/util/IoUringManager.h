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
#include <variant>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
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

// The reads of one batch, as the argument of `ReadPolicy::readBatch`: read `i`
// reads `numberOfBytes()[i]` bytes, starting at byte `offsets()[i]` of the
// file, into `buffers()[i]`. A type of its own, so that the three spans have
// the same length by construction: the constructor is the one place that
// checks it, and a policy cannot be handed spans of different lengths. Views
// only: the spans and the buffers they point to must outlive the `readBatch`
// call.
class BatchOfReads {
 public:
  // Throw if the three spans differ in length.
  BatchOfReads(ql::span<const size_t> numberOfBytes,
               ql::span<const uint64_t> offsets, ql::span<char*> buffers)
      : numberOfBytes_{numberOfBytes}, offsets_{offsets}, buffers_{buffers} {
    AD_CONTRACT_CHECK(offsets_.size() == numberOfBytes_.size() &&
                          buffers_.size() == numberOfBytes_.size(),
                      "The three spans of a batch of reads describe the same "
                      "reads, so they must have the same length.");
  }

  [[nodiscard]] size_t size() const { return numberOfBytes_.size(); }
  [[nodiscard]] ql::span<const size_t> numberOfBytes() const {
    return numberOfBytes_;
  }
  [[nodiscard]] ql::span<const uint64_t> offsets() const { return offsets_; }
  [[nodiscard]] ql::span<char*> buffers() const { return buffers_; }

 private:
  ql::span<const size_t> numberOfBytes_;
  ql::span<const uint64_t> offsets_;
  ql::span<char*> buffers_;
};

template <typename T>
CPP_requires(ReadPolicy_,
             requires(T& policy, int fd, const BatchOfReads& reads)(
                 concepts::constructible_from<T, unsigned>,
                 // Must provide `readBatch` with the following parameters.
                 policy.readBatch(fd, reads)));

// The pluggable I/O backend of `BatchManager`: it specifies how the reads in a
// batch are carried out. See `IoUringPolicy` (via io_uring) and `SyncIoPolicy`
// (blocking `pread` fallback) below. Must additionally be constructible from a
// ring size (`unsigned`). `readBatch(fd, reads)` reads the `reads` from `fd`
// and returns, or throws, only once no read of the batch can write into its
// buffer any more; it reports a failed or short read by throwing.
template <typename T>
CPP_concept ReadPolicyConcept = concepts::constructible_from<T, unsigned> &&
                                CPP_requires_ref(ReadPolicy_, T);

// Abstract base of `BatchManager` so a pool can hold managers of different
// `ReadPolicy`s behind a unified interface and pick the backend at runtime.
class BatchManagerBase {
 public:
  virtual ~BatchManagerBase() = default;

  // Read a batch: read `i` reads `numberOfBytes[i]` bytes from `fd`, starting
  // at byte `offsets[i]` of the file, into `buffers[i]`. Return (or throw) only
  // once no read of the batch can write into its buffer any more, so the
  // caller may free the buffers right away. Throw if the three spans differ in
  // length or a read fails or is short.
  virtual void readBatch(int fd, ql::span<const size_t> numberOfBytes,
                         ql::span<const uint64_t> offsets,
                         ql::span<char*> buffers) = 0;
};

// `BatchManager` packs the input spans into a `BatchOfReads` (which checks
// their lengths) and delegates the reads from the underlying Vocabulary to the
// `Policy`, which must satisfy the `ReadPolicy` concept above.
template <typename ReadPolicy>
class BatchManager final : public BatchManagerBase {
  static_assert(
      ReadPolicyConcept<ReadPolicy>,
      "BatchManager's ReadPolicy must satisfy the ReadPolicyConcept concept.");

 public:
  explicit BatchManager(unsigned ringSize = 256) : policy_(ringSize) {}

  BatchManager(const BatchManager&) = delete;
  BatchManager& operator=(const BatchManager&) = delete;

  void readBatch(int fd, ql::span<const size_t> numberOfBytes,
                 ql::span<const uint64_t> offsets,
                 ql::span<char*> buffers) override {
    // Delegate the I/O work to the policy.
    policy_.readBatch(fd, BatchOfReads{numberOfBytes, offsets, buffers});
  }

 private:
  [[no_unique_address]] ReadPolicy policy_;
};

// Fallback implementation for the `IoUringPolicy` below. Schedules pread calls
// in a synchronous (blocking) manner. Single-threaded use only.
struct SyncIoPolicy {
  // The ring size is ignored; the parameter exists only so the policy is
  // constructible the same way as `IoUringPolicy`.
  explicit SyncIoPolicy(unsigned = 256) {}

  ~SyncIoPolicy() = default;
  SyncIoPolicy(const SyncIoPolicy&) = delete;
  SyncIoPolicy& operator=(const SyncIoPolicy&) = delete;

  // Immediately execute the `reads` from file descriptor `fd` synchronously.
  // This blocks the calling thread.
  void readBatch(int fd, const BatchOfReads& reads) const;

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
  virtual size_t numberOfQueued() const noexcept = 0;

  // Return the number of SQEs, and of CQEs, that the ring has room for.
  virtual size_t sqCapacity() const noexcept = 0;
  virtual size_t cqCapacity() const noexcept = 0;

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

namespace detail {
// What probing the kernel of a new ring for `IORING_OP_READ` and
// `IORING_OP_NOP` found. `IORING_OP_READ` and the probe exist since Linux 5.6
// (`IORING_OP_NOP` since 5.1); on 5.1 to 5.5 a ring can be set up, but every
// read would complete with `EINVAL`.
enum class OpcodeSupport {
  Supported,
  // The probe itself failed: before Linux 5.6, if `io_uring_register` is
  // blocked, or if memory is short.
  ProbeFailed,
  // The kernel answered the probe, but lacks one of the two operations.
  OpcodeMissing,
};

// A function that probes the kernel of `ring`. `probeOpcodes` asks the kernel;
// tests pass a probe that fails, which a kernel that supports both operations
// cannot produce.
using OpcodeProbe = OpcodeSupport (*)(io_uring* ring) noexcept;
OpcodeSupport probeOpcodes(io_uring* ring) noexcept;
}  // namespace detail

// Return an `IoUringInterface` backed by liburing with (at least) `ringSize`
// SQEs; liburing rounds the size up to a power of two. Throw if the ring
// cannot be set up, for example because `io_uring_setup` is blocked by
// seccomp, or if `probe` does not confirm that the kernel supports
// `IORING_OP_READ` and `IORING_OP_NOP`, so that `ad_utility::makeBatchManager`
// falls back to synchronous reads.
std::unique_ptr<IoUringInterface> makeLiburingRing(
    unsigned ringSize, detail::OpcodeProbe probe = &detail::probeOpcodes);

// A `ReadPolicy` that reads batches with io_uring, one batch at a time:
// `readBatch` returns (or throws) only once the kernel no longer holds any read
// of the batch, so the caller may free the target buffers right away.
// Single-threaded use only. See https://github.com/axboe/liburing.
//
// Between two `readBatch` calls the ring holds no read, only no-ops of reads
// that were cancelled (they refer to no buffer). Errors are handled only once
// the kernel holds no read of the batch:
// - A failed or short read throws at the end of `readBatch`.
// - If `io_uring_submit` keeps failing with `EAGAIN` or `ENOMEM` (the kernel is
//   out of memory for requests), or keeps taking no SQE, the queued reads are
//   rewritten into no-ops, the reads that the kernel took are reaped, and the
//   batch is read again with `pread`.
// - Any other submit error means that the ring is unusable. The queued reads
//   are rewritten into no-ops, the reads that the kernel took are reaped, the
//   ring is torn down (without waiting for the no-ops in it, which write
//   nothing), and this batch and every later one are read with `pread`.
// - A failed wait for completions terminates the process: the kernel may
//   still write into the buffers of the reads it holds, and there is no way to
//   learn when it is done.
//
// Neither copyable nor movable: the policy owns the ring, whose kernel-side
// state cannot be duplicated, and it lives inside its `BatchManager`.
class IoUringPolicy : public NoCopyNoMove {
 public:
  // How often in a row an `io_uring_submit` that fails with `EAGAIN` or
  // `ENOMEM` (or takes no SQE) is retried, after a back-off, before the queued
  // reads are cancelled.
  static constexpr unsigned MAX_SUBMIT_RETRIES = 16;

  // The size of a wave: when no completion is ready while `readBatch` finds the
  // ring full, or reaps the last reads of the batch, wait for up to this many
  // completions at once, then reap all ready ones (see `reapWave`). This
  // amortizes the `io_uring_enter` call of the wait over the wave. The value is
  // not tuned.
  static constexpr unsigned REAP_WAVE = 8;

  // The largest read, in bytes: a completion reports the number of bytes read
  // as an `int`. Linux reads at most about 2 GiB per request (`MAX_RW_COUNT`),
  // so a read close to this limit is reported as short.
  static constexpr size_t MAX_NUMBER_OF_BYTES_PER_READ =
      static_cast<size_t>(std::numeric_limits<int>::max());

  // The request id of a no-op (the no-ops of cancelled reads keep this id). A
  // read's request id is its index in the batch, so no read gets this id. It
  // is not `uint64_t` max: that is liburing's `LIBURING_UDATA_TIMEOUT`, whose
  // completions liburing consumes itself on kernels before 5.11, so the no-op
  // would never be reaped.
  static constexpr uint64_t NO_OP_REQUEST_ID =
      std::numeric_limits<uint64_t>::max() - 1;

 private:
  // The ring, or `nullptr` once a submit error has shown it to be unusable;
  // the policy then reads every batch with `pread`.
  std::unique_ptr<IoUringInterface> ring_;
  // The most requests (reads and no-ops) that the policy keeps in the ring.
  const size_t ringSize_;

  // The reads of the current batch that occupy a ring slot but have not yet
  // been reaped via a completion queue entry (CQE), i.e. that are prepared or
  // submitted but not yet completed. Zero between two batches.
  size_t numberOfOutstandingReads_ = 0;
  // Of those, the reads that are prepared but not yet taken by the kernel.
  size_t numberOfQueuedReads_ = 0;

  // No-ops of cancelled reads (of this or an earlier batch) that are queued or
  // in the kernel. They occupy ring slots until their CQE is reaped. The
  // queued no-ops precede every queued read, because a batch whose reads are
  // cancelled queues no more.
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

  // Why the queued reads of a batch were cancelled.
  struct SubmitFailure {
    std::string message;
    // Whether the error shows the ring to be unusable (and not just out of
    // memory for requests).
    bool ringIsBroken;
  };

  // The batch that `readBatch` is reading. A read's request id is its index.
  struct Batch {
    BatchOfReads reads_;
    // The reads that were prepared so far: those with the indices
    // `[0, numberOfPreparedReads_)`.
    size_t numberOfPreparedReads_ = 0;
    // The first failed read of the batch.
    std::optional<ReadFailure> firstFailure_ = std::nullopt;
    // Set if the queued reads of the batch were cancelled.
    std::optional<SubmitFailure> submitFailure_ = std::nullopt;
  };

  // The requests (reads and no-ops) that the kernel has taken and whose
  // completion has not been reaped yet.
  size_t numberOfRequestsInKernel() const noexcept {
    return numberOfOutstandingReads_ - numberOfQueuedReads_ +
           numberOfNoOpsInKernel_;
  }

  // Whether every slot of the ring holds a request: a read or a no-op that is
  // queued, in the kernel, or completed but not yet reaped.
  bool ringIsFull() const noexcept {
    return numberOfOutstandingReads_ + numberOfNoOpsQueued_ +
               numberOfNoOpsInKernel_ >=
           ringSize_;
  }

  // The number of CQEs that `reapWave` takes out of the completion queue at
  // once. It reaps every ready CQE, which can be more than a wave (they may
  // have been ready before it waited), in several chunks then.
  static constexpr size_t REAP_CHUNK =
      IoUringInterface::MAX_COMPLETIONS_PER_POP;

  // Prepare and submit the reads of `batch` and reap their completions, until
  // the kernel holds none of them.
  void run(int fd, Batch& batch);

  // Reap every ready CQE and update the bookkeeping. If none is ready, first
  // block until at least `min(REAP_WAVE, numberOfRequestsToComplete)` CQEs are
  // ready. `numberOfRequestsToComplete` is the number of requests the caller
  // waits for; it must be > 0 and at most `numberOfRequestsInKernel()`, so
  // that the wave never waits for more completions than will arrive. Reaps at
  // least one CQE, but possibly fewer than the wave if a signal ends the wait
  // early, so the callers loop until their condition holds. A failed or short
  // read is recorded in `batch`, not thrown, so that the other reads of the
  // batch are reaped first. Retries an interrupted wait and terminates if
  // waiting fails otherwise.
  void reapWave(Batch& batch, size_t numberOfRequestsToComplete);

  // Apply one reaped completion to the bookkeeping (see `reapWave`).
  void processCompletion(Batch& batch,
                         const IoUringInterface::Completion& completion);

  // Hand the queued SQEs to the kernel. Retries `EINTR` at once, and retries
  // `EAGAIN` and `ENOMEM` (or a submit that takes no SQE) up to
  // `MAX_SUBMIT_RETRIES` times in a row after a back-off. If the submit fails
  // for good, cancels the queued reads and returns why.
  std::optional<SubmitFailure> submitQueued();

  // The kernel took `numberOfTakenSqes` SQEs: first the queued no-ops, then
  // reads.
  void onSqesTaken(size_t numberOfTakenSqes);

  // Rewrite the queued reads of `batch` into no-ops and forget them.
  void cancelQueuedReads(const Batch& batch);

  // Tear down the unusable ring, so that every later batch is read with
  // `pread`, and log `reason` once.
  void retireRing(const std::string& reason);

 public:
  // Set up a liburing ring with (at least) `ringSize` SQEs. `ringSize` must be
  // > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringPolicy(unsigned ringSize);

  // Run on `ring` instead of a liburing ring, for tests. The policy keeps at
  // most `ring->sqCapacity()` requests in the ring. Throws if `ring` is null,
  // has no SQE, or has fewer CQEs than SQEs.
  explicit IoUringPolicy(std::unique_ptr<IoUringInterface> ring);

  // Read the `reads` from `fd`. Returns once every read has completed. Throws
  // if a read failed or was short, but only once the kernel holds none of the
  // batch's reads. Each read must be at most `MAX_NUMBER_OF_BYTES_PER_READ`
  // bytes long.
  void readBatch(int fd, const BatchOfReads& reads);
};

using BatchIoManager = BatchManager<IoUringPolicy>;
#else
using BatchIoManager = BatchManager<SyncIoPolicy>;
#endif

// Build a batch manager. When io_uring is compiled in and the runtime flag
// `preferIoUring` is set, try to build an `IoUringPolicy` manager. If its ring
// cannot be set up at runtime, or the kernel does not confirm the operations
// that the ring needs (see `makeLiburingRing`), clear `preferIoUring` and fall
// back to a `SyncIoPolicy` manager. Passing the flag by reference makes this
// probe-once: after the first failure, every subsequent call goes straight to
// the sync manager, so we don't repeat a failing setup.
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
