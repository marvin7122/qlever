// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IO_URING_IOURINGPOLICY_H
#define QLEVER_SRC_UTIL_IO_URING_IOURINGPOLICY_H

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "backports/span.h"
#include "util/NoCopyNoMove.h"
#include "util/io_uring/BatchOfReads.h"

#ifdef QLEVER_HAS_IO_URING

// liburing's ring type. Only `IoUringPolicy.cpp` includes liburing, so that
// its macros reach no other translation unit; the test seam of the opcode
// probe below needs only the name.
struct io_uring;

namespace ad_utility {

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
// seccomp, or if the kernel does not confirm that it supports
// `IORING_OP_READ` and `IORING_OP_NOP` (see `detail::OpcodeSupport`), so that
// `makeBatchManager` falls back to synchronous reads.
std::unique_ptr<IoUringInterface> makeLiburingRing(unsigned ringSize);

namespace detail {
// What probing the kernel of a new ring for `IORING_OP_READ` and
// `IORING_OP_NOP` found. Both exist since Linux 5.6; on 5.1 to 5.5 a ring can
// be set up, but every read would complete with `EINVAL`.
enum class OpcodeSupport {
  Supported,
  // The probe itself failed: before Linux 5.6 (which also introduced the
  // probe), if `io_uring_register` is blocked, or if memory is short.
  ProbeFailed,
  // The kernel answered the probe, but lacks one of the two operations.
  OpcodeMissing,
};

// The test seam of the probe: `makeLiburingRing` calls the function that
// `opcodeProbe()` refers to (initially `probeOpcodes`, which asks the kernel),
// so that tests can make a probe fail on a kernel that supports both
// operations. Tests replace it by assigning through the returned reference.
using OpcodeProbe = OpcodeSupport (*)(::io_uring* ring) noexcept;
OpcodeSupport probeOpcodes(::io_uring* ring) noexcept;
OpcodeProbe& opcodeProbe() noexcept;
}  // namespace detail

// A `ReadPolicy` that reads batches with io_uring, one batch at a time:
// `readBatch` returns (or throws) only once the kernel no longer holds any read
// of the batch, so the caller may free the target buffers right away.
// Single-threaded use only. See https://github.com/axboe/liburing.
//
// Each read of a batch goes through the states of `ReadState`:
//
//   NotQueued --prepare--> Queued --kernel takes it--> InKernel --CQE--> Done
//                            |
//                            +--- submit fails for good --> Done (no-op)
//
// Between two `readBatch` calls the ring holds no read, only no-ops of reads
// that were cancelled (they refer to no buffer). Errors are handled only once
// the kernel holds no read of the batch:
// - A failed or short read throws at the end of `readBatch`.
// - An `io_uring_submit` that keeps failing with `EAGAIN` (the kernel is out of
//   memory for requests), or that keeps taking no SQE, cancels the queued
//   reads; the batch is then read again with `pread`.
// - Any other submit error means that the ring is unusable. If the kernel holds
//   no read of the batch, the queued reads are cancelled, the ring is torn down
//   (without waiting for the no-ops in it, which write nothing), and this batch
//   and every later one are read with `pread`. If the kernel does hold reads of
//   the batch, the caller cannot be allowed to free their buffers, so the
//   process terminates.
// - A failed wait for completions terminates the process.
//
// Neither copyable nor movable: the policy owns the ring, whose kernel-side
// state cannot be duplicated, and it lives inside its `BatchManager`.
class IoUringPolicy : public NoCopyNoMove {
 public:
  // The largest read, in bytes: a completion reports the number of bytes read
  // as an `int`. Linux reads at most about 2 GiB per request (`MAX_RW_COUNT`),
  // so a read close to this limit is reported as short.
  static constexpr size_t MAX_NUMBER_OF_BYTES_PER_READ =
      static_cast<size_t>(std::numeric_limits<int>::max());

  // Set up a liburing ring with (at least) `ringSize` SQEs.
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

  // The request id of a no-op (the no-ops of cancelled reads keep this id). A
  // read's request id is its index in the batch, so no read gets this id. It
  // is not `uint64_t` max: that is liburing's `LIBURING_UDATA_TIMEOUT`, whose
  // completions liburing consumes itself on kernels without
  // `IORING_FEAT_EXT_ARG` (before 5.11), so the no-op would never be reaped.
  static constexpr uint64_t NO_OP_REQUEST_ID =
      std::numeric_limits<uint64_t>::max() - 1;

 private:
  enum class ReadState : uint8_t { NotQueued, Queued, InKernel, Done };

  // The batch that `readBatch` is reading. Reads are queued in index order,
  // and the kernel takes queued SQEs in order, so the queued reads are always
  // `[numberOfTaken_, numberOfQueued_)`; the reads before `numberOfTaken_` are
  // `InKernel` or `Done`.
  struct Batch {
    int fd_;
    BatchOfReads reads_;
    std::vector<ReadState> states_;
    size_t numberOfQueued_ = 0;
    size_t numberOfTaken_ = 0;
    size_t numberOfDone_ = 0;
    // The first failed or short read.
    std::optional<std::string> readError_;
    // Why the queued reads were cancelled, if they were. The batch is then
    // read again with `pread`.
    std::optional<std::string> cancelReason_;
    // The submit error that showed the ring to be unusable, if one did.
    std::optional<std::string> ringError_;

    Batch(int fd, const BatchOfReads& reads)
        : fd_{fd}, reads_{reads}, states_(reads.size(), ReadState::NotQueued) {}

    // Whether no more reads may be queued: the batch throws anyway, or its
    // queued reads were cancelled.
    bool stoppedQueueing() const {
      return readError_.has_value() || cancelReason_.has_value();
    }
    // The reads that the kernel took and whose completion has not been reaped
    // yet (they may still write into their buffers).
    size_t numberOfReadsInKernel() const {
      return numberOfTaken_ - numberOfDone_;
    }
    // The ring slots that the batch's reads occupy (queued, in the kernel,
    // or completed but not reaped).
    size_t numberOfSlots() const { return numberOfQueued_ - numberOfDone_; }
  };

  // The size of a wave: wait for up to this many completions at once (fewer if
  // fewer requests are in the kernel), then reap all ready ones in one pass.
  // This amortizes `io_uring_enter` and the CQ-head update over the wave.
  static constexpr unsigned REAP_WAVE = 8;

  // How often in a row an `io_uring_submit` that fails with `EAGAIN` is
  // retried before the queued reads are cancelled.
  static constexpr unsigned MAX_SUBMIT_RETRIES = 16;

  // The ring, or `nullptr` once a submit error has shown it to be unusable;
  // the policy then reads every batch with `pread`.
  std::unique_ptr<IoUringInterface> ring_;
  // The most requests that the policy keeps in the ring.
  size_t ringSize_;

  // No-ops of cancelled reads (of this or an earlier batch) that are queued or
  // in the kernel. They occupy ring slots until their CQE is reaped.
  size_t numberOfNoOpsQueued_ = 0;
  size_t numberOfNoOpsInKernel_ = 0;

  // Fill the free ring slots with reads, submit them and reap waves of
  // completions until the batch is done, or has failed and left the ring, or
  // the ring broke while the kernel held no read of the batch.
  void run(Batch& batch);

  // Submit the queued SQEs. Retry `EINTR`; retry `EAGAIN`, and a submit that
  // takes no SQE, up to `MAX_SUBMIT_RETRIES` times in a row, then cancel the
  // queued reads. On any other error, cancel the queued reads and mark the ring
  // as broken if the kernel holds no read of the batch, and terminate
  // otherwise.
  void submit(Batch& batch);

  // The kernel took `numberOfTakenSqes` SQEs: first the queued no-ops, then
  // reads.
  void onTaken(Batch& batch, size_t numberOfTakenSqes);

  // Rewrite the queued reads into no-ops, for the reason `cancelReason`.
  void cancelQueuedReads(Batch& batch, std::string cancelReason);

  // Block until at least `minComplete` requests have completed, then reap all
  // ready completions. Terminates if waiting fails: the kernel may hold reads
  // of the batch, or no-ops whose wait would fail again in the next batch.
  void reap(Batch& batch, unsigned minComplete);

  // Check that the ring's queue holds exactly what `batch` and the no-op
  // counts say.
  void checkQueue(const Batch& batch) const;

  // Tear down the unusable ring, so that every later batch is read with
  // `pread`, and log `reason` once.
  void retireRing(const std::string& reason);
};

}  // namespace ad_utility

#endif  // QLEVER_HAS_IO_URING

#endif  // QLEVER_SRC_UTIL_IO_URING_IOURINGPOLICY_H
