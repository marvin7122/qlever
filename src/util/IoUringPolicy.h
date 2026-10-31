// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IOURINGPOLICY_H
#define QLEVER_SRC_UTIL_IOURINGPOLICY_H

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

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "backports/span.h"
#include "util/BatchHandle.h"
#include "util/RingSize.h"

namespace ad_utility {

#ifdef QLEVER_HAS_IO_URING

// The operations of an io_uring instance that `IoUringPolicy` uses. An
// interface so that unit tests can run the policy against a simulated ring
// (`test/util/FakeIoUring.h`) that takes only some of the submitted requests,
// fails submits and completes requests in any order.
class IoUringInterface {
 public:
  // A reaped completion: the request id of its SQE and its result (`res`).
  struct Completion {
    uint64_t requestId;
    int result;
  };

  virtual ~IoUringInterface() = default;

  // Fill a free SQE with a read of `numBytes` bytes at `offset` of `fd` into
  // `buffer`, tagged with `requestId`. Return false if no SQE is free.
  virtual bool prepareRead(int fd, char* buffer, size_t numBytes,
                           uint64_t offset, uint64_t requestId) = 0;

  // The number of prepared SQEs that the kernel has not taken yet.
  virtual size_t numQueued() const = 0;

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

// An `IoUringInterface` backed by liburing. Throws if the ring cannot be set
// up, for example because `io_uring_setup` is blocked by seccomp.
std::unique_ptr<IoUringInterface> makeLiburingRing(RingSize ringSize);

// Reads batches with io_uring, one batch at a time: `addBatch` returns (or
// throws) only once the kernel no longer holds any read of the batch, so the
// caller may free the target buffers right away. `wait()` has nothing left to
// do. Single-threaded use only. See https://github.com/axboe/liburing.
//
// Each read of a batch goes through the states of `ReadState`:
//
//   NotQueued --prepare--> Queued --kernel takes it--> InKernel --CQE--> Done
//                            |
//                            +--- submit fails for good --> Done (no-op)
//
// Between two `addBatch` calls the ring holds no read, only no-ops of reads
// that were cancelled (they refer to no buffer). Errors are reported only
// after the batch has left the ring: a failed or short read throws at the end
// of `addBatch`, and so does an `io_uring_submit` that keeps failing with
// `EAGAIN` (the kernel is out of memory for requests). A failure that leaves
// the kernel holding reads, so that throwing would let the caller free buffers
// the kernel still writes into, terminates the process instead: any other
// submit error, and a failed wait for completions.
class IoUringPolicy {
 public:
  using BatchHandle = ::ad_utility::BatchHandle;

  // Not copyable or movable: the policy owns the ring, whose kernel-side
  // state cannot be duplicated, and no caller needs a move (managers live in
  // `std::unique_ptr`s).
  IoUringPolicy(const IoUringPolicy&) = delete;
  IoUringPolicy& operator=(const IoUringPolicy&) = delete;
  IoUringPolicy(IoUringPolicy&&) = delete;
  IoUringPolicy& operator=(IoUringPolicy&&) = delete;

  // `ringSize` must be > 0 (power of 2 preferred; liburing rounds up).
  explicit IoUringPolicy(RingSize ringSize);

  // Run on `ring` instead of a liburing ring. `ring` must have room for at
  // least `ringSize` SQEs and `2 * ringSize` CQEs. For tests.
  IoUringPolicy(RingSize ringSize, std::unique_ptr<IoUringInterface> ring);

  // Read the batch: read `i` reads `numberOfBytesToRead[i]` bytes from `fd` at
  // offset `offsets[i]` into `buffers[i]`; the three spans must have the same
  // length. `handle` is unused. Returns once every read has completed. Throws
  // if a read failed or was short, or if its reads could not be submitted, but
  // only once the kernel holds none of its reads.
  void addBatch(int fd, ql::span<const size_t> numberOfBytesToRead,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle);

  // Nothing to do: `addBatch` has already completed the batch.
  void wait([[maybe_unused]] BatchHandle handle) {}

 private:
  enum class ReadState : uint8_t { NotQueued, Queued, InKernel, Done };

  // The batch that `addBatch` is reading. Reads are queued in index order, and
  // the kernel takes queued SQEs in order, so the queued reads are always
  // `[numTaken, numQueued)`; the reads before `numTaken` are `InKernel` or
  // `Done`.
  struct Batch {
    ql::span<const size_t> numBytes;
    std::vector<ReadState> states;
    size_t numQueued = 0;
    size_t numTaken = 0;
    size_t numDone = 0;
    std::optional<std::string> error;

    // The reads that the kernel took and whose completion has not been reaped
    // yet (they may still write into their buffers).
    size_t numReadsInKernel() const { return numTaken - numDone; }
    // The ring slots that the batch's reads occupy (queued, in the kernel,
    // or completed but not reaped).
    size_t numSlots() const { return numQueued - numDone; }
  };

  // The request id of a no-op. A read's request id is its index in the batch,
  // so no read gets this id. It is not `uint64_t` max: that is liburing's
  // `LIBURING_UDATA_TIMEOUT`, whose completions liburing consumes itself on
  // kernels without `IORING_FEAT_EXT_ARG` (before 5.11), so the no-op would
  // never be reaped.
  static constexpr uint64_t NO_OP_REQUEST_ID =
      std::numeric_limits<uint64_t>::max() - 1;

 public:
  // The request id that `IoUringPolicy` gives the no-ops of cancelled reads.
  static constexpr uint64_t noOpRequestId() { return NO_OP_REQUEST_ID; }

 private:
  // The size of a wave: wait for up to this many completions at once (fewer if
  // fewer requests are in the kernel), then reap all ready ones in one pass.
  // This amortizes `io_uring_enter` and the CQ-head update over the wave.
  static constexpr unsigned REAP_WAVE_SIZE = 8;

  // How often in a row an `io_uring_submit` that fails with `EAGAIN` is
  // retried before the queued reads are cancelled.
  static constexpr unsigned MAX_SUBMIT_RETRIES = 16;

  std::unique_ptr<IoUringInterface> ring_;
  RingSize ringSize_;

  // No-ops of cancelled reads (of this or an earlier batch) that are queued or
  // in the kernel. They occupy ring slots until their CQE is reaped.
  size_t numNoOpsQueued_ = 0;
  size_t numNoOpsInKernel_ = 0;

  // Fill the free ring slots with reads, submit them and reap waves of
  // completions until the batch is done or has failed and left the ring.
  void run(int fd, ql::span<const uint64_t> offsets, ql::span<char*> buffers,
           Batch& batch);

  // Submit the queued SQEs, retrying `EINTR`, and `EAGAIN` up to
  // `MAX_SUBMIT_RETRIES` times. If `EAGAIN` persists, cancel the queued reads.
  // Terminates on any other error.
  void submit(Batch& batch);

  // The kernel took `numTaken` SQEs: first the queued no-ops, then reads.
  void onTaken(Batch& batch, size_t numTaken);

  // Rewrite the queued reads into no-ops and fail the batch with
  // `errorMessage`.
  void cancelQueuedReads(Batch& batch, std::string errorMessage);

  // Block until at least `minComplete` requests have completed, then reap all
  // ready completions. Terminates if waiting fails.
  void reap(Batch& batch, unsigned minComplete);

  // Check that the ring's queue holds exactly what `batch` and the no-op
  // counts say.
  void checkQueue(const Batch& batch) const;
};

using BatchIoManager = BatchManager<IoUringPolicy>;

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGPOLICY_H
