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

#include "backports/span.h"
#include "util/BatchHandle.h"
#include "util/HashMap.h"
#include "util/MonotonicCounter.h"
#include "util/OutstandingReadCounts.h"
#include "util/RingSize.h"

namespace ad_utility {

#ifdef QLEVER_HAS_IO_URING

// Persistent io_uring manager that accepts multiple named batches of indices to
// be read from the underlying storage medium, submits all SQEs in `addBatch`
// (blocking if the ring is full), and lets the caller block on a specific batch
// via `wait()`. Single-threaded use only. See https://github.com/axboe/liburing
// for more details.
class IoUringPolicy {
 public:
 private:
  io_uring ring_{};
  RingSize ringSize_;

  // How many reads are currently outstanding, in total and per batch.
  OutstandingReadCounts outstandingReadCounts_;

  // The total number of outstanding reads. `addBatch` keeps it at most
  // `ringSize_`, so that a free SQE is always available and the
  // completion queue (twice as large as the submission queue) cannot
  // overflow.
  size_t numberOfOutstandingReadRequests() const {
    return outstandingReadCounts_.total();
  }

  // Batches with a failed or short read, mapped to the message of their first
  // failure. `addBatch` and the `wait()` of another batch also reap CQEs, so a
  // failure is kept here until the `wait()` of its own batch throws it.
  ad_utility::HashMap<BatchHandle, const char*> failedBatches_;

  // Per-read metadata needed when a completion (CQE) is reaped from the
  // completion queue: which batch the read belongs to, and how many bytes it
  // was supposed to read (so that reading fewer bytes than expected can be
  // detected). See `outstandingReadsByRequestId_`.
  struct OutstandingRead {
    BatchHandle batchHandle;
    size_t expectedNumberOfReadBytes;
  };

  // Mints a unique request id for each individual read. The id is stored
  // in the SQE's `user_data` and recovered from the matching CQE to look
  // up the read's `OutstandingRead` metadata. A `MonotonicCounter`, so
  // the ids are increasing (and hence unique) by construction.
  MonotonicCounter nextRequestIdToAssign_;

  // Maps a read's request id to its metadata. An entry is inserted when the
  // read is prepared in `addBatch` and erased when its completion is reaped.
  ad_utility::HashMap<uint64_t, OutstandingRead> outstandingReadsByRequestId_;

  // The size of a wave: when the ring is full or `wait()` blocks, wait for up
  // to this many completions (fewer if fewer requests can complete), then reap
  // all ready completion queue entries from the completion queue in one pass.
  // This amortizes `io_uring_enter` and the completion-queue-head update over
  // the wave.
  static constexpr unsigned REAP_WAVE_SIZE = 8;

  // Block the application thread until at least `minComplete` CQEs are ready,
  // then reap every ready CQE.
  void drainAtLeast(unsigned minComplete);

  // Apply one completion to the bookkeeping of the outstanding reads. Always
  // updates the counts, also for a failed read, and records a failed or short
  // read in `failedBatches_`.
  void processCqe(int numberOfBytesRead, uint64_t requestId);

 public:
  // Not copyable or movable: this policy owns the io_uring ring and the
  // bookkeeping of its outstanding reads. A copy would double-own the
  // ring, and so would a merely defaulted move: it would copy `ring_`
  // member by member (the ring file descriptor and the pointers into
  // the memory-mapped submission and completion queues), leaving two
  // objects to tear the same ring down in `io_uring_queue_exit`. A
  // hand-written move could transfer the ring and the bookkeeping, and
  // the outstanding batches would survive it, but no caller needs one:
  // the policy is only ever held inside its `BatchManager`, which a
  // `std::unique_ptr` owns.
  IoUringPolicy(const IoUringPolicy&) = delete;
  IoUringPolicy& operator=(const IoUringPolicy&) = delete;
  IoUringPolicy(IoUringPolicy&&) = delete;
  IoUringPolicy& operator=(IoUringPolicy&&) = delete;

  // `ringSize` must be > 0 (power of 2 preferred; liburing rounds up).
  // `explicit`, so that a bare `ringSize` cannot silently convert to a
  // `IouringPolicy` (which sets up a ring) where a policy is expected.
  explicit IoUringPolicy(RingSize ringSize);

  // Not defaulted: it has to shut down the io_uring ring (see the `.cpp`).
  ~IoUringPolicy();

  // Enqueue a batch of read requests and submit them to the kernel. Blocks the
  // calling thread only when the submission queue is full, in order to drain
  // completion queue entries and thus free slots in the submission queue. Read
  // `i` reads `numberOfBytesToRead[i]` bytes from file descriptor `fd`,
  // starting at offset `offsets[i]` (from the start of the file), into the
  // buffer starting at `buffers[i]`. The reads are tracked under `handle`,
  // which can be passed to `wait()` to block until this batch has completed.
  void addBatch(int fd, ql::span<const size_t> numberOfBytesToRead,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle);

  // Block until every read in the batch that is represented by the `handle` has
  // completed. (The `handle` was submitted along the read requests using
  // `addBatch`.) Throw if a read of this batch failed or read less bytes than
  // expected. Throw only after all of the batch's reads have been reaped.
  //
  // The throw is deferred until then, even if a failure is already known:
  // once `wait` has thrown, the caller treats the batch as finished and may
  // reuse or release its buffers. A read that is not yet reaped may still
  // be in flight, so the kernel could still write into such a buffer.
  // Reaping every read of the batch first rules that out, and it also
  // leaves no completion of this batch behind in the bookkeeping.
  void wait(BatchHandle handle);
};

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGPOLICY_H
