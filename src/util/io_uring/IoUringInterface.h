// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IO_URING_IOURINGINTERFACE_H
#define QLEVER_SRC_UTIL_IO_URING_IOURINGINTERFACE_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include "backports/span.h"

namespace ad_utility {

// The io_uring calls that `IoUringPolicy` makes, behind an interface, so that
// tests can run the policy on a simulated ring (`test/util/io_uring/
// FakeIoUring.h`) and inject the failures that a real kernel produces only
// rarely (partial and failed submits, interrupted and failed waits, failed or
// short reads, completions in any order).
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

  // Wait a moment before a submit that failed with `EAGAIN` is retried while
  // nothing is in the kernel (so there is no completion to wait for instead).
  virtual void backOff() = 0;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IO_URING_IOURINGINTERFACE_H
