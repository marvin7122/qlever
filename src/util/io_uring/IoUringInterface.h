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
#include <memory>

#include "backports/span.h"

namespace ad_utility {

// The io_uring calls that `IoUringPolicy` makes, behind an interface, so that
// tests can run the policy on a simulated ring: count its waits and its chunks
// of reaped completions, and inject the failures that a real kernel produces
// only rarely (partial submits, interrupted waits, failed or short reads).
class IoUringInterface {
 public:
  // A reaped completion: the request id of its SQE and its result (the CQE's
  // `res`): the number of bytes read on success, or `-errno` on failure.
  struct Completion {
    uint64_t requestId;
    int result;
  };

  virtual ~IoUringInterface() = default;

  // Fill a free SQE with a read of `numberOfBytes` bytes at `offset` of `fd`
  // into `buffer`, tagged with `requestId`. Return false if no SQE is free.
  // The kernel sees the SQE only at a later `submit`.
  virtual bool prepareRead(int fd, char* buffer, size_t numberOfBytes,
                           uint64_t offset, uint64_t requestId) = 0;

  // `io_uring_submit`: hand the prepared SQEs to the kernel, which takes them
  // in the order in which they were prepared, possibly not all of them. Return
  // the number of SQEs taken, or `-errno`.
  virtual int submit() = 0;

  // `io_uring_wait_cqes`: block until at least `minComplete` completions are
  // ready. Return 0 or `-errno`. Neither submits prepared SQEs nor consumes
  // completions.
  virtual int waitForCompletions(unsigned minComplete) = 0;

  // Move up to `completions.size()` ready completions into `completions` and
  // release their CQ slots. Return how many were moved; 0 if none is ready.
  virtual size_t popCompletions(ql::span<Completion> completions) = 0;

  // Wait a moment before a submit that took no SQE is retried while no read is
  // in the kernel (so there is no completion to wait for instead).
  virtual void backOff() = 0;
};

#ifdef QLEVER_HAS_IO_URING
// Return an `IoUringInterface` backed by liburing with (at least) `ringSize`
// SQEs. Throw if the ring cannot be set up, for example because
// `io_uring_setup` is blocked by seccomp.
std::unique_ptr<IoUringInterface> makeLiburingRing(unsigned ringSize);
#endif

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IO_URING_IOURINGINTERFACE_H
