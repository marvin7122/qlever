// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_FAKEIOURING_H
#define QLEVER_TEST_UTIL_FAKEIOURING_H

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <deque>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "util/Exception.h"
#include "util/IoUringInterface.h"

namespace ad_utility::testing {

// What a `FakeIoUring` saw. Owned by the test, so that it can be read also
// after the `IoUringPolicy` that owns the ring has destroyed it.
struct FakeIoUringObservations {
  // Calls of `waitForCompletions`, including interrupted and failed ones.
  size_t numberOfWaits = 0;
  // Calls of `popCompletions` that returned at least one completion.
  size_t numberOfChunks = 0;
  // Faults that were injected.
  size_t numberOfPartialSubmits = 0;
  size_t numberOfFailedSubmits = 0;
  size_t numberOfInterruptedWaits = 0;
  size_t numberOfFaultyReads = 0;
  // The reads that were still in the simulated kernel or completed but not
  // reaped when the ring was destroyed. A real kernel can still write into the
  // buffers of the former after `io_uring_queue_exit`.
  size_t numberOfReadsInKernelAtTeardown = 0;
  bool tornDown = false;
};

// A simulated io_uring for tests of `IoUringPolicy`. The "kernel" reads from
// `file` (the read's `fd` is ignored) and writes into the target buffer only
// when it completes a read, in random order. Driven by a seeded random
// generator, it can take only some of the prepared SQEs, fail submits,
// interrupt waits, and make reads short or fail. It checks the two
// preconditions whose violation makes a real ring misbehave silently: a wait
// for more completions than reads in the kernel (blocks forever), and more
// requests in the kernel and completed than the CQ has room for (completions
// are dropped).
class FakeIoUring final : public IoUringInterface {
 public:
  // Probabilities of the faults, per call.
  struct Faults {
    double partialSubmit = 0;
    double submitInterrupted = 0;
    double submitOutOfMemory = 0;
    double waitInterrupted = 0;
    double shortRead = 0;
    double readError = 0;
    // If nonzero, every submit (or wait) fails with this `errno`.
    int brokenSubmitErrno = 0;
    int brokenWaitErrno = 0;
    // Whether a submit may already complete some (a random number) or all of
    // the requests in the kernel, as for reads from the page cache. Otherwise,
    // a wait completes exactly as many requests as it waits for.
    bool completeOnSubmit = true;
    bool completeAllOnSubmit = false;
  };

  FakeIoUring(std::string file, size_t sqCapacity, Faults faults,
              FakeIoUringObservations& observations, uint32_t seed = 42)
      : file_{std::move(file)},
        sqCapacity_{sqCapacity},
        cqCapacity_{2 * sqCapacity},
        faults_{faults},
        observations_{observations},
        random_{seed} {}

  ~FakeIoUring() override {
    observations_.tornDown = true;
    observations_.numberOfReadsInKernelAtTeardown =
        kernel_.size() + completions_.size();
  }

  FakeIoUring(const FakeIoUring&) = delete;
  FakeIoUring& operator=(const FakeIoUring&) = delete;

  // Change the faults, e.g. to break the ring after some batches.
  void setFaults(Faults faults) { faults_ = faults; }

  // Post a completion that no prepared read belongs to, as a broken kernel or
  // a bug in the bookkeeping would.
  void injectCompletion(Completion completion) {
    completions_.push_back(completion);
  }

  // The reads that are prepared but not taken by the kernel, and those in the
  // kernel or completed but not reaped.
  size_t numberOfQueued() const { return queue_.size(); }
  size_t numberOfReadsInKernel() const {
    return kernel_.size() + completions_.size();
  }

  bool prepareRead([[maybe_unused]] int fd, char* buffer, size_t numberOfBytes,
                   uint64_t offset, uint64_t requestId) override {
    // Like a real SQ, a slot is free again once the kernel took its SQE.
    if (queue_.size() >= sqCapacity_) {
      return false;
    }
    queue_.push_back(Request{buffer, numberOfBytes, offset, requestId});
    return true;
  }

  int submit() override {
    if (faults_.brokenSubmitErrno != 0) {
      ++observations_.numberOfFailedSubmits;
      return -faults_.brokenSubmitErrno;
    }
    if (happens(faults_.submitOutOfMemory)) {
      ++observations_.numberOfFailedSubmits;
      return -EAGAIN;
    }
    if (happens(faults_.submitInterrupted)) {
      ++observations_.numberOfFailedSubmits;
      return -EINTR;
    }
    size_t numberTaken = queue_.size();
    if (numberTaken > 0 && happens(faults_.partialSubmit)) {
      numberTaken = uniform(0, numberTaken - 1);
      ++observations_.numberOfPartialSubmits;
    }
    for (size_t i = 0; i < numberTaken; ++i) {
      kernel_.push_back(queue_.front());
      queue_.pop_front();
    }
    AD_CORRECTNESS_CHECK(kernel_.size() + completions_.size() <= cqCapacity_,
                         "A real completion queue would overflow.");
    if (faults_.completeAllOnSubmit) {
      completeRandomRequests(kernel_.size());
    } else if (faults_.completeOnSubmit) {
      completeRandomRequests(uniform(0, kernel_.size()));
    }
    return static_cast<int>(numberTaken);
  }

  int waitForCompletions(unsigned minComplete) override {
    ++observations_.numberOfWaits;
    if (faults_.brokenWaitErrno != 0) {
      return -faults_.brokenWaitErrno;
    }
    // Like the kernel, return `-EINTR` only while no completion is ready.
    if (completions_.empty() && happens(faults_.waitInterrupted)) {
      ++observations_.numberOfInterruptedWaits;
      return -EINTR;
    }
    // Like `io_uring_wait_cqes`, count the completions that are already ready.
    AD_CORRECTNESS_CHECK(minComplete <= kernel_.size() + completions_.size(),
                         "A real ring would block forever on this wait.");
    while (completions_.size() < minComplete) {
      completeRandomRequests(1);
    }
    return 0;
  }

  size_t popCompletions(ql::span<Completion> completions) override {
    const size_t numberPopped =
        std::min(completions.size(), completions_.size());
    for (size_t i = 0; i < numberPopped; ++i) {
      completions[i] = completions_.front();
      completions_.pop_front();
    }
    if (numberPopped > 0) {
      ++observations_.numberOfChunks;
    }
    return numberPopped;
  }

  void backOff() override {}

 private:
  struct Request {
    char* buffer;
    size_t numberOfBytes;
    uint64_t offset;
    uint64_t requestId;
  };

  bool happens(double probability) {
    return probability > 0 && std::bernoulli_distribution{probability}(random_);
  }
  size_t uniform(size_t min, size_t max) {
    return std::uniform_int_distribution<size_t>{min, max}(random_);
  }

  // Complete `numberToComplete` random requests of the kernel: write a read's
  // bytes into its buffer and post its completion.
  void completeRandomRequests(size_t numberToComplete) {
    numberToComplete = std::min(numberToComplete, kernel_.size());
    for (size_t completed = 0; completed < numberToComplete; ++completed) {
      const size_t i = uniform(0, kernel_.size() - 1);
      const Request request = kernel_[i];
      kernel_.erase(kernel_.begin() + static_cast<ptrdiff_t>(i));
      completions_.push_back(Completion{request.requestId, perform(request)});
    }
  }

  // Perform `request` and return its result (`res`).
  int perform(const Request& request) {
    if (happens(faults_.readError)) {
      ++observations_.numberOfFaultyReads;
      return -EIO;
    }
    size_t numberOfBytes =
        std::min(request.numberOfBytes,
                 file_.size() - std::min<size_t>(file_.size(), request.offset));
    if (numberOfBytes > 0 && happens(faults_.shortRead)) {
      numberOfBytes = uniform(0, numberOfBytes - 1);
    }
    if (numberOfBytes != request.numberOfBytes) {
      ++observations_.numberOfFaultyReads;
    }
    if (numberOfBytes > 0) {
      std::memcpy(request.buffer, file_.data() + request.offset, numberOfBytes);
    }
    return static_cast<int>(numberOfBytes);
  }

  std::string file_;
  size_t sqCapacity_;
  size_t cqCapacity_;
  Faults faults_;
  FakeIoUringObservations& observations_;
  std::mt19937 random_;
  std::deque<Request> queue_;
  std::vector<Request> kernel_;
  std::deque<Completion> completions_;
};

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_FAKEIOURING_H
