// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_IO_URING_FAKEIOURING_H
#define QLEVER_TEST_UTIL_IO_URING_FAKEIOURING_H

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <optional>
#include <random>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"
#include "util/io_uring/IoUringInterface.h"

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
  size_t numberOfEarlyWaits = 0;
  size_t numberOfCancelledSqes = 0;
  // Calls of `backOff` while requests were in the simulated kernel, so that
  // the policy could have reaped one instead.
  size_t numberOfBackOffsWithRequestsInKernel = 0;
  // The target buffers of the reads that failed or were short.
  std::unordered_set<const char*> faultyBuffers;
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
// interrupt waits or end them early, and make reads short or fail. It checks
// the two preconditions whose violation makes a real ring misbehave silently:
// a wait for more completions than requests in the kernel (blocks forever),
// and more requests in the kernel and completed than the CQ has room for
// (completions are dropped).
class FakeIoUring final : public IoUringInterface,
                          public ad_utility::NoCopyNoMove {
 public:
  // How many of the requests in the kernel a submit completes at once.
  enum class CompletionOnSubmit { None, Random, All };

  // Probabilities of the faults, per call.
  struct Faults {
    double partialSubmit = 0;
    double submitInterrupted = 0;
    double submitOutOfMemory = 0;
    double waitInterrupted = 0;
    // A wait returns 0 as soon as one completion is ready, like a wait that a
    // signal ends while a completion is ready.
    double waitReturnsEarly = 0;
    double shortRead = 0;
    double readError = 0;
    // If set, every submit fails with this `errno`, after
    // `numberOfSubmitsBeforeBreak` successful ones.
    std::optional<int> brokenSubmitErrno;
    size_t numberOfSubmitsBeforeBreak = 0;
    // If set, every wait fails with this `errno`.
    std::optional<int> brokenWaitErrno;
    CompletionOnSubmit completionOnSubmit = CompletionOnSubmit::Random;
  };

  // A ring with room for `sqCapacity` SQEs (by default `ringSize`) and
  // `2 * ringSize` CQEs.
  FakeIoUring(std::string file, size_t ringSize, Faults faults,
              FakeIoUringObservations& observations, uint32_t seed = 42,
              std::optional<size_t> sqCapacity = std::nullopt)
      : file_{std::move(file)},
        sqCapacity_{sqCapacity.value_or(ringSize)},
        cqCapacity_{2 * ringSize},
        faults_{std::move(faults)},
        observations_{observations},
        random_{seed} {}

  ~FakeIoUring() override {
    observations_.tornDown = true;
    observations_.numberOfReadsInKernelAtTeardown = numberOfReadsInKernel();
  }

  // Change the faults, e.g. to break the ring after some batches.
  void setFaults(Faults faults) { faults_ = std::move(faults); }

  // Post a completion that no prepared read belongs to, as a broken kernel or
  // a bug in the bookkeeping would.
  void injectCompletion(Completion completion) {
    completions_.push_back(PendingCompletion{completion, false});
  }

  // The SQEs that are prepared but not taken by the kernel.
  size_t numberOfQueued() const { return queue_.size(); }
  // The prepared reads (not no-ops) that the kernel has not taken.
  size_t numberOfQueuedReads() const {
    return static_cast<size_t>(ql::ranges::count_if(
        queue_, [](const Request& request) { return !request.isNoOp; }));
  }
  // The reads (not no-ops) in the kernel or completed but not reaped.
  size_t numberOfReadsInKernel() const {
    auto isRead = [](const auto& requestOrCompletion) {
      return !requestOrCompletion.isNoOp;
    };
    return static_cast<size_t>(ql::ranges::count_if(kernel_, isRead) +
                               ql::ranges::count_if(completions_, isRead));
  }

  bool prepareRead([[maybe_unused]] int fd, char* buffer, size_t numberOfBytes,
                   uint64_t offset, uint64_t requestId) override {
    // Like a real SQ, a slot is free again once the kernel took its SQE.
    if (queue_.size() >= sqCapacity_) {
      return false;
    }
    queue_.push_back(Request{false, buffer, numberOfBytes, offset, requestId});
    return true;
  }

  int submit() override {
    if (faults_.brokenSubmitErrno.has_value()) {
      if (faults_.numberOfSubmitsBeforeBreak == 0) {
        ++observations_.numberOfFailedSubmits;
        return -faults_.brokenSubmitErrno.value();
      }
      --faults_.numberOfSubmitsBeforeBreak;
    }
    // Alternate between the two `errno`s of a kernel that is out of memory.
    if (happens(faults_.submitOutOfMemory)) {
      ++observations_.numberOfFailedSubmits;
      return observations_.numberOfFailedSubmits % 2 == 0 ? -ENOMEM : -EAGAIN;
    }
    if (happens(faults_.submitInterrupted)) {
      ++observations_.numberOfFailedSubmits;
      return -EINTR;
    }
    size_t numberOfTaken = queue_.size();
    if (numberOfTaken > 0 && happens(faults_.partialSubmit)) {
      numberOfTaken = uniform(0, numberOfTaken - 1);
      ++observations_.numberOfPartialSubmits;
    }
    for (size_t i = 0; i < numberOfTaken; ++i) {
      kernel_.push_back(queue_.front());
      queue_.pop_front();
    }
    AD_CORRECTNESS_CHECK(kernel_.size() + completions_.size() <= cqCapacity_,
                         "A real completion queue would overflow.");
    switch (faults_.completionOnSubmit) {
      case CompletionOnSubmit::None:
        break;
      case CompletionOnSubmit::Random:
        completeRandomRequests(uniform(0, kernel_.size()));
        break;
      case CompletionOnSubmit::All:
        completeRandomRequests(kernel_.size());
        break;
    }
    return static_cast<int>(numberOfTaken);
  }

  int waitForCompletions(unsigned minComplete) override {
    ++observations_.numberOfWaits;
    if (faults_.brokenWaitErrno.has_value()) {
      return -faults_.brokenWaitErrno.value();
    }
    // Like the kernel, return `-EINTR` only while no completion is ready.
    if (completions_.empty() && happens(faults_.waitInterrupted)) {
      ++observations_.numberOfInterruptedWaits;
      return -EINTR;
    }
    // Like `io_uring_wait_cqes`, count the completions that are already ready.
    AD_CORRECTNESS_CHECK(minComplete <= kernel_.size() + completions_.size(),
                         "A real ring would block forever on this wait.");
    if (completions_.size() < minComplete &&
        happens(faults_.waitReturnsEarly)) {
      ++observations_.numberOfEarlyWaits;
      if (completions_.empty()) {
        completeRandomRequests(1);
      }
      return 0;
    }
    while (completions_.size() < minComplete) {
      completeRandomRequests(1);
    }
    return 0;
  }

  size_t popCompletions(ql::span<Completion> completions) override {
    const size_t numberOfPopped = std::min(
        {completions.size(), completions_.size(), MAX_COMPLETIONS_PER_POP});
    for (size_t i = 0; i < numberOfPopped; ++i) {
      completions[i] = completions_.front().completion;
      completions_.pop_front();
    }
    if (numberOfPopped > 0) {
      ++observations_.numberOfChunks;
    }
    return numberOfPopped;
  }

  size_t cancelQueued(uint64_t noOpRequestId) override {
    for (Request& request : queue_) {
      request = Request{true, nullptr, 0, 0, noOpRequestId};
    }
    observations_.numberOfCancelledSqes += queue_.size();
    return queue_.size();
  }

  void backOff() override {
    if (!kernel_.empty() || !completions_.empty()) {
      ++observations_.numberOfBackOffsWithRequestsInKernel;
    }
  }

 private:
  struct Request {
    bool isNoOp;
    char* buffer;
    size_t numberOfBytes;
    uint64_t offset;
    uint64_t requestId;
  };

  struct PendingCompletion {
    Completion completion;
    bool isNoOp;
  };

  bool happens(double probability) {
    return probability > 0 && std::bernoulli_distribution{probability}(random_);
  }
  size_t uniform(size_t min, size_t max) {
    return std::uniform_int_distribution<size_t>{min, max}(random_);
  }

  // Complete `numberOfRequests` random requests of the kernel: write a read's
  // bytes into its buffer and post its completion.
  void completeRandomRequests(size_t numberOfRequests) {
    numberOfRequests = std::min(numberOfRequests, kernel_.size());
    for (size_t completed = 0; completed < numberOfRequests; ++completed) {
      const size_t i = uniform(0, kernel_.size() - 1);
      const Request request = kernel_[i];
      kernel_.erase(kernel_.begin() + static_cast<ptrdiff_t>(i));
      completions_.push_back(PendingCompletion{
          Completion{request.requestId, perform(request)}, request.isNoOp});
    }
  }

  // Perform `request` and return its result (`res`).
  int perform(const Request& request) {
    if (request.isNoOp) {
      return 0;
    }
    if (happens(faults_.readError)) {
      observations_.faultyBuffers.insert(request.buffer);
      return -EIO;
    }
    size_t numberOfBytes =
        std::min(request.numberOfBytes,
                 file_.size() - std::min<size_t>(file_.size(), request.offset));
    if (numberOfBytes > 0 && happens(faults_.shortRead)) {
      numberOfBytes = uniform(0, numberOfBytes - 1);
    }
    if (numberOfBytes != request.numberOfBytes) {
      observations_.faultyBuffers.insert(request.buffer);
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
  std::deque<PendingCompletion> completions_;
};

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_IO_URING_FAKEIOURING_H
