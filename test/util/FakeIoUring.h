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
#include <iterator>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/IoUringManager.h"
#include "util/Views.h"

#ifdef QLEVER_HAS_IO_URING

namespace ad_utility::testing {

// A simulated io_uring for tests of `IoUringPolicy`. The "kernel" reads from
// `file` (the read's `fd` is ignored) and writes into the target buffer only
// when it completes a read. Driven by a seeded random generator, it takes only
// some of the queued SQEs, fails submits with `EINTR` and with bursts of
// `EAGAIN`, interrupts waits, completes requests in any order, and makes reads
// short or fail. It checks the contract of `IoUringInterface`: a free SQE for
// every prepared read, no wait for more completions than requests in the
// kernel, and (in `submit`) that the requests in the simulated kernel plus
// the unreaped completions never exceed the CQ capacity - the bound that keeps
// a real completion queue from overflowing.
class FakeIoUring final : public IoUringInterface {
 public:
  // The faults to inject: probabilities per call, and a script for the first
  // submits.
  struct Faults {
    double partialSubmit = 0;
    double submitInterrupted = 0;
    // The probability that a submit starts a burst of `EAGAIN`, the error with
    // which the kernel reports that it has no memory for the requests. A burst
    // makes 1 to `maxEagainBurst` submits in a row fail. A burst of more than
    // `BatchManagerParameters::maxSubmitRetries` failures makes the policy
    // cancel the queued reads; it rides out shorter ones.
    double submitOutOfMemory = 0;
    size_t maxEagainBurst = 1;
    double waitInterrupted = 0;
    // A wait returns 0 as soon as one completion is ready, like a wait that a
    // signal ends while a completion is ready.
    double waitReturnsEarly = 0;
    double shortRead = 0;
    double readError = 0;
    // The results of the first submits, one per call: a negative value fails
    // the submit with that `-errno`; a value `n >= 0` makes the kernel take
    // (at most) `n` of the queued SQEs. The probabilistic submit faults above
    // apply only once the script is used up.
    std::vector<int> scriptedSubmits;
    // If nonzero, every wait fails with this `errno`.
    int brokenWaitErrno = 0;
    // Whether a submit may already complete some of the requests it took.
    bool completeOnSubmit = true;
    // Whether a submit completes all requests in the kernel (implies
    // `completeOnSubmit`).
    bool completeAllOnSubmit = false;
  };

  //____________________________________________________________________________
  // A ring with room for `sqCapacity` SQEs and twice as many CQEs, like a
  // liburing ring.
  FakeIoUring(std::string file, size_t sqCapacity, Faults faults, uint32_t seed)
      : FakeIoUring(std::move(file), sqCapacity, faults, seed, 2 * sqCapacity) {
  }

  //____________________________________________________________________________
  // A ring with room for `sqCapacity` SQEs and `cqCapacity` CQEs. Like
  // `IoUringPolicy`, it needs at least as many CQEs as SQEs.
  FakeIoUring(std::string file, size_t sqCapacity, Faults faults, uint32_t seed,
              size_t cqCapacity)
      : file_{std::move(file)},
        sqCapacity_{sqCapacity},
        cqCapacity_{cqCapacity},
        faults_{faults},
        random_{seed} {
    AD_CONTRACT_CHECK(cqCapacity_ >= sqCapacity_,
                      "A ring needs at least as many CQEs as SQEs.");
    // `submit` draws the length of an `EAGAIN` burst from
    // [1, `maxEagainBurst`].
    AD_CONTRACT_CHECK(faults_.maxEagainBurst >= 1,
                      "An EAGAIN burst fails at least one submit.");
    for (const double probability :
         {faults_.partialSubmit, faults_.submitInterrupted,
          faults_.submitOutOfMemory, faults_.waitInterrupted, faults_.shortRead,
          faults_.readError}) {
      AD_CONTRACT_CHECK(probability >= 0 && probability <= 1,
                        "The probability of a fault must be in [0, 1].");
    }
    // The kernel never holds more requests than the completion queue has room
    // for (see `takeQueuedSqes`).
    kernel_.reserve(cqCapacity_);
  }

  //____________________________________________________________________________
  // Whether, since the last `resetObservations`, a read was made short or
  // failed.
  bool injectedReadFault() const noexcept { return injectedReadFault_; }

  // How often each fault was injected over the lifetime of the ring, so that
  // tests can check that their faults actually occur.
  struct FaultCounts {
    // Submits that took only some of the queued SQEs (possibly none).
    size_t partialSubmits = 0;
    // Submits that failed with `EINTR`.
    size_t interruptedSubmits = 0;
    // Submits that failed with `EAGAIN`, also within a burst.
    size_t outOfMemorySubmits = 0;
    // Waits that failed with `EINTR`.
    size_t interruptedWaits = 0;
    // Waits that returned 0 with fewer completions ready than asked for.
    size_t earlyWaits = 0;
    // Reads that failed or read fewer bytes than requested, also those that
    // are short because they end past the end of the file.
    size_t faultyReads = 0;
    // Reads that `cancelQueued` rewrote into no-ops.
    size_t cancelledReads = 0;
  };

  // How often the policy waited (also interrupted and failed waits), for how
  // many completions, and how many of its pops returned at least one
  // completion, over the lifetime of the ring.
  struct CallCounts {
    size_t waits = 0;
    std::vector<unsigned> waitSizes;
    size_t chunks = 0;
  };

  //____________________________________________________________________________
  // The calls counted so far (see `CallCounts`).
  const CallCounts& callCounts() const noexcept { return callCounts_; }

  //____________________________________________________________________________
  // The faults injected so far (see `FaultCounts`).
  const FaultCounts& faultCounts() const noexcept { return faultCounts_; }

  //____________________________________________________________________________
  // Forget the read faults injected so far (see `injectedReadFault`).
  void resetObservations() noexcept { injectedReadFault_ = false; }

  //____________________________________________________________________________
  // The reads (not no-ops) that are queued, in the kernel, or completed but
  // not reaped. Between two `readBatch` calls this must be zero.
  size_t numberOfReadsInRing() const noexcept {
    auto isRead = [](const auto& requestOrCompletion) {
      return !requestOrCompletion.isNoOp;
    };
    return static_cast<size_t>(
        std::count_if(queue_.begin(), queue_.end(), isRead) +
        std::count_if(kernel_.begin(), kernel_.end(), isRead) +
        std::count_if(completions_.begin(), completions_.end(), isRead));
  }

  // The calls of `IoUringInterface`, on the simulated ring.

  //____________________________________________________________________________
  // Queue a read of `numberOfBytes` bytes at `offset` into `buffer`, so that
  // the next `submit` hands it to the kernel. Return false if the SQ is full.
  bool prepareRead([[maybe_unused]] int fd, char* buffer, size_t numberOfBytes,
                   uint64_t offset, uint64_t requestId) override {
    if (queue_.size() >= sqCapacity_) {
      return false;
    }
    queue_.push_back(Request{false, buffer, numberOfBytes, offset, requestId});
    return true;
  }

  //____________________________________________________________________________
  // The number of SQEs that the kernel has not taken yet.
  size_t numberOfQueued() const noexcept override { return queue_.size(); }

  //____________________________________________________________________________
  // The room for SQEs and for CQEs of the simulated ring.
  size_t sqCapacity() const noexcept override { return sqCapacity_; }
  size_t cqCapacity() const noexcept override { return cqCapacity_; }

  //____________________________________________________________________________
  // The faults apply in this order: the script, as long as it lasts; then the
  // rest of a running `EAGAIN` burst, a new burst, `EINTR`, and a partial
  // submit. A submit that fails takes no SQE.
  int submit() override {
    const size_t submitIndex = numberOfSubmits_++;
    if (submitIndex < faults_.scriptedSubmits.size()) {
      return scriptedSubmit(faults_.scriptedSubmits[submitIndex]);
    }

    if (const std::optional<int> failure = injectedSubmitFailure();
        failure.has_value()) {
      return failure.value();
    }

    return takeQueuedSqes(numberOfSqesToTake());
  }

  //____________________________________________________________________________
  // Complete random requests of the kernel until `minComplete` completions are
  // ready, unless the wait is interrupted or broken. Return 0 or `-errno`.
  int waitForCompletions(unsigned minComplete) override {
    ++callCounts_.waits;
    callCounts_.waitSizes.push_back(minComplete);
    if (faults_.brokenWaitErrno != 0) {
      return -faults_.brokenWaitErrno;
    }
    if (happens(faults_.waitInterrupted)) {
      ++faultCounts_.interruptedWaits;
      return -EINTR;
    }
    // Like `io_uring_wait_cqes`, the wait counts the completions that are
    // already ready. A real wait for more completions than those plus the
    // requests in the kernel would block forever.
    AD_CORRECTNESS_CHECK(minComplete <= kernel_.size() + completions_.size(),
                         "A real ring would block forever on this wait.");
    // (Only draws from the generator if the fault is enabled, so that it does
    // not change the faults of the tests that do not use it.)
    if (faults_.waitReturnsEarly > 0 && completions_.size() < minComplete &&
        happens(faults_.waitReturnsEarly)) {
      ++faultCounts_.earlyWaits;
      if (completions_.empty()) {
        // The check above guarantees a request in the kernel here.
        AD_CORRECTNESS_CHECK(!kernel_.empty());
        completeRandomRequests(1);
      }
      return 0;
    }
    while (completions_.size() < minComplete) {
      completeRandomRequests(1);
    }
    return 0;
  }

  //____________________________________________________________________________
  // Move up to `completions.size()` ready completions, oldest first, into
  // `completions`. Return how many were moved.
  size_t popCompletions(ql::span<Completion> completions) override {
    const size_t numberOfPopped =
        std::min(completions.size(), completions_.size());
    ql::ranges::transform(completions_ | ql::views::take(numberOfPopped),
                          completions.begin(), &PendingCompletion::completion);
    completions_.erase(
        completions_.begin(),
        completions_.begin() + static_cast<ptrdiff_t>(numberOfPopped));
    if (numberOfPopped > 0) {
      ++callCounts_.chunks;
    }
    return numberOfPopped;
  }

  //____________________________________________________________________________
  // Rewrite the queued SQEs into no-ops tagged with `noOpRequestId`. Return the
  // request ids of the rewritten reads, in queue order.
  std::vector<uint64_t> cancelQueued(uint64_t noOpRequestId) override {
    std::vector<uint64_t> cancelledIds;
    cancelledIds.reserve(queue_.size());
    for (Request& request : queue_) {
      if (!request.isNoOp) {
        cancelledIds.push_back(request.requestId);
        ++faultCounts_.cancelledReads;
      }
      request = Request{true, nullptr, 0, 0, noOpRequestId};
    }
    return cancelledIds;
  }

  //____________________________________________________________________________
  // Back off without waiting: the simulated kernel needs no time to recover.
  void backOff() noexcept override {}

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

  //____________________________________________________________________________
  // A submit with the scripted result `scripted`: a negative value is returned
  // as the error, a value `n >= 0` takes (at most) `n` queued SQEs.
  int scriptedSubmit(int scripted) {
    if (scripted < 0) {
      return scripted;
    }
    return takeQueuedSqes(
        std::min(queue_.size(), static_cast<size_t>(scripted)));
  }

  //____________________________________________________________________________
  // The `-errno` with which a submit fails because of an injected fault
  // (`EAGAIN` of a burst, or `EINTR`), or `std::nullopt` if it does not fail.
  std::optional<int> injectedSubmitFailure() {
    if (eagainBurstLeft_ > 0) {
      --eagainBurstLeft_;
      ++faultCounts_.outOfMemorySubmits;
      return -EAGAIN;
    }
    if (happens(faults_.submitOutOfMemory)) {
      eagainBurstLeft_ = uniform(1, faults_.maxEagainBurst) - 1;
      ++faultCounts_.outOfMemorySubmits;
      return -EAGAIN;
    }
    if (happens(faults_.submitInterrupted)) {
      ++faultCounts_.interruptedSubmits;
      return -EINTR;
    }
    return std::nullopt;
  }

  //____________________________________________________________________________
  // The number of queued SQEs that a submit takes: all of them, or fewer for
  // a partial submit. A partial submit may take nothing, which `IoUringPolicy`
  // retries like `EAGAIN`.
  size_t numberOfSqesToTake() {
    const size_t numberOfQueuedSqes = queue_.size();
    if (numberOfQueuedSqes == 0 || !happens(faults_.partialSubmit)) {
      return numberOfQueuedSqes;
    }
    ++faultCounts_.partialSubmits;
    return uniform(0, numberOfQueuedSqes - 1);
  }

  //____________________________________________________________________________
  // The kernel takes the first `numberOfTakenSqes` queued SQEs, in queue
  // order, and may complete some of them right away. Return the result of the
  // submit.
  int takeQueuedSqes(size_t numberOfTakenSqes) {
    AD_CORRECTNESS_CHECK(numberOfTakenSqes <= queue_.size(),
                         "The kernel cannot take more SQEs than are queued.");
    ql::ranges::copy(queue_ | ql::views::take(numberOfTakenSqes),
                     std::back_inserter(kernel_));
    queue_.erase(queue_.begin(),
                 queue_.begin() + static_cast<ptrdiff_t>(numberOfTakenSqes));
    AD_CORRECTNESS_CHECK(kernel_.size() + completions_.size() <= cqCapacity_,
                         "The requests in the kernel and the unreaped "
                         "completions must fit into the completion queue, "
                         "or a real ring would drop completions.");
    // The kernel may already have completed some of the requests.
    if (faults_.completeAllOnSubmit) {
      completeRandomRequests(kernel_.size());
    } else if (faults_.completeOnSubmit) {
      completeRandomRequests(uniform(0, kernel_.size()));
    }
    return static_cast<int>(numberOfTakenSqes);
  }

  //____________________________________________________________________________
  // Draw whether an event of probability `probability` happens.
  bool happens(double probability) {
    return std::bernoulli_distribution{probability}(random_);
  }

  //____________________________________________________________________________
  // Draw a number from [`min`, `max`], uniformly.
  size_t uniform(size_t min, size_t max) {
    return std::uniform_int_distribution<size_t>{min, max}(random_);
  }

  //____________________________________________________________________________
  // Complete `numberOfRequestsToComplete` random requests of the kernel: write
  // a read's bytes into its buffer and post its completion.
  void completeRandomRequests(size_t numberOfRequestsToComplete) {
    const size_t numberOfCompleted =
        std::min(numberOfRequestsToComplete, kernel_.size());
    for ([[maybe_unused]] size_t completed :
         ad_utility::integerRange(numberOfCompleted)) {
      const size_t i = uniform(0, kernel_.size() - 1);
      const Request request = kernel_[i];
      kernel_.erase(kernel_.begin() + static_cast<ptrdiff_t>(i));
      completions_.push_back(PendingCompletion{
          Completion{request.requestId, simulateRequest(request)},
          request.isNoOp});
    }
  }

  //____________________________________________________________________________
  // Perform `request` on the simulated file: write a read's bytes into its
  // buffer. Return the result (`res`) of its completion.
  int simulateRequest(const Request& request) {
    if (request.isNoOp) {
      return 0;
    }
    if (happens(faults_.readError)) {
      injectedReadFault_ = true;
      ++faultCounts_.faultyReads;
      return -EIO;
    }
    size_t numberOfBytes =
        std::min(request.numberOfBytes,
                 file_.size() - std::min<size_t>(file_.size(), request.offset));
    if (numberOfBytes > 0 && happens(faults_.shortRead)) {
      numberOfBytes = uniform(0, numberOfBytes - 1);
    }
    if (numberOfBytes != request.numberOfBytes) {
      injectedReadFault_ = true;
      ++faultCounts_.faultyReads;
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
  std::mt19937 random_;
  std::deque<Request> queue_;
  std::vector<Request> kernel_;
  std::deque<PendingCompletion> completions_;
  size_t eagainBurstLeft_ = 0;
  // The number of calls of `submit` so far.
  size_t numberOfSubmits_ = 0;
  bool injectedReadFault_ = false;
  FaultCounts faultCounts_;
  CallCounts callCounts_;
};

}  // namespace ad_utility::testing

#endif  // QLEVER_HAS_IO_URING

#endif  // QLEVER_TEST_UTIL_FAKEIOURING_H
