// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests of `IoUringPolicy` on a simulated ring (`FakeIoUring`): the waves and
// chunks in which it reaps completions, and its handling of the failures that
// a real kernel produces only rarely. The simulated kernel writes into a
// target buffer only when it completes the read, so a read that is still in
// the ring after the policy returned (or threw) is a read that a real kernel
// could still write into a freed buffer.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cerrno>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "util/FakeIoUring.h"
#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"

#ifdef QLEVER_HAS_IO_URING

namespace {
using ad_utility::IoUringPolicy;
using ad_utility::testing::FakeIoUring;
using ad_utility::testing::FakeIoUringObservations;
using ::testing::HasSubstr;
using Faults = FakeIoUring::Faults;

// 64 blocks of 4 equal letters.
std::string makeFile() {
  std::string file;
  for (size_t i = 0; i < 64; ++i) {
    file.append(4, static_cast<char>('A' + i % 26));
  }
  return file;
}

// A batch of `numberOfReads` reads of 4 bytes each, from consecutive blocks of
// the file of `makeFile`, starting at block `firstBlock`. Owns the buffers.
class TestBatch {
 public:
  explicit TestBatch(size_t numberOfReads, size_t firstBlock = 0)
      : buffers_(numberOfReads, std::string(4, '\0')) {
    for (size_t i = 0; i < numberOfReads; ++i) {
      numBytes_.push_back(4);
      offsets_.push_back(4 * ((firstBlock + i) % 64));
      pointers_.push_back(buffers_[i].data());
    }
  }

  void addTo(IoUringPolicy& policy, IoUringPolicy::BatchHandle handle) {
    // The simulated kernel ignores the file descriptor.
    policy.addBatch(/*fd=*/-1, numBytes_, offsets_, pointers_, handle);
  }

  // Whether every read has written its bytes of `file` into its buffer.
  bool isComplete(const std::string& file) const {
    for (size_t i = 0; i < buffers_.size(); ++i) {
      if (buffers_[i] != file.substr(offsets_[i], 4)) {
        return false;
      }
    }
    return true;
  }

 private:
  std::vector<size_t> numBytes_;
  std::vector<uint64_t> offsets_;
  std::vector<std::string> buffers_;
  std::vector<char*> pointers_;
};

// An `IoUringPolicy` of `ringSize` on a `FakeIoUring` with `faults`; `fake`
// stays valid as long as `policy`.
struct PolicyOnFakeRing {
  FakeIoUringObservations& observations;
  FakeIoUring* fake;
  IoUringPolicy policy;

  PolicyOnFakeRing(unsigned ringSize, Faults faults,
                   FakeIoUringObservations& observations, uint32_t seed = 42)
      : PolicyOnFakeRing(ringSize,
                         std::make_unique<FakeIoUring>(
                             makeFile(), ringSize, faults, observations, seed),
                         observations) {}

 private:
  PolicyOnFakeRing(unsigned ringSize, std::unique_ptr<FakeIoUring> ring,
                   FakeIoUringObservations& observations)
      : observations{observations},
        fake{ring.get()},
        policy{ringSize, std::move(ring)} {}
};
}  // namespace

// When the ring is full, `addBatch` waits for a wave of `REAP_WAVE` (8)
// completions, not for one, and so does `wait()`: 64 reads through a ring of
// 16 take 6 waves in `addBatch` and 2 in `wait()`. Reaping one completion per
// wait would take 64 waits.
TEST(IoUringPolicyOnFakeRing, fullRingIsRefilledInWaves) {
  const std::string file = makeFile();
  TestBatch batch(64);
  FakeIoUringObservations observations;
  Faults faults;
  faults.completeOnSubmit = false;
  PolicyOnFakeRing ring(16, faults, observations);
  batch.addTo(ring.policy, 0);
  EXPECT_EQ(observations.numberOfWaits, size_t{6});
  ring.policy.wait(0);
  EXPECT_EQ(observations.numberOfWaits, size_t{8});
  EXPECT_EQ(observations.numberOfChunks, size_t{8});
  EXPECT_TRUE(batch.isComplete(file));
}

// One wave reaps every ready completion, in chunks of `REAP_CHUNK` (64): 200
// completions that are ready at once take one wait and 4 chunks.
TEST(IoUringPolicyOnFakeRing, readyCompletionsAreReapedInChunks) {
  const std::string file = makeFile();
  TestBatch batch(200);
  FakeIoUringObservations observations;
  Faults faults;
  faults.completeAllOnSubmit = true;
  PolicyOnFakeRing ring(256, faults, observations);
  batch.addTo(ring.policy, 0);
  ring.policy.wait(0);
  EXPECT_EQ(observations.numberOfWaits, size_t{1});
  EXPECT_EQ(observations.numberOfChunks, size_t{4});
  EXPECT_TRUE(batch.isComplete(file));
}

// The kernel may take only some of the submitted SQEs, or none (`EAGAIN`,
// `EINTR`). The policy submits the rest before it waits, so that it never
// waits for a read that the kernel does not have, which would block forever.
TEST(IoUringPolicyOnFakeRing, partialSubmitsAreCompleted) {
  const std::string file = makeFile();
  std::vector<TestBatch> batches;
  batches.reserve(20);
  for (size_t i = 0; i < 20; ++i) {
    batches.emplace_back(20, i);
  }
  FakeIoUringObservations observations;
  Faults faults;
  faults.partialSubmit = 0.5;
  faults.submitInterrupted = 0.1;
  faults.submitOutOfMemory = 0.1;
  PolicyOnFakeRing ring(8, faults, observations);
  for (size_t i = 0; i < batches.size(); ++i) {
    batches[i].addTo(ring.policy, i);
    ring.policy.wait(i);
    EXPECT_TRUE(batches[i].isComplete(file)) << i;
  }
  EXPECT_GT(observations.numberOfPartialSubmits, size_t{0});
  EXPECT_GT(observations.numberOfFailedSubmits, size_t{0});
}

// A signal interrupts waits with an empty completion queue (`-EINTR`); the
// policy waits again, also in the destructor, which must not tear down the
// ring while reads are in the kernel.
TEST(IoUringPolicyOnFakeRing, interruptedWaitsAreRetried) {
  const std::string file = makeFile();
  TestBatch waited(40);
  TestBatch neverWaited(10, 40);
  FakeIoUringObservations observations;
  {
    Faults faults;
    faults.completeOnSubmit = false;
    faults.waitInterrupted = 0.9;
    PolicyOnFakeRing ring(16, faults, observations);
    waited.addTo(ring.policy, 0);
    ring.policy.wait(0);
    EXPECT_TRUE(waited.isComplete(file));
    neverWaited.addTo(ring.policy, 1);
  }
  EXPECT_GT(observations.numberOfInterruptedWaits, size_t{0});
  EXPECT_TRUE(observations.tornDown);
  EXPECT_EQ(observations.numberOfReadsInKernelAtTeardown, size_t{0});
  EXPECT_TRUE(neverWaited.isComplete(file));
}

// If `addBatch` throws while reads are in the kernel (here because a reaped
// completion belongs to no read), it reaps them first: its caller frees the
// buffers when it unwinds. The policy stays usable.
TEST(IoUringPolicyOnFakeRing, throwingAddBatchLeavesNoReadInTheKernel) {
  const std::string file = makeFile();
  TestBatch failing(16);
  TestBatch next(16, 16);
  FakeIoUringObservations observations;
  Faults faults;
  faults.completeOnSubmit = false;
  PolicyOnFakeRing ring(4, faults, observations);
  ring.fake->injectCompletion({/*requestId=*/12345, /*result=*/0});
  AD_EXPECT_THROW_WITH_MESSAGE(failing.addTo(ring.policy, 0),
                               HasSubstr("not outstanding"));
  EXPECT_EQ(ring.fake->numberOfReadsInKernel(), size_t{0});
  EXPECT_EQ(ring.fake->numberOfQueued(), size_t{0});
  next.addTo(ring.policy, 1);
  ring.policy.wait(1);
  EXPECT_TRUE(next.isComplete(file));
}

// If `wait()` throws while reads are in the kernel, it reaps them first, also
// those of other batches, which stay usable.
TEST(IoUringPolicyOnFakeRing, throwingWaitLeavesNoReadInTheKernel) {
  const std::string file = makeFile();
  TestBatch first(8);
  TestBatch second(8, 8);
  FakeIoUringObservations observations;
  Faults faults;
  faults.completeOnSubmit = false;
  PolicyOnFakeRing ring(64, faults, observations);
  first.addTo(ring.policy, 0);
  second.addTo(ring.policy, 1);
  ring.fake->injectCompletion({/*requestId=*/12345, /*result=*/0});
  AD_EXPECT_THROW_WITH_MESSAGE(ring.policy.wait(0),
                               HasSubstr("not outstanding"));
  EXPECT_EQ(ring.fake->numberOfReadsInKernel(), size_t{0});
  EXPECT_TRUE(first.isComplete(file));
  ring.policy.wait(1);
  EXPECT_TRUE(second.isComplete(file));
}

// If submitting fails for good, the queued reads are cancelled: the `wait()`
// of their batch throws the failure, and the policy refuses every later batch,
// because the cancelled SQEs stay in the submission queue.
TEST(IoUringPolicyOnFakeRing, failedSubmitCancelsTheQueuedReads) {
  TestBatch batch(20);
  TestBatch later(4);
  FakeIoUringObservations observations;
  Faults faults;
  faults.brokenSubmitErrno = EIO;
  PolicyOnFakeRing ring(8, faults, observations);
  batch.addTo(ring.policy, 0);
  AD_EXPECT_THROW_WITH_MESSAGE(ring.policy.wait(0),
                               HasSubstr("io_uring_submit failed"));
  EXPECT_EQ(ring.fake->numberOfReadsInKernel(), size_t{0});
  ring.fake->setFaults(Faults{});
  AD_EXPECT_THROW_WITH_MESSAGE(later.addTo(ring.policy, 1),
                               HasSubstr("cannot submit reads any more"));
  EXPECT_EQ(ring.fake->numberOfReadsInKernel(), size_t{0});
}

// If waiting fails other than with `EINTR`, the reads in the kernel can still
// write into their buffers, and the policy cannot learn when they are done.
// It neither throws nor returns, but terminates.
TEST(IoUringPolicyOnFakeRingDeathTest, failedWaitTerminates) {
  auto addAndWait = [] {
    TestBatch batch(4);
    FakeIoUringObservations observations;
    Faults faults;
    faults.completeOnSubmit = false;
    faults.brokenWaitErrno = EFAULT;
    PolicyOnFakeRing ring(8, faults, observations);
    batch.addTo(ring.policy, 0);
    ring.policy.wait(0);
  };
  EXPECT_DEATH(addAndWait(), "io_uring_wait_cqes failed");
}

// A handle whose batch is still outstanding must not be reused: the counts of
// the two batches would be mixed up.
TEST(IoUringPolicyOnFakeRing, outstandingHandleCannotBeReused) {
  const std::string file = makeFile();
  TestBatch batch(4);
  TestBatch reused(4);
  FakeIoUringObservations observations;
  Faults faults;
  faults.completeOnSubmit = false;
  PolicyOnFakeRing ring(8, faults, observations);
  batch.addTo(ring.policy, 0);
  AD_EXPECT_THROW_WITH_MESSAGE(reused.addTo(ring.policy, 0),
                               HasSubstr("must not be reused"));
  ring.policy.wait(0);
  EXPECT_TRUE(batch.isComplete(file));
}

// Random batches, rings and faults. After every call, nothing is queued; a
// `wait()` either returns with every read of its batch complete or throws a
// read failure; and once every batch is waited for, no read is in the ring.
TEST(IoUringPolicyOnFakeRing, randomFaults) {
  const std::string file = makeFile();
  size_t numberOfFailedWaits = 0;
  for (uint32_t seed = 0; seed < 200; ++seed) {
    std::vector<std::unique_ptr<TestBatch>> batches;
    FakeIoUringObservations observations;
    Faults faults;
    faults.partialSubmit = 0.3;
    faults.submitInterrupted = 0.1;
    faults.submitOutOfMemory = 0.1;
    faults.waitInterrupted = 0.3;
    faults.shortRead = 0.01;
    faults.readError = 0.01;
    faults.completeOnSubmit = seed % 2 == 0;
    const unsigned ringSize = std::vector<unsigned>{1, 2, 8, 32}[seed % 4];
    std::mt19937 random{seed};
    {
      PolicyOnFakeRing ring(ringSize, faults, observations, seed);
      // Keep up to 3 batches outstanding, and wait for them in random order.
      std::vector<IoUringPolicy::BatchHandle> outstanding;
      IoUringPolicy::BatchHandle nextHandle = 0;
      for (size_t step = 0; step < 30; ++step) {
        if (outstanding.size() < 3 && random() % 2 == 0) {
          batches.push_back(
              std::make_unique<TestBatch>(1 + random() % 40, random() % 64));
          batches.back()->addTo(ring.policy, nextHandle);
          outstanding.push_back(nextHandle++);
        } else if (!outstanding.empty()) {
          const size_t i = random() % outstanding.size();
          const auto handle = outstanding[i];
          outstanding.erase(outstanding.begin() + static_cast<ptrdiff_t>(i));
          try {
            ring.policy.wait(handle);
            EXPECT_TRUE(batches[handle]->isComplete(file))
                << "seed " << seed << ", batch " << handle;
          } catch (const std::exception& e) {
            ++numberOfFailedWaits;
            EXPECT_THAT(e.what(), ::testing::AnyOf(HasSubstr("I/O error"),
                                                   HasSubstr("fewer bytes")));
          }
        }
        EXPECT_EQ(ring.fake->numberOfQueued(), size_t{0}) << "seed " << seed;
      }
      for (const auto handle : outstanding) {
        try {
          ring.policy.wait(handle);
        } catch (const std::exception&) {
          ++numberOfFailedWaits;
        }
      }
      EXPECT_EQ(ring.fake->numberOfReadsInKernel(), size_t{0}) << "seed " << seed;
    }
    EXPECT_EQ(observations.numberOfReadsInKernelAtTeardown, size_t{0});
  }
  // The faults actually occurred.
  EXPECT_GT(numberOfFailedWaits, size_t{0});
}

#endif  // QLEVER_HAS_IO_URING
