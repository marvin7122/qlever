// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests of `IoUringPolicy`. Most run on a simulated ring (`FakeIoUring`):
// they count the waves and chunks in which the policy reaps completions, and
// inject the failures that a real kernel produces only rarely. The simulated
// kernel writes into a target buffer only when it completes the read, so a
// read that is still in the ring after the policy threw is a read that a real
// kernel could still write into a freed buffer. The tests at the end run on a
// real ring.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"
#include "util/IoUringManagerTestHelpers.h"
#include "util/Log.h"
#include "util/io_uring/FakeIoUring.h"

#ifdef QLEVER_HAS_IO_URING

namespace {
using namespace ioUringManagerTestHelpers;
using ad_utility::IoUringPolicy;
using ad_utility::testing::FakeIoUring;
using ad_utility::testing::FakeIoUringObservations;
using ::testing::AllOf;
using ::testing::HasSubstr;
using Faults = FakeIoUring::Faults;
using CompletionOnSubmit = FakeIoUring::CompletionOnSubmit;

constexpr size_t REAP_WAVE = IoUringPolicy::REAP_WAVE;
constexpr size_t REAP_CHUNK = IoUringPolicy::REAP_CHUNK;

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
      numberOfBytesPerRead_.push_back(4);
      offsets_.push_back(4 * ((firstBlock + i) % 64));
      pointers_.push_back(buffers_[i].data());
    }
  }

  void addTo(IoUringPolicy& policy, IoUringPolicy::BatchHandle handle) {
    // The simulated kernel ignores the file descriptor.
    policy.addBatch(/*fd=*/-1, numberOfBytesPerRead_, offsets_, pointers_,
                    handle);
  }

  // Whether read `i` has written its bytes of `file` into its buffer.
  bool isReadComplete(size_t i, const std::string& file) const {
    return buffers_[i] == file.substr(offsets_[i], 4);
  }
  // Whether the buffer of read `i` was never written.
  bool isReadUntouched(size_t i) const {
    return buffers_[i] == std::string(4, '\0');
  }
  // Whether every read is complete.
  bool isComplete(const std::string& file) const {
    for (size_t i = 0; i < buffers_.size(); ++i) {
      if (!isReadComplete(i, file)) {
        return false;
      }
    }
    return true;
  }
  // Whether one of the buffers is in `buffers`.
  bool hasBufferIn(const std::unordered_set<const char*>& buffers) const {
    for (const char* pointer : pointers_) {
      if (buffers.contains(pointer)) {
        return true;
      }
    }
    return false;
  }

 private:
  std::vector<size_t> numberOfBytesPerRead_;
  std::vector<uint64_t> offsets_;
  std::vector<std::string> buffers_;
  std::vector<char*> pointers_;
};

// An `IoUringPolicy` of `ringSize` on a `FakeIoUring` with `faults`; `fake`
// stays valid as long as `policy`.
struct PolicyOnFakeRing {
  FakeIoUring* fake;
  IoUringPolicy policy;

  PolicyOnFakeRing(unsigned ringSize, Faults faults,
                   FakeIoUringObservations& observations, uint32_t seed = 42,
                   std::optional<size_t> sqCapacity = std::nullopt)
      : PolicyOnFakeRing(ringSize, std::make_unique<FakeIoUring>(
                                       makeFile(), ringSize, std::move(faults),
                                       observations, seed, sqCapacity)) {}

 private:
  PolicyOnFakeRing(unsigned ringSize, std::unique_ptr<FakeIoUring> ring)
      : fake{ring.get()}, policy{ringSize, std::move(ring)} {}
};

Faults noCompletionOnSubmit() {
  Faults faults;
  faults.completionOnSubmit = CompletionOnSubmit::None;
  return faults;
}
}  // namespace

// When the ring is full and no completion is ready, `addBatch` waits for a
// wave of `REAP_WAVE` completions, not for one, and so does `wait()`: 64 reads
// through a ring of 16 take (64 - 16) / `REAP_WAVE` waves in `addBatch` and
// 16 / `REAP_WAVE` in `wait()`. Reaping one completion per wait would take 64.
TEST(IoUringPolicyOnFakeRing, fullRingIsRefilledInWaves) {
  static_assert(16 % REAP_WAVE == 0);
  const std::string file = makeFile();
  TestBatch batch(64);
  FakeIoUringObservations observations;
  PolicyOnFakeRing ring(16, noCompletionOnSubmit(), observations);
  batch.addTo(ring.policy, 0);
  EXPECT_EQ(observations.numberOfWaits, (64 - 16) / REAP_WAVE);
  ring.policy.wait(0);
  EXPECT_EQ(observations.numberOfWaits, 64 / REAP_WAVE);
  EXPECT_EQ(observations.numberOfChunks, 64 / REAP_WAVE);
  EXPECT_TRUE(batch.isComplete(file));
}

// Completions that are ready are reaped without waiting, all of them, in
// chunks of `REAP_CHUNK`: 200 ready completions take no wait and
// ceil(200 / `REAP_CHUNK`) chunks.
TEST(IoUringPolicyOnFakeRing, readyCompletionsAreReapedInChunksWithoutWaiting) {
  const std::string file = makeFile();
  TestBatch batch(200);
  FakeIoUringObservations observations;
  Faults faults;
  faults.completionOnSubmit = CompletionOnSubmit::All;
  PolicyOnFakeRing ring(256, faults, observations);
  batch.addTo(ring.policy, 0);
  ring.policy.wait(0);
  EXPECT_EQ(observations.numberOfWaits, size_t{0});
  EXPECT_EQ(observations.numberOfChunks, (200 + REAP_CHUNK - 1) / REAP_CHUNK);
  EXPECT_TRUE(batch.isComplete(file));
}

// A signal can end the wait for a wave early: with at least one completion
// ready, `io_uring_wait_cqes` then returns 0 although fewer completions than
// the wave are ready. The policy reaps what is ready and waits again: here
// every wait returns after one completion, so 16 reads take 16 waits.
TEST(IoUringPolicyOnFakeRing, waitThatReturnsEarlyIsRepeated) {
  const std::string file = makeFile();
  TestBatch batch(16);
  FakeIoUringObservations observations;
  Faults faults = noCompletionOnSubmit();
  faults.waitReturnsEarly = 1.0;
  PolicyOnFakeRing ring(64, faults, observations);
  batch.addTo(ring.policy, 0);
  ring.policy.wait(0);
  EXPECT_EQ(observations.numberOfWaits, size_t{16});
  EXPECT_EQ(observations.numberOfEarlyWaits, size_t{16});
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
    Faults faults = noCompletionOnSubmit();
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
  PolicyOnFakeRing ring(4, noCompletionOnSubmit(), observations);
  ring.fake->injectCompletion({/*requestId=*/12345, /*result=*/0});
  AD_EXPECT_THROW_WITH_MESSAGE(failing.addTo(ring.policy, 0),
                               HasSubstr("not outstanding"));
  EXPECT_EQ(ring.fake->numberOfReadsInKernel(), size_t{0});
  next.addTo(ring.policy, 1);
  ring.policy.wait(1);
  EXPECT_TRUE(next.isComplete(file));
}

// If `wait()` throws while reads are in the kernel, it reaps them first, also
// those of other batches, which stay usable. The batch that threw is
// forgotten, also its failure: the destructor does not report it as never
// waited for.
TEST(IoUringPolicyOnFakeRing, throwingWaitLeavesNoReadInTheKernel) {
  const std::string file = makeFile();
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();
  TestBatch first(8);
  TestBatch second(8, 8);
  FakeIoUringObservations observations;
  {
    PolicyOnFakeRing ring(64, noCompletionOnSubmit(), observations);
    // Every read of `first` fails, and its failures are ready at once.
    Faults faults;
    faults.completionOnSubmit = CompletionOnSubmit::All;
    faults.readError = 1.0;
    ring.fake->setFaults(faults);
    first.addTo(ring.policy, 0);
    ring.fake->setFaults(noCompletionOnSubmit());
    second.addTo(ring.policy, 1);
    ring.fake->injectCompletion({/*requestId=*/12345, /*result=*/0});
    AD_EXPECT_THROW_WITH_MESSAGE(ring.policy.wait(0),
                                 HasSubstr("not outstanding"));
    EXPECT_EQ(ring.fake->numberOfReadsInKernel(), size_t{0});
    ring.policy.wait(1);
    EXPECT_TRUE(second.isComplete(file));
  }
  EXPECT_THAT(logStream.str(), ::testing::Not(HasSubstr("never `wait()`ed")));
}

// A failed read is thrown by the `wait()` of its own batch, also when the
// `wait()` of another batch reaped it, and only once.
TEST(IoUringPolicyOnFakeRing, failureIsThrownByItsOwnBatch) {
  const std::string file = makeFile();
  TestBatch bad(4);
  TestBatch good(4, 4);
  FakeIoUringObservations observations;
  Faults faults;
  faults.completionOnSubmit = CompletionOnSubmit::All;
  PolicyOnFakeRing ring(64, faults, observations);
  faults.readError = 1.0;
  ring.fake->setFaults(faults);
  bad.addTo(ring.policy, 0);
  faults.readError = 0;
  ring.fake->setFaults(faults);
  good.addTo(ring.policy, 1);
  // Every completion is ready, so this reaps the failed reads of `bad`, too.
  ring.policy.wait(1);
  EXPECT_TRUE(good.isComplete(file));
  EXPECT_EQ(ring.fake->numberOfReadsInKernel(), size_t{0});
  AD_EXPECT_THROW_WITH_MESSAGE(
      ring.policy.wait(0),
      AllOf(HasSubstr("I/O error in IoUringPolicy read operation for request"),
            HasSubstr(std::strerror(EIO))));
  // Reported once.
  ring.policy.wait(0);
}

// If submitting fails for good, `addBatch` cancels the queued reads (they
// become no-ops), reaps the reads of the batch that are already in the
// kernel, and throws. The ring stays usable.
TEST(IoUringPolicyOnFakeRing, failedSubmitWithReadsInTheKernel) {
  const std::string file = makeFile();
  TestBatch batch(24);
  TestBatch later(20, 24);
  FakeIoUringObservations observations;
  Faults faults = noCompletionOnSubmit();
  // The first submit (of 16 reads when the ring is full) succeeds; the one of
  // the last 8 reads fails.
  faults.brokenSubmitErrno = EIO;
  faults.numberOfSubmitsBeforeBreak = 1;
  PolicyOnFakeRing ring(16, faults, observations);
  AD_EXPECT_THROW_WITH_MESSAGE(batch.addTo(ring.policy, 0),
                               HasSubstr("io_uring_submit failed"));
  EXPECT_EQ(ring.fake->numberOfReadsInKernel(), size_t{0});
  EXPECT_EQ(observations.numberOfCancelledSqes, size_t{8});
  for (size_t i = 0; i < 16; ++i) {
    EXPECT_TRUE(batch.isReadComplete(i, file)) << i;
  }
  for (size_t i = 16; i < 24; ++i) {
    EXPECT_TRUE(batch.isReadUntouched(i)) << i;
  }
  ring.fake->setFaults(Faults{});
  later.addTo(ring.policy, 1);
  ring.policy.wait(1);
  EXPECT_TRUE(later.isComplete(file));
}

// A submit that takes no SQE (`EAGAIN`) is retried `MAX_SUBMIT_RETRIES` times,
// then the batch fails as above, and the ring stays usable.
TEST(IoUringPolicyOnFakeRing, submitRetriesRunOut) {
  const std::string file = makeFile();
  TestBatch batch(4);
  TestBatch later(4, 4);
  FakeIoUringObservations observations;
  Faults faults;
  faults.submitOutOfMemory = 1.0;
  PolicyOnFakeRing ring(8, faults, observations);
  AD_EXPECT_THROW_WITH_MESSAGE(
      batch.addTo(ring.policy, 0),
      HasSubstr(absl::StrCat("after ", IoUringPolicy::MAX_SUBMIT_RETRIES,
                             " retries")));
  EXPECT_EQ(observations.numberOfFailedSubmits,
            IoUringPolicy::MAX_SUBMIT_RETRIES + 1);
  ring.fake->setFaults(Faults{});
  later.addTo(ring.policy, 1);
  ring.policy.wait(1);
  EXPECT_TRUE(later.isComplete(file));
}

// If no SQE is free although the policy expects one (a ring smaller than its
// size), `addBatch` forgets the read and throws.
TEST(IoUringPolicyOnFakeRing, missingSqeIsRolledBack) {
  TestBatch batch(4);
  FakeIoUringObservations observations;
  PolicyOnFakeRing ring(8, noCompletionOnSubmit(), observations, 42,
                        /*sqCapacity=*/2);
  AD_EXPECT_THROW_WITH_MESSAGE(batch.addTo(ring.policy, 0),
                               HasSubstr("no free SQE"));
  EXPECT_EQ(ring.fake->numberOfReadsInKernel(), size_t{0});
  // The 2 prepared reads became no-ops.
  EXPECT_EQ(observations.numberOfCancelledSqes, size_t{2});
  // Nothing of the batch is left to wait for.
  ring.policy.wait(0);
}

// If waiting fails other than with `EINTR`, the reads in the kernel can still
// write into their buffers, and the policy cannot learn when they are done.
// It neither throws nor returns, but terminates.
TEST(IoUringPolicyOnFakeRingDeathTest, failedWaitTerminates) {
  auto addAndWait = [] {
    TestBatch batch(4);
    FakeIoUringObservations observations;
    Faults faults = noCompletionOnSubmit();
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
  PolicyOnFakeRing ring(8, noCompletionOnSubmit(), observations);
  batch.addTo(ring.policy, 0);
  AD_EXPECT_THROW_WITH_MESSAGE(reused.addTo(ring.policy, 0),
                               HasSubstr("must not be reused"));
  ring.policy.wait(0);
  EXPECT_TRUE(batch.isComplete(file));
}

// Random batches, rings and faults. After every call nothing is queued; a
// `wait()` throws if and only if a read of its own batch failed or was short,
// and otherwise every read of the batch is complete; once every batch is
// waited for, no read is in the ring.
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
    faults.waitReturnsEarly = 0.3;
    faults.shortRead = 0.01;
    faults.readError = 0.01;
    faults.completionOnSubmit =
        seed % 2 == 0 ? CompletionOnSubmit::Random : CompletionOnSubmit::None;
    const unsigned ringSize = std::array<unsigned, 4>{1, 2, 8, 32}[seed % 4];
    std::mt19937 random{seed};
    {
      PolicyOnFakeRing ring(ringSize, faults, observations, seed);
      // The faults of a batch are all known once its `wait()` returned or
      // threw, because then every read of the batch has completed.
      auto waitAndCheck = [&](IoUringPolicy::BatchHandle handle) {
        const TestBatch& batch = *batches[handle];
        try {
          ring.policy.wait(handle);
          EXPECT_FALSE(batch.hasBufferIn(observations.faultyBuffers))
              << "seed " << seed << ", batch " << handle;
          EXPECT_TRUE(batch.isComplete(file))
              << "seed " << seed << ", batch " << handle;
        } catch (const std::exception& e) {
          ++numberOfFailedWaits;
          EXPECT_TRUE(batch.hasBufferIn(observations.faultyBuffers))
              << "seed " << seed << ", batch " << handle;
          EXPECT_THAT(e.what(), ::testing::AnyOf(HasSubstr("I/O error"),
                                                 HasSubstr("fewer bytes")));
        }
      };
      // Keep up to 3 batches outstanding, and wait for them in random order.
      std::vector<IoUringPolicy::BatchHandle> outstanding;
      for (size_t step = 0; step < 30; ++step) {
        if (outstanding.size() < 3 && random() % 2 == 0) {
          const IoUringPolicy::BatchHandle handle = batches.size();
          batches.push_back(
              std::make_unique<TestBatch>(1 + random() % 40, random() % 64));
          batches.back()->addTo(ring.policy, handle);
          outstanding.push_back(handle);
        } else if (!outstanding.empty()) {
          const size_t i = random() % outstanding.size();
          const auto handle = outstanding[i];
          outstanding.erase(outstanding.begin() + static_cast<ptrdiff_t>(i));
          waitAndCheck(handle);
        }
        EXPECT_EQ(ring.fake->numberOfQueued(), size_t{0}) << "seed " << seed;
      }
      for (const auto handle : outstanding) {
        waitAndCheck(handle);
      }
      EXPECT_EQ(ring.fake->numberOfReadsInKernel(), size_t{0})
          << "seed " << seed;
    }
    EXPECT_EQ(observations.numberOfReadsInKernelAtTeardown, size_t{0});
  }
  // The faults actually occurred.
  EXPECT_GT(numberOfFailedWaits, size_t{0});
}

// Fixture of the tests that need `IoUringPolicy` on a real ring; skips them if
// io_uring is not available at runtime.
class IoUringPolicyOnRealRing : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!ioUringAvailableAtRuntime()) {
      GTEST_SKIP() << "io_uring is compiled in, but not available at runtime "
                      "(e.g. blocked by seccomp inside Docker)";
    }
  }
};

// A short read on a real ring is thrown by the `wait()` of its own batch, and
// only once; the other batch is not affected. (That this also holds when the
// `wait()` of another batch reaps the failed read is ensured by
// `IoUringPolicyOnFakeRing.failureIsThrownByItsOwnBatch`.)
TEST_F(IoUringPolicyOnRealRing, failureIsThrownByItsOwnBatch) {
  using Manager = ad_utility::BatchManager<ad_utility::IoUringPolicy>;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");  // 12 bytes
  ReadBatchForTesting bad;
  bad.add({{0, 4}, {8, 16}});  // the second read is past EOF: short read
  ReadBatchForTesting good;
  good.add({{0, 4}, {4, 4}, {8, 4}});
  // Declared after the batches, so that their buffers outlive the drain in the
  // destructor of `IoUringPolicy` if the test unwinds early.
  Manager manager(64);
  auto badHandle = bad.submitTo(manager, fd);
  auto goodHandle = good.submitTo(manager, fd);
  // Cached reads complete during submission, so this wave can also reap the
  // failed read of `bad`.
  manager.wait(goodHandle);
  EXPECT_THAT(good.result(), ::testing::ElementsAre("AAAA", "BBBB", "CCCC"));
  AD_EXPECT_THROW_WITH_MESSAGE(manager.wait(badHandle),
                               HasSubstr("read fewer bytes than requested"));
  // The successful read of the failed batch landed in its buffer.
  EXPECT_THAT(bad.result(), ::testing::ElementsAre("AAAA", ::testing::_));
  // Reported once; nothing of `bad` is outstanding any more.
  manager.wait(badHandle);
}

// A failure that `addBatch` reaps while it waits for a free slot in the full
// ring is also kept for the failed batch: neither `addBatch` nor the `wait()`
// of the other batch throws it.
TEST_F(IoUringPolicyOnRealRing, failureReapedByAddBatchIsThrownByItsOwnBatch) {
  using Manager = ad_utility::BatchManager<ad_utility::IoUringPolicy>;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");  // 12 bytes
  ReadBatchForTesting bad;
  bad.add({{0, 4}, {8, 16}});  // the second read is past EOF: short read
  ReadBatchForTesting good;
  good.add({{0, 4}, {4, 4}, {8, 4}});
  // A ring of 2 slots, which `bad` fills, so submitting `good` reaps the
  // completions of `bad` in `addBatch`.
  Manager manager(2);
  auto badHandle = bad.submitTo(manager, fd);
  auto goodHandle = good.submitTo(manager, fd);
  manager.wait(goodHandle);
  EXPECT_THAT(good.result(), ::testing::ElementsAre("AAAA", "BBBB", "CCCC"));
  AD_EXPECT_THROW_WITH_MESSAGE(manager.wait(badHandle),
                               HasSubstr("read fewer bytes than requested"));
  // The successful read of the failed batch landed in its buffer.
  EXPECT_THAT(bad.result(), ::testing::ElementsAre("AAAA", ::testing::_));
}

// A read that the kernel fails (here with `EBADF`, because the descriptor is
// the write end of a pipe) is thrown by the `wait()` of its batch, with the
// request id and the `errno` text.
TEST_F(IoUringPolicyOnRealRing, ioErrorIsThrownByItsOwnBatch) {
  std::array<int, 2> pipeFds{};
  ASSERT_EQ(pipe(pipeFds.data()), 0);
  absl::Cleanup closePipe{[&pipeFds] {
    close(pipeFds[0]);
    close(pipeFds[1]);
  }};
  ReadBatchForTesting batch;
  batch.add(0, 4);
  ad_utility::BatchManager<ad_utility::IoUringPolicy> manager(64);
  auto handle = batch.submitTo(manager, pipeFds[1]);
  AD_EXPECT_THROW_WITH_MESSAGE(
      manager.wait(handle),
      AllOf(HasSubstr("I/O error in IoUringPolicy read operation for request"),
            HasSubstr(std::strerror(EBADF))));
}

// A failed batch that is never waited for is reported by the destructor, so
// that its error does not get lost.
TEST_F(IoUringPolicyOnRealRing, unwaitedFailureIsLoggedByTheDestructor) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");  // 12 bytes
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();
  ReadBatchForTesting bad;
  bad.add(8, 16);  // past EOF: short read
  ReadBatchForTesting good;
  good.add(0, 4);
  {
    // A ring of 1 slot, which `bad` fills, so submitting `good` reaps the
    // failed read of `bad` in `addBatch`.
    ad_utility::BatchManager<ad_utility::IoUringPolicy> manager(1);
    bad.submitTo(manager, fd);
    manager.wait(good.submitTo(manager, fd));
  }
  EXPECT_THAT(logStream.str(),
              ifLogLevelIsCompiledIn<LogLevel::Enum::WARN>(
                  AllOf(HasSubstr("1 failed batch(es) that were never"),
                        HasSubstr("read fewer bytes than requested"))));
}

// Drop the manager while reads are still outstanding (submitted but never
// waited). `IoUringPolicy`'s destructor drains the outstanding completions
// (and logs a warning) before tearing down the ring, so the kernel is done
// writing into the target buffers. `batch` is declared before `manager`, so its
// buffers outlive the destructor's drain. After the manager is destroyed every
// read has completed, so the results are correct. This draining is specific to
// the asynchronous io_uring backend, so the test is not part of the typed
// suite.
TEST_F(IoUringPolicyOnRealRing, dropRunningManager) {
  using Manager = ad_utility::BatchManager<ad_utility::IoUringPolicy>;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  // Redirect the global logging stream so we can assert on the destructor's
  // warning.
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});
  {
    Manager manager(64);
    batch.submitTo(manager, fd);  // submit, but never wait
    // `manager` is destroyed here; its destructor drains the outstanding reads.
  }

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
  EXPECT_THAT(logStream.str(), ifLogLevelIsCompiledIn<LogLevel::Enum::WARN>(
                                   ::testing::HasSubstr("still outstanding")));
}
#endif  // QLEVER_HAS_IO_URING
