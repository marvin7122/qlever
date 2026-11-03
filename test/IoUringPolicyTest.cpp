// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests of `IoUringPolicy`. Most run on a simulated ring (`FakeIoUring`),
// which injects the failures that a real kernel produces only rarely. The
// simulated kernel writes into a target buffer only when it completes the
// read, so a read that is still in the ring after `readBatch` returned is a
// read that a real kernel could still write into a freed buffer. The
// `LiburingRing` tests run on a real ring.

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <filesystem>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"
#include "util/IoUringManagerTestHelpers.h"
#include "util/Log.h"
#include "util/io_uring/BatchOfReads.h"
#include "util/io_uring/FakeIoUring.h"
#include "util/io_uring/IoUringPolicy.h"

#ifdef QLEVER_HAS_IO_URING

namespace {

using namespace ::testing;
using namespace ioUringManagerTestHelpers;

// Skip the current test unless io_uring works at runtime.
#define SKIP_WITHOUT_IO_URING()                                              \
  if (!ioUringAvailableAtRuntime()) {                                        \
    GTEST_SKIP() << "io_uring is compiled in, but not available at runtime " \
                    "(e.g. blocked by seccomp inside Docker)";               \
  }

// `LiburingRing::cancelQueued` rewrites the queued SQEs in the real ring: once
// submitted, they complete as no-ops and never write into their buffers. It
// finds the queued SQEs from the kernel's head of the submission queue, so the
// test also runs after `numberOfReadsBefore` reads have gone through the ring:
// with 6 of 8 slots used before, the 4 cancelled SQEs are in the slots 6, 7, 0
// and 1, so the positions wrap around the end of the queue.
TEST(LiburingRing, cancelledReadsCompleteAsNoOps) {
  SKIP_WITHOUT_IO_URING();
  using Completion = ad_utility::IoUringInterface::Completion;
  // The policy's real no-op id, so that a collision with an id that liburing
  // reserves would show up here.
  constexpr uint64_t noOp = ad_utility::IoUringPolicy::NO_OP_REQUEST_ID;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  for (const size_t numberOfReadsBefore : {size_t{0}, size_t{6}}) {
    SCOPED_TRACE(absl::StrCat(numberOfReadsBefore, " reads before"));
    auto ring = ad_utility::makeLiburingRing(8);
    std::array<Completion, 8> completions{};
    // Move the kernel's head of the submission queue forward.
    std::vector<std::string> readBuffers(numberOfReadsBefore,
                                         std::string(4, '-'));
    for (size_t i : ql::views::iota(size_t{0}, numberOfReadsBefore)) {
      ASSERT_TRUE(ring->prepareRead(fd, readBuffers[i].data(), 4, 0, i));
    }
    if (numberOfReadsBefore > 0) {
      ASSERT_EQ(ring->submit(), static_cast<int>(numberOfReadsBefore));
      ASSERT_EQ(
          ring->waitForCompletions(static_cast<unsigned>(numberOfReadsBefore)),
          0);
      ASSERT_EQ(ring->popCompletions(completions), numberOfReadsBefore);
      EXPECT_THAT(readBuffers, Each(std::string("AAAA")));
    }

    constexpr size_t NUMBER_OF_READS = 4;
    std::vector<std::string> buffers(NUMBER_OF_READS, std::string(4, '-'));
    for (size_t i : ql::views::iota(size_t{0}, NUMBER_OF_READS)) {
      ASSERT_TRUE(ring->prepareRead(fd, buffers[i].data(), 4, 4 * i, i));
    }
    EXPECT_THAT(ring->cancelQueued(noOp), ElementsAre(0, 1, 2, 3));
    // No-ops that are cancelled again are not reported again.
    EXPECT_THAT(ring->cancelQueued(noOp), IsEmpty());
    ASSERT_EQ(ring->submit(), static_cast<int>(NUMBER_OF_READS));
    ASSERT_EQ(ring->waitForCompletions(NUMBER_OF_READS), 0);
    ASSERT_EQ(ring->popCompletions(completions), NUMBER_OF_READS);
    // Every cancelled read completes as a no-op with result 0.
    // A vector, not a `take` view: gtest would print the view with range-v3's
    // `operator<<`, which GCC 11 fails to compile for this element type.
    const std::vector<Completion> reaped(completions.begin(),
                                         completions.begin() + NUMBER_OF_READS);
    EXPECT_THAT(reaped,
                ::testing::Each(::testing::AllOf(
                    AD_FIELD(Completion, requestId, ::testing::Eq(noOp)),
                    AD_FIELD(Completion, result, ::testing::Eq(0)))));
    EXPECT_THAT(buffers, Each(std::string(4, '-')));
  }
}

// A model-based test of `IoUringPolicy` on `FakeIoUring`, which takes only
// some of the submitted SQEs, fails submits with `EINTR` and with bursts of
// `EAGAIN` (some longer than the policy retries), interrupts waits, completes
// requests in any order, and makes reads short or fail. For many random
// seeds, ring sizes and batches it checks:
// - after every `readBatch`, the ring holds no read of the batch, so the
//   caller may free the buffers;
// - `readBatch` throws iff a read of the batch was faulty, and otherwise every
//   buffer holds the bytes of its file range, also if the policy cancelled
//   reads and read the batch again with `pread` (from the real file, which
//   has the same content as the fake's).
TEST(IoUringPolicyModel, randomFaultsNeverLeaveReadsInTheRing) {
  using ad_utility::testing::FakeIoUring;
  std::string file;
  for (size_t i : ql::views::iota(size_t{0}, size_t{64})) {
    file += absl::StrFormat("%04zu", i);
  }
  auto [tmp, fd] = makeTempFile(file);
  FakeIoUring::FaultCounts total;
  for (uint32_t seed : ql::views::iota(uint32_t{0}, uint32_t{300})) {
    SCOPED_TRACE(absl::StrCat("seed ", seed));
    std::mt19937 random{seed};
    auto pick = [&random](std::initializer_list<double> values) {
      return *(values.begin() + std::uniform_int_distribution<size_t>{
                                    0, values.size() - 1}(random));
    };
    FakeIoUring::Faults faults;
    faults.partialSubmit = pick({0, 0.3, 0.9});
    faults.submitInterrupted = pick({0, 0.1});
    faults.submitOutOfMemory = pick({0, 0.05, 0.3});
    faults.maxEagainBurst = static_cast<size_t>(pick({1, 4, 40}));
    faults.waitInterrupted = pick({0, 0.1});
    faults.shortRead = pick({0, 0.01, 0.1});
    faults.readError = pick({0, 0.01});
    const auto ringSize = static_cast<size_t>(pick({1, 2, 4, 8, 16}));
    auto fakeRing = std::make_unique<FakeIoUring>(file, ringSize, faults, seed);
    FakeIoUring& fake = *fakeRing;
    ad_utility::IoUringPolicy policy{std::move(fakeRing)};

    for (size_t batchIndex : ql::views::iota(size_t{0}, size_t{10})) {
      SCOPED_TRACE(absl::StrCat("batch ", batchIndex));
      ReadBatchForTesting batch;
      const size_t numberOfReads =
          std::uniform_int_distribution<size_t>{0, 40}(random);
      for ([[maybe_unused]] size_t read :
           ql::views::iota(size_t{0}, numberOfReads)) {
        const size_t numberOfBytes =
            std::uniform_int_distribution<size_t>{1, 16}(random);
        const size_t offset = std::uniform_int_distribution<size_t>{
            0, file.size() - numberOfBytes}(random);
        batch.add(offset, numberOfBytes);
      }
      fake.resetObservations();
      bool threw = false;
      try {
        batch.readWith(policy, fd);
      } catch (const std::exception& e) {
        threw = true;
        EXPECT_THAT(e.what(), HasSubstr("IoUringPolicy"));
      }
      ASSERT_EQ(fake.numberOfReadsInRing(), 0u);
      EXPECT_EQ(threw, fake.injectedReadFault());
      if (!threw) {
        const auto expected = ::ranges::to<std::vector>(
            ql::views::iota(size_t{0}, numberOfReads) |
            ql::views::transform([&batch, &file](size_t i) {
              return file.substr(batch.offset(i), batch.result()[i].size());
            }));
        EXPECT_THAT(batch.result(), ::testing::ElementsAreArray(expected));
      }
    }
    const auto& counts = fake.faultCounts();
    total.partialSubmits += counts.partialSubmits;
    total.interruptedSubmits += counts.interruptedSubmits;
    total.outOfMemorySubmits += counts.outOfMemorySubmits;
    total.interruptedWaits += counts.interruptedWaits;
    total.faultyReads += counts.faultyReads;
    total.cancelledReads += counts.cancelledReads;
  }
  // Every kind of fault occurred.
  EXPECT_GT(total.partialSubmits, 0u);
  EXPECT_GT(total.interruptedSubmits, 0u);
  EXPECT_GT(total.outOfMemorySubmits, 0u);
  EXPECT_GT(total.interruptedWaits, 0u);
  EXPECT_GT(total.faultyReads, 0u);
  EXPECT_GT(total.cancelledReads, 0u);
}

// A file of `numberOfReads` distinct chunks of 4 bytes, and a batch that reads
// each chunk.
struct ChunkedFile {
  std::string content_;
  ReadBatchForTesting batch_;
  std::vector<std::string> expected_;

  // `explicit`, so that a bare number does not silently become a file.
  explicit ChunkedFile(size_t numberOfReads) {
    for (size_t i : ql::views::iota(size_t{0}, numberOfReads)) {
      batch_.add(content_.size(), 4);
      expected_.push_back(absl::StrFormat("%04zu", i));
      content_ += expected_.back();
    }
  }
};

// Read a batch of `numberOfReads` reads of 4 bytes each with an `IoUringPolicy`
// on a `FakeIoUring` with `ringSize` slots that injects `faults`.
void readWithFaults(ad_utility::testing::FakeIoUring::Faults faults,
                    size_t ringSize, size_t numberOfReads) {
  ChunkedFile chunks{numberOfReads};
  auto [tmp, fd] = makeTempFile(chunks.content_);
  ad_utility::IoUringPolicy policy{
      std::make_unique<ad_utility::testing::FakeIoUring>(chunks.content_,
                                                         ringSize, faults, 0)};
  chunks.batch_.readWith(policy, fd);
}

// A submit error other than `EINTR` and `EAGAIN` before the kernel took any
// read of the batch means that the ring is unusable, but nothing was handed to
// the kernel. The policy tears the ring down, logs that once, and reads this
// batch and every later one with `pread`. The fake ring would fail the first
// ten submits, so a ring that is not torn down fails (and logs) again.
TEST(IoUringPolicy, aFatalSubmitRetiresTheRing) {
  using ad_utility::testing::FakeIoUring;
  ChunkedFile chunks{3};
  auto [tmp, fd] = makeTempFile(chunks.content_);
  FakeIoUring::Faults faults;
  faults.scriptedSubmits.assign(10, -EBADF);
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();
  ad_utility::IoUringPolicy policy{
      std::make_unique<FakeIoUring>(chunks.content_, 8, faults, 0)};
  for ([[maybe_unused]] size_t round : ql::views::iota(size_t{0}, size_t{3})) {
    ReadBatchForTesting batch;
    batch.add({{4, 4}, {0, 4}, {8, 4}});
    batch.readWith(policy, fd);
    EXPECT_THAT(batch.result(), ElementsAre("0001", "0000", "0002"));
  }
  const std::string log = logStream.str();
  EXPECT_THAT(log,
              ifLogLevelIsCompiledIn<ad_utility::LogLevel::Enum::WARN>(
                  AllOf(HasSubstr("io_uring_submit failed in IoUringPolicy"),
                        HasSubstr("reads with pread from now on"))));
  // Logged at most once: the ring was torn down after the first batch.
  EXPECT_EQ(log.find("reads with pread"), log.rfind("reads with pread"));
}

// A fatal submit error while the kernel holds only no-ops (of reads that were
// cancelled in an earlier batch) also retires the ring, without waiting for
// the no-ops: the wait would fail on the broken ring and terminate. Batch 1:
// every submit fails with `EAGAIN` (17 times: the first try and 16 retries),
// so its reads are cancelled and read with `pread`, and their 2 no-ops stay
// queued. Batch 2: the first submit takes only those 2 no-ops, the second one
// fails with `EBADF`.
TEST(IoUringPolicy, aFatalSubmitWithOnlyNoOpsInTheKernelRetiresTheRing) {
  using ad_utility::testing::FakeIoUring;
  ChunkedFile chunks{4};
  auto [tmp, fd] = makeTempFile(chunks.content_);
  FakeIoUring::Faults faults;
  faults.scriptedSubmits.assign(17, -EAGAIN);
  faults.scriptedSubmits.push_back(2);
  faults.scriptedSubmits.push_back(-EBADF);
  faults.brokenWaitErrno = EBADF;
  faults.completeOnSubmit = false;
  ad_utility::IoUringPolicy policy{
      std::make_unique<FakeIoUring>(chunks.content_, 8, faults, 0)};
  ReadBatchForTesting first;
  first.add({{0, 4}, {4, 4}});
  first.readWith(policy, fd);
  EXPECT_THAT(first.result(), ElementsAre("0000", "0001"));
  ReadBatchForTesting second;
  second.add({{8, 4}, {12, 4}});
  second.readWith(policy, fd);
  EXPECT_THAT(second.result(), ElementsAre("0002", "0003"));
}

// A submit error other than `EINTR` and `EAGAIN` while the kernel holds reads
// of the batch: throwing could let the caller free buffers that the kernel
// still writes into, so the process terminates. The first submit takes the 16
// reads that fit into the ring and completes none of them; the first wave
// reaps 8 of them, so 8 are still in the kernel when the second submit (of the
// next 8 reads) fails.
TEST(IoUringPolicy, aFatalSubmitWithReadsInTheKernelTerminates) {
  // Avoid warnings and crashes when running all tests at once.
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ad_utility::testing::FakeIoUring::Faults faults;
  faults.scriptedSubmits = {16, -EBADF};
  faults.completeOnSubmit = false;
  EXPECT_DEATH_IF_SUPPORTED(
      readWithFaults(faults, 16, 24),
      "io_uring_submit failed in IoUringPolicy.*may still write");
}

// A failed wait for completions terminates the process.
TEST(IoUringPolicy, aFailedWaitTerminates) {
  // Avoid warnings and crashes when running all tests at once.
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ad_utility::testing::FakeIoUring::Faults faults;
  faults.brokenWaitErrno = EBADF;
  // Keep both reads in the kernel, so that the policy has to wait.
  faults.completeOnSubmit = false;
  EXPECT_DEATH_IF_SUPPORTED(readWithFaults(faults, 8, 2),
                            "io_uring_wait_cqes failed in IoUringPolicy");
}

// A read whose length does not fit into the `int` of its completion is
// rejected before anything is queued.
TEST(IoUringPolicy, aReadLongerThanAnIntIsRejected) {
  using ad_utility::IoUringPolicy;
  IoUringPolicy policy{std::make_unique<ad_utility::testing::FakeIoUring>(
      "AAAA", 8, ad_utility::testing::FakeIoUring::Faults{}, 0)};
  std::vector<size_t> numberOfBytes{
      4, IoUringPolicy::MAX_NUMBER_OF_BYTES_PER_READ + 1};
  std::vector<uint64_t> offsets{0, 0};
  std::string buffer(4, '-');
  std::vector<char*> buffers{buffer.data(), buffer.data()};
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      policy.readBatch(
          -1, ad_utility::BatchOfReads{numberOfBytes, offsets, buffers}),
      HasSubstr("must fit into the `int`"), ad_utility::Exception);
  EXPECT_EQ(buffer, "----");
}

// `IoUringPolicy` needs a ring with at least one SQE and at least as many CQEs
// as SQEs.
TEST(IoUringPolicy, invalidConstructionThrows) {
  using ad_utility::IoUringPolicy;
  using ad_utility::testing::FakeIoUring;
  auto makeFake = [](size_t sqCapacity, size_t cqCapacity) {
    return std::make_unique<FakeIoUring>("AAAA", sqCapacity,
                                         FakeIoUring::Faults{}, 0, cqCapacity);
  };
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(IoUringPolicy(nullptr),
                                        HasSubstr("a ring to run on"),
                                        ad_utility::Exception);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(IoUringPolicy(makeFake(0, 8)),
                                        HasSubstr("needs an SQE"),
                                        ad_utility::Exception);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(IoUringPolicy(makeFake(8, 7)),
                                        HasSubstr("at least as many CQEs"),
                                        ad_utility::Exception);
  EXPECT_NO_THROW(IoUringPolicy(makeFake(8, 8)));
}

// A liburing ring of 8 SQEs has exactly 8 SQEs and twice as many CQEs, so it
// satisfies what `IoUringPolicy` checks.
TEST(LiburingRing, capacitiesMatchTheRingSize) {
  SKIP_WITHOUT_IO_URING();
  auto ring = ad_utility::makeLiburingRing(8);
  EXPECT_EQ(ring->sqCapacity(), 8u);
  EXPECT_EQ(ring->cqCapacity(), 16u);
}

// The number of file descriptors that the process has open.
size_t numberOfOpenFileDescriptors() {
  return static_cast<size_t>(
      std::distance(std::filesystem::directory_iterator{"/proc/self/fd"},
                    std::filesystem::directory_iterator{}));
}

// A probe that fails, and one that finds an operation missing.
ad_utility::detail::OpcodeSupport probeFails(::io_uring*) noexcept {
  return ad_utility::detail::OpcodeSupport::ProbeFailed;
}
ad_utility::detail::OpcodeSupport opcodeIsMissing(::io_uring*) noexcept {
  return ad_utility::detail::OpcodeSupport::OpcodeMissing;
}

// If the kernel does not confirm `IORING_OP_READ` and `IORING_OP_NOP` (before
// Linux 5.6 every read would fail with `EINVAL`), `makeLiburingRing` tears the
// ring down again (no file descriptor is left open) and throws with the cause,
// so that `makeBatchManager`, which catches it, falls back to `pread`.
TEST(LiburingRing, aKernelWithoutTheNeededOperationsIsNotUsed) {
  SKIP_WITHOUT_IO_URING();
  const std::pair<ad_utility::detail::OpcodeProbe, std::string_view>
      probesAndCauses[] = {{&probeFails, "the probe"},
                           {&opcodeIsMissing, "does not support"}};
  for (const auto& [probe, cause] : probesAndCauses) {
    SCOPED_TRACE(cause);
    const size_t numberOfFileDescriptorsBefore = numberOfOpenFileDescriptors();
    AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::makeLiburingRing(8, probe),
                                 HasSubstr(cause));
    EXPECT_EQ(numberOfOpenFileDescriptors(), numberOfFileDescriptorsBefore);
  }
  // The probe of the running kernel confirms both operations.
  EXPECT_NO_THROW(ad_utility::makeLiburingRing(8));
}

}  // namespace

#endif  // QLEVER_HAS_IO_URING
