// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"
#include "util/FakeIoUring.h"
#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"
#include "util/IoUringManagerTestHelpers.h"
#include "util/Log.h"
#include "util/PageCacheFirstPolicy.h"
#include "util/PageCacheReadTestHelpers.h"
#include "util/Views.h"
#include "util/io_uring/BatchOfReads.h"
#include "util/io_uring/IoUringPolicy.h"

namespace {

using namespace ::testing;
using namespace ioUringManagerTestHelpers;

// Test helper that builds the file content and the matching batch of reads
// together. `addRead(bytes)` appends `bytes` as the next region of the file,
// registers a read of that region, and remembers `bytes` as the expected
// result.
class SequentialReadScenarioForTesting {
 private:
  std::string content_;
  std::vector<std::string> expected_;
  ReadBatchForTesting batch_;

 public:
  // Append `bytes` as the next region of the file and register a read for it.
  void addRead(std::string_view bytes) {
    batch_.add(content_.size(), bytes.size());  // offset = current end of file
    expected_.emplace_back(bytes);
    content_.append(bytes);
  }

  // The full file content to pass to `makeTempFile`.
  const std::string& content() const { return content_; }

  // Read all reads with `manager` from file `fd`.
  template <typename Manager>
  void readWith(Manager& manager, int fd) {
    batch_.readWith(manager, fd);
  }

  // The bytes actually read (valid once the batch has completed).
  const std::vector<std::string>& results() const { return batch_.result(); }

  // The bytes that we expect to be read, in request order.
  const std::vector<std::string>& expected() const { return expected_; }
};

#ifdef QLEVER_HAS_IO_URING
// Return `true` iff io_uring actually works at runtime. Even when compiled
// in, the `io_uring_setup` syscall can be blocked (for example, by the
// default seccomp profile of Docker, which also applies to the `RUN` steps
// of the CI image build that execute this test suite). The result is probed
// once and cached.
bool ioUringAvailableAtRuntime() {
  static const bool available = []() {
    bool preferIoUring = true;
    [[maybe_unused]] auto manager = ad_utility::makeBatchManager(preferIoUring);
    return preferIoUring;
  }();
  return available;
}
#endif

// Typed test fixture: each `TypeParam` is a `BatchManager` instantiated with a
// concrete I/O policy, with and without the page-cache fast path
// (`PageCacheFirstPolicy`). When io_uring is present the tests run against both
// the `IoUringPolicy` and the `SyncIoPolicy` backends. If io_uring is not
// present (at compile time, or blocked at runtime, see
// `ioUringAvailableAtRuntime`), the tests run against `SyncIoPolicy` only.
template <typename T>
class IoUringManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef QLEVER_HAS_IO_URING
    if constexpr (std::is_same_v<
                      T, ad_utility::BatchManager<ad_utility::IoUringPolicy>> ||
                  std::is_same_v<T, ad_utility::BatchManager<
                                        ad_utility::PageCacheFirstPolicy<
                                            ad_utility::IoUringPolicy>>>) {
      if (!ioUringAvailableAtRuntime()) {
        GTEST_SKIP() << "io_uring is compiled in, but not available at "
                        "runtime (e.g. blocked by seccomp inside Docker)";
      }
    }
#endif
  }
};

#ifdef QLEVER_HAS_IO_URING
using ManagerTypes = ::testing::Types<
    ad_utility::BatchManager<ad_utility::IoUringPolicy>,
    ad_utility::BatchManager<ad_utility::SyncIoPolicy>,
    ad_utility::BatchManager<
        ad_utility::PageCacheFirstPolicy<ad_utility::IoUringPolicy>>,
    ad_utility::BatchManager<
        ad_utility::PageCacheFirstPolicy<ad_utility::SyncIoPolicy>>>;
#else
using ManagerTypes =
    ::testing::Types<ad_utility::BatchManager<ad_utility::SyncIoPolicy>,
                     ad_utility::BatchManager<ad_utility::PageCacheFirstPolicy<
                         ad_utility::SyncIoPolicy>>>;
#endif

TYPED_TEST_SUITE(IoUringManagerTest, ManagerTypes);

// The basic happy path: a single batch of reads is read.
// Each read result lands with the correct bytes in its own target buffer.
// The non-sequential offsets in `fileOffsets` (8, 0, 12) also cover
// order-independence of reads within a batch.
TYPED_TEST(IoUringManagerTest, SingleBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});

  TypeParam manager{64};
  batch.readWith(manager, fd);

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
}

// `readBatch` requires the per-request spans (byte counts, offsets, target
// buffers) to all have the same length. Passing spans of differing lengths is
// a precondition violation and must throw before any I/O is issued.
TYPED_TEST(IoUringManagerTest, mismatchedSpanLengthsThrow) {
  auto [tmp, fd] = makeTempFile("DOESNT_MATTER");

  TypeParam manager{64};

  std::string buffer(4, '\0');
  std::vector<size_t> numBytes{4};            // length 1
  std::vector<uint64_t> fileOffsets{0, 4};    // length 2 -> mismatch
  std::vector<char*> buffers{buffer.data()};  // length 1

  AD_EXPECT_THROW_WITH_MESSAGE(
      manager.readBatch(fd, numBytes, fileOffsets, buffers),
      HasSubstr("must have the same length"));
}

// A single `readBatch` call requesting more reads (400) than the submission
// ring buffer can hold at once (64) forces the manager to submit the SQEs in
// successive rounds: it fills the ring buffer with as many SQEs as fit, reaps
// their CQEs, then refills with the next round of SQEs until all 400 reads
// complete. Verify every read still lands in the correct buffer. Each 4-byte
// chunk holds a distinct repeated character, so a misrouted read is detected as
// a mismatch.
TYPED_TEST(IoUringManagerTest, BatchLargerThanRing) {
  constexpr size_t N = 400;
  constexpr size_t CHUNKSIZE = 4;

  // Each chunk holds a distinct repeated character, so a read landing in the
  // wrong buffer is detected as a mismatch. The chunk bytes are written to the
  // file and reused as the expected result, so the pattern isn't duplicated.
  SequentialReadScenarioForTesting scenario;
  for (size_t i = 0; i < N; ++i) {
    scenario.addRead(std::string(CHUNKSIZE, static_cast<char>('A' + (i % 26))));
  }

  auto [tmp, fd] = makeTempFile(scenario.content());
  TypeParam manager{64};
  scenario.readWith(manager, fd);

  EXPECT_THAT(scenario.results(),
              ::testing::ElementsAreArray(scenario.expected()));
}

// A batch much larger than the ring completes with every read in its own
// buffer. Ring 4 is smaller than a `REAP_WAVE`, so the ring-full wait is
// clamped to the outstanding reads; ring 8 is exactly one wave; with ring 256,
// more than 64 CQEs can be ready at once, so a wave is reaped in several
// chunks.
TYPED_TEST(IoUringManagerTest, BatchMuchLargerThanRing) {
  const std::pair<unsigned, size_t> ringSizeAndNumReads[] = {
      {4, 80}, {8, 80}, {256, 600}};
  for (auto [ringSize, numReads] : ringSizeAndNumReads) {
    SequentialReadScenarioForTesting scenario;
    for (size_t i = 0; i < numReads; ++i) {
      // Distinct chunks, so that a completion in the wrong buffer is detected.
      scenario.addRead(absl::StrFormat("%04zu", i));
    }
    auto [tmp, fd] = makeTempFile(scenario.content());
    TypeParam manager{ringSize};
    // All reads are page-cache hits; send them through the decorated policy.
    manager.setPageCacheFastPath(false);
    scenario.readWith(manager, fd);
    EXPECT_THAT(scenario.results(),
                ::testing::ElementsAreArray(scenario.expected()))
        << "ring size " << ringSize;
  }
}

// Many small batches (1, 3 or 5 reads each), read one after the other on the
// same manager, all see the correct bytes. Batch sizes that are not multiples
// of the reap wave (1, 3, 5, ...) cover partial waves within a batch.
TYPED_TEST(IoUringManagerTest, ManySmallBatches) {
  constexpr size_t M = 12;
  std::string fileContent;
  std::vector<std::vector<std::string>> expected(M);
  std::vector<ReadBatchForTesting> batches(M);
  size_t numChunks = 0;
  for (size_t i = 0; i < M; ++i) {
    const size_t numReads = 2 * (i % 3) + 1;
    for (size_t j = 0; j < numReads; ++j) {
      // Distinct chunks, so that a completion in the wrong buffer is detected.
      std::string chunk = absl::StrFormat("%03zu", numChunks++);
      batches[i].add(fileContent.size(), chunk.size());
      fileContent.append(chunk);
      expected[i].push_back(std::move(chunk));
    }
  }
  auto [tmp, fd] = makeTempFile(fileContent);

  TypeParam manager{64};
  // All reads are page-cache hits; send them through the decorated policy.
  manager.setPageCacheFastPath(false);
  for (auto& batch : batches) {
    batch.readWith(manager, fd);
  }
  for (size_t i = 0; i < M; ++i) {
    EXPECT_THAT(batches[i].result(), ::testing::ElementsAreArray(expected[i]))
        << "mismatch at batch " << i;
  }
}

// A failed batch does not affect the batches that the same manager reads
// before and after it.
TYPED_TEST(IoUringManagerTest, ErrorInOneBatchKeepsOtherBatchesConsistent) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");  // 12 bytes

  TypeParam manager{64};
  // The reads are page-cache hits; send them through the decorated policy.
  manager.setPageCacheFastPath(false);
  ReadBatchForTesting before;
  before.add({{0, 4}, {4, 4}, {8, 4}});
  ReadBatchForTesting bad;
  bad.add(8, 16);  // past EOF: short read
  ReadBatchForTesting after;
  after.add({{8, 4}, {0, 4}});

  before.readWith(manager, fd);
  AD_EXPECT_THROW_WITH_MESSAGE(bad.readWith(manager, fd),
                               HasSubstr("read fewer bytes than requested"));
  after.readWith(manager, fd);
  EXPECT_THAT(before.result(), ::testing::ElementsAre("AAAA", "BBBB", "CCCC"));
  EXPECT_THAT(after.result(), ::testing::ElementsAre("CCCC", "AAAA"));
}

// Request more bytes than the file contains, i.e. read past EOF. A read that
// cannot be fully satisfied is a short read, which both policies must report as
// an error (`std::runtime_error`).
TYPED_TEST(IoUringManagerTest, ReadPastEofThrows) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");  // 8 bytes

  TypeParam manager{64};
  ReadBatchForTesting batch;
  batch.add(0, 16);  // request more than the 8 available

  AD_EXPECT_THROW_WITH_MESSAGE(batch.readWith(manager, fd),
                               HasSubstr("read fewer bytes than requested"));
}

// Two reads, each larger than a memory page (4 KiB on Linux), exercise
// multi-page `pread`/SQEs -- including one at a large non-zero offset, which is
// the realistic size for reading a compressed vocabulary block. Each half holds
// a distinct non-repeating pattern (stepped by a prime so it does not align to
// a power-of-two boundary), so a truncated or misaligned read is detected, not
// just a wrong length.
TYPED_TEST(IoUringManagerTest, LargeReads) {
  constexpr size_t kHalf = 32 * 1024;  // 32 KiB, well past a 4 KiB page
  std::string first(kHalf, '\0');
  std::string second(kHalf, '\0');
  for (size_t i = 0; i < kHalf; ++i) {
    first[i] = static_cast<char>(i % 251);
    second[i] = static_cast<char>((i + 100) % 251);
  }

  SequentialReadScenarioForTesting scenario;
  scenario.addRead(first);   // read at offset 0
  scenario.addRead(second);  // read at offset 32 KiB
  auto [tmp, fd] = makeTempFile(scenario.content());

  TypeParam manager{64};
  scenario.readWith(manager, fd);

  EXPECT_THAT(scenario.results(),
              ::testing::ElementsAreArray(scenario.expected()));
}

// Heterogeneous read sizes within a batch.
TYPED_TEST(IoUringManagerTest, differingReadSizes) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 1}, {0, 2}, {12, 3}});

  TypeParam manager{64};
  batch.readWith(manager, fd);

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("C", "AA", "DDD"));
}

// Zero-length reads interleaved with non-zero reads in one batch.
TYPED_TEST(IoUringManagerTest, zeroLengthReadsWithNonZeroLengthReads) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 1}, {0, 0}, {12, 3}});

  TypeParam manager{64};
  batch.readWith(manager, fd);

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("C", "", "DDD"));
}

#ifdef QLEVER_HAS_IO_URING
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
        const size_t numBytes =
            std::uniform_int_distribution<size_t>{1, 16}(random);
        const size_t offset = std::uniform_int_distribution<size_t>{
            0, file.size() - numBytes}(random);
        batch.add(offset, numBytes);
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
// batch and every later one with `pread` (the fake ring would fail every
// further submit).
TEST(IoUringPolicy, aFatalSubmitRetiresTheRing) {
  using ad_utility::testing::FakeIoUring;
  ChunkedFile chunks{3};
  auto [tmp, fd] = makeTempFile(chunks.content_);
  FakeIoUring::Faults faults;
  faults.scriptedSubmits = {-EBADF, -EBADF, -EBADF};
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
  EXPECT_THAT(log, HasSubstr("io_uring_submit failed in IoUringPolicy"));
  EXPECT_THAT(log, HasSubstr("reads with pread from now on"));
  // Logged once: the ring was torn down after the first batch.
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
// and `makeBatchManager` falls back to synchronous reads.
TEST(LiburingRing, aKernelWithoutTheNeededOperationsIsNotUsed) {
  SKIP_WITHOUT_IO_URING();
  using ad_utility::detail::opcodeProbe;
  const auto original = opcodeProbe();
  absl::Cleanup restore{[original]() { opcodeProbe() = original; }};
  const std::pair<ad_utility::detail::OpcodeProbe, std::string_view>
      probesAndCauses[] = {{&probeFails, "the probe"},
                           {&opcodeIsMissing, "does not support"}};
  for (const auto& [probe, cause] : probesAndCauses) {
    SCOPED_TRACE(cause);
    opcodeProbe() = probe;
    const size_t numberOfFileDescriptorsBefore = numberOfOpenFileDescriptors();
    AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::makeLiburingRing(8),
                                 HasSubstr(cause));
    EXPECT_EQ(numberOfOpenFileDescriptors(), numberOfFileDescriptorsBefore);

    bool preferIoUring = true;
    auto manager = ad_utility::makeBatchManager(preferIoUring);
    EXPECT_FALSE(preferIoUring);
    auto [tmp, fd] = makeTempFile("AAAABBBB");
    ReadBatchForTesting batch;
    batch.add({{4, 4}, {0, 4}});
    manager->setPageCacheFastPath(false);
    batch.readWith(*manager, fd);
    EXPECT_THAT(batch.result(), ElementsAre("BBBB", "AAAA"));
  }
}
#endif

// A `BatchOfReads` rejects spans of different lengths, so that no policy can
// be handed them.
TEST(BatchOfReads, rejectsSpansOfDifferentLengths) {
  std::vector<size_t> numberOfBytes{4, 4};
  std::vector<uint64_t> offsets{0};
  std::string buffer(8, '-');
  std::vector<char*> buffers{buffer.data(), buffer.data() + 4};
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::BatchOfReads(numberOfBytes, offsets, buffers),
      HasSubstr("must have the same length"));
  std::vector<uint64_t> twoOffsets{0, 4};
  const ad_utility::BatchOfReads reads{numberOfBytes, twoOffsets, buffers};
  EXPECT_EQ(reads.size(), 2u);
}

// Check that the `manager` (as returned by `makeBatchManager`, see the tests
// below) performs correct reads via the type-erased `BatchManagerBase`
// interface.
void expectManagerWorks(ad_utility::BatchManagerBase& manager) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");
  ReadBatchForTesting batch;
  batch.add({{4, 4}, {0, 4}});
  batch.readWith(manager, fd);
  EXPECT_THAT(batch.result(), ::testing::ElementsAre("BBBB", "AAAA"));
}

// True if `manager` is a `BatchManager` for `Policy`, decorated with
// `PageCacheFirstPolicy` exactly where the page-cache fast path is supported.
template <typename Policy>
bool usesPolicy(ad_utility::BatchManagerBase& manager) {
  if (ad_utility::pageCacheFastPathIsSupported()) {
    return dynamic_cast<ad_utility::BatchManager<
               ad_utility::PageCacheFirstPolicy<Policy>>*>(&manager) != nullptr;
  }
  return dynamic_cast<ad_utility::BatchManager<Policy>*>(&manager) != nullptr;
}

// With `preferIoUring == false`, `makeBatchManager` must return a
// `SyncIoPolicy`-backed manager without probing io_uring, and leave the flag
// `false`.
TEST(MakeBatchManager, syncBackendWhenIoUringNotPreferred) {
  bool preferIoUring = false;
  auto manager = ad_utility::makeBatchManager(preferIoUring);
  ASSERT_NE(manager, nullptr);
  EXPECT_FALSE(preferIoUring);
  EXPECT_TRUE(usesPolicy<ad_utility::SyncIoPolicy>(*manager));
  expectManagerWorks(*manager);
}

// With `preferIoUring == true`, the backend depends on the runtime
// environment: if io_uring is compiled in and its setup succeeds, an
// `IoUringPolicy`-backed manager is returned and the flag stays `true`.
// Otherwise (io_uring not compiled in, or its setup fails at runtime, e.g.
// blocked by seccomp inside Docker), the flag is set to `false` and a
// `SyncIoPolicy`-backed manager is returned. Either way, the returned
// manager must work.
TEST(MakeBatchManager, backendMatchesFlagWhenIoUringPreferred) {
  bool preferIoUring = true;
  auto manager = ad_utility::makeBatchManager(preferIoUring);
  ASSERT_NE(manager, nullptr);
#ifdef QLEVER_HAS_IO_URING
  if (preferIoUring) {
    EXPECT_TRUE(usesPolicy<ad_utility::IoUringPolicy>(*manager));
  } else {
    EXPECT_TRUE(usesPolicy<ad_utility::SyncIoPolicy>(*manager));
  }
#else
  EXPECT_FALSE(preferIoUring);
  EXPECT_TRUE(usesPolicy<ad_utility::SyncIoPolicy>(*manager));
#endif
  expectManagerWorks(*manager);
}

// The manager from `makeBatchManager` works with the page-cache fast path
// switched on and off.
TEST(MakeBatchManager, worksWithTheFastPathOnAndOff) {
  bool preferIoUring = false;
  auto manager = ad_utility::makeBatchManager(preferIoUring);
  expectManagerWorks(*manager);
  manager->setPageCacheFastPath(false);
  expectManagerWorks(*manager);
}
}  // namespace
