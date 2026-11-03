// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

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
#include <initializer_list>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"
#include "util/IoUringManagerTestHelpers.h"
#include "util/Log.h"
#include "util/PageCacheReadTestHelpers.h"
#include "util/Views.h"
#include "util/io_uring/BatchOfReads.h"

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

  TypeParam manager(64);
  batch.readWith(manager, fd);

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
}

// `readBatch` requires the per-request spans (byte counts, offsets, target
// buffers) to all have the same length. Passing spans of differing lengths is
// a precondition violation and must throw before any I/O is issued.
TYPED_TEST(IoUringManagerTest, mismatchedSpanLengthsThrow) {
  auto [tmp, fd] = makeTempFile("DOESNT_MATTER");

  TypeParam manager(64);

  std::string buffer(4, '\0');
  std::vector<size_t> numBytes{4};            // length 1
  std::vector<uint64_t> fileOffsets{0, 4};    // length 2 -> mismatch
  std::vector<char*> buffers{buffer.data()};  // length 1

  AD_EXPECT_THROW_WITH_MESSAGE(
      manager.readBatch(fd, numBytes, fileOffsets, buffers),
      HasSubstr("must have the same length"));
}

// MultipleBatchesSequential: 3 batches read one after the other.
TYPED_TEST(IoUringManagerTest, MultipleBatchesSequential) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDDEEEEFFFFGGGG");

  TypeParam manager(64);

  auto readAndCheck = [&](uint64_t offset, size_t numBytes,
                          std::string_view expected) {
    ReadBatchForTesting batch;
    batch.add(offset, numBytes);
    batch.readWith(manager, fd);
    EXPECT_THAT(batch.result(), ::testing::ElementsAre(expected));
  };

  readAndCheck(0, 4, "AAAA");
  readAndCheck(4, 4, "BBBB");
  readAndCheck(8, 4, "CCCC");
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
  TypeParam manager(64);
  scenario.readWith(manager, fd);

  EXPECT_THAT(scenario.results(),
              ::testing::ElementsAreArray(scenario.expected()));
}

// A batch much larger than the ring completes with every read in its own
// buffer. For `IoUringPolicy`: ring 4 is smaller than a `REAP_WAVE`, so the
// ring-full wait is clamped to the outstanding reads; ring 8 is exactly one
// wave; with ring 256, more than `IoUringInterface::MAX_COMPLETIONS_PER_POP`
// (64) CQEs can be ready at once, so a wave is reaped in several chunks. The
// other policies have no ring and read synchronously.
TYPED_TEST(IoUringManagerTest, BatchMuchLargerThanRing) {
  constexpr std::array<std::pair<unsigned, size_t>, 3> ringSizeAndNumberOfReads{
      {{4, 80}, {8, 80}, {256, 600}}};
  for (const auto& [ringSize, numberOfReads] : ringSizeAndNumberOfReads) {
    SequentialReadScenarioForTesting scenario;
    for (size_t i : ql::views::iota(size_t{0}, numberOfReads)) {
      // Distinct chunks, so that a completion in the wrong buffer is detected.
      scenario.addRead(absl::StrFormat("%04d", i));
    }
    auto [tmp, fd] = makeTempFile(scenario.content());
    TypeParam manager(ringSize);
    // The reads would be page-cache hits. Switch the fast path off (a no-op for
    // the policies without it) so that they go through the wrapped policy, the
    // ring for `IoUringPolicy`.
    manager.setPageCacheFastPath(false);
    scenario.readWith(manager, fd);
    EXPECT_THAT(scenario.results(),
                ::testing::ElementsAreArray(scenario.expected()))
        << "ring size " << ringSize;
  }
}

// Many small batches (1, 3 or 5 reads each), read one after the other on the
// same manager, all see the correct bytes. The batch sizes 1, 3 and 5 are not
// multiples of the reap wave (8), so they end in partial waves.
TYPED_TEST(IoUringManagerTest, ManySmallBatches) {
  constexpr size_t M = 12;
  std::string fileContent;
  std::vector<std::vector<std::string>> expected(M);
  std::vector<ReadBatchForTesting> batches(M);
  size_t numberOfChunks = 0;
  for (auto&& [i, batchAndExpected] :
       ::ranges::views::enumerate(::ranges::views::zip(batches, expected))) {
    auto& [batch, expectedOfBatch] = batchAndExpected;
    const size_t numberOfReads = 2 * (static_cast<size_t>(i) % 3) + 1;
    for ([[maybe_unused]] size_t j :
         ql::views::iota(size_t{0}, numberOfReads)) {
      // Distinct chunks, so that a completion in the wrong buffer is detected.
      std::string chunk = absl::StrFormat("%03d", numberOfChunks++);
      batch.add(fileContent.size(), chunk.size());
      fileContent.append(chunk);
      expectedOfBatch.push_back(std::move(chunk));
    }
  }
  auto [tmp, fd] = makeTempFile(fileContent);

  TypeParam manager(64);
  // The reads would be page-cache hits. Switch the fast path off (a no-op for
  // the policies without it) so that they go through the wrapped policy, the
  // ring for `IoUringPolicy`.
  manager.setPageCacheFastPath(false);
  for (auto& batch : batches) {
    batch.readWith(manager, fd);
  }
  for (const auto& [i, batchAndExpected] :
       ::ranges::views::enumerate(::ranges::views::zip(batches, expected))) {
    const auto& [batch, expectedOfBatch] = batchAndExpected;
    EXPECT_THAT(batch.result(), ::testing::ElementsAreArray(expectedOfBatch))
        << "mismatch at batch " << i;
  }
}

// A failed batch does not affect the batches that the same manager reads
// before and after it.
TYPED_TEST(IoUringManagerTest, ErrorInOneBatchKeepsOtherBatchesConsistent) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");  // 12 bytes
  ReadBatchForTesting before;
  before.add({{0, 4}, {4, 4}, {8, 4}});
  ReadBatchForTesting bad;
  bad.add({{0, 4}, {8, 16}});  // the second read is past EOF: short read
  ReadBatchForTesting after;
  after.add({{8, 4}, {0, 4}});
  TypeParam manager(64);
  // The reads would be page-cache hits. Switch the fast path off (a no-op for
  // the policies without it) so that they go through the wrapped policy, the
  // ring for `IoUringPolicy`.
  manager.setPageCacheFastPath(false);

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

  TypeParam manager(64);
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

  TypeParam manager(64);
  scenario.readWith(manager, fd);

  EXPECT_THAT(scenario.results(),
              ::testing::ElementsAreArray(scenario.expected()));
}

// Heterogeneous read sizes within a batch.
TYPED_TEST(IoUringManagerTest, differingReadSizes) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 1}, {0, 2}, {12, 3}});

  TypeParam manager(64);
  batch.readWith(manager, fd);

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("C", "AA", "DDD"));
}

// Zero-length reads interleaved with non-zero reads in one batch.
TYPED_TEST(IoUringManagerTest, zeroLengthReadsWithNonZeroLengthReads) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 1}, {0, 0}, {12, 3}});

  TypeParam manager(64);
  batch.readWith(manager, fd);

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("C", "", "DDD"));
}

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
