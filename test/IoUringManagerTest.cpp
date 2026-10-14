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
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
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

#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"
#include "util/Log.h"
#include "util/PageCacheReadTestHelpers.h"
#include "util/VocabBlockCache.h"

namespace {

using namespace ::testing;

// Writes `content` to a temporary file and keeps it open for reading.
// `fd()` exposes the file descriptor; the file is removed from disk on
// destruction. Use `makeTempFile` below to get the file and its fd in one step.
class TempFile {
 public:
  explicit TempFile(std::string_view content)
      : path_{absl::StrCat(gtestCurrentTestName(), ".tmp")} {
    // Open for reading and writing (`"w+b"`): the tests read from this file's
    // `fd()` via `pread`/io_uring.
    readFile_ = ad_utility::File{path_, "w+b"};
    readFile_.write(content.data(), content.size());
    // Flush the libc stream buffer so the written bytes reach the kernel and
    // are visible to the reads the tests issue on `fd()`.
    readFile_.flush();
  }
  // Movable (so a factory can return it by value). The moved-from object's
  // `path_` is cleared so that only the live object removes the file.
  TempFile(TempFile&& other) noexcept
      : path_{std::move(other.path_)}, readFile_{std::move(other.readFile_)} {
    other.path_.clear();
  }
  // Close the descriptor and remove the file from disk.
  ~TempFile() {
    if (!path_.empty()) {
      readFile_.close();
      std::remove(path_.c_str());
    }
  }
  int fd() const { return readFile_.fd(); }

 private:
  std::string path_;
  ad_utility::File readFile_;
};

// Create a temporary file holding `content` and return it together with its
// file descriptor. The returned `tmp` must be kept alive for as long as `fd` is
// used.
std::pair<TempFile, int> makeTempFile(std::string_view content) {
  TempFile tmp{content};
  int fd = tmp.fd();
  return {std::move(tmp), fd};
}

// Test helper that accumulates a batch of read requests and owns their target
// buffers. After the batch has completed, `result()` returns the bytes that
// each read produced, in request order.
class ReadBatchForTesting {
 public:
  // Add a read of `numBytes` bytes at `offset`; returns the read's index.
  size_t add(uint64_t offset, size_t numBytes) {
    offsets_.push_back(offset);
    numBytes_.push_back(numBytes);
    targetBuffers_.emplace_back(numBytes, '\0');
    return targetBuffers_.size() - 1;
  }

  // Add several reads at once from a range of `(offset, numBytes)` pairs.
  void add(ql::span<const std::pair<uint64_t, size_t>> reads) {
    for (const auto& [offset, numBytes] : reads) {
      add(offset, numBytes);
    }
  }

  void add(std::initializer_list<std::pair<uint64_t, size_t>> reads) {
    add(ql::span<const std::pair<uint64_t, size_t>>{reads.begin(),
                                                    reads.size()});
  }

  // Submit all accumulated reads to `manager` for file `fd`, carried out as
  // specified by `options`; returns the handle.
  template <typename Manager>
  typename Manager::BatchHandle submitTo(
      Manager& manager, int fd,
      const ad_utility::BatchReadOptions& options = {}) {
    // Build the buffer pointers here, after all buffers have been added, so the
    // addresses are stable (no further `add` will reallocate `targetBuffers_`).
    // `addBatch` copies each address into its read request, so this temporary
    // vector need not outlive the call.
    std::vector<char*> pointers = ::ranges::to_vector(
        targetBuffers_ | ql::views::transform([](std::string& buffer) {
          return buffer.data();
        }));
    return manager.addBatch(fd, numBytes_, offsets_, pointers, options);
  }

  // Run `ad_utility::readLeadingPageCacheHits` on all accumulated reads.
  size_t readLeadingPageCacheHits(int fd) {
    std::vector<char*> pointers = ::ranges::to_vector(
        targetBuffers_ | ql::views::transform([](std::string& buffer) {
          return buffer.data();
        }));
    return ad_utility::readLeadingPageCacheHits(fd, numBytes_, offsets_,
                                                pointers);
  }

  // The bytes read by each read, in request order (valid once the batch has
  // completed).
  const std::vector<std::string>& result() const { return targetBuffers_; }

 private:
  std::vector<size_t> numBytes_;
  std::vector<uint64_t> offsets_;
  std::vector<std::string> targetBuffers_;
};

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

  // Submit all reads to `manager` for file `fd`; returns the handle.
  template <typename Manager>
  typename Manager::BatchHandle submitTo(Manager& manager, int fd) {
    return batch_.submitTo(manager, fd);
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
// concrete I/O policy. When io_uring is present the tests run against both the
// `IoUringPolicy` and the `SyncIoPolicy` backends. If io_uring is not present
// (at compile time, or blocked at runtime, see `ioUringAvailableAtRuntime`),
// the tests run against `SyncIoPolicy` only.
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

// The basic happy path: a single batch of reads is submitted and waited on.
// Each read result lands with the correct bytes in its own target buffer.
// The non-sequential offsets in `fileOffsets` (8, 0, 12) also cover
// order-independence of reads within a batch.
TYPED_TEST(IoUringManagerTest, SingleBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});

  TypeParam manager(64);
  manager.wait(batch.submitTo(manager, fd));

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
}

// `addBatch` requires the per-request spans (byte counts, offsets, target
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
      std::ignore = manager.addBatch(fd, numBytes, fileOffsets, buffers),
      HasSubstr("spans must have same length"));
}

// MultipleBatchesSequential: 3 batches submitted and waited in order.
TYPED_TEST(IoUringManagerTest, MultipleBatchesSequential) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDDEEEEFFFFGGGG");

  TypeParam manager(64);

  auto makeAndWait = [&](uint64_t offset, size_t numBytes,
                         std::string_view expected) {
    ReadBatchForTesting batch;
    batch.add(offset, numBytes);
    manager.wait(batch.submitTo(manager, fd));
    EXPECT_THAT(batch.result(), ::testing::ElementsAre(expected));
  };

  makeAndWait(0, 4, "AAAA");
  makeAndWait(4, 4, "BBBB");
  makeAndWait(8, 4, "CCCC");
}

// Verify that batch handles can be waited on out of submission order: batches A
// and B are submitted, then their batch handles are `wait()`ed in reverse
// (batch B's handle before batch A's handle). Each batch handle must still
// resolve its own batch's read correctly, so the wait order is independent of
// the submission order.
TYPED_TEST(IoUringManagerTest, WaitOutOfOrder) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");

  TypeParam manager(64);

  ReadBatchForTesting batchA;
  batchA.add(0, 4);
  ReadBatchForTesting batchB;
  batchB.add(4, 4);

  auto handleA = batchA.submitTo(manager, fd);
  auto handleB = batchB.submitTo(manager, fd);

  manager.wait(handleB);
  manager.wait(handleA);

  EXPECT_THAT(batchA.result(), ::testing::ElementsAre("AAAA"));
  EXPECT_THAT(batchB.result(), ::testing::ElementsAre("BBBB"));
}

// A single `addBatch` call requesting more reads (400) than the submission ring
// buffer can hold at once (64) forces the manager to submit the SQEs in
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
  manager.wait(scenario.submitTo(manager, fd));

  EXPECT_THAT(scenario.results(),
              ::testing::ElementsAreArray(scenario.expected()));
}

// Verify that many independent `addBatch` calls can be outstanding (submitted
// to the kernel but not yet waited on) at once, and that the manager tracks
// each batch's completion correctly. M batches of one read each are submitted
// before any `wait`. Since the kernel posts completions in arbitrary order,
// the manager must map each one back to its issuing batch.
TYPED_TEST(IoUringManagerTest, MultipleSmallBatchesPipelined) {
  // M 4-byte chunks; chunk i is filled with the character 'A'+i. Since M <= 26
  // the chunks have distinct contents, so a read whose data lands in the wrong
  // buffer is detected as a mismatch below.
  constexpr size_t M = 20;
  std::string fileContent;
  std::vector<std::string> expected;
  std::vector<ReadBatchForTesting> batches(M);
  for (size_t i = 0; i < M; ++i) {
    std::string chunk(4, static_cast<char>('A' + i));
    batches[i].add(fileContent.size(), chunk.size());
    fileContent.append(chunk);
    expected.push_back(std::move(chunk));
  }
  auto [tmp, fd] = makeTempFile(fileContent);

  TypeParam manager(64);

  // Submit all M batches (one read each) before waiting on any of them, so they
  // are outstanding concurrently.
  std::vector<typename TypeParam::BatchHandle> batchHandles(M);
  for (size_t i = 0; i < M; ++i) {
    batchHandles[i] = batches[i].submitTo(manager, fd);
  }
  // Only now wait on each handle. Each wait must block until that batch's own
  // read has completed, regardless of the order the kernel posted completions.
  for (size_t i = 0; i < M; ++i) {
    manager.wait(batchHandles[i]);
  }

  // Each batch's read must have landed in its own buffer; a completion routed
  // to the wrong buffer shows up as a mismatch here.
  for (size_t i = 0; i < M; ++i) {
    EXPECT_THAT(batches[i].result(), ::testing::ElementsAre(expected[i]))
        << "mismatch at batch " << i;
  }
}

// Request more bytes than the file contains, i.e. read past EOF. A read that
// cannot be fully satisfied is a short read, which both policies must report as
// an error (`std::runtime_error`). `SyncIoPolicy` throws in `addBatch`,
// `IoUringPolicy` in `wait`.
TYPED_TEST(IoUringManagerTest, ReadPastEofThrows) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");  // 8 bytes

  TypeParam manager(64);
  ReadBatchForTesting batch;
  batch.add(0, 16);  // request more than the 8 available

  AD_EXPECT_THROW_WITH_MESSAGE(manager.wait(batch.submitTo(manager, fd)),
                               HasSubstr("read fewer bytes than requested"));
}

namespace {
// The block cache counters, to measure one test step by their differences.
struct BlockCacheCounts {
  uint64_t hits_, inFlightHits_, misses_, inserts_;
  static BlockCacheCounts now() {
    const auto& c = ad_utility::vocab::vocabBlockCacheCounters;
    return {c.hits_.load(), c.inFlightHits_.load(), c.misses_.load(),
            c.inserts_.load()};
  }
  BlockCacheCounts operator-(const BlockCacheCounts& other) const {
    return {hits_ - other.hits_, inFlightHits_ - other.inFlightHits_,
            misses_ - other.misses_, inserts_ - other.inserts_};
  }
};

// Whether batches submitted to `Manager` reach the io_uring policy (and thus
// the block cache): the synchronous policy ignores `BatchReadOptions`, and
// the page-cache-first wrapper may serve the reads before the ring sees them.
// The bytes are the same with every backend; only the counter assertions
// below need this distinction.
#ifdef QLEVER_HAS_IO_URING
template <typename Manager>
inline constexpr bool reachesBlockCache =
    std::is_same_v<Manager,
                   ad_utility::BatchManager<ad_utility::IoUringPolicy>>;
#else
template <typename Manager>
inline constexpr bool reachesBlockCache = false;
#endif

// Read `reads` from `fd` in one batch with `options` and check the bytes.
template <typename Manager>
void readAndCheck(Manager& manager, int fd, const std::string& content,
                  std::vector<std::pair<uint64_t, size_t>> reads,
                  const ad_utility::BatchReadOptions& options) {
  ReadBatchForTesting batch;
  batch.add(reads);
  manager.wait(batch.submitTo(manager, fd, options));
  std::vector<std::string> expected;
  for (const auto& [offset, numBytes] : reads) {
    expected.push_back(content.substr(offset, numBytes));
  }
  EXPECT_THAT(batch.result(), ::testing::ElementsAreArray(expected));
}
}  // namespace

// With a block cache, the blocks read via `O_DIRECT` are kept, and a later
// batch that requests other bytes of the same blocks (partial-block hits) is
// served from the cache without reading them again. Only the io_uring policy
// uses the cache; the bytes are the same with both policies.
TYPED_TEST(IoUringManagerTest, DirectIoBlockCacheServesPartialBlockHits) {
  constexpr size_t block = ad_utility::export_prototypes::kDirectIoBlockSize;
  std::string content;
  for (size_t i = 0; i < 4 * block; ++i) {
    content.push_back(static_cast<char>('a' + (i * 11) % 26));
  }
  auto [tmp, fd] = makeTempFile(content);
  ad_utility::export_prototypes::DirectIoFile directFile;
  try {
    directFile.open(absl::StrCat(gtestCurrentTestName(), ".tmp"), true);
  } catch (const std::exception& e) {
    GTEST_SKIP() << "O_DIRECT is not supported here: " << e.what();
  }
  TypeParam manager(16);
  ad_utility::BatchReadOptions options;
  options.useRegisteredBuffers = true;
  options.directIoFd = directFile.fd();
  // A capacity that no other test uses, so that this thread's cache starts
  // empty.
  options.blockCacheNumBlocks = 17;
  constexpr bool usesCache = reachesBlockCache<TypeParam>;

  auto before = BlockCacheCounts::now();
  readAndCheck(manager, fd, content, {{10, 5}, {block + 7, 20}}, options);
  auto first = BlockCacheCounts::now() - before;
  before = BlockCacheCounts::now();
  // Other bytes of blocks 0 and 1 (partial-block hits) and block 2 (a miss).
  readAndCheck(manager, fd, content,
               {{100, 30}, {block + 2000, 50}, {2 * block + 5, 10}}, options);
  auto second = BlockCacheCounts::now() - before;
  if (usesCache) {
    EXPECT_EQ(first.hits_, 0u);
    EXPECT_EQ(first.misses_, 2u);
    EXPECT_EQ(first.inserts_, 2u);
    EXPECT_EQ(second.hits_, 2u);
    EXPECT_EQ(second.misses_, 1u);
    EXPECT_EQ(second.inserts_, 1u);
  } else {
    EXPECT_EQ(first.hits_ + first.misses_ + second.hits_ + second.misses_, 0u);
  }
}

// A cache of one block evicts the previous block on every insert, so reading
// block 0, then block 1, then block 0 again misses three times.
TYPED_TEST(IoUringManagerTest, DirectIoBlockCacheEvicts) {
  constexpr size_t block = ad_utility::export_prototypes::kDirectIoBlockSize;
  std::string content(3 * block, 'x');
  for (size_t i = 0; i < content.size(); ++i) {
    content[i] = static_cast<char>('A' + (i * 5) % 26);
  }
  auto [tmp, fd] = makeTempFile(content);
  ad_utility::export_prototypes::DirectIoFile directFile;
  try {
    directFile.open(absl::StrCat(gtestCurrentTestName(), ".tmp"), true);
  } catch (const std::exception& e) {
    GTEST_SKIP() << "O_DIRECT is not supported here: " << e.what();
  }
  TypeParam manager(16);
  ad_utility::BatchReadOptions options;
  options.useRegisteredBuffers = true;
  options.directIoFd = directFile.fd();
  options.blockCacheNumBlocks = 1;
  constexpr bool usesCache = reachesBlockCache<TypeParam>;
  auto before = BlockCacheCounts::now();
  readAndCheck(manager, fd, content, {{3, 4}}, options);
  readAndCheck(manager, fd, content, {{block + 3, 4}}, options);
  readAndCheck(manager, fd, content, {{8, 4}}, options);
  auto delta = BlockCacheCounts::now() - before;
  if (usesCache) {
    EXPECT_EQ(delta.hits_, 0u);
    EXPECT_EQ(delta.misses_, 3u);
    // Block 0 was inserted last, so both requests for it now hit.
    before = BlockCacheCounts::now();
    readAndCheck(manager, fd, content, {{8, 4}, {9, 4}}, options);
    auto again = BlockCacheCounts::now() - before;
    EXPECT_EQ(again.hits_, 2u);
    EXPECT_EQ(again.misses_, 0u);
  }
}

// Within one batch, a request for a block whose read is still in flight (not
// the directly preceding request, so it cannot join the open read) is copied
// from that read's slot instead of reading the block again. This includes the
// short last block of the file, which is never inserted into the cache.
TYPED_TEST(IoUringManagerTest, DirectIoBlockCacheJoinsInFlightReads) {
  constexpr size_t block = ad_utility::export_prototypes::kDirectIoBlockSize;
  std::string content;
  for (size_t i = 0; i < 2 * block + 100; ++i) {
    content.push_back(static_cast<char>('a' + (i * 13) % 26));
  }
  auto [tmp, fd] = makeTempFile(content);
  ad_utility::export_prototypes::DirectIoFile directFile;
  try {
    directFile.open(absl::StrCat(gtestCurrentTestName(), ".tmp"), true);
  } catch (const std::exception& e) {
    GTEST_SKIP() << "O_DIRECT is not supported here: " << e.what();
  }
  TypeParam manager(16);
  ad_utility::BatchReadOptions options;
  options.useRegisteredBuffers = true;
  options.directIoFd = directFile.fd();
  // A capacity that no other test uses, so that this thread's cache starts
  // empty.
  options.blockCacheNumBlocks = 23;
  constexpr bool usesCache = reachesBlockCache<TypeParam>;

  auto before = BlockCacheCounts::now();
  // Blocks 0, 1, 0 (in flight), 1 (joins the open read), 2 (short), 0 (in
  // flight), 2 (joins the open read, within the bytes the short read
  // returns).
  readAndCheck(manager, fd, content,
               {{0, 4},
                {block + 1, 4},
                {8, 4},
                {block + 9, 4},
                {2 * block + 10, 5},
                {100, 7},
                {2 * block + 50, 50}},
               options);
  auto delta = BlockCacheCounts::now() - before;
  if (usesCache) {
    EXPECT_EQ(delta.hits_, 0u);
    EXPECT_EQ(delta.misses_, 3u);
    EXPECT_EQ(delta.inFlightHits_, 2u);
    // The short last block is not cached.
    EXPECT_EQ(delta.inserts_, 2u);
  } else {
    EXPECT_EQ(delta.hits_ + delta.inFlightHits_ + delta.misses_, 0u);
  }
}

// With `directIoBlockSize` = 16 KiB, every `O_DIRECT` read fetches (and the
// cache keeps) the enclosing 16 KiB block, so a later request for another
// 4 KiB block of it hits. Switching back to 4 KiB blocks on the same manager
// registers the arena again with 4 KiB slots.
TYPED_TEST(IoUringManagerTest, DirectIoLargerBlockSize) {
  constexpr size_t smallBlock =
      ad_utility::export_prototypes::kDirectIoBlockSize;
  constexpr size_t largeBlock = 4 * smallBlock;
  std::string content;
  for (size_t i = 0; i < 3 * largeBlock + 100; ++i) {
    content.push_back(static_cast<char>('a' + (i * 17) % 26));
  }
  auto [tmp, fd] = makeTempFile(content);
  ad_utility::export_prototypes::DirectIoFile directFile;
  try {
    directFile.open(absl::StrCat(gtestCurrentTestName(), ".tmp"), true);
  } catch (const std::exception& e) {
    GTEST_SKIP() << "O_DIRECT is not supported here: " << e.what();
  }
  TypeParam manager(16);
  ad_utility::BatchReadOptions options;
  options.useRegisteredBuffers = true;
  options.directIoFd = directFile.fd();
  // A capacity that no other test uses, so that this thread's cache starts
  // empty.
  options.blockCacheNumBlocks = 29;
  options.directIoBlockSize = largeBlock;
  constexpr bool usesCache = reachesBlockCache<TypeParam>;

  auto before = BlockCacheCounts::now();
  readAndCheck(manager, fd, content, {{10, 5}, {largeBlock + 7, 20}}, options);
  auto first = BlockCacheCounts::now() - before;
  before = BlockCacheCounts::now();
  // Other 4 KiB blocks of the two cached 16 KiB blocks, and the short last
  // block of the file (read, but not cached).
  readAndCheck(manager, fd, content,
               {{smallBlock + 3, 10},
                {largeBlock + 3 * smallBlock, 40},
                {3 * largeBlock + 50, 50}},
               options);
  auto second = BlockCacheCounts::now() - before;
  if (usesCache) {
    EXPECT_EQ(first.misses_, 2u);
    EXPECT_EQ(first.inserts_, 2u);
    EXPECT_EQ(second.hits_, 2u);
    EXPECT_EQ(second.misses_, 1u);
    EXPECT_EQ(second.inserts_, 0u);
  } else {
    EXPECT_EQ(first.hits_ + first.misses_ + second.hits_ + second.misses_, 0u);
  }

  // Back to 4 KiB blocks; the request that spans two 4 KiB blocks does not fit
  // into a slot and uses a plain read.
  options.directIoBlockSize = smallBlock;
  readAndCheck(manager, fd, content,
               {{10, 5}, {largeBlock + 7, 20}, {2 * smallBlock - 3, 10}},
               options);
}

// A read that is fully satisfied returns the requested bytes from the requested
// offset.
TEST(ReadFullyOrThrow, FullReadSucceeds) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  std::vector<char> targetBuffer(4);
  ad_utility::SyncIoPolicy::readFullyOrThrow(fd, targetBuffer.data(), 4, 4);
  EXPECT_EQ(std::string(targetBuffer.data(), 4), "BBBB");
}

// An invalid file descriptor makes the underlying `pread` call fail (return
// -1), which must cause a throw.
TEST(ReadFullyOrThrow, PreadFailureThrows) {
  std::vector<char> targetBuffer(4);
  constexpr int invalidFd = -1;
  AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::SyncIoPolicy::readFullyOrThrow(
                                   invalidFd, targetBuffer.data(), 4, 0),
                               HasSubstr("pread failed"));
}

// Requesting more bytes than the file contains (read past EOF) is a short read
// and must throw.
TEST(ReadFullyOrThrow, ShortReadThrows) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");  // 8 bytes
  std::vector<char> targetBuffer(16);
  AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::SyncIoPolicy::readFullyOrThrow(
                                   fd, targetBuffer.data(), 16, 0),
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
  manager.wait(scenario.submitTo(manager, fd));

  EXPECT_THAT(scenario.results(),
              ::testing::ElementsAreArray(scenario.expected()));
}

// Heterogeneous read sizes within a batch.
TYPED_TEST(IoUringManagerTest, differingReadSizes) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 1}, {0, 2}, {12, 3}});

  TypeParam manager(64);
  manager.wait(batch.submitTo(manager, fd));

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("C", "AA", "DDD"));
}

// Zero-length reads interleaved with non-zero reads in one batch.
TYPED_TEST(IoUringManagerTest, zeroLengthReadsWithNonZeroLengthReads) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 1}, {0, 0}, {12, 3}});

  TypeParam manager(64);
  manager.wait(batch.submitTo(manager, fd));

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("C", "", "DDD"));
}

// Dropping a `SyncIoPolicy`-backed manager with reads submitted but never
// waited: the synchronous policy performs all reads eagerly in `submitTo`, so
// by the time the manager is destroyed nothing is in flight, the destructor has
// nothing to drain, and it logs no warning. This is the counterpart to the
// io_uring-specific `dropRunningManager` test below.
TEST(IoUringManagerDrop, dropSyncManagerHasNothingInFlight) {
  using Manager = ad_utility::BatchManager<ad_utility::SyncIoPolicy>;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  // Capture the log so we can assert that the destructor stays silent.
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});
  {
    Manager manager(64);
    batch.submitTo(manager, fd);  // reads happen synchronously here
    // `manager` is destroyed here; nothing is in flight, so no warning.
  }

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
  EXPECT_THAT(logStream.str(), ::testing::IsEmpty());
}

#ifdef QLEVER_HAS_IO_URING
// Drop the manager while reads are still in flight (submitted but never
// waited). `IoUringPolicy`'s destructor drains the outstanding completions
// (and logs a warning) before tearing down the ring, so the kernel is done
// writing into the target buffers. `batch` is declared before `manager`, so its
// buffers outlive the destructor's drain. After the manager is destroyed every
// read has completed, so the results are correct. This draining is specific to
// the asynchronous io_uring backend, so the test is not part of the typed
// suite.
TEST(IoUringManagerDrop, dropRunningManager) {
  if (!ioUringAvailableAtRuntime()) {
    GTEST_SKIP() << "io_uring is compiled in, but not available at runtime "
                    "(e.g. blocked by seccomp inside Docker)";
  }
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
    // `manager` is destroyed here; its destructor drains the in-flight reads.
  }

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
  EXPECT_THAT(logStream.str(), ::testing::HasSubstr("still in flight"));
}
#endif

// Wait on `BatchHandle` for which the read call has already been reaped from
// the completion queue.
TYPED_TEST(IoUringManagerTest, waitOnNonExistingBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});

  TypeParam manager(64);
  auto realHandle = batch.submitTo(manager, fd);

  manager.wait(realHandle);
  // wait again on the same, already waited on handle.
  manager.wait(realHandle);

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
}

// Wait on `BatchHandle` for which no actual batch of read calls has been
// issued.
TYPED_TEST(IoUringManagerTest, fakeHandle) {
  // create a handle for which no request has been queued.
  auto fakeHandle = 999;

  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});

  TypeParam manager(64);
  auto realHandle = batch.submitTo(manager, fd);

  // waiting on fake handle should never block.
  manager.wait(fakeHandle);
  // Wait on the real handle so the reads actually complete before we check
  // them.
  manager.wait(realHandle);

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
}

// A freshly written file is in the page cache, so every read of a batch is
// served synchronously, unless the platform or file system lacks `RWF_NOWAIT`
// (then none is). The served reads carry the correct bytes.
TEST(ReadLeadingPageCacheHits, servesCachedReads) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});
  const size_t numServed = batch.readLeadingPageCacheHits(fd);
  EXPECT_THAT(numServed, ::testing::AnyOf(0u, 3u));
  if (numServed == 3) {
    EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
  }
}

// A read that cannot be fully served (here: past the end of the file) ends the
// served prefix, even if later reads could be served, so the regular read path
// reports its error.
TEST(ReadLeadingPageCacheHits, stopsAtFirstShortRead) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  ReadBatchForTesting batch;
  batch.add({{0, 4}, {4, 8}, {4, 4}});
  EXPECT_LE(batch.readLeadingPageCacheHits(fd), 1u);
}

// An empty batch serves nothing.
TEST(ReadLeadingPageCacheHits, emptyBatch) {
  auto [tmp, fd] = makeTempFile("AAAA");
  EXPECT_EQ(ad_utility::readLeadingPageCacheHits(fd, {}, {}, {}), 0u);
}

// Spans of different lengths violate the precondition and are rejected before
// any read.
TEST(ReadLeadingPageCacheHits, mismatchedSpanLengthsThrow) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  std::vector<size_t> numBytes{4, 4};
  std::vector<uint64_t> offsets{0};
  std::vector<char> storage(8);
  std::vector<char*> buffers{storage.data(), storage.data() + 4};
  EXPECT_ANY_THROW(
      ad_utility::readLeadingPageCacheHits(fd, numBytes, offsets, buffers));
  std::vector<uint64_t> twoOffsets{0, 4};
  std::vector<char*> oneBuffer{storage.data()};
  EXPECT_ANY_THROW(ad_utility::readLeadingPageCacheHits(fd, numBytes,
                                                        twoOffsets, oneBuffer));
}

// Check that the `manager` (as returned by `makeBatchManager`, see the tests
// below) performs correct reads via the type-erased `BatchManagerBase`
// interface.
void expectManagerWorks(ad_utility::BatchManagerBase& manager) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");
  ReadBatchForTesting batch;
  batch.add({{4, 4}, {0, 4}});
  manager.wait(batch.submitTo(manager, fd));
  EXPECT_THAT(batch.result(), ::testing::ElementsAre("BBBB", "AAAA"));
}

// With `preferIoUring == false`, `makeBatchManager` must return a
// `SyncIoPolicy`-backed manager without probing io_uring, and leave the flag
// `false`.
TEST(MakeBatchManager, syncBackendWhenIoUringNotPreferred) {
  bool preferIoUring = false;
  auto manager = ad_utility::makeBatchManager(preferIoUring);
  ASSERT_NE(manager, nullptr);
  EXPECT_FALSE(preferIoUring);
  EXPECT_NE(dynamic_cast<ad_utility::BatchManager<ad_utility::SyncIoPolicy>*>(
                manager.get()),
            nullptr);
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
    EXPECT_NE(
        dynamic_cast<ad_utility::BatchManager<
            ad_utility::PageCacheFirstPolicy<ad_utility::IoUringPolicy>>*>(
            manager.get()),
        nullptr);
  } else {
    EXPECT_NE(dynamic_cast<ad_utility::BatchManager<ad_utility::SyncIoPolicy>*>(
                  manager.get()),
              nullptr);
  }
#else
  EXPECT_FALSE(preferIoUring);
  EXPECT_NE(dynamic_cast<ad_utility::BatchManager<ad_utility::SyncIoPolicy>*>(
                manager.get()),
            nullptr);
#endif
  expectManagerWorks(*manager);
}

// Run `readPageCacheHits` on `reads` (pairs of file offset and size) of `fd`
// and return the positions of the reads that were not served together with
// the buffers (filled with '-' before the call).
std::pair<std::vector<size_t>, std::vector<std::string>> readHits(
    int fd, const std::vector<std::pair<uint64_t, size_t>>& reads) {
  std::vector<size_t> numBytes;
  std::vector<uint64_t> offsets;
  std::vector<std::string> buffers;
  for (const auto& [offset, size] : reads) {
    offsets.push_back(offset);
    numBytes.push_back(size);
    buffers.emplace_back(size, '-');
  }
  std::vector<char*> targets;
  for (auto& buffer : buffers) {
    targets.push_back(buffer.data());
  }
  auto notServed =
      ad_utility::readPageCacheHits(fd, numBytes, offsets, targets);
  return {std::move(notServed), std::move(buffers)};
}

// All positions `0 .. n - 1`.
std::vector<size_t> allPositions(size_t n) {
  std::vector<size_t> positions(n);
  std::iota(positions.begin(), positions.end(), size_t{0});
  return positions;
}

// A batch whose file was just written is in the page cache and is served
// completely; adjacent ranges (the first two reads) are read in one call.
TEST(ReadPageCacheHits, hitOnlyBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  auto [notServed, buffers] = readHits(fd, {{0, 4}, {4, 4}, {12, 4}, {4, 0}});
  if (!ad_utility::pageCacheFastPathIsSupported()) {
    EXPECT_EQ(notServed, allPositions(4));
    return;
  }
  EXPECT_TRUE(notServed.empty());
  EXPECT_EQ(buffers[0], "AAAA");
  EXPECT_EQ(buffers[1], "BBBB");
  EXPECT_EQ(buffers[2], "DDDD");
  EXPECT_EQ(buffers[3], "");
}

// Reads that cannot be served completely (here: beyond the end of the file)
// are all returned, whether or not `RWF_NOWAIT` is supported.
TEST(ReadPageCacheHits, missOnlyBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  auto [notServed, buffers] = readHits(fd, {{16, 4}, {40, 4}, {20, 4}});
  EXPECT_EQ(notServed, allPositions(3));
}

// A mixed batch: a short read in the middle of a run of adjacent ranges
// returns the incomplete read and the rest of its run, the complete reads of
// the run and of other runs are served.
TEST(ReadPageCacheHits, mixedBatchWithShortRead) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  auto [notServed, buffers] =
      readHits(fd, {{0, 4}, {8, 4}, {12, 2}, {14, 4}, {18, 4}, {4, 4}});
  if (!ad_utility::pageCacheFastPathIsSupported()) {
    EXPECT_EQ(notServed, allPositions(6));
    return;
  }
  EXPECT_EQ(notServed, (std::vector<size_t>{3, 4}));
  EXPECT_EQ(buffers[0], "AAAA");
  EXPECT_EQ(buffers[1], "CCCC");
  EXPECT_EQ(buffers[2], "DD");
  EXPECT_EQ(buffers[5], "BBBB");
}

// Fault injection for `readPageCacheHits`: the tests below replace its
// `preadv2(RWF_NOWAIT)` call. They need the fast path to be compiled in.
using pageCacheReadTestHelpers::ScopedPageCacheRead;

// Number of calls of the injected page-cache reads below.
size_t numPageCacheReads = 0;

// Fill `iov` with plain blocking `pread` calls (which work on any file
// system, unlike `preadv2` with `RWF_NOWAIT`): the number of bytes read, a
// short count at the end of the file, or -1 on a real I/O error. This lets the
// tests below exercise the batching of `readPageCacheHits` without depending
// on `RWF_NOWAIT` being supported for the test files (it is rejected with
// `EOPNOTSUPP` in the docker builds, for example on overlayfs).
int64_t blockingFill(int fd, const ::iovec* iov, int iovcnt, int64_t offset) {
  int64_t total = 0;
  for (int i = 0; i < iovcnt; ++i) {
    auto* base = static_cast<char*>(iov[i].iov_base);
    size_t remaining = iov[i].iov_len;
    while (remaining > 0) {
      ssize_t numBytesRead = ::pread(fd, base, remaining, offset + total);
      if (numBytesRead < 0) {
        return -1;
      }
      if (numBytesRead == 0) {
        return total;
      }
      base += numBytesRead;
      remaining -= static_cast<size_t>(numBytesRead);
      total += numBytesRead;
    }
  }
  return total;
}

// A fully cached run, counted.
int64_t countedCachedRead(int fd, const ::iovec* iov, int iovcnt,
                          int64_t offset) {
  ++numPageCacheReads;
  return blockingFill(fd, iov, iovcnt, offset);
}

// `EOPNOTSUPP`, counted.
int64_t countedNotSupported(int fd, const ::iovec* iov, int iovcnt,
                            int64_t offset) {
  ++numPageCacheReads;
  return pageCacheReadTestHelpers::notSupported(fd, iov, iovcnt, offset);
}

// Only the first 6 bytes of every run are "cached".
int64_t sixBytesCached(int fd, const ::iovec* iov, int iovcnt, int64_t offset) {
  int64_t numBytesRead = blockingFill(fd, iov, iovcnt, offset);
  return numBytesRead < 0 ? numBytesRead : std::min<int64_t>(numBytesRead, 6);
}

// `RWF_NOWAIT` may return 0 before the end of the file (readv(2)).
int64_t zeroBytes(int, const ::iovec*, int, int64_t) { return 0; }

// Skip a test if `RWF_NOWAIT` is not available on this platform.
#define SKIP_WITHOUT_PAGE_CACHE_FAST_PATH()                 \
  if (!ad_utility::pageCacheFastPathIsSupported()) {        \
    GTEST_SKIP() << "preadv2(RWF_NOWAIT) is not available"; \
  }

// Reads that are not cached (`EAGAIN`) are all returned, their buffers stay
// untouched, and the fast path stays enabled.
TEST(ReadPageCacheHits, notCachedReadsAreReturned) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  ScopedPageCacheRead inject{&pageCacheReadTestHelpers::nothingCached};
  auto [notServed, buffers] = readHits(fd, {{0, 4}, {4, 4}, {12, 4}});
  EXPECT_EQ(notServed, allPositions(3));
  EXPECT_EQ(buffers[0], "----");
  EXPECT_TRUE(ad_utility::pageCacheFastPathIsSupported());
}

// A partial read serves the reads it covers completely; the incomplete read
// and the rest of its run are returned. A read of 0 bytes serves nothing.
TEST(ReadPageCacheHits, partialAndZeroByteReads) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  {
    ScopedPageCacheRead inject{&sixBytesCached};
    auto [notServed, buffers] = readHits(fd, {{0, 4}, {4, 4}, {12, 4}});
    EXPECT_EQ(notServed, (std::vector<size_t>{1}));
    EXPECT_EQ(buffers[0], "AAAA");
    EXPECT_EQ(buffers[2], "DDDD");
  }
  {
    ScopedPageCacheRead inject{&zeroBytes};
    auto [notServed, buffers] = readHits(fd, {{0, 4}, {4, 4}, {12, 4}});
    EXPECT_EQ(notServed, allPositions(3));
  }
}

// `EOPNOTSUPP` disables the fast path for the process: the failed run and all
// later runs of the batch are returned without further calls, and so are all
// reads of later batches.
TEST(ReadPageCacheHits, notSupportedDisablesTheFastPath) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  numPageCacheReads = 0;
  {
    ScopedPageCacheRead inject{&countedNotSupported};
    auto [notServed, buffers] = readHits(fd, {{0, 4}, {8, 4}, {12, 2}});
    EXPECT_EQ(notServed, allPositions(3));
    EXPECT_EQ(numPageCacheReads, 1u);
    EXPECT_FALSE(ad_utility::pageCacheFastPathIsSupported());
    auto [notServed2, buffers2] = readHits(fd, {{0, 4}});
    EXPECT_EQ(notServed2, allPositions(1));
    EXPECT_EQ(numPageCacheReads, 1u);
  }
  // The guard re-enabled the fast path.
  EXPECT_TRUE(ad_utility::pageCacheFastPathIsSupported());
}

// A run of more than `IOV_MAX` adjacent reads is split into several calls.
TEST(ReadPageCacheHits, runsAreSplitAtIovMax) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  const size_t numReads = static_cast<size_t>(IOV_MAX) + 1;
  std::string content(numReads, 'x');
  content.back() = 'y';
  auto [tmp, fd] = makeTempFile(content);
  std::vector<std::pair<uint64_t, size_t>> reads;
  for (size_t i = 0; i < numReads; ++i) {
    reads.emplace_back(i, 1);
  }
  numPageCacheReads = 0;
  ScopedPageCacheRead inject{&countedCachedRead};
  auto [notServed, buffers] = readHits(fd, reads);
  EXPECT_TRUE(notServed.empty());
  EXPECT_EQ(numPageCacheReads, 2u);
  EXPECT_EQ(buffers.front(), "x");
  EXPECT_EQ(buffers.back(), "y");
}

// An empty batch is trivially served, and spans of different lengths are
// rejected.
TEST(ReadPageCacheHits, emptyBatchAndContract) {
  auto [tmp, fd] = makeTempFile("AAAA");
  EXPECT_TRUE(ad_utility::readPageCacheHits(fd, {}, {}, {}).empty());
  std::vector<size_t> numBytes{4, 4};
  std::vector<uint64_t> offsets{0};
  std::string buffer(8, '-');
  std::vector<char*> targets{buffer.data(), buffer.data() + 4};
  EXPECT_ANY_THROW(
      ad_utility::readPageCacheHits(fd, numBytes, offsets, targets));
}
}  // namespace
