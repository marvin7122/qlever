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
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
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
#include "util/NvmePassthrough.h"

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

  // Submit all accumulated reads to `manager` for file `fd`; returns the
  // handle.
  template <typename Manager>
  typename Manager::BatchHandle submitTo(Manager& manager, int fd) {
    // Build the buffer pointers here, after all buffers have been added, so the
    // addresses are stable (no further `add` will reallocate `targetBuffers_`).
    // `addBatch` copies each address into its read request, so this temporary
    // vector need not outlive the call.
    std::vector<char*> pointers = ::ranges::to_vector(
        targetBuffers_ | ql::views::transform([](std::string& buffer) {
          return buffer.data();
        }));
    return manager.addBatch(fd, numBytes_, offsets_, pointers);
  }

  // Submit all accumulated reads directly to `policy` under `handle`.
  template <typename Policy>
  void submitToPolicy(Policy& policy, int fd,
                      typename Policy::BatchHandle handle) {
    std::vector<char*> pointers = ::ranges::to_vector(
        targetBuffers_ | ql::views::transform([](std::string& buffer) {
          return buffer.data();
        }));
    policy.addBatch(fd, numBytes_, offsets_, pointers, handle);
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
                      T, ad_utility::BatchManager<ad_utility::IoUringPolicy>>) {
      if (!ioUringAvailableAtRuntime()) {
        GTEST_SKIP() << "io_uring is compiled in, but not available at "
                        "runtime (e.g. blocked by seccomp inside Docker)";
      }
    }
#endif
  }
};

#ifdef QLEVER_HAS_IO_URING
using ManagerTypes =
    ::testing::Types<ad_utility::BatchManager<ad_utility::IoUringPolicy>,
                     ad_utility::BatchManager<ad_utility::SyncIoPolicy>>;
#else
using ManagerTypes =
    ::testing::Types<ad_utility::BatchManager<ad_utility::SyncIoPolicy>>;
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
        dynamic_cast<ad_utility::BatchManager<ad_utility::IoUringPolicy>*>(
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

namespace nvme = ad_utility::nvmePassthrough;

// File-offset to LBA translation is pure arithmetic: an aligned range maps to
// (namespace, starting LBA, 0-based block count).
TEST(NvmePassthroughTranslation, alignedRangeTranslates) {
  const auto params = nvme::translateToReadParams(
      /*fileOffset=*/4096, /*numBytes=*/8192, /*namespaceId=*/3,
      /*logicalBlockSize=*/4096);
  ASSERT_TRUE(params.has_value());
  EXPECT_EQ(params->namespaceId, 3u);
  EXPECT_EQ(params->startLba, 1u);
  EXPECT_EQ(params->numBlocksZeroBased, 1u);
  EXPECT_EQ(params->transferBytes, 8192u);
}

// A read that is not whole blocks, an invalid request, or one beyond the
// command limits translates to `std::nullopt`, so the caller keeps the plain
// read path.
TEST(NvmePassthroughTranslation, untranslatableRangesFallBack) {
  using nvme::translateToReadParams;
  EXPECT_FALSE(translateToReadParams(100, 4096, 1, 512).has_value());
  EXPECT_FALSE(translateToReadParams(0, 100, 1, 512).has_value());
  EXPECT_FALSE(translateToReadParams(0, 0, 1, 512).has_value());
  EXPECT_FALSE(translateToReadParams(0, 512, 0, 512).has_value());
  EXPECT_FALSE(translateToReadParams(0, 512, 1, 0).has_value());
  EXPECT_FALSE(translateToReadParams(0, 0x10001ULL * 512, 1, 512).has_value());
  // A starting LBA that would wrap around is rejected ...
  EXPECT_FALSE(translateToReadParams(512, 512, 1, 512,
                                     std::numeric_limits<uint64_t>::max())
                   .has_value());
  // ... while the same base with offset zero still translates exactly.
  const auto atMax = translateToReadParams(
      0, 512, 1, 512, std::numeric_limits<uint64_t>::max());
  ASSERT_TRUE(atMax.has_value());
  EXPECT_EQ(atMax->startLba, std::numeric_limits<uint64_t>::max());
}

// Call `planBlockReads` with owning vectors.
nvme::BlockReadPlan plan(std::vector<uint64_t> offsets,
                         std::vector<size_t> sizes, uint64_t maxGapBlocks = 0,
                         std::optional<uint64_t> readLimit = std::nullopt) {
  return nvme::planBlockReads(offsets, sizes, maxGapBlocks, readLimit);
}

// Check that every word `i` of `plan` lies inside the run that covers it, at
// the right staging offset.
void expectSlicesInsideRuns(const nvme::BlockReadPlan& plan,
                            const std::vector<uint64_t>& offsets,
                            const std::vector<size_t>& sizes) {
  ASSERT_EQ(plan.slices.size(), offsets.size());
  for (size_t i = 0; i < offsets.size(); ++i) {
    if (sizes[i] == 0) {
      continue;
    }
    const auto& slice = plan.slices[i];
    EXPECT_EQ(slice.numBytes, sizes[i]);
    size_t numCovering = 0;
    for (const auto& run : plan.runs) {
      if (offsets[i] >= run.fileOffset &&
          offsets[i] + sizes[i] <= run.fileOffset + run.numBytes) {
        EXPECT_EQ(slice.stagingOffset - run.stagingOffset,
                  offsets[i] - run.fileOffset);
        ++numCovering;
      }
    }
    EXPECT_EQ(numCovering, 1u) << "word " << i;
  }
}

// Whole-block coverage: adjacent words merge into one run while the slices
// keep the input order; disjoint words give separate runs with accumulating
// staging offsets.
TEST(NvmeBlockCoalescing, coversWordsWithMergedRuns) {
  std::vector<uint64_t> offsets{0, 100, 1000, 5000};
  std::vector<size_t> sizes{512, 100, 600, 10};
  const auto p = plan(offsets, sizes);
  ASSERT_EQ(p.runs.size(), 2u);
  EXPECT_EQ(p.runs[0].fileOffset, 0u);
  EXPECT_EQ(p.runs[0].numBytes, 2048u);
  EXPECT_EQ(p.runs[1].fileOffset, 4608u);
  EXPECT_EQ(p.runs[1].numBytes, 512u);
  EXPECT_EQ(p.stagingBytes, 2560u);
  EXPECT_EQ(p.slices[2].stagingOffset, 1000u);
  EXPECT_EQ(p.slices[3].stagingOffset, 2048u + 392);
  expectSlicesInsideRuns(p, offsets, sizes);
}

// Duplicate words share their blocks but keep one slice each, zero-length
// words cover nothing, and an empty input plans nothing.
TEST(NvmeBlockCoalescing, duplicatesAndEmptyWords) {
  const auto p = plan({600, 600, 2000}, {100, 100, 0});
  ASSERT_EQ(p.runs.size(), 1u);
  EXPECT_EQ(p.runs[0].fileOffset, 512u);
  EXPECT_EQ(p.runs[0].numBytes, 512u);
  ASSERT_EQ(p.slices.size(), 3u);
  EXPECT_EQ(p.slices[0].stagingOffset, 88u);
  EXPECT_EQ(p.slices[1].stagingOffset, 88u);
  EXPECT_EQ(p.slices[2].numBytes, 0u);
  const auto empty = plan({}, {});
  EXPECT_TRUE(empty.runs.empty());
  EXPECT_TRUE(empty.slices.empty());
  EXPECT_EQ(empty.stagingBytes, 0u);
}

// Gaps up to the allowance are swallowed into one run; larger gaps and a zero
// allowance keep separate runs.
TEST(NvmeBlockCoalescing, gapAllowance) {
  const auto merged = plan({0, 5 * 512}, {10, 10}, 32);
  ASSERT_EQ(merged.runs.size(), 1u);
  EXPECT_EQ(merged.runs[0].numBytes, 6 * 512u);
  expectSlicesInsideRuns(merged, {0, 5 * 512}, {10, 10});
  EXPECT_EQ(plan({0, 100 * 512}, {10, 10}, 32).runs.size(), 2u);
  EXPECT_EQ(plan({0, 5 * 512}, {10, 10}, 0).runs.size(), 2u);
}

// Runs stop before exceeding 256 blocks (128 KiB), even if every gap would be
// swallowed: with the largest useful allowance (256 blocks), words 100 blocks
// apart merge into commands of at most 128 KiB.
TEST(NvmeBlockCoalescing, runsAreAtMost128KiB) {
  std::vector<uint64_t> offsets;
  std::vector<size_t> sizes;
  for (uint64_t w = 0; w < 10; ++w) {
    offsets.push_back(w * 100 * 512);
    sizes.push_back(10);
  }
  const auto p = plan(offsets, sizes, 256);
  // Blocks 0..200 (201 blocks), 300..500, 600..800, 900 (block 300 would make
  // the first run 301 blocks long).
  ASSERT_EQ(p.runs.size(), 4u);
  EXPECT_EQ(p.runs[0].numBytes, 201 * 512u);
  EXPECT_EQ(p.runs[3].numBytes, 512u);
  for (const auto& run : p.runs) {
    EXPECT_LE(run.numBytes, nvme::kCoalesceMaxRunBlocks * 512);
  }
  expectSlicesInsideRuns(p, offsets, sizes);
  // With the default allowance (32 blocks), every word is its own command.
  EXPECT_EQ(plan(offsets, sizes, 32).runs.size(), 10u);
}

// With a read limit, the last run ends there instead of covering the rest of
// its final block; everything else is unchanged.
TEST(NvmeBlockCoalescing, clampsLastRunToReadLimit) {
  const auto p = plan({0, 5000}, {10, 100}, 0, 5100);
  ASSERT_EQ(p.runs.size(), 2u);
  EXPECT_EQ(p.runs[0].numBytes, 512u);
  EXPECT_EQ(p.runs[1].fileOffset, 4608u);
  EXPECT_EQ(p.runs[1].numBytes, 5100u - 4608u);
  EXPECT_EQ(p.stagingBytes, 1024u);
  EXPECT_EQ(p.slices[1].stagingOffset, 512u + (5000 - 4608));
  EXPECT_EQ(plan({0}, {512}, 0, 512).runs[0].numBytes, 512u);
  EXPECT_EQ(plan({5000}, {100}).runs[0].numBytes, 512u);
  // A word past the read limit is a contract violation.
  EXPECT_ANY_THROW(plan({5000}, {200}, 0, 5100));
}

// The median gap between consecutive words in file order, independent of the
// input order; overlapping and duplicate words have gap zero, and empty words
// are ignored.
TEST(NvmeMedianGap, medianGapBytes) {
  auto gap = [](std::vector<uint64_t> offsets, std::vector<size_t> sizes) {
    return nvme::medianGapBytes(offsets, sizes);
  };
  // Gaps 0, 800, 8900.
  EXPECT_EQ(gap({10000, 0, 1000, 100}, {100, 100, 100, 100}), 800u);
  EXPECT_EQ(gap({0, 0, 50}, {100, 100, 10}), 0u);
  // Gaps 900 and 5000: the upper median.
  EXPECT_EQ(gap({0, 1000, 6100}, {100, 100, 10}), 5000u);
  EXPECT_EQ(gap({0}, {10}), std::nullopt);
  EXPECT_EQ(gap({0, 5000}, {0, 10}), std::nullopt);
  EXPECT_EQ(gap({}, {}), std::nullopt);
}

// The block device of an NVMe namespace is found from the name of its
// generic character device.
TEST(NvmeDeviceNames, blockDeviceNameForGenericCharDevice) {
  using nvme::blockDeviceNameForGenericCharDevice;
  EXPECT_EQ(blockDeviceNameForGenericCharDevice("ng1n1"), "nvme1n1");
  EXPECT_EQ(blockDeviceNameForGenericCharDevice("ng10n23"), "nvme10n23");
  for (std::string_view name :
       {"nvme1n1", "ng1", "ngn1", "ng1n", "ng1x1", "ng1n1p1", "null", ""}) {
    EXPECT_EQ(blockDeviceNameForGenericCharDevice(name), std::nullopt) << name;
  }
}

// The capability probe fails closed without throwing: an invalid fd, a
// regular file, and a character device that is not an NVMe namespace (such as
// `/dev/null`) are all "not capable".
TEST(NvmePassthroughProbe, failsClosedForNonDevices) {
  EXPECT_FALSE(nvme::isPassthroughCandidate(-1, 1));
  auto [tmp, fd] = makeTempFile("X");
  EXPECT_FALSE(nvme::isPassthroughCandidate(fd, 1));
  ad_utility::File nullFile{"/dev/null", "r"};
  EXPECT_FALSE(nvme::isPassthroughCandidate(nullFile.fd(), 1));
  EXPECT_EQ(nvme::nvmeNamespaceIdOf(nullFile.fd()), std::nullopt);
}

#ifdef QLEVER_HAS_IO_URING
// Build an `IoUringPolicy` with `options`, or return `nullptr` if the kernel
// does not allow the ring (no io_uring at all, or no 128-byte SQEs).
std::unique_ptr<ad_utility::IoUringPolicy> makePolicy(
    const nvme::Options& options) {
  try {
    return std::make_unique<ad_utility::IoUringPolicy>(64, options);
  } catch (const ad_utility::Exception&) {
    return nullptr;
  }
}

// Passthrough is off by default, and a policy with disabled options is a plain
// policy.
TEST(NvmePassthroughPolicy, disabledByDefault) {
  if (!ioUringAvailableAtRuntime()) {
    GTEST_SKIP() << "io_uring is not available at runtime";
  }
  ad_utility::IoUringPolicy plain(64);
  EXPECT_FALSE(plain.isNvmePassthroughEnabled());
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  ad_utility::IoUringPolicy disabled(64, nvme::Options{});
  EXPECT_FALSE(disabled.isNvmePassthroughEnabled());
  ReadBatchForTesting batch;
  batch.add({{4, 4}, {0, 4}});
  batch.submitToPolicy(disabled, fd, 0);
  disabled.wait(0);
  EXPECT_THAT(batch.result(), ElementsAre("BBBB", "AAAA"));
}

// Enabled options with a zero namespace id or block size are a contract
// violation.
TEST(NvmePassthroughPolicy, invalidOptionsThrow) {
  EXPECT_THROW(ad_utility::IoUringPolicy(64, nvme::Options{true, 0, 512}),
               ad_utility::Exception);
  EXPECT_THROW(ad_utility::IoUringPolicy(64, nvme::Options{true, 1, 0}),
               ad_utility::Exception);
}

// With passthrough enabled, a regular file fails the capability probe and is
// read with plain reads, byte-identical to a plain policy.
TEST(NvmePassthroughPolicy, regularFilesKeepPlainReads) {
  auto policy = makePolicy(nvme::Options{true, 1, 512});
  if (!policy) {
    GTEST_SKIP() << "io_uring with 128-byte SQEs is not available at runtime";
  }
  EXPECT_EQ(policy->isNvmePassthroughEnabled(), nvme::kUringCmdSupported);
  std::string content(2048, 'x');
  content.replace(512, 4, "ABCD");
  auto [tmp, fd] = makeTempFile(content);
  EXPECT_FALSE(policy->isNvmeCapable(fd));
  ReadBatchForTesting batch;
  batch.add({{512, 512}, {512, 4}, {0, 512}});
  batch.submitToPolicy(*policy, fd, 7);
  policy->wait(7);
  EXPECT_EQ(batch.result()[0].substr(0, 4), "ABCD");
  EXPECT_EQ(batch.result()[1], "ABCD");
  EXPECT_EQ(batch.result()[2], std::string(512, 'x'));
}

// The command path, observed without an NVMe device: once the probe result
// for a regular file is overridden to "capable", every whole-block read of it
// is submitted as a native NVMe command, which the kernel rejects for a
// regular file, so `wait` throws (a plain read of the same range succeeds, see
// above). A read that is not whole blocks still takes the plain path and
// succeeds on the same fd.
TEST(NvmePassthroughPolicy, capableFdGetsNativeCommands) {
  auto policy = makePolicy(nvme::Options{true, 1, 512});
  if (!policy || !policy->isNvmePassthroughEnabled()) {
    GTEST_SKIP() << "NVMe passthrough is not available in this build or at "
                    "runtime";
  }
  auto [tmp, fd] = makeTempFile(std::string(2048, 'x'));
  policy->setNvmeCapableForTesting(fd, true);
  EXPECT_TRUE(policy->isNvmeCapable(fd));

  ReadBatchForTesting unaligned;
  unaligned.add({{100, 4}});
  unaligned.submitToPolicy(*policy, fd, 0);
  policy->wait(0);
  EXPECT_THAT(unaligned.result(), ElementsAre("xxxx"));

  ReadBatchForTesting aligned;
  aligned.add({{512, 1024}});
  aligned.submitToPolicy(*policy, fd, 1);
  EXPECT_ANY_THROW(policy->wait(1));
}
#endif

#if defined(QLEVER_HAS_IO_URING) && defined(QLEVER_HAS_NVME_URING_CMD)
// Preparing a passthrough SQE sets the `uring_cmd` opcode, the NVMe command
// operation and the native read command (namespace, buffer, length, starting
// LBA split across CDW10/11, 0-based block count in CDW12), and zeroes the
// rest of the command area. No device is involved.
TEST(NvmePassthroughSqe, preparesNvmeReadCommand) {
  const auto params =
      nvme::translateToReadParams(/*fileOffset=*/8192, /*numBytes=*/4096,
                                  /*namespaceId=*/2, /*logicalBlockSize=*/4096);
  ASSERT_TRUE(params.has_value());
  std::string buffer(4096, '\0');
  // The C type is 64 bytes; the command area needs the 128-byte SQE128 slot.
  alignas(io_uring_sqe) unsigned char sqeStorage[128];
  std::memset(sqeStorage, 0xFF, sizeof(sqeStorage));
  auto* sqe = reinterpret_cast<io_uring_sqe*>(sqeStorage);
  nvme::preparePassthroughRead(sqe, /*deviceFd=*/7, *params, buffer.data());

  EXPECT_EQ(sqe->opcode, IORING_OP_URING_CMD);
  EXPECT_EQ(sqe->fd, 7);
  EXPECT_EQ(sqe->cmd_op, static_cast<uint32_t>(NVME_URING_CMD_IO));
  nvme_uring_cmd cmd{};
  const auto* tail = sqeStorage + offsetof(io_uring_sqe, cmd);
  std::memcpy(&cmd, tail, sizeof(cmd));
  // 02h is the NVM Read opcode (01h would be Write).
  EXPECT_EQ(cmd.opcode, 0x02);
  EXPECT_EQ(cmd.nsid, 2u);
  EXPECT_EQ(cmd.addr, reinterpret_cast<__u64>(buffer.data()));
  EXPECT_EQ(cmd.data_len, 4096u);
  EXPECT_EQ(cmd.cdw10, 2u);
  EXPECT_EQ(cmd.cdw11, 0u);
  EXPECT_EQ(cmd.cdw12, 0u);
  for (size_t i = sizeof(cmd); i < nvme::kUringCmdDataSize; ++i) {
    EXPECT_EQ(tail[i], 0u) << "nonzero byte at command offset " << i;
  }
}
#endif
}  // namespace
