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

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <deque>
#include <functional>
#include <initializer_list>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"
#include "util/FakeIoUring.h"
#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"
#include "util/Log.h"
#include "util/Views.h"

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
    std::vector<char*> pointers = targetBufferPointers();
    return manager.addBatch(fd, numBytes_, offsets_, pointers);
  }

  // Add all accumulated reads to the read policy `policy` (not a manager) for
  // file `fd`, under the batch handle `handle`.
  template <typename Policy>
  void addTo(Policy& policy, int fd, typename Policy::BatchHandle handle) {
    std::vector<char*> pointers = targetBufferPointers();
    policy.addBatch(fd, numBytes_, offsets_, pointers, handle);
  }

  // The bytes read by each read, in request order (valid once the batch has
  // completed).
  const std::vector<std::string>& result() const { return targetBuffers_; }

 private:
  // Build the buffer pointers only after all buffers have been added, so the
  // addresses are stable (no further `add` will reallocate `targetBuffers_`).
  // `addBatch` copies each address into its read request, so the returned
  // vector need not outlive the call.
  std::vector<char*> targetBufferPointers() {
    return ::ranges::to_vector(targetBuffers_ |
                               ql::views::transform([](std::string& buffer) {
                                 return buffer.data();
                               }));
  }

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

  // Add all reads to the read policy `policy` under the handle `handle`.
  template <typename Policy>
  void addTo(Policy& policy, int fd, typename Policy::BatchHandle handle) {
    batch_.addTo(policy, fd, handle);
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

// A batch much larger than the ring completes with every read in its own
// buffer. For `IoUringPolicy`: ring 4 is smaller than a `REAP_WAVE`, so the
// wave for a full ring is clamped to the requests in the kernel; ring 8 is
// exactly one wave; with ring 256, more than `REAP_CHUNK` (64) CQEs can be
// ready at once, so they are reaped in several chunks. `SyncIoPolicy` has no
// ring and reads synchronously.
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
    manager.wait(scenario.submitTo(manager, fd));
    EXPECT_THAT(scenario.results(),
                ::testing::ElementsAreArray(scenario.expected()))
        << "ring size " << ringSize;
  }
}

// Waiting on the last of many small batches (1, 3 or 5 reads each) can reap
// the CQEs of the other batches in the same waves. The remaining waits may then
// find their batches already complete and must still see the correct bytes.
// The batch sizes 1, 3 and 5 are not multiples of the wave size (8), so the
// waves cut across batches.
TYPED_TEST(IoUringManagerTest, WaveReapCompletesOtherBatches) {
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
  std::vector<typename TypeParam::BatchHandle> handles;
  handles.reserve(M);
  for (auto& batch : batches) {
    handles.push_back(batch.submitTo(manager, fd));
  }
  // Wait on the last batch first, then on the rest in submission order.
  manager.wait(handles.back());
  for (const auto& handle :
       ql::span<const typename TypeParam::BatchHandle>{handles}.first(M - 1)) {
    manager.wait(handle);
  }
  for (const auto& [i, batchAndExpected] :
       ::ranges::views::enumerate(::ranges::views::zip(batches, expected))) {
    const auto& [batch, expectedOfBatch] = batchAndExpected;
    EXPECT_THAT(batch.result(), ::testing::ElementsAreArray(expectedOfBatch))
        << "mismatch at batch " << i;
  }
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
// by the time the manager is destroyed nothing is outstanding, the destructor
// has nothing to drain, and it logs no warning. This is the counterpart to the
// io_uring-specific `dropRunningManager` test below.
TEST(IoUringManagerDrop, dropSyncManagerHasNothingOutstanding) {
  using Manager = ad_utility::BatchManager<ad_utility::SyncIoPolicy>;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  // Capture the log so we can assert that the destructor stays silent.
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});
  {
    Manager manager(64);
    batch.submitTo(manager, fd);  // reads happen synchronously here
    // `manager` is destroyed here; nothing is outstanding, so no warning.
  }

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
  EXPECT_THAT(logStream.str(), ::testing::IsEmpty());
}

#ifdef QLEVER_HAS_IO_URING
// Drop the manager while reads are still outstanding (submitted but never
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
    // `manager` is destroyed here; its destructor drains the outstanding reads.
  }

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
  EXPECT_THAT(logStream.str(), ifLogLevelIsCompiledIn<LogLevel::Enum::WARN>(
                                   ::testing::HasSubstr("still outstanding")));
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
#ifdef QLEVER_HAS_IO_URING
// The script of a `ScriptedRing`, and what it observed. Owned by the test, so
// that it can be read also after the policy has destroyed the ring.
struct RingScript {
  // The results of the next submits, consumed from the front: `std::nullopt`
  // submits for real; a value <= 0 is returned without submitting (a negative
  // value fails the submit with that `-errno`, 0 means that the kernel took no
  // SQE). Once it is empty, every submit is real.
  std::deque<std::optional<int>> submitResults;
  // The number of the next waits that fail with `-EINTR` without waiting.
  size_t numberOfWaitsToInterrupt = 0;
  // If nonzero, every wait fails with this `errno`.
  int brokenWaitErrno = 0;
  // Called before every wait that reaches the real ring, e.g. to write the
  // bytes that the reads of a pipe wait for.
  std::function<void()> beforeRealWait;

  // The requests (reads and no-ops) that the kernel took, and the completions
  // that the policy reaped. Every request that reached the kernel is reaped
  // iff they are equal.
  size_t numberOfTakenRequests = 0;
  size_t numberOfReapedCompletions = 0;
  // The calls of `backOff`.
  size_t numberOfBackOffs = 0;
};

// A liburing ring whose submits and waits follow a `RingScript`. Tests run
// `IoUringPolicy` on it to inject the failures that a real kernel produces only
// rarely, while the reads still go to the real kernel.
class ScriptedRing final : public ad_utility::IoUringInterface {
 public:
  ScriptedRing(unsigned ringSize, RingScript& script)
      : ring_{ad_utility::makeLiburingRing(ringSize)}, script_{script} {}

  bool prepareRead(int fd, char* buffer, size_t numberOfBytes, uint64_t offset,
                   uint64_t requestId) override {
    return ring_->prepareRead(fd, buffer, numberOfBytes, offset, requestId);
  }
  size_t numberOfQueued() const noexcept override {
    return ring_->numberOfQueued();
  }
  size_t sqCapacity() const noexcept override { return ring_->sqCapacity(); }
  size_t cqCapacity() const noexcept override { return ring_->cqCapacity(); }

  int submit() override {
    if (!script_.submitResults.empty()) {
      const std::optional<int> result = script_.submitResults.front();
      script_.submitResults.pop_front();
      if (result.has_value()) {
        return result.value();
      }
    }
    const int result = ring_->submit();
    if (result > 0) {
      script_.numberOfTakenRequests += static_cast<size_t>(result);
    }
    return result;
  }

  int waitForCompletions(unsigned minComplete) override {
    if (script_.brokenWaitErrno != 0) {
      return -script_.brokenWaitErrno;
    }
    if (script_.numberOfWaitsToInterrupt > 0) {
      --script_.numberOfWaitsToInterrupt;
      return -EINTR;
    }
    // A real wait for more completions than the kernel took would block the
    // test forever. Return an error instead, which no real wait returns here,
    // so that the policy terminates with this errno in its message.
    if (script_.numberOfReapedCompletions + minComplete >
        script_.numberOfTakenRequests) {
      return -ETIME;
    }
    if (script_.beforeRealWait) {
      script_.beforeRealWait();
    }
    return ring_->waitForCompletions(minComplete);
  }

  size_t popCompletions(ql::span<Completion> completions) override {
    const size_t numberOfPopped = ring_->popCompletions(completions);
    script_.numberOfReapedCompletions += numberOfPopped;
    return numberOfPopped;
  }

  std::vector<uint64_t> cancelQueued(uint64_t noOpRequestId) override {
    return ring_->cancelQueued(noOpRequestId);
  }

  void backOff() override { ++script_.numberOfBackOffs; }

 private:
  std::unique_ptr<ad_utility::IoUringInterface> ring_;
  RingScript& script_;
};

// Tests of `IoUringPolicy` for failed reads and failing io_uring calls. The
// policy must not throw while the kernel may still write into the target
// buffers of a batch, since the callers free them when they unwind. The tests
// of this fixture run on a `ScriptedRing`, which needs a real ring; the tests
// `IoUringPolicyOnFakeRing` run on `FakeIoUring`, also where io_uring is
// blocked at runtime.
class IoUringPolicyFailureTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!ioUringAvailableAtRuntime()) {
      GTEST_SKIP() << "io_uring is compiled in, but not available at runtime "
                      "(e.g. blocked by seccomp inside Docker)";
    }
  }

  // A policy on a `ScriptedRing` with (at least) `ringSize` SQEs.
  static ad_utility::IoUringPolicy makePolicy(unsigned ringSize,
                                              RingScript& script) {
    return ad_utility::IoUringPolicy{
        std::make_unique<ScriptedRing>(ringSize, script)};
  }
};

// A file of `numberOfChunks` chunks of 4 bytes, chunk `i` filled with a
// character that depends on `i`, and a batch that reads each chunk.
std::pair<std::string, ReadBatchForTesting> chunkedFileAndBatch(
    size_t numberOfChunks) {
  std::string content;
  ReadBatchForTesting batch;
  for (size_t i : ad_utility::integerRange(numberOfChunks)) {
    batch.add(content.size(), 4);
    content.append(4, static_cast<char>('A' + i % 26));
  }
  return {std::move(content), std::move(batch)};
}

constexpr size_t MAX_SUBMIT_RETRIES = ad_utility::DEFAULT_MAX_SUBMIT_RETRIES;

// A policy on a `FakeIoUring`, and the fake ring, which the policy owns.
struct PolicyOnFakeRing {
  ad_utility::testing::FakeIoUring& fake;
  ad_utility::IoUringPolicy policy;
};

// Run a policy on `fake`, and keep a reference to `fake` for the checks.
PolicyOnFakeRing makePolicyOnFakeRing(
    std::unique_ptr<ad_utility::testing::FakeIoUring> fake) {
  ad_utility::testing::FakeIoUring& fakeRef = *fake;
  return {fakeRef, ad_utility::IoUringPolicy{std::move(fake)}};
}

// The 4-byte chunks of `file`, the expected result of the batch of
// `chunkedFileAndBatch`.
std::vector<std::string> chunksOf(std::string_view file) {
  std::vector<std::string> chunks;
  for (size_t i : ad_utility::integerRange((file.size() + 3) / 4)) {
    chunks.emplace_back(file.substr(4 * i, 4));
  }
  return chunks;
}

constexpr size_t REAP_WAVE = ad_utility::IoUringPolicy::REAP_WAVE;
constexpr size_t REAP_CHUNK =
    ad_utility::IoUringInterface::MAX_COMPLETIONS_PER_POP;

// _____________________________________________________________________________
// A read past the end of the file is short. `wait()` throws its error only
// once the other reads of the batch have completed too.
TEST_F(IoUringPolicyFailureTest, failedReadIsThrownAfterAllReadsOfItsBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();
  RingScript script;
  ReadBatchForTesting batch;
  batch.add({{0, 16}, {0, 4}, {4, 4}});
  {
    auto policy = makePolicy(64, script);
    batch.addTo(policy, fd, 0);
    AD_EXPECT_THROW_WITH_MESSAGE(policy.wait(0),
                                 HasSubstr("read fewer bytes than requested in "
                                           "IoUringPolicy (8 of 16 bytes)"));
    EXPECT_EQ(script.numberOfTakenRequests, 3U);
    EXPECT_EQ(script.numberOfReapedCompletions, 3U);
  }
  EXPECT_THAT(batch.result()[1], Eq("AAAA"));
  EXPECT_THAT(batch.result()[2], Eq("BBBB"));
  EXPECT_THAT(logStream.str(), Not(HasSubstr("still outstanding")));
}

// _____________________________________________________________________________
// A submit keeps failing with `EAGAIN` in the middle of a batch that is larger
// than the ring, while earlier reads of the batch are in the kernel.
// `addBatch` reaps them before it throws, and the queued reads are rewritten
// into no-ops: the next batch hands them to the kernel, and they do not touch
// their buffers. (How many reads are queued when the submit fails depends on
// how many completions the wave that freed their slots reaped.)
TEST_F(IoUringPolicyFailureTest, failedSubmitCancelsQueuedReadsIntoNoOps) {
  constexpr size_t numberOfReads = 100;
  constexpr size_t ringSize = 64;
  auto [content, failing] = chunkedFileAndBatch(numberOfReads);
  auto [tmp, fd] = makeTempFile(content);
  RingScript script;
  auto [content2, next] = chunkedFileAndBatch(3);
  {
    auto policy = makePolicy(ringSize, script);
    // The first submit takes a full ring of reads. When the ring is full
    // again, the submit fails, also on every retry.
    script.submitResults.push_back(std::nullopt);
    script.submitResults.insert(script.submitResults.end(),
                                MAX_SUBMIT_RETRIES + 1, -EAGAIN);
    AD_EXPECT_THROW_WITH_MESSAGE(
        failing.addTo(policy, fd, 0),
        HasSubstr("io_uring_submit failed in IoUringPolicy"));
    EXPECT_TRUE(script.submitResults.empty());
    EXPECT_EQ(script.numberOfBackOffs, MAX_SUBMIT_RETRIES);
    EXPECT_EQ(script.numberOfTakenRequests, ringSize);
    EXPECT_EQ(script.numberOfReapedCompletions, ringSize);
    // The reads that the kernel took are complete, the others never started.
    for (size_t i : ad_utility::integerRange(numberOfReads)) {
      EXPECT_EQ(failing.result()[i],
                i < ringSize ? content.substr(4 * i, 4) : std::string(4, '\0'))
          << i;
    }

    // The next batch hands the no-ops of the cancelled reads to the kernel.
    next.addTo(policy, fd, 1);
    policy.wait(1);
    EXPECT_THAT(next.result(), ElementsAre("AAAA", "BBBB", "CCCC"));
    EXPECT_GT(script.numberOfTakenRequests, ringSize + 3);
    EXPECT_LE(script.numberOfTakenRequests, numberOfReads + 3);
  }
  // The no-ops completed (in `wait()` or in the destructor) without touching
  // the buffers of the cancelled reads.
  EXPECT_EQ(script.numberOfReapedCompletions, script.numberOfTakenRequests);
  for (size_t i = ringSize; i < numberOfReads; ++i) {
    EXPECT_EQ(failing.result()[i], std::string(4, '\0')) << i;
  }
}

// _____________________________________________________________________________
// A submit that fails with `EAGAIN` or `ENOMEM`, or takes no SQE, is retried
// after a back-off, and `EINTR` at once. Only failures in a row count towards
// the retries.
TEST_F(IoUringPolicyFailureTest, transientSubmitFailuresAreRetried) {
  auto [content, small] = chunkedFileAndBatch(3);
  auto [tmp, fd] = makeTempFile(content);
  RingScript script;
  auto policy = makePolicy(64, script);
  script.submitResults = {-EAGAIN, 0, -EINTR, -ENOMEM};
  small.addTo(policy, fd, 0);
  policy.wait(0);
  EXPECT_THAT(small.result(), ElementsAre("AAAA", "BBBB", "CCCC"));
  EXPECT_EQ(script.numberOfBackOffs, 3U);

  // Ten failures, progress, ten more failures: 20 in total, but never more
  // than `MAX_SUBMIT_RETRIES` in a row. A refill of the ring of 8 prepares at
  // most 8 reads, so `addBatch` submits at least 1 + (100 - 8) / 8 times, and
  // the script is used up.
  auto [content2, large] = chunkedFileAndBatch(100);
  auto [tmp2, fd2] = makeTempFile(content2);
  auto smallRingPolicy = makePolicy(8, script);
  script.submitResults = {std::nullopt};
  script.submitResults.insert(script.submitResults.end(), 10, -EAGAIN);
  script.submitResults.push_back(std::nullopt);
  script.submitResults.insert(script.submitResults.end(), 10, -ENOMEM);
  large.addTo(smallRingPolicy, fd2, 1);
  smallRingPolicy.wait(1);
  EXPECT_TRUE(script.submitResults.empty());
  EXPECT_EQ(::ranges::to_vector(large.result() | ql::views::join),
            ::ranges::to_vector(content2));
  EXPECT_EQ(script.numberOfBackOffs, 23U);
  EXPECT_EQ(script.numberOfReapedCompletions, script.numberOfTakenRequests);
}

// _____________________________________________________________________________
// Transient submit failures are retried also within one submit of the queued
// reads: ten failures, a partial submit, ten more failures.
TEST(IoUringPolicyOnFakeRing,
     transientSubmitFailuresWithinOneSubmitAreRetried) {
  using ad_utility::testing::FakeIoUring;
  FakeIoUring::Faults faults;
  faults.scriptedSubmits.assign(10, -EAGAIN);
  faults.scriptedSubmits.push_back(1);
  faults.scriptedSubmits.insert(faults.scriptedSubmits.end(), 10, -ENOMEM);
  ad_utility::IoUringPolicy policy{
      std::make_unique<FakeIoUring>("AAAABBBBCCCC", 8, faults, 0)};
  ReadBatchForTesting batch;
  batch.add({{0, 4}, {4, 4}, {8, 4}});
  batch.addTo(policy, -1, 0);
  policy.wait(0);
  EXPECT_THAT(batch.result(), ElementsAre("AAAA", "BBBB", "CCCC"));
}

// _____________________________________________________________________________
// With `maxSubmitRetries = 2`, two consecutive `EAGAIN` are ridden out, and the
// third makes `addBatch` cancel the batch after exactly 2 back-offs.
TEST(IoUringPolicyOnFakeRing, maxSubmitRetriesIsConfigurable) {
  using ad_utility::testing::FakeIoUring;
  const std::string file = "AAAABBBBCCCC";
  ad_utility::BatchManagerParameters parameters;
  parameters.maxSubmitRetries = 2;
  const auto makeRing = [&file](size_t numberOfFailures) {
    FakeIoUring::Faults faults;
    faults.scriptedSubmits.assign(numberOfFailures, -EAGAIN);
    return std::make_unique<FakeIoUring>(file, 8, faults, 0);
  };
  ReadBatchForTesting batch;
  batch.add({{0, 4}, {4, 4}, {8, 4}});

  {
    ad_utility::IoUringPolicy policy{makeRing(2), parameters};
    batch.addTo(policy, -1, 0);
    policy.wait(0);
    EXPECT_THAT(batch.result(), ElementsAre("AAAA", "BBBB", "CCCC"));
  }

  ReadBatchForTesting failing;
  failing.add({{0, 4}, {4, 4}, {8, 4}});
  auto fakePtr = makeRing(3);
  FakeIoUring& fake = *fakePtr;
  ad_utility::IoUringPolicy policy{std::move(fakePtr), parameters};
  AD_EXPECT_THROW_WITH_MESSAGE(failing.addTo(policy, -1, 0),
                               HasSubstr("(after 2 retries)"));
  EXPECT_EQ(fake.faultCounts().cancelledReads, 3U);
}

// _____________________________________________________________________________
// A policy must retry a failed submit at least once.
TEST(IoUringPolicyOnFakeRing, zeroSubmitRetriesAreRejected) {
  using ad_utility::testing::FakeIoUring;
  ad_utility::BatchManagerParameters parameters;
  parameters.maxSubmitRetries = 0;
  AD_EXPECT_THROW_WITH_MESSAGE(
      (ad_utility::IoUringPolicy{
          std::make_unique<FakeIoUring>("", 8, FakeIoUring::Faults{}, 0),
          parameters}),
      HasSubstr("must retry a failed submit at least once"));
}

// _____________________________________________________________________________
// After a whole ring of 8 reads was cancelled, the ring holds only queued
// no-ops.
// The next batch hands them to the kernel first, also if a submit takes only
// some of them.
TEST(IoUringPolicyOnFakeRing, ringFullOfNoOpsIsSubmittedPartially) {
  using ad_utility::testing::FakeIoUring;
  const std::string file = "AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJ";
  FakeIoUring::Faults faults;
  // All `MAX_SUBMIT_RETRIES + 1` submits of the first batch fail. Then the
  // kernel takes 3, and then 2 of the 8 no-ops.
  faults.scriptedSubmits.assign(MAX_SUBMIT_RETRIES + 1, -EAGAIN);
  faults.scriptedSubmits.push_back(3);
  faults.scriptedSubmits.push_back(2);
  auto [fake, policy] =
      makePolicyOnFakeRing(std::make_unique<FakeIoUring>(file, 8, faults, 0));

  ReadBatchForTesting cancelled;
  for (size_t i : ad_utility::integerRange(size_t{10})) {
    cancelled.add(4 * i, 4);
  }
  AD_EXPECT_THROW_WITH_MESSAGE(
      cancelled.addTo(policy, -1, 0),
      HasSubstr(absl::StrCat("(after ", MAX_SUBMIT_RETRIES, " retries)")));
  EXPECT_EQ(fake.faultCounts().cancelledReads, 8U);
  EXPECT_EQ(fake.numberOfQueued(), 8U);
  EXPECT_EQ(fake.numberOfReadsInRing(), 0U);
  EXPECT_THAT(cancelled.result(), Each(Eq(std::string(4, '\0'))));

  for (uint64_t handle : {uint64_t{1}, uint64_t{2}}) {
    ReadBatchForTesting next;
    next.add({{0, 4}, {36, 4}, {8, 4}});
    next.addTo(policy, -1, handle);
    policy.wait(handle);
    EXPECT_THAT(next.result(), ElementsAre("AAAA", "JJJJ", "CCCC"));
    EXPECT_EQ(fake.numberOfReadsInRing(), 0U);
  }
  EXPECT_EQ(fake.numberOfQueued(), 0U);
}

// _____________________________________________________________________________
// A submit error other than `EINTR`, `EAGAIN` and `ENOMEM` is not retried. The
// queued reads are cancelled, and `addBatch` throws once the reads of the batch
// that the kernel holds have been reaped.
TEST_F(IoUringPolicyFailureTest, brokenSubmitThrowsAfterReapingTheKernelReads) {
  constexpr size_t ringSize = 64;
  auto [content, batch] = chunkedFileAndBatch(100);
  auto [tmp, fd] = makeTempFile(content);
  RingScript script;
  auto policy = makePolicy(ringSize, script);
  // Nothing is in the kernel when the submit fails.
  script.submitResults = {-EBADF};
  AD_EXPECT_THROW_WITH_MESSAGE(batch.addTo(policy, fd, 0),
                               HasSubstr("io_uring_submit failed"));
  EXPECT_THAT(batch.result(), Each(Eq(std::string(4, '\0'))));
  EXPECT_EQ(script.numberOfTakenRequests, 0U);

  // The kernel took 64 reads before the submit of the next ones fails. (A
  // fresh policy, whose ring holds no no-ops of the first batch.) The real ring
  // typically completes these cached reads within the submit, so the wave that
  // makes room reaps all of them, and none is in the kernel when the submit
  // fails; that case is `brokenSubmitReapsTheKernelReadsBeforeThrowing`.
  RingScript script2;
  auto policy2 = makePolicy(ringSize, script2);
  ReadBatchForTesting again = batch;
  script2.submitResults = {std::nullopt, -EBADF};
  AD_EXPECT_THROW_WITH_MESSAGE(again.addTo(policy2, fd, 0),
                               HasSubstr("io_uring_submit failed"));
  EXPECT_EQ(script2.numberOfBackOffs, 0U);
  EXPECT_EQ(script2.numberOfTakenRequests, ringSize);
  EXPECT_EQ(script2.numberOfReapedCompletions, ringSize);
  for (size_t i : ad_utility::integerRange(size_t{100})) {
    EXPECT_EQ(again.result()[i],
              i < ringSize ? content.substr(4 * i, 4) : std::string(4, '\0'))
        << i;
  }

  // Both policies read the next batch: `policy2`, and `policy` after it hands
  // the 64 no-ops of its cancelled reads to the kernel.
  auto [content3, next2] = chunkedFileAndBatch(3);
  next2.addTo(policy2, fd, 1);
  policy2.wait(1);
  EXPECT_THAT(next2.result(), ElementsAre("AAAA", "BBBB", "CCCC"));
  auto [content2, next] = chunkedFileAndBatch(3);
  next.addTo(policy, fd, 2);
  policy.wait(2);
  EXPECT_THAT(next.result(), ElementsAre("AAAA", "BBBB", "CCCC"));
}

// _____________________________________________________________________________
// A broken submit while the kernel holds reads of the batch: the first submit
// takes 3 of the 6 reads and completes none of them, the second one fails with
// `EBADF`. `addBatch` reaps the 3 reads before it throws, and the 3 reads that
// never reached the kernel are cancelled.
TEST(IoUringPolicyOnFakeRing, brokenSubmitReapsTheKernelReadsBeforeThrowing) {
  using ad_utility::testing::FakeIoUring;
  auto [content, batch] = chunkedFileAndBatch(6);
  FakeIoUring::Faults faults;
  faults.scriptedSubmits = {3, -EBADF};
  faults.completeOnSubmit = false;
  auto [fake, policy] = makePolicyOnFakeRing(
      std::make_unique<FakeIoUring>(content, 8, faults, 0));
  AD_EXPECT_THROW_WITH_MESSAGE(batch.addTo(policy, -1, 0),
                               HasSubstr("io_uring_submit failed"));
  // Checked right after the throw, before any other call reaps completions.
  EXPECT_EQ(fake.numberOfReadsInRing(), 0U);
  EXPECT_EQ(fake.faultCounts().cancelledReads, 3U);
  EXPECT_THAT(batch.result(),
              ElementsAre("AAAA", "BBBB", "CCCC", std::string(4, '\0'),
                          std::string(4, '\0'), std::string(4, '\0')));
}

// _____________________________________________________________________________
// A wait that fails with an error other than `EINTR` terminates the process:
// the kernel may still write into the buffers of the reads it holds.
TEST_F(IoUringPolicyFailureTest, brokenWaitTerminates) {
  // The reads come from a pipe that is still empty, so that `wait()` finds no
  // completion ready and has to wait. (A ready completion is reaped without a
  // wait, and the child of the death test would consume it from the
  // completion queue, which it shares with this process.)
  std::array<int, 2> pipeFds{};
  ASSERT_EQ(pipe(pipeFds.data()), 0);
  absl::Cleanup closePipe{[&pipeFds] {
    close(pipeFds[0]);
    close(pipeFds[1]);
  }};
  ReadBatchForTesting batch;
  batch.add({{0, 4}});
  RingScript script;
  auto policy = makePolicy(64, script);
  batch.addTo(policy, pipeFds[0], 0);
  script.brokenWaitErrno = EIO;
  EXPECT_DEATH_IF_SUPPORTED(policy.wait(0), "io_uring_wait_cqes failed");
  script.brokenWaitErrno = 0;
  ASSERT_EQ(write(pipeFds[1], "AAAA", 4), 4);
  policy.wait(0);
  EXPECT_THAT(batch.result(), ElementsAre("AAAA"));
}

// _____________________________________________________________________________
// A wait that a signal interrupts is retried, both in `wait()` and in the
// destructor.
TEST_F(IoUringPolicyFailureTest, interruptedWaitsAreRetried) {
  // The reads come from a pipe that is filled only right before a wait reaches
  // the real ring, so that no completion is ready before and the policy has to
  // wait. (A ready completion is reaped without a wait.)
  std::array<int, 2> pipeFds{};
  ASSERT_EQ(pipe(pipeFds.data()), 0);
  absl::Cleanup closePipe{[&pipeFds] {
    close(pipeFds[0]);
    close(pipeFds[1]);
  }};
  RingScript script;
  ReadBatchForTesting waited;
  waited.add({{0, 4}, {0, 4}});
  ReadBatchForTesting unwaited;
  unwaited.add({{0, 4}, {0, 4}, {0, 4}});
  {
    auto policy = makePolicy(64, script);
    waited.addTo(policy, pipeFds[0], 0);
    script.numberOfWaitsToInterrupt = 2;
    script.beforeRealWait = [&pipeFds, &script] {
      ASSERT_EQ(write(pipeFds[1], "AAAAAAAA", 8), 8);
      script.beforeRealWait = nullptr;
    };
    policy.wait(0);
    EXPECT_THAT(waited.result(), ElementsAre("AAAA", "AAAA"));

    unwaited.addTo(policy, pipeFds[0], 1);
    // Set only after `addTo`, which does not wait here, so that the interrupt
    // hits the wait of the destructor.
    script.numberOfWaitsToInterrupt = 1;
    script.beforeRealWait = [&pipeFds, &script] {
      ASSERT_EQ(write(pipeFds[1], "BBBBBBBBBBBB", 12), 12);
      script.beforeRealWait = nullptr;
    };
  }
  // The destructor reaped all three reads of the unwaited batch.
  EXPECT_EQ(script.numberOfWaitsToInterrupt, 0U);
  EXPECT_EQ(script.numberOfTakenRequests, 5U);
  EXPECT_EQ(script.numberOfReapedCompletions, 5U);
  EXPECT_THAT(unwaited.result(), ElementsAre("BBBB", "BBBB", "BBBB"));
}

// _____________________________________________________________________________
// The failure of a read is thrown by the `wait()` of its own batch, whichever
// call reaps its completion, and is never dropped.
TEST(IoUringPolicyOnFakeRing, failedReadIsThrownByTheWaitOfItsOwnBatch) {
  using ad_utility::testing::FakeIoUring;
  const std::string file = "AAAABBBB";
  for (uint32_t seed : ad_utility::integerRange(20u)) {
    ad_utility::IoUringPolicy policy{
        std::make_unique<FakeIoUring>(file, 4, FakeIoUring::Faults{}, seed)};
    ReadBatchForTesting failing;
    failing.add({{0, 4}, {4, 16}, {0, 4}});
    ReadBatchForTesting succeeding;
    succeeding.add({{4, 4}, {0, 4}, {4, 4}});
    failing.addTo(policy, -1, 0);
    succeeding.addTo(policy, -1, 1);
    policy.wait(1);
    EXPECT_THAT(succeeding.result(), ElementsAre("BBBB", "AAAA", "BBBB"));
    AD_EXPECT_THROW_WITH_MESSAGE(policy.wait(0),
                                 HasSubstr("read fewer bytes than requested"));
  }
}

// _____________________________________________________________________________
// The first failed read of a batch is the one that its `wait()` throws.
TEST(IoUringPolicyOnFakeRing, firstFailureOfABatchIsThrown) {
  using ad_utility::testing::FakeIoUring;
  // A ring with a single SQE handles the reads one after the other.
  ad_utility::IoUringPolicy policy{
      std::make_unique<FakeIoUring>("AAAABBBB", 1, FakeIoUring::Faults{}, 0)};
  ReadBatchForTesting batch;
  batch.add({{0, 16}, {4, 16}, {0, 4}});
  batch.addTo(policy, -1, 0);
  AD_EXPECT_THROW_WITH_MESSAGE(policy.wait(0), HasSubstr("(8 of 16 bytes)"));
}

// _____________________________________________________________________________
// A read that the kernel fails is thrown with its `errno` by the `wait()` of
// its batch.
TEST(IoUringPolicyOnFakeRing, readErrorIsThrownWithItsErrno) {
  using ad_utility::testing::FakeIoUring;
  FakeIoUring::Faults faults;
  faults.readError = 1;
  ad_utility::IoUringPolicy policy{
      std::make_unique<FakeIoUring>("AAAABBBB", 4, faults, 0)};
  ReadBatchForTesting batch;
  batch.add({{0, 4}, {4, 4}});
  batch.addTo(policy, -1, 0);
  AD_EXPECT_THROW_WITH_MESSAGE(
      policy.wait(0),
      HasSubstr(absl::StrCat("I/O error in IoUringPolicy read operation: ",
                             std::generic_category().message(EIO))));
}

// _____________________________________________________________________________
// `FakeIoUring` keeps the contract of `IoUringInterface`: a full submission
// queue rejects a read, the kernel takes the SQEs in queue order, and
// `cancelQueued` returns the ids of the reads it rewrote.
TEST(FakeIoUring, keepsTheContractOfTheInterface) {
  using ad_utility::testing::FakeIoUring;
  FakeIoUring::Faults faults;
  faults.scriptedSubmits = {1};
  faults.completeOnSubmit = false;
  FakeIoUring fake{"AAAABBBBCCCC", 2, faults, 0};
  EXPECT_EQ(fake.sqCapacity(), 2U);
  EXPECT_EQ(fake.cqCapacity(), 4U);
  std::array<std::string, 3> buffers{std::string(4, '\0'), std::string(4, '\0'),
                                     std::string(4, '\0')};
  EXPECT_TRUE(fake.prepareRead(-1, buffers[0].data(), 4, 0, 10));
  EXPECT_TRUE(fake.prepareRead(-1, buffers[1].data(), 4, 4, 11));
  EXPECT_FALSE(fake.prepareRead(-1, buffers[2].data(), 4, 8, 12));
  EXPECT_EQ(fake.numberOfQueued(), 2U);

  // The scripted submit takes only the first SQE.
  EXPECT_EQ(fake.submit(), 1);
  EXPECT_EQ(fake.numberOfQueued(), 1U);
  EXPECT_EQ(fake.cancelQueued(99), std::vector<uint64_t>{11});
  EXPECT_EQ(fake.faultCounts().cancelledReads, 1U);

  // The read in the kernel completes in the wait, the no-op with the next
  // submit and wait.
  EXPECT_EQ(fake.waitForCompletions(1), 0);
  std::array<ad_utility::IoUringInterface::Completion, 2> completions{};
  ASSERT_EQ(fake.popCompletions(completions), 1U);
  EXPECT_EQ(completions[0].requestId, 10U);
  EXPECT_EQ(completions[0].result, 4);
  EXPECT_EQ(buffers[0], "AAAA");
  EXPECT_EQ(fake.submit(), 1);
  EXPECT_EQ(fake.waitForCompletions(1), 0);
  ASSERT_EQ(fake.popCompletions(completions), 1U);
  EXPECT_EQ(completions[0].requestId, 99U);
  EXPECT_EQ(completions[0].result, 0);
  EXPECT_EQ(buffers[1], std::string(4, '\0'));
  EXPECT_EQ(fake.numberOfReadsInRing(), 0U);
  EXPECT_FALSE(fake.injectedReadFault());
}

// _____________________________________________________________________________
// A read that ends past the end of the file is short, and counts as a faulty
// read. A wait for more completions than the ring holds, and a ring with fewer
// CQEs than SQEs, violate the contract.
TEST(FakeIoUring, readPastTheEndIsShortAndContractViolationsThrow) {
  using ad_utility::testing::FakeIoUring;
  FakeIoUring::Faults faults;
  faults.completeOnSubmit = false;
  FakeIoUring fake{"AAAABBBB", 2, faults, 0};
  std::string buffer(8, '\0');
  ASSERT_TRUE(fake.prepareRead(-1, buffer.data(), 8, 4, 0));
  EXPECT_EQ(fake.submit(), 1);
  AD_EXPECT_THROW_WITH_MESSAGE(fake.waitForCompletions(2),
                               HasSubstr("would block forever"));
  EXPECT_EQ(fake.waitForCompletions(1), 0);
  std::array<ad_utility::IoUringInterface::Completion, 1> completion{};
  ASSERT_EQ(fake.popCompletions(completion), 1U);
  EXPECT_EQ(completion[0].result, 4);
  EXPECT_EQ(buffer.substr(0, 4), "BBBB");
  EXPECT_TRUE(fake.injectedReadFault());
  EXPECT_EQ(fake.faultCounts().faultyReads, 1U);

  AD_EXPECT_THROW_WITH_MESSAGE((FakeIoUring{"", 4, faults, 0, 2}),
                               HasSubstr("at least as many CQEs as SQEs"));
  faults.readError = 2;
  AD_EXPECT_THROW_WITH_MESSAGE((FakeIoUring{"", 4, faults, 0}),
                               HasSubstr("must be in [0, 1]"));
}

// _____________________________________________________________________________
// On a simulated ring with random faults (partial and interrupted submits,
// bursts of `EAGAIN` longer than the retries, interrupted waits, short and
// failed reads, completions in any order), no read is left in the ring after a
// batch. A batch fails iff one of its reads failed, or the submit failed at
// least `MAX_SUBMIT_RETRIES + 1` times. Half of the seeds use a completion
// queue that is no larger than the submission queue.
TEST(IoUringPolicyOnFakeRing, randomFaults) {
  using ad_utility::testing::FakeIoUring;
  std::string file;
  for (size_t i : ad_utility::integerRange(size_t{1000})) {
    file.push_back(static_cast<char>('a' + (i * 7) % 26));
  }
  FakeIoUring::Faults faults;
  faults.partialSubmit = 0.3;
  faults.submitInterrupted = 0.1;
  faults.waitInterrupted = 0.2;
  faults.waitReturnsEarly = 0.2;
  faults.shortRead = 0.01;
  faults.readError = 0.01;
  FakeIoUring::FaultCounts totals;
  for (uint32_t seed : ad_utility::integerRange(300u)) {
    // Short bursts of `EAGAIN` that the retries ride out, and a few longer
    // than the retries; for every third seed also bursts that span several
    // batches, so that a batch starts with a ring full of no-ops.
    faults.submitOutOfMemory = seed % 3 == 0 ? 0.01 : 0.05;
    faults.maxEagainBurst = seed % 3 == 0 ? 100 : 20;
    auto [fake, policy] = makePolicyOnFakeRing(std::make_unique<FakeIoUring>(
        file, 8, faults, seed, seed % 2 == 0 ? size_t{16} : size_t{8}));
    std::mt19937 random{seed};
    for (uint64_t handle : ad_utility::integerRange(uint64_t{5})) {
      ReadBatchForTesting batch;
      std::vector<std::string> expected;
      const size_t numberOfReads = random() % 30 + 1;
      for ([[maybe_unused]] size_t i :
           ad_utility::integerRange(numberOfReads)) {
        const size_t offset = random() % 990;
        const size_t numberOfBytes = random() % 10 + 1;
        batch.add(offset, numberOfBytes);
        expected.push_back(file.substr(offset, numberOfBytes));
      }
      fake.resetObservations();
      const FakeIoUring::FaultCounts before = fake.faultCounts();
      std::optional<std::string> error;
      try {
        batch.addTo(policy, -1, handle);
        policy.wait(handle);
      } catch (const std::exception& e) {
        error = e.what();
      }
      ASSERT_EQ(fake.numberOfReadsInRing(), 0U) << seed;
      const bool submitFailed =
          error.has_value() &&
          error.value().find("io_uring_submit failed") != std::string::npos;
      // Only failed submits and partial submits (which may take no SQE) count
      // towards the retries.
      const size_t numberOfFailedSubmits =
          fake.faultCounts().outOfMemorySubmits - before.outOfMemorySubmits +
          fake.faultCounts().partialSubmits - before.partialSubmits;
      if (submitFailed) {
        EXPECT_GE(numberOfFailedSubmits, MAX_SUBMIT_RETRIES + 1) << seed;
      } else if (fake.injectedReadFault()) {
        EXPECT_TRUE(error.has_value()) << seed;
      } else {
        EXPECT_FALSE(error.has_value()) << seed << ": " << error.value();
        EXPECT_EQ(batch.result(), expected) << seed;
      }
    }
    const auto& counts = fake.faultCounts();
    totals.partialSubmits += counts.partialSubmits;
    totals.interruptedSubmits += counts.interruptedSubmits;
    totals.interruptedWaits += counts.interruptedWaits;
    totals.earlyWaits += counts.earlyWaits;
    totals.faultyReads += counts.faultyReads;
    totals.cancelledReads += counts.cancelledReads;
  }
  // Every fault actually occurred.
  EXPECT_GT(totals.partialSubmits, 0U);
  EXPECT_GT(totals.interruptedSubmits, 0U);
  EXPECT_GT(totals.interruptedWaits, 0U);
  EXPECT_GT(totals.earlyWaits, 0U);
  EXPECT_GT(totals.faultyReads, 0U);
  EXPECT_GT(totals.cancelledReads, 0U);
}

// When the ring is full and no completion is ready, `addBatch` waits for a
// wave of `REAP_WAVE` completions, not for one, and so does `wait()`: 64 reads
// through a ring of 16 take (64 - 16) / `REAP_WAVE` waits in `addBatch` and
// 16 / `REAP_WAVE` in `wait()`. Reaping one completion per wait would take 64.
TEST(IoUringPolicyWaves, fullRingIsRefilledInWaves) {
  using ad_utility::testing::FakeIoUring;
  static_assert(16 % REAP_WAVE == 0);
  auto [file, batch] = chunkedFileAndBatch(64);
  FakeIoUring::Faults faults;
  faults.completeOnSubmit = false;
  auto fakeOwner = std::make_unique<FakeIoUring>(file, 16, faults, 0);
  const FakeIoUring& fake = *fakeOwner;
  ad_utility::IoUringPolicy policy{std::move(fakeOwner)};
  batch.addTo(policy, -1, 0);
  EXPECT_EQ(fake.callCounts().waits, (64 - 16) / REAP_WAVE);
  policy.wait(0);
  EXPECT_EQ(fake.callCounts().waits, 64 / REAP_WAVE);
  EXPECT_EQ(fake.callCounts().chunks, 64 / REAP_WAVE);
  EXPECT_THAT(batch.result(), ElementsAreArray(chunksOf(file)));
}

// Completions that are ready are reaped without waiting, all of them, in
// chunks of `REAP_CHUNK`: 200 ready completions take no wait and
// ceil(200 / `REAP_CHUNK`) chunks.
TEST(IoUringPolicyWaves, readyCompletionsAreReapedInChunksWithoutWaiting) {
  using ad_utility::testing::FakeIoUring;
  auto [file, batch] = chunkedFileAndBatch(200);
  FakeIoUring::Faults faults;
  faults.completeAllOnSubmit = true;
  auto fakeOwner = std::make_unique<FakeIoUring>(file, 256, faults, 0);
  const FakeIoUring& fake = *fakeOwner;
  ad_utility::IoUringPolicy policy{std::move(fakeOwner)};
  batch.addTo(policy, -1, 0);
  policy.wait(0);
  EXPECT_EQ(fake.callCounts().waits, 0U);
  EXPECT_EQ(fake.callCounts().chunks, (200 + REAP_CHUNK - 1) / REAP_CHUNK);
  EXPECT_THAT(batch.result(), ElementsAreArray(chunksOf(file)));
}

// A signal can end the wait for a wave early: with at least one completion
// ready, `io_uring_wait_cqes` can return 0 although fewer completions than the
// wave are ready. The policy reaps what is ready and waits again: here every
// wait returns after one completion, so 16 reads take 16 waits.
TEST(IoUringPolicyWaves, waitThatReturnsEarlyIsRepeated) {
  using ad_utility::testing::FakeIoUring;
  auto [file, batch] = chunkedFileAndBatch(16);
  FakeIoUring::Faults faults;
  faults.completeOnSubmit = false;
  faults.waitReturnsEarly = 1.0;
  auto fakeOwner = std::make_unique<FakeIoUring>(file, 64, faults, 0);
  const FakeIoUring& fake = *fakeOwner;
  ad_utility::IoUringPolicy policy{std::move(fakeOwner)};
  batch.addTo(policy, -1, 0);
  policy.wait(0);
  EXPECT_EQ(fake.callCounts().waits, 16U);
  EXPECT_EQ(fake.faultCounts().earlyWaits, 16U);
  EXPECT_THAT(batch.result(), ElementsAreArray(chunksOf(file)));
}

// `wait()` waits for at most the remaining reads of its own batch, not for a
// wave of the reads of other batches: next to a batch of 15 reads, the `wait()`
// of a batch of 1 read waits for 1 completion at a time.
TEST(IoUringPolicyWaves, waitWaitsOnlyForItsOwnBatch) {
  using ad_utility::testing::FakeIoUring;
  auto [file, single] = chunkedFileAndBatch(1);
  auto [file2, large] = chunkedFileAndBatch(15);
  FakeIoUring::Faults faults;
  faults.completeOnSubmit = false;
  auto fakeOwner = std::make_unique<FakeIoUring>(file2, 64, faults, 0);
  const FakeIoUring& fake = *fakeOwner;
  ad_utility::IoUringPolicy policy{std::move(fakeOwner)};
  single.addTo(policy, -1, 0);
  large.addTo(policy, -1, 1);
  policy.wait(0);
  // The simulated kernel completes the requests in random order, so `wait()`
  // may need several waits until it gets the one read of its batch.
  EXPECT_THAT(fake.callCounts().waitSizes, AllOf(Not(IsEmpty()), Each(1U)));
  policy.wait(1);
  EXPECT_THAT(single.result(), ElementsAre(file.substr(0, 4)));
  EXPECT_THAT(large.result(), ElementsAreArray(chunksOf(file2)));
}
#endif
}  // namespace
