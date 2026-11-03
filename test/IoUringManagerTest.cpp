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
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <memory>
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
  EXPECT_THAT(logStream.str(), ifLogLevelIsCompiledIn<LogLevel::Enum::WARN>(
                                   ::testing::HasSubstr("still in flight")));
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
// The real liburing calls of an `IoUringPolicy`, wrapped so that a test can
// inject the failures that a real kernel produces only rarely, and count what
// reached the kernel. Must outlive the policy that uses `calls()`.
class FaultInjectingLiburingCalls {
 public:
  // The number of the next waits that fail with `-EINTR` (as when a signal
  // arrives) without waiting.
  size_t numberOfWaitsToInterrupt_ = 0;
  // The number of the next submits that fail with `-EINTR` without submitting.
  size_t numberOfSubmitsToInterrupt_ = 0;
  // If set, the number of submits that succeed before every submit fails
  // with `-EAGAIN` (as when the kernel cannot allocate requests).
  std::optional<size_t> numberOfSubmitsBeforeFailure_;
  // The number of prepared SQEs that the next submit does not hand to the
  // kernel, as in a partial submit.
  unsigned numberOfSqesToHoldBack_ = 0;
  // Called (once) by the first submit that fails.
  std::function<void()> onFailedSubmit_;

  // The number of SQEs that the kernel took, and of CQEs that the policy got
  // from a wait (and consumed). Every read that reached the kernel is reaped
  // iff they are equal.
  size_t numberOfSubmittedSqes_ = 0;
  size_t numberOfReapedCqes_ = 0;
  // Waits that were refused because no read was in the kernel, so a real wait
  // would have blocked forever.
  size_t numberOfWaitsWithoutReadsInTheKernel_ = 0;

  ad_utility::IoUringPolicy::LiburingCalls calls() {
    return {[this](io_uring* ring) { return submit(ring); },
            [this](io_uring* ring, io_uring_cqe** cqe) {
              return waitCqe(ring, cqe);
            }};
  }

 private:
  int submit(io_uring* ring) {
    if (numberOfSubmitsToInterrupt_ > 0) {
      --numberOfSubmitsToInterrupt_;
      return -EINTR;
    }
    if (numberOfSubmitsBeforeFailure_.has_value()) {
      if (numberOfSubmitsBeforeFailure_.value() == 0) {
        if (onFailedSubmit_) {
          std::exchange(onFailedSubmit_, nullptr)();
        }
        return -EAGAIN;
      }
      --numberOfSubmitsBeforeFailure_.value();
    }
    // `io_uring_submit` hands the SQEs up to `sq.sqe_tail` to the kernel. To
    // hold some of them back, lower the tail during the call (the held back
    // SQEs must not have been flushed, i.e. they are after `sq.sqe_head`).
    const unsigned numberOfHeldBack = std::min(
        numberOfSqesToHoldBack_, ring->sq.sqe_tail - ring->sq.sqe_head);
    numberOfSqesToHoldBack_ = 0;
    ring->sq.sqe_tail -= numberOfHeldBack;
    const int ret = io_uring_submit(ring);
    ring->sq.sqe_tail += numberOfHeldBack;
    if (ret > 0) {
      numberOfSubmittedSqes_ += static_cast<size_t>(ret);
    }
    return ret;
  }

  int waitCqe(io_uring* ring, io_uring_cqe** cqe) {
    if (numberOfWaitsToInterrupt_ > 0) {
      --numberOfWaitsToInterrupt_;
      return -EINTR;
    }
    // Refuse instead of blocking the test forever.
    if (numberOfReapedCqes_ == numberOfSubmittedSqes_) {
      ++numberOfWaitsWithoutReadsInTheKernel_;
      return -ETIME;
    }
    const int ret = io_uring_wait_cqe(ring, cqe);
    if (ret == 0) {
      ++numberOfReapedCqes_;
    }
    return ret;
  }
};

// Tests of `IoUringPolicy` itself for failed reads and failing liburing calls.
// Each one checks that the kernel is done with every read of a batch before
// the batch's error is thrown, since the callers free the target buffers when
// they unwind.
class IoUringPolicyFailureTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!ioUringAvailableAtRuntime()) {
      GTEST_SKIP() << "io_uring is compiled in, but not available at runtime "
                      "(e.g. blocked by seccomp inside Docker)";
    }
  }
};

// A read past the end of the file is short. `wait()` throws its error only
// after the other reads of the batch have completed too.
TEST_F(IoUringPolicyFailureTest, failedReadIsThrownAfterAllReadsOfItsBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();
  FaultInjectingLiburingCalls liburing;
  {
    ad_utility::IoUringPolicy policy(64, liburing.calls());
    ReadBatchForTesting batch;
    batch.add({{0, 16}, {0, 4}, {4, 4}});
    batch.addTo(policy, fd, 0);
    AD_EXPECT_THROW_WITH_MESSAGE(policy.wait(0),
                                 HasSubstr("read fewer bytes than requested"));
    EXPECT_EQ(liburing.numberOfSubmittedSqes_, 3U);
    EXPECT_EQ(liburing.numberOfReapedCqes_, 3U);
    EXPECT_THAT(batch.result()[1], Eq("AAAA"));
    EXPECT_THAT(batch.result()[2], Eq("BBBB"));
  }
  // Nothing was left for the destructor to reap.
  EXPECT_THAT(logStream.str(), Not(HasSubstr("still in flight")));
}

// A failed read is thrown by the `wait()` of its own batch, not by the
// `wait()` of another batch that happens to reap its completion, and it does
// not affect the batches after it.
TEST_F(IoUringPolicyFailureTest, failedReadIsThrownByTheWaitOfItsOwnBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  ad_utility::IoUringPolicy policy(64);

  ReadBatchForTesting failing;
  failing.add({{0, 16}, {4, 16}, {8, 16}});
  ReadBatchForTesting succeeding;
  succeeding.add({{4, 4}});
  failing.addTo(policy, fd, 0);
  succeeding.addTo(policy, fd, 1);

  // The completions of batch 0 are posted first, so this `wait()` reaps them.
  policy.wait(1);
  EXPECT_THAT(succeeding.result(), ElementsAre("BBBB"));
  AD_EXPECT_THROW_WITH_MESSAGE(policy.wait(0),
                               HasSubstr("read fewer bytes than requested"));

  ReadBatchForTesting next;
  next.add({{0, 4}});
  next.addTo(policy, fd, 2);
  policy.wait(2);
  EXPECT_THAT(next.result(), ElementsAre("AAAA"));
}

// A wait that a signal interrupts is retried, both in `wait()` and in the
// destructor.
TEST_F(IoUringPolicyFailureTest, interruptedWaitsAreRetried) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");
  FaultInjectingLiburingCalls liburing;
  ReadBatchForTesting waited;
  waited.add({{0, 4}, {4, 4}});
  ReadBatchForTesting unwaited;
  unwaited.add({{8, 4}});
  {
    ad_utility::IoUringPolicy policy(64, liburing.calls());
    waited.addTo(policy, fd, 0);
    liburing.numberOfWaitsToInterrupt_ = 2;
    policy.wait(0);
    EXPECT_THAT(waited.result(), ElementsAre("AAAA", "BBBB"));

    unwaited.addTo(policy, fd, 1);
    liburing.numberOfWaitsToInterrupt_ = 1;
  }
  EXPECT_EQ(liburing.numberOfSubmittedSqes_, 3U);
  EXPECT_EQ(liburing.numberOfReapedCqes_, 3U);
  EXPECT_THAT(unwaited.result(), ElementsAre("CCCC"));
}

// A submit that a signal interrupts, or that hands only some of the prepared
// SQEs to the kernel, is repeated until the kernel has taken all of them.
TEST_F(IoUringPolicyFailureTest, interruptedAndPartialSubmitsAreCompleted) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDDEEEE");
  FaultInjectingLiburingCalls liburing;
  ad_utility::IoUringPolicy policy(64, liburing.calls());

  ReadBatchForTesting interrupted;
  interrupted.add({{0, 4}, {4, 4}});
  liburing.numberOfSubmitsToInterrupt_ = 1;
  interrupted.addTo(policy, fd, 0);
  policy.wait(0);
  EXPECT_THAT(interrupted.result(), ElementsAre("AAAA", "BBBB"));

  ReadBatchForTesting partial;
  partial.add({{0, 4}, {4, 4}, {8, 4}, {12, 4}, {16, 4}});
  liburing.numberOfSqesToHoldBack_ = 2;
  partial.addTo(policy, fd, 1);
  policy.wait(1);
  EXPECT_THAT(partial.result(),
              ElementsAre("AAAA", "BBBB", "CCCC", "DDDD", "EEEE"));
  EXPECT_EQ(liburing.numberOfSubmittedSqes_, 7U);
  EXPECT_EQ(liburing.numberOfWaitsWithoutReadsInTheKernel_, 0U);
}

// A submit fails for good in the middle of a batch that is larger than the
// ring, while earlier reads of the same batch and a read of another batch are
// in the kernel. `addBatch` throws only after the kernel is done with all of
// them, the reads that were never submitted are never submitted later, and the
// policy works again for the next batch.
TEST_F(IoUringPolicyFailureTest, failedSubmitReapsTheReadsInTheKernelFirst) {
  constexpr size_t numberOfReads = 10;
  SequentialReadScenarioForTesting scenario;
  for (size_t i = 0; i < numberOfReads; ++i) {
    scenario.addRead(std::string(4, static_cast<char>('A' + i)));
  }
  auto [tmp, fd] = makeTempFile(scenario.content());
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();
  FaultInjectingLiburingCalls liburing;
  {
    ad_utility::IoUringPolicy policy(4, liburing.calls());
    // The read of the other batch is from an empty pipe, so it stays in the
    // kernel until the failing submit writes into the pipe. Its completion
    // therefore comes after those of the failing batch.
    std::array<int, 2> pipeFds{};
    ASSERT_EQ(pipe(pipeFds.data()), 0);
    absl::Cleanup closePipe{[&pipeFds] {
      close(pipeFds[0]);
      close(pipeFds[1]);
    }};
    ReadBatchForTesting other;
    other.add({{0, 4}});
    other.addTo(policy, pipeFds[0], 0);
    liburing.onFailedSubmit_ = [&pipeFds] {
      ASSERT_EQ(write(pipeFds[1], "PIPE", 4), 4);
    };

    // The ring holds 4 reads: the first submit of the batch succeeds, the
    // second one (when the ring is full again) fails.
    liburing.numberOfSubmitsBeforeFailure_ = 1;
    ReadBatchForTesting failing;
    for (size_t i = 0; i < numberOfReads; ++i) {
      failing.add(4 * i, 4);
    }
    AD_EXPECT_THROW_WITH_MESSAGE(failing.addTo(policy, fd, 1),
                                 HasSubstr("io_uring_submit failed"));
    EXPECT_EQ(liburing.numberOfSubmittedSqes_, 4U);
    // Also the read of the other batch was reaped (before the ring was torn
    // down). Otherwise, the `wait()` below would block forever.
    ASSERT_EQ(liburing.numberOfReapedCqes_, 4U);
    EXPECT_THAT(::ranges::to_vector(failing.result() | ql::views::take(3)),
                ElementsAre("AAAA", "BBBB", "CCCC"));
    policy.wait(0);
    EXPECT_THAT(other.result(), ElementsAre("PIPE"));

    // The next batch runs on a fresh ring, which never sees the SQE that was
    // prepared but not submitted.
    liburing.numberOfSubmitsBeforeFailure_ = std::nullopt;
    scenario.addTo(policy, fd, 2);
    policy.wait(2);
    EXPECT_THAT(scenario.results(), ElementsAreArray(scenario.expected()));
    EXPECT_EQ(liburing.numberOfSubmittedSqes_, 4 + numberOfReads);
  }
  EXPECT_EQ(liburing.numberOfReapedCqes_, 4 + numberOfReads);
  EXPECT_EQ(liburing.numberOfWaitsWithoutReadsInTheKernel_, 0U);
  EXPECT_THAT(logStream.str(), Not(HasSubstr("still in flight")));
}
#endif
}  // namespace
