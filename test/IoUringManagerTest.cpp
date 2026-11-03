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

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <deque>
#include <filesystem>
#include <functional>
#include <initializer_list>
#include <iterator>
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

  // Read all accumulated reads from file `fd` with `reader`: a batch manager,
  // which takes the three spans, or a read policy, which takes them as a
  // `BatchOfReads`.
  template <typename Reader>
  void readWith(Reader& reader, int fd) {
    std::vector<char*> pointers = targetBufferPointers();
    if constexpr (std::is_base_of_v<ad_utility::BatchManagerBase, Reader>) {
      reader.readBatch(fd, numBytes_, offsets_, pointers);
    } else {
      reader.readBatch(fd,
                       ad_utility::BatchOfReads{numBytes_, offsets_, pointers});
    }
  }

  // The bytes read by each read, in request order (valid once the batch has
  // completed).
  const std::vector<std::string>& result() const { return targetBuffers_; }

 private:
  // Build the buffer pointers only after all buffers have been added, so the
  // addresses are stable (no further `add` will reallocate `targetBuffers_`).
  // `readBatch` copies each address into its read request, so the returned
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

  // Read all reads with `reader` (a batch manager or a read policy) from file
  // `fd`.
  template <typename Reader>
  void readWith(Reader& reader, int fd) {
    batch_.readWith(reader, fd);
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

  auto makeAndWait = [&](uint64_t offset, size_t numBytes,
                         std::string_view expected) {
    ReadBatchForTesting batch;
    batch.add(offset, numBytes);
    batch.readWith(manager, fd);
    EXPECT_THAT(batch.result(), ::testing::ElementsAre(expected));
  };

  makeAndWait(0, 4, "AAAA");
  makeAndWait(4, 4, "BBBB");
  makeAndWait(8, 4, "CCCC");
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

// _____________________________________________________________________________
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
    scenario.readWith(manager, fd);
    EXPECT_THAT(scenario.results(),
                ::testing::ElementsAreArray(scenario.expected()))
        << "ring size " << ringSize;
  }
}

// _____________________________________________________________________________
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
  // Called once, before the next wait that reaches the real ring, e.g. to
  // write the bytes that the reads of a pipe wait for. It may install the next
  // callback.
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
      // Taken out before it runs, so that it can install the next one.
      std::exchange(script_.beforeRealWait, nullptr)();
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

// A pipe that stays empty until the test writes into it. Both ends are closed
// when it is destroyed.
class EmptyPipe {
 public:
  EmptyPipe() { AD_CORRECTNESS_CHECK(pipe(fds_.data()) == 0); }
  ~EmptyPipe() {
    close(fds_[0]);
    close(fds_[1]);
  }
  EmptyPipe(const EmptyPipe&) = delete;
  EmptyPipe& operator=(const EmptyPipe&) = delete;

  int readEnd() const noexcept { return fds_[0]; }
  int writeEnd() const noexcept { return fds_[1]; }

 private:
  std::array<int, 2> fds_{};
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

constexpr size_t MAX_SUBMIT_RETRIES =
    ad_utility::IoUringPolicy::MAX_SUBMIT_RETRIES;

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

// _____________________________________________________________________________
// A read past the end of the file is short. `readBatch` throws its error only
// once the other reads of the batch have completed too.
TEST_F(IoUringPolicyFailureTest, failedReadIsThrownAfterAllReadsOfItsBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  RingScript script;
  ReadBatchForTesting batch;
  batch.add({{0, 16}, {0, 4}, {4, 4}});
  auto policy = makePolicy(64, script);
  AD_EXPECT_THROW_WITH_MESSAGE(batch.readWith(policy, fd),
                               HasSubstr("read fewer bytes than requested in "
                                         "IoUringPolicy (8 of 16 bytes)"));
  EXPECT_EQ(script.numberOfTakenRequests, 3U);
  EXPECT_EQ(script.numberOfReapedCompletions, 3U);
  EXPECT_THAT(batch.result()[1], Eq("AAAA"));
  EXPECT_THAT(batch.result()[2], Eq("BBBB"));
}

// _____________________________________________________________________________
// A submit keeps failing with `EAGAIN` in the middle of a batch that is larger
// than the ring, while earlier reads of the batch are in the kernel. They are
// reaped, the queued reads are rewritten into no-ops, and the batch is read
// again with `pread`. The next batch hands the no-ops to the kernel. (How many
// reads are queued when the submit fails depends on how many completions the
// wave that freed their slots reaped.)
TEST_F(IoUringPolicyFailureTest, failedSubmitIsReadAgainWithPread) {
  constexpr size_t numberOfReads = 100;
  constexpr size_t ringSize = 64;
  auto [content, failing] = chunkedFileAndBatch(numberOfReads);
  auto [tmp, fd] = makeTempFile(content);
  RingScript script;
  auto [content2, next] = chunkedFileAndBatch(3);
  auto policy = makePolicy(ringSize, script);
  // The first submit takes a full ring of reads. When the ring is full again,
  // the submit fails, also on every retry.
  script.submitResults.push_back(std::nullopt);
  script.submitResults.insert(script.submitResults.end(),
                              MAX_SUBMIT_RETRIES + 1, -EAGAIN);
  failing.readWith(policy, fd);
  EXPECT_TRUE(script.submitResults.empty());
  EXPECT_EQ(script.numberOfBackOffs, MAX_SUBMIT_RETRIES);
  EXPECT_EQ(script.numberOfTakenRequests, ringSize);
  EXPECT_EQ(script.numberOfReapedCompletions, ringSize);
  EXPECT_THAT(failing.result(), ElementsAreArray(chunksOf(content)));

  // The next batch hands the no-ops of the cancelled reads to the kernel.
  next.readWith(policy, fd);
  EXPECT_THAT(next.result(), ElementsAre("AAAA", "BBBB", "CCCC"));
  EXPECT_GT(script.numberOfTakenRequests, ringSize + 3);
  EXPECT_LE(script.numberOfTakenRequests, numberOfReads + 3);
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
  small.readWith(policy, fd);
  EXPECT_THAT(small.result(), ElementsAre("AAAA", "BBBB", "CCCC"));
  EXPECT_EQ(script.numberOfBackOffs, 3U);

  // Ten failures, progress, ten more failures: 20 in total, but never more
  // than `MAX_SUBMIT_RETRIES` in a row. A refill of the ring of 8 prepares at
  // most 8 reads, so `readBatch` submits at least 1 + (100 - 8) / 8 times, and
  // the script is used up.
  auto [content2, large] = chunkedFileAndBatch(100);
  auto [tmp2, fd2] = makeTempFile(content2);
  auto smallRingPolicy = makePolicy(8, script);
  script.submitResults = {std::nullopt};
  script.submitResults.insert(script.submitResults.end(), 10, -EAGAIN);
  script.submitResults.push_back(std::nullopt);
  script.submitResults.insert(script.submitResults.end(), 10, -ENOMEM);
  large.readWith(smallRingPolicy, fd2);
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
  batch.readWith(policy, -1);
  EXPECT_THAT(batch.result(), ElementsAre("AAAA", "BBBB", "CCCC"));
}

// _____________________________________________________________________________
// After a whole ring of 8 reads was cancelled (and read with `pread`), the ring
// holds only queued no-ops. The next batch hands them to the kernel first,
// also if a submit takes only some of them.
TEST(IoUringPolicyOnFakeRing, ringFullOfNoOpsIsSubmittedPartially) {
  using ad_utility::testing::FakeIoUring;
  const std::string file = "AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJ";
  auto [tmp, fd] = makeTempFile(file);
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
  cancelled.readWith(policy, fd);
  EXPECT_THAT(cancelled.result(), ElementsAreArray(chunksOf(file)));
  EXPECT_EQ(fake.faultCounts().cancelledReads, 8U);
  EXPECT_EQ(fake.numberOfQueued(), 8U);
  EXPECT_EQ(fake.numberOfReadsInRing(), 0U);

  for ([[maybe_unused]] size_t round : ad_utility::integerRange(size_t{2})) {
    ReadBatchForTesting next;
    next.add({{0, 4}, {36, 4}, {8, 4}});
    next.readWith(policy, fd);
    EXPECT_THAT(next.result(), ElementsAre("AAAA", "JJJJ", "CCCC"));
    EXPECT_EQ(fake.numberOfReadsInRing(), 0U);
  }
  EXPECT_EQ(fake.numberOfQueued(), 0U);
}

// _____________________________________________________________________________
// A submit error other than `EINTR`, `EAGAIN` and `ENOMEM` is not retried: the
// ring is unusable. The queued reads are cancelled, the reads of the batch
// that the kernel holds are reaped, the ring is torn down, and this batch and
// every later one are read with `pread`. The warning is logged once.
TEST_F(IoUringPolicyFailureTest, brokenSubmitRetiresTheRing) {
  constexpr size_t ringSize = 64;
  auto [content, batch] = chunkedFileAndBatch(100);
  auto [tmp, fd] = makeTempFile(content);
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();

  // Nothing is in the kernel when the submit fails.
  RingScript script;
  auto policy = makePolicy(ringSize, script);
  script.submitResults = {-EBADF};
  batch.readWith(policy, fd);
  EXPECT_THAT(batch.result(), ElementsAreArray(chunksOf(content)));
  EXPECT_EQ(script.numberOfTakenRequests, 0U);

  // The kernel took 64 reads before the submit of the next ones fails. (A
  // fresh policy.) The real ring typically completes these cached reads within
  // the submit, so the wave that makes room reaps all of them, and none is in
  // the kernel when the ring breaks; that case is
  // `brokenSubmitReapsTheKernelReadsBeforeRetiring`.
  RingScript script2;
  auto policy2 = makePolicy(ringSize, script2);
  ReadBatchForTesting again = batch;
  script2.submitResults = {std::nullopt, -EBADF};
  again.readWith(policy2, fd);
  EXPECT_EQ(script2.numberOfBackOffs, 0U);
  EXPECT_EQ(script2.numberOfTakenRequests, ringSize);
  EXPECT_EQ(script2.numberOfReapedCompletions, ringSize);
  EXPECT_THAT(again.result(), ElementsAreArray(chunksOf(content)));

  // Both policies keep working with `pread`; their rings are gone, so the
  // scripts see nothing more.
  for (auto* retired : {&policy, &policy2}) {
    auto [content3, next] = chunkedFileAndBatch(3);
    next.readWith(*retired, fd);
    EXPECT_THAT(next.result(), ElementsAre("AAAA", "BBBB", "CCCC"));
  }
  EXPECT_EQ(script.numberOfTakenRequests, 0U);
  EXPECT_EQ(script2.numberOfTakenRequests, ringSize);
  const std::string log = logStream.str();
  EXPECT_THAT(log,
              ifLogLevelIsCompiledIn<ad_utility::LogLevel::Enum::WARN>(
                  AllOf(HasSubstr("io_uring_submit failed in IoUringPolicy"),
                        HasSubstr("reads with pread from now on"))));
}

// A `FakeIoUring` that records, when the policy destroys it, how many reads it
// still holds.
class RecordingRing final : public ad_utility::IoUringInterface {
 public:
  RecordingRing(std::unique_ptr<ad_utility::testing::FakeIoUring> fake,
                std::optional<size_t>& numberOfReadsInRingAtDestruction)
      : fake_{std::move(fake)},
        numberOfReadsInRingAtDestruction_{numberOfReadsInRingAtDestruction} {}
  ~RecordingRing() override {
    numberOfReadsInRingAtDestruction_ = fake_->numberOfReadsInRing();
  }
  bool prepareRead(int fd, char* buffer, size_t numberOfBytes, uint64_t offset,
                   uint64_t requestId) override {
    return fake_->prepareRead(fd, buffer, numberOfBytes, offset, requestId);
  }
  size_t numberOfQueued() const noexcept override {
    return fake_->numberOfQueued();
  }
  size_t sqCapacity() const noexcept override { return fake_->sqCapacity(); }
  size_t cqCapacity() const noexcept override { return fake_->cqCapacity(); }
  int submit() override { return fake_->submit(); }
  int waitForCompletions(unsigned minComplete) override {
    return fake_->waitForCompletions(minComplete);
  }
  size_t popCompletions(ql::span<Completion> completions) override {
    return fake_->popCompletions(completions);
  }
  std::vector<uint64_t> cancelQueued(uint64_t noOpRequestId) override {
    return fake_->cancelQueued(noOpRequestId);
  }
  void backOff() override { fake_->backOff(); }

 private:
  std::unique_ptr<ad_utility::testing::FakeIoUring> fake_;
  std::optional<size_t>& numberOfReadsInRingAtDestruction_;
};

// _____________________________________________________________________________
// A broken submit while the kernel holds reads of the batch: the first submit
// takes 3 of the 6 reads and completes none of them, the second one fails with
// `EBADF`. The policy reaps the 3 reads before it tears the ring down, so the
// ring holds no read when it is destroyed, and the batch is read with `pread`.
TEST(IoUringPolicyOnFakeRing, brokenSubmitReapsTheKernelReadsBeforeRetiring) {
  using ad_utility::testing::FakeIoUring;
  auto [content, batch] = chunkedFileAndBatch(6);
  auto [tmp, fd] = makeTempFile(content);
  FakeIoUring::Faults faults;
  faults.scriptedSubmits = {3, -EBADF};
  faults.completeOnSubmit = false;
  std::optional<size_t> numberOfReadsInRingAtDestruction;
  ad_utility::IoUringPolicy policy{std::make_unique<RecordingRing>(
      std::make_unique<FakeIoUring>(content, 8, faults, 0),
      numberOfReadsInRingAtDestruction)};
  batch.readWith(policy, fd);
  ASSERT_TRUE(numberOfReadsInRingAtDestruction.has_value());
  EXPECT_EQ(numberOfReadsInRingAtDestruction.value(), 0U);
  EXPECT_THAT(batch.result(), ElementsAreArray(chunksOf(content)));
}

// A fatal submit error while the kernel holds only no-ops (of reads that were
// cancelled in an earlier batch) retires the ring without waiting for the
// no-ops: here the wait would fail and terminate. Batch 1: every submit fails
// with `EAGAIN` (17 times: the first try and 16 retries), so its reads are
// cancelled and read with `pread`, and their 2 no-ops stay queued. Batch 2:
// the first submit takes only those 2 no-ops, the second one fails with
// `EBADF`.
TEST(IoUringPolicyOnFakeRing, brokenSubmitWithOnlyNoOpsInTheKernel) {
  using ad_utility::testing::FakeIoUring;
  auto [content, unused] = chunkedFileAndBatch(4);
  auto [tmp, fd] = makeTempFile(content);
  FakeIoUring::Faults faults;
  faults.scriptedSubmits.assign(17, -EAGAIN);
  faults.scriptedSubmits.push_back(2);
  faults.scriptedSubmits.push_back(-EBADF);
  faults.brokenWaitErrno = EBADF;
  faults.completeOnSubmit = false;
  ad_utility::IoUringPolicy policy{
      std::make_unique<FakeIoUring>(content, 8, faults, 0)};
  ReadBatchForTesting first;
  first.add({{0, 4}, {4, 4}});
  first.readWith(policy, fd);
  EXPECT_THAT(first.result(), ElementsAre("AAAA", "BBBB"));
  ReadBatchForTesting second;
  second.add({{8, 4}, {12, 4}});
  second.readWith(policy, fd);
  EXPECT_THAT(second.result(), ElementsAre("CCCC", "DDDD"));
}

// _____________________________________________________________________________
// A wait that fails with an error other than `EINTR` terminates the process:
// the kernel may still write into the buffers of the reads it holds.
TEST_F(IoUringPolicyFailureTest, brokenWaitTerminates) {
  // The reads come from a pipe that stays empty, so that `readBatch` finds no
  // completion ready and has to wait. The child of the death test sets up its
  // own policy and ring, so that this process's ring is not touched.
  EmptyPipe pipe;
  EXPECT_DEATH_IF_SUPPORTED(
      {
        RingScript script;
        script.brokenWaitErrno = EIO;
        auto policy = makePolicy(64, script);
        ReadBatchForTesting batch;
        batch.add({{0, 4}});
        batch.readWith(policy, pipe.readEnd());
      },
      "io_uring_wait_cqes failed");
}

// _____________________________________________________________________________
// A wait that a signal interrupts is retried.
TEST_F(IoUringPolicyFailureTest, interruptedWaitsAreRetried) {
  // The reads come from a pipe that is filled only right before a wait reaches
  // the real ring, so that no completion is ready before and the policy has to
  // wait. (A ready completion is reaped without a wait.)
  EmptyPipe pipe;
  RingScript script;
  ReadBatchForTesting batch;
  batch.add({{0, 4}, {0, 4}});
  auto policy = makePolicy(64, script);
  script.numberOfWaitsToInterrupt = 2;
  script.beforeRealWait = [&pipe] {
    ASSERT_EQ(write(pipe.writeEnd(), "AAAAAAAA", 8), 8);
  };
  batch.readWith(policy, pipe.readEnd());
  EXPECT_THAT(batch.result(), ElementsAre("AAAA", "AAAA"));
  EXPECT_EQ(script.numberOfWaitsToInterrupt, 0U);
  EXPECT_EQ(script.numberOfTakenRequests, 2U);
  EXPECT_EQ(script.numberOfReapedCompletions, 2U);
}

// _____________________________________________________________________________
// The first failed read of a batch is the one that `readBatch` throws.
TEST(IoUringPolicyOnFakeRing, firstFailureOfABatchIsThrown) {
  using ad_utility::testing::FakeIoUring;
  // A ring with a single SQE handles the reads one after the other.
  ad_utility::IoUringPolicy policy{
      std::make_unique<FakeIoUring>("AAAABBBB", 1, FakeIoUring::Faults{}, 0)};
  ReadBatchForTesting batch;
  batch.add({{0, 16}, {4, 16}, {0, 4}});
  AD_EXPECT_THROW_WITH_MESSAGE(batch.readWith(policy, -1),
                               HasSubstr("(8 of 16 bytes)"));
}

// _____________________________________________________________________________
// A read that the kernel fails is thrown with its `errno` by `readBatch`.
TEST(IoUringPolicyOnFakeRing, readErrorIsThrownWithItsErrno) {
  using ad_utility::testing::FakeIoUring;
  FakeIoUring::Faults faults;
  faults.readError = 1;
  ad_utility::IoUringPolicy policy{
      std::make_unique<FakeIoUring>("AAAABBBB", 4, faults, 0)};
  ReadBatchForTesting batch;
  batch.add({{0, 4}, {4, 4}});
  AD_EXPECT_THROW_WITH_MESSAGE(
      batch.readWith(policy, -1),
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
// batch, and a batch fails iff one of its reads failed. A batch whose reads
// were cancelled is read with `pread` from the real file, which has the same
// content as the fake's. Half of the seeds use a completion queue that is no
// larger than the submission queue.
TEST(IoUringPolicyOnFakeRing, randomFaults) {
  using ad_utility::testing::FakeIoUring;
  std::string file;
  for (size_t i : ad_utility::integerRange(size_t{1000})) {
    file.push_back(static_cast<char>('a' + (i * 7) % 26));
  }
  auto [tmp, fd] = makeTempFile(file);
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
    for ([[maybe_unused]] size_t round : ad_utility::integerRange(size_t{5})) {
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
      std::optional<std::string> error;
      try {
        batch.readWith(policy, fd);
      } catch (const std::exception& e) {
        error = e.what();
      }
      ASSERT_EQ(fake.numberOfReadsInRing(), 0U) << seed;
      EXPECT_EQ(error.has_value(), fake.injectedReadFault()) << seed;
      if (!error.has_value()) {
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

// `IoUringPolicy` needs a ring with at least one SQE. (`FakeIoUring` itself
// rejects a ring with fewer CQEs than SQEs, which the policy also requires, so
// that check of the policy cannot be reached with a fake.)
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
  EXPECT_NO_THROW(IoUringPolicy(makeFake(8, 8)));
}

// `LiburingRing::cancelQueued` rewrites the queued SQEs in the real ring: once
// submitted, they complete as no-ops and never write into their buffers. It
// finds the queued SQEs from the kernel's head of the submission queue, so the
// test also runs after `numberOfReadsBefore` reads have gone through the ring:
// with 6 of 8 slots used before, the 4 cancelled SQEs are in the slots 6, 7, 0
// and 1, so the positions wrap around the end of the queue.
TEST_F(IoUringPolicyFailureTest, cancelledReadsCompleteAsNoOps) {
  using Completion = ad_utility::IoUringInterface::Completion;
  constexpr uint64_t noOp = ad_utility::IoUringPolicy::NO_OP_REQUEST_ID;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  for (const size_t numberOfReadsBefore : {size_t{0}, size_t{6}}) {
    SCOPED_TRACE(absl::StrCat(numberOfReadsBefore, " reads before"));
    auto ring = ad_utility::makeLiburingRing(8);
    std::array<Completion, 8> completions{};
    // Move the kernel's head of the submission queue forward.
    std::vector<std::string> readBuffers(numberOfReadsBefore,
                                         std::string(4, '-'));
    for (size_t i = 0; i < numberOfReadsBefore; ++i) {
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

    constexpr size_t numberOfCancelledReads = 4;
    std::vector<std::string> buffers(numberOfCancelledReads,
                                     std::string(4, '-'));
    for (size_t i = 0; i < numberOfCancelledReads; ++i) {
      ASSERT_TRUE(ring->prepareRead(fd, buffers[i].data(), 4, 4 * i, i));
    }
    EXPECT_THAT(ring->cancelQueued(noOp), ElementsAre(0, 1, 2, 3));
    // No-ops that are cancelled again are not reported again.
    EXPECT_THAT(ring->cancelQueued(noOp), IsEmpty());
    ASSERT_EQ(ring->submit(), static_cast<int>(numberOfCancelledReads));
    ASSERT_EQ(ring->waitForCompletions(numberOfCancelledReads), 0);
    ASSERT_EQ(ring->popCompletions(completions), numberOfCancelledReads);
    // Every cancelled read completes as a no-op with result 0. A vector, not a
    // `take` view: gtest would print the view with range-v3's `operator<<`,
    // which GCC 11 fails to compile for this element type.
    const std::vector<Completion> reaped(
        completions.begin(), completions.begin() + numberOfCancelledReads);
    EXPECT_THAT(reaped, Each(AllOf(AD_FIELD(Completion, requestId, Eq(noOp)),
                                   AD_FIELD(Completion, result, Eq(0)))));
    EXPECT_THAT(buffers, Each(std::string(4, '-')));
  }
}

// A liburing ring of 8 SQEs has exactly 8 SQEs and twice as many CQEs, so it
// satisfies what `IoUringPolicy` checks.
TEST_F(IoUringPolicyFailureTest, ringCapacitiesMatchTheRingSize) {
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
ad_utility::detail::OpcodeSupport probeFails(io_uring*) noexcept {
  return ad_utility::detail::OpcodeSupport::ProbeFailed;
}
ad_utility::detail::OpcodeSupport opcodeIsMissing(io_uring*) noexcept {
  return ad_utility::detail::OpcodeSupport::OpcodeMissing;
}

// If the kernel does not confirm `IORING_OP_READ` and `IORING_OP_NOP` (before
// Linux 5.6 every read would fail with `EINVAL`), `makeLiburingRing` tears the
// ring down again (no file descriptor is left open) and throws with the cause,
// so that `makeBatchManager`, which catches it, falls back to `pread`.
TEST_F(IoUringPolicyFailureTest, aKernelWithoutTheNeededOperationsIsNotUsed) {
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

constexpr size_t REAP_WAVE = ad_utility::IoUringPolicy::REAP_WAVE;
constexpr size_t REAP_CHUNK =
    ad_utility::IoUringInterface::MAX_COMPLETIONS_PER_POP;

// _____________________________________________________________________________
// When the ring is full and no completion is ready, `readBatch` waits for a
// wave of `REAP_WAVE` completions, not for one, both to free slots and for the
// last reads: 64 reads through a ring of 16 take (64 - 16) / `REAP_WAVE` waits
// to free slots and 16 / `REAP_WAVE` for the last reads. Reaping one completion
// per wait would take 64.
TEST(IoUringPolicyWaves, fullRingIsRefilledInWaves) {
  using ad_utility::testing::FakeIoUring;
  static_assert(16 % REAP_WAVE == 0);
  auto [file, batch] = chunkedFileAndBatch(64);
  FakeIoUring::Faults faults;
  faults.completeOnSubmit = false;
  auto [fake, policy] =
      makePolicyOnFakeRing(std::make_unique<FakeIoUring>(file, 16, faults, 0));
  batch.readWith(policy, -1);
  EXPECT_EQ(fake.callCounts().waits, 64 / REAP_WAVE);
  EXPECT_EQ(fake.callCounts().chunks, 64 / REAP_WAVE);
  EXPECT_THAT(batch.result(), ElementsAreArray(chunksOf(file)));
}

// _____________________________________________________________________________
// Completions that are ready are reaped without waiting, all of them, in
// chunks of `REAP_CHUNK`: 200 ready completions take no wait and
// ceil(200 / `REAP_CHUNK`) chunks.
TEST(IoUringPolicyWaves, readyCompletionsAreReapedInChunksWithoutWaiting) {
  using ad_utility::testing::FakeIoUring;
  auto [file, batch] = chunkedFileAndBatch(200);
  FakeIoUring::Faults faults;
  faults.completeAllOnSubmit = true;
  auto [fake, policy] =
      makePolicyOnFakeRing(std::make_unique<FakeIoUring>(file, 256, faults, 0));
  batch.readWith(policy, -1);
  EXPECT_EQ(fake.callCounts().waits, 0U);
  EXPECT_EQ(fake.callCounts().chunks, (200 + REAP_CHUNK - 1) / REAP_CHUNK);
  EXPECT_THAT(batch.result(), ElementsAreArray(chunksOf(file)));
}

// _____________________________________________________________________________
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
  auto [fake, policy] =
      makePolicyOnFakeRing(std::make_unique<FakeIoUring>(file, 64, faults, 0));
  batch.readWith(policy, -1);
  EXPECT_EQ(fake.callCounts().waits, 16U);
  EXPECT_EQ(fake.faultCounts().earlyWaits, 16U);
  EXPECT_THAT(batch.result(), ElementsAreArray(chunksOf(file)));
}
#endif
}  // namespace
