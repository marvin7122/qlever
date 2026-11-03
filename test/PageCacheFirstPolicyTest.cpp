// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/IoUringManagerTestHelpers.h"
#include "util/PageCacheReadTestHelpers.h"
#include "util/Views.h"
#include "util/io_uring/IoUringManager.h"
#include "util/io_uring/PageCacheFirstPolicy.h"

namespace {

using namespace ::testing;
using namespace ioUringManagerTestHelpers;

using ad_utility::PageCacheReader;
using ad_utility::UnservedReads;
using pageCacheReadTestHelpers::counted;
using pageCacheReadTestHelpers::fullyCached;
using pageCacheReadTestHelpers::nothingCached;
using pageCacheReadTestHelpers::notSupported;

// Run `reader.readPageCacheHits` on `reads` (pairs of file offset and size) of
// `fd` and return, in this order, the reads that were not served and the
// buffers (filled with '-' before the call).
std::pair<UnservedReads, std::vector<std::string>> notServedAndBuffers(
    PageCacheReader& reader, int fd,
    const std::vector<std::pair<uint64_t, size_t>>& reads) {
  std::vector<size_t> numBytes;
  numBytes.reserve(reads.size());
  std::vector<uint64_t> offsets;
  offsets.reserve(reads.size());
  std::vector<std::string> buffers;
  buffers.reserve(reads.size());
  for (const auto& [offset, size] : reads) {
    offsets.push_back(offset);
    numBytes.push_back(size);
    buffers.emplace_back(size, '-');
  }
  std::vector<char*> targets;
  targets.reserve(buffers.size());
  for (auto& buffer : buffers) {
    targets.push_back(buffer.data());
  }
  auto notServed = reader.readPageCacheHits(fd, numBytes, offsets, targets);
  return {std::move(notServed), std::move(buffers)};
}

// True iff no read of the batch was served.
bool nothingRead(const UnservedReads& unserved) {
  return std::holds_alternative<UnservedReads::NothingRead>(unserved.reads_);
}

// True iff every read of the batch was served.
bool allRead(const UnservedReads& unserved) {
  return std::holds_alternative<UnservedReads::AllRead>(unserved.reads_);
}

// Matches an `UnservedReads` for which some reads were served and the reads at
// `positions` were not.
auto remainingPositions(std::vector<size_t> positions) {
  return Field(&UnservedReads::reads_,
               VariantWith<UnservedReads::RemainingPositions>(
                   Property(&UnservedReads::RemainingPositions::positions,
                            ElementsAreArray(std::move(positions)))));
}

// Only the first 6 bytes of every run are "cached": the whole run is read, but
// only 6 bytes are reported as read.
int64_t sixBytesCached(int fd, const ::iovec* iov, int iovcnt,
                       int64_t offset) noexcept {
  const int64_t numBytesRead = fullyCached(fd, iov, iovcnt, offset);
  return numBytesRead < 0 ? numBytesRead : std::min<int64_t>(numBytesRead, 6);
}

// `RWF_NOWAIT` may return 0 before the end of the file (readv(2)).
int64_t zeroBytes(int, const ::iovec*, int, int64_t) noexcept { return 0; }

// _____________________________________________________________________________
// Reads that cannot be served completely (here: beyond the end of the file)
// are all returned, whether or not `RWF_NOWAIT` is supported. This test uses
// the real `preadv2(RWF_NOWAIT)`.
TEST(ReadPageCacheHits, missOnlyBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  PageCacheReader reader;
  auto [notServed, buffers] =
      notServedAndBuffers(reader, fd, {{16, 4}, {40, 4}, {20, 4}});
  EXPECT_TRUE(nothingRead(notServed));
}

// The tests below inject the page-cache read, so they do not depend on the
// real `preadv2(RWF_NOWAIT)` being supported for the test file or on the file
// still being cached. They need the fast path to be compiled in.

// _____________________________________________________________________________
// A batch whose reads are all in the page cache is served completely;
// adjacent ranges (the first two reads) are read in one call.
TEST(ReadPageCacheHits, hitOnlyBatch) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  size_t numberOfReads = 0;
  PageCacheReader reader{counted(numberOfReads, &fullyCached)};
  auto [notServed, buffers] =
      notServedAndBuffers(reader, fd, {{0, 4}, {4, 4}, {12, 4}, {4, 0}});
  EXPECT_TRUE(allRead(notServed));
  EXPECT_EQ(numberOfReads, 3u);
  EXPECT_EQ(buffers[0], "AAAA");
  EXPECT_EQ(buffers[1], "BBBB");
  EXPECT_EQ(buffers[2], "DDDD");
  EXPECT_EQ(buffers[3], "");
}

// _____________________________________________________________________________
// A mixed batch: a short read in the middle of a run of adjacent ranges
// returns the incomplete read and the rest of its run, the complete reads
// of the run and of other runs are served.
TEST(ReadPageCacheHits, mixedBatchWithShortRead) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  PageCacheReader reader{&fullyCached};
  auto [notServed, buffers] = notServedAndBuffers(
      reader, fd, {{0, 4}, {8, 4}, {12, 2}, {14, 4}, {18, 4}, {4, 4}});
  EXPECT_THAT(notServed, remainingPositions({3, 4}));
  EXPECT_EQ(buffers[0], "AAAA");
  EXPECT_EQ(buffers[1], "CCCC");
  EXPECT_EQ(buffers[2], "DD");
  EXPECT_EQ(buffers[5], "BBBB");
}

// _____________________________________________________________________________
// Reads that are not cached (`EAGAIN`) are all returned, their buffers stay
// untouched, and the reader stays switched on.
TEST(ReadPageCacheHits, notCachedReadsAreReturned) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  size_t numberOfReads = 0;
  PageCacheReader reader{counted(numberOfReads, &nothingCached)};
  auto [notServed, buffers] =
      notServedAndBuffers(reader, fd, {{0, 4}, {4, 4}, {12, 4}});
  EXPECT_TRUE(nothingRead(notServed));
  EXPECT_EQ(buffers[0], "----");
  EXPECT_EQ(numberOfReads, 2u);
  // The next batch tries again.
  std::ignore = notServedAndBuffers(reader, fd, {{0, 4}});
  EXPECT_EQ(numberOfReads, 3u);
}

// _____________________________________________________________________________
// A partial read serves the reads it covers completely; the incomplete read
// and the rest of its run are returned. A read of 0 bytes serves nothing.
TEST(ReadPageCacheHits, partialAndZeroByteReads) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  {
    PageCacheReader reader{&sixBytesCached};
    auto [notServed, buffers] =
        notServedAndBuffers(reader, fd, {{0, 4}, {4, 4}, {12, 4}});
    EXPECT_THAT(notServed, remainingPositions({1}));
    EXPECT_EQ(buffers[0], "AAAA");
    EXPECT_EQ(buffers[2], "DDDD");
  }
  {
    PageCacheReader reader{&zeroBytes};
    auto [notServed, buffers] =
        notServedAndBuffers(reader, fd, {{0, 4}, {4, 4}, {12, 4}});
    EXPECT_TRUE(nothingRead(notServed));
  }
}

// _____________________________________________________________________________
// `EOPNOTSUPP`, `ENOSYS` and `EPERM` switch the reader off: the failed run and
// all later runs of the batch are returned without further calls, and so are
// all reads of later batches. Other readers are not affected.
TEST(ReadPageCacheHits, permanentErrorsSwitchTheReaderOff) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  for (int errorCode : {EOPNOTSUPP, ENOSYS, EPERM}) {
    size_t numberOfReads = 0;
    PageCacheReader reader{
        counted(numberOfReads, pageCacheReadTestHelpers::failsWith(errorCode))};
    auto [notServed, buffers] =
        notServedAndBuffers(reader, fd, {{0, 4}, {8, 4}, {12, 2}});
    EXPECT_TRUE(nothingRead(notServed));
    EXPECT_EQ(numberOfReads, 1u);
    auto [notServed2, unusedBuffers] =
        notServedAndBuffers(reader, fd, {{0, 4}});
    EXPECT_TRUE(nothingRead(notServed2));
    EXPECT_EQ(numberOfReads, 1u);
  }
  PageCacheReader otherReader{&fullyCached};
  EXPECT_TRUE(allRead(notServedAndBuffers(otherReader, fd, {{0, 4}}).first));
}

// _____________________________________________________________________________
// Any other error (here `EIO`) leaves the reads unserved, but does not switch
// the reader off: the next run and the next batch try again.
TEST(ReadPageCacheHits, otherErrorsAreRetried) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  size_t numberOfReads = 0;
  PageCacheReader reader{
      counted(numberOfReads, pageCacheReadTestHelpers::failsWith(EIO))};
  auto [notServed, buffers] =
      notServedAndBuffers(reader, fd, {{0, 4}, {8, 4}, {12, 2}});
  EXPECT_TRUE(nothingRead(notServed));
  EXPECT_EQ(numberOfReads, 2u);
  auto [notServed2, unusedBuffers] = notServedAndBuffers(reader, fd, {{0, 4}});
  EXPECT_TRUE(nothingRead(notServed2));
  EXPECT_EQ(numberOfReads, 3u);
}

// _____________________________________________________________________________
// A permanent error after some runs were served: the served runs stay served,
// the failed run and the runs after it are returned without further calls, and
// so are all reads of later batches.
TEST(ReadPageCacheHits, switchingOffInTheMiddleOfABatch) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  size_t numberOfReads = 0;
  PageCacheReader reader{
      [&numberOfReads](int fd, const ::iovec* iov, int iovcnt, int64_t offset) {
        if (numberOfReads++ == 0) {
          return fullyCached(fd, iov, iovcnt, offset);
        }
        return notSupported(fd, iov, iovcnt, offset);
      }};
  auto [notServed, buffers] =
      notServedAndBuffers(reader, fd, {{0, 4}, {8, 4}, {4, 4}});
  EXPECT_THAT(notServed, remainingPositions({1, 2}));
  EXPECT_EQ(buffers[0], "AAAA");
  EXPECT_EQ(numberOfReads, 2u);
  auto [notServed2, unusedBuffers] = notServedAndBuffers(reader, fd, {{0, 4}});
  EXPECT_TRUE(nothingRead(notServed2));
  EXPECT_EQ(numberOfReads, 2u);
}

// _____________________________________________________________________________
// A run of more than `IOV_MAX` adjacent reads is split into chunks. A short
// first chunk leaves the rest of that chunk unserved, but the next chunk is
// still read and served.
TEST(ReadPageCacheHits, aShortChunkDoesNotStopTheNextChunk) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  const auto iovMax = static_cast<size_t>(IOV_MAX);
  const size_t numReads = iovMax + 1;
  auto [tmp, fd] = makeTempFile(std::string(numReads, 'x'));
  std::vector<std::pair<uint64_t, size_t>> reads;
  reads.reserve(numReads);
  for (size_t i : ad_utility::integerRange(numReads)) {
    reads.emplace_back(i, 1);
  }
  size_t numberOfReads = 0;
  // The first chunk only gets 1 byte (its first read), the second one is
  // served completely.
  PageCacheReader reader{
      [&numberOfReads](int fd, const ::iovec* iov, int iovcnt, int64_t offset) {
        const int64_t numBytesRead = fullyCached(fd, iov, iovcnt, offset);
        return numberOfReads++ == 0 ? std::min<int64_t>(numBytesRead, 1)
                                    : numBytesRead;
      }};
  auto [notServed, buffers] = notServedAndBuffers(reader, fd, reads);
  EXPECT_THAT(notServed, remainingPositions(::ranges::to<std::vector<size_t>>(
                             ::ranges::views::iota(size_t{1}, iovMax))));
  EXPECT_EQ(numberOfReads, 2u);
  EXPECT_EQ(buffers.front(), "x");
  EXPECT_EQ(buffers.back(), "x");
}

// _____________________________________________________________________________
// A run of more than `IOV_MAX` adjacent reads is split into several calls.
TEST(ReadPageCacheHits, runsAreSplitAtIovMax) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  const size_t numReads = static_cast<size_t>(IOV_MAX) + 1;
  std::string content(numReads, 'x');
  content.back() = 'y';
  auto [tmp, fd] = makeTempFile(content);
  std::vector<std::pair<uint64_t, size_t>> reads;
  reads.reserve(numReads);
  for (size_t i : ad_utility::integerRange(numReads)) {
    reads.emplace_back(i, 1);
  }
  size_t numberOfReads = 0;
  PageCacheReader reader{counted(numberOfReads, &fullyCached)};
  auto [notServed, buffers] = notServedAndBuffers(reader, fd, reads);
  EXPECT_TRUE(allRead(notServed));
  EXPECT_EQ(numberOfReads, 2u);
  EXPECT_EQ(buffers.front(), "x");
  EXPECT_EQ(buffers.back(), "y");
}

// _____________________________________________________________________________
// An empty batch serves nothing, and spans of different lengths are
// rejected.
TEST(ReadPageCacheHits, emptyBatchAndContract) {
  auto [tmp, fd] = makeTempFile("AAAA");
  PageCacheReader reader{&fullyCached};
  EXPECT_TRUE(nothingRead(reader.readPageCacheHits(fd, {}, {}, {})));
  std::vector<size_t> numBytes{4, 4};
  std::vector<uint64_t> offsets{0};
  std::string buffer(8, '-');
  std::vector<char*> targets{buffer.data(), buffer.data() + 4};
  AD_EXPECT_THROW_WITH_MESSAGE(
      reader.readPageCacheHits(fd, numBytes, offsets, targets),
      HasSubstr("offsets.size() == numBytes.size()"));
}

// _____________________________________________________________________________
// `fromNotServed` picks the alternative from the number of unserved reads, so
// a `RemainingPositions` is never empty and never holds the whole batch, and
// it rejects positions that are not strictly ascending or not in the batch.
TEST(UnservedReads, fromNotServed) {
  using U = UnservedReads;
  EXPECT_TRUE(nothingRead(U::fromNotServed({}, 0)));
  EXPECT_TRUE(nothingRead(U::fromNotServed({0, 1, 2}, 3)));
  EXPECT_TRUE(allRead(U::fromNotServed({}, 3)));
  EXPECT_THAT(U::fromNotServed({0, 2}, 3), remainingPositions({0, 2}));
  AD_EXPECT_THROW_WITH_MESSAGE(U::fromNotServed({2, 0}, 3),
                               HasSubstr("strictly ascending"));
  AD_EXPECT_THROW_WITH_MESSAGE(U::fromNotServed({1, 1}, 3),
                               HasSubstr("strictly ascending"));
  AD_EXPECT_THROW_WITH_MESSAGE(U::fromNotServed({3}, 3),
                               HasSubstr("not in the batch"));
}

// A `ReadPolicy` that reads synchronously and records the reads it receives.
// The records are static because a `BatchManager` owns its policy.
struct RecordingPolicy {
  using BatchId = uint64_t;
  // The number of reads of every `addBatch` call.
  static inline std::vector<size_t> batchSizes_;
  // The file offsets of all reads, in the order of arrival.
  static inline std::vector<uint64_t> receivedOffsets_;
  // The handles of all `wait` calls, in the order of arrival.
  static inline std::vector<BatchId> waitedFor_;
  static void reset() {
    batchSizes_.clear();
    receivedOffsets_.clear();
    waitedFor_.clear();
  }

  explicit RecordingPolicy([[maybe_unused]] unsigned ringSize) {}
  // Reads synchronously, but reports the reads as in flight, so that the
  // tests can see whether `wait` reaches the policy.
  bool addBatch(int fd, ql::span<const size_t> numBytes,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                [[maybe_unused]] BatchId handle) {
    batchSizes_.push_back(numBytes.size());
    for (auto&& [numberOfBytesToRead, fileOffset, targetBuffer] :
         ::ranges::views::zip(numBytes, offsets, buffers)) {
      receivedOffsets_.push_back(fileOffset);
      ad_utility::SyncIoPolicy::readFullyOrThrow(
          fd, targetBuffer, numberOfBytesToRead, fileOffset);
    }
    return true;
  }
  void wait(BatchId handle) const { waitedFor_.push_back(handle); }
};

// A manager whose (page-cache-first) policy is the `RecordingPolicy`
// above, so tests can observe which reads reached the policy.
using RecordedManager =
    ad_utility::BatchManager<ad_utility::PageCacheFirstPolicy<RecordingPolicy>>;

// _____________________________________________________________________________
// The reads that the page-cache read serves are not passed on to the decorated
// policy; the others are, in their original order. The offsets `0, 4` are
// adjacent and form one run, `12` and `8` are runs of their own.
TEST(PageCacheFirstPolicy, onlyTheMissesAreForwarded) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  RecordingPolicy::reset();
  size_t numberOfReads = 0;
  RecordedManager manager(
      64, counted(numberOfReads, pageCacheReadTestHelpers::cachedAt({0})));
  ReadBatchForTesting batch;
  batch.add({{0, 4}, {4, 4}, {12, 4}, {8, 4}});
  const auto handle = batch.submitTo(manager, fd);
  manager.wait(handle);
  EXPECT_THAT(batch.result(), ElementsAre("AAAA", "BBBB", "DDDD", "CCCC"));
  EXPECT_THAT(RecordingPolicy::batchSizes_, ElementsAre(size_t{2}));
  EXPECT_THAT(RecordingPolicy::receivedOffsets_,
              ElementsAre(uint64_t{12}, uint64_t{8}));
  EXPECT_THAT(RecordingPolicy::waitedFor_, ElementsAre(handle.id_));
  EXPECT_EQ(numberOfReads, size_t{3});
}

// _____________________________________________________________________________
// If the page-cache read serves every read, the decorated policy is not
// involved at all: it receives no batch, the handle says that no read is in
// flight, and waiting for the batch (also twice) returns without asking it.
TEST(PageCacheFirstPolicy, aBatchThatIsServedCompletelyHasNothingToWaitFor) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  RecordingPolicy::reset();
  size_t numberOfReads = 0;
  RecordedManager manager(64, counted(numberOfReads, &fullyCached));
  ReadBatchForTesting batch;
  batch.add({{0, 4}, {8, 4}});
  const auto handle = batch.submitTo(manager, fd);
  EXPECT_FALSE(handle.hasReadsInFlight_);
  manager.wait(handle);
  manager.wait(handle);
  EXPECT_THAT(batch.result(), ElementsAre("AAAA", "CCCC"));
  EXPECT_THAT(RecordingPolicy::batchSizes_, IsEmpty());
  EXPECT_THAT(RecordingPolicy::waitedFor_, IsEmpty());
  EXPECT_EQ(numberOfReads, size_t{2});
}

// _____________________________________________________________________________
// If the page-cache read serves nothing, the decorated policy receives the
// batch unchanged.
TEST(PageCacheFirstPolicy, aBatchThatIsNotServedIsPassedOnUnchanged) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  RecordingPolicy::reset();
  size_t numberOfReads = 0;
  RecordedManager manager(64, counted(numberOfReads, &nothingCached));
  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});
  manager.wait(batch.submitTo(manager, fd));
  EXPECT_THAT(batch.result(), ElementsAre("CCCC", "AAAA", "DDDD"));
  EXPECT_THAT(RecordingPolicy::batchSizes_, ElementsAre(size_t{3}));
  EXPECT_THAT(RecordingPolicy::receivedOffsets_,
              ElementsAre(uint64_t{8}, uint64_t{0}, uint64_t{12}));
  // One page-cache read per run; the three reads are not adjacent.
  EXPECT_EQ(numberOfReads, size_t{3});
}

// _____________________________________________________________________________
// The fast path is asked for every batch: while it is switched off, the
// page-cache read is not called and all reads are passed on; switching it on
// again restores it, on the same manager.
TEST(PageCacheFirstPolicy, theFastPathCanBeSwitchedOffAndOn) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  RecordingPolicy::reset();
  size_t numberOfReads = 0;
  bool isEnabled = false;
  RecordedManager manager(64, counted(numberOfReads, &fullyCached),
                          [&isEnabled] { return isEnabled; });
  {
    ReadBatchForTesting batch;
    batch.add({{0, 4}, {8, 4}});
    manager.wait(batch.submitTo(manager, fd));
    EXPECT_THAT(batch.result(), ElementsAre("AAAA", "CCCC"));
    EXPECT_EQ(numberOfReads, size_t{0});
    EXPECT_THAT(RecordingPolicy::batchSizes_, ElementsAre(size_t{2}));
  }
  isEnabled = true;
  {
    ReadBatchForTesting batch;
    batch.add({{0, 4}, {8, 4}});
    manager.wait(batch.submitTo(manager, fd));
    EXPECT_THAT(batch.result(), ElementsAre("AAAA", "CCCC"));
    EXPECT_EQ(numberOfReads, size_t{2});
    EXPECT_THAT(RecordingPolicy::batchSizes_, ElementsAre(size_t{2}));
  }
}

// _____________________________________________________________________________
// If the kernel or file system rejects `RWF_NOWAIT`, the first batch is passed
// on completely, and later batches do not even try the page-cache read.
TEST(PageCacheFirstPolicy, anUnsupportedFastPathIsLeftAlone) {
  SKIP_WITHOUT_PAGE_CACHE_FAST_PATH();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  RecordingPolicy::reset();
  size_t numberOfReads = 0;
  RecordedManager manager(64, counted(numberOfReads, &notSupported));
  for ([[maybe_unused]] size_t round : ad_utility::integerRange(size_t{2})) {
    ReadBatchForTesting batch;
    batch.add({{0, 4}, {8, 4}});
    manager.wait(batch.submitTo(manager, fd));
    EXPECT_THAT(batch.result(), ElementsAre("AAAA", "CCCC"));
  }
  EXPECT_EQ(numberOfReads, size_t{1});
  EXPECT_THAT(RecordingPolicy::batchSizes_, ElementsAre(size_t{2}, size_t{2}));
}

}  // namespace
