// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 Johannes Kalmbach <johannes.kalmbach@gmail.com>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/VocabularyOnDisk.h"

#include <absl/cleanup/cleanup.h>
#include <absl/functional/bind_front.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <numeric>

#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "util/ExceptionHandling.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/MmapVector.h"
#include "util/StringUtils.h"
#include "util/Views.h"

using OffsetAndSize = VocabularyOnDisk::OffsetAndSize;

// ____________________________________________________________________________
OffsetAndSize VocabularyOnDisk::getOffsetAndSize(uint64_t i) const {
  AD_CORRECTNESS_CHECK(i < size());
  // Read the offset of the word at index `i` and the offset of the next word
  // (which marks the end of the word at index `i`) in a single `pread`.
  std::array<Offset, 2> offsets{};
  // Assert no unexpected padding.
  static_assert(sizeof(offsets) == sizeof(Offset) * 2);
  offsetsFile_.read(offsets.data(), sizeof(offsets),
                    static_cast<off_t>(i * sizeof(Offset)));
  return {offsets[0], offsets[1] - offsets[0]};
}

// _____________________________________________________________________________
std::string VocabularyOnDisk::operator[](uint64_t idx) const {
  AD_CONTRACT_CHECK(idx < size());
  auto offsetAndSize = getOffsetAndSize(idx);
  std::string result(offsetAndSize.size_, '\0');
  file_.read(result.data(), offsetAndSize.size_,
             static_cast<off_t>(offsetAndSize.offset_));
  return result;
}

namespace {
// Given the `offsets` of a chunk of words and a starting position `first`
// within that chunk, return how many words starting at `first` can be read into
// a single data buffer without their combined size exceeding
// `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH`, but always at least one word, even
// if that single word is larger than the limit (a word must not be split).
size_t numWordsWithinLimit(ql::span<const uint64_t> offsets, size_t first) {
  // Common case: all remaining words of the chunk fit. Checking this first
  // (i.e. looking at the end right away) avoids a search in the expected case
  // where a whole batch of words comfortably fits within the limit.
  if (offsets.back() - offsets[first] <=
      VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH.getBytes()) {
    return offsets.size() - 1 - first;
  }
  // Otherwise binary-search for the largest prefix that fits. `offsets` is
  // ascending, so the number of words that fit is the number of offsets in
  // `[first, total]` that are `<= offsets[first] + limit`, minus one.
  uint64_t threshold =
      offsets[first] + VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH.getBytes();
  auto begin = offsets.begin() + first;
  auto it = std::upper_bound(begin, offsets.end(), threshold);
  size_t numFit = static_cast<size_t>(it - begin) - 1;
  return std::max<size_t>(numFit, 1);
}

// Turn the offsets of a chunk of words into an input range of sub-chunks, where
// each sub-chunk contains the offsets of a maximal number of words that can be
// read into a single data buffer without exceeding
// `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH`, but always at least one word, even
// if that single word is larger than the limit (a word must not be split).
auto chunkOffsets(ql::span<const uint64_t> offsets) {
  AD_CORRECTNESS_CHECK(!offsets.empty());
  return ad_utility::InputRangeFromGetCallable{
      [offsets,
       start = size_t{0}]() mutable -> std::optional<ql::span<const uint64_t>> {
        if (start >= offsets.size() - 1) {
          return std::nullopt;
        }
        size_t numWords = numWordsWithinLimit(offsets, start);
        size_t oldBegin = std::exchange(start, start + numWords);
        return offsets.subspan(oldBegin, numWords + 1);
      }};
}

// Map a chunk of offsets to an input range of string views, where each string
// view corresponds to a word in the chunk. The string views are backed by the
// given `data` buffer, which must contain the concatenated string data of all
// words in the chunk, starting at the offset of the first word in the chunk.
auto mapOffsetsToStringViews(ql::span<const uint64_t> offsets,
                             std::string_view data) {
  AD_CORRECTNESS_CHECK(!offsets.empty());
  auto initialStart = offsets.front();
  // `std::views::sliding` is not available in C++20, and also not in range-v3,
  // so we use `zip` with a dropped view instead, which is equivalent to a
  // sliding view of size 2.
  return ::ranges::views::zip(offsets, offsets | ::ranges::views::drop(1)) |
         ql::views::transform([data, initialStart](const auto& pair) {
           auto [begin, end] = pair;
           return std::string_view{data.data() + begin - initialStart,
                                   end - begin};
         });
}
}  // namespace

// _____________________________________________________________________________
auto VocabularyOnDisk::chunkToWords(ql::span<const uint64_t> offsets) const {
  return ad_utility::allView(ad_utility::CachingTransformInputRange{
             chunkOffsets(offsets),
             [this, data = std::string{}](
                 ql::span<const uint64_t> subOffsets) mutable {
               data.resize(subOffsets.back() - subOffsets.front());
               file_.read(data.data(), data.size(),
                          static_cast<off_t>(subOffsets.front()));
               return mapOffsetsToStringViews(subOffsets, data);
             }}) |
         ql::views::join;
}

// _____________________________________________________________________________
auto VocabularyOnDisk::readOffsetsInBatches() const {
  // For each batch, read the offsets of its words from disk to memory. We read
  // one extra offset that marks the end of the last word in the batch.
  return ad_utility::CachingTransformInputRange{
      ::ranges::views::stride(::ranges::views::iota(size_t{0}, size()),
                              VOCABULARY_SCAN_MAX_WORDS_PER_BATCH),
      [this,
       buffer =
           std::array<uint64_t, VOCABULARY_SCAN_MAX_WORDS_PER_BATCH + 1>{}](
          size_t chunkStart) mutable {
        size_t numWords = std::min<size_t>(VOCABULARY_SCAN_MAX_WORDS_PER_BATCH,
                                           size() - chunkStart);
        size_t numWordsPlusOne = numWords + 1;
        offsetsFile_.read(buffer.data(), numWordsPlusOne * sizeof(uint64_t),
                          static_cast<off_t>(chunkStart * sizeof(uint64_t)));
        return ql::span<const uint64_t>{buffer.data(), numWordsPlusOne};
      }};
}

// _____________________________________________________________________________
VocabularyScanRange VocabularyOnDisk::scanAll() const {
  // Range of all words in the vocabulary.
  auto words = ad_utility::OwningView{readOffsetsInBatches()} |
               ql::views::transform(
                   absl::bind_front(&VocabularyOnDisk::chunkToWords, this)) |
               ql::views::join;
  // Pair each word with its index in the vocabulary.
  return VocabularyScanRange{ad_utility::CachingTransformInputRange{
      std::move(words), [index = uint64_t{0}](std::string_view word) mutable {
        return IndexAndWord{index++, word};
      }}};
}

// _____________________________________________________________________________
void VocabularyOnDisk::readThroughManager(ad_utility::BatchManagerBase& manager,
                                          int fd,
                                          ql::span<const size_t> numBytes,
                                          ql::span<const uint64_t> offsets,
                                          ql::span<char*> buffers,
                                          ql::span<const size_t> positions) {
  if (positions.empty()) {
    return;
  }
  auto select = [&positions](auto values) {
    return ::ranges::to_vector(
        positions |
        ql::views::transform([&values](size_t i) { return values[i]; }));
  };
  auto selectedNumBytes = select(numBytes);
  auto selectedOffsets = select(offsets);
  auto selectedBuffers = select(buffers);
  manager.wait(
      manager.addBatch(fd, selectedNumBytes, selectedOffsets, selectedBuffers));
}

// The page this thread keeps for one vocabulary file. A thread that calls
// `lookupBatch` on two vocabularies has one entry per vocabulary.
VocabularyOnDisk::OwnedPage& VocabularyOnDisk::threadOwnedPage(
    bool words) const {
  struct Entry {
    const VocabularyOnDisk* vocab = nullptr;
    uint64_t epoch = 0;
    OwnedPage words_;
    OwnedPage offsets_;
  };
  thread_local std::vector<Entry> entries;
  for (Entry& entry : entries) {
    if (entry.vocab != this) {
      continue;
    }
    if (entry.epoch != slotEpoch_) {
      entry.words_ = OwnedPage{};
      entry.offsets_ = OwnedPage{};
      entry.epoch = slotEpoch_;
    }
    return words ? entry.words_ : entry.offsets_;
  }
  entries.emplace_back();
  Entry& created = entries.back();
  created.vocab = this;
  created.epoch = slotEpoch_;
  return words ? created.words_ : created.offsets_;
}

// True when `[offset, offset + length)` lies inside one 4096-byte page.
// `pageStart` receives that page's file offset.
bool VocabularyOnDisk::containedInOneOwnedPage(uint64_t offset, size_t length,
                                               uint64_t* pageStart) {
  using Page = VocabularyOnDisk::OwnedPage;
  if (length > Page::kBytes) {
    return false;
  }
  if (offset > std::numeric_limits<uint64_t>::max() - length) {
    return false;
  }
  const uint64_t start = offset & ~(Page::kBytes - 1);
  if (offset + length > start + Page::kBytes) {
    return false;
  }
  *pageStart = start;
  return true;
}

// Split the ranges into those inside one page (returned, sorted by page)
// and the rest (appended to `notServed`: ranges that cross a page).
std::vector<VocabularyOnDisk::SinglePageRange>
VocabularyOnDisk::partitionSinglePageRanges(ql::span<const uint64_t> offsets,
                                            ql::span<const size_t> lengths,
                                            std::vector<size_t>& notServed) {
  const size_t numRanges = offsets.size();
  std::vector<SinglePageRange> inPage;
  inPage.reserve(numRanges);
  for (size_t i = 0; i < numRanges; ++i) {
    uint64_t pageStart = 0;
    if (!containedInOneOwnedPage(offsets[i], lengths[i], &pageStart)) {
      notServed.push_back(i);
    } else {
      inPage.push_back(SinglePageRange{i, pageStart});
    }
  }
  ql::ranges::sort(inPage,
                   [](const SinglePageRange& a, const SinglePageRange& b) {
                     return a.pageStart < b.pageStart ||
                            (a.pageStart == b.pageStart && a.index < b.index);
                   });
  return inPage;
}

// Read the 4096-byte page at `pageStart` with one non-blocking `preadv2`
// and store it in `slot` when every grouped range ends inside the bytes
// read. True when the slot now serves the group. An `EOPNOTSUPP` disables
// the page-cache fast path for the rest of the process, so later groups
// and lookups stop probing.
bool VocabularyOnDisk::fetchPageIntoSlot(OwnedPage& slot, int fd,
                                         uint64_t pageStart,
                                         ql::span<const SinglePageRange> group,
                                         ql::span<const uint64_t> offsets,
                                         ql::span<const size_t> lengths) {
  std::array<char, VocabularyOnDisk::OwnedPage::kBytes> buffer{};
  ::iovec iovec{buffer.data(), buffer.size()};
  const int64_t numRead = ad_utility::detail::pageCacheRead()(
      fd, &iovec, 1, static_cast<int64_t>(pageStart));
  if (numRead < 0 && errno == EOPNOTSUPP) {
    ad_utility::detail::disablePageCacheFastPathSupport();
    return false;
  }
  if (numRead <= 0) {
    return false;
  }
  const uint64_t available = pageStart + static_cast<uint64_t>(numRead);
  for (const SinglePageRange& range : group) {
    const uint64_t end = offsets[range.index] + lengths[range.index];
    if (end < offsets[range.index] || end > available) {
      return false;
    }
  }
  slot.fd_ = fd;
  slot.pageStart_ = pageStart;
  slot.validBytes_ = static_cast<size_t>(numRead);
  std::memcpy(slot.bytes_.data(), buffer.data(), slot.validBytes_);
  return true;
}

// Copy each range that lies in a page `slot` already holds. When two or more
// ranges share a page the slot does not hold, `fetchPageIntoSlot` reads that
// page with one `preadv2(RWF_NOWAIT)` and those ranges are copied from it. A
// range that crosses a page, and a page with a single range the slot does
// not hold, stay unserved. The returned indices are ascending.
std::vector<size_t> VocabularyOnDisk::copyRangesFromOwnedPage(
    OwnedPage& slot, int fd, ql::span<const uint64_t> offsets,
    ql::span<const size_t> lengths, ql::span<char*> destinations) {
  const size_t numRanges = offsets.size();
  std::vector<size_t> notServed;
  notServed.reserve(numRanges);
  std::vector<SinglePageRange> inPage =
      partitionSinglePageRanges(offsets, lengths, notServed);
  size_t group = 0;
  while (group < inPage.size()) {
    size_t groupEnd = group + 1;
    while (groupEnd < inPage.size() &&
           inPage[groupEnd].pageStart == inPage[group].pageStart) {
      ++groupEnd;
    }
    const ql::span<const SinglePageRange> grouped{inPage.data() + group,
                                                  groupEnd - group};
    const uint64_t pageStart = inPage[group].pageStart;
    // When the fast path was found unsupported (at entry, or by an earlier
    // group in this loop), the rest stays unserved without further probes.
    bool served = ad_utility::pageCacheFastPathIsSupported();
    if (served) {
      for (const SinglePageRange& range : grouped) {
        if (!slot.covers(fd, pageStart, offsets[range.index],
                         lengths[range.index])) {
          served = false;
          break;
        }
      }
      if (!served && grouped.size() >= 2) {
        served =
            fetchPageIntoSlot(slot, fd, pageStart, grouped, offsets, lengths);
      }
    }
    for (const SinglePageRange& range : grouped) {
      if (served) {
        std::memcpy(destinations[range.index],
                    slot.bytes_.data() +
                        static_cast<size_t>(offsets[range.index] - pageStart),
                    lengths[range.index]);
      } else {
        notServed.push_back(range.index);
      }
    }
    group = groupEnd;
  }
  std::sort(notServed.begin(), notServed.end());
  return notServed;
}

// _____________________________________________________________________________
std::vector<VocabularyOnDisk::OffsetPair> VocabularyOnDisk::readOffsetPairs(
    ad_utility::BatchManagerBase& manager, ql::span<const size_t> indices,
    bool pageCacheFastPath) const {
  // For each requested index `i`, read its offset together with the next offset
  // (which bounds the string) as one 16-byte pair from `.offsets`.
  const size_t numIndices = indices.size();
  std::vector<OffsetPair> offsetPairs(numIndices);
  std::vector<size_t> sizes(numIndices, sizeof(OffsetPair));
  std::vector<uint64_t> fileOffsets(numIndices);
  std::vector<char*> targets(numIndices);
  for (auto&& [fileOffset, index, target, offsetPair] :
       ::ranges::views::zip(fileOffsets, indices, targets, offsetPairs)) {
    AD_CONTRACT_CHECK(index < size());
    fileOffset = index * sizeof(uint64_t);
    target = reinterpret_cast<char*>(&offsetPair);
  }
  if (!pageCacheFastPath) {
    manager.wait(
        manager.addBatch(offsetsFile_.fd(), sizes, fileOffsets, targets));
    return offsetPairs;
  }

  // `positions` are the batch indices still to read. A page the export already
  // copied, or a page that holds two or more of these pairs, is copied here.
  std::vector<size_t> positions = copyRangesFromOwnedPage(
      threadOwnedPage(false), offsetsFile_.fd(), fileOffsets, sizes, targets);
  if (positions.empty()) {
    return offsetPairs;
  }

  // The pairs of consecutive indices overlap in the file, so read each run of
  // consecutive indices as one range of `runLength + 1` offsets.
  std::vector<size_t> runBegins{0};
  for (size_t p = 1; p < positions.size(); ++p) {
    const size_t previous = positions[p - 1];
    const size_t current = positions[p];
    if (indices[current] != indices[previous] + 1) {
      runBegins.push_back(p);
    }
  }
  runBegins.push_back(positions.size());
  const size_t numRuns = runBegins.size() - 1;
  std::vector<size_t> runOffsetBase(numRuns);
  size_t numStoredOffsets = 0;
  for (size_t run = 0; run < numRuns; ++run) {
    runOffsetBase[run] = numStoredOffsets;
    numStoredOffsets += (runBegins[run + 1] - runBegins[run]) + 1;
  }
  std::vector<uint64_t> runOffsets(numStoredOffsets);
  std::vector<size_t> runSizes(numRuns);
  std::vector<uint64_t> runFileOffsets(numRuns);
  std::vector<char*> runTargets(numRuns);
  for (size_t run = 0; run < numRuns; ++run) {
    const size_t begin = runBegins[run];
    const size_t length = runBegins[run + 1] - begin;
    runSizes[run] = (length + 1) * sizeof(uint64_t);
    runFileOffsets[run] = fileOffsets[positions[begin]];
    runTargets[run] =
        reinterpret_cast<char*>(runOffsets.data() + runOffsetBase[run]);
  }
  auto missedRuns = ad_utility::readPageCacheHits(offsetsFile_.fd(), runSizes,
                                                  runFileOffsets, runTargets);

  // Fill the pairs of the served runs, and collect the pairs of the missed runs
  // for `manager`.
  std::vector<size_t> missedPositions;
  auto missedRun = missedRuns.begin();
  for (size_t run = 0; run < numRuns; ++run) {
    const size_t begin = runBegins[run];
    const size_t end = runBegins[run + 1];
    if (missedRun != missedRuns.end() && *missedRun == run) {
      ++missedRun;
      for (size_t p = begin; p < end; ++p) {
        missedPositions.push_back(positions[p]);
      }
      continue;
    }
    const uint64_t* runStart = runOffsets.data() + runOffsetBase[run];
    for (size_t p = begin; p < end; ++p) {
      const size_t withinRun = p - begin;
      offsetPairs[positions[p]] =
          OffsetPair{runStart[withinRun], runStart[withinRun + 1]};
    }
  }
  readThroughManager(manager, offsetsFile_.fd(), sizes, fileOffsets, targets,
                     missedPositions);
  return offsetPairs;
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyOnDisk::readStrings(
    ad_utility::BatchManagerBase& manager,
    ql::span<const OffsetPair> offsetPairs, bool pageCacheFastPath) const {
  // Read the string data. String `i` starts at `offset_` with length
  // `nextOffset_ - offset_`; the strings are packed contiguously into the
  // builder's buffer, with one precomputed view per word at its fixed offset.
  const size_t numIndices = offsetPairs.size();
  std::vector<size_t> sizes(numIndices);
  std::vector<uint64_t> fileOffsets(numIndices);
  for (auto&& [size, fileOffset, offsetPair] :
       ::ranges::views::zip(sizes, fileOffsets, offsetPairs)) {
    size = offsetPair.wordSize();
    fileOffset = offsetPair.offset();
  }

  // `lookupBatch` rejects empty input, so `sizes` is non-empty here, as the
  // builder requires.
  AD_CORRECTNESS_CHECK(!sizes.empty());
  ContiguousVocabBatchBuilder builder(sizes);
  // Bind the returned array: `addBatch` takes a span, and the pointers must
  // stay alive until `wait` returns.
  auto targets = builder.targets();
  ql::span<char*> targetSpan{targets};
  if (pageCacheFastPath) {
    std::vector<size_t> positions = copyRangesFromOwnedPage(
        threadOwnedPage(true), file_.fd(), fileOffsets, sizes, targetSpan);
    std::vector<size_t> subsetSizes;
    std::vector<uint64_t> subsetOffsets;
    std::vector<char*> subsetTargets;
    subsetSizes.reserve(positions.size());
    subsetOffsets.reserve(positions.size());
    subsetTargets.reserve(positions.size());
    for (size_t index : positions) {
      subsetSizes.push_back(sizes[index]);
      subsetOffsets.push_back(fileOffsets[index]);
      subsetTargets.push_back(targetSpan[index]);
    }
    auto missedSubset = ad_utility::readPageCacheHits(
        file_.fd(), subsetSizes, subsetOffsets, subsetTargets);
    std::vector<size_t> missed;
    missed.reserve(missedSubset.size());
    for (size_t subsetIndex : missedSubset) {
      missed.push_back(positions[subsetIndex]);
    }
    readThroughManager(manager, file_.fd(), sizes, fileOffsets, targetSpan,
                       missed);
  } else {
    manager.wait(manager.addBatch(file_.fd(), sizes, fileOffsets, targetSpan));
  }
  return std::move(builder).finalize();
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyOnDisk::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  auto manager = ioManagers_->pop().value();
  // Return the `manager` to the pool on every exit path (including exceptions,
  // e.g. an out-of-range index in phase 1), so we never leak an `IoManager`
  // (and its io_uring buffers) out of the pool.
  absl::Cleanup returnManager{[this, &manager]() {
    ad_utility::terminateIfThrows(
        [this, &manager]() { ioManagers_->push(std::move(manager)); },
        "returning the `IoManager` to the pool in "
        "`VocabularyOnDisk::lookupBatch`");
  }};

  const bool pageCacheFastPath =
      getRuntimeParameter<
          &RuntimeParameters::vocabularyIouringPageCacheFastPath_>() &&
      ad_utility::pageCacheFastPathIsSupported();
  auto offsetPairs = readOffsetPairs(*manager, indices, pageCacheFastPath);
  return readStrings(*manager, offsetPairs, pageCacheFastPath);
}

// _____________________________________________________________________________
VocabLookupOutput VocabularyOnDisk::lookupBatchesStreamed(
    VocabLookupInput rangeOfIndexBatches) const {
  return ad_utility::vocabulary::lookupBatchesStreamed(
      *this, std::move(rangeOfIndexBatches));
}

// _____________________________________________________________________________
VocabularyOnDisk::WordWriter::WordWriter(const std::string& outFilename)
    : file_{outFilename, "w"},
      offsetsFile_{absl::StrCat(outFilename, offsetSuffix_), "w"} {}

// _____________________________________________________________________________
uint64_t VocabularyOnDisk::WordWriter::operator()(
    std::string_view word, [[maybe_unused]] bool isExternalDummy) {
  offsetsFile_.write(&currentOffset_, sizeof(currentOffset_));
  currentOffset_ += file_.write(word.data(), word.size());
  return numWords_++;
}

// _____________________________________________________________________________
void VocabularyOnDisk::WordWriter::finishImpl() {
  // End offset of last vocabulary entry, also consistent with the empty
  // vocabulary.
  offsetsFile_.write(&currentOffset_, sizeof(currentOffset_));
  ++numWords_;
  // Write the `MmapVectorMetaData` trailer that older vocabulary files also
  // used. Only the `size_` field is read by `VocabularyOnDisk` (`capacity_`
  // and `bytesize_` are MmapVector-internal and unused here), but we keep
  // the full struct so that older binaries can also read these new files.
  ad_utility::MmapVectorMetaData{numWords_, numWords_,
                                 numWords_ * sizeof(uint64_t)}
      .writeToFile(offsetsFile_);
  file_.close();
  offsetsFile_.close();
}

// _____________________________________________________________________________
VocabularyOnDisk::WordWriter::~WordWriter() {
  if (!finishWasCalled()) {
    ad_utility::terminateIfThrows([this]() { this->finish(); },
                                  "Calling `finish` from the destructor of "
                                  "`VocabularyOnDisk::WordWriter`");
  }
}

// _____________________________________________________________________________
void VocabularyOnDisk::open(const std::string& filename) {
  file_.open(filename, "r");
  offsetsFile_.open(filename + offsetSuffix_, "r");
  // The thread-local pages belong to the previous descriptors. A new epoch
  // makes every thread drop them on its next lookup.
  slotEpoch_ = freshSlotEpoch();

  // Read the offset count from the `MmapVectorMetaData` trailer, which is
  // the canonical layout used by both old and new vocabulary files.
  uint64_t numOffsets =
      ad_utility::MmapVectorMetaData::readFromFile(offsetsFile_).size_;
  AD_CORRECTNESS_CHECK(numOffsets > 0);
  size_ = numOffsets - 1;

  // Initialize pool of persistent `BatchIoManager`s for `lookupBatch`.
  ioManagers_ = std::make_unique<ad_utility::data_structures::ThreadSafeQueue<
      std::unique_ptr<ad_utility::BatchManagerBase>>>(
      NUM_VOCAB_BATCH_IO_MANAGERS);
  bool preferIoUring = true;
  for (size_t i = 0; i < NUM_VOCAB_BATCH_IO_MANAGERS; ++i) {
    ioManagers_->push(ad_utility::makeBatchManager(preferIoUring));
  }
}
