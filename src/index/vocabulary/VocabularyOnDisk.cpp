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
#include <absl/strings/str_cat.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <numeric>

#include "backports/filesystem.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "util/ExceptionHandling.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/Log.h"
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
  bufferedWordsFile().read(result.data(), offsetAndSize.size_,
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
               bufferedWordsFile().read(data.data(), data.size(),
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

  // The pairs of consecutive indices overlap in the file, so read each run of
  // consecutive indices `[runBegins[r], runBegins[r + 1])` as one range of
  // `runLength + 1` offsets into `runOffsets`.
  std::vector<size_t> runBegins{0};
  for (size_t i = 1; i < numIndices; ++i) {
    if (indices[i] != indices[i - 1] + 1) {
      runBegins.push_back(i);
    }
  }
  runBegins.push_back(numIndices);
  const size_t numRuns = runBegins.size() - 1;
  std::vector<uint64_t> runOffsets(numIndices + numRuns);
  std::vector<size_t> runSizes(numRuns);
  std::vector<uint64_t> runFileOffsets(numRuns);
  std::vector<char*> runTargets(numRuns);
  for (size_t run = 0; run < numRuns; ++run) {
    const size_t begin = runBegins[run];
    const size_t length = runBegins[run + 1] - begin;
    runSizes[run] = (length + 1) * sizeof(uint64_t);
    runFileOffsets[run] = fileOffsets[begin];
    runTargets[run] = reinterpret_cast<char*>(runOffsets.data() + begin + run);
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
      for (size_t i = begin; i < end; ++i) {
        missedPositions.push_back(i);
      }
      continue;
    }
    const uint64_t* runStart = runOffsets.data() + begin + run;
    for (size_t i = begin; i < end; ++i) {
      offsetPairs[i] = OffsetPair{runStart[i - begin], runStart[i - begin + 1]};
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
    size = offsetPair.nextOffset_ - offsetPair.offset_;
    fileOffset = offsetPair.offset_;
  }

  // `lookupBatch` rejects empty input, so `sizes` is non-empty here, as the
  // builder requires.
  ContiguousVocabBatchBuilder builder(sizes);
  // Bind the returned array: the reads take spans, and the pointers must stay
  // alive until all reads have completed.
  auto targets = builder.targets();
  if (nvme_) {
    readWordsWithNvmePassthrough(manager, sizes, fileOffsets,
                                 ql::span<char*>{targets}, pageCacheFastPath);
  } else if (pageCacheFastPath) {
    auto missed =
        ad_utility::readPageCacheHits(file_.fd(), sizes, fileOffsets, targets);
    readThroughManager(manager, file_.fd(), sizes, fileOffsets,
                       ql::span<char*>{targets}, missed);
  } else {
    manager.wait(manager.addBatch(file_.fd(), sizes, fileOffsets,
                                  ql::span<char*>{targets}));
  }
  return std::move(builder).finalize();
}

// _____________________________________________________________________________
void VocabularyOnDisk::readWordsWithNvmePassthrough(
    ad_utility::BatchManagerBase& manager, ql::span<const size_t> sizes,
    ql::span<const uint64_t> fileOffsets, ql::span<char*> targets,
    bool pageCacheFastPath) const {
  namespace nvme = ad_utility::nvmePassthrough;
  const int bufferedFd = nvme_->bufferedWordsFile_.fd();
  // The positions of the words that are not yet read.
  std::vector<size_t> positions;
  if (pageCacheFastPath) {
    positions =
        ad_utility::readPageCacheHits(bufferedFd, sizes, fileOffsets, targets);
  } else {
    positions.resize(sizes.size());
    std::iota(positions.begin(), positions.end(), size_t{0});
  }
  if (positions.empty()) {
    return;
  }
  auto select = [&positions](auto values) {
    return ::ranges::to_vector(
        positions |
        ql::views::transform([&values](size_t i) { return values[i]; }));
  };
  const auto selectedSizes = select(sizes);
  const auto selectedOffsets = select(fileOffsets);
  auto selectedTargets = select(targets);

  // Route the batch by its locality. A batch whose words are close together
  // (median gap at most `maxBufferedMedianGapBytes_`) is read through the
  // buffered words file, where the kernel's readahead serves several words per
  // device read, which passthrough cannot do. A batch with larger gaps (or
  // with fewer than two words) pays about one device read per word on both
  // paths, and passthrough has the cheaper per-read path.
  const auto medianGap = nvme::medianGapBytes(selectedOffsets, selectedSizes);
  const bool usePassthrough =
      !medianGap.has_value() ||
      medianGap.value() > nvme_->maxBufferedMedianGapBytes_;
  size_t bucket = 0;
  while (bucket + 1 < nvme_->medianGapHistogram_.size() &&
         medianGap.has_value() &&
         medianGap.value() >= (uint64_t{1024} << (2 * bucket))) {
    ++bucket;
  }
  if (!medianGap.has_value()) {
    bucket = nvme_->medianGapHistogram_.size() - 1;
  }
  ++nvme_->medianGapHistogram_[bucket];

  if (!usePassthrough) {
    ++nvme_->numBufferedBatches_;
    nvme_->numBufferedWords_ += positions.size();
    logNvmePassthroughCounters();
    manager.wait(manager.addBatch(bufferedFd, selectedSizes, selectedOffsets,
                                  selectedTargets));
    return;
  }

  // Read whole-block runs that cover the words (merging gaps of at most
  // `maxGapBlocks_` blocks) into a staging buffer, then copy each word to its
  // target. The runs are block-aligned, so the batch manager can submit each
  // of them as one native NVMe read.
  const auto plan = nvme::planBlockReads(
      selectedOffsets, selectedSizes, nvme_->maxGapBlocks_, nvme_->readLimit_);
  std::vector<char> staging(plan.stagingBytes);
  std::vector<size_t> runSizes;
  std::vector<uint64_t> runOffsets;
  std::vector<char*> runTargets;
  for (const auto& run : plan.runs) {
    runSizes.push_back(run.numBytes);
    runOffsets.push_back(run.fileOffset);
    runTargets.push_back(staging.data() + run.stagingOffset);
  }
  ++nvme_->numPassthroughBatches_;
  nvme_->numPassthroughWords_ += positions.size();
  nvme_->numPassthroughCommands_ += plan.runs.size();
  nvme_->numPassthroughCommandBytes_ +=
      ::ranges::accumulate(runSizes, uint64_t{0});
  logNvmePassthroughCounters();
  manager.wait(manager.addBatch(file_.fd(), runSizes, runOffsets, runTargets));
  for (auto&& [target, slice] :
       ::ranges::views::zip(selectedTargets, plan.slices)) {
    if (slice.numBytes > 0) {
      std::memcpy(target, staging.data() + slice.stagingOffset, slice.numBytes);
    }
  }
}

// _____________________________________________________________________________
void VocabularyOnDisk::logNvmePassthroughCounters() const {
  const uint64_t numBatches =
      nvme_->numPassthroughBatches_ + nvme_->numBufferedBatches_;
  if (numBatches % 64 != 0) {
    return;
  }
  std::string histogram;
  for (const auto& count : nvme_->medianGapHistogram_) {
    absl::StrAppend(&histogram, histogram.empty() ? "" : " ", count.load());
  }
  AD_LOG_INFO << "NVMe passthrough routing for \"" << file_.name()
              << "\": " << nvme_->numPassthroughBatches_ << " batches ("
              << nvme_->numPassthroughWords_ << " words) with passthrough in "
              << nvme_->numPassthroughCommands_ << " commands of "
              << nvme_->numPassthroughCommandBytes_ << " bytes, "
              << nvme_->numBufferedBatches_ << " batches ("
              << nvme_->numBufferedWords_
              << " words) buffered; median gap histogram (<1K <4K <16K <64K "
                 "<256K <1M <4M >=4M or none): "
              << histogram << std::endl;
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
  ad_utility::nvmePassthrough::Options nvmeOptions;
  nvme_.reset();
  if (getRuntimeParameter<&RuntimeParameters::vocabularyNvmePassthrough_>()) {
    nvmeOptions = setUpNvmePassthrough(filename);
  }
  bool preferIoUring = true;
  for (size_t i = 0; i < NUM_VOCAB_BATCH_IO_MANAGERS; ++i) {
    ioManagers_->push(ad_utility::makeBatchManager(
        preferIoUring, /*ringSize=*/256, nvmeOptions));
  }
  // An NVMe generic character device can only be read with passthrough
  // commands, which need `io_uring`.
  if (nvme_ && !preferIoUring &&
      ad_utility::nvmePassthrough::isPassthroughCandidate(
          file_.fd(), nvmeOptions.namespaceId)) {
    AD_THROW(absl::StrCat("The vocabulary words file \"", filename,
                          "\" is an NVMe device, which requires io_uring, but "
                          "io_uring is not available"));
  }
}

// _____________________________________________________________________________
ad_utility::nvmePassthrough::Options VocabularyOnDisk::setUpNvmePassthrough(
    const std::string& filename) {
  namespace nvme = ad_utility::nvmePassthrough;
  const uint32_t namespaceId = static_cast<uint32_t>(
      getRuntimeParameter<&RuntimeParameters::vocabularyNvmeNamespaceId_>());
  auto state = std::make_unique<NvmePassthroughState>();
  state->maxGapBlocks_ =
      getRuntimeParameter<&RuntimeParameters::vocabularyNvmeMaxGapBlocks_>();
  state->maxBufferedMedianGapBytes_ =
      getRuntimeParameter<
          &RuntimeParameters::vocabularyNvmeMaxBufferedMedianGap_>()
          .getBytes();
  struct stat wordsFileStat {};
  AD_CORRECTNESS_CHECK(::fstat(file_.fd(), &wordsFileStat) == 0);
  if (S_ISREG(wordsFileStat.st_mode)) {
    // No device to send commands to, but the routing and the coalesced reads
    // are the same, which makes them testable without NVMe hardware.
    state->bufferedWordsFile_.open(filename, "r");
    state->readLimit_ = static_cast<uint64_t>(wordsFileStat.st_size);
  } else if (S_ISCHR(wordsFileStat.st_mode)) {
    // Find the block device of the same namespace via the name of the
    // character device, e.g. `/sys/dev/char/239:1` -> `.../ng1n1` -> `nvme1n1`.
    const std::string sysfsPath =
        absl::StrCat("/sys/dev/char/", major(wordsFileStat.st_rdev), ":",
                     minor(wordsFileStat.st_rdev));
    ql::error_code error;
    const auto target = ql::filesystem::read_symlink(sysfsPath, error);
    const auto blockDeviceName =
        error ? std::nullopt
              : nvme::blockDeviceNameForGenericCharDevice(
                    target.filename().string());
    if (!blockDeviceName.has_value()) {
      AD_THROW(absl::StrCat("The vocabulary words file \"", filename,
                            "\" is a character device, but not an NVMe "
                            "generic character device (/dev/ngXnY)"));
    }
    state->bufferedWordsFile_.open(absl::StrCat("/dev/", *blockDeviceName),
                                   "r");
    const int blockFd = state->bufferedWordsFile_.fd();
    const std::optional<uint32_t> expected{namespaceId};
    if (nvme::nvmeNamespaceIdOf(file_.fd()) != expected ||
        nvme::nvmeNamespaceIdOf(blockFd) != expected) {
      AD_THROW(absl::StrCat(
          "The NVMe devices of the vocabulary words file \"", filename,
          "\" do not report the namespace id ", namespaceId,
          " (runtime parameter vocabulary-nvme-namespace-id)"));
    }
    int logicalBlockSize = 0;
    uint64_t deviceSize = 0;
    AD_CORRECTNESS_CHECK(::ioctl(blockFd, BLKSSZGET, &logicalBlockSize) == 0);
    AD_CORRECTNESS_CHECK(::ioctl(blockFd, BLKGETSIZE64, &deviceSize) == 0);
    if (static_cast<uint64_t>(logicalBlockSize) != nvme::kCoalesceBlockSize) {
      AD_THROW(absl::StrCat(
          "NVMe passthrough for the vocabulary requires a "
          "namespace with 512-byte logical blocks, but ",
          *blockDeviceName, " has ", logicalBlockSize, "-byte blocks"));
    }
    state->readLimit_ = deviceSize;
  } else {
    AD_THROW(
        absl::StrCat("NVMe passthrough requires the vocabulary words file "
                     "\"",
                     filename,
                     "\" to be a regular file or an NVMe generic "
                     "character device"));
  }
  nvme_ = std::move(state);
  AD_LOG_INFO << "NVMe passthrough enabled for the vocabulary words file \""
              << filename << "\" (namespace " << namespaceId << ")"
              << std::endl;
  return {true, namespaceId, static_cast<uint32_t>(nvme::kCoalesceBlockSize)};
}

// _____________________________________________________________________________
void VocabularyOnDisk::setIoManagersForTesting(
    std::vector<std::unique_ptr<ad_utility::BatchManagerBase>> managers) {
  ioManagers_ = std::make_unique<ad_utility::data_structures::ThreadSafeQueue<
      std::unique_ptr<ad_utility::BatchManagerBase>>>(managers.size());
  for (auto& manager : managers) {
    ioManagers_->push(std::move(manager));
  }
}
