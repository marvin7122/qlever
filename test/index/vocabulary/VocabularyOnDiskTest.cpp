// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022-2026 Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de), UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "../../util/GTestHelpers.h"
#include "../../util/MmapVectorLegacyFormat.h"
#include "../../util/RuntimeParametersTestHelpers.h"
#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "global/RuntimeParameters.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/File.h"
#include "util/Forward.h"
#include "util/MmapVector.h"

namespace {
using namespace vocabulary_test;

// Store a `VocabularyOnDisk` and read it back from file. For each instance of
// `VocabularyCreator` that exists at the same time, a different filename has to
// be chosen.
class VocabularyCreator {
 private:
  std::string vocabFilename_;

 public:
  explicit VocabularyCreator(std::string filename)
      : vocabFilename_{std::move(filename)} {
    ad_utility::deleteFile(vocabFilename_, false);
  }
  // Move-only: a moved-from creator has an empty filename and deletes nothing.
  VocabularyCreator(VocabularyCreator&& other) noexcept
      : vocabFilename_{std::exchange(other.vocabFilename_, {})} {}
  VocabularyCreator& operator=(VocabularyCreator&&) =
      delete;  // not needed (TODO: why?)
  VocabularyCreator(const VocabularyCreator&) = delete;
  VocabularyCreator& operator=(const VocabularyCreator&) = delete;

  ~VocabularyCreator() {
    if (!vocabFilename_.empty()) {
      ad_utility::deleteFile(vocabFilename_);
    }
  }

  // Create and return a `VocabularyOnDisk` from words.
  void createVocabularyImpl(const std::vector<std::string>& words) {
    auto writer = VocabularyOnDisk::WordWriter(vocabFilename_);
    for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
      EXPECT_EQ(writer(word, false), static_cast<uint64_t>(i));
    }
    writer.readableName() = "blubb";
    EXPECT_EQ(writer.readableName(), "blubb");
    static std::atomic<unsigned> doFinish = 0;
    // In some tests, call `finish` explicitly, in others let the destructor
    // handle this.
    if (doFinish.fetch_add(1) % 2 == 0) {
      writer.finish();
    }
  }

  // Create and return a `VocabularyOnDisk` from words. The ids will be [0, ..
  // words.size()).
  auto createVocabulary(const std::vector<std::string>& words) {
    createVocabularyImpl(words);
    VocabularyOnDisk vocabulary;
    vocabulary.open(vocabFilename_);
    return vocabulary;
  }
};

// Owns a `VocabularyOnDisk` together with the `VocabularyCreator` that manages
// its backing file, so the file lives as long as the vocabulary reading from
// it.
class VocabularyOnDiskHandle {
 public:
  VocabularyOnDiskHandle(std::string filename,
                         const std::vector<std::string>& words)
      : creator_{std::move(filename)},
        vocabulary_{creator_.createVocabulary(words)} {}

  // Non-copyable/movable: a copy would give two `creator_`s the same file, so
  // both destructors would unlink it (double free).
  VocabularyOnDiskHandle(const VocabularyOnDiskHandle&) = delete;
  VocabularyOnDiskHandle& operator=(const VocabularyOnDiskHandle&) = delete;
  VocabularyOnDiskHandle(VocabularyOnDiskHandle&&) = delete;
  VocabularyOnDiskHandle& operator=(VocabularyOnDiskHandle&&) = delete;

 private:
  // `vocabulary_` is declared after `creator_`, because the `vocabulary_`
  // should be destroyed before the `creator_`: the `vocabulary_` must be torn
  // down before the `creator_` unlinks the file.
  VocabularyCreator creator_;
  VocabularyOnDisk vocabulary_;

 public:
  // Access the underlying vocabulary transparently, so call sites can treat
  // the handle like the `VocabularyOnDisk` it wraps.
  VocabularyOnDisk& operator*() { return vocabulary_; }
  VocabularyOnDisk* operator->() { return &vocabulary_; }
};

VocabularyOnDiskHandle createVocabularyFromWords(
    const std::vector<std::string>& words) {
  return VocabularyOnDiskHandle{absl::StrCat(gtestCurrentTestName(), ".dat"),
                                words};
}

auto createVocabulary() {
  return [c = VocabularyCreator{absl::StrCat(gtestCurrentTestName(), ".dat")}](
             auto&&... args) mutable {
    return c.createVocabulary(AD_FWD(args)...);
  };
}

VocabularyOnDiskHandle createExampleVocabulary() {
  return createVocabularyFromWords({"alpha", "delta", "beta", "42", "gamma"});
}

// Create a `VocabularyOnDisk` from `words` and assert that `scanAll` yields
// exactly those words in order: both as bare words and as `IndexAndWord`s with
// contiguous indices `0, 1, 2, ...` (also across batch boundaries).
void expectScanAllYields(const std::vector<std::string>& words) {
  VocabularyCreator creator{gtestCurrentTestName()};
  auto vocabulary = creator.createVocabulary(words);

  EXPECT_THAT(scanAllToVector(vocabulary.scanAll()),
              ::testing::ElementsAreArray(words));

  auto indexAndWords = scanAllToIndexAndWordVector(vocabulary.scanAll());
  ASSERT_EQ(indexAndWords.size(), words.size());
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(indexAndWords[i].first, i) << "at index " << i;
    EXPECT_EQ(indexAndWords[i].second, words[i]) << "at index " << i;
  }
}

}  // namespace

TEST(VocabularyOnDisk, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(createVocabulary());
}

TEST(VocabularyOnDisk, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(createVocabulary());
}

TEST(VocabularyOnDisk, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary());
}

TEST(VocabularyOnDisk, AccessOperatorWithNonContiguousIds) {
  std::vector<std::string> words{"game",  "4",      "nobody", "33",
                                 "alpha", "\n\1\t", "222",    "1111"};
  std::vector<uint64_t> ids{2, 4, 8, 16, 17, 19, 42, 42 * 42 + 7};
  testAccessOperatorForUnorderedVocabulary(createVocabulary());
}

TEST(VocabularyOnDisk, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary());
}

// Older versions of QLever stored the offsets file as an
// `ad_utility::MmapVector<uint64_t>`. Such a file rounds its capacity up to a
// multiple of the page size, so the offsets array is followed by a (large)
// region of unused capacity before the metadata trailer at the very end. This
// test writes the offsets file in exactly that legacy format and makes sure
// that the current `VocabularyOnDisk` still reads it back correctly.
TEST(VocabularyOnDisk, ReadLegacyMmapVectorOffsetsFormat) {
  std::string vocabFilename = "vocabularyOnDisk.legacyMmapFormat";
  std::string offsetsFilename = vocabFilename + ".offsets";
  absl::Cleanup cleanup{[&]() {
    ad_utility::deleteFile(vocabFilename);
    ad_utility::deleteFile(offsetsFilename);
  }};

  const std::array<std::string_view, 7> words{
      "alpha",
      "bravo",
      "charlie",
      "",
      "a longer word with spaces and \1\n\t control chars",
      "delta",
      "z"};

  // Write the words file (the plain concatenation of all words) and collect the
  // offsets: one offset per word plus a final offset marking the end of the
  // last word.
  std::vector<uint64_t> offsets;
  {
    ad_utility::File wordsFile{vocabFilename, "w"};
    uint64_t currentOffset = 0;
    for (std::string_view word : words) {
      offsets.push_back(currentOffset);
      currentOffset += wordsFile.write(word.data(), word.size());
    }
    offsets.push_back(currentOffset);
  }
  // Write the offsets file in the legacy `MmapVector<uint64_t>` on-disk layout,
  // which rounds its capacity up to a page boundary and appends the metadata
  // trailer, reproducing the legacy format with a region of unused capacity.
  ad_utility::testing::writeLegacyMmapVectorFile(offsetsFilename, offsets);

  // Sanity check that we actually exercise the "unused capacity" path: the
  // offsets file is considerably larger than the offsets plus the trailer alone
  // would require.
  ad_utility::File offsetsFile{offsetsFilename, "r"};
  EXPECT_GT(offsetsFile.sizeOfFile(),
            static_cast<off_t>((words.size() + 1) * sizeof(uint64_t)) +
                ad_utility::MmapVectorMetaData::numBytes);
  offsetsFile.close();

  VocabularyOnDisk vocabulary;
  vocabulary.open(vocabFilename);
  ASSERT_EQ(vocabulary.size(), words.size());
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(vocabulary[i], words[i]) << "at index " << i;
  }
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAll) {
  // A basic scan over many small words (fits into a single batch).
  std::vector<std::string> words;
  for (size_t i = 0; i < 3000; ++i) {
    words.push_back(absl::StrCat("word", i, std::string(i % 7, 'x')));
  }
  expectScanAllYields(words);
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAllEmptyVocabulary) {
  VocabularyCreator creator{gtestCurrentTestName()};
  auto vocabulary = creator.createVocabulary({});
  auto range = vocabulary.scanAll();
  EXPECT_FALSE(range.get().has_value());
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAllByteLimitForcesMultipleBatches) {
  // `scanAll` caps a batch's word data at
  // `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH` (10 MB). Four words of 3 MB each
  // (12 MB total) therefore don't fit into a single batch: the byte limit (not
  // the word-count limit) forces a batch boundary after three words.
  constexpr size_t wordSize = 3'000'000;
  expectScanAllYields({std::string(wordSize, 'a'), std::string(wordSize, 'b'),
                       std::string(wordSize, 'c'), std::string(wordSize, 'd')});
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAllSingleWordExceedsLimit) {
  // A single word larger than `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH` (10 MB)
  // must still be scanned; it is returned in a batch of its own even though it
  // exceeds the limit, and the surrounding small words are unaffected.
  expectScanAllYields({"before", std::string(11'000'000, 'x'), "after"});
}

// A `lookupBatch` result must equal the individual `vocab[]` lookups for the
// same indices, including for reordered and duplicated indices.
TEST(VocabularyOnDisk, LookupBatchMatchesIndividualLookups) {
  auto vocab = createExampleVocabulary();
  std::array<size_t, 8> indices{2, 0, 3, 1, 1, 4, 0, 3};
  auto result = vocab->lookupBatch(indices);
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(*vocab, result,
                                                                indices);
}

// With `vocabulary-iouring-page-cache-fast-path`, the words and offsets that
// are in the page cache are read before the batch manager sees the rest. The
// result must be byte-identical to the result without the fast path, for runs
// of consecutive indices as well as for reordered and duplicated indices.
TEST(VocabularyOnDisk, LookupBatchPageCacheFastPathIsByteIdentical) {
  auto vocab = createExampleVocabulary();
  std::array<size_t, 13> indices{0, 1, 2, 3, 4, 2, 0, 3, 1, 1, 4, 0, 3};
  auto withoutFastPath = vocab->lookupBatch(indices);
  setRuntimeParameter<&RuntimeParameters::vocabularyIouringPageCacheFastPath_>(
      true);
  absl::Cleanup resetParameter{[]() {
    setRuntimeParameter<
        &RuntimeParameters::vocabularyIouringPageCacheFastPath_>(false);
  }};
  auto withFastPath = vocab->lookupBatch(indices);
  EXPECT_THAT(withFastPath, ::testing::ElementsAreArray(withoutFastPath));
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
      *vocab, withFastPath, indices);
}

// An empty batch is an invalid request and must throw.
TEST(VocabularyOnDisk, LookupBatchEmptyThrows) {
  auto vocab = createExampleVocabulary();
  EXPECT_ANY_THROW(vocab->lookupBatch(ql::span<const size_t>{}));
}

// An out-of-range index in a batch must throw.
TEST(VocabularyOnDisk, LookupBatchOutOfRangeIndexThrows) {
  auto vocab = createExampleVocabulary();
  std::array<size_t, 2> indices{0, 99};
  EXPECT_ANY_THROW(vocab->lookupBatch(indices));
}

// Each batch yielded by `lookupBatchesStreamed` must equal the individual
// `vocab[]` lookups for that batch's indices, and the batches must be yielded
// in input order.
TEST(VocabularyOnDisk, LookupBatchesStreamedMatchesIndividualLookups) {
  auto vocab = createExampleVocabulary();

  std::vector<std::vector<size_t>> batches{{2, 0, 3}, {1}, {4, 0, 1}};
  // `VocabLookupInput` takes ownership of the batches, so keep a copy to
  // compare against.
  const auto expectedBatches = batches;
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  vocabulary_test::assertStreamedLookupMatchesVocabularyAtIndices(
      *vocab, streamed, expectedBatches);
}

// An empty input stream (no batches) is valid and must produce no results.
TEST(VocabularyOnDisk, LookupBatchesStreamedEmptyStreamYieldsNothing) {
  auto vocab = createExampleVocabulary();
  std::vector<std::vector<size_t>> noBatches;
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(noBatches)});
  EXPECT_EQ(ql::ranges::distance(streamed), 0);
}

// An out-of-range index within a streamed batch must throw when that batch is
// pulled.
TEST(VocabularyOnDisk, LookupBatchesStreamedOutOfRangeIndexThrows) {
  auto vocab = createExampleVocabulary();
  std::vector<std::vector<size_t>> batches{{0, 99}};
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  EXPECT_ANY_THROW({
    for ([[maybe_unused]] auto& r : streamed) {
    }
  });
}

// An empty batch within the stream is an invalid request and must throw when
// the batch is pulled (an empty input stream with no batches is still valid,
// see above).
TEST(VocabularyOnDisk, LookupBatchesStreamedEmptyBatchThrows) {
  auto vocab = createExampleVocabulary();
  std::vector<std::vector<size_t>> batches{{2, 0}, {}, {1}};
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  EXPECT_ANY_THROW({
    for ([[maybe_unused]] auto& r : streamed) {
    }
  });
}

namespace {
// A batch manager that stands in for the device: it records every read that
// is submitted to it and serves it with a blocking `pread`.
class RecordingBatchManager : public ad_utility::BatchManagerBase {
 public:
  struct Read {
    int fd_;
    uint64_t offset_;
    size_t numBytes_;
  };
  explicit RecordingBatchManager(std::shared_ptr<std::vector<Read>> reads)
      : reads_{std::move(reads)} {}

  BatchHandle addBatch(int fd, ql::span<const size_t> numBytes,
                       ql::span<const uint64_t> offsets,
                       ql::span<char*> buffers) override {
    for (const auto& [n, offset, buffer] :
         ::ranges::views::zip(numBytes, offsets, buffers)) {
      reads_->push_back(Read{fd, offset, n});
      ad_utility::SyncIoPolicy::readFullyOrThrow(fd, buffer, n, offset);
    }
    return nextHandle_++;
  }
  void wait(BatchHandle) override {}

 private:
  std::shared_ptr<std::vector<Read>> reads_;
  BatchHandle nextHandle_ = 0;
};

// Ten words of 3000 bytes each ("aaa...", "bbb...", ...), stored back to back.
std::vector<std::string> tenLargeWords() {
  std::vector<std::string> words;
  for (char c = 'a'; c < 'a' + 10; ++c) {
    words.emplace_back(3000, c);
  }
  return words;
}

// Create a vocabulary from `words` with NVMe passthrough enabled (on a regular
// words file), with the given gap parameters, and let `reads` record every
// read of its batch lookups.
struct NvmeVocabularyForTesting {
  std::shared_ptr<std::vector<RecordingBatchManager::Read>> reads_ =
      std::make_shared<std::vector<RecordingBatchManager::Read>>();
  VocabularyOnDiskHandle vocabulary_;

  NvmeVocabularyForTesting(const std::vector<std::string>& words,
                           size_t maxGapBlocks, size_t maxBufferedMedianGap)
      : vocabulary_{[&]() {
          auto a = setRuntimeParameterForTest<
              &RuntimeParameters::vocabularyNvmePassthrough_>(true);
          auto b = setRuntimeParameterForTest<
              &RuntimeParameters::vocabularyNvmeMaxGapBlocks_>(maxGapBlocks);
          auto c = setRuntimeParameterForTest<
              &RuntimeParameters::vocabularyNvmeMaxBufferedMedianGap_>(
              ad_utility::MemorySize::bytes(maxBufferedMedianGap));
          return createVocabularyFromWords(words);
        }()} {
    std::vector<std::unique_ptr<ad_utility::BatchManagerBase>> managers;
    managers.push_back(std::make_unique<RecordingBatchManager>(reads_));
    vocabulary_->setIoManagersForTesting(std::move(managers));
  }

  // The recorded reads of the words file through `fd`.
  std::vector<RecordingBatchManager::Read> readsOf(int fd) const {
    std::vector<RecordingBatchManager::Read> result;
    for (const auto& read : *reads_) {
      if (read.fd_ == fd) {
        result.push_back(read);
      }
    }
    return result;
  }
};
}  // namespace

// Without `vocabulary-nvme-passthrough`, all reads of the words file go through
// the one words file descriptor, as before.
TEST(VocabularyOnDisk, NvmePassthroughIsOffByDefault) {
  auto vocab = createExampleVocabulary();
  auto [passthroughFd, bufferedFd] = vocab->wordsFileDescriptorsForTesting();
  EXPECT_EQ(passthroughFd, bufferedFd);
}

// A scattered batch (median gap above the threshold) is read from the
// passthrough file descriptor as whole-block runs, one per word with the gap
// allowance of zero, and the result is byte-identical.
TEST(VocabularyOnDisk, NvmePassthroughScatteredBatchReadsWholeBlocks) {
  const auto words = tenLargeWords();
  NvmeVocabularyForTesting nvmeVocab{words, /*maxGapBlocks=*/0,
                                     /*maxBufferedMedianGap=*/1024};
  auto& vocab = *nvmeVocab.vocabulary_;
  auto [passthroughFd, bufferedFd] = vocab.wordsFileDescriptorsForTesting();
  ASSERT_NE(passthroughFd, bufferedFd);
  // Gaps of 12000 and 9000 bytes.
  std::array<size_t, 3> indices{9, 0, 5};
  auto result = vocab.lookupBatch(indices);
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(vocab, result,
                                                                indices);
  const auto commands = nvmeVocab.readsOf(passthroughFd);
  ASSERT_EQ(commands.size(), 3u);
  const uint64_t fileSize = 10 * 3000;
  for (const auto& command : commands) {
    EXPECT_EQ(command.offset_ % 512, 0u);
    // Whole blocks, except for the last block of the file.
    EXPECT_TRUE(command.numBytes_ % 512 == 0 ||
                command.offset_ + command.numBytes_ == fileSize);
  }
  EXPECT_TRUE(nvmeVocab.readsOf(bufferedFd).empty());
}

// The gap allowance merges the words of a scattered batch into fewer, larger
// commands (here: one command of at most 128 KiB for words that span 30000
// bytes).
TEST(VocabularyOnDisk, NvmePassthroughGapAllowanceMergesCommands) {
  const auto words = tenLargeWords();
  NvmeVocabularyForTesting nvmeVocab{words, /*maxGapBlocks=*/256,
                                     /*maxBufferedMedianGap=*/1024};
  auto& vocab = *nvmeVocab.vocabulary_;
  std::array<size_t, 3> indices{9, 0, 5};
  auto result = vocab.lookupBatch(indices);
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(vocab, result,
                                                                indices);
  const auto commands =
      nvmeVocab.readsOf(vocab.wordsFileDescriptorsForTesting().first);
  ASSERT_EQ(commands.size(), 1u);
  EXPECT_EQ(commands[0].offset_, 0u);
  EXPECT_EQ(commands[0].numBytes_, 10u * 3000);
}

// A dense batch (median gap at most the threshold) is read through the
// buffered file descriptor, one exact read per word, like without passthrough.
TEST(VocabularyOnDisk, NvmePassthroughDenseBatchReadsBuffered) {
  const auto words = tenLargeWords();
  NvmeVocabularyForTesting nvmeVocab{words, /*maxGapBlocks=*/32,
                                     /*maxBufferedMedianGap=*/1024};
  auto& vocab = *nvmeVocab.vocabulary_;
  auto [passthroughFd, bufferedFd] = vocab.wordsFileDescriptorsForTesting();
  // Gaps 0, 0, 3000 (median 0).
  std::array<size_t, 4> indices{3, 1, 2, 5};
  auto result = vocab.lookupBatch(indices);
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(vocab, result,
                                                                indices);
  EXPECT_TRUE(nvmeVocab.readsOf(passthroughFd).empty());
  const auto reads = nvmeVocab.readsOf(bufferedFd);
  ASSERT_EQ(reads.size(), 4u);
  for (const auto& [read, index] : ::ranges::views::zip(reads, indices)) {
    EXPECT_EQ(read.offset_, index * 3000);
    EXPECT_EQ(read.numBytes_, 3000u);
  }
}

// A batch with a single word has no locality to exploit and is read with
// passthrough; empty words are never read.
TEST(VocabularyOnDisk, NvmePassthroughSingleAndEmptyWords) {
  std::vector<std::string> words{"", "alpha", "", "beta"};
  NvmeVocabularyForTesting nvmeVocab{words, 32, 1024};
  auto& vocab = *nvmeVocab.vocabulary_;
  std::array<size_t, 3> indices{0, 3, 2};
  auto result = vocab.lookupBatch(indices);
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(vocab, result,
                                                                indices);
  const auto commands =
      nvmeVocab.readsOf(vocab.wordsFileDescriptorsForTesting().first);
  ASSERT_EQ(commands.size(), 1u);
  // The words file has 9 bytes, so the one block is cut at its end.
  EXPECT_EQ(commands[0].offset_, 0u);
  EXPECT_EQ(commands[0].numBytes_, 9u);
}

// With the real batch managers, every access path returns the same words with
// NVMe passthrough enabled (on a regular file) as without: batch lookups of
// dense, scattered, duplicate and empty words (also with the page-cache fast
// path), `operator[]`, and `scanAll`.
TEST(VocabularyOnDisk, NvmePassthroughIsByteIdentical) {
  auto words = tenLargeWords();
  words.insert(words.begin() + 4, "");
  words.push_back("tail");
  auto enabled = setRuntimeParameterForTest<
      &RuntimeParameters::vocabularyNvmePassthrough_>(true);
  auto threshold = setRuntimeParameterForTest<
      &RuntimeParameters::vocabularyNvmeMaxBufferedMedianGap_>(
      ad_utility::MemorySize::bytes(1024));
  auto vocab = createVocabularyFromWords(words);
  auto [passthroughFd, bufferedFd] = vocab->wordsFileDescriptorsForTesting();
  EXPECT_NE(passthroughFd, bufferedFd);
  std::vector<std::vector<size_t>> batches{
      {0, 1, 2, 3}, {11, 0, 6}, {4, 4, 10, 11}, {5}, {7, 7, 7}};
  for (bool pageCacheFastPath : {false, true}) {
    auto fastPath = setRuntimeParameterForTest<
        &RuntimeParameters::vocabularyIouringPageCacheFastPath_>(
        pageCacheFastPath);
    for (const auto& indices : batches) {
      auto result = vocab->lookupBatch(indices);
      ASSERT_EQ(result.size(), indices.size());
      for (const auto& [word, index] : ::ranges::views::zip(result, indices)) {
        EXPECT_EQ(word, words[index]) << "at index " << index;
      }
    }
  }
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ((*vocab)[i], words[i]);
  }
  EXPECT_THAT(scanAllToVector(vocab->scanAll()),
              ::testing::ElementsAreArray(words));
}

// With NVMe passthrough enabled, a words file that is a character device, but
// not an NVMe generic character device, fails when the vocabulary is opened.
TEST(VocabularyOnDisk, NvmePassthroughRejectsOtherCharacterDevices) {
  std::string filename = absl::StrCat(gtestCurrentTestName(), ".dat");
  VocabularyCreator creator{filename};
  creator.createVocabulary({"alpha", "beta"});
  // Replace the words file by a symbolic link to `/dev/null`.
  ad_utility::deleteFile(filename);
  ql::filesystem::create_symlink("/dev/null", filename);
  auto enabled = setRuntimeParameterForTest<
      &RuntimeParameters::vocabularyNvmePassthrough_>(true);
  VocabularyOnDisk vocabulary;
  AD_EXPECT_THROW_WITH_MESSAGE(
      vocabulary.open(filename),
      ::testing::HasSubstr("not an NVMe generic character device"));
}
