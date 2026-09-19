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

#include <thread>

#include "../../util/GTestHelpers.h"
#include "../../util/MmapVectorLegacyFormat.h"
#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "global/Constants.h"
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
  // words.size()). `preferIoUring == false` forces the synchronous `pread`
  // fallback backend, which covers that path in the concurrency tests.
  auto createVocabulary(const std::vector<std::string>& words,
                        bool preferIoUring = true) {
    createVocabularyImpl(words);
    VocabularyOnDisk vocabulary;
    vocabulary.open(vocabFilename_, preferIoUring);
    return vocabulary;
  }
};

// Owns a `VocabularyOnDisk` together with the `VocabularyCreator` that manages
// its backing file, so the file lives as long as the vocabulary reading from
// it.
class VocabularyOnDiskHandle {
 public:
  VocabularyOnDiskHandle(std::string filename,
                         const std::vector<std::string>& words,
                         bool preferIoUring = true)
      : creator_{std::move(filename)},
        vocabulary_{creator_.createVocabulary(words, preferIoUring)} {}

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
    const std::vector<std::string>& words, bool preferIoUring = true) {
  // The sync-fallback variant gets its own backing file so both variants can
  // coexist within one test.
  return VocabularyOnDiskHandle{
      absl::StrCat(gtestCurrentTestName(),
                   preferIoUring ? ".dat" : ".sync.dat"),
      words, preferIoUring};
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

// Build a deterministic word list with varying lengths, including an empty
// word, for the concurrency tests below.
std::vector<std::string> makeConcurrentTestWords(size_t numWords) {
  std::vector<std::string> words;
  words.reserve(numWords);
  words.emplace_back("");
  for (size_t i = 1; i < numWords; ++i) {
    words.push_back(absl::StrCat("word", i, "_", std::string(i % 17, 'x')));
  }
  return words;
}

// Deterministic pseudo-random batches of vocabulary indices (fixed seed, so no
// engine state is shared between threads).
std::vector<std::vector<size_t>> makeConcurrentTestBatches(size_t vocabSize,
                                                           size_t numBatches,
                                                           size_t batchSize) {
  std::vector<std::vector<size_t>> batches(numBatches,
                                           std::vector<size_t>(batchSize));
  uint64_t state = 0x9E3779B97F4A7C15ull;
  for (auto& batch : batches) {
    for (auto& idx : batch) {
      state = state * 6364136223846793005ull + 1442695040888963407ull;
      idx = (state >> 33) % vocabSize;
    }
  }
  return batches;
}

// Snapshot every batch via serial `lookupBatch` calls: the reference that the
// concurrent runs below must reproduce byte-identically.
std::vector<std::vector<std::string>> snapshotBatchesSerial(
    const VocabularyOnDisk& vocab,
    const std::vector<std::vector<size_t>>& batches) {
  std::vector<std::vector<std::string>> snapshots;
  snapshots.reserve(batches.size());
  for (const auto& batch : batches) {
    auto result = vocab.lookupBatch(batch);
    snapshots.emplace_back(result->begin(), result->end());
  }
  return snapshots;
}

// Run `lookupBatch` concurrently: thread `t` looks up `perThreadBatches[t]` in
// order and snapshots each result. Worker exceptions are collected into
// `errors` so they surface as test failures instead of `std::terminate`.
std::vector<std::vector<std::vector<std::string>>> snapshotBatchesConcurrent(
    const VocabularyOnDisk& vocab,
    const std::vector<std::vector<std::vector<size_t>>>& perThreadBatches,
    std::vector<std::string>& errors) {
  const size_t numThreads = perThreadBatches.size();
  std::vector<std::vector<std::vector<std::string>>> snapshots(numThreads);
  errors.assign(numThreads, {});
  std::vector<std::thread> threads;
  threads.reserve(numThreads);
  for (size_t t = 0; t < numThreads; ++t) {
    threads.emplace_back([&, t] {
      try {
        auto& own = snapshots[t];
        own.reserve(perThreadBatches[t].size());
        for (const auto& batch : perThreadBatches[t]) {
          auto result = vocab.lookupBatch(batch);
          own.emplace_back(result->begin(), result->end());
        }
      } catch (const std::exception& e) {
        errors[t] = e.what();
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  return snapshots;
}

// Assert that no worker failed and every per-thread snapshot equals the serial
// reference for the same batch sequence.
void expectConcurrentSnapshotsMatchSerial(
    const std::vector<std::vector<std::vector<std::string>>>& snapshots,
    const std::vector<std::vector<std::string>>& expected,
    const std::vector<std::string>& errors) {
  for (const auto& error : errors) {
    EXPECT_TRUE(error.empty()) << error;
  }
  for (const auto& perThread : snapshots) {
    ASSERT_EQ(perThread.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
      EXPECT_THAT(perThread[i], ::testing::ElementsAreArray(expected[i]))
          << "batch " << i;
    }
  }
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

// Concurrent `lookupBatch` calls from many threads must return byte-identical
// results to the serial path: each thread drives its exclusively owned ring,
// so no two threads ever share a ring on the I/O path.
TEST(VocabularyOnDisk, LookupBatchConcurrentMatchesSerial) {
  constexpr size_t numWords = 2000;
  constexpr size_t numThreads = 8;
  auto vocab = createVocabularyFromWords(makeConcurrentTestWords(numWords));
  auto batches =
      makeConcurrentTestBatches(numWords, /*numBatches=*/32, /*batchSize=*/64);
  auto expected = snapshotBatchesSerial(*vocab, batches);
  std::vector<std::vector<std::vector<size_t>>> perThread(numThreads, batches);
  std::vector<std::string> errors;
  auto snapshots = snapshotBatchesConcurrent(*vocab, perThread, errors);
  expectConcurrentSnapshotsMatchSerial(snapshots, expected, errors);
}

// Same as above, but with the synchronous `pread` fallback backend forced: the
// per-thread plumbing (owned sync managers, pool fallback) must be correct
// independent of the backend.
TEST(VocabularyOnDisk, LookupBatchConcurrentSyncFallbackMatchesSerial) {
  constexpr size_t numWords = 1000;
  constexpr size_t numThreads = 4;
  auto vocab = createVocabularyFromWords(makeConcurrentTestWords(numWords),
                                         /*preferIoUring=*/false);
  auto batches =
      makeConcurrentTestBatches(numWords, /*numBatches=*/16, /*batchSize=*/32);
  auto expected = snapshotBatchesSerial(*vocab, batches);
  std::vector<std::vector<std::vector<size_t>>> perThread(numThreads, batches);
  std::vector<std::string> errors;
  auto snapshots = snapshotBatchesConcurrent(*vocab, perThread, errors);
  expectConcurrentSnapshotsMatchSerial(snapshots, expected, errors);
}

// Oversubscription: more threads than `NUM_VOCAB_BATCH_IO_MANAGERS`. Threads
// without an owned ring share the pooled managers, and the per-thread rotation
// of the batch order migrates load across threads. Everything must still match
// the serial path.
TEST(VocabularyOnDisk, LookupBatchOversubscribedThreadsMatchesSerial) {
  constexpr size_t numWords = 500;
  const size_t numThreads = 3 * NUM_VOCAB_BATCH_IO_MANAGERS + 1;
  auto vocab = createVocabularyFromWords(makeConcurrentTestWords(numWords));
  auto batches =
      makeConcurrentTestBatches(numWords, /*numBatches=*/24, /*batchSize=*/16);
  auto expected = snapshotBatchesSerial(*vocab, batches);
  // Thread `t` looks up all batches starting at offset `t`, wrapping around.
  std::vector<std::vector<std::vector<size_t>>> perThread(numThreads);
  for (size_t t = 0; t < numThreads; ++t) {
    perThread[t].reserve(batches.size());
    for (size_t k = 0; k < batches.size(); ++k) {
      perThread[t].push_back(batches[(t + k) % batches.size()]);
    }
  }
  std::vector<std::string> errors;
  auto snapshots = snapshotBatchesConcurrent(*vocab, perThread, errors);
  for (const auto& error : errors) {
    EXPECT_TRUE(error.empty()) << error;
  }
  ASSERT_EQ(snapshots.size(), numThreads);
  for (size_t t = 0; t < numThreads; ++t) {
    ASSERT_EQ(snapshots[t].size(), batches.size());
    for (size_t k = 0; k < batches.size(); ++k) {
      EXPECT_THAT(snapshots[t][k], ::testing::ElementsAreArray(
                                       expected[(t + k) % batches.size()]))
          << "thread " << t << " batch " << k;
    }
  }
}
