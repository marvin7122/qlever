// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include <array>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "../../util/RuntimeParametersTestHelpers.h"
#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Exception.h"
#include "util/Forward.h"

namespace {
using namespace vocabulary_test;

// A common suffix for all files to reduce the probability of colliding file
// names, when other tests are run in parallel.
std::string suffix = ".vocabularyInternalExternalTest.dat";

// Store a VocabularyInternalExternal and read it back from file. For each
// instance of `VocabularyCreator` that exists at the same time, a different
// filename has to be chosen.
class VocabularyCreator {
 private:
  std::string vocabFilename_;

 public:
  explicit VocabularyCreator(const std::string& filename)
      : vocabFilename_{filename + suffix} {
    deleteVocabularyFiles<VocabularyInternalExternal>(vocabFilename_);
  }
  ~VocabularyCreator() {
    deleteVocabularyFiles<VocabularyInternalExternal>(vocabFilename_);
  }

  // Create and return a `VocabularyInternalExternal` from the given words.
  auto createVocabularyImpl(const std::vector<std::string>& words) {
    VocabularyInternalExternal vocabulary;
    {
      auto writerPtr =
          VocabularyInternalExternal::makeDiskWriterPtr(vocabFilename_);
      auto& writer = *writerPtr;
      for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
        EXPECT_EQ(writer(word, i % 2 == 0), static_cast<uint64_t>(i));
      }
      writer.readableName() = "blabbiblu";
      EXPECT_EQ(writer.readableName(), "blabbiblu");
      static std::atomic<unsigned> doFinish = 0;
      // In some tests, call `finish` explicitly, in others let the destructor
      // handle this.
      if (doFinish.fetch_add(1) % 2 == 0) {
        writer.finish();
      }
    }
    vocabulary.open(vocabFilename_);
    return vocabulary;
  }

  // Like `createVocabularyImpl` above, but the resulting vocabulary will be
  // destroyed and re-initialized from disk before it is returned.
  auto createVocabularyFromDiskImpl(const std::vector<std::string>& words) {
    { createVocabularyImpl(words); }
    VocabularyInternalExternal vocabulary;
    vocabulary.open(vocabFilename_);
    return vocabulary;
  }

  // Create and return a `VocabularyInternalExternal` from words. The ids will
  // be [0, .. words.size()).
  auto createVocabulary(const std::vector<std::string>& words) {
    return createVocabularyImpl(words);
  }

  // Create and return a `VocabularyInternalExternal` from words. The ids will
  // be [0, .. words.size()). Note: The resulting vocabulary will be destroyed
  // and re-initialized from disk before it is returned.
  auto createVocabularyFromDisk(const std::vector<std::string>& words) {
    return createVocabularyFromDiskImpl(words);
  }
};

auto createVocabulary(std::string filename) {
  return [c = VocabularyCreator{std::move(filename)}](auto&&... args) mutable {
    return c.createVocabulary(AD_FWD(args)...);
  };
}

auto createVocabularyFromDisk(std::string filename) {
  return [c = VocabularyCreator{std::move(filename)}](auto&&... args) mutable {
    return c.createVocabularyFromDisk(AD_FWD(args)...);
  };
}

}  // namespace

TEST(VocabularyInternalExternal, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(
      createVocabulary("lowerUpperBoundStdLess1"));
  testUpperAndLowerBoundWithStdLess(
      createVocabularyFromDisk("lowerUpperBoundStdLess2"));
}

TEST(VocabularyInternalExternal, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      createVocabulary("lowerUpperBoundNumeric1"));
  testUpperAndLowerBoundWithNumericComparator(
      createVocabularyFromDisk("lowerUpperBoundNumeric2"));
}

TEST(VocabularyInternalExternal, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary("AccessOperator1"));
  testAccessOperatorForUnorderedVocabulary(
      createVocabularyFromDisk("AccessOperator2"));
}

// _____________________________________________________________________________
TEST(VocabularyInternalExternal, LookupBatchMatchesAccessOperator) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta",
                                       "epsilon"};
  // The batch result must preserve request order across all-internal,
  // all-external, and mixed-source requests, including duplicates.
  auto vocab = createVocabulary("LookupBatch")(words);
  const std::array<size_t, 7> indices{4, 1, 0, 3, 1, 2, 4};
  auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  AD_EXPECT_THROW_WITH_MESSAGE(vocab.lookupBatch(ql::span<const size_t>{}),
                               ::testing::HasSubstr("!indices.empty()"));

  // The test writer marks even IDs as external; odd IDs and ID 0 (the first
  // milestone) are also stored in the internal vocabulary.
  const std::array<size_t, 3> ramOnly{0, 1, 3};
  assertLookupResultMatchesVocabularyAtIndices(
      vocab, vocab.lookupBatch(ramOnly), ramOnly);
  const std::array<size_t, 3> diskOnly{2, 4, 2};
  assertLookupResultMatchesVocabularyAtIndices(
      vocab, vocab.lookupBatch(diskOnly), diskOnly);

  // Keep a mixed result alive while another lookup is performed, exercising
  // ownership of the backing storage returned by both vocabulary sources.
  auto retainedMixedResult = vocab.lookupBatch(indices);
  auto subsequentResult = vocab.lookupBatch(ramOnly);
  assertLookupResultMatchesVocabularyAtIndices(vocab, retainedMixedResult,
                                               indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, subsequentResult,
                                               ramOnly);
}

// _____________________________________________________________________________
// Words of the internal vocabulary are returned as views into it (two lookups
// of the same word see the same bytes); words of the external vocabulary are
// read into a buffer that each result owns.
TEST(VocabularyInternalExternal, LookupBatchDoesNotCopyInternalWords) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta"};
  auto vocab = createVocabulary("LookupBatchDoesNotCopyInternalWords")(words);
  // ID 1 is in the internal vocabulary, ID 2 only in the external one (see
  // `createVocabularyImpl`).
  const std::array<size_t, 2> indices{1, 2};
  auto first = vocab.lookupBatch(indices);
  auto second = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, first, indices);
  EXPECT_EQ(first[0].data(), second[0].data());
  EXPECT_NE(first[1].data(), second[1].data());
}

// _____________________________________________________________________________
// The external words of a result are owned by the result, so a batch of only
// external words stays valid after the vocabulary is closed.
TEST(VocabularyInternalExternal, LookupBatchExternalWordsOutliveClose) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta"};
  auto vocab = createVocabulary("LookupBatchExternalWordsOutliveClose")(words);
  const std::array<size_t, 2> diskOnly{2, 2};
  auto result = vocab.lookupBatch(diskOnly);
  vocab.close();

  EXPECT_THAT(result, ::testing::ElementsAre("gamma", "gamma"));
}

// _____________________________________________________________________________
TEST(VocabularyInternalExternal, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary("EmptyVocabulary"));
}

// _____________________________________________________________________________
TEST(VocabularyInternalExternal, ScanAll) {
  // `scanAll` delegates to the external vocabulary and must yield all words in
  // order.
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta"};
  auto vocab = createVocabulary("ScanAll")(words);
  EXPECT_THAT(scanAllToVector(vocab.scanAll()),
              ::testing::ElementsAreArray(words));
}

// _____________________________________________________________________________
TEST(VocabularyInternalExternal, ScanAllEmptyVocabulary) {
  auto vocab = createVocabulary("ScanAllEmpty")(std::vector<std::string>{});
  EXPECT_TRUE(scanAllToVector(vocab.scanAll()).empty());
}

// _____________________________________________________________________________
// `lookupBatch` and `operator[]` return the same words with and without the
// rank directory of the internal vocabulary
// (`vocabulary-internal-rank-lookup`), for random sparse and dense sets of
// internal words and random batches with repetitions.
TEST(VocabularyInternalExternal, LookupBatchIsIndependentOfInternalLookupMode) {
  const std::string filename =
      "LookupBatchIsIndependentOfInternalLookupMode" + suffix;
  for (double internalDensity : {0.0, 0.01, 0.3, 0.9, 1.0}) {
    deleteVocabularyFiles<VocabularyInternalExternal>(filename);
    std::mt19937_64 gen{static_cast<uint64_t>(internalDensity * 100) + 1};
    std::bernoulli_distribution isInternal{internalDensity};
    std::vector<std::string> words;
    {
      // A milestone distance larger than the vocabulary, so that only the
      // first word and the random internal words are in RAM.
      VocabularyInternalExternal::WordWriter writer{filename, 1'000'000};
      for (size_t i = 0; i < 3000; ++i) {
        words.push_back(absl::StrCat("word", 1'000'000 + i));
        EXPECT_EQ(writer(words.back(), !isInternal(gen)), i);
      }
      writer.finish();
    }
    std::uniform_int_distribution<size_t> pick{0, words.size() - 1};
    std::vector<std::vector<size_t>> batches{{0}, {words.size() - 1}};
    for (size_t batchSize : {1, 7, 500, 4000}) {
      std::vector<size_t> batch;
      for (size_t i = 0; i < batchSize; ++i) {
        batch.push_back(pick(gen));
      }
      batches.push_back(std::move(batch));
    }
    std::vector<size_t> all(words.size());
    std::iota(all.begin(), all.end(), size_t{0});
    batches.push_back(all);
    ql::ranges::reverse(all);
    batches.push_back(all);

    for (bool rankLookup : {false, true}) {
      auto cleanupRank = setRuntimeParameterForTest<
          &RuntimeParameters::vocabularyInternalRankLookup_>(rankLookup);
      VocabularyInternalExternal vocab;
      vocab.open(filename);
      EXPECT_EQ(vocab.internalVocab().hasIndexRankDirectory(), rankLookup);
      // The prefetch distance (only used with the rank directory) must not
      // change the results, also when it exceeds the batch size.
      for (size_t prefetchDistance : {0, 1, 4, 32, 100'000}) {
        auto cleanupPrefetch = setRuntimeParameterForTest<
            &RuntimeParameters::vocabularyInternalRankPrefetchDistance_>(
            prefetchDistance);
        for (const auto& batch : batches) {
          std::vector<std::string> expected;
          for (size_t index : batch) {
            expected.push_back(words.at(index));
            ASSERT_EQ(vocab[index], words.at(index));
          }
          EXPECT_THAT(vocab.lookupBatch(batch),
                      ::testing::ElementsAreArray(expected))
              << "rank lookup " << rankLookup << ", prefetch distance "
              << prefetchDistance << ", density " << internalDensity;
        }
      }
    }
  }
  deleteVocabularyFiles<VocabularyInternalExternal>(filename);
}
