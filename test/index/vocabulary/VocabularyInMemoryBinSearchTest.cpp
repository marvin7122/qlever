// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include <array>

#include "./VocabularyTestHelpers.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "util/Forward.h"

namespace {
using namespace vocabulary_test;

// A common suffix for all files to reduce the probability of colliding file
// names, when other tests are run in parallel.
std::string suffix = ".vocabularyInMemoryBinSearchTest.dat";

// Store a VocabularyInMemoryBinSearch and read it back from file. For each
// instance of `VocabularyCreator` that exists at the same time, a different
// filename has to be chosen.
class VocabularyCreator {
 private:
  std::string vocabFilename_;

  // Remove both the vocabulary and its .ids sidecar before setup and after
  // teardown so stale data cannot affect another test using the same filename.
  void removeVocabularyFiles() {
    ad_utility::deleteFile(vocabFilename_, false);
    ad_utility::deleteFile(vocabFilename_ + ".ids", false);
  }

 public:
  explicit VocabularyCreator(std::string filename)
      : vocabFilename_{filename + suffix} {
    removeVocabularyFiles();
  }
  ~VocabularyCreator() { removeVocabularyFiles(); }

  // Create and return a `VocabularyInMemoryBinSearch` from words and ids.
  // `words` and `ids` must have the same size. If `ids` is `nullopt`, then
  // ascending IDs starting at 0 will be automatically assigned to the words.
  auto createVocabularyImpl(
      const std::vector<std::string>& words,
      std::optional<std::vector<uint64_t>> ids = std::nullopt) {
    VocabularyInMemoryBinSearch vocabulary;
    {
      auto writer = VocabularyInMemoryBinSearch::WordWriter(vocabFilename_);
      if (ids.has_value()) {
        AD_CORRECTNESS_CHECK(ids.value().size() == words.size());
      }
      uint64_t idx = 0;
      for (auto& word : words) {
        size_t actualIdx = ids.has_value() ? ids.value().at(idx) : idx;
        EXPECT_EQ(writer(word, actualIdx), actualIdx);
        ++idx;
      }
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

  // Like `createVocabularyImpl`, but the resulting vocabulary will be destroyed
  // and re-initialized from disk before it is returned.
  auto createVocabularyFromDiskImpl(
      const std::vector<std::string>& words,
      std::optional<std::vector<uint64_t>> ids = std::nullopt) {
    { createVocabularyImpl(words, std::move(ids)); }
    VocabularyInMemoryBinSearch vocabulary;
    vocabulary.open(vocabFilename_);
    return vocabulary;
  }

  // Create and return a `VocabularyInMemoryBinSearch` from words. The ids will
  // be [0, .. words.size()).
  auto createVocabulary(const std::vector<std::string>& words) {
    return createVocabularyImpl(words);
  }

  // Create and return a `VocabularyInMemoryBinSearch` from words. The ids will
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

TEST(VocabularyInMemoryBinSearch, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(
      createVocabulary("lowerUpperBoundStdLess1"));
  testUpperAndLowerBoundWithStdLess(
      createVocabularyFromDisk("lowerUpperBoundStdLess2"));
}

TEST(VocabularyInMemoryBinSearch, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      createVocabulary("lowerUpperBoundNumeric1"));
  testUpperAndLowerBoundWithNumericComparator(
      createVocabularyFromDisk("lowerUpperBoundNumeric2"));
}

TEST(VocabularyInMemoryBinSearch, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary("AccessOperator1"));
  testAccessOperatorForUnorderedVocabulary(
      createVocabularyFromDisk("AccessOperator2"));
}

TEST(VocabularyInMemoryBinSearch, AccessOperatorWithNonContiguousIds) {
  testAccessOperatorForUnorderedVocabulary(
      createVocabulary("AccessOperatorWithNonContiguousIds1"));
  testAccessOperatorForUnorderedVocabulary(
      createVocabularyFromDisk("AccessOperatorWithNonContiguousIds2"));
}

TEST(VocabularyInMemoryBinSearch, LookupBatchOutlivesClose) {
  auto vocab = createVocabulary("LookupBatchOutlivesVocabulary")(
      std::vector<std::string>{"alpha", "beta", "gamma"});
  const std::array<size_t, 4> indices{2, 0, 2, 1};
  auto result = vocab.lookupBatch(indices);
  vocab.close();

  EXPECT_THAT(result,
              ::testing::ElementsAre("gamma", "alpha", "gamma", "beta"));
}

TEST(VocabularyInMemoryBinSearch, LookupBatchRejectsMissingIndex) {
  auto vocab = createVocabulary("LookupBatchRejectsMissingIndex")(
      std::vector<std::string>{"alpha", "beta"});
  const std::array<size_t, 1> missingIndex{2};

  EXPECT_THROW(vocab.lookupBatch(missingIndex), ad_utility::Exception);
}

TEST(VocabularyInMemoryBinSearch, LookupBatchRejectsEmptyBatch) {
  auto vocab = createVocabulary("LookupBatchRejectsEmptyBatch")(
      std::vector<std::string>{"alpha", "beta"});
  const std::array<size_t, 0> indices{};

  EXPECT_THROW(vocab.lookupBatch(indices), ad_utility::Exception);
}

TEST(VocabularyInMemoryBinSearch, LookupBatchOutlivesVocabulary) {
  VocabBatchLookupResult result;
  {
    auto vocab = createVocabulary("LookupBatchOutlivesVocabularyOnly")(
        std::vector<std::string>{"alpha", "beta", "gamma"});
    const std::array<size_t, 4> indices{2, 0, 2, 1};
    result = vocab.lookupBatch(indices);
  }

  EXPECT_THAT(result,
              ::testing::ElementsAre("gamma", "alpha", "gamma", "beta"));
}

TEST(VocabularyInMemoryBinSearch, ErrorOnNonAscendingIds) {
  std::vector<std::string> words{"game", "4", "nobody"};
  std::vector<uint64_t> ids{2, 4, 3};
  VocabularyCreator creator1{"ErrorOnNonAscendingIds1"};
  ASSERT_THROW(creator1.createVocabularyImpl(words, ids),
               ad_utility::Exception);
  VocabularyCreator creator2{"ErrorOnNonAscendingIds2"};
  ASSERT_THROW(creator2.createVocabularyFromDiskImpl(words, ids),
               ad_utility::Exception);
}

TEST(VocabularyInMemoryBinSearch, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary("EmptyVocabulary"));
}
