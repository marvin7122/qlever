// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

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

TEST(VocabularyInternalExternal, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary("EmptyVocabulary"));
}

// `numWords` distinct words of varying length, in index order.
std::vector<std::string> makeWords(size_t numWords) {
  std::vector<std::string> words;
  words.reserve(numWords);
  for (size_t i : ql::views::iota(size_t{0}, numWords)) {
    words.push_back(absl::StrCat("word-", i, "-", std::string(i % 5, 'y')));
  }
  return words;
}

// _____________________________________________________________________________
// Verify that `lookupBatch` returns the same words as `operator[]`, in the
// order of the indices, for batches that mix internal and external words,
// contain duplicates, consist of only internal or only external words, or
// consist of a single word; that internal words are views into the vocabulary
// (no copy);
// and that the empty batch is rejected.
TEST(VocabularyInternalExternal, LookupBatch) {
  const auto words = makeWords(40);
  // `createVocabulary` writes the words with an even index as external only;
  // the words with an odd index, and the word with index 0 (which is always
  // cached), are also in the internal vocabulary.
  const auto vocab = createVocabulary("LookupBatch")(words);
  const auto isInternal = [&vocab](size_t index) {
    return vocab.internalVocab()[index].has_value();
  };
  ASSERT_TRUE(isInternal(0));
  ASSERT_TRUE(isInternal(1));
  ASSERT_FALSE(isInternal(2));

  const auto checkLookupBatch = [&](const std::vector<size_t>& indices) {
    const auto result = vocab.lookupBatch(indices);
    vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(vocab, result,
                                                                  indices);
    for (const auto& [position, index] : ::ranges::views::enumerate(indices)) {
      if (const auto internalWord = vocab.internalVocab()[index];
          internalWord.has_value()) {
        EXPECT_EQ((*result)[position].data(), internalWord.value().data())
            << "internal word " << index << " was copied";
      }
    }
  };

  // All words, a reordered batch with duplicates, and single words.
  checkLookupBatch(::ranges::to<std::vector<size_t>>(
      ql::views::iota(size_t{0}, words.size())));
  checkLookupBatch({39, 0, 7, 7, 2, 1});
  checkLookupBatch({1});
  checkLookupBatch({2});
  // Only internal words, only external words.
  checkLookupBatch({1, 3, 5});
  checkLookupBatch({2, 4, 6});
  AD_EXPECT_THROW_WITH_MESSAGE(vocab.lookupBatch(ql::span<const size_t>{}),
                               ::testing::HasSubstr("!indices.empty()"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      vocab.lookupBatch(std::vector<size_t>{1, words.size()}),
      ::testing::HasSubstr("index < size()"));
}

// _____________________________________________________________________________
// Verify that a result of a mixed batch (so that the external words come from
// a sub-batch result that the result keeps alive) stays valid after a later
// lookup whose result has already been destroyed: the external words are
// owned by the result itself, not by a buffer of the vocabulary.
TEST(VocabularyInternalExternal, LookupBatchResultOutlivesLaterLookups) {
  const auto words = makeWords(20);
  const auto vocab =
      createVocabulary("LookupBatchResultOutlivesLaterLookups")(words);

  // Mixed batch: 2 and 4 are external, 1 is internal (see `createVocabulary`).
  const std::vector<size_t> first{2, 1, 4};
  const auto firstResult = vocab.lookupBatch(first);
  {
    const auto secondResult = vocab.lookupBatch(std::vector<size_t>{8, 3, 10});
    EXPECT_EQ((*secondResult)[0], words[8]);
  }

  for (const auto& [index, word] : ::ranges::views::zip(first, *firstResult)) {
    EXPECT_EQ(word, words[index]);
  }
}

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
