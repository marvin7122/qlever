//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "VocabularyTestHelpers.h"
#include "index/vocabulary/StringSortComparator.h"
#include "index/vocabulary/UnicodeVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"

using Vocab = ad_utility::vocabulary::UnicodeVocabulary<
    ad_utility::vocabulary::VocabularyInMemory,
    ad_utility::vocabulary::SimpleStringComparator>;
using namespace vocabulary_test;

auto createVocabulary(const std::vector<std::string>& words) {
  ad_utility::vocabulary::SimpleStringComparator comparator{"en", "us", false};
  Vocab v{comparator};
  ad_utility::vocabulary::VocabularyInMemory::Words w;
  w.build(words);
  return Vocab(comparator, std::move(w));
}

using Level = ad_utility::vocabulary::SimpleStringComparator::Level;
TEST(ad_utility::vocabulary::UnicodeVocabulary, LowercaseAscii) {
  const std::vector<std::string> words{"alpha", "beta",    "camma",
                                       "delta", "epsilon", "frikadelle"};
  std::vector<Level> levels{Level::PRIMARY,   Level::SECONDARY,
                            Level::TERTIARY,  Level::QUARTERNARY,
                            Level::IDENTICAL, Level::TOTAL};

  for (const auto level : levels) {
    auto makeWordSmaller = [](std::string word) {
      word.back()--;
      return word;
    };
    auto makeWordLarger = [](std::string word) {
      word.back()++;
      return word;
    };

    testUpperAndLowerBoundContiguousIDs(createVocabulary(words), makeWordLarger,
                                        makeWordSmaller, level, words);
  }
}

TEST(ad_utility::vocabulary::UnicodeVocabulary, UpperAndLowercase) {
  const std::vector<std::string> words{"alpha", "ALPHA", "beta", "BETA"};

  // On the `PRIMARY` and `SECONDARY` Level, uppercase letters are equal to
  // their lowercase equivalents, so we cannot use these levels here.
  std::vector<Level> levels{Level::TERTIARY, Level::QUARTERNARY,
                            Level::IDENTICAL, Level::TOTAL};

  for (const auto level : levels) {
    auto makeWordSmaller = [](std::string word) {
      if (word.back() == 'A') {
        word.back() = 'a';
      } else {
        word.back()--;
      }
      return word;
    };

    auto makeWordLarger = [](std::string word) {
      if (word.back() == 'a') {
        word.back() = 'A';
      } else {
        word.back()++;
      }
      return word;
    };

    testUpperAndLowerBoundContiguousIDs(createVocabulary(words), makeWordLarger,
                                        makeWordSmaller, level, words);
  }
}

TEST(ad_utility::vocabulary::UnicodeVocabulary, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary);
}

TEST(ad_utility::vocabulary::UnicodeVocabulary, EmptyVocabulary) {
  testEmptyVocabularyWithComparator(createVocabulary, Level::PRIMARY);
  testEmptyVocabularyWithComparator(createVocabulary, Level::TOTAL);
}

// _____________________________________________________________________________
TEST(ad_utility::vocabulary::UnicodeVocabulary, ScanAll) {
  // `scanAll` must yield all words in order (it simply delegates to the
  // underlying vocabulary).
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta"};
  auto vocab = createVocabulary(words);
  EXPECT_THAT(scanAllToVector(vocab.scanAll()),
              ::testing::ElementsAreArray(words));
}

// _____________________________________________________________________________
TEST(ad_utility::vocabulary::UnicodeVocabulary, ScanAllEmptyVocabulary) {
  auto vocab = createVocabulary({});
  EXPECT_TRUE(scanAllToVector(vocab.scanAll()).empty());
}
