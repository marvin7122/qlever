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

// _____________________________________________________________________________
// `lookupBatch` returns the same words as `operator[]`, in the order of the
// indices, for batches that mix internal and external words, contain
// duplicates, consist of only internal or only external words, or of a single
// word. Internal words are views into the vocabulary (no copy); external
// words are owned by the result. The empty batch is rejected.
TEST(VocabularyInternalExternal, LookupBatch) {
  std::vector<std::string> words;
  for (size_t i = 0; i < 40; ++i) {
    words.push_back(absl::StrCat("word-", i, "-", std::string(i % 5, 'y')));
  }
  // `createVocabulary` writes every second word (even index) to the internal
  // vocabulary.
  auto vocab = createVocabulary("LookupBatch")(words);
  const auto check = [&](const std::vector<size_t>& indices) {
    const auto result = vocab.lookupBatch(indices);
    ASSERT_EQ(result->size(), indices.size());
    for (size_t i = 0; i < indices.size(); ++i) {
      EXPECT_EQ((*result)[i], words[indices[i]]) << "index " << indices[i];
      const auto internal = vocab.internalVocab()[indices[i]];
      if (internal.has_value()) {
        EXPECT_EQ((*result)[i].data(), internal.value().data())
            << "internal word " << indices[i] << " was copied";
      }
    }
  };
  check(::ranges::to<std::vector<size_t>>(
      ql::views::iota(size_t{0}, words.size())));
  check({39, 0, 7, 7, 2, 1});
  check({0, 2, 4});
  check({1, 3, 5});
  check({0});
  check({1});
  AD_EXPECT_THROW_WITH_MESSAGE(vocab.lookupBatch(std::vector<size_t>{}),
                               ::testing::HasSubstr("!indices.empty()"));
}

// A result stays valid after further lookups: the external words are owned by
// the result itself, not by a buffer of the vocabulary.
TEST(VocabularyInternalExternal, LookupBatchResultOutlivesLaterLookups) {
  std::vector<std::string> words;
  for (size_t i = 0; i < 20; ++i) {
    words.push_back(absl::StrCat("w", i));
  }
  auto vocab = createVocabulary("LookupBatchResultOutlivesLaterLookups")(words);
  const std::vector<size_t> first{1, 3, 0};
  const auto firstResult = vocab.lookupBatch(first);
  // Kept alive on purpose while `firstResult` is checked: a buffer reused
  // between lookups would overwrite the words of the first result.
  [[maybe_unused]] const auto secondResult =
      vocab.lookupBatch(std::vector<size_t>{5, 7, 9});
  for (size_t i = 0; i < first.size(); ++i) {
    EXPECT_EQ((*firstResult)[i], words[first[i]]);
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
