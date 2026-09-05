// Copyright 2024 - 2026, The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <johannes.kalmbach@gmail.com>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <vector>

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
    ad_utility::deleteFile(vocabFilename_, false);
  }
  ~VocabularyCreator() { ad_utility::deleteFile(vocabFilename_); }

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

// `lookupBatch` must match `operator[]` in request order, including cache
// hits (even `i` in the writer: stored in RAM) and misses (odd `i`: disk
// only), plus reordered and duplicated indices.
TEST(VocabularyInternalExternal, LookupBatchMatchesAccessOperator) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta",
                                       "epsilon"};
  auto vocab = createVocabulary("LookupBatch")(words);
  const std::array<size_t, 7> indices{4, 1, 0, 3, 1, 2, 4};
  auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  EXPECT_ANY_THROW(vocab.lookupBatch(ql::span<const size_t>{}));

  // Even writer indices are RAM-cached; odd indices are disk-only.
  const std::array<size_t, 3> ramOnly{0, 2, 4};
  assertLookupResultMatchesVocabularyAtIndices(
      vocab, vocab.lookupBatch(ramOnly), ramOnly);
  const std::array<size_t, 3> diskOnly{1, 3, 1};
  assertLookupResultMatchesVocabularyAtIndices(
      vocab, vocab.lookupBatch(diskOnly), diskOnly);
}

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

// Test that `lookupBatch` with a mix of internal (RAM-cached) and external
// (disk-only) indices returns results in request order and that the returned
// `VocabBatchLookupResult` keeps both the internal vocabulary's word storage
// and the external vocabulary's disk result alive (multi-owner semantics).
TEST(VocabularyInternalExternal, LookupBatchMixedInternalExternalOwnersAlive) {
  // Use the WordWriter directly to control which words go to internal vs external.
  // Even indices (0, 2, 4) -> internal + external (isExternal = false)
  // Odd indices  (1, 3)    -> external only (isExternal = true)
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta",
                                       "epsilon"};
  std::string filename = "LookupBatchMixedOwners" + suffix;
  ad_utility::deleteFile(filename, false);
  {
    auto writerPtr = VocabularyInternalExternal::makeDiskWriterPtr(filename);
    auto& writer = *writerPtr;
    for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
      EXPECT_EQ(writer(word, i % 2 == 1), static_cast<uint64_t>(i));
    }
    writer.finish();
  }
  VocabularyInternalExternal vocab;
  vocab.open(filename);

  // Request indices in mixed order: external(1), internal(0), external(3),
  // internal(4), internal(2), external(1 duplicate), internal(0 duplicate).
  const std::array<size_t, 7> indices{1, 0, 3, 4, 2, 1, 0};
  auto result = vocab.lookupBatch(indices);

  // Validate results match operator[] in request order.
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);

  // The result is a MultiOwnerVocabBatchLookupData that holds:
  // - the external vocabulary's disk lookup result (for indices 1, 3)
  // - the internal vocabulary's wordStorage (for indices 0, 2, 4)
  // As long as `result` is alive, both owners are kept alive and all
  // string_views remain valid. Accessing the views here validates this.
  EXPECT_EQ((*result)[0], "beta");   // external
  EXPECT_EQ((*result)[1], "alpha");  // internal
  EXPECT_EQ((*result)[2], "delta");  // external
  EXPECT_EQ((*result)[3], "epsilon"); // internal
  EXPECT_EQ((*result)[4], "gamma");  // internal
  EXPECT_EQ((*result)[5], "beta");   // external (duplicate)
  EXPECT_EQ((*result)[6], "alpha");  // internal (duplicate)

  // Also test the case where all indices hit internal (no external owner needed).
  const std::array<size_t, 3> ramOnly{0, 2, 4};
  auto ramResult = vocab.lookupBatch(ramOnly);
  assertLookupResultMatchesVocabularyAtIndices(vocab, ramResult, ramOnly);
  EXPECT_EQ((*ramResult)[0], "alpha");
  EXPECT_EQ((*ramResult)[1], "gamma");
  EXPECT_EQ((*ramResult)[2], "epsilon");

  // And the case where all indices hit external (no internal owner needed).
  const std::array<size_t, 3> diskOnly{1, 3, 1};
  auto diskResult = vocab.lookupBatch(diskOnly);
  assertLookupResultMatchesVocabularyAtIndices(vocab, diskResult, diskOnly);
  EXPECT_EQ((*diskResult)[0], "beta");
  EXPECT_EQ((*diskResult)[1], "delta");
  EXPECT_EQ((*diskResult)[2], "beta");
}
