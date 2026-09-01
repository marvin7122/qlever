// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/functional/function_ref.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <optional>
#include <utility>

#include "../../util/GTestHelpers.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/File.h"
#include <functional>
#include "test/util/GTestHelpers.h"

namespace {
// _____________________________________________________________________________
// A class that executes a passed function in its constructor.
class Caller {
 public:
  explicit Caller(absl::FunctionRef<void()> f) { std::invoke(f); }
};

// _____________________________________________________________________________
// A class inheriting from `WordWriterBase` that throws when initializing a
// member.
class WordWriterThrowing : public WordWriterBase {
 private:
  Caller caller_;

 public:
  // ___________________________________________________________________________
  WordWriterThrowing()
      : caller_{[]() { throw std::runtime_error("Constructor failed"); }} {}
  uint64_t operator()(std::string_view, bool) override { return 0; }
  void finishImpl() override {}
};

// _____________________________________________________________________________
// A class inheriting from `WordWriterBase` that doesn't call finish.
class WordWriterNoFinish : public WordWriterBase {
 public:
  WordWriterNoFinish() {}
  uint64_t operator()(std::string_view, bool) override { return 0; }
  void finishImpl() override {}
};
}  // namespace

// _____________________________________________________________________________
TEST(VocabularyTypes, verifyWordWriterBaseDestructorBehavesAsExpected) {
  // Test that the original exception from `WordWriterThrowing` is propagated.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(WordWriterThrowing{},
                                        ::testing::StrEq("Constructor failed"),
                                        std::runtime_error);

  // Test that the no finish exception is thrown when destroying a
  // `WordWriterNoFinish`.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      WordWriterNoFinish{}, ::testing::HasSubstr("WordWriterBase::finish was"),
      std::runtime_error);

  // Test that no exception is thrown when `finish` is called.
  EXPECT_NO_THROW({
    WordWriterNoFinish writer;
    writer.finish();
  });
}

// _____________________________________________________________________________

TEST(VocabBatchLookupData, ContiguousBuilderExposesViewsAndKeepsDataAlive) {
  const std::array<size_t, 2> sizes{3, 3};
  ContiguousVocabBatchBuilder builder(sizes);
  ASSERT_EQ(builder.targets().size(), 2u);
  std::memcpy(builder.targets()[0], "foo", 3);
  std::memcpy(builder.targets()[1], "bar", 3);

  VocabBatchLookupResult result = std::move(builder).finalize();
  EXPECT_THAT(result, ::testing::ElementsAre("foo", "bar"));
}

// _____________________________________________________________________________

TEST(VocabBatchLookupData, ContiguousBuilderEmpty) {
  AD_EXPECT_THROW_WITH_MESSAGE(ContiguousVocabBatchBuilder({}),
                               ::testing::HasSubstr("!wordSizes.empty()"));
}

// _____________________________________________________________________________

TEST(VocabBatchLookupData, MakeStringVectorResultKeepsViewsValid) {
  auto result = makeStringVectorVocabBatchLookupResult({"alpha", "beta"});

  EXPECT_THAT(result, ::testing::ElementsAre("alpha", "beta"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ScatterBatchResultRetainsOwner) {
  auto first = makeStringVectorVocabBatchLookupResult({"apple", "banana"});
  auto second = makeStringVectorVocabBatchLookupResult({"cherry"});

  MultiSourceVocabBatchAssembler assembler(3);
  const std::array<size_t, 2> firstPos{0, 2};
  const std::array<size_t, 1> secondPos{1};
  assembler.scatterSubBatchResultAtPositions(std::move(first), firstPos);
  assembler.scatterSubBatchResultAtPositions(std::move(second), secondPos);

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(result, ::testing::ElementsAre("apple", "cherry", "banana"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MultiSourceAssemblerDoesNotCopyBytes) {
  auto first = makeStringVectorVocabBatchLookupResult({"alpha", "beta"});
  auto second = makeStringVectorVocabBatchLookupResult({"gamma"});

  const char* alphaData = first[0].data();
  const char* gammaData = second[0].data();

  MultiSourceVocabBatchAssembler assembler(3);
  const std::array<size_t, 2> firstPos{0, 2};
  const std::array<size_t, 1> secondPos{1};
  assembler.scatterSubBatchResultAtPositions(std::move(first), firstPos);
  assembler.scatterSubBatchResultAtPositions(std::move(second), secondPos);

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(result, ::testing::ElementsAre("alpha", "gamma", "beta"));
  EXPECT_EQ(result[0].data(), alphaData);
  EXPECT_EQ(result[1].data(), gammaData);
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MultiSourceAssemblerRequiresStorageOwner) {
  MultiSourceVocabBatchAssembler assembler(1);
  assembler.assignWordAtPosition(0, "orphan");
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)std::move(assembler).finalizeVocabBatchLookupResult(),
      ::testing::HasSubstr("!storageOwners_.empty()"));
}

// _____________________________________________________________________________
// Fixture for the "batch result outlives its vocabulary" tests: provides a
// one-word `VocabularyInMemoryBinSearch` built via a `WordWriter`, with
// per-test filenames so the suites are independent.
class VocabBatchLookupDataVocabTest : public ::testing::Test {
 protected:
  // Build a vocabulary containing exactly `word` at index 0 and open it.
  VocabularyInMemoryBinSearch buildVocab(std::string_view word) {
    const std::string filename = gtestCurrentTestName();
    ad_utility::deleteFile(filename, false);
    ad_utility::deleteFile(filename + ".ids", false);
    VocabularyInMemoryBinSearch vocabulary;
    {
      VocabularyInMemoryBinSearch::WordWriter writer{filename};
      writer(word, 0);
      writer.finish();
    }
    vocabulary.open(filename);
    return vocabulary;
  }
};

// _____________________________________________________________________________
// A batch result obtained from `VocabularyInMemoryBinSearch` stays valid when
// the vocabulary is `close()`d afterwards: the result owns the bytes, and
// `close()` only installs a fresh empty buffer instead of mutating the old one.
TEST_F(VocabBatchLookupDataVocabTest, MultiSourceAssemblerOutlivesClose) {
  auto vocabulary = buildVocab("ram-word");
  const std::array<size_t, 1> positions{0};
  auto batch = vocabulary.lookupBatch(positions);
  const char* wordData = batch[0].data();
  MultiSourceVocabBatchAssembler assembler(1);
  assembler.scatterSubBatchResultAtPositions(std::move(batch), positions);
  auto result = std::move(assembler).finalizeVocabBatchLookupResult();

  vocabulary.close();
  EXPECT_EQ(vocabulary.size(), 0u);
  EXPECT_THAT(result, ::testing::ElementsAre("ram-word"));
  EXPECT_EQ(result[0].data(), wordData);
}

// _____________________________________________________________________________
// Same guarantee when the vocabulary object is destroyed entirely while the
// batch result still lives: shared ownership of the word storage keeps the
// bytes alive past the destructor.
TEST_F(VocabBatchLookupDataVocabTest,
       MultiSourceAssemblerOutlivesVocabularyDestruction) {
  auto vocabulary = std::make_optional(buildVocab("other-word"));
  const std::array<size_t, 1> positions{0};
  auto batch = vocabulary->lookupBatch(positions);
  const char* wordData = batch[0].data();
  MultiSourceVocabBatchAssembler assembler(1);
  assembler.scatterSubBatchResultAtPositions(std::move(batch), positions);
  auto result = std::move(assembler).finalizeVocabBatchLookupResult();

  vocabulary.reset();
  EXPECT_THAT(result, ::testing::ElementsAre("other-word"));
  EXPECT_EQ(result[0].data(), wordData);
}

// _____________________________________________________________________________
// Verify that `ArenaVocabBatchBuilder` supports incremental word appends:
// each word is copied into the arena-backed storage in order, and the
// finalized batch result exposes all appended words with their contents
// intact.
TEST(PmrVocabBatchLookupData, IncrementalAppendsProduceWordsInOrder) {
  ArenaVocabBatchBuilder builder(2);
  builder.appendWord("foo");
  builder.appendWord("barbaz");

  VocabBatchLookupResult result = std::move(builder).finalize();
  EXPECT_THAT(result, ::testing::ElementsAre("foo", "barbaz"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ScatterSubBatchSizeMismatchThrows) {
  auto batch = makeStringVectorVocabBatchLookupResult({"only-one"});
  MultiSourceVocabBatchAssembler assembler(2);
  const std::array<size_t, 2> positions{0, 1};
  // Test a mismatch between two target positions and one batch word.
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(batch), positions),
      ::testing::HasSubstr("subBatchResult.size() == targetPositions.size()"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ArenaVocabBatchBuilderKeepsViewsAlive) {
  ArenaVocabBatchBuilder builder(2);
  builder.appendWord("one");
  builder.appendWord("two");
  auto result = std::move(builder).finalize();
  EXPECT_THAT(result, ::testing::ElementsAre("one", "two"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MakePmrVocabBatchLookupResultCopiesWords) {
  auto result = makePmrVocabBatchLookupResult({"first", "second"});
  EXPECT_THAT(result, ::testing::ElementsAre("first", "second"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ScatterSubBatchDoubleWriteThrows) {
  auto batch1 = makeStringVectorVocabBatchLookupResult({"first"});
  auto batch2 = makeStringVectorVocabBatchLookupResult({"second"});
  MultiSourceVocabBatchAssembler assembler(2);
  const std::array<size_t, 1> pos0{0};
  assembler.scatterSubBatchResultAtPositions(std::move(batch1), pos0);
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(batch2), pos0),
      ::testing::HasSubstr("!slotFilledTracking_[resultPosition]"));
}

// _____________________________________________________________________________
// Verify that a legitimately empty word does not trip any correctness check:
// the filled/unfilled invariant is structural, not based on the view contents.
TEST(VocabBatchLookupData, MultiSourceVocabBatchAssemblerToleratesEmptyWord) {
  auto batch = makeStringVectorVocabBatchLookupResult({"", "x"});
  MultiSourceVocabBatchAssembler assembler(2);
  const std::array<size_t, 2> positions{1, 0};
  assembler.scatterSubBatchResultAtPositions(std::move(batch), positions);

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(result, ::testing::ElementsAre("x", ""));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MultiSourceVocabBatchAssemblerSuccessfulAssembly) {
  MultiSourceVocabBatchAssembler assembler(3);

  assembler.assignWordAtPosition(1, "middle");

  auto subBatch = makeStringVectorVocabBatchLookupResult({"first", "last"});
  const std::array<size_t, 2> subPositions{0, 2};
  assembler.scatterSubBatchResultAtPositions(std::move(subBatch), subPositions);

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  ASSERT_FALSE(result.empty());
  EXPECT_THAT(result, ::testing::ElementsAre("first", "middle", "last"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData,
     MultiSourceVocabBatchAssemblerDoubleAssignmentThrows) {
  MultiSourceVocabBatchAssembler assembler(2);
  assembler.assignWordAtPosition(0, "first");

  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.assignWordAtPosition(0, "overwrite"),
      ::testing::HasSubstr("!slotFilledTracking_[resultPosition]"));
}

// _____________________________________________________________________________
// Formal architectural verification: prove that MultiSourceVocabBatchAssembler
// strictly satisfies the `ad_utility::InvariantStatefulClass` concept at
// compile-time.
static_assert(
    ad_utility::InvariantStatefulClass<MultiSourceVocabBatchAssembler>,
    "MultiSourceVocabBatchAssembler must satisfy "
    "ad_utility::InvariantStatefulClass");

// _____________________________________________________________________________
TEST(VocabBatchLookupData,
     MultiSourceVocabBatchAssemblerIncompleteCoverageThrows) {
  MultiSourceVocabBatchAssembler assembler(2);
  auto subBatch = makeStringVectorVocabBatchLookupResult({"first"});
  const std::array<size_t, 1> subPositions{0};
  assembler.scatterSubBatchResultAtPositions(std::move(subBatch), subPositions);
  // Leave slot 1 unassigned.

  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)std::move(assembler).finalizeVocabBatchLookupResult(),
      ::testing::HasSubstr("ql::ranges::all_of("));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData,
     MultiSourceVocabBatchAssemblerOutOfBoundsPositionThrows) {
  MultiSourceVocabBatchAssembler assembler(2);
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.assignWordAtPosition(2, "out-of-bounds"),
      ::testing::HasSubstr("resultPosition < assembledWordViews_.size()"));

  auto subBatch = makeStringVectorVocabBatchLookupResult({"out-of-bounds"});
  const std::array<size_t, 1> invalidPos{5};
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(subBatch),
                                                 invalidPos),
      ::testing::HasSubstr("resultPosition < assembledWordViews_.size()"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MarkerBatchLookupsAndMergeInInputOrder) {
  MarkerBatchLookups<2> lookups;
  lookups[0] = makeStringVectorVocabBatchLookupResult({"apple", "cherry"});
  lookups[1] = makeStringVectorVocabBatchLookupResult({"banana"});

  IndicesAndPositionsByMarker<2> partitions;
  partitions[0].addPair(0, 0);  // apple -> pos 0
  partitions[1].addPair(0, 1);  // banana -> pos 1
  partitions[0].addPair(1, 2);  // cherry -> pos 2

  auto result =
      mergeMarkerBatchesInInputOrder(std::move(lookups), partitions);
  EXPECT_THAT(result, ::testing::ElementsAre("apple", "banana", "cherry"));
}
