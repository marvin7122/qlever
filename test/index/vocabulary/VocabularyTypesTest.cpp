// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/functional/function_ref.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cstring>

#include "../../util/GTestHelpers.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/File.h"

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
// `asResult` exposes the span over the filled views, and the returned aliasing
// shared_ptr keeps the backing buffer/views alive after the original owning
// shared_ptr is dropped (the whole point of the aliasing shared_ptr).
TEST(VocabBatchLookupData, AsResultExposesViewsAndKeepsDataAlive) {
  auto data = std::make_shared<VocabBatchLookupData>();
  data->buffer() = {'f', 'o', 'o', 'b', 'a', 'r'};
  data->views().emplace_back(data->buffer().data(), 3);      // "foo"
  data->views().emplace_back(data->buffer().data() + 3, 3);  // "bar"

  VocabBatchLookupResult result = VocabBatchLookupData::asResult(data);

  EXPECT_THAT(*result, ::testing::ElementsAre("foo", "bar"));

  // Drop our reference; the aliasing shared_ptr must keep the data alive.
  data.reset();
  EXPECT_THAT(*result, ::testing::ElementsAre("foo", "bar"));
}

// _____________________________________________________________________________
// An empty lookup result is valid: no views, empty span.
TEST(VocabBatchLookupData, AsResultEmpty) {
  auto data = std::make_shared<VocabBatchLookupData>();
  VocabBatchLookupResult result = VocabBatchLookupData::asResult(data);
  EXPECT_TRUE(result->empty());
}

// _____________________________________________________________________________
// Verify that an owning string batch exposes all input words as valid views.
TEST(VocabBatchLookupData, MakeStringVectorResultKeepsViewsValid) {
  auto result = makeStringVectorVocabBatchLookupResult({"alpha", "beta"});

  EXPECT_THAT(*result, ::testing::ElementsAre("alpha", "beta"));
}

// _____________________________________________________________________________
// Verify that scattering preserves input order and retains the child batch
// owner.
TEST(VocabBatchLookupData, ScatterBatchResultRetainsOwner) {
  auto first = makeStringVectorVocabBatchLookupResult({"alpha", "beta"});
  auto second = makeStringVectorVocabBatchLookupResult({"gamma"});
  const char* alphaData = (*first)[0].data();
  const char* gammaData = (*second)[0].data();

  MultiSourceVocabBatchAssembler assembler(3);
  const std::array<size_t, 2> firstPositions{2, 0};
  const std::array<size_t, 1> secondPositions{1};
  assembler.scatterSubBatchResultAtPositions(std::move(first), firstPositions);
  assembler.scatterSubBatchResultAtPositions(std::move(second),
                                             secondPositions);

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(*result, ::testing::ElementsAre("beta", "gamma", "alpha"));
  EXPECT_EQ((*result)[2].data(), alphaData);
  EXPECT_EQ((*result)[1].data(), gammaData);
}

// _____________________________________________________________________________
// Verify that keeping child batches alive preserves their original string
// storage.
TEST(VocabBatchLookupData, KeepAliveVocabBatchDoesNotCopyBytes) {
  auto firstOwner = std::make_shared<StringVectorVocabBatchLookupData>();
  firstOwner->buffer() = {"alpha", "beta"};
  firstOwner->views() = {firstOwner->buffer()[0], firstOwner->buffer()[1]};
  auto first = StringVectorVocabBatchLookupData::asResult(firstOwner);

  auto secondOwner = std::make_shared<StringVectorVocabBatchLookupData>();
  secondOwner->buffer() = {"gamma"};
  secondOwner->views() = {secondOwner->buffer()[0]};
  auto second = StringVectorVocabBatchLookupData::asResult(secondOwner);

  const char* alphaData = (*first)[0].data();
  const char* gammaData = (*second)[0].data();

  MultiSourceVocabBatchAssembler assembler(3);
  const std::array<size_t, 2> firstPos{0, 2};
  const std::array<size_t, 1> secondPos{1};
  assembler.scatterSubBatchResultAtPositions(std::move(first), firstPos);
  assembler.scatterSubBatchResultAtPositions(std::move(second), secondPos);
  firstOwner.reset();
  secondOwner.reset();

  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  EXPECT_THAT(*result, ::testing::ElementsAre("alpha", "gamma", "beta"));
  EXPECT_EQ((*result)[0].data(), alphaData);
  EXPECT_EQ((*result)[1].data(), gammaData);
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MultiSourceAssemblerRequiresStorageOwner) {
  MultiSourceVocabBatchAssembler assembler(1);
  assembler.assignWordAtPosition(0, "orphan");
  AD_EXPECT_THROW_WITH_MESSAGE(
      std::move(assembler).finalizeVocabBatchLookupResult(),
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
    const std::string filename =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
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
// A view obtained from `VocabularyInMemoryBinSearch` stays valid when the
// vocabulary is `close()`d afterwards: the batch result retains
// `wordStorage()` shared ownership of the bytes, and `close()` only installs a
// fresh empty buffer instead of mutating the old one.
TEST_F(VocabBatchLookupDataVocabTest, MultiSourceAssemblerOutlivesClose) {
  auto vocabulary = buildVocab("ram-word");
  auto maybeWord = vocabulary[0];
  ASSERT_TRUE(maybeWord.has_value());
  const char* wordData = maybeWord->data();
  MultiSourceVocabBatchAssembler assembler(1);
  assembler.assignWordAtPosition(0, maybeWord.value());
  assembler.registerStorageOwner(vocabulary.wordStorage());
  auto result = std::move(assembler).finalizeVocabBatchLookupResult();

  vocabulary.close();
  EXPECT_EQ(vocabulary.size(), 0u);
  EXPECT_THAT(*result, ::testing::ElementsAre("ram-word"));
  EXPECT_EQ((*result)[0].data(), wordData);
}

// _____________________________________________________________________________
// Same guarantee when the vocabulary object is destroyed entirely while the
// batch result still lives: shared ownership of the word storage keeps the
// bytes alive past the destructor.
TEST_F(VocabBatchLookupDataVocabTest,
       MultiSourceAssemblerOutlivesVocabularyDestruction) {
  auto vocabulary = std::make_optional(buildVocab("other-word"));
  auto maybeWord = (*vocabulary)[0];
  ASSERT_TRUE(maybeWord.has_value());
  const char* wordData = maybeWord->data();
  MultiSourceVocabBatchAssembler assembler(1);
  assembler.assignWordAtPosition(0, maybeWord.value());
  assembler.registerStorageOwner(vocabulary->wordStorage());
  auto result = std::move(assembler).finalizeVocabBatchLookupResult();

  vocabulary.reset();
  EXPECT_THAT(*result, ::testing::ElementsAre("other-word"));
  EXPECT_EQ((*result)[0].data(), wordData);
}

// _____________________________________________________________________________
// Tests for `PmrVocabBatchLookupData`: the `monotonic_buffer_resource` backing
// used when words are produced incrementally with sizes not known up front
// (e.g. decompressing one word at a time in `CompressedVocabulary`). Each word
// gets a pointer-stable allocation, so appending a later (differently sized)
// word never invalidates an earlier `string_view`, unlike the single growing
// buffer of `VocabBatchLookupData`, which would reallocate and leave the
// already-recorded views dangling.
TEST(PmrVocabBatchLookupData, PmrAsResultPointerStableAcrossAppends) {
  auto data = std::make_shared<PmrVocabBatchLookupData>();
  data->buffer() = std::make_unique<ql::pmr::monotonic_buffer_resource>();
  auto* resource = data->buffer().get();

  // Allocate each word separately from the monotonic resource and record a view
  // into it. Because the allocations are pointer-stable, the first view stays
  // valid after the second word is appended.
  auto appendWord = [&](std::string_view word) {
    char* p = static_cast<char*>(resource->allocate(word.size()));
    std::memcpy(p, word.data(), word.size());
    data->views().emplace_back(p, word.size());
  };
  appendWord("foo");
  std::string_view firstView = data->views().front();
  appendWord("barbaz");
  // Appending the second word did not invalidate the first view.
  EXPECT_EQ(firstView, "foo");

  VocabBatchLookupResult result = PmrVocabBatchLookupData::asResult(data);
  EXPECT_THAT(*result, ::testing::ElementsAre("foo", "barbaz"));

  // The aliasing shared_ptr keeps the resource (and thus its allocations)
  // alive.
  data.reset();
  EXPECT_THAT(*result, ::testing::ElementsAre("foo", "barbaz"));
}

// _____________________________________________________________________________
// An empty pmr lookup result is valid: no views, empty span (matches the
// `VocabBatchLookupData` `AsResultEmpty` case).
TEST(PmrVocabBatchLookupData, PmrAsResultEmpty) {
  auto data = std::make_shared<PmrVocabBatchLookupData>();
  data->buffer() = std::make_unique<ql::pmr::monotonic_buffer_resource>();
  VocabBatchLookupResult result = PmrVocabBatchLookupData::asResult(data);
  EXPECT_TRUE(result->empty());
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ScatterSubBatchSizeMismatchThrows) {
  auto batch = makeStringVectorVocabBatchLookupResult({"only-one"});
  MultiSourceVocabBatchAssembler assembler(2);
  const std::array<size_t, 2> positions{0, 1};
  // Two positions but one word in the batch.
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(batch), positions),
      ::testing::HasSubstr("subBatchResult->size() == resultPositions.size()"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ArenaVocabBatchBuilderKeepsViewsAlive) {
  ArenaVocabBatchBuilder builder(2);
  builder.appendWord("one");
  builder.appendWord("two");
  auto result = std::move(builder).finalize();
  EXPECT_THAT(*result, ::testing::ElementsAre("one", "two"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MakePmrVocabBatchLookupResultCopiesWords) {
  auto result = makePmrVocabBatchLookupResult({"first", "second"});
  EXPECT_THAT(*result, ::testing::ElementsAre("first", "second"));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, ScatterSubBatchDoubleWriteThrows) {
  auto batch1 = makeStringVectorVocabBatchLookupResult({"first"});
  auto batch2 = makeStringVectorVocabBatchLookupResult({"second"});
  MultiSourceVocabBatchAssembler assembler(2);
  const std::array<size_t, 1> pos0{0};
  assembler.scatterSubBatchResultAtPositions(std::move(batch1), pos0);
  // Attempting to scatter to position 0 again must throw because it was already
  // written.
  AD_EXPECT_THROW_WITH_MESSAGE(
      assembler.scatterSubBatchResultAtPositions(std::move(batch2), pos0),
      ::testing::HasSubstr("!slotFilledTracking_[targetPosition]"));
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
  EXPECT_THAT(*result, ::testing::ElementsAre("x", ""));
}

// _____________________________________________________________________________
TEST(VocabBatchLookupData, MultiSourceVocabBatchAssemblerSuccessfulAssembly) {
  MultiSourceVocabBatchAssembler assembler(3);

  // Direct word assignment at position 1:
  assembler.assignWordAtPosition(1, "middle");

  // Scatter sub-batch at positions 0 and 2:
  auto subBatch = makeStringVectorVocabBatchLookupResult({"first", "last"});
  const std::array<size_t, 2> subPositions{0, 2};
  assembler.scatterSubBatchResultAtPositions(std::move(subBatch), subPositions);

  // Finalize and check results:
  auto result = std::move(assembler).finalizeVocabBatchLookupResult();
  ASSERT_NE(result, nullptr);
  EXPECT_THAT(*result, ::testing::ElementsAre("first", "middle", "last"));
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
  // Slot 1 remains unassigned.

  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)std::move(assembler).finalizeVocabBatchLookupResult(),
      ::testing::HasSubstr("std::all_of(slotFilledTracking_"));
}
