// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <array>

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include "util/GTestHelpers.h"
#include "VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/span.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/Exception.h"
#include "util/Serializer/ByteBufferSerializer.h"

namespace {

using namespace vocabulary_test;
using namespace ad_utility::vocabulary;

// A stateless test "compressor" that applies a trivial transformation to a string.
// Satisfies the in-place decompression interface: `maxDecompressedSize` provides
// the output bound, while `decompressInto` writes into caller-provided storage
// and returns the number of bytes written.
struct DummyDecoder {
  static size_t maxDecompressedSize(std::string_view compressed) {
    return compressed.size();
  }

  static size_t decompressInto(std::string_view compressed,
                               ql::span<char> out) {
    AD_CONTRACT_CHECK(out.size() >= compressed.size());
    for (auto&& [dest, src] :
         ::ranges::views::zip(out.subspan(0, compressed.size()), compressed)) {
      dest = static_cast<char>(src - 2);
    }
    return compressed.size();
  }

  static std::string decompress(std::string_view compressed) {
    std::string result{compressed.size(), '\0'};
    decompressInto(compressed, ql::span<char>{result.data(), result.size()});
    return result;
  }
  // This class has no state, but it still needs to be serialized.
  template <typename T>
  friend std::true_type allowTrivialSerialization(DummyDecoder, T);
};

// A wrapper for the stateless dummy compression.
struct DummyCompressionWrapper
    : ad_utility::vocabulary::detail::DecoderMultiplexer<DummyDecoder> {
  using Base = ad_utility::vocabulary::detail::DecoderMultiplexer<DummyDecoder>;
  using Base::Base;

  static std::string compress(std::string_view uncompressed) {
    std::string result{uncompressed};
    for (auto& c : result) {
      c += 2;
    }
    return result;
  }

  static std::tuple<int, std::vector<std::string>, DummyDecoder> compressAll(
      const std::vector<std::string>& strings) {
    std::vector<std::string> result;
    for (const auto& string : strings) {
      result.push_back(compress(string));
    }
    return {0, std::move(result), DummyDecoder{}};
  }
};

// _______________________________________________________
TEST(CompressedVocabulary, CompressionIsActuallyApplied) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  CompressedVocabulary<VocabularyInMemory, DummyCompressionWrapper> vocab;
  {
    auto writerPtr = vocab.makeDiskWriterPtr("vocabtmp.txt");
    auto& writer = *writerPtr;
    for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
      ASSERT_EQ(writer(word, false), static_cast<uint64_t>(i));
    }
    writer.readableName() = "blabb";
    EXPECT_EQ(writer.readableName(), "blabb");
    // Test the case that the destructor implicitly calls `finish`.
  }

  VocabularyInMemory simple;
  simple.open("vocabtmp.txt.words");
  ad_utility::deleteFile("vocabtmp.txt.words");

  ASSERT_EQ(simple.size(), words.size());
  for (size_t i = 0; i < simple.size(); ++i) {
    ASSERT_NE(simple[i], words[i]);
    ASSERT_EQ(DummyDecoder::decompress(simple[i]), words[i]);
  }
}

// The generic tests from the vocabulary testing framework, templated on all the
// compressors that we have defined.

// Add additional compression wrappers to the following type list. They will
// then automatically be tested.
using Compressors =
    ::testing::Types<FsstSquaredCompressionWrapper, FsstCompressionWrapper,
                     PrefixCompressionWrapper, DummyCompressionWrapper>;

// _____________________________________________________________________________
template <typename Compressor>
struct CompressedVocabularyF : public testing::Test {
  static_assert(ad_utility::vocabulary::CompressionWrapper<Compressor>);
  // Return a lambda that takes a vector of strings, builds a
  // CompressedVocabulary containing those strings, and returns that
  // vocabulary.
  static auto createCompressedVocabulary() {
    return [](const std::vector<std::string>& words,
              std::string filename = gtestCurrentTestName()) {
      ad_utility::deleteFile(filename, false);
      CompressedVocabulary<VocabularyOnDisk, Compressor, 4> vocab;
      auto writerPtr = vocab.makeDiskWriterPtr(filename);
      writeWordsAndFinish(*writerPtr, words);
      vocab.open(filename);
      return vocab;
    };
  }
};

TYPED_TEST_SUITE(CompressedVocabularyF, Compressors);

// _____________________________________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(this->createCompressedVocabulary());
}

// _____________________________________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      this->createCompressedVocabulary());
}

// _____________________________________________________________________________
TYPED_TEST(CompressedVocabularyF, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(this->createCompressedVocabulary());
}

// _____________________________________________________________________________
// `lookupBatch` must agree with the per-word `operator[]` for arbitrary index
// combinations: same words, same order as requested (duplicates included), with
// the returned batch owning the lifetime of all decompressed string views. An
// empty index list is a contract violation and must throw.
TYPED_TEST(CompressedVocabularyF, LookupBatchMatchesAccessOperator) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta",
                                       "epsilon"};
  auto vocab = this->createCompressedVocabulary()(words);
  const std::array<size_t, 7> indices{4, 1, 0, 3, 1, 2, 4};
  const auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  AD_EXPECT_THROW_WITH_MESSAGE(vocab.lookupBatch(ql::span<const size_t>{}),
                               ::testing::HasSubstr("!indices.empty()"));
}

// _____________________________________________________________________________
// Verify that a vocabulary containing the empty string word ("") decompresses
// correctly through `lookupBatch` without allocations or crashes across all
// compressors (exercising the `boundOnDecompressedWordSize == 0` fast path).
TYPED_TEST(CompressedVocabularyF, LookupBatchEmptyWordInVocabulary) {
  const std::vector<std::string> words{"alpha", "", "beta", "", "gamma"};
  auto vocab = this->createCompressedVocabulary()(words);
  const std::array<size_t, 6> indices{1, 0, 3, 2, 4, 1};
  const auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  EXPECT_TRUE(result[0].empty());
  EXPECT_EQ(result[1], "alpha");
  EXPECT_TRUE(result[2].empty());
  EXPECT_EQ(result[3], "beta");
  EXPECT_EQ(result[4], "gamma");
  EXPECT_TRUE(result[5].empty());
}

// _____________________________________________________________________________
// Verify the generic framework's empty-vocabulary contract: lookups, iteration,
// and size must all behave on a vocabulary with zero words.
TYPED_TEST(CompressedVocabularyF, EmptyVocabulary) {
  testEmptyVocabulary(this->createCompressedVocabulary());
}

// _____________________________________________________________________________
// Serialization round trip: write the compressed vocabulary to disk with the
// serializer, read it back, and verify every word survives identically.
TYPED_TEST(CompressedVocabularyF, WriteAndReadWithSerializer) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  // Create vocabulary with small block size (4 words per block).
  // Use VocabularyInMemory as the underlying vocabulary.
  CompressedVocabulary<VocabularyInMemory, TypeParam, 4> vocab;
  std::string filename = gtestCurrentTestName();
  auto writerPtr = vocab.makeDiskWriterPtr(filename);
  auto& writer = *writerPtr;
  for (const auto& word : words) {
    writer(word, false);
  }
  writer.finish();
  vocab.open(filename);

  // Write using serializer.
  ad_utility::serialization::ByteBufferWriteSerializer writeSerializer;
  writeSerializer | vocab;
  const auto& blob = writeSerializer.data();
  ASSERT_FALSE(blob.empty());

  // Read using serializer into a different vocabulary.
  CompressedVocabulary<VocabularyInMemory, TypeParam, 4> readVocab;
  ad_utility::serialization::ByteBufferReadSerializer readSerializer{blob};
  readSerializer | readVocab;
  assertThatRangesAreEqual(vocab, readVocab);

  // Cleanup files.
  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
// Zero-copy deserialization: opening a vocabulary serialized by this same
// process must map/reference the existing buffers instead of decompressing
// everything anew, and lookups must still return the correct words.
TYPED_TEST(CompressedVocabularyF, ZeroCopyDeserialization) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  // Create vocabulary with small block size (4 words per block) on top of an
  // in-memory (and hence zero-copy-capable) underlying vocabulary.
  CompressedVocabulary<VocabularyInMemory, TypeParam, 4> vocab;
  std::string filename = gtestCurrentTestName();
  auto writerPtr = vocab.makeDiskWriterPtr(filename);
  auto& writer = *writerPtr;
  for (const auto& word : words) {
    writer(word, false);
  }
  writer.finish();
  vocab.open(filename);

  // Write using an aligned serializer (required for zero-copy reads).
  ad_utility::serialization::AlignedByteBufferWriteSerializer writeSerializer;
  writeSerializer | vocab;

  // Read back the words as a non-owning, zero-copy view, and the (small)
  // decoders normally.
  ad_utility::serialization::AlignedByteBufferReadSerializer readSerializer{
      std::move(writeSerializer).data()};
  auto view =
      (CompressedVocabulary<VocabularyInMemory, TypeParam,
                            4>::fromZeroCopyDeserializer(readSerializer));
  assertThatRangesAreEqual(vocab, view);

  ad_utility::deleteFile(filename);
}

}  // namespace

// _____________________________________________________________________________
// `scanAll` must yield every word in index order, spanning multiple decoder
// blocks (the fixture's small block size forces many blocks for 111 words).
TYPED_TEST(CompressedVocabularyF, ScanAll) {
  auto createVocab = TestFixture::createCompressedVocabulary();
  std::vector<std::string> words;
  for (size_t i = 0; i < 111; ++i) {
    words.push_back(absl::StrCat("someWord", i, std::string(i % 13, 'y')));
  }
  // NOTE: The fixture uses a decoder block size of 4, so this vocabulary has
  // many decoder blocks that the scan has to span.
  auto vocab = createVocab(words);

  using ::testing::ElementsAreArray;
  EXPECT_THAT(scanAllToVector(vocab.scanAll()), ElementsAreArray(words));
  // Abandon a scan early; the destructor has to clean up properly.
  {
    auto range = vocab.scanAll();
    auto it = ql::ranges::begin(range);
    ASSERT_NE(it, ql::ranges::end(range));
    IndexAndWord indexAndWord = *it;
    EXPECT_EQ(indexAndWord.index_, 0);
    EXPECT_EQ(indexAndWord.word_, words.at(0));
  }
}

// _____________________________________________________________________________
// Regression test for a dangling-view bug this lookup path once had: an
// intermediate local `std::pmr::string` uses the small-string optimization
// regardless of its allocator, so for short words a saved `string_view`
// pointed into the destroyed local object instead of the arena.
//
// Detection strength:
//  - Under AddressSanitizer builds (CMAKE_BUILD_TYPE=Asan), reading through a
//    dangling view is expected to produce a stack-use-after-return diagnostic.
//  - In normal builds it is a practical tripwire, not a proof: reading a
//    dangling view is UB, so we clobber the stack with sentinel bytes and
//    verify content byte-for-byte, which makes corruption likely but not
//    formally guaranteed.
TYPED_TEST(CompressedVocabularyF, LookupBatchShortWordViewsStayValid) {
  // Verify that this platform uses inline storage for `std::pmr::string`;
  // short words therefore use the Small String Optimization (SSO) and would
  // otherwise end up inside a destroyed stack object rather than the arena.
  requirePmrStringInlineStorage(15);

  // All words deliberately short (<= 15 chars): every one takes the SSO
  // path in a `pmr::string`-based implementation, and none would end up in
  // the monotonic buffer that owns the result's storage.
  std::vector<std::string> words;
  words.reserve(64);
  for (int i = 0; i < 64; ++i) {
    words.push_back(absl::StrCat("s", i));
  }
  // Enforce the test premise: all words must fit within standard library SSO
  // capacity (<= 15 bytes on 64-bit platforms).
  ASSERT_TRUE(ql::ranges::all_of(
      words, [](const auto& word) { return word.size() <= 15; }));
  auto vocab = this->createCompressedVocabulary()(words);

  const auto indices =
      ::ranges::to<std::vector>(ql::views::iota(size_t{0}, words.size()));
  const auto result = vocab.lookupBatch(indices);
  ASSERT_EQ(result.size(), indices.size());

  // Clobber the stack region a dangling SSO view would point into. Two deep
  // frames of sentinel bytes leave no plausible intact copy behind.
  auto churn = []() { clobberStack(); };
  churn();
  churn();

  for (size_t i = 0; i < indices.size(); ++i) {
    ASSERT_EQ(result[i], words[i]);
  }
}

// _____________________________________________________________________________
// `scanAll` on an empty vocabulary must yield an empty (but valid) range,
// not an error or a dangling iterator.
TYPED_TEST(CompressedVocabularyF, ScanAllEmptyVocabulary) {
  auto createVocab = TestFixture::createCompressedVocabulary();
  auto vocab = createVocab({});
  auto range = vocab.scanAll();
  EXPECT_EQ(ql::ranges::begin(range), ql::ranges::end(range));
}

// _____________________________________________________________________________
// A vocabulary containing the empty string word ("") must be scanned correctly
// across all compressors, exercising the `maxDecompressedSize == 0` fast path
// in `scanAll`'s buffered decode. The test also checks that empty yielded views
// have non-null data pointers.
TYPED_TEST(CompressedVocabularyF, ScanAllEmptyWordInVocabulary) {
  auto createVocab = TestFixture::createCompressedVocabulary();
  const std::vector<std::string> words{"alpha", "", "beta", "", "gamma"};
  auto vocab = createVocab(words);
  std::vector<std::string> scannedWords;
  for (const IndexAndWord& entry : vocab.scanAll()) {
    if (entry.word_.empty()) {
      EXPECT_NE(entry.word_.data(), nullptr);
    }
    scannedWords.emplace_back(entry.word_);
  }
  using ::testing::ElementsAreArray;
  EXPECT_THAT(scannedWords, ElementsAreArray(words));
}

// _____________________________________________________________________________
// Test the documented reuse semantics of `scanAll`: each yielded
// `string_view` points into storage that is reused for the next element, so a
// previously yielded view must no longer represent the old word after the next
// element has been pulled. The pointer is expected to remain stable while the
// range is alive, while its contents are overwritten.
TYPED_TEST(CompressedVocabularyF, ScanAllViewInvalidAfterNextPull) {
  auto createVocab = TestFixture::createCompressedVocabulary();
  // The first word is longer than the second so the decode buffer allocated on
  // the first pull is guaranteed to have enough capacity for the second word
  // without reallocating, ensuring the buffer pointer remains stable and
  // well-defined while its contents are overwritten.
  const std::vector<std::string> words{"firstMuchLongerWordPreallocatingBuffer",
                                       "secondShort"};
  auto vocab = createVocab(words);

  auto range = vocab.scanAll();
  auto it = ql::ranges::begin(range);
  ASSERT_NE(it, ql::ranges::end(range));
  IndexAndWord first = *it;
  ASSERT_EQ(first.index_, 0u);
  ASSERT_EQ(first.word_, words[0]);
  const char* firstData = first.word_.data();

  ++it;
  ASSERT_NE(it, ql::ranges::end(range));
  IndexAndWord second = *it;
  ASSERT_EQ(second.index_, 1u);
  ASSERT_EQ(second.word_, words[1]);

  // The stale view must no longer represent the first word: the underlying
  // buffer was reused for the second decompression.
  EXPECT_EQ(first.word_.data(), firstData);
  EXPECT_NE(first.word_, words[0]);
}

// _____________________________________________________________________________
// `lookupBatch` with several decoder blocks: a small block size (2 words per
// block) forces multiple decoders, exercising the per-request decoder
// selection. Covers a single-element batch, a batch with repeated indices,
// and a mixed batch crossing block boundaries; results must match
// `operator[]` exactly and appear in request order, with all views staying
// valid while the returned result object lives.
TYPED_TEST(CompressedVocabularyF, LookupBatchAcrossDecoderBlocks) {
  const std::vector<std::string> words{"alpha", "beta",  "gamma", "delta",
                                       "epsi",  "zeta",  "eta",   "theta",
                                       "iota",  "kappa", "",      "lambda"};

  const std::string filename =
      std::string{gtestCurrentTestName()} + "-blocks";
  ad_utility::deleteFile(filename, false);
  CompressedVocabulary<VocabularyInMemory, TypeParam, 2> vocab;
  {
    auto writerPtr = vocab.makeDiskWriterPtr(filename);
    writeWordsAndFinish(*writerPtr, words);
  }
  vocab.open(filename);
  ASSERT_EQ(vocab.size(), words.size());

  // Single-element batch: boundary case with exactly one requested index.
  const std::array<size_t, 1> singleIdx{7};
  auto single = vocab.lookupBatch(singleIdx);
  ASSERT_EQ(single.size(), 1u);
  EXPECT_EQ(single[0], "theta");

  // Multi-block batch containing repeated indices and the empty-string word:
  // every entry must match the per-word `operator[]`, in request order.
  const std::array<size_t, 8> indices{10, 0, 11, 5, 5, 2, 10, 9};
  const auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  EXPECT_TRUE(result[0].empty());
  EXPECT_EQ(result[6], "");

    // All views yielded by the result must stay intact while the result object
  // is alive, including after an unrelated allocation has occurred. This
  // exercises the result's ownership of the decompressed storage; the
  // comparison loop below verifies that the returned words remain unchanged.
  std::string unrelatedAllocation;
  unrelatedAllocation.append(64, 'x');
  ASSERT_EQ(unrelatedAllocation.size(), 64u);
  EXPECT_EQ(unrelatedAllocation, std::string(64, 'x'));
  for (size_t i = 0; i < indices.size(); ++i) {
    const size_t idx = indices[i];
    EXPECT_EQ(result[i], vocab[idx]) << "at vocabulary index " << idx;
  }

  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
TEST(DecoderMultiplexer, DirectDecompressIntoAndMaxDecompressedSize) {
  std::vector<DummyDecoder> decoders{DummyDecoder{}, DummyDecoder{}};
  ad_utility::vocabulary::detail::DecoderMultiplexer<DummyDecoder> mux{
      std::move(decoders)};
  ASSERT_EQ(mux.numDecoders(), 2u);

  const std::string compressed = DummyCompressionWrapper::compress("testword");
  const size_t bound = mux.maxDecompressedSize(compressed, 0);
  EXPECT_EQ(bound, compressed.size());

  std::string outputBuffer(bound, '\0');
  std::string scratch;
  const size_t written = mux.decompressInto(
      compressed, 0, ql::span<char>{outputBuffer.data(), outputBuffer.size()},
      scratch);
  EXPECT_EQ(written, 8u);
  EXPECT_EQ(std::string_view(outputBuffer.data(), written), "testword");
  EXPECT_EQ(mux.decompress(compressed, 0), "testword");

  // An undersized output buffer must be rejected by the underlying decoder's
  // contract check (`out.size() >= compressed.size()`).
  ql::span<char> undersized{outputBuffer.data(), bound - 1};
  AD_EXPECT_THROW_WITH_MESSAGE(
      static_cast<void>(mux.decompressInto(compressed, 0, undersized, scratch)),
      ::testing::HasSubstr("out.size() >= compressed.size()"));

  // Out-of-range decoder indices must be rejected for all dispatching
  // methods rather than silently reading out of bounds.
  const size_t invalidIndex = mux.numDecoders();
  EXPECT_THROW(
      static_cast<void>(mux.maxDecompressedSize(compressed, invalidIndex)),
      std::out_of_range);
  EXPECT_THROW(
      static_cast<void>(mux.decompressInto(
          compressed, invalidIndex,
          ql::span<char>{outputBuffer.data(), outputBuffer.size()}, scratch)),
      std::out_of_range);
  EXPECT_THROW(static_cast<void>(mux.decompress(compressed, invalidIndex)),
               std::out_of_range);
}
