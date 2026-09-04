//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include "VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/Serializer/ByteBufferSerializer.h"

namespace {

using namespace vocabulary_test;
using namespace ad_utility::vocabulary;
// A stateless "compressor" that applies a trivial transformation to a string
struct DummyDecoder {
  static std::string decompress(std::string_view compressed) {
    std::string result{compressed};
    for (char& c : result) {
      c -= 2;
    }
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

  CompressedVocabulary<VocabularyInMemory, DummyCompressionWrapper> v;
  {
    auto writerPtr = v.makeDiskWriterPtr("vocabtmp.txt");
    auto& writer = *writerPtr;
    for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
      ASSERT_EQ(writer(word, false), static_cast<uint64_t>(i));
    }
    writer.readableName() = "blabb";
    EXPECT_EQ(writer.readableName(), "blabb");
    // Test the case that the destructor implicitly calls `finish`.
    // The other unit tests have
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

// _________________________________________________________________________
template <typename Compressor>
struct CompressedVocabularyF : public testing::Test {
  static_assert(ad_utility::vocabulary::CompressionWrapper<Compressor>);
  // Tests for the FSST-compressed vocabulary. These use the generic testing
  // framework that was set up for all the other vocabularies.
  static auto createCompressedVocabulary() {
    std::string filename = gtestCurrentTestName();
    return [filename =
                std::move(filename)](const std::vector<std::string>& words) {
      // We deliberately set the blocksize to a very small number.
      CompressedVocabulary<VocabularyOnDisk, Compressor, 4> vocab;
      auto writerPtr = vocab.makeDiskWriterPtr(filename);
      auto& writer = *writerPtr;
      for (const auto& word : words) {
        writer(word, false);
      }
      writer.finish();
      vocab.open(filename);
      return vocab;
    };
  }
};
TYPED_TEST_SUITE(CompressedVocabularyF, Compressors);

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(this->createCompressedVocabulary());
}

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(
      this->createCompressedVocabulary());
}

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(this->createCompressedVocabulary());
}

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, LookupBatchMatchesAccessOperator) {
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta",
                                       "epsilon"};
  auto vocab = this->createCompressedVocabulary()(words);
  const std::array<size_t, 7> indices{4, 1, 0, 3, 1, 2, 4};
  auto result = vocab.lookupBatch(indices);
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  EXPECT_ANY_THROW(vocab.lookupBatch(ql::span<const size_t>{}));
}

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, LookupBatchVariousIndexSequences) {
  // Block size is 4 (set in createCompressedVocabulary), so we use words that
  // span multiple decoder blocks to test correct decompression across block
  // boundaries.
  const std::vector<std::string> words{
      "word0", "word1", "word2", "word3",  // block 0
      "word4", "word5", "word6", "word7",  // block 1
      "word8", "word9"                     // block 2 (partial)
  };
  auto vocab = this->createCompressedVocabulary()(words);

  // Empty indices (std::vector, not span).
  {
    std::vector<size_t> emptyIndices;
    auto result = vocab.lookupBatch(emptyIndices);
    EXPECT_TRUE(result.empty());
  }

  // Single index.
  {
    auto result = vocab.lookupBatch(std::vector<size_t>{5});
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], words[5]);
  }

  // Multiple indices in order.
  {
    auto result = vocab.lookupBatch(std::vector<size_t>{0, 2, 4, 6});
    ASSERT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], words[0]);
    EXPECT_EQ(result[1], words[2]);
    EXPECT_EQ(result[2], words[4]);
    EXPECT_EQ(result[3], words[6]);
  }

  // Multiple indices out of order.
  {
    auto result = vocab.lookupBatch(std::vector<size_t>{7, 2, 9, 0});
    ASSERT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], words[7]);
    EXPECT_EQ(result[1], words[2]);
    EXPECT_EQ(result[2], words[9]);
    EXPECT_EQ(result[3], words[0]);
  }

  // Duplicate indices.
  {
    auto result = vocab.lookupBatch(std::vector<size_t>{1, 1, 5, 5, 1});
    ASSERT_EQ(result.size(), 5);
    EXPECT_EQ(result[0], words[1]);
    EXPECT_EQ(result[1], words[1]);
    EXPECT_EQ(result[2], words[5]);
    EXPECT_EQ(result[3], words[5]);
    EXPECT_EQ(result[4], words[1]);
  }

  // Indices spanning multiple decoder blocks (block size = 4).
  {
    // Covers block 0 (indices 0-3), block 1 (indices 4-7), block 2 (indices 8-9).
    auto result = vocab.lookupBatch(std::vector<size_t>{0, 3, 4, 7, 8, 9});
    ASSERT_EQ(result.size(), 6);
    EXPECT_EQ(result[0], words[0]);
    EXPECT_EQ(result[1], words[3]);
    EXPECT_EQ(result[2], words[4]);
    EXPECT_EQ(result[3], words[7]);
    EXPECT_EQ(result[4], words[8]);
    EXPECT_EQ(result[5], words[9]);
  }

  // All indices in reverse order.
  {
    std::vector<size_t> reverseIndices;
    for (size_t i = words.size(); i > 0; --i) {
      reverseIndices.push_back(i-1);
    }
    auto result = vocab.lookupBatch(reverseIndices);
    ASSERT_EQ(result.size(), words.size());
    for (size_t i = 0; i < words.size(); ++i) {
      EXPECT_EQ(result[i], words[words.size() - 1 - i]);
    }
  }
}

// _______________________________________________________
TYPED_TEST(CompressedVocabularyF, EmptyVocabulary) {
  testEmptyVocabulary(this->createCompressedVocabulary());
}

// _______________________________________________________
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

// _______________________________________________________
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
TYPED_TEST(CompressedVocabularyF, ScanAllEmptyVocabulary) {
  auto createVocab = TestFixture::createCompressedVocabulary();
  auto vocab = createVocab({});
  auto range = vocab.scanAll();
  EXPECT_EQ(ql::ranges::begin(range), ql::ranges::end(range));
}
