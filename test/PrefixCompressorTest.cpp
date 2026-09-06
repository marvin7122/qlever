// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <string_view>

#include "backports/span.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/PrefixHeuristic.h"
#include "util/GTestHelpers.h"
#include "util/Views.h"

TEST(PrefixCompressor, CompressionPreservesWords) {
  PrefixCompressor p;
  p.buildCodebook(std::vector<std::string>{"alph", "alpha", "al"});

  std::vector<std::string> words{
      "a",     "al",       "alp",     "alph",
      "alpha", "alphabet", "betabet", std::string{0, 0, 'a', 1}};

  for (const auto& word : words) {
    ASSERT_NE(p.compress(word), word);
    ASSERT_EQ(p.decompress(p.compress(word)), word);
  }
}

TEST(PrefixCompressor, OverlappingPrefixes) {
  PrefixCompressor p;
  p.buildCodebook(std::vector<std::string>{"alph", "alpha", "al"});

  // 1 byte for prefix "alpha" + 3 bytes for "bet".
  ASSERT_EQ(p.compress("alphabet").size(), 4u);

  // The encoding is one byte longer because of the "no prefix" code.
  std::string_view s = "nothing";
  ASSERT_EQ(p.compress(s).size(), s.size() + 1);

  // Matches the shorter prefix "al".
  ASSERT_EQ(p.compress("alfa").size(), 3u);

  // Matches no prefix, but is a prefix of some of the prefixes.
  ASSERT_EQ(p.compress("a").size(), 2u);
}

TEST(PrefixCompressor, TooManyPrefixesThrow) {
  PrefixCompressor p;
  std::vector<std::string> tooManyPrefixes;
  for (size_t i = 0; i < NUM_COMPRESSION_PREFIXES + 1; ++i) {
    tooManyPrefixes.push_back(std::to_string(i));
  }
  ASSERT_THROW(p.buildCodebook(tooManyPrefixes), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(PrefixCompressor, DecompressIntoMatchesDecompress) {
  PrefixCompressor p;
  p.buildCodebook(std::vector<std::string>{"alph", "alpha", "al"});
  auto checkWord = [&](std::string_view word) {
    const std::string compressed = p.compress(word);
    const std::string viaString = p.decompress(compressed);
    std::string intoBuf(p.maxDecompressedSize(compressed), '\0');
    const size_t n = p.decompressInto(
        compressed, ql::span<char>{intoBuf.data(), intoBuf.size()});
    EXPECT_EQ(n, viaString.size());
    EXPECT_EQ(std::string_view(intoBuf.data(), n), viaString);
    EXPECT_EQ(viaString, word);
  };
  for (std::string_view word :
       {"a", "al", "alp", "alph", "alpha", "alphabet", "nothing"}) {
    checkWord(word);
  }
  const std::string onlyPrefix = p.compress("alpha");
  ASSERT_EQ(onlyPrefix.size(), 1u);
  checkWord("alpha");
  AD_EXPECT_THROW_WITH_MESSAGE(static_cast<void>(p.maxDecompressedSize("")),
                               ::testing::HasSubstr("!compressedWord.empty()"));

  // Ensure that decompressing into an undersized output buffer fails the contract check.
  const std::string compressed = p.compress("alphabet");
  std::string smallBuf(1, '\0');
  AD_EXPECT_THROW_WITH_MESSAGE(
      static_cast<void>(p.decompressInto(
          compressed, ql::span<char>{smallBuf.data(), smallBuf.size()})),
      ::testing::HasSubstr("out.size() >= maxDecompressedSize"));
}

// _____________________________________________________________________________
TEST(PrefixCompressor, PrefixIndexBoundaryMarkers) {
  PrefixCompressor p;
  p.buildCodebook(std::vector<std::string>{"alpha"});

  const std::string compressedAlpha = p.compress("alpha");
  const std::string compressedBeta = p.compress("beta");

  EXPECT_EQ(p.prefixIndex(compressedAlpha), 0u);
  EXPECT_FALSE(p.prefixIndex(compressedBeta).has_value());
  EXPECT_FALSE(p.prefixIndex(std::string(1, static_cast<char>(NO_PREFIX_CHAR))).has_value());
  EXPECT_FALSE(p.prefixIndex(std::string(1, '\0')).has_value());
  EXPECT_EQ(p.maxDecompressedSize(compressedAlpha), 5u);
  EXPECT_EQ(p.maxDecompressedSize(compressedBeta), 4u);

  std::string output(p.maxDecompressedSize(compressedAlpha), '\0');
  EXPECT_EQ(p.decompressInto(compressedAlpha,
                             ql::span<char>{output.data(), output.size()}),
            5u);
  EXPECT_EQ(output, "alpha");

}

TEST(PrefixCompressor, MaximumNumberOfPrefixes) {
  PrefixCompressor p;
  std::vector<std::string> maximalNumberOfPrefixes;
  for (size_t i = 0; i < NUM_COMPRESSION_PREFIXES; ++i) {
    maximalNumberOfPrefixes.push_back("aaaaa" + std::to_string(i));
  }

  p.buildCodebook(maximalNumberOfPrefixes);

  // Check that all prefixes are correctly found
  for (const auto& prefix : maximalNumberOfPrefixes) {
    auto comp = p.compress(prefix);
    ASSERT_EQ(comp.size(), 1u);
    ASSERT_EQ(prefix, p.decompress(comp));
  }
}

// _____________________________________________________________________________
TEST(PrefixCompressor, prefixCompression) {
  using namespace ::testing;

  EXPECT_THAT(calculatePrefixes({}, 1), UnorderedElementsAre());
  EXPECT_THAT(calculatePrefixes({"", "a", "ab", "abc"}, 1),
              UnorderedElementsAre("a"));
  EXPECT_THAT(calculatePrefixes({"", "a", "ab", "abc"}, 2),
              UnorderedElementsAre("a", "ab"));
  EXPECT_THAT(calculatePrefixes({"", "a", "ab", "abc", "abcd"}, 2),
              UnorderedElementsAre("a", "ab"));
  EXPECT_THAT(calculatePrefixes({"", "a", "ab", "abc", "abcd"}, 3),
              UnorderedElementsAre("a", "ab", "abc"));
  EXPECT_THAT(calculatePrefixes({"", "a", "ab", "abc", "abcd"}, 4),
              UnorderedElementsAre("", "a", "ab", "abc"));
  EXPECT_THAT(calculatePrefixes({"a", "b"}, 1), UnorderedElementsAre(""));
  EXPECT_THAT(calculatePrefixes({"a", "b"}, 2), UnorderedElementsAre("", ""));

  // Newlines handling
  std::vector<std::string> input;
  for (size_t i : ad_utility::integerRange<size_t>(200)) {
    input.push_back(absl::StrCat("\"\"\"\nabc\t\n34as\n\ndj", i, "\"\"\""));
  }

  // There must be at least one of the compression prefixes that compresses the
  // common structure of the literals.
  EXPECT_THAT(calculatePrefixes(input, 127),
              Contains(ContainsRegex("\nabc\t\n")));
}

// _____________________________________________________________________________
TEST(PrefixCompressor, PrefixIndexBoundaryValues) {
  // Test boundary values for prefixIndex method
  // MIN_COMPRESSION_PREFIX = 129
  // NUM_COMPRESSION_PREFIXES = 126
  // Valid range: [129, 255)
  // NO_PREFIX_CHAR = 255

  // Exactly MIN_COMPRESSION_PREFIX (129) -> should return index 0
  std::string word_min(1, static_cast<char>(MIN_COMPRESSION_PREFIX));
  word_min += "test";
  auto idx_min = PrefixCompressor::prefixIndex(word_min);
  ASSERT_TRUE(idx_min.has_value());
  EXPECT_EQ(*idx_min, 0u);

  // MIN_COMPRESSION_PREFIX + NUM_COMPRESSION_PREFIXES - 1 (129 + 126 - 1 = 254) -> should return index 125
  std::string word_max_valid(1, static_cast<char>(MIN_COMPRESSION_PREFIX + NUM_COMPRESSION_PREFIXES - 1));
  word_max_valid += "test";
  auto idx_max_valid = PrefixCompressor::prefixIndex(word_max_valid);
  ASSERT_TRUE(idx_max_valid.has_value());
  EXPECT_EQ(*idx_max_valid, NUM_COMPRESSION_PREFIXES - 1u);

  // MIN_COMPRESSION_PREFIX + NUM_COMPRESSION_PREFIXES (129 + 126 = 255) -> should return nullopt
  std::string word_no_prefix(1, static_cast<char>(MIN_COMPRESSION_PREFIX + NUM_COMPRESSION_PREFIXES));
  word_no_prefix += "test";
  auto idx_no_prefix = PrefixCompressor::prefixIndex(word_no_prefix);
  EXPECT_FALSE(idx_no_prefix.has_value());

  // Value below MIN_COMPRESSION_PREFIX (e.g., 0) -> should return nullopt
  std::string word_below(1, static_cast<char>(0));
  word_below += "test";
  auto idx_below = PrefixCompressor::prefixIndex(word_below);
  EXPECT_FALSE(idx_below.has_value());

  // Value 255 (NO_PREFIX_CHAR) -> should return nullopt
  std::string word_above(1, static_cast<char>(255));
  word_above += "test";
  auto idx_above = PrefixCompressor::prefixIndex(word_above);
  EXPECT_FALSE(idx_above.has_value());
}
