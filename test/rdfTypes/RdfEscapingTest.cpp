// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2022 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "rdfTypes/RdfEscaping.h"
using namespace RdfEscaping;

// ___________________________________________________________________________
TEST(RdfEscapingTest, hexadecimalCharactersToUtf8Codepoint) {
  using detail::hexadecimalCharactersToUtf8Codepoint;
  // Ordinary cases: one-, two-, three- and four-byte codepoints. The expected
  // values use the C++ `\u`/`\U` escapes matching the hexadecimal input (except
  // for `0041`, since `A` would be an ill-formed universal character name).
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("0041"), "A");
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("00e4"), "\u00e4");
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("2702"), "\u2702");
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("0001F600"), "\U0001F600");
  // Corner cases: shorter and full-length (8 hex digits) inputs are accepted.
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("41"), "A");
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("1F600"), "\U0001F600");
  // An input longer than a single codepoint (more than 8 hex digits) violates
  // the contract check.
  AD_EXPECT_THROW_WITH_MESSAGE(
      hexadecimalCharactersToUtf8Codepoint("000000000"),
      ::testing::HasSubstr("Assertion `hex.size() <= 8` failed"));
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, escapeForCsv) {
  ASSERT_EQ(escapeForCsv("abc"), "abc");
  ASSERT_EQ(escapeForCsv("a\nb\rc,d"), "\"a\nb\rc,d\"");
  ASSERT_EQ(escapeForCsv("\""), "\"\"\"\"");
  ASSERT_EQ(escapeForCsv("a\"b"), "\"a\"\"b\"");
  ASSERT_EQ(escapeForCsv("a\"\"c"), "\"a\"\"\"\"c\"");
  ASSERT_EQ(escapeForCsv(","), "\",\"");
  ASSERT_EQ(escapeForCsv("\r"), "\"\r\"");
  ASSERT_EQ(escapeForCsv("\"\"\""), "\"\"\"\"\"\"\"\"");
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, escapeForTsv) {
  ASSERT_EQ(escapeForTsv("abc"), "abc");
  ASSERT_EQ(escapeForTsv("a\nb\tc"), "a\\nb c");
  ASSERT_EQ(escapeForTsv("a\rb"), "a\rb");
  ASSERT_EQ(escapeForTsv("\n\n\n"), "\\n\\n\\n");
}

namespace {
// Return the result of `appendFunction` applied to a buffer that already
// contains `prefix:`, so that each check also verifies that the existing
// content of the buffer is kept.
std::string appendToPrefix(void (*appendFunction)(std::string&,
                                                  std::string_view),
                           std::string_view input) {
  std::string out{"prefix:"};
  appendFunction(out, input);
  return out;
}
}  // namespace

// ___________________________________________________________________________
TEST(RdfEscapingTest, appendEscapedForCsv) {
  const auto csv = [](std::string_view input) {
    return appendToPrefix(appendEscapedForCsv, input);
  };
  // Fields without a special character are appended unchanged.
  EXPECT_EQ(csv(""), "prefix:");
  EXPECT_EQ(csv("abc"), "prefix:abc");
  EXPECT_EQ(csv("a\tb"), "prefix:a\tb");
  // Each of `,`, `\r`, `\n` and `"` on its own triggers the quoting.
  EXPECT_EQ(csv(","), "prefix:\",\"");
  EXPECT_EQ(csv("a\rb"), "prefix:\"a\rb\"");
  EXPECT_EQ(csv("a\nb"), "prefix:\"a\nb\"");
  EXPECT_EQ(csv("a\nb\rc,d"), "prefix:\"a\nb\rc,d\"");
  // Quotes are doubled, including adjacent quotes and quotes at the borders.
  EXPECT_EQ(csv("\""), "prefix:\"\"\"\"");
  EXPECT_EQ(csv("a\"\"c"), "prefix:\"a\"\"\"\"c\"");
  EXPECT_EQ(csv("\"a\""), "prefix:\"\"\"a\"\"\"");

  // Consecutive calls append to the same buffer.
  std::string out;
  appendEscapedForCsv(out, "a,b");
  appendEscapedForCsv(out, "c");
  EXPECT_EQ(out, "\"a,b\"c");

  // An `input` that points into `out` violates the contract.
  AD_EXPECT_THROW_WITH_MESSAGE(appendEscapedForCsv(out, out),
                               ::testing::HasSubstr("must not point into"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      appendEscapedForCsv(out, std::string_view{out}.substr(3)),
      ::testing::HasSubstr("must not point into"));
  EXPECT_EQ(out, "\"a,b\"c");
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, appendEscapedForTsv) {
  const auto tsv = [](std::string_view input) {
    return appendToPrefix(appendEscapedForTsv, input);
  };
  // Fields without a tab or newline are appended unchanged, in particular
  // `\r`, `,` and `"`, which are only special in CSV.
  EXPECT_EQ(tsv(""), "prefix:");
  EXPECT_EQ(tsv("abc"), "prefix:abc");
  EXPECT_EQ(tsv("a\rb,\""), "prefix:a\rb,\"");
  // Tabs become spaces and newlines become the two characters `\` and `n`.
  EXPECT_EQ(tsv("\t"), "prefix: ");
  EXPECT_EQ(tsv("\n"), "prefix:\\n");
  EXPECT_EQ(tsv("a\nb\tc"), "prefix:a\\nb c");
  EXPECT_EQ(tsv("\t\n"), "prefix: \\n");

  // Consecutive calls append to the same buffer.
  std::string out;
  appendEscapedForTsv(out, "a\tb");
  appendEscapedForTsv(out, "c");
  EXPECT_EQ(out, "a bc");

  // An `input` that points into `out` violates the contract.
  AD_EXPECT_THROW_WITH_MESSAGE(appendEscapedForTsv(out, out),
                               ::testing::HasSubstr("must not point into"));
  EXPECT_EQ(out, "a bc");
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, validRDFLiteralFromNormalized) {
  ASSERT_EQ(validRDFLiteralFromNormalized(R"(""\a\"")"), R"("\"\\a\\\"")");
  ASSERT_EQ(validRDFLiteralFromNormalized(R"("\b\"@en)"), R"("\\b\\"@en)");
  ASSERT_EQ(validRDFLiteralFromNormalized(R"("\c""^^<s>)"), R"("\\c\""^^<s>)");
  ASSERT_EQ(validRDFLiteralFromNormalized("\"\nhi\r\\\""), R"("\nhi\r\\")");
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizedContentFromLiteralOrIri) {
  auto f = [](std::string_view s) {
    return normalizedContentFromLiteralOrIri(std::string{s});
  };
  ASSERT_EQ(f("<bladiblu>"), "bladiblu");
  ASSERT_EQ(f("\"bladibla\""), "bladibla");
  ASSERT_EQ(f("\"bimm\"@en"), "bimm");
  ASSERT_EQ(f("\"bumm\"^^<http://www.mycustomiris.com/sometype>"), "bumm");
}

TEST(RdfEscapingTest, invalidEscapeThrows) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      normalizeRDFLiteral("\"invalid\\Escape\""),
      ::testing::HasSubstr("Unsupported escape sequence"));
}
// ___________________________________________________________________________
TEST(RdfEscapingTest, escapeForXml) {
  ASSERT_EQ(escapeForXml("abc\n\t;"), "abc\n\t;");
  ASSERT_EQ(escapeForXml("a&b\"'c<d>"), "a&amp;b&quot;&apos;c&lt;d&gt;");
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizeLiteralWithQuotesToNormalizedString) {
  ASSERT_EQ(
      "Hello \" \\World",
      asStringViewUnsafe(normalizeLiteralWithQuotes(R"("Hello \" \\World")")));
  ASSERT_THROW(normalizeLiteralWithQuotes("no quotes"), ad_utility::Exception);
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizeLiteralWithoutQuotesToNormalizedString) {
  ASSERT_EQ(
      "Hello \" \\World",
      asStringViewUnsafe(normalizeLiteralWithoutQuotes(R"(Hello \" \\World)")));
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizeIriWithBracketsToNormalizedString) {
  ASSERT_EQ("https://example.org/books/book1",
            asStringViewUnsafe(
                normalizeIriWithBrackets("<https://example.org/books/book1>")));
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizeIriWithoutBracketsToNormalizedString) {
  ASSERT_EQ("https://example.org/books/book1",
            asStringViewUnsafe(normalizeIriWithoutBrackets(
                "https://example.org/books/book1")));
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizeLanguageTagToNormalizedString) {
  ASSERT_EQ("se", asStringViewUnsafe(normalizeLanguageTag("@se")));
  ASSERT_EQ("se", asStringViewUnsafe(normalizeLanguageTag("se")));
}
