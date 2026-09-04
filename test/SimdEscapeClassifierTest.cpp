// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>

#include "engine/export_v2/SimdEscapeClassifier.h"

namespace {

using ql::engine::export_v2::EscapeFormat;
using ql::engine::export_v2::SimdEscapeClassifier;

template <EscapeFormat Format>
void expectScalarAndSimdAgree(char escape) {
  for (const size_t length : {1, 15, 16, 31, 32, 33, 63, 64, 65, 250}) {
    std::string input(length, 'a');
    EXPECT_EQ(SimdEscapeClassifier::findFirstEscapeScalar<Format>(input),
              std::string_view::npos);
    EXPECT_EQ(SimdEscapeClassifier::findFirstEscapeSimd<Format>(input),
              std::string_view::npos);
    for (size_t position = 0; position < length; ++position) {
      input[position] = escape;
      EXPECT_EQ(SimdEscapeClassifier::findFirstEscapeScalar<Format>(input),
                position);
      EXPECT_EQ(SimdEscapeClassifier::findFirstEscapeSimd<Format>(input),
                position);
      input[position] = 'a';
    }
  }
}

TEST(SimdEscapeClassifierTest, ScalarAndSimdAgreeAtBoundaries) {
  expectScalarAndSimdAgree<EscapeFormat::Csv>(',');
  expectScalarAndSimdAgree<EscapeFormat::Tsv>('\t');
  expectScalarAndSimdAgree<EscapeFormat::Turtle>('"');

  EXPECT_EQ(SimdEscapeClassifier::findFirstEscapeScalar<EscapeFormat::Csv>(""),
            std::string_view::npos);
  EXPECT_EQ(SimdEscapeClassifier::findFirstEscapeSimd<EscapeFormat::Csv>(""),
            std::string_view::npos);
}

TEST(SimdEscapeClassifierTest, ClassifiesEveryPositionInAChunk) {
  std::string input(32, 'a');
  EXPECT_EQ(SimdEscapeClassifier::classify32<EscapeFormat::Turtle>(
                {input.data(), input.size()})
                .raw(),
            0);
  for (size_t position = 0; position < input.size(); ++position) {
    input[position] = '\\';
    const auto mask = SimdEscapeClassifier::classify32<EscapeFormat::Turtle>(
        {input.data(), input.size()});
    EXPECT_EQ(mask.raw(), uint32_t{1} << position);
    EXPECT_EQ(mask.firstEscape(), position);
    EXPECT_EQ(mask.count(), 1);
    input[position] = 'a';
  }
}

template <EscapeFormat Format>
std::string escaped(std::string_view input) {
  std::string output(input.size() * 2, '\0');
  const auto written = SimdEscapeClassifier::copyAndEscape<Format>(
      input, ql::span<char>{output.data(), output.size()});
  output.resize(written.size());
  return output;
}

TEST(SimdEscapeClassifierTest, CopiesAndEscapesAcrossChunkBoundaries) {
  std::string turtle(65, 'a');
  turtle[0] = '"';
  turtle[31] = '\\';
  turtle[32] = '\n';
  turtle[64] = '\r';
  std::string expected =
      "\\\"" + std::string(30, 'a') + R"(\\\n)" + std::string(31, 'a') + "\\r";
  EXPECT_EQ(escaped<EscapeFormat::Turtle>(turtle), expected);

  EXPECT_EQ(escaped<EscapeFormat::Csv>("a,\"b\n"), "a,\"\"b\n");
  EXPECT_EQ(escaped<EscapeFormat::Tsv>("a\tb\nc\\d\r"), "a b\\nc\\\\d\\r");
  EXPECT_EQ(escaped<EscapeFormat::Tsv>(""), "");
}

TEST(SimdEscapeClassifierTest, RecognizesOnlyFormatSpecificCharacters) {
  static_assert(
      SimdEscapeClassifier::isEscapeCharacter<EscapeFormat::Csv>(','));
  static_assert(
      !SimdEscapeClassifier::isEscapeCharacter<EscapeFormat::Turtle>(','));
  static_assert(
      SimdEscapeClassifier::isEscapeCharacter<EscapeFormat::Tsv>('\t'));
  static_assert(
      !SimdEscapeClassifier::isEscapeCharacter<EscapeFormat::Csv>('\t'));
}

}  // namespace
