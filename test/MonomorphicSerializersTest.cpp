// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <string>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "engine/FastExportStreamFormatter.h"
#include "engine/MonomorphicSerializers.h"
#include "util/OverloadCallOperator.h"

using namespace ql::serialization;
using namespace ql::export_formatting;

// Helper to serialize with string sink
template <typename Fn>
std::string captureOutput(Fn&& fn) {
  std::string out;
  auto sink = [&](std::string_view chunk) { out.append(chunk); };
  FastExportStreamFormatter formatter(sink);
  fn(formatter);
  std::move(formatter).finalize();
  return out;
}

TEST(MonomorphicSerializersTest, MonomorphicTripleCsvSerialization) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri,
                                              ColumnType::Literal>;

  std::string result = captureOutput([&](FastExportStreamFormatter& fmt) {
    Serializer::serializeRow<ExportFormat::Csv>(
        fmt, std::string_view{"<http://example.org/subj>"},
        std::string_view{"<http://example.org/pred>"},
        std::string_view{"\"Hello, World!\""});
  });

  EXPECT_EQ(result,
            "<http://example.org/subj>,<http://example.org/pred>,\"Hello, "
            "World!\"\n");
}

TEST(MonomorphicSerializersTest, MonomorphicTripleTurtleSerialization) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri,
                                              ColumnType::Literal>;

  std::string result = captureOutput([&](FastExportStreamFormatter& fmt) {
    Serializer::serializeRow<ExportFormat::Turtle>(
        fmt, std::string_view{"<http://example.org/s>"},
        std::string_view{"<http://example.org/p>"},
        std::string_view{"\"val\""});
  });

  EXPECT_EQ(result,
            "<http://example.org/s> <http://example.org/p> \"val\" .\n");
}

TEST(MonomorphicSerializersTest, MonomorphicMixedTypesTsvSerialization) {
  using Serializer =
      MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Literal,
                               ColumnType::Int, ColumnType::Double>;

  std::string result = captureOutput([&](FastExportStreamFormatter& fmt) {
    Serializer::serializeRow<ExportFormat::Tsv>(
        fmt, std::string_view{"<http://example.org/city>"},
        std::string_view{"\"Freiburg\""}, 230000, 153.07);
  });

  EXPECT_EQ(result,
            "<http://example.org/city>\t\"Freiburg\"\t230000\t153.07\n");
}

TEST(MonomorphicSerializersTest, MonomorphicSpanAndBatchSerialization) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri,
                                              ColumnType::Int>;

  std::vector<std::array<CellValue, 3>> rows = {
      {CellValue::makeIri("<http://a>"), CellValue::makeIri("<http://b>"),
       CellValue::makeInt(10)},
      {CellValue::makeIri("<http://c>"), CellValue::makeIri("<http://d>"),
       CellValue::makeInt(20)}};

  std::string result = captureOutput([&](FastExportStreamFormatter& fmt) {
    Serializer::serializeBatch<ExportFormat::Csv>(fmt, rows);
  });

  EXPECT_EQ(result, "<http://a>,<http://b>,10\n<http://c>,<http://d>,20\n");
}

TEST(MonomorphicSerializersTest, DynamicRowSerializerEquivalence) {
  const std::vector<ColumnType> schema = {ColumnType::Iri, ColumnType::Literal,
                                          ColumnType::Int};
  DynamicRowSerializer dynamicSerializer(schema);
  using Monomorphic =
      MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Literal,
                               ColumnType::Int>;

  std::array<CellValue, 3> row = {CellValue::makeIri("<http://example.org/x>"),
                                  CellValue::makeLiteral("\"test\""),
                                  CellValue::makeInt(42)};

  std::string dynamicOut = captureOutput([&](FastExportStreamFormatter& fmt) {
    dynamicSerializer.serializeRow<ExportFormat::Csv>(
        fmt, ql::span<const CellValue>(row));
  });

  std::string monomorphicOut =
      captureOutput([&](FastExportStreamFormatter& fmt) {
        Monomorphic::serializeRow<ExportFormat::Csv>(
            fmt, ql::span<const CellValue>(row));
      });

  EXPECT_EQ(dynamicOut, monomorphicOut);
  EXPECT_EQ(dynamicOut, "<http://example.org/x>,\"test\",42\n");
}

TEST(MonomorphicSerializersTest, FastPathTemplateDispatch) {
  const std::vector<ColumnType> schema = {ColumnType::Iri, ColumnType::Iri,
                                          ColumnType::Literal};
  std::array<CellValue, 3> row = {CellValue::makeIri("<http://s>"),
                                  CellValue::makeIri("<http://p>"),
                                  CellValue::makeLiteral("\"o\"")};

  std::string dispatchedOut =
      captureOutput([&](FastExportStreamFormatter& fmt) {
        dispatchMonomorphicSerializer(
            schema, ad_utility::OverloadCallOperator{
                        [&](DynamicRowSerializer& s) {
                          s.template serializeRow<ExportFormat::Turtle>(
                              fmt, ql::span<const CellValue>(row));
                        },
                        [&]<ColumnType... Types>() {
                          using S = MonomorphicRowSerializer<Types...>;
                          S::template serializeRow<ExportFormat::Turtle>(
                              fmt, ql::span<const CellValue>(row));
                        }});
      });

  EXPECT_EQ(dispatchedOut, "<http://s> <http://p> \"o\" .\n");
}

// _____________________________________________________________________________
// SWAR delimiter wiring: `writeFieldSeparator` / `writeTripleEnd` emit the
// same bytes as the previous scalar stores for every export format.
TEST(MonomorphicSerializersTest, SwarFieldSeparatorAndTripleEnd) {
  const auto check = [](ExportFormat format, std::string_view separator,
                        std::string_view terminator) {
    std::string sep = captureOutput([&](FastExportStreamFormatter& fmt) {
      fmt.writeFieldSeparator(format);
    });
    EXPECT_EQ(sep, separator);
    std::string end = captureOutput(
        [&](FastExportStreamFormatter& fmt) { fmt.writeTripleEnd(format); });
    EXPECT_EQ(end, terminator);
  };
  check(ExportFormat::Turtle, " ", " .\n");
  check(ExportFormat::NTriples, " ", " .\n");
  check(ExportFormat::Csv, ",", "\n");
  check(ExportFormat::Tsv, "\t", "\n");
}

// _____________________________________________________________________________
// SWAR delimiter wiring: `writePacked` matches the equivalent scalar writes,
// including an exact-fit fixed-span buffer where no 8-byte SWAR store fits.
TEST(MonomorphicSerializersTest, SwarWritePackedMatchesScalar) {
  using ad_utility::SwarDelimiterPacker;
  // Streaming mode over all predefined packed delimiters.
  const std::string packed = captureOutput([&](FastExportStreamFormatter& fmt) {
    fmt.writePacked(SwarDelimiterPacker::TRIPLE_S_TO_P_IRI);
    fmt.writePacked(SwarDelimiterPacker::TRIPLE_P_TO_O_LIT);
    fmt.writePacked(SwarDelimiterPacker::TRIPLE_O_LIT_END);
    fmt.writePacked(SwarDelimiterPacker::DELIM_CSV_QUOTE_COMMA_QUOTE);
    fmt.writePacked(SwarDelimiterPacker::DELIM_TSV_TAB_NEWLINE);
    fmt.writePacked(ad_utility::PackedDelimiter{});
  });
  EXPECT_EQ(packed, "> <> \"\" .\n\",\"\t\n");

  // Exact-fit fixed-span buffer: scalar tail fallback, no overflow.
  std::array<char, 4> exactFit{};
  FastExportStreamFormatter fmt{
      ql::span<char>{exactFit.data(), exactFit.size()}};
  fmt.writePacked(SwarDelimiterPacker::TRIPLE_O_IRI_END);
  EXPECT_EQ(fmt.currentChunk(), "> .\n");
}
