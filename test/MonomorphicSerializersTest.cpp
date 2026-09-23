// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "engine/FastExportStreamFormatter.h"
#include "engine/MonomorphicSerializers.h"

using namespace ql::serialization;
using namespace ql::export_formatting;

// Helper to serialize with string sink
template <typename Fn>
std::string captureOutput(Fn&& fn) {
  std::string out;
  auto sink = [&](std::string_view chunk) { out.append(chunk); };
  FastExportStreamFormatter formatter(sink);
  fn(formatter);
  static_cast<void>(std::move(formatter).finalize());
  return out;
}

TEST(MonomorphicSerializersTest, MonomorphicTripleCsvSerialization) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri,
                                              ColumnType::Literal>;

  std::string result = captureOutput([&](FastExportStreamFormatter& fmt) {
    Serializer::serializeRow<ExportFormat::Csv>(
        fmt, "<http://example.org/subj>", "<http://example.org/pred>",
        "\"Hello, World!\"");
  });

  // The literal contains a comma, so RFC 4180 requires quoting with doubled
  // delimiters (like `FastExportStreamFormatter::writeTerm` produces).
  EXPECT_EQ(result,
            "<http://example.org/subj>,<http://example.org/pred>,\"\"\"Hello, "
            "World!\"\"\"\n");
}

TEST(MonomorphicSerializersTest, MonomorphicTripleTurtleSerialization) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri,
                                              ColumnType::Literal>;

  std::string result = captureOutput([&](FastExportStreamFormatter& fmt) {
    Serializer::serializeRow<ExportFormat::Turtle>(
        fmt, "<http://example.org/s>", "<http://example.org/p>", "\"val\"");
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
        fmt, "<http://example.org/city>", "\"Freiburg\"", 230000, 153.07);
  });

  // PortableDoubleToChars yields the shortest round-trip spelling on every
  // platform, so a single expectation holds for Apple and non-Apple builds.
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
  EXPECT_EQ(dynamicOut, "<http://example.org/x>,\"\"\"test\"\"\",42\n");
}

TEST(MonomorphicSerializersTest, RareSchemaFallbackMatchesDynamic) {
  // {Double, Double, Double} has no monomorphic fast path and falls back
  // to DynamicRowSerializer; the outputs must agree.
  const std::vector<ColumnType> schema = {
      ColumnType::Double, ColumnType::Double, ColumnType::Double};
  std::array<CellValue, 3> row = {CellValue(1.5), CellValue(2.5),
                                  CellValue(3.5)};
  DynamicRowSerializer dynamicSerializer(schema);
  std::string dynamicOut = captureOutput([&](FastExportStreamFormatter& fmt) {
    dynamicSerializer.serializeRow<ExportFormat::Csv>(
        fmt, ql::span<const CellValue>(row));
  });
  std::string dispatchedOut =
      captureOutput([&](FastExportStreamFormatter& fmt) {
        dispatchMonomorphicSerializer(schema, [&](auto& serializer) {
          serializer.template serializeRow<ExportFormat::Csv>(
              fmt, ql::span<const CellValue>(row));
        });
      });
  EXPECT_EQ(dispatchedOut, dynamicOut);
}

TEST(MonomorphicSerializersTest, UndefinedKeepsTabularColumnsAligned) {
  std::array<CellValue, 2> row = {CellValue::makeIri("<http://s>"),
                                  CellValue()};
  std::string csv = captureOutput([&](FastExportStreamFormatter& fmt) {
    MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Undefined>::
        serializeRow<ExportFormat::Csv>(fmt, ql::span<const CellValue>(row));
  });
  EXPECT_EQ(csv, "<http://s>,UNDEF\n");
}

TEST(MonomorphicSerializersTest, FastPathTemplateDispatch) {
  const std::vector<ColumnType> schema = {ColumnType::Iri, ColumnType::Iri,
                                          ColumnType::Literal};
  std::array<CellValue, 3> row = {CellValue::makeIri("<http://s>"),
                                  CellValue::makeIri("<http://p>"),
                                  CellValue::makeLiteral("\"o\"")};

  std::string dispatchedOut =
      captureOutput([&](FastExportStreamFormatter& fmt) {
        dispatchMonomorphicSerializer(schema, [&](auto& serializer) {
          serializer.template serializeRow<ExportFormat::Turtle>(
              fmt, ql::span<const CellValue>(row));
        });
      });

  EXPECT_EQ(dispatchedOut, "<http://s> <http://p> \"o\" .\n");
}

TEST(MonomorphicSerializersTest, CsvLiteralMatchesProductionWriteTerm) {
  // Normalized literal with an embedded quote and a comma: CSV must quote
  // the complete term with doubled quotes, exactly like
  // `FastExportStreamFormatter::writeTerm` (RFC 4180 round-trip).
  const std::string literal = "\"a\\\"b, c\"";
  const qlever::constructExport::EvaluatedTermData term{literal, nullptr};

  std::string productionOut =
      captureOutput([&](FastExportStreamFormatter& fmt) {
        fmt.writeTerm(term, ExportFormat::Csv);
      });
  std::string monomorphicOut =
      captureOutput([&](FastExportStreamFormatter& fmt) {
        detail::MonomorphicCellWriter<ColumnType::Literal, ExportFormat::Csv>::
            write(fmt, CellValue::makeLiteral(literal));
      });

  EXPECT_EQ(monomorphicOut, productionOut);
  // `"` + doubled delimiters + `"` : `"a\"b, c"` -> `"""a\""b, c"""`.
  EXPECT_EQ(monomorphicOut, "\"\"\"a\\\"\"b, c\"\"\"");
}

TEST(MonomorphicSerializersTest, UndefinedDispatchSelectsUndefPlaceholder) {
  // One-column schema: `Undefined` must dispatch to its own specialization
  // (writing `UNDEF`), not to `String` via the `default` arm.
  const std::vector<ColumnType> schema1 = {ColumnType::Undefined};
  const std::array<CellValue, 1> row1 = {CellValue()};
  DynamicRowSerializer dynamic1(schema1);
  std::string dynamicOut1 = captureOutput([&](FastExportStreamFormatter& fmt) {
    dynamic1.serializeRow<ExportFormat::Csv>(fmt,
                                             ql::span<const CellValue>(row1));
  });
  std::string dispatchedOut1 =
      captureOutput([&](FastExportStreamFormatter& fmt) {
        dispatchMonomorphicSerializer(schema1, [&](auto& serializer) {
          serializer.template serializeRow<ExportFormat::Csv>(
              fmt, ql::span<const CellValue>(row1));
        });
      });
  EXPECT_EQ(dispatchedOut1, dynamicOut1);
  EXPECT_EQ(dispatchedOut1, "UNDEF\n");

  // Two-column schema with `Undefined` in both positions (covers the outer
  // switch on `c0` and the inner switch on `c1` of `dispatch2Col`).
  const std::vector<ColumnType> schema2 = {ColumnType::Undefined,
                                           ColumnType::Undefined};
  const std::array<CellValue, 2> row2 = {CellValue(), CellValue()};
  DynamicRowSerializer dynamic2(schema2);
  std::string dynamicOut2 = captureOutput([&](FastExportStreamFormatter& fmt) {
    dynamic2.serializeRow<ExportFormat::Csv>(fmt,
                                             ql::span<const CellValue>(row2));
  });
  std::string dispatchedOut2 =
      captureOutput([&](FastExportStreamFormatter& fmt) {
        dispatchMonomorphicSerializer(schema2, [&](auto& serializer) {
          serializer.template serializeRow<ExportFormat::Csv>(
              fmt, ql::span<const CellValue>(row2));
        });
      });
  EXPECT_EQ(dispatchedOut2, dynamicOut2);
  EXPECT_EQ(dispatchedOut2, "UNDEF,UNDEF\n");
}
