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
#include <limits>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

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
        fmt, "<http://example.org/subj>", "<http://example.org/pred>",
        "\"Hello, World!\"");
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
                        [&]<ColumnType... Types>() {
                          using S = MonomorphicRowSerializer<Types...>;
                          S::template serializeRow<ExportFormat::Turtle>(
                              fmt, ql::span<const CellValue>(row));
                        },
                        [&](DynamicRowSerializer& dynamicSerializer) {
                          dynamicSerializer.serializeRow<ExportFormat::Turtle>(
                              fmt, ql::span<const CellValue>(row));
                        }});
      });

  EXPECT_EQ(dispatchedOut, "<http://s> <http://p> \"o\" .\n");
}

// _____________________________________________________________________________
// The span and tuple paths must use the writer of each column's own type.
TEST(MonomorphicSerializersTest, SpanAndTuplePathsUsePerColumnTypes) {
  using Serializer =
      MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Literal,
                               ColumnType::Int>;
  std::array<CellValue, 3> row = {CellValue::makeIri("http://x"),
                                  CellValue::makeLiteral("\"a\""),
                                  CellValue::makeInt(7)};

  std::string spanOut = captureOutput([&](FastExportStreamFormatter& fmt) {
    Serializer::serializeRow<ExportFormat::Turtle>(
        fmt, ql::span<const CellValue>(row));
  });
  std::string tupleOut = captureOutput([&](FastExportStreamFormatter& fmt) {
    Serializer::serializeRowTuple<ExportFormat::Turtle>(
        fmt, std::make_tuple(row[0], row[1], row[2]));
  });

  EXPECT_EQ(spanOut, "<http://x> \"a\" 7 .\n");
  EXPECT_EQ(tupleOut, spanOut);
}

// _____________________________________________________________________________
// An `Undefined` column keeps its format-specific output (`UNDEF` in Turtle,
// empty in CSV) when dispatched from a runtime schema.
TEST(MonomorphicSerializersTest, DispatchKeepsUndefinedSemantics) {
  const std::vector<ColumnType> schema = {ColumnType::Iri,
                                          ColumnType::Undefined};
  const std::array<CellValue, 2> row = {CellValue::makeIri("<http://x>"),
                                        CellValue{}};
  auto serializeWith = [&](auto format) {
    constexpr ExportFormat Format = decltype(format)::value;
    return captureOutput([&](FastExportStreamFormatter& fmt) {
      dispatchMonomorphicSerializer(
          schema,
          ad_utility::OverloadCallOperator{
              [&]<ColumnType... Types>() {
                MonomorphicRowSerializer<Types...>::template serializeRow<
                    Format>(fmt, ql::span<const CellValue>(row));
              },
              [&](DynamicRowSerializer& dynamicSerializer) {
                dynamicSerializer.serializeRow<Format>(
                    fmt, ql::span<const CellValue>(row));
              }});
    });
  };

  EXPECT_EQ(serializeWith(
                std::integral_constant<ExportFormat, ExportFormat::Turtle>{}),
            "<http://x> UNDEF .\n");
  EXPECT_EQ(
      serializeWith(std::integral_constant<ExportFormat, ExportFormat::Csv>{}),
      "<http://x>,\n");
}

// _____________________________________________________________________________
// Writing past the end of a caller-provided span throws instead of
// terminating (the write functions are not `noexcept`).
TEST(MonomorphicSerializersTest, FixedSpanFormatterOverflowThrows) {
  std::array<char, 4> storage{};
  FastExportStreamFormatter fmt{ql::span<char>(storage)};
  fmt.writeRaw("abcd");
  EXPECT_THROW(fmt.writeChar('e'), ad_utility::Exception);
  EXPECT_THROW(fmt.writeRaw("xy"), ad_utility::Exception);
  EXPECT_THROW(fmt.writeInteger(42), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(MonomorphicSerializersTest, CellValueRejectsUnsignedOverflow) {
  EXPECT_EQ(CellValue{uint64_t{42}}.intVal_, 42);
  EXPECT_THROW(CellValue{std::numeric_limits<uint64_t>::max()},
               ad_utility::Exception);
  static_assert(!std::is_constructible_v<CellValue, std::string&&>);
  static_assert(std::is_constructible_v<CellValue, const std::string&>);
}
