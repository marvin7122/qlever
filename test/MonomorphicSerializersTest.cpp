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
#include <string_view>
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

// Visitor serving both `dispatchMonomorphicSerializer` call forms (see the
// benchmark of the same name). Namespace scope is required because member
// templates are not allowed in local classes.
struct MonomorphicDispatchTestVisitor {
  FastExportStreamFormatter& fmt;
  const ql::span<const CellValue> row;
  template <ColumnType... Types>
  void operator()() const {
    using S = MonomorphicRowSerializer<Types...>;
    S::template serializeRow<ExportFormat::Turtle>(
        fmt, ql::span<const CellValue>(row));
  }
  void operator()(DynamicRowSerializer& dynamicSerializer) const {
    dynamicSerializer.template serializeRow<ExportFormat::Turtle>(
        fmt, ql::span<const CellValue>(row));
  }
};

TEST(MonomorphicSerializersTest, UndefinedColumnDispatchesToUndefWriter) {
  // `ColumnType::Undefined` must reach the `Undefined` cell writer (which
  // emits `UNDEF`), not the `String` writer. Guards the `Undefined` cases in
  // `dispatch1Col`/`dispatch2Col`.
  const std::vector<ColumnType> schema = {ColumnType::Undefined};
  std::array<CellValue, 1> row = {CellValue{}};

  std::string dispatchedOut =
      captureOutput([&](FastExportStreamFormatter& fmt) {
        dispatchMonomorphicSerializer(schema,
                                      MonomorphicDispatchTestVisitor{fmt, row});
      });

  EXPECT_EQ(dispatchedOut, "UNDEF .\n");
}

TEST(MonomorphicSerializersTest, UndefinedSecondColumnDispatch) {
  const std::vector<ColumnType> schema = {ColumnType::Iri,
                                          ColumnType::Undefined};
  std::array<CellValue, 2> row = {CellValue::makeIri("<http://s>"),
                                  CellValue{}};

  std::string dispatchedOut =
      captureOutput([&](FastExportStreamFormatter& fmt) {
        dispatchMonomorphicSerializer(schema,
                                      MonomorphicDispatchTestVisitor{fmt, row});
      });

  EXPECT_EQ(dispatchedOut, "<http://s> UNDEF .\n");
}

TEST(MonomorphicSerializersTest, FastPathTemplateDispatch) {
  const std::vector<ColumnType> schema = {ColumnType::Iri, ColumnType::Iri,
                                          ColumnType::Literal};
  std::array<CellValue, 3> row = {CellValue::makeIri("<http://s>"),
                                  CellValue::makeIri("<http://p>"),
                                  CellValue::makeLiteral("\"o\"")};

  std::string dispatchedOut =
      captureOutput([&](FastExportStreamFormatter& fmt) {
        dispatchMonomorphicSerializer(schema,
                                      MonomorphicDispatchTestVisitor{fmt, row});
      });

  EXPECT_EQ(dispatchedOut, "<http://s> <http://p> \"o\" .\n");
}

// _____________________________________________________________________________
TEST(FastExportStreamFormatterTest, FixedSpanFlushKeepsBufferedBytes) {
  std::array<char, 16> buffer{};
  FastExportStreamFormatter formatter{ql::span<char>{buffer}};
  formatter.writeRaw("abc");
  // Without a sink, `flush()` must neither drop nor double-count the bytes.
  formatter.flush();
  EXPECT_EQ(formatter.currentChunk(), "abc");
  EXPECT_EQ(formatter.bytesBuffered(), 3u);
  EXPECT_EQ(formatter.totalBytesWritten(), 3u);
  formatter.writeRaw("de");
  EXPECT_EQ(formatter.currentChunk(), "abcde");
  EXPECT_EQ(formatter.totalBytesWritten(), 5u);

  auto summary = std::move(formatter).finalize();
  EXPECT_EQ(summary.totalBytesWritten_, 5u);
  EXPECT_EQ(summary.chunksEmitted_, 0u);
  EXPECT_EQ(std::string_view(buffer.data(), 5), "abcde");
}

// _____________________________________________________________________________
TEST(FastExportStreamFormatterTest, StreamingFlushEmitsChunk) {
  std::vector<std::string> chunks;
  FastExportStreamFormatter formatter{
      [&chunks](std::string_view chunk) { chunks.emplace_back(chunk); }};
  formatter.writeRaw("abc");
  formatter.flush();
  EXPECT_EQ(formatter.bytesBuffered(), 0u);
  EXPECT_EQ(formatter.totalBytesWritten(), 3u);
  formatter.writeRaw("de");
  auto summary = std::move(formatter).finalize();
  EXPECT_EQ(summary.totalBytesWritten_, 5u);
  EXPECT_EQ(summary.chunksEmitted_, 2u);
  EXPECT_THAT(chunks, ::testing::ElementsAre("abc", "de"));
}

// _____________________________________________________________________________
TEST(MonomorphicSerializersTest, FixedSpanOverflowPropagatesException) {
  // The row terminator no longer fits, the overflow exception must reach the
  // caller instead of terminating inside a `noexcept` helper.
  std::array<char, 3> buffer{};
  FastExportStreamFormatter formatter{ql::span<char>{buffer}};
  using Serializer = MonomorphicRowSerializer<ColumnType::Iri>;
  EXPECT_ANY_THROW(Serializer::serializeRow<ExportFormat::Csv>(
      formatter, std::string_view{"<a>"}));
  EXPECT_EQ(formatter.currentChunk(), "<a>");
}
