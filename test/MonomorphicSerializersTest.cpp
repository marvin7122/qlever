// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <charconv>
#include <cmath>
#include <iterator>
#include <limits>
#include <string>
#include <string_view>

#include "engine/export_v2/MonomorphicSerializers.h"
#include "global/ValueId.h"

namespace {

using ql::engine::export_v2::ColumnType;
using ql::engine::export_v2::MonomorphicRowSerializer;
using ql::engine::export_v2::RowFormat;

class RecordingWriter {
 public:
  void writeChar(char value) { output_.push_back(value); }
  void writeRaw(std::string_view value) { output_.append(value); }

  void writeEscapedCsv(std::string_view value) {
    output_.push_back('C');
    output_.append(value);
  }

  void writeEscapedTsv(std::string_view value) {
    output_.push_back('T');
    output_.append(value);
  }

  void writeEscapedTurtleLiteral(std::string_view value) {
    output_.push_back('L');
    output_.append(value);
  }

  void writeIri(std::string_view value) {
    output_.push_back('I');
    output_.append(value);
  }

  template <std::integral Value>
  void writeInteger(Value value) {
    appendNumber(value);
  }

  const std::string& output() const { return output_; }

 private:
  template <typename Value>
  void appendNumber(Value value) {
    char buffer[64];
    const auto [end, error] =
        std::to_chars(std::begin(buffer), std::end(buffer), value);
    ASSERT_EQ(error, std::errc{});
    output_.append(buffer, end);
  }

  std::string output_;
};

TEST(MonomorphicSerializersTest, SerializesDirectTypedTupleAsCsv) {
  using Serializer =
      MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Literal,
                               ColumnType::Integer, ColumnType::Double>;
  RecordingWriter writer;

  Serializer::serializeRow<RowFormat::Csv>(writer, "<city>", "Freiburg", 230000,
                                           153.07);

  EXPECT_EQ(writer.output(), "C<city>,CFreiburg,230000,153.07\n");
}

TEST(MonomorphicSerializersTest, SelectsTurtleOperationsAtCompileTime) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri,
                                              ColumnType::Literal>;
  RecordingWriter writer;

  Serializer::serializeRow<RowFormat::Turtle>(writer, "<s>", "<p>", "value");

  EXPECT_EQ(writer.output(), "I<s> I<p> Lvalue .\n");
}

TEST(MonomorphicSerializersTest, HandlesEmptyAndBoundaryValues) {
  using Serializer =
      MonomorphicRowSerializer<ColumnType::String, ColumnType::Integer,
                               ColumnType::Boolean, ColumnType::Undefined>;
  RecordingWriter writer;

  Serializer::serializeRow<RowFormat::Tsv>(
      writer, std::string_view{}, std::numeric_limits<int64_t>::min(),
      Id::makeFromBool(false), 0);

  EXPECT_EQ(writer.output(), "T\t-9223372036854775808\tfalse\t\n");
}

// Correctness vs Legacy CSV (`idToStringAndTypeForEncodedValue` in
// `src/index/ExportIds.cpp`). These checks gate swapping the live SELECT path
// onto CellWriter: every byte must match first.
TEST(MonomorphicSerializersTest, DoubleMatchesLegacyEncodedCsv) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Double>;
  for (const auto [value, legacy] :
       {std::pair{1.0, "1.0\n"},
        std::pair{-0.0, "-0.0\n"},
        std::pair{0.5, "0.5\n"},
        std::pair{153.07, "153.07\n"},
        std::pair{1e300, "1e+300\n"},
        std::pair{std::numeric_limits<double>::quiet_NaN(), "NaN\n"},
        std::pair{std::numeric_limits<double>::infinity(), "INF\n"},
        std::pair{-std::numeric_limits<double>::infinity(), "-INF\n"}}) {
    RecordingWriter writer;
    Serializer::serializeRow<RowFormat::Csv>(writer, value);
    EXPECT_EQ(writer.output(), legacy);
  }
}

TEST(MonomorphicSerializersTest, BooleanMatchesLegacyBoolLiteral) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Boolean>;
  for (const auto [id, legacy] : {std::pair{Id::makeFromBool(false), "false\n"},
                                  std::pair{Id::makeFromBool(true), "true\n"},
                                  std::pair{Id::makeBoolFromZeroOrOne(false),
                                            "0\n"},
                                  std::pair{Id::makeBoolFromZeroOrOne(true),
                                            "1\n"}}) {
    RecordingWriter writer;
    Serializer::serializeRow<RowFormat::Csv>(writer, id);
    EXPECT_EQ(writer.output(), legacy);
  }
}

TEST(MonomorphicSerializersTest, CsvIriWritesBareContentLikeLegacySelectCsv) {
  // The vocabulary path hands the writer bare content (Legacy strips `<>`
  // via `removeQuotesAndAngleBrackets`), so the CSV writer only escapes.
  using Serializer = MonomorphicRowSerializer<ColumnType::Iri>;
  RecordingWriter writer;
  Serializer::serializeRow<RowFormat::Csv>(writer, "https://example.org/x");
  EXPECT_EQ(writer.output(), "Chttps://example.org/x\n");
}

TEST(MonomorphicSerializersTest, ExposesTheStaticSchema) {
  using Serializer =
      MonomorphicRowSerializer<ColumnType::BlankNode, ColumnType::Boolean>;

  static_assert(Serializer::numColumns == 2);
  static_assert(Serializer::schema[0] == ColumnType::BlankNode);
  static_assert(Serializer::schema[1] == ColumnType::Boolean);
}

}  // namespace
