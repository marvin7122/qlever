// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <charconv>
#include <iterator>
#include <limits>
#include <string>
#include <string_view>

#include "engine/export_v2/MonomorphicSerializers.h"

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

  template <std::floating_point Value>
  void writeDouble(Value value) {
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

  Serializer::serializeRow<RowFormat::Tsv>(writer, std::string_view{},
                                           std::numeric_limits<int64_t>::min(),
                                           false, 0);

  EXPECT_EQ(writer.output(), "T\t-9223372036854775808\tfalse\t\n");
}

// Correctness vs Legacy CSV (`idToStringAndType` / ExportIds.cpp). These
// checks exist so we do not swap the live SELECT path onto CellWriter until
// the bytes match. RecordingWriter::writeDouble uses std::to_chars, which is
// what MonomorphicRowSerializer asks of a Writer.
TEST(MonomorphicSerializersTest, DoubleOneDoesNotMatchLegacyEncodedCsv) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Double>;
  RecordingWriter writer;
  Serializer::serializeRow<RowFormat::Csv>(writer, 1.0);
  EXPECT_EQ(writer.output(), "1\n");
  EXPECT_NE(writer.output(), "1.0\n");
}

TEST(MonomorphicSerializersTest, BooleanDoesNotUseEncodedZeroOneForms) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Boolean>;
  RecordingWriter writer;
  Serializer::serializeRow<RowFormat::Csv>(writer, false);
  EXPECT_EQ(writer.output(), "false\n");
  EXPECT_NE(writer.output(), "0\n");
}

TEST(MonomorphicSerializersTest, CsvIriKeepsBracketsUnlikeLegacySelectCsv) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Iri>;
  RecordingWriter writer;
  Serializer::serializeRow<RowFormat::Csv>(writer, "<https://example.org/x>");
  EXPECT_EQ(writer.output(), "C<https://example.org/x>\n");
  EXPECT_NE(writer.output(), "https://example.org/x\n");
}

TEST(MonomorphicSerializersTest, ExposesTheStaticSchema) {
  using Serializer =
      MonomorphicRowSerializer<ColumnType::BlankNode, ColumnType::Boolean>;

  static_assert(Serializer::numColumns == 2);
  static_assert(Serializer::schema[0] == ColumnType::BlankNode);
  static_assert(Serializer::schema[1] == ColumnType::Boolean);
}

}  // namespace
