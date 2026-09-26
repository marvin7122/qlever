// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <charconv>
#include <cstdint>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "backports/concepts.h"
#include "engine/export_v2/MonomorphicSerializers.h"
#include "global/Id.h"

namespace {

using ql::engine::export_v2::ColumnType;
using ql::engine::export_v2::MonomorphicRowSerializer;
using ql::engine::export_v2::RowFormat;
using ql::engine::export_v2::UndefinedCell;

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

  CPP_template(typename Value)(
      requires ql::concepts::integral<Value>) void writeInteger(Value value) {
    appendNumber(value);
  }

  CPP_template(typename Value)(requires ql::concepts::floating_point<
                               Value>) void writeDouble(Value value) {
    appendNumber(value);
  }

  // Everything written so far; valid while this writer is alive.
  const std::string& output() const { return output_; }

 private:
  template <typename Value>
  void appendNumber(Value value) {
    if constexpr (std::is_floating_point_v<Value>) {
      // No floating-point `std::to_chars` on macOS before 13.3; the
      // ostringstream default formatting matches the expected output.
      std::ostringstream stream;
      stream << value;
      output_ += stream.str();
      return;
    }
    std::array<char, 64> buffer;
    const auto [end, error] =
        std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
    ASSERT_EQ(error, std::errc{});
    output_.append(buffer.data(), end);
  }

  std::string output_;
};

TEST(MonomorphicSerializersTest, SerializesMonomorphicRowAsCsv) {
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
      Id::makeFromBool(false), UndefinedCell{});

  EXPECT_EQ(writer.output(), "T\t-9223372036854775808\tfalse\t\n");
}

TEST(MonomorphicSerializersTest, BooleanRendersStoredIdLiteral) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Boolean>;
  for (const auto& [id, expected] :
       {std::pair{Id::makeFromBool(false), "false\n"},
        std::pair{Id::makeFromBool(true), "true\n"},
        std::pair{Id::makeBoolFromZeroOrOne(false), "0\n"},
        std::pair{Id::makeBoolFromZeroOrOne(true), "1\n"}}) {
    RecordingWriter writer;
    Serializer::serializeRow<RowFormat::Csv>(writer, id);
    EXPECT_EQ(writer.output(), expected);
  }
}

TEST(MonomorphicSerializersTest, TurtleCoversBlankNodeAndDouble) {
  using Serializer =
      MonomorphicRowSerializer<ColumnType::BlankNode, ColumnType::Iri,
                               ColumnType::Double>;
  RecordingWriter writer;

  Serializer::serializeRow<RowFormat::Turtle>(writer, "_:b0", "<p>", 153.07);

  EXPECT_EQ(writer.output(), "_:b0 I<p> 153.07 .\n");
}

TEST(MonomorphicSerializersTest, UndefinedIsAnEmptyCsvField) {
  using Serializer =
      MonomorphicRowSerializer<ColumnType::Undefined, ColumnType::Integer,
                               ColumnType::Undefined>;
  RecordingWriter writer;

  Serializer::serializeRow<RowFormat::Csv>(writer, UndefinedCell{}, 42,
                                           UndefinedCell{});

  EXPECT_EQ(writer.output(), ",42,\n");
}

TEST(MonomorphicSerializersTest, NTriplesRendersIriAndLiteral) {
  using Serializer = MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri,
                                              ColumnType::Literal>;
  RecordingWriter writer;

  Serializer::serializeRow<RowFormat::NTriples>(writer, "<s>", "<p>", "lit");

  EXPECT_EQ(writer.output(), "I<s> I<p> Llit .\n");
}

TEST(MonomorphicSerializersTest, WriterConceptDetectsMissingOperations) {
  using ql::engine::export_v2::HasWriterOps;
  static_assert(HasWriterOps<RecordingWriter>);
  static_assert(!HasWriterOps<int>);
}

TEST(MonomorphicSerializersTest, ExposesTheStaticSchema) {
  using Serializer =
      MonomorphicRowSerializer<ColumnType::BlankNode, ColumnType::Boolean>;

  static_assert(Serializer::numColumns == 2);
  static_assert(Serializer::schema[0] == ColumnType::BlankNode);
  static_assert(Serializer::schema[1] == ColumnType::Boolean);
}

}  // namespace
