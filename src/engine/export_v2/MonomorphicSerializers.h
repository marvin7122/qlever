// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_V2_MONOMORPHICSERIALIZERS_H
#define QLEVER_SRC_ENGINE_EXPORT_V2_MONOMORPHICSERIALIZERS_H

#include <array>
#include <cstddef>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "backports/concepts.h"
#include "global/Id.h"

namespace ql::engine::export_v2 {

// The semantic type of one column in a schema that is known at compile time.
// It selects the writer operation for the cell (e.g. IRI vs. literal
// escaping), so the row loop contains no runtime dispatch on the value type.
enum class ColumnType {
  Iri,
  Literal,
  Integer,
  Double,
  BlankNode,
  Boolean,
  String,
  Undefined
};

// The output format. It is a template parameter so that delimiter,
// terminator, and escaping are fixed per instantiation. `Turtle` and
// `NTriples` rows are triples and therefore must have exactly three columns.
enum class RowFormat { Csv, Tsv, Turtle, NTriples };

// The value passed for a `ColumnType::Undefined` column. An explicit tag type
// (instead of an arbitrary ignored value) makes an unbound cell visible at the
// call site and rejects bound values for undefined columns at compile time.
struct UndefinedCell {};

namespace detail {

// A value that can be viewed as a string. Pointers are excluded because a
// null `const char*` would make the `std::string_view` construction undefined;
// string literals bind as arrays and are therefore still accepted.
template <typename Value>
CPP_concept StringLike =
    std::is_constructible_v<std::string_view, const Value&> &&
    !std::is_pointer_v<Value>;

template <RowFormat Format>
constexpr bool isRdfFormat =
    Format == RowFormat::Turtle || Format == RowFormat::NTriples;

template <ColumnType Type, RowFormat Format>
struct CellWriter {
  CPP_template(typename Writer, typename Value)(
      requires StringLike<Value>) static void write(Writer& writer,
                                                    const Value& value) {
    static_assert(Type == ColumnType::Iri || Type == ColumnType::Literal ||
                      Type == ColumnType::BlankNode ||
                      Type == ColumnType::String,
                  "Only Iri, Literal, BlankNode, and String columns accept a "
                  "string argument");
    const std::string_view string{value};
    if constexpr (Format == RowFormat::Csv) {
      writer.writeEscapedCsv(string);
    } else if constexpr (Format == RowFormat::Tsv) {
      writer.writeEscapedTsv(string);
    } else if constexpr (Type == ColumnType::Iri) {
      writer.writeIri(string);
    } else if constexpr (Type == ColumnType::Literal) {
      // The N-Triples literal escapes are a subset of the Turtle ones, so one
      // writer operation serves both formats.
      writer.writeEscapedTurtleLiteral(string);
    } else {
      writer.writeRaw(string);
    }
  }

  CPP_template(typename Writer, typename Value)(
      requires ql::concepts::integral<Value>) static void write(Writer& writer,
                                                                Value value) {
    static_assert(!std::is_same_v<Value, bool>,
                  "A Boolean column takes an Id, not a C++ bool; pass "
                  "Id::makeFromBool(...) instead");
    static_assert(Type == ColumnType::Integer,
                  "Only an Integer column accepts an integral argument");
    writer.writeInteger(value);
  }

  // A Boolean column takes the `Id`, not a C++ `bool`: the export renders the
  // stored literal (`true`/`false` or `0`/`1`, depending on how the `Id` was
  // created, see `Id::getBoolLiteral`), which a bare `bool` cannot reproduce.
  template <typename Writer>
  static void write(Writer& writer, Id id) {
    static_assert(Type == ColumnType::Boolean,
                  "Only a Boolean column accepts an Id argument");
    writer.writeRaw(id.getBoolLiteral());
  }

  CPP_template(typename Writer,
               typename Value)(requires ql::concepts::floating_point<
                               Value>) static void write(Writer& writer,
                                                         Value value) {
    static_assert(Type == ColumnType::Double,
                  "Only a Double column accepts a floating-point argument");
    writer.writeDouble(value);
  }

  // An unbound CSV/TSV cell is the empty field between two delimiters, so
  // nothing is written. RDF formats have no representation for an unbound
  // term, so `Undefined` columns are rejected for them at compile time.
  template <typename Writer>
  static void write(Writer&, UndefinedCell) {
    static_assert(Type == ColumnType::Undefined,
                  "Only an Undefined column accepts an UndefinedCell");
    static_assert(!isRdfFormat<Format>,
                  "Turtle and N-Triples cannot represent an unbound term");
  }
};

template <RowFormat Format, typename Writer>
void writeDelimiter(Writer& writer) {
  if constexpr (Format == RowFormat::Csv) {
    writer.writeChar(',');
  } else if constexpr (Format == RowFormat::Tsv) {
    writer.writeChar('\t');
  } else {
    writer.writeChar(' ');
  }
}

template <RowFormat Format, typename Writer>
void writeTerminator(Writer& writer) {
  if constexpr (isRdfFormat<Format>) {
    writer.writeRaw(" .\n");
  } else {
    writer.writeChar('\n');
  }
}

// Writer shape required by `MonomorphicRowSerializer`. Checked (not used for
// dispatch) so a writer missing an operation fails with a single readable
// message at the call site instead of deep inside `CellWriter`.
template <typename Writer>
CPP_requires(HasWriterOpsRequires,
             requires(Writer& writer, char c, std::string_view s, int i,
                      double d)(writer.writeChar(c), writer.writeRaw(s),
                                writer.writeEscapedCsv(s),
                                writer.writeEscapedTsv(s),
                                writer.writeEscapedTurtleLiteral(s),
                                writer.writeIri(s), writer.writeInteger(i),
                                writer.writeDouble(d)));

}  // namespace detail

template <typename Writer>
CPP_concept HasWriterOps =
    CPP_requires_ref(detail::HasWriterOpsRequires, Writer);

// Serialize the values of one row for a schema that is fixed at compile time.
// This class does not perform runtime schema dispatch: callers use it only
// when the concrete instantiation can be selected before the row loop.
template <ColumnType... ColumnTypes>
class MonomorphicRowSerializer {
 public:
  static_assert(sizeof...(ColumnTypes) > 0,
                "A row serializer needs at least one column");

  // The schema as values, e.g. for a caller that checks at runtime that a
  // result table matches the instantiation it is about to use.
  static constexpr size_t numColumns = sizeof...(ColumnTypes);
  static constexpr std::array<ColumnType, numColumns> schema{ColumnTypes...};

  // Write one row with one value per column (in schema order), separated by
  // the delimiter of `Format` and followed by its row terminator. Each value
  // must match its column type: a string for Iri/Literal/BlankNode/String, an
  // integer for Integer, a floating-point value for Double, an `Id` for
  // Boolean, and `UndefinedCell` for Undefined. A mismatch fails to compile.
  template <RowFormat Format, typename Writer, typename... Values>
  static void serializeRow(Writer& writer, const Values&... values) {
    static_assert(HasWriterOps<Writer>,
                  "The writer must provide the MonomorphicRowSerializer "
                  "operations (writeChar/writeRaw/writeEscapedCsv/writeEscaped"
                  "Tsv/writeEscapedTurtleLiteral/writeIri/writeInteger/"
                  "writeDouble)");
    static_assert(sizeof...(Values) == numColumns,
                  "The argument count must match the static schema");
    static_assert(!detail::isRdfFormat<Format> || numColumns == 3,
                  "A Turtle or N-Triples row is a triple with three columns");
    serializeTuple<Format>(writer, std::tie(values...),
                           std::make_index_sequence<numColumns>{});
    detail::writeTerminator<Format>(writer);
  }

 private:
  template <RowFormat Format, typename Writer, typename Tuple,
            size_t... Indices>
  static void serializeTuple(Writer& writer, const Tuple& values,
                             std::index_sequence<Indices...>) {
    // For every column: a delimiter before all but the first one, then the
    // cell itself.
    ((Indices == 0 ? void() : detail::writeDelimiter<Format>(writer),
      detail::CellWriter<schema[Indices], Format>::write(
          writer, std::get<Indices>(values))),
     ...);
  }
};

}  // namespace ql::engine::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_MONOMORPHICSERIALIZERS_H
