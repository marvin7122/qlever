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
#include <concepts>
#include <cstddef>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "global/Id.h"

namespace ql::engine::export_v2 {

// Define the semantic type of a column in a statically known export schema.
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

enum class RowFormat { Csv, Tsv, Turtle, NTriples };

namespace detail {

template <typename Value>
concept StringLike = requires(const Value& value) { std::string_view{value}; };

template <ColumnType Type, RowFormat Format>
struct CellWriter {
  template <typename Writer, StringLike Value>
  static void write(Writer& writer, const Value& value) {
    const std::string_view string{value};
    if constexpr (Type == ColumnType::Iri) {
      if constexpr (Format == RowFormat::Csv) {
        writer.writeEscapedCsv(string);
      } else if constexpr (Format == RowFormat::Tsv) {
        writer.writeEscapedTsv(string);
      } else {
        writer.writeIri(string);
      }
    } else if constexpr (Type == ColumnType::Literal) {
      if constexpr (Format == RowFormat::Csv) {
        writer.writeEscapedCsv(string);
      } else if constexpr (Format == RowFormat::Tsv) {
        writer.writeEscapedTsv(string);
      } else {
        writer.writeEscapedTurtleLiteral(string);
      }
    } else if constexpr (Type == ColumnType::BlankNode ||
                         Type == ColumnType::String) {
      if constexpr (Format == RowFormat::Csv) {
        writer.writeEscapedCsv(string);
      } else if constexpr (Format == RowFormat::Tsv) {
        writer.writeEscapedTsv(string);
      } else {
        writer.writeRaw(string);
      }
    } else {
      static_assert(Type == ColumnType::Iri || Type == ColumnType::Literal ||
                        Type == ColumnType::BlankNode ||
                        Type == ColumnType::String,
                    "This column type requires a non-string argument");
    }
  }

  template <typename Writer, std::integral Value>
  static void write(Writer& writer, Value value) {
    static_assert(!std::is_same_v<Value, bool>,
                  "A Boolean column takes an Id, not a C++ bool; pass "
                  "Id::makeFromBool(...) instead");
    static_assert(Type == ColumnType::Integer,
                  "This column type requires an integer argument");
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

  template <typename Writer, std::floating_point Value>
  static void write(Writer& writer, Value value) {
    static_assert(Type == ColumnType::Double,
                  "Only a Double column accepts floating arguments");
    writer.writeDouble(value);
  }

  template <typename Writer>
  static void writeUndefined(Writer& writer) {
    static_assert(Type == ColumnType::Undefined);
    if constexpr (Format == RowFormat::Turtle ||
                  Format == RowFormat::NTriples) {
      writer.writeRaw("UNDEF");
    }
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
  if constexpr (Format == RowFormat::Turtle || Format == RowFormat::NTriples) {
    writer.writeRaw(" .\n");
  } else {
    writer.writeChar('\n');
  }
}

template <ColumnType Type, RowFormat Format, typename Writer, typename Value>
void writeCell(Writer& writer, const Value& value) {
  if constexpr (Type == ColumnType::Undefined) {
    CellWriter<Type, Format>::writeUndefined(writer);
  } else {
    CellWriter<Type, Format>::write(writer, value);
  }
}

}  // namespace detail

// Writer shape required by `MonomorphicRowSerializer`. Checked (not used for
// dispatch) so a writer missing an operation fails with a single readable
// message at the call site instead of deep inside `CellWriter`.
template <typename Writer>
concept HasWriterOps =
    requires(Writer& writer, char c, std::string_view s, int i, double d) {
      writer.writeChar(c);
      writer.writeRaw(s);
      writer.writeEscapedCsv(s);
      writer.writeEscapedTsv(s);
      writer.writeEscapedTurtleLiteral(s);
      writer.writeIri(s);
      writer.writeInteger(i);
      writer.writeDouble(d);
    };

// Serialize typed tuple values for one compile-time schema. This class does
// not perform runtime schema dispatch. Callers only use it when the planner can
// select a concrete instantiation before entering the row loop.
template <ColumnType... ColumnTypes>
class MonomorphicRowSerializer {
 public:
  static_assert(sizeof...(ColumnTypes) > 0,
                "A row serializer needs at least one column");

  static constexpr size_t numColumns = sizeof...(ColumnTypes);
  static constexpr std::array<ColumnType, numColumns> schema{ColumnTypes...};

  template <RowFormat Format, typename Writer, typename... Values>
  static void serializeRow(Writer& writer, const Values&... values) {
    static_assert(HasWriterOps<Writer>,
                  "The writer must provide the MonomorphicRowSerializer "
                  "operations (writeChar/writeRaw/writeEscapedCsv/writeEscaped"
                  "Tsv/writeEscapedTurtleLiteral/writeIri/writeInteger/"
                  "writeDouble)");
    static_assert(sizeof...(Values) == numColumns,
                  "The argument count must match the static schema");
    serializeTuple<Format>(writer, std::tie(values...),
                           std::make_index_sequence<numColumns>{});
    detail::writeTerminator<Format>(writer);
  }

 private:
  template <RowFormat Format, typename Writer, typename Tuple,
            size_t... Indices>
  static void serializeTuple(Writer& writer, const Tuple& values,
                             std::index_sequence<Indices...>) {
    ((Indices == 0 ? void() : detail::writeDelimiter<Format>(writer),
      detail::writeCell<schema[Indices], Format>(writer,
                                                 std::get<Indices>(values))),
     ...);
  }
};

}  // namespace ql::engine::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_MONOMORPHICSERIALIZERS_H
