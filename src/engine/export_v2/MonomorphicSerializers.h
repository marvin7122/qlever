// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_V2_MONOMORPHICSERIALIZERS_H
#define QLEVER_SRC_ENGINE_EXPORT_V2_MONOMORPHICSERIALIZERS_H

#include <absl/strings/str_format.h>

#include <array>
#include <cmath>
#include <cstddef>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "global/Id.h"

namespace ql::engine::export_v2 {

// The semantic type of a column in a statically known export schema.
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

// The row oriented formats supported by the V2 serializer.
enum class RowFormat { Csv, Tsv, Turtle, NTriples };

namespace detail {

// Whether `Value` converts to `std::string_view`. Trait form (not `concept`)
// because the GCC 8 CI job compiles this header as C++17.
template <typename Value, typename = void>
struct IsStringLike : std::false_type {};
template <typename Value>
struct IsStringLike<Value, std::void_t<decltype(std::string_view{
                               std::declval<const Value&>()})>>
    : std::true_type {};
template <typename Value>
inline constexpr bool IsStringLike_v = IsStringLike<Value>::value;

// Render a double exactly like Legacy CSV
// (`ql::exportIds::idToStringAndTypeForEncodedValue` in
// `src/index/ExportIds.cpp`): integral values get one decimal place (`1.0`,
// never `1`), other values use
// `%.13g` with a `.0` suffix when rounding removed the separator, and
// non-finite values use the RDF spellings `NaN`/`INF`/`-INF`. `std::to_chars`
// matches none of this, so the cell formats here instead of delegating to
// `Writer::writeDouble`.
[[nodiscard]] inline std::string formatLegacyDouble(double value) {
  if (!std::isfinite(value)) {
    if (std::isnan(value)) {
      return "NaN";
    }
    return value > 0 ? "INF" : "-INF";
  }
  double intPart = 0.0;
  if (std::modf(value, &intPart) == 0.0) {
    // Integral doubles grow beyond any fixed buffer (`%.1f` of `1e300` is 301
    // digits), so format dynamically, exactly like Legacy.
    return absl::StrFormat("%.1f", value);
  }
  std::string out = absl::StrFormat("%.13g", value);
  if (out.find_last_of(".e") == std::string::npos) {
    out += ".0";
  }
  return out;
}

template <ColumnType Type, RowFormat Format>
struct CellWriter {
  template <typename Writer, typename Value,
            std::enable_if_t<IsStringLike_v<Value>, int> = 0>
  static void write(Writer& writer, const Value& value) {
    const std::string_view string{value};
    // Contract: for CSV/TSV the caller passes bare vocabulary content (Legacy
    // strips `<>` and quotes before escaping), so the writer only escapes.
    // Turtle keeps the bracketed/quoted representation instead.
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

  template <typename Writer, typename Value,
            std::enable_if_t<std::is_integral_v<Value>, int> = 0>
  static void write(Writer& writer, Value value) {
    static_assert(Type == ColumnType::Integer,
                  "This column type requires a string or floating argument");
    writer.writeInteger(value);
  }

  // A Boolean column takes the `Id`, not a C++ `bool`: Legacy renders the
  // stored literal (`true`/`false` or `0`/`1`, depending on how the `Id` was
  // created, see `Id::getBoolLiteral`), which a bare `bool` cannot reproduce.
  template <typename Writer>
  static void write(Writer& writer, Id id) {
    static_assert(Type == ColumnType::Boolean,
                  "Only a Boolean column accepts an Id argument");
    writer.writeRaw(id.getBoolLiteral());
  }

  template <typename Writer, typename Value,
            std::enable_if_t<std::is_floating_point_v<Value>, int> = 0>
  static void write(Writer& writer, Value value) {
    static_assert(Type == ColumnType::Double,
                  "Only a Double column accepts floating arguments");
    writer.writeRaw(formatLegacyDouble(value));
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

// Serializes typed tuple values for one compile time schema. This class does
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
