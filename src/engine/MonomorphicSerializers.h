
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_MONOMORPHICSERIALIZERS_H
#define QLEVER_SRC_ENGINE_MONOMORPHICSERIALIZERS_H

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "backports/concepts.h"
#include "backports/span.h"
#include "backports/StartsWithAndEndsWith.h"
#include "engine/ConstructTypes.h"
#include "engine/export_prototypes/FastExportStreamFormatter.h"
#include "global/Constants.h"
#include "util/Exception.h"
#include "util/Invariants.h"

namespace ql::serialization {

using ql::export_formatting::ExportFormat;
using ql::export_formatting::FastExportStreamFormatter;

// _____________________________________________________________________________
// Fundamental column and cell datatypes in SPARQL query engine export pipelines.
enum class ColumnType : uint8_t {
  Iri = 0,
  Literal = 1,
  Int = 2,
  Double = 3,
  BlankNode = 4,
  Boolean = 5,
  String = 6,
  Undefined = 7
};

// Convenient `constexpr` aliases.
inline constexpr ColumnType IRI = ColumnType::Iri;
inline constexpr ColumnType LITERAL = ColumnType::Literal;
inline constexpr ColumnType INT = ColumnType::Int;
inline constexpr ColumnType DOUBLE = ColumnType::Double;
inline constexpr ColumnType BLANK_NODE = ColumnType::BlankNode;
inline constexpr ColumnType BOOLEAN = ColumnType::Boolean;
inline constexpr ColumnType STRING = ColumnType::String;
inline constexpr ColumnType UNDEFINED = ColumnType::Undefined;

// Human-readable string representation of `ColumnType`.
[[nodiscard]] constexpr std::string_view toString(ColumnType type) noexcept {
  switch (type) {
    case ColumnType::Iri:
      return "IRI";
    case ColumnType::Literal:
      return "Literal";
    case ColumnType::Int:
      return "Int";
    case ColumnType::Double:
      return "Double";
    case ColumnType::BlankNode:
      return "BlankNode";
    case ColumnType::Boolean:
      return "Boolean";
    case ColumnType::String:
      return "String";
    case ColumnType::Undefined:
      return "Undefined";
  }
  return "Unknown";
}

// _____________________________________________________________________________
// Lightweight value holder representing a cell value across formats and types.
struct CellValue {
  ColumnType type_ = ColumnType::Undefined;
  std::string stringVal_{};
  int64_t intVal_ = 0;
  double doubleVal_ = 0.0;
  bool boolVal_ = false;

  constexpr CellValue() noexcept = default;

  explicit CellValue(std::string sv, ColumnType type = ColumnType::String) noexcept
      : type_(type), stringVal_(std::move(sv)) {}

  explicit CellValue(const char* s, ColumnType type = ColumnType::String)
      : type_(type), stringVal_(s) {}

  explicit constexpr CellValue(int64_t v) noexcept
      : type_(ColumnType::Int), intVal_(v) {}

  explicit constexpr CellValue(int v) noexcept
      : type_(ColumnType::Int), intVal_(v) {}

  explicit CellValue(uint64_t v) noexcept
      : type_(ColumnType::Int), intVal_(static_cast<int64_t>(v)) {
    AD_CONTRACT_CHECK(v <= static_cast<uint64_t>(INT64_MAX));
  }

  explicit constexpr CellValue(double v) noexcept
      : type_(ColumnType::Double), doubleVal_(v) {}

  explicit constexpr CellValue(bool v) noexcept
      : type_(ColumnType::Boolean), boolVal_(v) {}

  [[nodiscard]] static CellValue makeIri(std::string iri) noexcept {
    CellValue c;
    c.type_ = ColumnType::Iri;
    c.stringVal_ = std::move(iri);
    return c;
  }

  [[nodiscard]] static CellValue makeLiteral(std::string lit) noexcept {
    CellValue c;
    c.type_ = ColumnType::Literal;
    c.stringVal_ = std::move(lit);
    return c;
  }

  [[nodiscard]] static CellValue makeBlankNode(std::string bnode) noexcept {
    CellValue c;
    c.type_ = ColumnType::BlankNode;
    c.stringVal_ = std::move(bnode);
    return c;
  }

  [[nodiscard]] static constexpr CellValue makeInt(int64_t val) noexcept {
    CellValue c;
    c.type_ = ColumnType::Int;
    c.intVal_ = val;
    return c;
  }

  [[nodiscard]] static constexpr CellValue makeDouble(double val) noexcept {
    CellValue c;
    c.type_ = ColumnType::Double;
    c.doubleVal_ = val;
    return c;
  }
};

// _____________________________________________________________________________
// High-performance low-level direct buffer serializer / writer concept wrapper.
template <typename Writer>
concept FormatterWriter = requires(Writer& w, char c, std::string_view sv, int64_t i) {
  { w.writeChar(c) };
  { w.writeRaw(sv) };
  { w.writeInteger(i) };
  { w.writeEscapedCsv(sv) };
  { w.writeEscapedTsv(sv) };
  { w.writeEscapedTurtleLiteral(sv) };
  { w.writeIri(sv) };
};

namespace detail {

// Double / Float serialization without dynamic allocation
template <typename Writer>
inline void writeFormattedDouble(Writer& writer, double val) noexcept {
  std::array<char, 32> buffer;
  auto [ptr, ec] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), val);
  if (ec == std::errc{}) {
    writer.writeRaw(std::string_view(buffer.data(), ptr - buffer.data()));
  } else {
    writer.writeRaw("0.0");
  }
}

// _____________________________________________________________________________
// Monomorphic cell serializer specialized by compile-time ColumnType and ExportFormat.
template <ColumnType Type, ExportFormat Format>
struct MonomorphicCellWriter {
  template <typename Writer>
  static void write(Writer& writer, const CellValue& cell) {
    if constexpr (Type == ColumnType::Iri) {
      if constexpr (Format == ExportFormat::Csv) {
        writer.writeEscapedCsv(cell.stringVal_);
      } else if constexpr (Format == ExportFormat::Tsv) {
        writer.writeEscapedTsv(cell.stringVal_);
      } else {
        writer.writeIri(cell.stringVal_);
      }
    } else if constexpr (Type == ColumnType::Literal) {
      if constexpr (Format == ExportFormat::Csv) {
        writer.writeEscapedCsv(cell.stringVal_);
      } else if constexpr (Format == ExportFormat::Tsv) {
        writer.writeEscapedTsv(cell.stringVal_);
      } else {
        writer.writeEscapedTurtleLiteral(cell.stringVal_);
      }
    } else if constexpr (Type == ColumnType::Int) {
      writer.writeInteger(cell.intVal_);
    } else if constexpr (Type == ColumnType::Double) {
      writeFormattedDouble(writer, cell.doubleVal_);
    } else if constexpr (Type == ColumnType::BlankNode) {
      writer.writeRaw(cell.stringVal_);
    } else if constexpr (Type == ColumnType::Boolean) {
      writer.writeRaw(cell.boolVal_ ? "true" : "false");
    } else if constexpr (Type == ColumnType::String) {
      if constexpr (Format == ExportFormat::Csv) {
        writer.writeEscapedCsv(cell.stringVal_);
      } else if constexpr (Format == ExportFormat::Tsv) {
        writer.writeEscapedTsv(cell.stringVal_);
      } else {
        writer.writeRaw(cell.stringVal_);
      }
    } else {
      // ColumnType::Undefined
      if constexpr (Format == ExportFormat::Turtle || Format == ExportFormat::NTriples) {
        writer.writeRaw("UNDEF");
      }
    }
  }

  // Typed overload for raw string_view
  template <typename Writer>
  static void write(Writer& writer, std::string_view sv) {
    if constexpr (Type == ColumnType::Iri) {
      if constexpr (Format == ExportFormat::Csv) {
        writer.writeEscapedCsv(sv);
      } else if constexpr (Format == ExportFormat::Tsv) {
        writer.writeEscapedTsv(sv);
      } else {
        writer.writeIri(sv);
      }
    } else if constexpr (Type == ColumnType::Literal) {
      if constexpr (Format == ExportFormat::Csv) {
        writer.writeEscapedCsv(sv);
      } else if constexpr (Format == ExportFormat::Tsv) {
        writer.writeEscapedTsv(sv);
      } else {
        writer.writeEscapedTurtleLiteral(sv);
      }
    } else if constexpr (Type == ColumnType::BlankNode || Type == ColumnType::String) {
      if constexpr (Format == ExportFormat::Csv) {
        writer.writeEscapedCsv(sv);
      } else if constexpr (Format == ExportFormat::Tsv) {
        writer.writeEscapedTsv(sv);
      } else {
        writer.writeRaw(sv);
      }
    } else {
      writer.writeRaw(sv);
    }
  }

  // Typed overload for integral values
  template <typename Writer, typename T>
  requires std::is_integral_v<T>
  static void write(Writer& writer, T val) {
    if constexpr (Type == ColumnType::Boolean) {
      writer.writeRaw(val ? "true" : "false");
    } else {
      writer.writeInteger(val);
    }
  }

  // Typed overload for floating point values
  template <typename Writer, typename T>
  requires std::is_floating_point_v<T>
  static void write(Writer& writer, T val) {
    writeFormattedDouble(writer, static_cast<double>(val));
  }

  // Typed overload for EvaluatedTermData
  template <typename Writer>
  static void write(Writer& writer, const qlever::constructExport::EvaluatedTermData& term) {
    
  }
};

// Delimiter emitter helper.
template <ExportFormat Format, typename Writer>
inline void writeColumnDelimiter(Writer& writer) noexcept {
  if constexpr (Format == ExportFormat::Csv) {
    writer.writeChar(',');
  } else if constexpr (Format == ExportFormat::Tsv) {
    writer.writeChar('\t');
  } else if constexpr (Format == ExportFormat::Turtle || Format == ExportFormat::NTriples) {
    writer.writeChar(' ');
  }
}

// Row terminator emitter helper.
template <ExportFormat Format, typename Writer>
inline void writeRowTerminator(Writer& writer) noexcept {
  if constexpr (Format == ExportFormat::Turtle || Format == ExportFormat::NTriples) {
    writer.writeRaw(" .\n");
  } else {
    writer.writeChar('\n');
  }
}

}  // namespace detail

// _____________________________________________________________________________
// Dynamic per-cell serializer for arbitrary runtime schemas (polymorphic baseline).
// Contains runtime switch dispatch inside the per-cell loop.
class DynamicRowSerializer {
 private:
  std::vector<ColumnType> schema_;

 public:
  explicit DynamicRowSerializer(std::vector<ColumnType> schema)
      : schema_(std::move(schema)) {
    AD_CONTRACT_CHECK(!schema_.empty());
  }



  [[nodiscard]] const std::vector<ColumnType>& schema() const noexcept {
    return schema_;
  }

  [[nodiscard]] size_t numColumns() const noexcept {
    return schema_.size();
  }

  // Dynamic per-cell dispatch with inner loop branching
  template <ExportFormat Format, typename Writer>
  void serializeCell(Writer& writer, ColumnType type, const CellValue& cell) const {
    switch (type) {
      case ColumnType::Iri:
        detail::MonomorphicCellWriter<ColumnType::Iri, Format>::write(writer, cell);
        break;
      case ColumnType::Literal:
        detail::MonomorphicCellWriter<ColumnType::Literal, Format>::write(writer, cell);
        break;
      case ColumnType::Int:
        detail::MonomorphicCellWriter<ColumnType::Int, Format>::write(writer, cell);
        break;
      case ColumnType::Double:
        detail::MonomorphicCellWriter<ColumnType::Double, Format>::write(writer, cell);
        break;
      case ColumnType::BlankNode:
        detail::MonomorphicCellWriter<ColumnType::BlankNode, Format>::write(writer, cell);
        break;
      case ColumnType::Boolean:
        detail::MonomorphicCellWriter<ColumnType::Boolean, Format>::write(writer, cell);
        break;
      case ColumnType::String:
        detail::MonomorphicCellWriter<ColumnType::String, Format>::write(writer, cell);
        break;
      case ColumnType::Undefined:
        detail::MonomorphicCellWriter<ColumnType::Undefined, Format>::write(writer, cell);
        break;
    }
  }

  template <ExportFormat Format, typename Writer>
  void serializeRow(Writer& writer, ql::span<const CellValue> row) const {
    AD_CONTRACT_CHECK(row.size() >= schema_.size());
    for (size_t i = 0; i < schema_.size(); ++i) {
      if (i > 0) {
        detail::writeColumnDelimiter<Format>(writer);
      }
      serializeCell<Format>(writer, schema_[i], row[i]);
    }
    detail::writeRowTerminator<Format>(writer);
  }

  template <ExportFormat Format, typename Writer, typename RowContainer>
  size_t serializeBatch(Writer& writer, const RowContainer& rows) const {
    size_t count = 0;
    for (const auto& row : rows) {
      serializeRow<Format>(writer, row);
      ++count;
    }
    return count;
  }
};

// _____________________________________________________________________________
// MonomorphicRowSerializer<ColumnTypes...>:
// Compile-time specialized and loop-unrolled row serializer.
// Eliminates runtime branches for per-cell type dispatch, dynamic type switches, and virtual dispatch.
template <ColumnType... ColumnTypes>
class MonomorphicRowSerializer {
 public:
  static constexpr size_t NUM_COLUMNS = sizeof...(ColumnTypes);
  static constexpr std::array<ColumnType, NUM_COLUMNS> SCHEMA = {ColumnTypes...};

 private:
  // Helper to extract the `N`‑th column type.
  template <size_t Index>
  static constexpr ColumnType getColumnType() noexcept {
    return SCHEMA[Index];
  }

  // Compile-time unrolled cell serialization from variadic arguments.
  template <ExportFormat Format, typename Writer, size_t Index, typename FirstArg,
            typename... RestArgs>
  static void serializeVariadicCells(Writer& writer, const FirstArg& first,
                                     const RestArgs&... rest) {
    if constexpr (Index > 0) {
      detail::writeColumnDelimiter<Format>(writer);
    }
    detail::MonomorphicCellWriter<getColumnType<Index>(), Format>::write(writer, first);
    if constexpr (sizeof...(RestArgs) > 0) {
      serializeVariadicCells<Format, Writer, Index + 1>(writer, rest...);
    }
  }

  // Compile-time unrolled cell serialization from indexed span.
  template <ExportFormat Format, typename Writer, size_t... Is>
  static void serializeSpanCells(Writer& writer, ql::span<const CellValue> row,
                                 std::index_sequence<Is...>) {
    (
        [&]() {
          if constexpr (Is > 0) {
            detail::writeColumnDelimiter<Format>(writer);
          }
          detail::MonomorphicCellWriter<getColumnType<Is>(), Format>::write(writer, row[Is]);
        }(),
        ...);
  }

  // Compile-time unrolled cell serialization from tuple.
  template <ExportFormat Format, typename Writer, typename Tuple, size_t... Is>
  static void serializeTupleCells(Writer& writer, const Tuple& tuple,
                                  std::index_sequence<Is...>) {
    (
        [&]() {
          if constexpr (Is > 0) {
            detail::writeColumnDelimiter<Format>(writer);
          }
          detail::MonomorphicCellWriter<getColumnType<Is>(), Format>::write(writer,
                                                                   std::get<Is>(tuple));
        }(),
        ...);
  }

 public:
  [[nodiscard]] static constexpr size_t numColumns() noexcept { return NUM_COLUMNS; }
  [[nodiscard]] static constexpr const auto& schema() noexcept { return SCHEMA; }

  // ___________________________________________________________________________
  // Serialize a single row from variadic cell values (compile-time unrolled)
  template <ExportFormat Format, typename Writer, typename... CellArgs>
  static void serializeRow(Writer& writer, const CellArgs&... cells) {
    static_assert(sizeof...(CellArgs) == NUM_COLUMNS,
                  "MonomorphicRowSerializer argument count mismatch with schema");
    serializeVariadicCells<Format, Writer, 0>(writer, cells...);
    detail::writeRowTerminator<Format>(writer);
  }

  // ___________________________________________________________________________
  // Serialize a single row from span of CellValue (compile-time unrolled)
  template <ExportFormat Format, typename Writer>
  static void serializeRow(Writer& writer, ql::span<const CellValue> row) {
    AD_CONTRACT_CHECK(row.size() >= NUM_COLUMNS);
    serializeSpanCells<Format>(writer, row, std::make_index_sequence<NUM_COLUMNS>{});
    detail::writeRowTerminator<Format>(writer);
  }

  // ___________________________________________________________________________
  // Serialize a single row from std::tuple or std::array
  template <ExportFormat Format, typename Writer, typename Tuple>
  static void serializeRowTuple(Writer& writer, const Tuple& tuple) {
    static_assert(std::tuple_size_v<Tuple> == NUM_COLUMNS,
                  "Tuple size mismatch with monomorphic schema column count");
    serializeTupleCells<Format>(writer, tuple, std::make_index_sequence<NUM_COLUMNS>{});
    detail::writeRowTerminator<Format>(writer);
  }

  // ___________________________________________________________________________
  // High-throughput batch serialization over a collection of rows
  template <ExportFormat Format, typename Writer, typename RowContainer>
  static size_t serializeBatch(Writer& writer, const RowContainer& rows) {
    size_t count = 0;
    for (const auto& row : rows) {
      serializeRow<Format>(writer, row);
      ++count;
    }
    return count;
  }
};

// _____________________________________________________________________________
// Schema dispatch infrastructure:
// Maps a runtime schema (ql::span<const ColumnType>) to specialized
// compile-time MonomorphicRowSerializer<...> functions.

namespace detail {

// Fast-path dispatch for 1-column schemas.
template <typename Visitor, typename... Args>
decltype(auto) dispatch1Col(ColumnType c0, Visitor&& visitor, Args&&... args) {
  switch (c0) {
    case ColumnType::Iri:
      return visitor.template operator()<ColumnType::Iri>(std::forward<Args>(args)...);
    case ColumnType::Literal:
      return visitor.template operator()<ColumnType::Literal>(std::forward<Args>(args)...);
    case ColumnType::Int:
      return visitor.template operator()<ColumnType::Int>(std::forward<Args>(args)...);
    case ColumnType::Double:
      return visitor.template operator()<ColumnType::Double>(std::forward<Args>(args)...);
    case ColumnType::BlankNode:
      return visitor.template operator()<ColumnType::BlankNode>(std::forward<Args>(args)...);
    case ColumnType::Boolean:
      return visitor.template operator()<ColumnType::Boolean>(std::forward<Args>(args)...);
    case ColumnType::String:
    default:
      return visitor.template operator()<ColumnType::String>(std::forward<Args>(args)...);
  }
}

// Fast-path dispatch for 2-column schemas.
template <typename Visitor, typename... Args>
decltype(auto) dispatch2Col(ColumnType c0, ColumnType c1, Visitor&& visitor,
                            Args&&... args) {
  template <ColumnType C0>
  struct InnerDispatch {
    template <typename V, typename... VArgs>
    static decltype(auto) apply(V&& visitor, ColumnType c1, VArgs&&... args) {
      switch (c1) {
        case ColumnType::Iri:
          return visitor.template operator()<C0, ColumnType::Iri>(
              std::forward<VArgs>(args)...);
        case ColumnType::Literal:
          return visitor.template operator()<C0, ColumnType::Literal>(
              std::forward<VArgs>(args)...);
        case ColumnType::Int:
          return visitor.template operator()<C0, ColumnType::Int>(
              std::forward<VArgs>(args)...);
        case ColumnType::Double:
          return visitor.template operator()<C0, ColumnType::Double>(
              std::forward<VArgs>(args)...);
        case ColumnType::BlankNode:
          return visitor.template operator()<C0, ColumnType::BlankNode>(
              std::forward<VArgs>(args)...);
        case ColumnType::Boolean:
          return visitor.template operator()<C0, ColumnType::Boolean>(
              std::forward<VArgs>(args)...);
        case ColumnType::String:
        default:
          return visitor.template operator()<C0, ColumnType::String>(
              std::forward<VArgs>(args)...);
      }
    }
  };

  switch (c0) {
    case ColumnType::Iri:
      return InnerDispatch<ColumnType::Iri>::apply(std::forward<Visitor>(visitor), c1,
                                                   std::forward<Args>(args)...);
    case ColumnType::Literal:
      return InnerDispatch<ColumnType::Literal>::apply(std::forward<Visitor>(visitor), c1,
                                                       std::forward<Args>(args)...);
    case ColumnType::Int:
      return InnerDispatch<ColumnType::Int>::apply(std::forward<Visitor>(visitor), c1,
                                                   std::forward<Args>(args)...);
    case ColumnType::Double:
      return InnerDispatch<ColumnType::Double>::apply(std::forward<Visitor>(visitor), c1,
                                                      std::forward<Args>(args)...);
    case ColumnType::BlankNode:
      return InnerDispatch<ColumnType::BlankNode>::apply(std::forward<Visitor>(visitor), c1,
                                                         std::forward<Args>(args)...);
    case ColumnType::Boolean:
      return InnerDispatch<ColumnType::Boolean>::apply(std::forward<Visitor>(visitor), c1,
                                                       std::forward<Args>(args)...);
    case ColumnType::String:
    default:
      return InnerDispatch<ColumnType::String>::apply(std::forward<Visitor>(visitor), c1,
                                                       std::forward<Args>(args)...);
  }
}

// Hot 3-column schema registry and dispatch helper.
template <typename Visitor, typename... Args>
decltype(auto) dispatch3ColHot(size_t index, Visitor&& visitor, Args&&... args) {
  switch (index) {
    case 0:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Iri, ColumnType::Iri>(
          std::forward<Args>(args)...);
    case 1:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Iri, ColumnType::Literal>(
          std::forward<Args>(args)...);
    case 2:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Iri, ColumnType::Int>(
          std::forward<Args>(args)...);
    case 3:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Iri, ColumnType::Double>(
          std::forward<Args>(args)...);
    case 4:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Iri, ColumnType::BlankNode>(
          std::forward<Args>(args)...);
    case 5:
      return visitor.template operator()<ColumnType::BlankNode, ColumnType::Iri, ColumnType::Iri>(
          std::forward<Args>(args)...);
    case 6:
      return visitor.template operator()<ColumnType::BlankNode, ColumnType::Iri, ColumnType::Literal>(
          std::forward<Args>(args)...);
    case 7:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Literal, ColumnType::Int>(
          std::forward<Args>(args)...);
    case 8:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Literal, ColumnType::Double>(
          std::forward<Args>(args)...);
    case 9:
      return visitor.template operator()<ColumnType::Literal, ColumnType::Literal, ColumnType::Literal>(
          std::forward<Args>(args)...);
    case 10:
      return visitor.template operator()<ColumnType::Int, ColumnType::Int, ColumnType::Int>(
          std::forward<Args>(args)...);
    default:
      break;
  }

  // Unreachable for valid indices; defensive fallback.
  DynamicRowSerializer dynamicSerializer(
      {ColumnType::Undefined, ColumnType::Undefined, ColumnType::Undefined});
  return visitor(dynamicSerializer, std::forward<Args>(args)...);
}

// Fast-path dispatch for common 3-column SPARQL schemas.
template <typename Visitor, typename... Args>
decltype(auto) dispatch3Col(ColumnType c0, ColumnType c1, ColumnType c2,
                            Visitor&& visitor, Args&&... args) {
  static constexpr std::array<std::array<ColumnType, 3>, 11> kHot3ColSchemas = {{
    {{ColumnType::Iri, ColumnType::Iri, ColumnType::Iri}},
    {{ColumnType::Iri, ColumnType::Iri, ColumnType::Literal}},
    {{ColumnType::Iri, ColumnType::Iri, ColumnType::Int}},
    {{ColumnType::Iri, ColumnType::Iri, ColumnType::Double}},
    {{ColumnType::Iri, ColumnType::Iri, ColumnType::BlankNode}},
    {{ColumnType::BlankNode, ColumnType::Iri, ColumnType::Iri}},
    {{ColumnType::BlankNode, ColumnType::Iri, ColumnType::Literal}},
    {{ColumnType::Iri, ColumnType::Literal, ColumnType::Int}},
    {{ColumnType::Iri, ColumnType::Literal, ColumnType::Double}},
    {{ColumnType::Literal, ColumnType::Literal, ColumnType::Literal}},
    {{ColumnType::Int, ColumnType::Int, ColumnType::Int}}
  }};

  std::array<ColumnType, 3> key = {c0, c1, c2};
  for (size_t i = 0; i < kHot3ColSchemas.size(); ++i) {
    if (std::equal(key.begin(), key.end(), kHot3ColSchemas[i].begin())) {
      return dispatch3ColHot(i, std::forward<Visitor>(visitor),
                             std::forward<Args>(args)...);
    }
  }

  // Fallback to dynamic serializer for rare 3-column combinations.
  DynamicRowSerializer dynamicSerializer({c0, c1, c2});
  return visitor(dynamicSerializer, std::forward<Args>(args)...);
}

// Hot 4-column schema registry and dispatch helper.
template <typename Visitor, typename... Args>
decltype(auto) dispatch4ColHot(size_t index, Visitor&& visitor, Args&&... args) {
  switch (index) {
    case 0:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Iri, ColumnType::Iri,
                                         ColumnType::Iri>(std::forward<Args>(args)...);
    case 1:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Iri, ColumnType::Iri,
                                         ColumnType::Literal>(std::forward<Args>(args)...);
    case 2:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Literal, ColumnType::Int,
                                         ColumnType::Double>(std::forward<Args>(args)...);
    case 3:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Iri, ColumnType::Int,
                                         ColumnType::Double>(std::forward<Args>(args)...);
    case 4:
      return visitor.template operator()<ColumnType::Iri, ColumnType::Iri, ColumnType::Literal,
                                         ColumnType::Literal>(std::forward<Args>(args)...);
    default:
      break;
  }

  DynamicRowSerializer dynamicSerializer(
      {ColumnType::Undefined, ColumnType::Undefined, ColumnType::Undefined, ColumnType::Undefined});
  return visitor(dynamicSerializer, std::forward<Args>(args)...);
}

// Fast-path dispatch for 4-column schemas.
template <typename Visitor, typename... Args>
decltype(auto) dispatch4Col(ColumnType c0, ColumnType c1, ColumnType c2,
                            ColumnType c3, Visitor&& visitor, Args&&... args) {
  static constexpr std::array<std::array<ColumnType, 4>, 5> kHot4ColSchemas = {{
    {{ColumnType::Iri, ColumnType::Iri, ColumnType::Iri, ColumnType::Iri}},
    {{ColumnType::Iri, ColumnType::Iri, ColumnType::Iri, ColumnType::Literal}},
    {{ColumnType::Iri, ColumnType::Literal, ColumnType::Int, ColumnType::Double}},
    {{ColumnType::Iri, ColumnType::Iri, ColumnType::Int, ColumnType::Double}},
    {{ColumnType::Iri, ColumnType::Iri, ColumnType::Literal, ColumnType::Literal}}
  }};

  std::array<ColumnType, 4> key = {c0, c1, c2, c3};
  for (size_t i = 0; i < kHot4ColSchemas.size(); ++i) {
    if (std::equal(key.begin(), key.end(), kHot4ColSchemas[i].begin())) {
      return dispatch4ColHot(i, std::forward<Visitor>(visitor),
                             std::forward<Args>(args)...);
    }
  }

  // Fallback
  DynamicRowSerializer dynamicSerializer({c0, c1, c2, c3});
  return visitor(dynamicSerializer, std::forward<Args>(args)...);
}

}  // namespace detail

// _____________________________________________________________________________
// Dispatch to the fast-path monomorphic serializer.
// Inspects the runtime schema and invokes the visitor with column types as
// template parameters for the fast paths (1–4 columns), or with a
// `DynamicRowSerializer` instance for the fallback.
template <typename Visitor, typename... Args>
decltype(auto) dispatchMonomorphicSerializer(ql::span<const ColumnType> schema,
                                            Visitor&& visitor, Args&&... args) {
  AD_CONTRACT_CHECK(!schema.empty());

  switch (schema.size()) {
    case 1:
      return detail::dispatch1Col(schema[0], std::forward<Visitor>(visitor),
                                  std::forward<Args>(args)...);
    case 2:
      return detail::dispatch2Col(schema[0], schema[1], std::forward<Visitor>(visitor),
                                  std::forward<Args>(args)...);
    case 3:
      return detail::dispatch3Col(schema[0], schema[1], schema[2],
                                  std::forward<Visitor>(visitor),
                                  std::forward<Args>(args)...);
    case 4:
      return detail::dispatch4Col(schema[0], schema[1], schema[2], schema[3],
                                  std::forward<Visitor>(visitor),
                                  std::forward<Args>(args)...);
    default: {
      DynamicRowSerializer dynamicSerializer(
          std::vector<ColumnType>(schema.begin(), schema.end()));
      return visitor(dynamicSerializer, std::forward<Args>(args)...);
    }
  }
}

}  // namespace ql::serialization

#endif  // QLEVER_SRC_ENGINE_MONOMORPHICSERIALIZERS_H
