// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BRANCHLESSTYPEDISPATCHER_H
#define QLEVER_SRC_ENGINE_BRANCHLESSTYPEDISPATCHER_H

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string_view>
#include <system_error>

#include "backports/span.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/ValueId.h"
#include "util/Exception.h"

namespace ql::engine {

// Forward declarations
struct TypeFormatDescriptor;

// Function pointer signature for single-pass term formatting. A formatter
// writes `prefix`, the value of `id` (or `rawTerm`) and `suffix` to
// `[out, last)` and returns the one-past-the-end pointer of what it wrote, or
// `nullptr` if the output does not fit into `[out, last)`.
using TermFormatterFn = char* (*)(ValueId id, std::string_view rawTerm,
                                  char* out, char* last,
                                  std::string_view prefix,
                                  std::string_view suffix);

// _____________________________________________________________________________
// 16-entry lookup table descriptor mapping a 4-bit Datatype tag to delimiters
// and a fast, branchless formatting function pointer.
struct TypeFormatDescriptor {
  std::string_view prefix_{""};
  std::string_view suffix_{""};
  TermFormatterFn formatFn_{nullptr};

  constexpr TypeFormatDescriptor() = default;

  constexpr TypeFormatDescriptor(std::string_view prefix,
                                 std::string_view suffix,
                                 TermFormatterFn formatFn) noexcept
      : prefix_{prefix}, suffix_{suffix}, formatFn_{formatFn} {}
};

namespace detail {

// Copy `bytes` to `out` if they fit into `[out, last)`. Return the
// one-past-the-end pointer of the copy, or `nullptr` if they do not fit.
inline char* append(char* out, char* last, std::string_view bytes) noexcept {
  if (out == nullptr || static_cast<size_t>(last - out) < bytes.size()) {
    return nullptr;
  }
  std::memcpy(out, bytes.data(), bytes.size());
  return out + bytes.size();
}

// Format a double into `[out, last)`, returning the one-past-the-end pointer,
// or `nullptr` if it does not fit. Floating-point `std::to_chars` is only
// available on macOS 13.3 and later, but QLever still targets macOS 11.0, so
// fall back to `snprintf` on Apple platforms.
inline char* formatDoubleValue(char* out, char* last, double value) noexcept {
  if (out == nullptr) {
    return nullptr;
  }
#if defined(__APPLE__)
  const auto capacity = static_cast<size_t>(last - out);
  const int numChars = std::snprintf(out, capacity, "%.17g", value);
  // `snprintf` returns the length the output would have had; it was
  // truncated if that length does not leave room for the terminating null.
  if (numChars < 0 || static_cast<size_t>(numChars) >= capacity) {
    return nullptr;
  }
  return out + numChars;
#else
  auto [ptr, ec] = std::to_chars(out, last, value);
  return ec == std::errc{} ? ptr : nullptr;
#endif
}

// Format an integer into `[out, last)`, returning the one-past-the-end
// pointer, or `nullptr` if it does not fit.
template <typename Int>
char* formatIntegerValue(char* out, char* last, Int value) noexcept {
  if (out == nullptr) {
    return nullptr;
  }
  auto [ptr, ec] = std::to_chars(out, last, value);
  return ec == std::errc{} ? ptr : nullptr;
}

// Fast branchless copy for terms with opening and closing delimiters.
inline char* formatTermWithDelimiters(ValueId, std::string_view rawTerm,
                                      char* out, char* last,
                                      std::string_view prefix,
                                      std::string_view suffix) {
  out = append(out, last, prefix);
  out = append(out, last, rawTerm);
  return append(out, last, suffix);
}

// Fast branchless formatter for integer values.
inline char* formatInteger(ValueId id, std::string_view, char* out, char* last,
                           std::string_view prefix, std::string_view suffix) {
  out = append(out, last, prefix);
  out = formatIntegerValue(out, last, id.getInt());
  return append(out, last, suffix);
}

// Fast branchless formatter for double values.
inline char* formatDouble(ValueId id, std::string_view, char* out, char* last,
                          std::string_view prefix, std::string_view suffix) {
  out = append(out, last, prefix);
  out = formatDoubleValue(out, last, id.getDouble());
  return append(out, last, suffix);
}

// Branchless boolean lookup table.
inline constexpr std::array<std::string_view, 2> kBoolStrings{"false", "true"};

// Fast branchless formatter for boolean values.
inline char* formatBoolean(ValueId id, std::string_view, char* out, char* last,
                           std::string_view prefix, std::string_view suffix) {
  out = append(out, last, prefix);
  out = append(out, last, kBoolStrings[static_cast<size_t>(id.getBool())]);
  return append(out, last, suffix);
}

// Fast branchless formatter for blank node indices.
inline char* formatBlankNode(ValueId id, std::string_view, char* out,
                             char* last, std::string_view prefix,
                             std::string_view suffix) {
  out = append(out, last, prefix);
  out = formatIntegerValue(out, last, id.getBlankNodeIndex().get());
  return append(out, last, suffix);
}

// Formatter for date values. The date is formatted into a temporary string
// first (`Date::toStringAndType`), so this may allocate.
inline char* formatDate(ValueId id, std::string_view, char* out, char* last,
                        std::string_view prefix, std::string_view suffix) {
  out = append(out, last, prefix);
  out = append(out, last, id.getDate().toStringAndType().first);
  return append(out, last, suffix);
}

// Formatter for GeoPoint values. The point is formatted into a temporary
// string first (`GeoPoint::toStringAndType`), so this may allocate.
inline char* formatGeoPoint(ValueId id, std::string_view, char* out, char* last,
                            std::string_view prefix, std::string_view suffix) {
  out = append(out, last, prefix);
  out = append(out, last, id.getGeoPoint().toStringAndType().first);
  return append(out, last, suffix);
}

// Formatter for `Datatype::Undefined`, which is exported as the empty string.
inline char* formatUndefined(ValueId, std::string_view, char* out, char*,
                             std::string_view, std::string_view) {
  return out;
}

// Builds the default 16-entry lookup table for standard RDF N-Triples export.
constexpr std::array<TypeFormatDescriptor, 16> makeDefaultLut() {
  // The slots without a `Datatype` keep `formatFn_ == nullptr`, which
  // `dispatchTermFormat` rejects.
  std::array<TypeFormatDescriptor, 16> lut{};

  // 0: Undefined
  lut[static_cast<size_t>(Datatype::Undefined)] =
      TypeFormatDescriptor{"", "", &formatUndefined};

  // 1: Bool
  lut[static_cast<size_t>(Datatype::Bool)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#boolean>", &formatBoolean};

  // 2: Int
  lut[static_cast<size_t>(Datatype::Int)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#integer>", &formatInteger};

  // 3: Double
  lut[static_cast<size_t>(Datatype::Double)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#double>", &formatDouble};

  // VocabIndex (IRI by default when formatting raw names)
  lut[static_cast<size_t>(Datatype::VocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};

  // LocalVocabIndex
  lut[static_cast<size_t>(Datatype::LocalVocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};

  // SecondaryVocabIndex: same vocabulary-term treatment as the main
  // vocabularies (an unmapped slot would be rejected by the dispatcher).
  lut[static_cast<size_t>(Datatype::SecondaryVocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};

  // TextRecordIndex
  lut[static_cast<size_t>(Datatype::TextRecordIndex)] =
      TypeFormatDescriptor{"\"", "\"", &formatTermWithDelimiters};

  // Date
  lut[static_cast<size_t>(Datatype::Date)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", &formatDate};

  // GeoPoint
  lut[static_cast<size_t>(Datatype::GeoPoint)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      &formatGeoPoint};

  // WordVocabIndex
  lut[static_cast<size_t>(Datatype::WordVocabIndex)] =
      TypeFormatDescriptor{"\"", "\"", &formatTermWithDelimiters};

  // BlankNodeIndex
  lut[static_cast<size_t>(Datatype::BlankNodeIndex)] =
      TypeFormatDescriptor{"_:bn", "", &formatBlankNode};

  // EncodedVal
  lut[static_cast<size_t>(Datatype::EncodedVal)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};

  return lut;
}

// Builds the 16-entry lookup table for Turtle export (compact
// literals/numbers).
constexpr std::array<TypeFormatDescriptor, 16> makeTurtleLut() {
  // The slots without a `Datatype` keep `formatFn_ == nullptr`, which
  // `dispatchTermFormat` rejects.
  std::array<TypeFormatDescriptor, 16> lut{};

  lut[static_cast<size_t>(Datatype::Undefined)] =
      TypeFormatDescriptor{"", "", &formatUndefined};
  lut[static_cast<size_t>(Datatype::Bool)] =
      TypeFormatDescriptor{"", "", &formatBoolean};
  lut[static_cast<size_t>(Datatype::Int)] =
      TypeFormatDescriptor{"", "", &formatInteger};
  lut[static_cast<size_t>(Datatype::Double)] =
      TypeFormatDescriptor{"", "", &formatDouble};
  lut[static_cast<size_t>(Datatype::VocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::LocalVocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::SecondaryVocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::TextRecordIndex)] =
      TypeFormatDescriptor{"\"", "\"", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::Date)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", &formatDate};
  lut[static_cast<size_t>(Datatype::GeoPoint)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      &formatGeoPoint};
  lut[static_cast<size_t>(Datatype::WordVocabIndex)] =
      TypeFormatDescriptor{"\"", "\"", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::BlankNodeIndex)] =
      TypeFormatDescriptor{"_:bn", "", &formatBlankNode};
  lut[static_cast<size_t>(Datatype::EncodedVal)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};

  return lut;
}

// Builds the 16-entry lookup table for raw vocabulary entries (where terms
// already contain quotes/delimiters).
constexpr std::array<TypeFormatDescriptor, 16> makeRawVocabLut() {
  // The slots without a `Datatype` keep `formatFn_ == nullptr`, which
  // `dispatchTermFormat` rejects.
  std::array<TypeFormatDescriptor, 16> lut{};

  lut[static_cast<size_t>(Datatype::Undefined)] =
      TypeFormatDescriptor{"", "", &formatUndefined};
  lut[static_cast<size_t>(Datatype::Bool)] =
      TypeFormatDescriptor{"", "", &formatBoolean};
  lut[static_cast<size_t>(Datatype::Int)] =
      TypeFormatDescriptor{"", "", &formatInteger};
  lut[static_cast<size_t>(Datatype::Double)] =
      TypeFormatDescriptor{"", "", &formatDouble};
  lut[static_cast<size_t>(Datatype::VocabIndex)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::LocalVocabIndex)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::SecondaryVocabIndex)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::TextRecordIndex)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::Date)] =
      TypeFormatDescriptor{"", "", &formatDate};
  lut[static_cast<size_t>(Datatype::GeoPoint)] =
      TypeFormatDescriptor{"", "", &formatGeoPoint};
  lut[static_cast<size_t>(Datatype::WordVocabIndex)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};
  lut[static_cast<size_t>(Datatype::BlankNodeIndex)] =
      TypeFormatDescriptor{"_:bn", "", &formatBlankNode};
  lut[static_cast<size_t>(Datatype::EncodedVal)] =
      TypeFormatDescriptor{"", "", &formatTermWithDelimiters};

  return lut;
}

}  // namespace detail

// Global constexpr lookup tables.
inline constexpr auto kDefaultTypeFormatLut = detail::makeDefaultLut();
inline constexpr auto kTurtleTypeFormatLut = detail::makeTurtleLut();
inline constexpr auto kRawVocabTypeFormatLut = detail::makeRawVocabLut();

// _____________________________________________________________________________
// Deep branchless type dispatcher module.
//
// Eliminates branch mispredictions during export loops by indexing directly
// into a 16-entry constexpr lookup table using the 4-bit ValueId datatype tag.
class BranchlessTypeDispatcher {
 public:
  using LookupTable = std::array<TypeFormatDescriptor, 16>;

  // ___________________________________________________________________________
  // Format a single RDF term branchlessly into `out` and return the number of
  // bytes written. Throw if `out` is too small or if the datatype of `id` has
  // no entry in `lut`.
  static inline size_t dispatchTermFormat(
      ValueId id, std::string_view rawTerm, ql::span<char> out,
      const LookupTable& lut = kDefaultTypeFormatLut) {
    const uint8_t typeTag =
        static_cast<uint8_t>(id.getBits() >> ValueId::numDataBits) & 0x0F;
    const auto& desc = lut[typeTag];
    AD_CONTRACT_CHECK(desc.formatFn_ != nullptr,
                      "The lookup table has no formatter for this datatype");
    char* const end =
        desc.formatFn_(id, rawTerm, out.data(), out.data() + out.size(),
                       desc.prefix_, desc.suffix_);
    AD_CONTRACT_CHECK(end != nullptr,
                      "The output buffer is too small for the formatted term");
    return static_cast<size_t>(end - out.data());
  }

  // ___________________________________________________________________________
  // Format the terms of `ids` (with the corresponding `rawTerms`) one after the
  // other into `out` and return the total number of bytes written. Throw if
  // the sizes of `ids` and `rawTerms` differ or if `out` is too small.
  static inline size_t dispatchBatchTermFormat(
      ql::span<const ValueId> ids, ql::span<const std::string_view> rawTerms,
      ql::span<char> out, const LookupTable& lut = kDefaultTypeFormatLut) {
    AD_CONTRACT_CHECK(ids.size() == rawTerms.size());
    size_t numBytes = 0;
    for (size_t i = 0; i < ids.size(); ++i) {
      numBytes +=
          dispatchTermFormat(ids[i], rawTerms[i], out.subspan(numBytes), lut);
    }
    return numBytes;
  }

  // Access to built-in lookup tables.
  [[nodiscard]] static constexpr const LookupTable& defaultLut() noexcept {
    return kDefaultTypeFormatLut;
  }

  [[nodiscard]] static constexpr const LookupTable& turtleLut() noexcept {
    return kTurtleTypeFormatLut;
  }

  [[nodiscard]] static constexpr const LookupTable& rawVocabLut() noexcept {
    return kRawVocabTypeFormatLut;
  }
};

}  // namespace ql::engine

#endif  // QLEVER_SRC_ENGINE_BRANCHLESSTYPEDISPATCHER_H
