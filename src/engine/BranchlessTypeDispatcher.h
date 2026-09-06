
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BRANCHLESSTYPEDISPATCHER_H
#define QLEVER_SRC_ENGINE_BRANCHLESSTYPEDISPATCHER_H


#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "backports/span.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/ValueId.h"
#include "util/Invariants.h"

namespace ql::engine {

// Forward declarations
struct TypeFormatDescriptor;

// Function pointer signature for single-pass term formatting.
using TermFormatterFn = char* (*)(ValueId id, std::string_view rawTerm, char* out,
                                  std::string_view prefix,
                                  std::string_view suffix) noexcept;

// _____________________________________________________________________________
// Lookup table entry descriptor mapping a 4-bit Datatype tag to delimiters
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

// Fast branchless copy for terms with opening and closing delimiters.
inline char* formatTermWithDelimiters(ValueId, std::string_view rawTerm,
                                     char* out, std::string_view prefix,
                                     std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  std::memcpy(out, rawTerm.data(), rawTerm.size());
  out += rawTerm.size();
  std::memcpy(out, suffix.data(), suffix.size());
  out += suffix.size();
  return out;
}

// Fast branchless formatter for integer values.
inline char* formatInteger(ValueId id, std::string_view, char* out,
                           std::string_view prefix,
                           std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  auto [ptr, ec] = std::to_chars(out, out + 24, id.getInt());
  out = ptr;
  std::memcpy(out, suffix.data(), suffix.size());
  out += suffix.size();
  return out;
}

// Fast branchless formatter for double values.
inline char* formatDouble(ValueId id, std::string_view, char* out,
                          std::string_view prefix,
                          std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  auto [ptr, ec] = std::to_chars(out, out + 32, id.getDouble());
  out = ptr;
  std::memcpy(out, suffix.data(), suffix.size());
  out += suffix.size();
  return out;
}

// Branchless boolean lookup table.
inline constexpr std::array<std::string_view, 2> kBoolStrings{"false", "true"};

// Fast branchless formatter for boolean values.
inline char* formatBoolean(ValueId id, std::string_view, char* out,
                           std::string_view prefix,
                           std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  const std::string_view val = kBoolStrings[static_cast<size_t>(id.getBool())];
  std::memcpy(out, val.data(), val.size());
  

  std::memcpy(out, suffix.data(), suffix.size());
  out += suffix.size();
  return out;
}


  auto [str, type] = id.getDate().toStringAndType();
  std::memcpy(out, str.data(), str.size());
  out += str.size();
  std::memcpy(out, suffix.data(), suffix.size());
  
                            std::string_view suffix) noexcept {
  std::memcpy(out, prefix.data(), prefix.size());
  out += prefix.size();
  auto [str, type] = id.getGeoPoint().toStringAndType();
  

// No-op formatter for undefined or unmapped datatype slots.
inline char* formatUndefined(ValueId, std::string_view, char* out,
                             
  for (size_t i = 0; i < 16; ++i) {
    lut[i] = TypeFormatDescriptor{"", "", &formatUndefined};
  }

  
  lut[static_cast<size_t>(Datatype::Undefined)] =
      TypeFormatDescriptor{"", "", &formatUndefined};

  
  lut[static_cast<size_t>(Datatype::Bool)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#boolean>", &formatBoolean};

  
  lut[static_cast<size_t>(Datatype::Int)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#integer>", &formatInteger};

  
  lut[static_cast<size_t>(Datatype::Double)] = TypeFormatDescriptor{
      "\"", "\"^^<http://www.w3.org/2001/XMLSchema#double>", &formatDouble};

  
  lut[static_cast<size_t>(Datatype::VocabIndex)] =
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters};

  
  lut[static_cast<size_t>(Datatype::LocalVocabIndex)] =
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

// Builds the 16-entry lookup table for Turtle export (compact literals/numbers).
constexpr std::array<TypeFormatDescriptor, 16> makeTurtleLut() {
  std::array<TypeFormatDescriptor, 16> lut{};
  lut.fill(TypeFormatDescriptor{"", "", &formatUndefined});

  lut[static_cast<size_t>(Datatype::Undefined)] =
      TypeFormatDescriptor{"", "", &formatUndefined};
  lut[static_cast<size_t>(Datatype::Bool)] =
      
  lut[static_cast<size_t>(Datatype::LocalVocabIndex)] =
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
      

// Builds the 16-entry lookup table for raw vocabulary entries.
// For types using `formatTermWithDelimiters` (IRIs, literals), the raw term already contains delimiters.
// For other types, the value is serialized from the ValueId.
constexpr std::array<TypeFormatDescriptor, 16> makeRawVocabLut() {
    std::array<TypeFormatDescriptor, 16> lut = {
      TypeFormatDescriptor{"", "", &formatUndefined}, // Undefined
      TypeFormatDescriptor{"", "", &formatBoolean}, // Bool
      TypeFormatDescriptor{"", "", &formatInteger}, // Int
      TypeFormatDescriptor{"", "", &formatDouble}, // Double
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters}, // VocabIndex
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters}, // LocalVocabIndex
      TypeFormatDescriptor{"\"", "\"", &formatTermWithDelimiters}, // TextRecordIndex
      TypeFormatDescriptor{"\"", "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", &formatDate}, // Date
      TypeFormatDescriptor{"\"", "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>", &formatGeoPoint}, // GeoPoint
      TypeFormatDescriptor{"\"", "\"", &formatTermWithDelimiters}, // WordVocabIndex
      TypeFormatDescriptor{"_:bn", "", &formatBlankNode}, // BlankNodeIndex
      TypeFormatDescriptor{"<", ">", &formatTermWithDelimiters}, // EncodedVal
      TypeFormatDescriptor{"", "", &formatUndefined}, // 12
      TypeFormatDescriptor{"", "", &formatUndefined}, // 13
      TypeFormatDescriptor{"", "", &formatUndefined}, // 14
      TypeFormatDescriptor{"", "", &formatUndefined} // 15
  };
  lut[static_cast<size_t>(Datatype::Bool)] =
      TypeFormatDescriptor{"", "", &formatBoolean};
  lut[static_cast<size_t>(Datatype::Int)] =
      TypeFormatDescriptor{"", "", &formatInteger};
  lut[static_cast<size_t>(Datatype::Double)] =
      
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

  
inline constexpr auto kTurtleTypeFormatLut = detail::makeTurtleLut();
inline constexpr auto kRawVocabTypeFormatLut = detail::makeRawVocabLut();

// _____________________________________________________________________________
// Deep branchless type dispatcher module.
//

// into a 16-entry constexpr lookup table using the 4-bit ValueId datatype tag.
class BranchlessTypeDispatcher {
 public:
  using LookupTable = std::array<TypeFormatDescriptor, 16>;

  // ___________________________________________________________________________
  // Format a single RDF term branchlessly into `out`.
  // Precondition: `out` must point to sufficient pre-allocated memory.
  // Return: Pointer past the last byte written.
  static inline char* dispatchTermFormat(
      ValueId id, std::string_view rawTerm, char* out,
      const LookupTable& lut = kDefaultTypeFormatLut) noexcept {
    AD_CONTRACT_CHECK(out != nullptr);
    const uint8_t typeTag =
        static_cast<uint8_t>(id.getBits() >> ValueId::numDataBits) & 0x0F;
    const auto& desc = lut[typeTag];
    AD_CONTRACT_CHECK(desc.formatFn_ != nullptr);
    return desc.formatFn_(id, rawTerm, out, desc.prefix_, desc.suffix_);
  }

  // ___________________________________________________________________________
    // Batch format a contiguous slice of terms branchlessly.
  // Preconditions: `ids` and `rawTerms` must have identical lengths, and `out`
  // must point to sufficient pre-allocated memory and be non-null unless `ids` is empty.
  // Return: Total number of bytes written.
  static inline size_t dispatchBatchTermFormat(
      ql::span<const ValueId> ids, ql::span<const std::string_view> rawTerms,
      char* out, const LookupTable& lut = kDefaultTypeFormatLut) noexcept {
    AD_CONTRACT_CHECK(ids.size() == rawTerms.size());
    AD_CONTRACT_CHECK(out != nullptr || ids.empty());

    char* curr = out;
    const size_t numTerms = ids.size();
    for (size_t i = 0; i < numTerms; ++i) {
      curr = dispatchTermFormat(ids[i], rawTerms[i], curr, lut);
    }
    return static_cast<size_t>(curr - out);
  }

  // Access to built-in lookup tables.
  [[nodiscard]] static constexpr const LookupTable& defaultLut() noexcept {
    // Return constexpr reference to default N-Triples format lookup table.
    return kDefaultTypeFormatLut;
  }

  [[nodiscard]] static constexpr const LookupTable& turtleLookupTable() noexcept {
    // Return constexpr reference to Turtle compact format lookup table.
    return kTurtleTypeFormatLut;
  }

  [[nodiscard]] static constexpr const LookupTable& rawVocabLut() noexcept {
    return kRawVocabTypeFormatLut;
  }
};

}  // namespace ql::engine

#endif  // QLEVER_SRC_ENGINE_BRANCHLESSTYPEDISPATCHER_H
