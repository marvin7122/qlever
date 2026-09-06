// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// Note: UFR stands for University of Freiburg, Chair of Algorithms and Data Structures
//
// State that this file may be used only under the Apache 2.0 License,
// Direct the reader to the LICENSE file in the project root for details.

#ifndef QLEVER_SRC_ENGINE_SIMDESCAPECLASSIFIER_H
#define QLEVER_SRC_ENGINE_SIMDESCAPECLASSIFIER_H

#include <bit>
#include <cstddef>
#include <cstdint>

#include <string>
#include <string_view>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#include <immintrin.h>
#define QLEVER_SIMD_X86 1
#endif

#if defined(__GNUC__) || defined(__clang__)
#define QLEVER_AVX2_TARGET __attribute__((target("avx2")))
#define QLEVER_SSE2_TARGET __attribute__((target("sse2")))
#else
#define QLEVER_AVX2_TARGET
#define QLEVER_SSE2_TARGET
#endif

#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"
#include "util/Invariants.h"

// SimdEscapeClassifier facade: provides SIMD‑accelerated escape detection and
// serialization for multiple formats (CSV, TSV, Turtle, XML).
// Dispatches to AVX2, SSE2, or scalar implementations based on compile‑time ISA.
// Uses zero‑escape fast path: when a 32‑byte chunk mask is zero, copies raw bytes
// with a single memcpy, avoiding per‑character checks.
namespace ad_utility::simd {

// List supported serialization formats for SIMD escape classification.
enum class EscapeFormat {
  CsvQuote,    // Escape quotes (")
  CsvSpecial,  // RFC 4180 special characters (", ,, \r, \n)
  Tsv,         // IANA-TSV special characters (\t, \n, \r, \\)
  Turtle,      // Turtle / N-Triples literal content escapes (", \\, \n, \r)
  Xml          // XML special characters (&, <, >, ", ')
};

// Describe an invariant-bearing 32-bit classification mask for a 32-byte SIMD chunk.
class ChunkEscapeMask32 {
 private:
  uint32_t mask_ = 0;

 public:
  constexpr explicit ChunkEscapeMask32(uint32_t mask = 0) noexcept
      : mask_{mask} {}

  [[nodiscard]] constexpr bool hasEscape() const noexcept { return mask_ != 0; }
  [[nodiscard]] constexpr bool isAllClean() const noexcept { return mask_ == 0; }
  [[nodiscard]] constexpr uint32_t rawMask() const noexcept { return mask_; }

  [[nodiscard]] constexpr uint32_t firstEscapeIndex() const noexcept {
    return mask_ == 0 ? 32u : static_cast<uint32_t>(std::countr_zero(mask_));
  }

  [[nodiscard]] constexpr uint32_t countEscapes() const noexcept {
    return static_cast<uint32_t>(std::popcount(mask_));
  }
};

// Describe an invariant-bearing 16-bit classification mask for a 16-byte SIMD chunk.
class ChunkEscapeMask16 {
 private:
  uint16_t mask_ = 0;

 public:
  constexpr explicit ChunkEscapeMask16(uint16_t mask = 0) noexcept
      : mask_{mask} {}

  [[nodiscard]] constexpr bool hasEscape() const noexcept { return mask_ != 0; }
  [[nodiscard]] constexpr bool isAllClean() const noexcept { return mask_ == 0; }
  [[nodiscard]] constexpr uint16_t rawMask() const noexcept { return mask_; }

  // Return the index of the first escape character, or 16 if none.
  [[nodiscard]] constexpr uint32_t firstEscapeIndex() const noexcept {
    return mask_ == 0 ? 16u : static_cast<uint32_t>(std::countr_zero(mask_));
  }

  [[nodiscard]] constexpr uint32_t countEscapes() const noexcept {
    return static_cast<uint32_t>(std::popcount(mask_));
  }
};

namespace detail {

// Check whether a single character is an escape character.
template <EscapeFormat Format>
[[nodiscard]] constexpr inline bool isEscapeChar(char c) noexcept {
  if constexpr (Format == EscapeFormat::Turtle) {
    return c == '"' || c == '\\' || c == '\n' || c == '\r';
  } else if constexpr (Format == EscapeFormat::Tsv) {
    return c == '\t' || c == '\n' || c == '\r' || c == '\\';
  } else if constexpr (Format == EscapeFormat::CsvQuote) {
    return c == '"';
  } else if constexpr (Format == EscapeFormat::CsvSpecial) {
    return c == '"' || c == ',' || c == '\r' || c == '\n';
  } else if constexpr (Format == EscapeFormat::Xml) {
    return c == '&' || c == '<' || c == '>' || c == '"' || c == '\'';
  }
}

// Writes the escaped representation of character `c` into `dest` and returns
// the updated output pointer.
template <EscapeFormat Format>
inline char* emitEscape(char c, char* dest) noexcept {
  if constexpr (Format == EscapeFormat::Turtle) {
    if (c == '"') {
      dest[0] = '\\';
      dest[1] = '"';
      return dest + 2;
    } else if (c == '\\') {
      dest[0] = '\\';
      dest[1] = '\\';
      return dest + 2;
    } else if (c == '\n') {
      dest[0] = '\\';
      dest[1] = 'n';
      return dest + 2;
    } else if (c == '\r') {
      dest[0] = '\\';
      dest[1] = 'r';
      return dest + 2;
    }
    *dest = c;
    return dest + 1;
  } else if constexpr (Format == EscapeFormat::Tsv) {
    if (c == '\t') {
      *dest = ' ';
      return dest + 1;
    } else if (c == '\n') {
      dest[0] = '\\';
      dest[1] = 'n';
      return dest + 2;
    } else if (c == '\r') {
      dest[0] = '\\';
      dest[1] = 'r';
      return dest + 2;
    } else if (c == '\\') {
      dest[0] = '\\';
      dest[1] = '\\';
      return dest + 2;
    }
    
      return dest + 2;
    }
    *dest = c;
    return dest + 1;
  } else if constexpr (Format == EscapeFormat::Xml) {
    if (c == '&') {
      std::memcpy(dest, "&amp;", 5);
      return dest + 5;
    } else if (c == '<') {
      
      return dest + 6;
    } else if (c == '\'') {
      std::memcpy(dest, "&apos;", 6);
      

#if defined(QLEVER_SIMD_X86)

// Perform AVX2 32-byte vector classification.

    __m256i m3 = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\n'));
    __m256i m4 = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\r'));
    __m256i match =
        _mm256_or_si256(_mm256_or_si256(m1, m2), _mm256_or_si256(m3, m4));
    return static_cast<uint32_t>(_mm256_movemask_epi8(match));
  } else if constexpr (Format == EscapeFormat::Tsv) {
    __m256i m1 = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\t'));
    __m256i m2 = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\n'));
    __m256i m3 = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\r'));
    __m256i m4 = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\\'));
    __m256i match =
        _mm256_or_si256(_mm256_or_si256(m1, m2), _mm256_or_si256(m3, m4));
    return static_cast<uint32_t>(_mm256_movemask_epi8(match));
  
    __m256i m4 = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\r'));
    __m256i match =
        _mm256_or_si256(_mm256_or_si256(m1, m2), _mm256_or_si256(m3, m4));
    return static_cast<uint32_t>(_mm256_movemask_epi8(match));
  } else if constexpr (Format == EscapeFormat::Xml) {
    __m256i m1 = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('&'));
    
    return static_cast<uint32_t>(_mm256_movemask_epi8(match));
  }
}

// Perform SSE2 16-byte vector classification.
template <EscapeFormat Format>
QLEVER_SSE2_TARGET [[nodiscard]] inline uint16_t scanChunk16Sse2(
    const char* data) noexcept {
  __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data));
  if constexpr (Format == EscapeFormat::Turtle) {
    __m128i m1 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('"'));
    __m128i m2 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('\\'));
    __m128i m3 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('\n'));
    __m128i m4 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('\r'));
    __m128i match =
        _mm_or_si128(_mm_or_si128(m1, m2), _mm_or_si128(m3, m4));
    return static_cast<uint16_t>(_mm_movemask_epi8(match));
  } else if constexpr (Format == EscapeFormat::Tsv) {
    __m128i m1 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('\t'));
    __m128i m2 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('\n'));
    __m128i m3 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('\r'));
    __m128i m4 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('\\'));
    __m128i match =
        _mm_or_si128(_mm_or_si128(m1, m2), _mm_or_si128(m3, m4));
    return static_cast<uint16_t>(_mm_movemask_epi8(match));
  } else if constexpr (Format == EscapeFormat::CsvQuote) {
    __m128i match = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('"'));
    return static_cast<uint16_t>(_mm_movemask_epi8(match));
  } else if constexpr (Format == EscapeFormat::CsvSpecial) {
    __m128i m1 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('"'));
    
    __m128i m1 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('&'));
    __m128i m2 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('<'));
    __m128i m3 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('>'));
    __m128i m4 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('"'));
    __m128i m5 = _mm_cmpeq_epi8(chunk, _mm_set1_epi8('\''));
    __m128i match = _mm_or_si128(
        _mm_or_si128(_mm_or_si128(m1, m2), _mm_or_si128(m3, m4)), m5);
    return static_cast<uint16_t>(_mm_movemask_epi8(match));
  }
}

#endif  // QLEVER_SIMD_X86

template<std::size_t Bits, EscapeFormat Format>
[[nodiscard]] inline auto scanChunkScalar(const char* data) noexcept {
  using MaskType = std::conditional_t<Bits == 16, uint16_t, uint32_t>;
  MaskType mask = 0;
  for (std::size_t i = 0; i < Bits; ++i) {
    if (isEscapeChar<Format>(data[i])) {
      mask |= (MaskType(1) << i);
    }
  }
  return mask;
}

// Convenience aliases
template<EscapeFormat Format>
[[nodiscard]] inline uint32_t scanChunk32Scalar(const char* data) noexcept {
  return scanChunkScalar<32, Format>(data);
}

template<EscapeFormat Format>
[[nodiscard]] inline uint16_t scanChunk16Scalar(const char* data) noexcept {
  return scanChunkScalar<16, Format>(data);
}

}  // namespace detail


  // Classify a 32-byte unaligned memory slice and return a 32-bit bitmask.
  template <EscapeFormat Format = EscapeFormat::Turtle>
  [[nodiscard]] static inline ChunkEscapeMask32 scanChunk32(
      

  // ___________________________________________________________________________
  // Classify a 16-byte unaligned memory slice and return a 16-bit bitmask.
  template <EscapeFormat Format = EscapeFormat::Turtle>
  [[nodiscard]] static inline ChunkEscapeMask16 scanChunk16(
      const char* data) noexcept {
#if defined(QLEVER_SIMD_X86)
    return ChunkEscapeMask16{detail::scanChunk16Sse2<Format>(data)};
#else
    return ChunkEscapeMask16{detail::scanChunk16Scalar<Format>(data)};
#endif
  }

  // ___________________________________________________________________________
  // Check whether a single character requires escaping in the specified format.
  // Returns true if the character @p c is an escape character for the given @p Format.
  template <EscapeFormat Format = EscapeFormat::Turtle>
  [[nodiscard]] constexpr static inline bool isEscapeChar(char c) noexcept {
    return detail::isEscapeChar<Format>(c);
  }

  // ___________________________________________________________________________
  // Scan an arbitrary string slice for escape characters using vectorized SIMD.
  // Returns the index of the first escape character, or std::string_view::npos.
  template <EscapeFormat Format = EscapeFormat::Turtle>
  /// \brief Returns the index of the first escape character in @p text, or std::string_view::npos if none.
  /// \tparam Format The escape format to use (defaults to Turtle).
  /// \param text The input string view to scan.
  /// \return Index of first escape character, or std::string_view::npos.
  [[nodiscard]] static inline size_t findFirstEscape(
      std::string_view text) noexcept {
    const char* ptr = text.data();
    size_t len = text.size();
    size_t offset = 0;

#if defined(QLEVER_SIMD_X86)
    // Fast path: 32-byte AVX2 vector blocks
    while (len >= 32) {
      ChunkEscapeMask32 mask = scanChunk32<Format>(ptr);
      if (mask.hasEscape()) {
        return offset + mask.firstEscapeIndex();
      }
      ptr += 32;
      len -= 32;
      offset += 32;
    }

    // Secondary path: 16-byte SSE2 vector block
    if (len >= 16) {
      ChunkEscapeMask16 mask = scanChunk16<Format>(ptr);
      if (mask.hasEscape()) {
        return offset + mask.firstEscapeIndex();
      }
      ptr += 16;
      len -= 16;
      offset += 16;
    }
#endif

    // Tail: 0 to 15 remaining bytes
    while (len > 0) {
      if (detail::isEscapeChar<Format>(*ptr)) {
        return offset;
      }
      ++ptr;
      --len;
      ++offset;
    }
    return std::string_view::npos;
  }

  // ___________________________________________________________________________
  // Check whether `text` contains any characters requiring escaping.
  template <EscapeFormat Format = EscapeFormat::Turtle>
  [[nodiscard]] static inline bool hasEscapes(std::string_view text) noexcept {
    return findFirstEscape<Format>(text) != std::string_view::npos;
  }

  // ___________________________________________________________________________
  // Scan `text` and populate chunk masks into a caller-provided span.
  // Returns the number of chunk masks produced.
  template <EscapeFormat Format = EscapeFormat::Turtle>
  static inline size_t scanForEscapes(std::string_view text,
                                      ql::span<uint32_t> outMasks) noexcept {
    const char* ptr = text.data();
    size_t len = text.size();
    size_t maskIdx = 0;

    while (len >= 32 && maskIdx < outMasks.size()) {
      outMasks[maskIdx++] = scanChunk32<Format>(ptr).rawMask();
      ptr += 32;
      len -= 32;
    }
    if (len > 0 && maskIdx < outMasks.size()) {
      uint32_t tailMask = 0;
      for (size_t i = 0; i < len; ++i) {
        if (detail::isEscapeChar<Format>(ptr[i])) {
          tailMask |= (1u << i);
        }
      }
      outMasks[maskIdx++] = tailMask;
    }
    return maskIdx;
  }

  // ___________________________________________________________________________
  // Scan `text` and return a vector of 32-bit chunk classification masks.
  template <EscapeFormat Format = EscapeFormat::Turtle>
  [[nodiscard]] static inline std::vector<uint32_t> scanForEscapes(
      std::string_view text) {
    size_t numChunks = (text.size() + 31) / 32;
    std::vector<uint32_t> masks(numChunks);
    scanForEscapes<Format>(text, ql::span<uint32_t>{masks.data(), masks.size()});
    return masks;
  }

  // ___________________________________________________________________________
  // High-performance branchless copier and escape serializer.
  // Fast path copies 32-byte chunks with zero per-character checks when mask is 0.
  // Returns pointer past the last written byte in `dest`.
  template <EscapeFormat Format>
  static inline char* copyAndEscape(std::string_view input,
                                    char* dest) noexcept {
    const char* ptr = input.data();
    size_t len = input.size();

#if defined(QLEVER_SIMD_X86)
    while (len >= 32) {
      uint32_t mask = scanChunk32<Format>(ptr).rawMask();
      if (mask == 0) {
        // Zero-escape fast path: 32 raw bytes copied with single vector instruction
        std::memcpy(dest, ptr, 32);
        dest += 32;
        ptr += 32;
        len -= 32;
        continue;
      }

      // Escape characters present: process clean sub-slices and escapes
      uint32_t current = 0;
      while (mask != 0) {
        uint32_t next = static_cast<uint32_t>(std::countr_zero(mask));
        uint32_t cleanLen = next - current;
        if (cleanLen > 0) {
          std::memcpy(dest, ptr + current, cleanLen);
          dest += cleanLen;
        }
        dest = detail::emitEscape<Format>(ptr[next], dest);
        current = next + 1;
        mask &= (mask - 1);  // Clear lowest set bit
      }
      if (current < 32) {
        std::memcpy(dest, ptr + current, 32 - current);
        dest += (32 - current);
      }
      ptr += 32;
      len -= 32;
    }
#endif

    // Remaining tail bytes (0 to 31)
    while (len > 0) {
      if (detail::isEscapeChar<Format>(*ptr)) {
        dest = detail::emitEscape<Format>(*ptr, dest);
      } else {
        *dest++ = *ptr;
      }
      ++ptr;
      --len;
    }
    return dest;
  }

  // ___________________________________________________________________________
  // Escape an RFC 4180 CSV field. If no special characters (", ,, \r, \n) are
  // present, returns input directly. Otherwise quotes and doubles internal quotes.
  [[nodiscard]] static inline std::string escapeForCsv(std::string_view input);

  // ___________________________________________________________________________
  // Escape a field for IANA-TSV. If no tabs, newlines, carriage returns, or
  // backslashes are present, returns input directly. Otherwise replaces tabs
  // with spaces, newlines with \n, carriage returns with \r, and backslashes
  // with \\.
  [[nodiscard]] static inline std::string escapeForTsv(std::string_view input) {
    if (!hasEscapes<EscapeFormat::Tsv>(input)) [[likely]] {
      return std::string{input};
    }
    std::string result;
    result.resize(input.size() * 2);
    char* out = copyAndEscape<EscapeFormat::Tsv>(input, result.data());
    result.resize(static_cast<size_t>(out - result.data()));
    return result;
  }

  // ___________________________________________________________________________
  // Convert a normalized RDF literal into a valid Turtle literal using SIMD.
  // Escapes internal quotes ("), backslashes (\\), and newlines (\n, \r).
  [[nodiscard]] static inline std::string validRDFLiteralFromNormalized(
      std::string_view normLiteral) {
    AD_CONTRACT_CHECK(ql::starts_with(normLiteral, '"'));
    size_t posSecondQuote = normLiteral.find('"', 1);
    AD_CONTRACT_CHECK(posSecondQuote != std::string_view::npos);
    size_t posLastQuote = normLiteral.rfind('"');

    // If there are only two quotes and no internal special characters, pass through
    if (posSecondQuote == posLastQuote &&
        !hasEscapes<EscapeFormat::Turtle>(normLiteral)) [[likely]] {
      return std::string{normLiteral};
    }

    std::string_view normalizedContent =
        normLiteral.substr(1, posLastQuote - 1);
    std::string result;
    result.resize(normLiteral.size() * 2 + 2);
    char* out = result.data();
    *out++ = '"';
    out = copyAndEscape<EscapeFormat::Turtle>(normalizedContent, out);
    *out++ = '"';

    std::string_view suffix = normLiteral.substr(posLastQuote + 1);
    if (!suffix.empty()) {
      std::memcpy(out, suffix.data(), suffix.size());
      out += suffix.size();
    }
    result.resize(static_cast<size_t>(out - result.data()));
    return result;
  }
};

}  // namespace ad_utility::simd

#endif  // QLEVER_SRC_ENGINE_SIMDESCAPECLASSIFIER_H
