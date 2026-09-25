// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_V2_SIMDESCAPECLASSIFIER_H
#define QLEVER_SRC_ENGINE_EXPORT_V2_SIMDESCAPECLASSIFIER_H

#include <absl/numeric/bits.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "backports/span.h"
#include "util/Exception.h"

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#define QLEVER_EXPORT_V2_X86 1
#endif

#if defined(QLEVER_EXPORT_V2_X86) && (defined(__GNUC__) || defined(__clang__))
#define QLEVER_EXPORT_V2_AVX2_TARGET __attribute__((target("avx2")))
#else
#define QLEVER_EXPORT_V2_AVX2_TARGET
#endif

namespace ql::engine::export_v2 {

// The output format whose special characters are detected and escaped:
// CSV: `"`, `,`, `\r`, `\n`; TSV: TAB, `\n`, `\r`, `\\`;
// Turtle literals: `"`, `\\`, `\n`, `\r`.
enum class EscapeFormat { Csv, Tsv, Turtle };

// The result of classifying one 32-byte chunk: bit `i` is set iff byte `i` of
// the chunk is a special character of the format.
class EscapeMask32 {
 public:
  constexpr explicit EscapeMask32(uint32_t mask = 0) : mask_{mask} {}

  [[nodiscard]] constexpr bool hasEscape() const { return mask_ != 0; }
  [[nodiscard]] constexpr uint32_t raw() const { return mask_; }
  [[nodiscard]] constexpr uint32_t firstEscape() const {
    // `countr_zero(0)` is the bit width, i.e. 32 for an escape-free chunk.
    return static_cast<uint32_t>(absl::countr_zero(mask_));
  }
  [[nodiscard]] constexpr uint32_t count() const {
    return static_cast<uint32_t>(absl::popcount(mask_));
  }

 private:
  uint32_t mask_;
};

namespace detail {

template <EscapeFormat Format>
[[nodiscard]] constexpr bool isEscapeCharacter(char character) {
  if constexpr (Format == EscapeFormat::Csv) {
    return character == '"' || character == ',' || character == '\r' ||
           character == '\n';
  } else if constexpr (Format == EscapeFormat::Tsv) {
    return character == '\t' || character == '\n' || character == '\r' ||
           character == '\\';
  } else {
    return character == '"' || character == '\\' || character == '\n' ||
           character == '\r';
  }
}

template <EscapeFormat Format>
[[nodiscard]] uint32_t scanChunkScalar(const char* data) {
  uint32_t mask = 0;
  for (uint32_t index = 0; index < 32; ++index) {
    if (isEscapeCharacter<Format>(data[index])) {
      mask |= uint32_t{1} << index;
    }
  }
  return mask;
}

#ifdef QLEVER_EXPORT_V2_X86
template <EscapeFormat Format>
[[nodiscard]] QLEVER_EXPORT_V2_AVX2_TARGET uint32_t
scanChunkAvx2(const char* data) {
  const __m256i chunk =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data));
  __m256i matches;
  if constexpr (Format == EscapeFormat::Csv) {
    const __m256i quote = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('"'));
    const __m256i comma = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(','));
    const __m256i carriageReturn =
        _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\r'));
    const __m256i newline = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\n'));
    matches = _mm256_or_si256(_mm256_or_si256(quote, comma),
                              _mm256_or_si256(carriageReturn, newline));
  } else if constexpr (Format == EscapeFormat::Tsv) {
    const __m256i tab = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\t'));
    const __m256i newline = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\n'));
    const __m256i carriageReturn =
        _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\r'));
    const __m256i backslash = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\\'));
    matches = _mm256_or_si256(_mm256_or_si256(tab, newline),
                              _mm256_or_si256(carriageReturn, backslash));
  } else {
    const __m256i quote = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('"'));
    const __m256i backslash = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\\'));
    const __m256i newline = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\n'));
    const __m256i carriageReturn =
        _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8('\r'));
    matches = _mm256_or_si256(_mm256_or_si256(quote, backslash),
                              _mm256_or_si256(newline, carriageReturn));
  }
  return static_cast<uint32_t>(_mm256_movemask_epi8(matches));
}

[[nodiscard]] inline bool supportsAvx2() {
#if defined(__GNUC__) || defined(__clang__)
  static const bool result = __builtin_cpu_supports("avx2");
  return result;
#else
  return false;
#endif
}
#endif

template <EscapeFormat Format>
[[nodiscard]] char* emitEscaped(char character, char* output) {
  if constexpr (Format == EscapeFormat::Csv) {
    // Quote doubling only; the caller must wrap fields containing `,`, `\r`
    // or `\n` in double quotes for the output to be valid CSV. Byte-oriented:
    // non-ASCII bytes pass through untouched.
    if (character == '"') {
      *output++ = '"';
      *output++ = '"';
    } else {
      *output++ = character;
    }
  } else if constexpr (Format == EscapeFormat::Tsv) {
    if (character == '\t') {
      *output++ = '\\';
      *output++ = 't';
    } else if (character == '\\') {
      *output++ = '\\';
      *output++ = '\\';
    } else if (character == '\n' || character == '\r') {
      *output++ = '\\';
      *output++ = character == '\n' ? 'n' : 'r';
    }
  } else {
    *output++ = '\\';
    if (character == '\n') {
      *output++ = 'n';
    } else if (character == '\r') {
      *output++ = 'r';
    } else {
      *output++ = character;
    }
  }
  return output;
}

}  // namespace detail

// Find and escape the special characters of an `EscapeFormat`. The `Simd`
// functions use AVX2 when the CPU supports it (checked at runtime) and
// otherwise fall back to the equivalent scalar code; results are identical.
class SimdEscapeClassifier {
 private:
  // Classify the 32 bytes at `data`; `avx2Available` must be `useAvx2()`, so
  // loops query the CPU feature once instead of once per chunk.
  template <EscapeFormat Format>
  [[nodiscard]] static EscapeMask32 classifyChunk(
      const char* data, [[maybe_unused]] bool avx2Available) {
#ifdef QLEVER_EXPORT_V2_X86
    if (avx2Available) {
      return EscapeMask32{detail::scanChunkAvx2<Format>(data)};
    }
#endif
    return EscapeMask32{detail::scanChunkScalar<Format>(data)};
  }

  [[nodiscard]] static bool useAvx2() {
#ifdef QLEVER_EXPORT_V2_X86
    return detail::supportsAvx2();
#else
    return false;
#endif
  }

 public:
  // Classify the first 32 bytes of `input` (which must have at least 32).
  template <EscapeFormat Format>
  [[nodiscard]] static EscapeMask32 classify32(ql::span<const char> input) {
    AD_CONTRACT_CHECK(input.size() >= 32);
    return classifyChunk<Format>(input.data(), useAvx2());
  }

  template <EscapeFormat Format>
  [[nodiscard]] static constexpr bool isEscapeCharacter(char character) {
    return detail::isEscapeCharacter<Format>(character);
  }

  // Position of the first special character in `input`, or `npos`.
  template <EscapeFormat Format>
  [[nodiscard]] static size_t findFirstEscapeScalar(std::string_view input) {
    for (size_t index = 0; index < input.size(); ++index) {
      if (detail::isEscapeCharacter<Format>(input[index])) {
        return index;
      }
    }
    return std::string_view::npos;
  }

  // Same result as `findFirstEscapeScalar`, scanning 32 bytes at a time.
  template <EscapeFormat Format>
  [[nodiscard]] static size_t findFirstEscapeSimd(std::string_view input) {
#ifdef QLEVER_EXPORT_V2_X86
    if (detail::supportsAvx2()) {
      size_t offset = 0;
      while (input.size() - offset >= 32) {
        const EscapeMask32 mask{
            detail::scanChunkAvx2<Format>(input.data() + offset)};
        if (mask.hasEscape()) {
          return offset + mask.firstEscape();
        }
        offset += 32;
      }
      const size_t tail = findFirstEscapeScalar<Format>(input.substr(offset));
      return tail == std::string_view::npos ? tail : offset + tail;
    }
#endif
    return findFirstEscapeScalar<Format>(input);
  }

  // Copy `input` to `outputBuffer`, escaping every special character (see
  // `detail::emitEscaped`), and return the written prefix of `outputBuffer`.
  template <EscapeFormat Format>
  [[nodiscard]] static ql::span<char> copyAndEscape(
      std::string_view input, ql::span<char> outputBuffer) {
    // Worst case every input byte expands to two output bytes (quote
    // doubling / backslash escapes), so the output buffer must hold at
    // least twice the input size even when nothing needs escaping.
    AD_CONTRACT_CHECK(input.size() <= outputBuffer.size() / 2);
    if (input.empty()) {
      return {outputBuffer.data(), 0};
    }
    char* output = outputBuffer.data();
    char* const begin = output;
    size_t offset = 0;
    const bool avx2 = useAvx2();
    while (input.size() - offset >= 32) {
      uint32_t mask = classifyChunk<Format>(input.data() + offset, avx2).raw();
      if (mask == 0) {
        std::memcpy(output, input.data() + offset, 32);
        output += 32;
        offset += 32;
        continue;
      }

      uint32_t copied = 0;
      while (mask != 0) {
        const uint32_t escape = absl::countr_zero(mask);
        std::memcpy(output, input.data() + offset + copied, escape - copied);
        output += escape - copied;
        output = detail::emitEscaped<Format>(input[offset + escape], output);
        copied = escape + 1;
        mask &= mask - 1;
      }
      std::memcpy(output, input.data() + offset + copied, 32 - copied);
      output += 32 - copied;
      offset += 32;
    }

    for (; offset < input.size(); ++offset) {
      if (detail::isEscapeCharacter<Format>(input[offset])) {
        output = detail::emitEscaped<Format>(input[offset], output);
      } else {
        *output++ = input[offset];
      }
    }
    return {begin, static_cast<size_t>(output - begin)};
  }
};

}  // namespace ql::engine::export_v2

#undef QLEVER_EXPORT_V2_AVX2_TARGET
#undef QLEVER_EXPORT_V2_X86

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_SIMDESCAPECLASSIFIER_H
