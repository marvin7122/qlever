// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_FASTINTTOSTRING_H
#define QLEVER_SRC_UTIL_FASTINTTOSTRING_H

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>

#if defined(__x86_64__) || defined(_M_X64) || defined(__SSE2__)
#include <emmintrin.h>
#endif

#include "util/Exception.h"

namespace ad_utility {

namespace detail {

// _____________________________________________________________________________
// 2-digit lookup table for radix-100 decomposition.
// Contains 100 pairs of ASCII digits ("00", "01", ..., "99").
alignas(64) inline constexpr char DIGIT_PAIRS[200] =
    "00010203040506070809"
    "10111213141516171819"
    "20212223242526272829"
    "30313233343536373839"
    "40414243444546474849"
    "50515253545556575859"
    "60616263646566676869"
    "70717273747576777879"
    "80818283848586878889"
    "90919293949596979899";

// _____________________________________________________________________________
// Powers of 10 lookup table for exact branchless digit length determination.
inline constexpr uint64_t POWERS_OF_10_64[20] = {
    1ULL,
    10ULL,
    100ULL,
    1000ULL,
    10000ULL,
    100000ULL,
    1000000ULL,
    10000000ULL,
    100000000ULL,
    1000000000ULL,
    10000000000ULL,
    100000000000ULL,
    1000000000000ULL,
    10000000000000ULL,
    100000000000000ULL,
    1000000000000000ULL,
    10000000000000000ULL,
    100000000000000000ULL,
    1000000000000000000ULL,
    10000000000000000000ULL,
};

// _____________________________________________________________________________
// Fast 8-digit formatting using scalar radix decomposition and SSE2 or SWAR digit-pair conversion.
inline void format8Digits(uint32_t v, char* dst) noexcept {
  AD_CONTRACT_CHECK(dst != nullptr);
  AD_CONTRACT_CHECK(v < 100000000U);

#if defined(__x86_64__) || defined(_M_X64) || defined(__SSE2__)
  // SSE2 parallel vector radix conversion: decompose into 4 2-digit lanes
  uint32_t q = v / 10000;
  uint32_t r = v % 10000;
  uint16_t p0 = static_cast<uint16_t>(q / 100);
  uint16_t p1 = static_cast<uint16_t>(q % 100);
  uint16_t p2 = static_cast<uint16_t>(r / 100);
  uint16_t p3 = static_cast<uint16_t>(r % 100);

  __m128i pairs = _mm_setr_epi16(p0, p1, p2, p3, 0, 0, 0, 0);
  // tens = (pairs * 52429) >> 19  (approx division by 10)
  __m128i tens = _mm_srli_epi16(_mm_mulhi_epu16(pairs, _mm_set1_epi16(52429)), 3);
  __m128i tens10 = _mm_mullo_epi16(tens, _mm_set1_epi16(10));
  __m128i ones = _mm_sub_epi16(pairs, tens10);
  // Little-endian layout: tens in low byte, ones in high byte
  __m128i combined = _mm_or_si128(tens, _mm_slli_epi16(ones, 8));
  __m128i ascii = _mm_add_epi8(combined, _mm_set1_epi8('0'));
  _mm_storel_epi64(reinterpret_cast<__m128i*>(dst), ascii);
#else
  // SWAR / lookup table fallback
  uint32_t q = v / 10000;
  uint32_t r = v % 10000;
  uint32_t d0 = (q / 100) * 2;
  uint32_t d1 = (q % 100) * 2;
  uint32_t d2 = (r / 100) * 2;
  uint32_t d3 = (r % 100) * 2;
  std::memcpy(dst + 0, &DIGIT_PAIRS[d0], 2);
  std::memcpy(dst + 2, &DIGIT_PAIRS[d1], 2);
  std::memcpy(dst + 4, &DIGIT_PAIRS[d2], 2);
  std::memcpy(dst + 6, &DIGIT_PAIRS[d3], 2);
#endif
}

// _____________________________________________________________________________
inline void format4Digits(uint32_t v, char* dst) noexcept {
  AD_CONTRACT_CHECK(dst != nullptr);
  AD_CONTRACT_CHECK(v < 10000U);
  uint32_t d0 = (v / 100) * 2;
  uint32_t d1 = (v % 100) * 2;
  std::memcpy(dst + 0, &DIGIT_PAIRS[d0], 2);
  std::memcpy(dst + 2, &DIGIT_PAIRS[d1], 2);
}

// _____________________________________________________________________________
inline void format2Digits(uint32_t v, char* dst) noexcept {
  AD_CONTRACT_CHECK(dst != nullptr);
  AD_CONTRACT_CHECK(v < 100U);
  std::memcpy(dst, &DIGIT_PAIRS[v * 2], 2);
}

}  // namespace detail

// _____________________________________________________________________________
// Maximum character buffer length required for any 64-bit integer formatted
// to ASCII (including potential negative sign and null terminator).
inline constexpr size_t MAX_INT64_ASCII_LENGTH = 22;
inline constexpr size_t MAX_UINT64_ASCII_LENGTH = 21;

// Define standard Wikidata prefix constants.
inline constexpr std::string_view WIKIDATA_ENTITY_PREFIX =
    "http://www.wikidata.org/entity/Q";
inline constexpr std::string_view WIKIDATA_PROPERTY_PREFIX =
    "http://www.wikidata.org/prop/direct/P";

// _____________________________________________________________________________
// Determine the exact number of decimal digits in `val`.
// Estimates the decimal digit count from the bit width and corrects it using a power-of-ten comparison.
[[nodiscard]] inline constexpr uint32_t numDigits(uint64_t val) noexcept {
  if (val == 0) {
    return 1;
  }
  const uint32_t bitWidth = 64 - std::countl_zero(val);
  const uint32_t p = (bitWidth * 1233) >> 12;
  return p + static_cast<uint32_t>(val >= detail::POWERS_OF_10_64[p]);
}

// _____________________________________________________________________________
// Branchless determination of the exact number of decimal digits for 32-bit uint.
[[nodiscard]] inline constexpr uint32_t numDigits(uint32_t val) noexcept {
  return numDigits(static_cast<uint64_t>(val));
}

// _____________________________________________________________________________
// Zero-allocation conversion of uint64_t to ASCII.
// Writes digits directly to `out` and returns a pointer to one-past-the-end.
// Precondition: `out` must point to a buffer with at least `numDigits(val)` bytes.
inline char* formatUIntBranchless(uint64_t val, char* out) noexcept {
  AD_CONTRACT_CHECK(out != nullptr);
  const uint32_t len = numDigits(val);
  char* p = out + len;

  while (val >= 100000000ULL) {
    uint32_t rem = static_cast<uint32_t>(val % 100000000ULL);
    val /= 100000000ULL;
    p -= 8;
    detail::format8Digits(rem, p);
  }

  uint32_t val32 = static_cast<uint32_t>(val);
  while (val32 >= 100) {
    uint32_t rem = val32 % 100;
    val32 /= 100;
    p -= 2;
    std::memcpy(p, &detail::DIGIT_PAIRS[rem * 2], 2);
  }

  if (val32 < 10) {
    --p;
    *p = static_cast<char>('0' + val32);
  } else {
    p -= 2;
    std::memcpy(p, &detail::DIGIT_PAIRS[val32 * 2], 2);
  }

  return out + len;
}

// _____________________________________________________________________________
// Zero-allocation conversion of uint32_t to ASCII.
inline char* formatUInt32Branchless(uint32_t val, char* out) noexcept {
  AD_CONTRACT_CHECK(out != nullptr);
  const uint32_t len = numDigits(val);
  char* p = out + len;

  while (val >= 100) {
    uint32_t rem = val % 100;
    val /= 100;
    p -= 2;
    std::memcpy(p, &detail::DIGIT_PAIRS[rem * 2], 2);
  }

  if (val < 10) {
    --p;
    *p = static_cast<char>('0' + val);
  } else {
    p -= 2;
    std::memcpy(p, &detail::DIGIT_PAIRS[val * 2], 2);
  }

  return out + len;
}

// _____________________________________________________________________________
// Zero-allocation conversion of int64_t to ASCII, including a sign when needed.
// Writes sign (if negative) and digits directly to `out`.
// Returns a pointer to one-past-the-end.
// Precondition: `out` must point to a buffer of at least 21 bytes.
inline char* formatIntBranchless(int64_t val, char* out) noexcept {
  AD_CONTRACT_CHECK(out != nullptr);
  uint64_t uval;
  if (val < 0) {
    *out++ = '-';
    // Safe negation for INT64_MIN without undefined signed overflow
    uval = static_cast<uint64_t>(0) - static_cast<uint64_t>(val);
  } else {
    uval = static_cast<uint64_t>(val);
  }
  return formatUIntBranchless(uval, out);
}

// _____________________________________________________________________________
// Branchless, zero-allocation conversion of int32_t to ASCII.
inline char* formatInt32Branchless(int32_t val, char* out) noexcept {
  return formatIntBranchless(static_cast<int64_t>(val), out);
}

// _____________________________________________________________________________
// Allocation-free RDF Wikidata QID formatting ("http://www.wikidata.org/entity/Q" + id).
// Writes the prefix and ASCII digits directly into `out`.
// Returns a pointer to one-past-the-end.
inline char* formatQid(uint64_t id, char* out) noexcept {
  AD_CONTRACT_CHECK(out != nullptr);
  std::memcpy(out, WIKIDATA_ENTITY_PREFIX.data(), WIKIDATA_ENTITY_PREFIX.size());
  return formatUIntBranchless(id, out + WIKIDATA_ENTITY_PREFIX.size());
}

// _____________________________________________________________________________
// Single-pass RDF Wikidata Property PID formatting ("http://www.wikidata.org/prop/direct/P" + id).
inline char* formatPid(uint64_t id, char* out) noexcept {
  AD_CONTRACT_CHECK(out != nullptr);
  std::memcpy(out, WIKIDATA_PROPERTY_PREFIX.data(), WIKIDATA_PROPERTY_PREFIX.size());
  return formatUIntBranchless(id, out + WIKIDATA_PROPERTY_PREFIX.size());
}

// _____________________________________________________________________________
// Single-pass RDF IRI formatting with custom prefix and integer ID.
inline char* formatPrefixedId(std::string_view prefix, uint64_t id, char* out) noexcept {
  AD_CONTRACT_CHECK(out != nullptr);
  if (!prefix.empty()) {
    std::memcpy(out, prefix.data(), prefix.size());
  }
  return formatUIntBranchless(id, out + prefix.size());
}

// _____________________________________________________________________________
// Single-pass RDF IRI formatting with custom prefix and signed integer ID.
inline char* formatPrefixedInt(std::string_view prefix, int64_t id, char* out) noexcept {
  AD_CONTRACT_CHECK(out != nullptr);
  if (!prefix.empty()) {
    std::memcpy(out, prefix.data(), prefix.size());
  }
  return formatIntBranchless(id, out + prefix.size());
}

// _____________________________________________________________________________
// Provide convenient string-returning wrappers with exact string pre-sizing.
[[nodiscard]] inline std::string formatUIntToString(uint64_t val) {
  const uint32_t len = numDigits(val);
  std::string s;
  s.resize(len);
  formatUIntBranchless(val, s.data());
  return s;
}

[[nodiscard]] inline std::string formatIntToString(int64_t val) {
  const bool negative = val < 0;
  const uint64_t uval = negative ? (static_cast<uint64_t>(0) - static_cast<uint64_t>(val))
                                 : static_cast<uint64_t>(val);
  const uint32_t len = numDigits(uval) + (negative ? 1 : 0);
  std::string s;
  s.resize(len);
  formatIntBranchless(val, s.data());
  return s;
}

[[nodiscard]] inline std::string formatQidToString(uint64_t id) {
  const size_t totalLen = WIKIDATA_ENTITY_PREFIX.size() + numDigits(id);
  std::string s;
  s.resize(totalLen);
  formatQid(id, s.data());
  return s;
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_FASTINTTOSTRING_H
