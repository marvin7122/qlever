// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_PORTABLEDOUBLETOCHARS_H
#define QLEVER_SRC_ENGINE_PORTABLEDOUBLETOCHARS_H

#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <system_error>

namespace ql::engine::detail {

// Portable `std::to_chars` replacement for `double` values.
//
// The floating-point overloads of `std::to_chars` are unavailable in the
// macOS SDK when targeting macOS versions older than 13.3, but our macOS CI
// builds target macOS 11.0. On Apple platforms therefore fall back to
// `snprintf` with the shortest precision that still round-trips, which yields
// the same shortest round-trip representation as `std::to_chars` for typical
// values. Everywhere else this is a thin wrapper around `std::to_chars`.
//
// Like `std::to_chars`, this function never allocates and never throws.
inline std::to_chars_result doubleToChars(char* first, char* last,
                                          double value) noexcept {
#ifdef __APPLE__
  if (last <= first) {
    return {first, std::errc::value_too_large};
  }
  const auto capacity = static_cast<size_t>(last - first);
  // Find the shortest `%g` precision that parses back to the exact value.
  for (int precision = 1; precision <= 17; ++precision) {
    int len = std::snprintf(first, capacity, "%.*g", precision, value);
    if (len <= 0 || static_cast<size_t>(len) >= capacity) {
      break;
    }
    if (std::strtod(first, nullptr) == value) {
      return {first + len, std::errc{}};
    }
  }
  // Full precision round-trips all finite values; non-finite values are
  // emitted directly.
  int len = std::snprintf(first, capacity, "%.17g", value);
  if (len > 0 && static_cast<size_t>(len) < capacity) {
    return {first + len, std::errc{}};
  }
  return {first, std::errc::value_too_large};
#else
  return std::to_chars(first, last, value);
#endif
}

}  // namespace ql::engine::detail

#endif  // QLEVER_SRC_ENGINE_PORTABLEDOUBLETOCHARS_H
