// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_PORTABLEDOUBLETOCHARS_H
#define QLEVER_SRC_ENGINE_PORTABLEDOUBLETOCHARS_H

#include <array>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
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
  // `snprintf` needs room for a terminating NUL that `std::to_chars` does
  // not write, so format into a local buffer (large enough for any `%.17g`
  // output) and copy only the characters. An exact-fit `[first, last)` then
  // succeeds, as it does with `std::to_chars`.
  std::array<char, 32> tmp{};
  const auto copyOut = [&](int len) -> std::optional<std::to_chars_result> {
    if (len <= 0 || static_cast<size_t>(len) >= tmp.size() ||
        static_cast<size_t>(len) > capacity) {
      return std::nullopt;
    }
    std::memcpy(first, tmp.data(), static_cast<size_t>(len));
    return std::to_chars_result{first + len, std::errc{}};
  };
  // Find the shortest `%g` precision that parses back to the exact value.
  for (int precision = 1; precision <= 17; ++precision) {
    int len = std::snprintf(tmp.data(), tmp.size(), "%.*g", precision, value);
    if (len <= 0 || static_cast<size_t>(len) >= tmp.size()) {
      break;
    }
    if (std::strtod(tmp.data(), nullptr) == value) {
      if (auto result = copyOut(len)) {
        return *result;
      }
      return {first, std::errc::value_too_large};
    }
  }
  // Full precision round-trips all finite values; non-finite values are
  // emitted directly.
  if (auto result =
          copyOut(std::snprintf(tmp.data(), tmp.size(), "%.17g", value))) {
    return *result;
  }
  return {first, std::errc::value_too_large};
#else
  return std::to_chars(first, last, value);
#endif
}

}  // namespace ql::engine::detail

#endif  // QLEVER_SRC_ENGINE_PORTABLEDOUBLETOCHARS_H
