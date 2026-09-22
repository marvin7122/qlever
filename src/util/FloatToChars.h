// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_FLOATTOCHARS_H
#define QLEVER_SRC_UTIL_FLOATTOCHARS_H

#include <charconv>
#include <cstddef>
#include <cstdio>

namespace ad_utility {

// Format `value` into the buffer `[begin, end)` without any dynamic
// allocation and return a pointer one past the last character written (like
// `std::to_chars`). If the value cannot be formatted, `begin` is returned, so
// callers can detect failure via `result == begin`.
//
// On Apple platforms the floating-point overloads of `std::to_chars` are only
// available with a deployment target of macOS 13.3 or newer, so `snprintf`
// with `%.17g` (which round-trips every `double`) is used as a fallback there.
// The fallback output never exceeds 24 characters plus the null terminator,
// so a 32-byte buffer is always sufficient.
inline char* doubleToChars(char* begin, char* end, double value) noexcept {
#ifdef __APPLE__
  const auto capacity = static_cast<size_t>(end - begin);
  const int len = std::snprintf(begin, capacity, "%.17g", value);
  return len > 0 ? begin + len : begin;
#else
  const auto [ptr, ec] = std::to_chars(begin, end, value);
  return ec == std::errc{} ? ptr : begin;
#endif
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_FLOATTOCHARS_H
