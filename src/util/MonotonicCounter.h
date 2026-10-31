// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_MONOTONICCOUNTER_H
#define QLEVER_SRC_UTIL_MONOTONICCOUNTER_H

#include <cstdint>
#include <limits>

#include "util/Exception.h"

namespace ad_utility {

// A counter that can only increase. It exists so that "monotonically
// increasing" is enforced by the type instead of by convention: the
// stored value is private, and the only way to change it is `mintNext`,
// which hands out the current value and advances the counter. There is
// deliberately no way to set, decrement, or reset the value, so ids
// minted from a `MonotonicCounter` are unique and increasing for the
// whole lifetime of the counter.
class MonotonicCounter {
 public:
  // `explicit`, so that a bare integer cannot silently convert to a
  // counter where a counter is expected.
  explicit MonotonicCounter(uint64_t firstValue = 0) : value_{firstValue} {}

  // Return the current value, then advance the counter by one. Throws if
  // the counter would overflow, because wrapping around would hand out
  // an already-used value.
  [[nodiscard]] uint64_t mintNext() {
    AD_CORRECTNESS_CHECK(
        value_ != std::numeric_limits<uint64_t>::max(),
        "A MonotonicCounter overflowed: no fresh value left to mint.");
    return value_++;
  }

  // The value that the next call to `mintNext` will return.
  [[nodiscard]] uint64_t currentValue() const { return value_; }

 private:
  uint64_t value_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_MONOTONICCOUNTER_H
