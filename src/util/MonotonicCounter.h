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
#include "util/NoCopyNoMove.h"

namespace ad_utility {

// A counter that can only increase, starting at 0. It exists so that
// "monotonically increasing" is enforced by the type instead of by convention:
// the only way to change the value is `mintNext`, which hands out the current
// value and advances the counter. There is deliberately no way to set,
// decrement, reset, copy or move the counter, so the values minted from one
// `MonotonicCounter` are unique and increasing for its whole lifetime.
class MonotonicCounter : NoCopyNoMove {
 public:
  // Return the current value, then advance the counter by one. A counter that
  // starts at 0 and advances by one per call cannot reach the maximum of
  // `uint64_t` in practice, so the check guards an internal invariant: wrapping
  // around would hand out an already-used value.
  [[nodiscard]] uint64_t mintNext() {
    AD_CORRECTNESS_CHECK(
        nextValue_ != std::numeric_limits<uint64_t>::max(),
        "A MonotonicCounter overflowed: no fresh value left to mint.");
    return nextValue_++;
  }

 private:
  // The value that the next call to `mintNext` returns.
  uint64_t nextValue_ = 0;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_MONOTONICCOUNTER_H
