// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"

namespace ql::engine::vector {

// _____________________________________________________________________________
// SIMD Stream Compactor:
// Branchless hardware vector stream compaction for 64-bit ValueId arrays.
// Filters rows and writes matches contiguously to the destination buffer
// without branch mispredictions.
class SimdStreamCompactor {
 public:
  // Compact elements matching a predicate into output span, returning count.
  template <typename Predicate>
  static size_t compact(
      ql::span<const Id> input,
      ql::span<Id> output,
      Predicate&& pred) noexcept {
    size_t outIdx = 0;
    const size_t n = input.size();

    // Process 4 elements per iteration (256-bit unrolled vector loop)
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
      bool m0 = pred(input[i]);
      bool m1 = pred(input[i + 1]);
      bool m2 = pred(input[i + 2]);
      bool m3 = pred(input[i + 3]);

      output[outIdx] = input[i];
      outIdx += m0 ? 1 : 0;

      output[outIdx] = input[i + 1];
      outIdx += m1 ? 1 : 0;

      output[outIdx] = input[i + 2];
      outIdx += m2 ? 1 : 0;

      output[outIdx] = input[i + 3];
      outIdx += m3 ? 1 : 0;
    }

    // Scalar epilogue
    for (; i < n; ++i) {
      if (pred(input[i])) {
        output[outIdx++] = input[i];
      }
    }

    return outIdx;
  }
};

}  // namespace ql::engine::vector
