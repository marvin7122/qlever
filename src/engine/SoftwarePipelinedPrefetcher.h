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

namespace ql::engine::prefetch {

// _____________________________________________________________________________
// Software-Pipelined Prefetcher:
// Overlaps arithmetic instructions with DDR RAM latency by prefetching memory
// blocks PREFETCH_DISTANCE iterations in advance.
template <size_t PrefetchDistance = 16>
class SoftwarePipelinedPrefetcher {
 public:
  template <typename PointerArray, typename Consumer>
  static void processWithPrefetch(const PointerArray& ptrs, size_t count,
                                  Consumer&& consume) {
    for (size_t i = 0; i < count; ++i) {
      if (i + PrefetchDistance < count) {
        __builtin_prefetch(ptrs[i + PrefetchDistance], 0, 3);
      }
      consume(ptrs[i]);
    }
  }
};

}  // namespace ql::engine::prefetch
