// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_APPLYREPLIESPROBE_H
#define QLEVER_SRC_UTIL_APPLYREPLIESPROBE_H

#include <cstddef>
#include <vector>

#include "util/Exception.h"

// Throwaway probe for the end-to-end test of the apply-replies flow. Do not
// merge.
namespace ad_utility {

// Return the sum of the first `n` values.
inline int sumOfFirst(const std::vector<int>& values, size_t n) {
  AD_CONTRACT_CHECK(n <= values.size());
  int sum = 0;
  for (size_t i = 0; i < n; ++i) {
    sum += values[i];
  }
  return sum;
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_APPLYREPLIESPROBE_H
