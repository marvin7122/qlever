// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <numeric>
#include <vector>

#include "engine/SoftwarePipelinedPrefetcher.h"

using namespace ql::engine::prefetch;

TEST(SoftwarePipelinedPrefetcherTest, ProcessAllElementsCorrectly) {
  std::vector<int> data(1000);
  std::iota(data.begin(), data.end(), 0);

  std::vector<const int*> ptrs;
  ptrs.reserve(1000);
  for (const auto& val : data) {
    ptrs.push_back(&val);
  }

  int sum = 0;
  SoftwarePipelinedPrefetcher<16>::processWithPrefetch(
      ptrs, ptrs.size(), [&sum](const int* ptr) {
        sum += *ptr;
      });

  int expectedSum = (999 * 1000) / 2;
  EXPECT_EQ(sum, expectedSum);
}
