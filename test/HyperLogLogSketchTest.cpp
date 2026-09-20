// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <cmath>

#include "global/Id.h"
#include "index/HyperLogLogSketch.h"

using namespace ql::index::stats;

TEST(HyperLogLogSketchTest, AccurateDistinctEstimation) {
  HyperLogLogSketch<10> hll;

  constexpr uint64_t EXACT_COUNT = 50'000;
  for (uint64_t i = 0; i < EXACT_COUNT; ++i) {
    hll.insert(Id::fromBits(i * 17 + 1));
  }

  uint64_t estimate = hll.estimateCardinality();

  // For p=10 (1024 registers), standard error is ~1.04 / sqrt(1024) = ~3.25%
  double relativeError = std::abs(static_cast<double>(estimate) -
                                  static_cast<double>(EXACT_COUNT)) /
                         static_cast<double>(EXACT_COUNT);

  EXPECT_LE(relativeError, 0.05);  // within 5%
}

TEST(HyperLogLogSketchTest, SketchMergeCorrectness) {
  HyperLogLogSketch<10> hll1;
  HyperLogLogSketch<10> hll2;

  // Insert [0..20,000] into hll1
  for (uint64_t i = 0; i < 20'000; ++i) {
    hll1.insert(Id::fromBits(i));
  }

  // Insert [10,000..40,000] into hll2 (10,000 overlap)
  for (uint64_t i = 10'000; i < 40'000; ++i) {
    hll2.insert(Id::fromBits(i));
  }

  hll1.merge(hll2);

  uint64_t mergedEstimate = hll1.estimateCardinality();
  constexpr uint64_t TOTAL_DISTINCT = 40'000;

  double relativeError = std::abs(static_cast<double>(mergedEstimate) -
                                  static_cast<double>(TOTAL_DISTINCT)) /
                         static_cast<double>(TOTAL_DISTINCT);

  EXPECT_LE(relativeError, 0.05);
}
