// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include "engine/BlockedBloomFilter.h"
#include "global/Id.h"

using namespace ql::engine::filter;

TEST(BlockedBloomFilterTest, InsertAndQueryMatches) {
  BlockedBloomFilter filter{10000, 0.01};

  for (uint64_t i = 0; i < 5000; ++i) {
    filter.insert(Id::fromBits(i * 2));
  }

  // All inserted elements must be present (zero false negatives)
  for (uint64_t i = 0; i < 5000; ++i) {
    EXPECT_TRUE(filter.contains(Id::fromBits(i * 2)));
  }

  // Non-inserted elements should have ~1% false positive rate
  size_t falsePositives = 0;
  for (uint64_t i = 0; i < 5000; ++i) {
    if (filter.contains(Id::fromBits(i * 2 + 1))) {
      falsePositives++;
    }
  }

  double fpr = static_cast<double>(falsePositives) / 5000.0;
  EXPECT_LE(fpr, 0.03);  // within expected statistical bounds
}
