// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "engine/BlockedBloomFilter.h"
#include "global/Id.h"
#include "util/AllocatorTestHelpers.h"

using namespace ql::engine::filter;
using ad_utility::testing::makeAllocator;

// _____________________________________________________________________________
TEST(BlockedBloomFilterTest, InsertAndQueryMatches) {
  BlockedBloomFilter filter{10000, makeAllocator(), 0.01};

  for (uint64_t i = 0; i < 5000; ++i) {
    filter.insert(Id::fromBits(i * 2));
  }

  // All inserted elements must be present (no false negatives).
  for (uint64_t i = 0; i < 5000; ++i) {
    EXPECT_TRUE(filter.contains(Id::fromBits(i * 2)));
  }

  // The filter is sized for 10000 elements but holds 5000, so the observed
  // false-positive rate must stay well below the three-fold target.
  size_t falsePositives = 0;
  for (uint64_t i = 0; i < 5000; ++i) {
    if (filter.contains(Id::fromBits(i * 2 + 1))) {
      falsePositives++;
    }
  }
  double falsePositiveRate = static_cast<double>(falsePositives) / 5000.0;
  EXPECT_LE(falsePositiveRate, 0.03);
}

// _____________________________________________________________________________
TEST(BlockedBloomFilterTest, CreateFromColumn) {
  std::vector<Id> buildSide;
  for (uint64_t i = 0; i < 1000; ++i) {
    buildSide.push_back(Id::fromBits(i * 10));
  }
  const auto filter =
      BlockedBloomFilter::createFromColumn(buildSide, makeAllocator(), 0.01);
  for (const auto& id : buildSide) {
    EXPECT_TRUE(filter.contains(id));
  }
  // 1000 elements at p = 0.01 need ceil(9585.1 / 512) = 19 blocks.
  EXPECT_EQ(filter.numBlocks(), 19u);
}

// _____________________________________________________________________________
TEST(BlockedBloomFilterTest, FalsePositiveRateControlsSize) {
  // The requested rate must change the size: m = -n*ln(p)/ln(2)^2 bits.
  const BlockedBloomFilter strict{10000, makeAllocator(), 0.001};
  const BlockedBloomFilter medium{10000, makeAllocator(), 0.01};
  const BlockedBloomFilter loose{10000, makeAllocator(), 0.1};
  EXPECT_LT(loose.numBlocks(), medium.numBlocks());
  EXPECT_LT(medium.numBlocks(), strict.numBlocks());
  // ceil(95851 / 512) = 188 blocks for n = 10000, p = 0.01.
  EXPECT_EQ(medium.numBlocks(), 188u);
  EXPECT_EQ(medium.sizeBytes(), 188u * 64u);
}

// _____________________________________________________________________________
TEST(BlockedBloomFilterTest, InvalidFalsePositiveRate) {
  for (double rate :
       {0.0, 1.0, -0.5, 1.5, std::numeric_limits<double>::quiet_NaN()}) {
    EXPECT_ANY_THROW((BlockedBloomFilter{10, makeAllocator(), rate}));
  }
  // Too many elements for the conversion of the block count to `size_t`.
  EXPECT_ANY_THROW((BlockedBloomFilter{std::numeric_limits<size_t>::max(),
                                       makeAllocator(), 0.01}));
}

// _____________________________________________________________________________
TEST(BlockedBloomFilterTest, MemoryIsTakenFromTheAllocator) {
  using namespace ad_utility::memory_literals;
  // 1 M elements at p = 0.01 need about 1.2 MB, more than the limit.
  EXPECT_ANY_THROW(
      (BlockedBloomFilter{1'000'000, makeAllocator(100_kB), 0.01}));
  EXPECT_NO_THROW((BlockedBloomFilter{1'000, makeAllocator(100_kB), 0.01}));
}

// _____________________________________________________________________________
TEST(BlockedBloomFilterTest, EmptyFilter) {
  const std::vector<Id> emptyBuildSide;
  const auto filter =
      BlockedBloomFilter::createFromColumn(emptyBuildSide, makeAllocator());
  EXPECT_EQ(filter.numBlocks(), 1u);
  EXPECT_FALSE(filter.contains(Id::fromBits(1)));
}
