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

TEST(BlockedBloomFilterTest, CreateFromColumnAndPrune) {
  std::vector<Id> buildSide;
  buildSide.reserve(1000);
  for (uint64_t i = 0; i < 1000; ++i) {
    buildSide.push_back(Id::fromBits(i * 10));
  }

  auto filter = BlockedBloomFilter::createFromColumn(buildSide, 0.01);
  for (const auto& id : buildSide) {
    EXPECT_TRUE(filter.contains(id));
  }

  std::vector<Id> probeSide;
  probeSide.reserve(2000);
  for (uint64_t i = 0; i < 2000; ++i) {
    probeSide.push_back(
        Id::fromBits(i * 5));  // Every even index is in buildSide (i * 10)
  }

  auto matchingIndices = filter.pruneNonMatchingIndices(probeSide);
  // All 1000 matches must be present in matchingIndices
  size_t trueMatchesFound = 0;
  for (size_t idx : matchingIndices) {
    if (idx % 2 == 0 && idx < 2000) {
      trueMatchesFound++;
    }
  }
  EXPECT_EQ(trueMatchesFound, 1000);
}

TEST(BlockedBloomFilterTest, SemiJoinPushdownHelper) {
  std::vector<Id> buildSide = {Id::fromBits(42), Id::fromBits(100),
                               Id::fromBits(200), Id::fromBits(300)};
  SemiJoinPushdownHelper helper{buildSide};

  EXPECT_TRUE(helper.probe(Id::fromBits(42)));
  EXPECT_TRUE(helper.probe(Id::fromBits(100)));
  EXPECT_TRUE(helper.probe(Id::fromBits(200)));
  EXPECT_TRUE(helper.probe(Id::fromBits(300)));
  EXPECT_FALSE(helper.probe(Id::fromBits(999999)));

  std::vector<Id> probeSide = {Id::fromBits(1), Id::fromBits(42),
                               Id::fromBits(5), Id::fromBits(200),
                               Id::fromBits(9)};
  auto matchingIndices = helper.pruneNonMatchingIndices(probeSide);

  EXPECT_TRUE(std::find(matchingIndices.begin(), matchingIndices.end(), 1) !=
              matchingIndices.end());
  EXPECT_TRUE(std::find(matchingIndices.begin(), matchingIndices.end(), 3) !=
              matchingIndices.end());
}

TEST(BlockedBloomFilterTest, EmptyColumnHandling) {
  std::vector<Id> emptyBuildSide;
  auto filter = BlockedBloomFilter::createFromColumn(emptyBuildSide);
  EXPECT_FALSE(filter.contains(Id::fromBits(1)));

  SemiJoinPushdownHelper helper{emptyBuildSide};
  EXPECT_FALSE(helper.probe(Id::fromBits(1)));

  std::vector<Id> probeKeys = {Id::fromBits(1), Id::fromBits(2)};
  auto pruned = helper.pruneNonMatchingIndices(probeKeys);
  EXPECT_TRUE(pruned.empty());
}
