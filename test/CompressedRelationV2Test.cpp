// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include "global/Id.h"
#include "index/CompressedRelationV2.h"

using namespace ql::index::v2;

TEST(CompressedRelationV2Test, DatatypeBitmaskOperations) {
  auto mask = DatatypeBitmask::Iri | DatatypeBitmask::Literal;
  EXPECT_TRUE(hasFlag(mask, DatatypeBitmask::Iri));
  EXPECT_TRUE(hasFlag(mask, DatatypeBitmask::Literal));
  EXPECT_FALSE(hasFlag(mask, DatatypeBitmask::Integer));
  EXPECT_FALSE(hasFlag(mask, DatatypeBitmask::Date));

  mask = mask | DatatypeBitmask::Date;
  EXPECT_TRUE(hasFlag(mask, DatatypeBitmask::Date));
}

TEST(CompressedRelationV2Test, MinMaxBoundaryPushdown) {
  std::vector<CompressedBlockMetadataV2> blocks(3);

  // Block 0
  blocks[0].leafletHeader_.col1Min_ = Id::makeFromInt(10);
  blocks[0].leafletHeader_.col1Max_ = Id::makeFromInt(50);
  blocks[0].baseMetadata_.numRows_ = 1000;

  // Block 1
  blocks[1].leafletHeader_.col1Min_ = Id::makeFromInt(55);
  blocks[1].leafletHeader_.col1Max_ = Id::makeFromInt(120);
  blocks[1].baseMetadata_.numRows_ = 1000;

  // Block 2
  blocks[2].leafletHeader_.col1Min_ = Id::makeFromInt(125);
  blocks[2].leafletHeader_.col1Max_ = Id::makeFromInt(300);
  blocks[2].baseMetadata_.numRows_ = 1000;

  auto minVal = LeafletAggregator::getMinCol1(blocks);
  auto maxVal = LeafletAggregator::getMaxCol1(blocks);

  ASSERT_TRUE(minVal.has_value());
  ASSERT_TRUE(maxVal.has_value());
  EXPECT_EQ(minVal.value(), Id::makeFromInt(10));
  EXPECT_EQ(maxVal.value(), Id::makeFromInt(300));
}

TEST(CompressedRelationV2Test, ExactDistinctCountRetrieval) {
  CompressedRelationMetadataV2 meta;
  meta.baseMetadata_.numRows_ = 50000;
  meta.exactDistinctCol1_ = 12500;
  meta.exactDistinctCol2_ = 48000;

  EXPECT_EQ(LeafletAggregator::getExactDistinctCol1(meta), 12500u);
  EXPECT_EQ(LeafletAggregator::getExactDistinctCol2(meta), 48000u);
}

TEST(CompressedRelationV2Test, TypedCountPruningFastPath) {
  std::vector<CompressedBlockMetadataV2> blocks(4);

  // Block 0: Pure literals (1000 rows)
  blocks[0].leafletHeader_.datatypeBitmaskCol1_ = DatatypeBitmask::Literal;
  blocks[0].baseMetadata_.numRows_ = 1000;

  // Block 1: Pure IRIs (no literals, 2000 rows)
  blocks[1].leafletHeader_.datatypeBitmaskCol1_ = DatatypeBitmask::Iri;
  blocks[1].baseMetadata_.numRows_ = 2000;

  // Block 2: Mixed Literal + Integer (500 rows)
  blocks[2].leafletHeader_.datatypeBitmaskCol1_ =
      DatatypeBitmask::Literal | DatatypeBitmask::Integer;
  blocks[2].baseMetadata_.numRows_ = 500;

  // Block 3: Pure literals (1500 rows)
  blocks[3].leafletHeader_.datatypeBitmaskCol1_ = DatatypeBitmask::Literal;
  blocks[3].baseMetadata_.numRows_ = 1500;

  auto result = LeafletAggregator::countTypedColumn(blocks, DatatypeBitmask::Literal, 1);

  // Exact count from pure blocks: 1000 + 1500 = 2500
  EXPECT_EQ(result.exactCount, 2500u);

  // Ambiguous blocks needing row-level scan: only Block 2
  ASSERT_EQ(result.ambiguousBlockIndices.size(), 1u);
  EXPECT_EQ(result.ambiguousBlockIndices[0], 2u);
}
