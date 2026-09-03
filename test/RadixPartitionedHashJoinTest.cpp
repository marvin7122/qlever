// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include "engine/RadixPartitionedHashJoin.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"

using namespace ql::engine::join;

TEST(RadixPartitionedHashJoinTest, BasicPartitionAndJoin) {
  ad_utility::AllocatorWithLimit<Id> allocator{
      ad_utility::makeAllocationMemoryLimit<Id>()};

  IdTable leftTable{2, allocator};
  IdTable rightTable{2, allocator};

  // Populate left table
  for (int i = 0; i < 1000; ++i) {
    leftTable.push_back({Id::makeFromInt(i), Id::makeFromInt(i * 10)});
  }

  // Populate right table with overlapping keys
  for (int i = 500; i < 1500; ++i) {
    rightTable.push_back({Id::makeFromInt(i), Id::makeFromInt(i * 100)});
  }

  size_t matches = RadixPartitionedHashJoin<4>::executeJoinCount(
      leftTable, 0, rightTable, 0);

  // Overlap: keys [500..999] = 500 matching rows
  EXPECT_EQ(matches, 500u);
}

TEST(RadixPartitionedHashJoinTest, DisjointTablesZeroMatches) {
  ad_utility::AllocatorWithLimit<Id> allocator{
      ad_utility::makeAllocationMemoryLimit<Id>()};

  IdTable leftTable{1, allocator};
  IdTable rightTable{1, allocator};

  leftTable.push_back({Id::makeFromInt(10)});
  rightTable.push_back({Id::makeFromInt(20)});

  size_t matches = RadixPartitionedHashJoin<2>::executeJoinCount(
      leftTable, 0, rightTable, 0);

  EXPECT_EQ(matches, 0u);
}
