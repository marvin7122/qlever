// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <cstddef>

#include "engine/RadixPartitionedHashJoin.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/AllocatorTestHelpers.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"

using namespace ql::engine::join;
using ad_utility::testing::makeAllocator;
using namespace ad_utility::memory_literals;

// The two tables overlap on keys [500..999], each appearing once per table,
// so the join has exactly 500 matching pairs under bag semantics.
// _____________________________________________________________________________
TEST(RadixPartitionedHashJoinTest, BasicPartitionAndJoin) {
  auto allocator = makeAllocator(1_MB);

  IdTable leftTable{2, allocator};
  IdTable rightTable{2, allocator};
  leftTable.reserve(1000);
  rightTable.reserve(1000);

  for (int i = 0; i < 1000; ++i) {
    leftTable.push_back({Id::makeFromInt(i), Id::makeFromInt(i * 10)});
  }
  for (int i = 500; i < 1500; ++i) {
    rightTable.push_back({Id::makeFromInt(i), Id::makeFromInt(i * 100)});
  }

  // Fewer partitions than the default 64 for faster test execution; the
  // result is partition-count independent (see ResultIndependentOfRadixBits).
  size_t matches = RadixPartitionedHashJoin<4>::executeJoinCount(leftTable, 0,
                                                                 rightTable, 0);

  // Overlap: keys [500..999] = 500 matching rows
  EXPECT_EQ(matches, 500u);
}

// _____________________________________________________________________________
TEST(RadixPartitionedHashJoinTest, DuplicateKeysCountWithBagSemantics) {
  auto allocator = makeAllocator(1_MB);

  IdTable leftTable{1, allocator};
  IdTable rightTable{1, allocator};

  // Left key 7 appears 3 times, key 8 once; right key 7 appears twice.
  // Bag semantics: 3 * 2 = 6 matches for key 7, 0 for key 8.
  leftTable.push_back({Id::makeFromInt(7)});
  leftTable.push_back({Id::makeFromInt(7)});
  leftTable.push_back({Id::makeFromInt(7)});
  leftTable.push_back({Id::makeFromInt(8)});
  rightTable.push_back({Id::makeFromInt(7)});
  rightTable.push_back({Id::makeFromInt(7)});
  rightTable.push_back({Id::makeFromInt(9)});

  size_t matches = RadixPartitionedHashJoin<2>::executeJoinCount(leftTable, 0,
                                                                 rightTable, 0);

  EXPECT_EQ(matches, 6u);
}

// _____________________________________________________________________________
TEST(RadixPartitionedHashJoinTest, DisjointTablesZeroMatches) {
  auto allocator = makeAllocator(1_MB);

  IdTable leftTable{1, allocator};
  IdTable rightTable{1, allocator};

  leftTable.push_back({Id::makeFromInt(10)});
  rightTable.push_back({Id::makeFromInt(20)});

  size_t matches = RadixPartitionedHashJoin<2>::executeJoinCount(leftTable, 0,
                                                                 rightTable, 0);

  EXPECT_EQ(matches, 0u);
}

// Empty inputs have no matches, and a single row on each side counts only
// when the keys are equal.
// _____________________________________________________________________________
TEST(RadixPartitionedHashJoinTest, EmptyAndSingleRowInputs) {
  auto allocator = makeAllocator(1_MB);
  IdTable empty{1, allocator};
  IdTable single{1, allocator};
  single.push_back({Id::makeFromInt(42)});
  IdTable other{1, allocator};
  other.push_back({Id::makeFromInt(43)});

  EXPECT_EQ(RadixPartitionedHashJoin<2>::executeJoinCount(empty, 0, single, 0),
            0u);
  EXPECT_EQ(RadixPartitionedHashJoin<2>::executeJoinCount(single, 0, empty, 0),
            0u);
  EXPECT_EQ(RadixPartitionedHashJoin<2>::executeJoinCount(empty, 0, empty, 0),
            0u);
  EXPECT_EQ(RadixPartitionedHashJoin<2>::executeJoinCount(single, 0, single, 0),
            1u);
  EXPECT_EQ(RadixPartitionedHashJoin<2>::executeJoinCount(single, 0, other, 0),
            0u);
}

// All rows share one key: the count is the full cross product.
// _____________________________________________________________________________
TEST(RadixPartitionedHashJoinTest, AllRowsSameKey) {
  auto allocator = makeAllocator(1_MB);
  IdTable leftTable{1, allocator};
  IdTable rightTable{1, allocator};
  for (int i = 0; i < 5; ++i) {
    leftTable.push_back({Id::makeFromInt(3)});
  }
  for (int i = 0; i < 4; ++i) {
    rightTable.push_back({Id::makeFromInt(3)});
  }

  EXPECT_EQ(RadixPartitionedHashJoin<2>::executeJoinCount(leftTable, 0,
                                                          rightTable, 0),
            20u);
}

// The join column need not be column zero.
// _____________________________________________________________________________
TEST(RadixPartitionedHashJoinTest, NonZeroJoinColumn) {
  auto allocator = makeAllocator(1_MB);
  IdTable leftTable{3, allocator};
  IdTable rightTable{2, allocator};
  leftTable.push_back(
      {Id::makeFromInt(1), Id::makeFromInt(7), Id::makeFromInt(2)});
  leftTable.push_back(
      {Id::makeFromInt(3), Id::makeFromInt(8), Id::makeFromInt(4)});
  rightTable.push_back({Id::makeFromInt(9), Id::makeFromInt(7)});
  rightTable.push_back({Id::makeFromInt(9), Id::makeFromInt(7)});

  // Key 7 appears once on the left and twice on the right.
  EXPECT_EQ(RadixPartitionedHashJoin<2>::executeJoinCount(leftTable, 1,
                                                          rightTable, 1),
            2u);
}

// The result must not depend on the number of radix partitions.
// _____________________________________________________________________________
TEST(RadixPartitionedHashJoinTest, ResultIndependentOfRadixBits) {
  auto allocator = makeAllocator(1_MB);
  IdTable leftTable{1, allocator};
  IdTable rightTable{1, allocator};
  for (int i = 0; i < 200; ++i) {
    leftTable.push_back({Id::makeFromInt(i % 37)});
    rightTable.push_back({Id::makeFromInt(i % 41)});
  }

  size_t matches2 = RadixPartitionedHashJoin<2>::executeJoinCount(
      leftTable, 0, rightTable, 0);
  size_t matches4 = RadixPartitionedHashJoin<4>::executeJoinCount(
      leftTable, 0, rightTable, 0);
  size_t matchesDefault =
      RadixPartitionedHashJoin<>::executeJoinCount(leftTable, 0, rightTable, 0);
  EXPECT_EQ(matches2, matches4);
  EXPECT_EQ(matches2, matchesDefault);
}

// Partition indices stay in range and are stable per key.
// _____________________________________________________________________________
TEST(RadixPartitionedHashJoinTest, PartitionIndexInRangeAndStable) {
  for (int i = 0; i < 100; ++i) {
    Id key = Id::makeFromInt(i);
    size_t index = RadixPartitionedHashJoin<>::getPartitionIndex(key);
    EXPECT_LT(index, RadixPartitionedHashJoin<>::NUM_PARTITIONS);
    EXPECT_EQ(index, RadixPartitionedHashJoin<>::getPartitionIndex(key));
  }
}

// `partitionTable` preserves every row: the buckets jointly hold all keys.
// _____________________________________________________________________________
TEST(RadixPartitionedHashJoinTest, PartitionTablePreservesAllRows) {
  auto allocator = makeAllocator(1_MB);
  IdTable table{1, allocator};
  for (int i = 0; i < 100; ++i) {
    table.push_back({Id::makeFromInt(i * i)});
  }
  auto partitions = RadixPartitionedHashJoin<4>::partitionTable(table, 0);
  ASSERT_EQ(partitions.size(), 16u);
  size_t total = 0;
  for (const auto& bucket : partitions) {
    total += bucket.keys.size();
    for (const Id& key : bucket.keys) {
      EXPECT_EQ(RadixPartitionedHashJoin<4>::getPartitionIndex(key),
                static_cast<size_t>(&bucket - &partitions.front()));
    }
  }
  EXPECT_EQ(total, 100u);
}

// Invalid column indices are contract violations and throw.
// _____________________________________________________________________________
TEST(RadixPartitionedHashJoinTest, InvalidColumnIndexThrows) {
  auto allocator = makeAllocator(1_MB);
  IdTable leftTable{1, allocator};
  IdTable rightTable{1, allocator};
  leftTable.push_back({Id::makeFromInt(1)});
  rightTable.push_back({Id::makeFromInt(1)});

  EXPECT_THROW(RadixPartitionedHashJoin<2>::partitionTable(leftTable, 1),
               ad_utility::Exception);
  EXPECT_THROW(RadixPartitionedHashJoin<2>::executeJoinCount(leftTable, 1,
                                                             rightTable, 0),
               ad_utility::Exception);
  EXPECT_THROW(RadixPartitionedHashJoin<2>::executeJoinCount(leftTable, 0,
                                                             rightTable, 5),
               ad_utility::Exception);
}
