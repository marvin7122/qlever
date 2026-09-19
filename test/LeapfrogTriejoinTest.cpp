// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <vector>

#include "engine/LeapfrogTriejoin.h"
#include "global/Id.h"

using namespace ql::engine::wcoj;

TEST(LeapfrogTriejoinTest, ThreeWayIntersection) {
  std::vector<Id> listA = {Id::makeFromInt(1), Id::makeFromInt(3),
                           Id::makeFromInt(5), Id::makeFromInt(7),
                           Id::makeFromInt(9)};
  std::vector<Id> listB = {Id::makeFromInt(2), Id::makeFromInt(3),
                           Id::makeFromInt(6), Id::makeFromInt(7),
                           Id::makeFromInt(10)};
  std::vector<Id> listC = {Id::makeFromInt(3), Id::makeFromInt(4),
                           Id::makeFromInt(7), Id::makeFromInt(8),
                           Id::makeFromInt(11)};

  std::vector<LeapfrogIterator> iterators;
  iterators.emplace_back(listA);
  iterators.emplace_back(listB);
  iterators.emplace_back(listC);

  auto matches = LeapfrogJoin::intersect(iterators);

  // Common elements across all three lists: 3 and 7
  ASSERT_EQ(matches.size(), 2u);
  EXPECT_EQ(matches[0], Id::makeFromInt(3));
  EXPECT_EQ(matches[1], Id::makeFromInt(7));
}

TEST(LeapfrogTriejoinTest, FirstIteratorStartsLargestNoFalsePositive) {
  // Iterator 0 starts with the largest key and shares nothing with the
  // others; the old round-robin `p` handling wrongly emitted 10 here.
  std::vector<Id> listA = {Id::makeFromInt(10), Id::makeFromInt(11)};
  std::vector<Id> listB = {Id::makeFromInt(1), Id::makeFromInt(2)};
  std::vector<Id> listC = {Id::makeFromInt(3), Id::makeFromInt(4)};

  std::vector<LeapfrogIterator> iterators;
  iterators.emplace_back(listA);
  iterators.emplace_back(listB);
  iterators.emplace_back(listC);

  EXPECT_TRUE(LeapfrogJoin::intersect(iterators).empty());
}

TEST(LeapfrogTriejoinTest, FirstIteratorStartsLargestWithCommonKey) {
  // Iterator 0 starts largest, but 20 is common to all three lists.
  std::vector<Id> listA = {Id::makeFromInt(10), Id::makeFromInt(20),
                           Id::makeFromInt(30)};
  std::vector<Id> listB = {Id::makeFromInt(5), Id::makeFromInt(20),
                           Id::makeFromInt(25)};
  std::vector<Id> listC = {Id::makeFromInt(7), Id::makeFromInt(20),
                           Id::makeFromInt(40)};

  std::vector<LeapfrogIterator> iterators;
  iterators.emplace_back(listA);
  iterators.emplace_back(listB);
  iterators.emplace_back(listC);

  auto matches = LeapfrogJoin::intersect(iterators);
  ASSERT_EQ(matches.size(), 1u);
  EXPECT_EQ(matches[0], Id::makeFromInt(20));
}

TEST(LeapfrogTriejoinTest, DisjointListsEmptyResult) {
  std::vector<Id> listA = {Id::makeFromInt(1), Id::makeFromInt(2)};
  std::vector<Id> listB = {Id::makeFromInt(3), Id::makeFromInt(4)};

  std::vector<LeapfrogIterator> iterators;
  iterators.emplace_back(listA);
  iterators.emplace_back(listB);

  auto matches = LeapfrogJoin::intersect(iterators);
  EXPECT_TRUE(matches.empty());
}
