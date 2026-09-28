//
// Created by kalmbacj on 12/6/24.
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "backports/algorithm.h"

TEST(Range, Sort) {}

class AlgorithmBackportTest : public ::testing::Test {
 protected:
  void SetUp() override { testVector = {5, 3, 8, 1, 2, 7, 4, 6}; }
  std::vector<int> testVector;
};

// _____________________________________________________________________________
TEST_F(AlgorithmBackportTest, EraseSingleValue) {
  EXPECT_EQ(ql::backports::erase(testVector, 4), 1);
  std::vector<int> expected{5, 3, 8, 1, 2, 7, 6};
  EXPECT_EQ(testVector, expected);
}

// _____________________________________________________________________________
TEST_F(AlgorithmBackportTest, EraseIfRemovesCorrectElements) {
  auto isEven = [](int x) { return x % 2 == 0; };
  EXPECT_EQ(ql::backports::erase_if(testVector, isEven), 4);
  std::vector<int> expected{5, 3, 1, 7};
  EXPECT_EQ(testVector, expected);
}

// _____________________________________________________________________________
TEST(AlgorithmBackportMapTest, EraseIfRemovesMatchingEntries) {
  std::map<int, std::string> map{{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}};
  auto keyIsEven = [](const auto& entry) { return entry.first % 2 == 0; };
  EXPECT_EQ(ql::backports::erase_if(map, keyIsEven), 2);
  std::map<int, std::string> expected{{1, "a"}, {3, "c"}};
  EXPECT_EQ(map, expected);
  EXPECT_EQ(ql::backports::erase_if(map, keyIsEven), 0);
  EXPECT_EQ(map, expected);
}
