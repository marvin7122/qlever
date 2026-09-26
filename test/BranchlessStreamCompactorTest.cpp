// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <vector>

#include "engine/BranchlessStreamCompactor.h"
#include "global/Id.h"

using namespace ql::engine::vector;

TEST(BranchlessStreamCompactorTest, CompactEvenNumbers) {
  std::vector<Id> input;
  input.reserve(100);
  for (int i = 0; i < 100; ++i) {
    input.push_back(Id::makeFromInt(i));
  }

  std::vector<Id> output(100);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });

  EXPECT_EQ(count, 50u);
  for (size_t i = 0; i < count; ++i) {
    EXPECT_EQ(output[i], Id::makeFromInt(static_cast<int>(i * 2)));
  }
}

TEST(BranchlessStreamCompactorTest, EmptyInputReturnsZero) {
  std::vector<Id> input;
  std::vector<Id> output(4);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });
  EXPECT_EQ(count, 0u);
}

TEST(BranchlessStreamCompactorTest, AllElementsMatch) {
  std::vector<Id> input;
  for (int i = 0; i < 10; ++i) {
    input.push_back(Id::makeFromInt(2 * i));
  }
  std::vector<Id> output(10);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });
  EXPECT_EQ(count, 10u);
  for (size_t i = 0; i < count; ++i) {
    EXPECT_EQ(output[i], Id::makeFromInt(static_cast<int>(2 * i)));
  }
}

TEST(BranchlessStreamCompactorTest, NoElementsMatch) {
  std::vector<Id> input;
  for (int i = 0; i < 10; ++i) {
    input.push_back(Id::makeFromInt(2 * i + 1));
  }
  std::vector<Id> output(10);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });
  EXPECT_EQ(count, 0u);
}

TEST(BranchlessStreamCompactorTest, SizeNotDivisibleByFourUsesEpilogue) {
  // 7 elements: 4 via the unrolled loop, 3 via the scalar epilogue.
  std::vector<Id> input;
  for (int i = 0; i < 7; ++i) {
    input.push_back(Id::makeFromInt(i));
  }
  std::vector<Id> output(7);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });
  ASSERT_EQ(count, 4u);
  for (size_t i = 0; i < count; ++i) {
    EXPECT_EQ(output[i], Id::makeFromInt(static_cast<int>(i * 2)));
  }
}

TEST(BranchlessStreamCompactorTest, SmallerThanUnrollWidthSkipsLoop) {
  // 3 elements: the unrolled loop is skipped entirely.
  std::vector<Id> input;
  for (int i = 0; i < 3; ++i) {
    input.push_back(Id::makeFromInt(i));
  }
  std::vector<Id> output(3);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });
  ASSERT_EQ(count, 2u);
  EXPECT_EQ(output[0], Id::makeFromInt(0));
  EXPECT_EQ(output[1], Id::makeFromInt(2));
}
