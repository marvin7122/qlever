// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>

#include <cstdint>
#include <memory_resource>

#include "util/GTestHelpers.h"

TEST(GTestHelpersTest, CurrentTestSuiteAndTestName) {
  EXPECT_EQ(gtestCurrentTestSuiteName(), "GTestHelpersTest");
  EXPECT_EQ(gtestCurrentTestName(),
            "GTestHelpersTest_CurrentTestSuiteAndTestName");
  EXPECT_EQ(gtestCurrentTestSuiteName(false), "GTestHelpersTest");
  EXPECT_EQ(gtestCurrentTestName(false),
            "GTestHelpersTest_CurrentTestSuiteAndTestName");
}

// _____________________________________________________________________________
class GTestHelpersParameterizedTest
    : public ::testing::TestWithParam<const char*> {};

TEST_P(GTestHelpersParameterizedTest, SlashesAreReplaced) {
  const std::string suiteName = gtestCurrentTestSuiteName();
  const std::string testName = gtestCurrentTestName();
  EXPECT_THAT(suiteName, ::testing::Not(::testing::HasSubstr("/")));
  EXPECT_THAT(testName, ::testing::Not(::testing::HasSubstr("/")));
  EXPECT_THAT(testName,
              ::testing::StartsWith(gtestCurrentTestSuiteName() + "_"));
}

INSTANTIATE_TEST_SUITE_P(CustomInstantiation, GTestHelpersParameterizedTest,
                         ::testing::Values("param/1"));

// _____________________________________________________________________________
// Return true iff `pointer` points inside the object storage of `object`.
template <typename T>
static bool pointsIntoObject(const void* pointer, const T& object) {
  const auto start = reinterpret_cast<std::uintptr_t>(&object);
  const auto address = reinterpret_cast<std::uintptr_t>(pointer);
  return address >= start && address - start < sizeof(object);
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, PmrStringSsoCapacity) {
    // Verify that a string of exactly the discovered capacity is stored inside
  // the object, and that one with one additional character is not.
  size_t capacity = pmrStringSsoCapacity();
  requirePmrStringInlineStorage(capacity);
  std::pmr::string atCapacity(capacity, 'x');
  EXPECT_TRUE(pointsIntoObject(atCapacity.data(), atCapacity));
  std::pmr::string aboveCapacity(capacity + 1, 'y');
  EXPECT_FALSE(pointsIntoObject(aboveCapacity.data(), aboveCapacity));
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, AssertPmrStringUsesSso) {
  
  for (size_t size : {size_t{0}, size_t{7}, size_t{15}}) {
    // `maxSize == 0` is a rejected precondition, so probe from 1 on.
    if (size > 0) {
      requirePmrStringInlineStorage(size);
    }
    std::pmr::string shortString(size, 'x');
    EXPECT_TRUE(pointsIntoObject(shortString.data(), shortString));
    EXPECT_EQ(shortString.size(), size);
  }
  
  std::pmr::string longString(64, 'y');
  EXPECT_FALSE(pointsIntoObject(longString.data(), longString));
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, ClobberStack) {
  
  EXPECT_EQ(clobberStack<512>('X'), 'X');
  EXPECT_EQ(clobberStack<4096>('#'), '#');
}
