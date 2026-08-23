// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

//   Copyright 2026, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Marvin Stötzel <stoetzem@informatik.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>

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
                         ::testing::Values("param1"));

// _____________________________________________________________________________
TEST(GTestHelpersTest, AssertPmrStringUsesSso) {
  // SSO should hold for empty and small strings.
  EXPECT_NO_THROW(assertPmrStringUsesSso(0));
  EXPECT_NO_THROW(assertPmrStringUsesSso(7));
  EXPECT_NO_THROW(assertPmrStringUsesSso(15));
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, ClobberStack) {
  EXPECT_NO_THROW(clobberStack<512>('X'));
  EXPECT_NO_THROW(clobberStack<4096>('#'));
}
