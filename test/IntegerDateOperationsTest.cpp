// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <vector>

#include "engine/sparqlExpressions/IntegerDateOperations.h"
#include "global/Id.h"

using namespace ql::engine::scalar;

TEST(IntegerDateOperationsTest, ScalarDateExtraction) {
  auto dateId = Id::makeFromDate(DateYearOrDuration{Date{2026, 9, 3, 15, 30, 0.0}});

  EXPECT_EQ(IntegerDateOperations::extractYear(dateId), 2026);
  EXPECT_EQ(IntegerDateOperations::extractMonth(dateId), 9);
  EXPECT_EQ(IntegerDateOperations::extractDay(dateId), 3);
}

TEST(IntegerDateOperationsTest, NonDateReturnsZero) {
  auto intId = Id::makeFromInt(42);
  EXPECT_EQ(IntegerDateOperations::extractYear(intId), 0);
  EXPECT_EQ(IntegerDateOperations::extractMonth(intId), 0);
  EXPECT_EQ(IntegerDateOperations::extractDay(intId), 0);
}

TEST(IntegerDateOperationsTest, BatchYearExtraction) {
  std::vector<Id> dates = {
      Id::makeFromDate(DateYearOrDuration{Date{1999, 12, 31, 0, 0, 0.0}}),
      Id::makeFromDate(DateYearOrDuration{Date{2000, 1, 1, 0, 0, 0.0}}),
      Id::makeFromDate(DateYearOrDuration{Date{2026, 9, 3, 0, 0, 0.0}}),
  };

  std::vector<int64_t> years(dates.size(), 0);
  IntegerDateOperations::extractYearsBatch(dates, years);

  ASSERT_EQ(years.size(), 3u);
  EXPECT_EQ(years[0], 1999);
  EXPECT_EQ(years[1], 2000);
  EXPECT_EQ(years[2], 2026);
}
