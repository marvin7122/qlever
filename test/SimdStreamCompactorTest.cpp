// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <vector>

#include "engine/SimdStreamCompactor.h"
#include "global/Id.h"

using namespace ql::engine::vector;

TEST(SimdStreamCompactorTest, CompactEvenNumbers) {
  std::vector<Id> input;
  input.reserve(100);
  for (int i = 0; i < 100; ++i) {
    input.push_back(Id::makeFromInt(i));
  }

  std::vector<Id> output(100);
  size_t count = SimdStreamCompactor::compact(input, output, [](Id id) {
    return id.getInt() % 2 == 0;
  });

  EXPECT_EQ(count, 50u);
  for (size_t i = 0; i < count; ++i) {
    EXPECT_EQ(output[i], Id::makeFromInt(static_cast<int>(i * 2)));
  }
}
