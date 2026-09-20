// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "engine/RleVectorStream.h"
#include "global/Id.h"

using namespace ql::engine::rle;

TEST(RleVectorStreamTest, AppendAndMaterialize) {
  RleVectorStream stream;

  stream.append(Id::makeFromInt(1), 100);
  stream.append(Id::makeFromInt(2), 50);
  stream.append(Id::makeFromInt(2), 50);  // merges with previous run
  stream.append(Id::makeFromInt(3), 200);

  EXPECT_EQ(stream.numRuns(), 3u);
  EXPECT_EQ(stream.totalRows(), 400u);

  std::vector<Id> dest(400);
  stream.materialize(dest);

  for (size_t i = 0; i < 100; ++i) {
    EXPECT_EQ(dest[i], Id::makeFromInt(1));
  }
  for (size_t i = 100; i < 200; ++i) {
    EXPECT_EQ(dest[i], Id::makeFromInt(2));
  }
  for (size_t i = 200; i < 400; ++i) {
    EXPECT_EQ(dest[i], Id::makeFromInt(3));
  }
}

TEST(RleVectorStreamTest, AppendEdgeCases) {
  RleVectorStream stream;
  // Zero-length appends still record the value without growing the row count.
  stream.append(Id::makeFromInt(7), 0);
  EXPECT_EQ(stream.totalRows(), 0u);

  // Merging past UINT32_MAX saturates the run and spills into a fresh one.
  stream.append(Id::makeFromInt(9), std::numeric_limits<uint32_t>::max());
  stream.append(Id::makeFromInt(9), 10);
  ASSERT_EQ(stream.numRuns(), 3u);
  EXPECT_EQ(stream.runs()[1].length_, std::numeric_limits<uint32_t>::max());
  EXPECT_EQ(stream.runs()[2].length_, 10u);
  EXPECT_EQ(stream.totalRows(),
            static_cast<size_t>(std::numeric_limits<uint32_t>::max()) + 10u);
}

TEST(RleVectorStreamTest, MaterializeExactlySizedDestination) {
  RleVectorStream stream;
  stream.append(Id::makeFromInt(4), 3);
  stream.append(Id::makeFromInt(5), 2);

  // Destination sized exactly to the row count, no slack.
  std::vector<Id> dest(stream.totalRows());
  stream.materialize(dest);

  EXPECT_EQ(dest[0], Id::makeFromInt(4));
  EXPECT_EQ(dest[2], Id::makeFromInt(4));
  EXPECT_EQ(dest[3], Id::makeFromInt(5));
  EXPECT_EQ(dest[4], Id::makeFromInt(5));
}
