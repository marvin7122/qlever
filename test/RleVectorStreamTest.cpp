// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <limits>
#include <utility>
#include <vector>

#include "engine/RleVectorStream.h"
#include "global/Id.h"

using namespace ql::engine::rle;

// _____________________________________________________________________________
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
  // Appending zero rows changes nothing.
  stream.append(Id::makeFromInt(7), 0);
  EXPECT_EQ(stream.numRuns(), 0u);
  EXPECT_EQ(stream.totalRows(), 0u);

  // Merging past UINT32_MAX saturates the run and spills into a fresh one.
  constexpr uint32_t max = std::numeric_limits<uint32_t>::max();
  stream.append(Id::makeFromInt(9), max - 5);
  stream.append(Id::makeFromInt(9), 15);
  ASSERT_EQ(stream.numRuns(), 2u);
  EXPECT_EQ(stream.runs()[0].length_, max);
  EXPECT_EQ(stream.runs()[1].length_, 10u);
  EXPECT_EQ(stream.totalRows(), static_cast<size_t>(max) + 10u);

  // A moved-from stream is empty.
  RleVectorStream moved{std::move(stream)};
  EXPECT_EQ(moved.numRuns(), 2u);
  EXPECT_EQ(stream.numRuns(), 0u);
  EXPECT_EQ(stream.totalRows(), 0u);
}

// _____________________________________________________________________________
TEST(RleVectorStreamTest, MaterializeIntoTooSmallDestinationThrows) {
  RleVectorStream stream;
  stream.append(Id::makeFromInt(4), 3);
  std::vector<Id> dest(2);
  EXPECT_ANY_THROW(stream.materialize(dest));
}

// _____________________________________________________________________________
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

// _____________________________________________________________________________
TEST(RleVectorStreamTest, FromColumn) {
  auto I = Id::makeFromInt;
  std::vector<Id> column{I(1), I(1), I(1), I(2), I(3), I(3), I(1)};

  auto stream = RleVectorStream::fromColumn(column, 4);
  ASSERT_TRUE(stream.has_value());
  EXPECT_EQ(stream->numRuns(), 4u);
  EXPECT_EQ(stream->totalRows(), column.size());
  std::vector<std::pair<Id, uint32_t>> runs;
  for (const auto& run : stream->runs()) {
    runs.emplace_back(run.value_, run.length_);
  }
  EXPECT_EQ(runs, (std::vector<std::pair<Id, uint32_t>>{
                      {I(1), 3}, {I(2), 1}, {I(3), 2}, {I(1), 1}}));
  std::vector<Id> materialized(column.size());
  stream->materialize(materialized);
  EXPECT_EQ(materialized, column);

  // One run more than allowed.
  EXPECT_FALSE(RleVectorStream::fromColumn(column, 3).has_value());

  // An empty column has no runs, also with a limit of zero.
  auto empty = RleVectorStream::fromColumn({}, 0);
  ASSERT_TRUE(empty.has_value());
  EXPECT_EQ(empty->numRuns(), 0u);
  EXPECT_FALSE(RleVectorStream::fromColumn(column, 0).has_value());
}
