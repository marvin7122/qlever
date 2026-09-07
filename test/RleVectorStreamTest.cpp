// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

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

// Boundary tests required by rule:tests - test empty input and edge cases
TEST(RleVectorStreamTest, EmptyStream) {
  RleVectorStream stream;
  EXPECT_EQ(stream.numRuns(), 0u);
  EXPECT_EQ(stream.totalRows(), 0u);
  
  std::vector<Id> dest(0);
  stream.materialize(dest);
  EXPECT_TRUE(dest.empty());
}

TEST(RleVectorStreamTest, SingleElement) {
  RleVectorStream stream;
  stream.append(Id::makeFromInt(42), 1);
  EXPECT_EQ(stream.numRuns(), 1u);
  EXPECT_EQ(stream.totalRows(), 1u);
  
  std::vector<Id> dest(1);
  stream.materialize(dest);
  EXPECT_EQ(dest[0], Id::makeFromInt(42));
}

TEST(RleVectorStreamTest, MergeIdenticalAdjacentRuns) {
  RleVectorStream stream;
  stream.append(Id::makeFromInt(1), 100);
  stream.append(Id::makeFromInt(1), 100);  // merges
  EXPECT_EQ(stream.numRuns(), 1u);
  EXPECT_EQ(stream.totalRows(), 200u);
}

TEST(RleVectorStreamTest, BuilderFinalization) {
  RleVectorStream stream = std::move(
    RleVectorStream::Builder(&stream).add(Id::makeFromInt(1), 100)
                                  .add(Id::makeFromInt(2), 200)
                                  .build()
  );
  EXPECT_EQ(stream.numRuns(), 2u);
  EXPECT_EQ(stream.totalRows(), 300u);
}
