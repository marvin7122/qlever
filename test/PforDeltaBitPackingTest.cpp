// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <vector>

#include "global/Id.h"
#include "index/PforDeltaBitPacking.h"

using namespace ql::index::compression;

TEST(PforDeltaBitPackingTest, CompressAndDecompressMonotonicIds) {
  std::vector<Id> inputIds;
  inputIds.reserve(64);

  // Generate 64 sorted monotonic IDs starting at base 1,000,000 with small deltas
  for (uint64_t i = 0; i < 64; ++i) {
    inputIds.push_back(Id::fromBits(1'000'000 + i * 3));
  }

  auto compressed = PforDeltaBitPacking::compressBlock(inputIds);

  // Max delta is 63 * 3 = 189 -> requires only 8 bits per ID!
  EXPECT_LE(compressed.bitWidth_, 8);

  std::vector<Id> decompressed(64);
  PforDeltaBitPacking::decompressBlock(compressed, 64, decompressed);

  for (size_t i = 0; i < 64; ++i) {
    EXPECT_EQ(decompressed[i], inputIds[i]);
  }
}
