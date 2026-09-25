// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <vector>

#include "global/Id.h"
#include "index/PforDeltaBitPacking.h"
#include "util/GTestHelpers.h"

using ql::index::compression::PforDeltaBitPacking;

namespace {
// Compress `ids`, check the bit width, and check that decompression restores
// `ids` exactly.
void expectRoundTrip(const std::vector<Id>& ids, uint8_t expectedBitWidth,
                     ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto compressed = PforDeltaBitPacking::compressBlock(ids);
  EXPECT_EQ(compressed.bitWidth_, expectedBitWidth);
  EXPECT_EQ(compressed.numValues_, ids.size());
  std::vector<Id> decompressed(ids.size());
  PforDeltaBitPacking::decompressBlock(compressed, decompressed);
  EXPECT_EQ(decompressed, ids);
}

// `n` `Id`s with bits `start + i * step`.
std::vector<Id> arithmetic(uint64_t start, uint64_t step, size_t n = 64) {
  std::vector<Id> ids;
  for (uint64_t i = 0; i < n; ++i) {
    ids.push_back(Id::fromBits(start + i * step));
  }
  return ids;
}
}  // namespace

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, MonotonicIds) {
  // Largest offset 63 * 3 = 189 needs 8 bits.
  expectRoundTrip(arithmetic(1'000'000, 3), 8);
  // Largest offset 63 * 2 = 126 needs 7 bits; values straddle word boundaries.
  expectRoundTrip(arithmetic(1'000'000, 2), 7);
}

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, BoundaryWidthsAndSizes) {
  // All values equal: zero bits, no packed words.
  auto constant = arithmetic(42, 0);
  expectRoundTrip(constant, 0);
  EXPECT_TRUE(
      PforDeltaBitPacking::compressBlock(constant).packedWords_.empty());
  // A single value.
  expectRoundTrip(arithmetic(7, 1, 1), 0);
  // Full 64-bit offsets.
  expectRoundTrip(
      {Id::fromBits(0), Id::fromBits(~uint64_t{0}), Id::fromBits(1)}, 64);
  // A partial block.
  expectRoundTrip(arithmetic(100, 5, 10), 6);
}

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, UnsortedIdsUseTheMinimumAsBase) {
  // The base is the smallest value, so unsorted input still packs tightly.
  std::vector<Id> ids{Id::fromBits(1'010), Id::fromBits(1'000),
                      Id::fromBits(1'015), Id::fromBits(1'003)};
  expectRoundTrip(ids, 4);
  EXPECT_EQ(PforDeltaBitPacking::compressBlock(ids).baseValue_,
            Id::fromBits(1'000));
}

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, ContractViolations) {
  EXPECT_ANY_THROW(PforDeltaBitPacking::compressBlock(std::vector<Id>{}));
  EXPECT_ANY_THROW(PforDeltaBitPacking::compressBlock(arithmetic(0, 1, 65)));

  auto compressed = PforDeltaBitPacking::compressBlock(arithmetic(0, 1, 10));
  std::vector<Id> tooSmall(9);
  EXPECT_ANY_THROW(PforDeltaBitPacking::decompressBlock(compressed, tooSmall));

  std::vector<Id> output(64);
  auto corrupted = compressed;
  corrupted.numValues_ = 64;
  EXPECT_ANY_THROW(PforDeltaBitPacking::decompressBlock(corrupted, output));
}
