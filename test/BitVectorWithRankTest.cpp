// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <numeric>
#include <optional>
#include <random>
#include <vector>

#include "util/BitVectorWithRank.h"
#include "util/HugePages.h"

using ad_utility::BitVectorWithRank;

namespace {

// Check `rankIfContained` of `bits` (built from `sortedValues` and
// `universeSize`), see below.
void expectRanksMatchDefinitionImpl(const BitVectorWithRank& bits,
                                    const std::vector<uint64_t>& sortedValues,
                                    uint64_t universeSize) {
  size_t nextPosition = 0;
  for (uint64_t value = 0; value < universeSize + 70; ++value) {
    std::optional<uint64_t> expected;
    if (nextPosition < sortedValues.size() &&
        sortedValues[nextPosition] == value) {
      expected = nextPosition;
      ++nextPosition;
    }
    ASSERT_EQ(bits.rankIfContained(value), expected) << "value " << value;
  }
  EXPECT_EQ(bits.rankIfContained(std::numeric_limits<uint64_t>::max()),
            std::nullopt);
  // Prefetching is only a hint, also outside the universe.
  bits.prefetch(0);
  bits.prefetch(universeSize);
  bits.prefetch(std::numeric_limits<uint64_t>::max());
}

// Check `rankIfContained` for every value in `[0, universeSize + 70)` against
// the definition: the position of the value in `sortedValues`, if contained.
// Both with and without huge pages, which only change the allocation.
void expectRanksMatchDefinition(const std::vector<uint64_t>& sortedValues,
                                uint64_t universeSize) {
  for (bool useHugePages : {false, true}) {
    BitVectorWithRank bits{sortedValues, universeSize, useHugePages};
    const size_t numBytes = 64 * ((universeSize + 447) / 448);
    EXPECT_EQ(bits.universeSize(), universeSize);
    EXPECT_EQ(bits.numBytes(), numBytes);
    const size_t alignment =
        useHugePages ? BitVectorWithRank::hugePageSize : size_t{64};
    EXPECT_EQ(bits.numAllocatedBytes(),
              (numBytes + alignment - 1) / alignment * alignment);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(bits.allocationBegin()) % alignment,
              0);
    expectRanksMatchDefinitionImpl(bits, sortedValues, universeSize);
  }
}

// A random strictly ascending subset of `[0, universeSize)`, each value
// contained with the given `density`.
std::vector<uint64_t> randomSubset(uint64_t universeSize, double density,
                                   uint64_t seed) {
  std::mt19937_64 gen{seed};
  std::bernoulli_distribution contained{density};
  std::vector<uint64_t> result;
  for (uint64_t value = 0; value < universeSize; ++value) {
    if (contained(gen)) {
      result.push_back(value);
    }
  }
  return result;
}

}  // namespace

// _____________________________________________________________________________
TEST(BitVectorWithRank, Empty) {
  BitVectorWithRank defaultConstructed;
  EXPECT_EQ(defaultConstructed.universeSize(), 0);
  EXPECT_EQ(defaultConstructed.numBytes(), 0);
  EXPECT_EQ(defaultConstructed.rankIfContained(0), std::nullopt);
  expectRanksMatchDefinition({}, 0);
  expectRanksMatchDefinition({}, 1000);
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, BlockAndWordBoundaries) {
  // Values at the first and last bit of every word of the first blocks, and
  // the last value of the universe.
  for (uint64_t universeSize : {1ULL, 63ULL, 64ULL, 65ULL, 447ULL, 448ULL,
                                449ULL, 896ULL, 897ULL, 2000ULL}) {
    std::vector<uint64_t> values;
    for (uint64_t value = 0; value < universeSize; ++value) {
      // First and last bit of every word, the two values around the middle
      // anchor of every block (bits 223 and 224, a genuinely block-level
      // boundary), and the last value of the universe.
      if (value % 64 == 0 || value % 64 == 63 || value % 448 == 223 ||
          value % 448 == 224 || value + 1 == universeSize) {
        values.push_back(value);
      }
    }
    expectRanksMatchDefinition(values, universeSize);
  }
  // All values, and only the last one.
  std::vector<uint64_t> all(1500);
  std::iota(all.begin(), all.end(), uint64_t{0});
  expectRanksMatchDefinition(all, 1500);
  expectRanksMatchDefinition({1499}, 1500);
  // The universe may be larger than the largest value.
  expectRanksMatchDefinition({0, 5}, 3000);
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, MiddleAnchorBoundaries) {
  // Values around the middle anchor (bit 224 of each block, counted down to
  // from below and up to from above), around the word that holds the anchor
  // (bits 192-255), and the same pattern shifted into later blocks.
  for (uint64_t blockBase : {0ULL, 448ULL, 896ULL}) {
    for (uint64_t delta :
         {0ULL, 63ULL, 64ULL, 127ULL, 191ULL, 192ULL, 223ULL, 224ULL, 225ULL,
          255ULL, 256ULL, 319ULL, 383ULL, 384ULL, 447ULL}) {
      const uint64_t value = blockBase + delta;
      expectRanksMatchDefinition({value}, value + 1);
    }
    expectRanksMatchDefinition(
        {blockBase + 223, blockBase + 224, blockBase + 225}, blockBase + 226);
  }
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, RandomSparseAndDense) {
  for (double density : {0.001, 0.05, 0.5, 0.95, 1.0}) {
    for (uint64_t seed : {1ULL, 2ULL, 3ULL}) {
      uint64_t universeSize = 5000 + seed * 333;
      expectRanksMatchDefinition(randomSubset(universeSize, density, seed),
                                 universeSize);
    }
  }
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, InvalidInputThrows) {
  std::vector<uint64_t> notAscending{3, 2};
  EXPECT_THROW((BitVectorWithRank{notAscending, 10}), ad_utility::Exception);
  std::vector<uint64_t> repeated{2, 2};
  EXPECT_THROW((BitVectorWithRank{repeated, 10}), ad_utility::Exception);
  std::vector<uint64_t> outOfRange{2, 10};
  EXPECT_THROW((BitVectorWithRank{outOfRange, 10}), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, MoveLeavesEmptySet) {
  std::vector<uint64_t> values{1, 500};
  BitVectorWithRank bits{values, 1000, true};
  BitVectorWithRank moved{std::move(bits)};
  EXPECT_EQ(moved.rankIfContained(500), 1);
  EXPECT_EQ(bits.universeSize(), 0);
  EXPECT_EQ(bits.rankIfContained(500), std::nullopt);
  bits = std::move(moved);
  EXPECT_EQ(bits.rankIfContained(500), 1);
  EXPECT_EQ(moved.numBytes(), 0);
}

// _____________________________________________________________________________
TEST(HugePages, ModeAndAnonHugePageBytes) {
  using namespace ad_utility;
  EXPECT_EQ(selectedTransparentHugePagesMode("always [madvise] never\n"),
            "madvise");
  EXPECT_EQ(selectedTransparentHugePagesMode("[always] madvise never"),
            "always");
  EXPECT_EQ(selectedTransparentHugePagesMode("garbage"), "unknown");
  auto mode = transparentHugePagesMode();
  EXPECT_TRUE(mode == "always" || mode == "madvise" || mode == "never" ||
              mode == "unknown")
      << mode;
#if defined(__linux__)
  // On Linux, `/proc/self/smaps` is readable; the result is at most the
  // intersection of the range with the overlapping mappings.
  std::vector<uint64_t> values{0, 1'000'000};
  BitVectorWithRank bits{values, 4'000'000, true};
  auto hugeBytes =
      anonHugePageBytes(bits.allocationBegin(), bits.numAllocatedBytes());
  ASSERT_TRUE(hugeBytes.has_value());
  if (mode == "never") {
    EXPECT_EQ(hugeBytes.value(), 0);
  }
  EXPECT_EQ(anonHugePageBytes(nullptr, 0), 0);
#else
  // Without `/proc/self/smaps` (e.g. on macOS) there is no huge-page
  // information, so the result is always `std::nullopt`.
  EXPECT_EQ(anonHugePageBytes(nullptr, 0), std::nullopt);
#endif
}
