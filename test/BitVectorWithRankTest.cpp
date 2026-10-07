// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <limits>
#include <numeric>
#include <optional>
#include <random>
#include <vector>

#include "util/BitVectorWithRank.h"

using ad_utility::BitVectorWithRank;

namespace {

// Check `rankIfContained` for every value in `[0, universeSize + 70)` against
// the definition: the position of the value in `sortedValues`, if contained.
void expectRanksMatchDefinition(const std::vector<uint64_t>& sortedValues,
                                uint64_t universeSize) {
  BitVectorWithRank bits{sortedValues, universeSize};
  EXPECT_EQ(bits.universeSize(), universeSize);
  EXPECT_EQ(bits.numBytes(), 64 * ((universeSize + 447) / 448));
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
      if (value % 64 == 0 || value % 64 == 63 || value % 448 == 447 ||
          value + 1 == universeSize) {
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
