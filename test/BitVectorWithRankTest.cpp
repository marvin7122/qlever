// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <optional>
#include <random>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/BitVectorWithRank.h"
#include "util/GTestHelpers.h"

using ad_utility::BitVectorWithRank;

namespace {

constexpr uint64_t numberOfBitsPerBlock =
    BitVectorWithRank::numberOfBitsPerBlock;
constexpr uint64_t middleBit = BitVectorWithRank::middleBit;

// Check every element of `[0, universeSize + 2)` against the definition: a
// contained element has the rank of its position in `sortedValues`, all others
// (also those at or beyond the end of the universe) have no rank.
void expectRanksOfAllValues(const std::vector<uint64_t>& sortedValues,
                            uint64_t universeSize) {
  BitVectorWithRank set{sortedValues, universeSize};
  EXPECT_EQ(set.universeSize(), universeSize);
  size_t numberOfSmallerValues = 0;
  for (uint64_t element = 0; element < universeSize + 2; ++element) {
    const bool isContained = numberOfSmallerValues < sortedValues.size() &&
                             sortedValues[numberOfSmallerValues] == element;
    if (isContained) {
      ASSERT_EQ(set.rankIfContained(element),
                std::optional<uint64_t>{numberOfSmallerValues})
          << element;
      ++numberOfSmallerValues;
    } else {
      ASSERT_EQ(set.rankIfContained(element), std::nullopt) << element;
    }
  }
}

// The sorted values of `[0, universeSize)` for which `isContained` is true.
template <typename Predicate>
std::vector<uint64_t> valuesWhere(uint64_t universeSize,
                                  Predicate isContained) {
  std::vector<uint64_t> values;
  for (uint64_t element = 0; element < universeSize; ++element) {
    if (isContained(element)) {
      values.push_back(element);
    }
  }
  return values;
}

}  // namespace

// _____________________________________________________________________________
TEST(BitVectorWithRank, Empty) {
  // The empty universe.
  BitVectorWithRank defaultConstructed;
  EXPECT_EQ(defaultConstructed.universeSize(), 0);
  EXPECT_EQ(defaultConstructed.numberOfBytes(), 0);
  EXPECT_EQ(defaultConstructed.rankIfContained(0), std::nullopt);
  EXPECT_EQ(
      defaultConstructed.rankIfContained(std::numeric_limits<uint64_t>::max()),
      std::nullopt);
  expectRanksOfAllValues({}, 0);

  // The empty set over a universe of several blocks.
  expectRanksOfAllValues({}, 3 * numberOfBitsPerBlock + 5);
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, AllValuesContained) {
  for (const uint64_t universeSize :
       {uint64_t{1}, middleBit, numberOfBitsPerBlock, numberOfBitsPerBlock + 1,
        5 * numberOfBitsPerBlock - 1}) {
    expectRanksOfAllValues(
        valuesWhere(universeSize, [](uint64_t) { return true; }), universeSize);
  }
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, BlockAndWordBoundaries) {
  // The values next to the start, the middle bit, and the end of each block,
  // and next to each word boundary.
  const uint64_t universeSize = 4 * numberOfBitsPerBlock;
  auto isNearBoundary = [](uint64_t element) {
    const uint64_t offset = element % numberOfBitsPerBlock;
    const uint64_t offsetInWord = offset % 64;
    return offset <= 1 || offset + 1 >= numberOfBitsPerBlock ||
           offset + 1 == middleBit || offset == middleBit ||
           offsetInWord == 0 || offsetInWord == 63;
  };
  expectRanksOfAllValues(valuesWhere(universeSize, isNearBoundary),
                         universeSize);
  // The complement: the values next to the boundaries are holes.
  expectRanksOfAllValues(
      valuesWhere(universeSize,
                  [&](uint64_t element) { return !isNearBoundary(element); }),
      universeSize);
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, LastPartialBlock) {
  // A universe that ends inside the first half, at the middle bit, and inside
  // the second half of the last block. Its last element is contained.
  for (const uint64_t lastOffset : {uint64_t{0}, uint64_t{63}, middleBit - 1,
                                    middleBit, numberOfBitsPerBlock - 2}) {
    const uint64_t universeSize = 2 * numberOfBitsPerBlock + lastOffset + 1;
    auto values = valuesWhere(
        universeSize, [](uint64_t element) { return element % 3 == 0; });
    if (values.back() != universeSize - 1) {
      values.push_back(universeSize - 1);
    }
    expectRanksOfAllValues(values, universeSize);
    BitVectorWithRank set{values, universeSize};
    EXPECT_EQ(set.numberOfBytes(), 3 * 64);
  }
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, Memory) {
  // 64 bytes per started block of 448 values, independent of the values.
  EXPECT_EQ(BitVectorWithRank({}, 1).numberOfBytes(), 64);
  EXPECT_EQ(BitVectorWithRank({}, numberOfBitsPerBlock).numberOfBytes(), 64);
  EXPECT_EQ(BitVectorWithRank({}, numberOfBitsPerBlock + 1).numberOfBytes(),
            128);
  const std::vector<uint64_t> values{0, 1, 2};
  EXPECT_EQ(
      BitVectorWithRank(values, 100 * numberOfBitsPerBlock).numberOfBytes(),
      100 * 64);

  // The same without building the set, also for universes that are too large
  // to build.
  for (const uint64_t universeSize :
       {uint64_t{0}, uint64_t{1}, numberOfBitsPerBlock,
        numberOfBitsPerBlock + 1, 100 * numberOfBitsPerBlock}) {
    EXPECT_EQ(BitVectorWithRank::numberOfBytesForUniverse(universeSize),
              BitVectorWithRank({}, universeSize).numberOfBytes());
  }
  // The 1,580,300,680 vocabulary indices of Wikidata.
  EXPECT_EQ(BitVectorWithRank::numberOfBytesForUniverse(1'580'300'680),
            225'757'248);
  EXPECT_EQ(
      BitVectorWithRank::numberOfBytesForUniverse(
          std::numeric_limits<uint64_t>::max()),
      (std::numeric_limits<uint64_t>::max() / numberOfBitsPerBlock + 1) * 64);
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, RandomSetsMatchBinarySearch) {
  std::mt19937_64 randomEngine{42};
  // Sparse (like the in-RAM words of `VocabularyInternalExternal`), medium,
  // and dense sets.
  for (const double fractionContained : {0.001, 0.05, 0.5, 0.99}) {
    std::bernoulli_distribution isContained{fractionContained};
    const uint64_t universeSize = 50 * numberOfBitsPerBlock + 17;
    const auto values = valuesWhere(
        universeSize, [&](uint64_t) { return isContained(randomEngine); });
    BitVectorWithRank set{values, universeSize};
    for (uint64_t element = 0; element < universeSize; ++element) {
      const auto it = ql::ranges::lower_bound(values, element);
      const std::optional<uint64_t> expected =
          it != values.end() && *it == element
              ? std::optional<uint64_t>{static_cast<uint64_t>(it -
                                                              values.begin())}
              : std::nullopt;
      ASSERT_EQ(set.rankIfContained(element), expected)
          << element << ' ' << fractionContained;
    }
  }
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, Move) {
  const std::vector<uint64_t> values{3, 500, 1000};
  BitVectorWithRank set{values, 1001};
  BitVectorWithRank moved{std::move(set)};
  EXPECT_EQ(moved.rankIfContained(500), std::optional<uint64_t>{1});
  // A moved-from set is the empty set over the empty universe.
  EXPECT_EQ(set.universeSize(), 0);
  EXPECT_EQ(set.rankIfContained(500), std::nullopt);
  BitVectorWithRank assigned;
  assigned = std::move(moved);
  EXPECT_EQ(assigned.rankIfContained(1000), std::optional<uint64_t>{2});
  EXPECT_EQ(assigned.rankIfContained(999), std::nullopt);
  EXPECT_EQ(moved.universeSize(), 0);
  EXPECT_EQ(moved.rankIfContained(500), std::nullopt);
  static_assert(!std::is_copy_constructible_v<BitVectorWithRank>);
  static_assert(!std::is_copy_assignable_v<BitVectorWithRank>);
}

// _____________________________________________________________________________
TEST(BitVectorWithRank, ContractViolations) {
  using V = std::vector<uint64_t>;
  // An element outside of the universe.
  AD_EXPECT_THROW_WITH_MESSAGE(BitVectorWithRank(V{1, 10}, 10),
                               ::testing::HasSubstr("universeSize_"));
  // Not strictly ascending.
  AD_EXPECT_THROW_WITH_MESSAGE(BitVectorWithRank(V{1, 1}, 10),
                               ::testing::HasSubstr("previous"));
  AD_EXPECT_THROW_WITH_MESSAGE(BitVectorWithRank(V{5, 2}, 10),
                               ::testing::HasSubstr("previous"));
}
