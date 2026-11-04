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
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "index/vocabulary/SortedVocabIndices.h"
#include "util/Views.h"

using ad_utility::vocabulary::PositionLookup;
using ad_utility::vocabulary::SortedVocabIndices;

namespace {

// Random strictly ascending indices with holes: each index of
// `[0, universeSize)` is contained with probability `fractionContained`.
std::vector<uint64_t> randomIndicesWithHoles(std::mt19937_64& randomEngine,
                                             uint64_t universeSize,
                                             double fractionContained) {
  std::bernoulli_distribution isContained{fractionContained};
  std::vector<uint64_t> indices;
  for (const uint64_t index : ad_utility::integerRange(universeSize)) {
    if (isContained(randomEngine)) {
      indices.push_back(index);
    }
  }
  return indices;
}

// Expect that the binary search and the rank directory find the same position
// for every index of `[0, endIndex() + 2)`, and that this position is the
// position in `indices`. The indices are once owned and once viewed.
void expectBothLookupsAgree(const std::vector<uint64_t>& indices) {
  using Storage = SortedVocabIndices::Storage;
  const SortedVocabIndices binarySearch{Storage{indices},
                                        PositionLookup::BinarySearch};
  const SortedVocabIndices rankDirectory{Storage{Storage::View{indices}},
                                         PositionLookup::RankDirectory};
  ASSERT_EQ(binarySearch.endIndex(), rankDirectory.endIndex());
  EXPECT_THAT(rankDirectory.view(), ::testing::ElementsAreArray(indices));
  size_t position = 0;
  for (const uint64_t index :
       ad_utility::integerRange(binarySearch.endIndex() + 2)) {
    const bool isContained =
        position < indices.size() && indices[position] == index;
    const auto expected =
        isContained ? std::optional<size_t>{position} : std::nullopt;
    ASSERT_EQ(binarySearch.positionOf(index), expected) << index;
    ASSERT_EQ(rankDirectory.positionOf(index), expected) << index;
    position += isContained;
  }
}

}  // namespace

// _____________________________________________________________________________
TEST(SortedVocabIndices, RandomHolePatterns) {
  std::mt19937_64 randomEngine{1234};
  for (const double fractionContained : {0.001, 0.01, 0.3, 0.9, 1.0}) {
    for (const uint64_t universeSize :
         {uint64_t{1}, uint64_t{447}, uint64_t{448}, uint64_t{20'000}}) {
      expectBothLookupsAgree(randomIndicesWithHoles(randomEngine, universeSize,
                                                    fractionContained));
    }
  }
}

// _____________________________________________________________________________
TEST(SortedVocabIndices, Empty) {
  expectBothLookupsAgree({});
  const SortedVocabIndices defaultConstructed;
  EXPECT_EQ(defaultConstructed.endIndex(), 0);
  EXPECT_EQ(defaultConstructed.positionOf(0), std::nullopt);
  EXPECT_EQ(defaultConstructed.numberOfBytesOfRankDirectory(), 0);
}

// _____________________________________________________________________________
TEST(SortedVocabIndices, MemoryOfRankDirectory) {
  using Storage = SortedVocabIndices::Storage;
  // A single index far out: the directory spans `[0, endIndex())`, 64 bytes
  // per 448 indices.
  const std::vector<uint64_t> indices{448 * 10 - 1};
  EXPECT_EQ(SortedVocabIndices(Storage{indices}, PositionLookup::RankDirectory)
                .numberOfBytesOfRankDirectory(),
            10 * 64);
  EXPECT_EQ(SortedVocabIndices(Storage{indices}, PositionLookup::BinarySearch)
                .numberOfBytesOfRankDirectory(),
            0);
}

// _____________________________________________________________________________
TEST(SortedVocabIndices, RankDirectoryIfSmaller) {
  using Storage = SortedVocabIndices::Storage;
  auto numberOfBytesOfRankDirectory = [](std::vector<uint64_t> indices) {
    return SortedVocabIndices(Storage{std::move(indices)},
                              PositionLookup::RankDirectoryIfSmaller)
        .numberOfBytesOfRankDirectory();
  };
  // 448 possible indices cost 64 bytes, as much as 8 indices. So the directory
  // is built for 9 indices, but not for 8.
  std::vector<uint64_t> nineIndices{0, 1, 2, 3, 4, 5, 6, 7, 447};
  EXPECT_EQ(numberOfBytesOfRankDirectory(nineIndices), 64);
  nineIndices.erase(nineIndices.begin());
  EXPECT_EQ(numberOfBytesOfRankDirectory(nineIndices), 0);
  // No indices, and an index for which `endIndex()` would overflow (no
  // exception, the binary search is used).
  EXPECT_EQ(numberOfBytesOfRankDirectory({}), 0);
  EXPECT_EQ(
      numberOfBytesOfRankDirectory({0, std::numeric_limits<uint64_t>::max()}),
      0);

  // Both choices find the same positions.
  std::mt19937_64 randomEngine{99};
  for (const double fractionContained : {0.005, 0.5}) {
    const auto indices =
        randomIndicesWithHoles(randomEngine, 5000, fractionContained);
    const SortedVocabIndices ifSmaller{Storage{indices},
                                       PositionLookup::RankDirectoryIfSmaller};
    EXPECT_EQ(ifSmaller.numberOfBytesOfRankDirectory() > 0,
              fractionContained > 1.0 / 56);
    const SortedVocabIndices binarySearch{Storage{indices},
                                          PositionLookup::BinarySearch};
    for (const uint64_t index : ad_utility::integerRange(uint64_t{5002})) {
      ASSERT_EQ(ifSmaller.positionOf(index), binarySearch.positionOf(index));
    }
  }
}

// _____________________________________________________________________________
TEST(SortedVocabIndices, RankDirectoryChecksTheIndices) {
  using Storage = SortedVocabIndices::Storage;
  // Not strictly ascending.
  AD_EXPECT_THROW_WITH_MESSAGE(
      SortedVocabIndices(Storage{std::vector<uint64_t>{4, 2}},
                         PositionLookup::RankDirectory),
      ::testing::HasSubstr("previous.value() < element"));
  // `endIndex()` would overflow.
  AD_EXPECT_THROW_WITH_MESSAGE(
      SortedVocabIndices(
          Storage{std::vector<uint64_t>{std::numeric_limits<uint64_t>::max()}},
          PositionLookup::RankDirectory),
      ::testing::HasSubstr("max()"));
}
