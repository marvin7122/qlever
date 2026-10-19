// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <random>
#include <vector>

#include "index/vocabulary/PackedWordOffsets.h"

namespace {
using ad_utility::vocabulary::PackedWordOffsets;

// Build from `offsets` and check every `offset` and `offsetPair` against them.
PackedWordOffsets buildAndCheck(const std::vector<uint64_t>& offsets) {
  PackedWordOffsets::Builder builder{offsets.size()};
  for (uint64_t o : offsets) {
    builder.push(o);
  }
  auto packed = std::move(builder).finish();
  EXPECT_EQ(packed.numOffsets(), offsets.size());
  EXPECT_EQ(packed.numWords(), offsets.empty() ? 0 : offsets.size() - 1);
  for (size_t i = 0; i < offsets.size(); ++i) {
    EXPECT_EQ(packed.offset(i), offsets[i]) << "offset " << i;
  }
  for (size_t i = 0; i + 1 < offsets.size(); ++i) {
    auto [start, end] = packed.offsetPair(i);
    EXPECT_EQ(start, offsets[i]) << "word " << i;
    EXPECT_EQ(end, offsets[i + 1]) << "word " << i;
  }
  return packed;
}

// Offsets from the lengths, starting at `start`.
std::vector<uint64_t> offsetsFromLengths(const std::vector<uint64_t>& lengths,
                                         uint64_t start = 0) {
  std::vector<uint64_t> offsets{start};
  for (uint64_t l : lengths) {
    offsets.push_back(offsets.back() + l);
  }
  return offsets;
}

// _____________________________________________________________________________
TEST(PackedWordOffsets, prefixSumMatchesLoop) {
  std::mt19937_64 random{7};
  std::array<uint8_t, PackedWordOffsets::kLengthsPerBlock + 8> bytes{};
  for (size_t round = 0; round < 200; ++round) {
    for (auto& b : bytes) {
      // Many maximal bytes, to test the lanes for overflow.
      b = round % 3 == 0 ? 255 : static_cast<uint8_t>(random());
    }
    uint64_t expected = 0;
    for (size_t j = 0; j <= PackedWordOffsets::kLengthsPerBlock; ++j) {
      EXPECT_EQ(PackedWordOffsets::prefixSum(bytes.data(), j), expected) << j;
      if (j < PackedWordOffsets::kLengthsPerBlock) {
        expected += bytes[j];
      }
    }
  }
}

// _____________________________________________________________________________
TEST(PackedWordOffsets, emptyAndSmall) {
  auto empty = buildAndCheck({});
  EXPECT_EQ(empty.memoryBytes(), 0u);
  // A vocabulary without words still has its end offset.
  auto noWords = buildAndCheck({0});
  EXPECT_EQ(noWords.numBlocks(), 1u);
  buildAndCheck({0, 5});
  buildAndCheck({3, 3, 3, 10});
  // Words of length 254 (stored) and 255 (escaped).
  buildAndCheck({0, 254, 508});
  auto escaped = buildAndCheck({0, 255, 300});
  EXPECT_EQ(escaped.numEscapeBlocks(), 1u);
}

// _____________________________________________________________________________
// Exactly one, two and a bit more than two full groups, so that the last
// word of a group (whose end is the next group's base) is covered, as well
// as a large start offset (above 2^32).
TEST(PackedWordOffsets, groupBoundaries) {
  constexpr size_t k = PackedWordOffsets::kOffsetsPerBlock;
  for (size_t numOffsets : {k - 1, k, k + 1, 2 * k, 2 * k + 1, 3 * k + 5}) {
    std::vector<uint64_t> lengths;
    for (size_t i = 0; i + 1 < numOffsets; ++i) {
      lengths.push_back(i % 13);
    }
    auto packed = buildAndCheck(offsetsFromLengths(lengths, uint64_t{1} << 34));
    EXPECT_EQ(packed.numBlocks(), (numOffsets + k - 1) / k);
    EXPECT_EQ(packed.numEscapeBlocks(), 0u);
    EXPECT_EQ(packed.memoryBytes(), packed.numBlocks() * 64);
  }
}

// _____________________________________________________________________________
// Random lengths with a realistic distribution (mostly short) and some long
// words, so that some groups are escape groups; a long word as the last word
// of a group is not stored as a length and does not escape the group.
TEST(PackedWordOffsets, randomWithEscapes) {
  constexpr size_t k = PackedWordOffsets::kOffsetsPerBlock;
  std::mt19937_64 random{42};
  std::vector<uint64_t> lengths;
  for (size_t i = 0; i < 20000; ++i) {
    uint64_t r = random() % 1000;
    lengths.push_back(r < 990 ? random() % 40 : 200 + random() % 5000);
  }
  auto offsets = offsetsFromLengths(lengths);
  auto packed = buildAndCheck(offsets);
  // Count the expected escape groups.
  size_t expectedEscapes = 0;
  for (size_t g = 0; g * k < offsets.size(); ++g) {
    bool escape = false;
    for (size_t j = g * k; j + 1 < std::min((g + 1) * k, offsets.size()); ++j) {
      escape |= offsets[j + 1] - offsets[j] > 254;
    }
    expectedEscapes += escape;
  }
  EXPECT_GT(expectedEscapes, 0u);
  EXPECT_EQ(packed.numEscapeBlocks(), expectedEscapes);
  EXPECT_EQ(packed.memoryBytes(),
            packed.numBlocks() * 64 + expectedEscapes * k * 8);

  // Only the last word of the first group is long: no escape.
  std::vector<uint64_t> lastLong(k + 3, 1);
  lastLong[k - 1] = 100000;
  auto noEscape = buildAndCheck(offsetsFromLengths(lastLong));
  EXPECT_EQ(noEscape.numEscapeBlocks(), 0u);
}

// _____________________________________________________________________________
TEST(PackedWordOffsets, builderRejectsDescendingOffsets) {
  PackedWordOffsets::Builder builder;
  builder.push(10);
  EXPECT_ANY_THROW(builder.push(9));
  PackedWordOffsets::Builder builder2;
  for (size_t i = 0; i < PackedWordOffsets::kOffsetsPerBlock; ++i) {
    builder2.push(100);
  }
  // The first offset of the next group is checked against the previous group.
  EXPECT_ANY_THROW(builder2.push(99));
  EXPECT_ANY_THROW(builder2.push(PackedWordOffsets::kEscapeFlag));
}

}  // namespace
