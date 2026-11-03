// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BITVECTORWITHRANK_H
#define QLEVER_SRC_UTIL_BITVECTORWITHRANK_H

#include <absl/numeric/bits.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"

namespace ad_utility {

// An immutable set of integers from `[0, universeSize())` that answers "is
// `value` contained, and if so, how many contained integers are smaller than
// `value`" (the rank of `value`) in constant time: one cache line and at most
// four popcounts per query.
//
// Layout: one bit per integer of the universe, split into blocks of
// `numBitsPerBlock` (448) bits. Each block occupies exactly one 64-byte cache
// line: the rank of the middle bit of the block (8 bytes), followed by the 7
// words of bits of the block. Because the stored rank belongs to the middle of
// the block (and not to its start, as in SDSL or Rank9), a query counts the
// bits between its position and the middle, which are at most half a block (4
// words). The memory is `64 * ceil(universeSize / 448)` bytes, that is 8/7 bits
// per integer of the universe, independent of how many integers are contained.
//
// The set is not copyable (it is typically large), but movable.
class BitVectorWithRank : public ad_utility::NoCopy {
 public:
  static constexpr size_t numWordsPerBlock = 7;
  static constexpr uint64_t numBitsPerBlock = numWordsPerBlock * 64;
  // The offset of the bit inside each block whose rank is stored.
  static constexpr uint64_t middleBit = numBitsPerBlock / 2;

 private:
  using Bits = std::array<uint64_t, numWordsPerBlock>;
  struct alignas(64) Block {
    // The number of contained values that are smaller than the value at
    // `middleBit` of this block (in all previous blocks plus the first half of
    // this block).
    uint64_t rankOfMiddleBit_ = 0;
    Bits bits_{};
  };
  static_assert(sizeof(Block) == 64);

  std::vector<Block> blocks_;
  uint64_t universeSize_ = 0;

 public:
  // The empty set over the empty universe.
  BitVectorWithRank() = default;

  // The set of the given `sortedValues`, which must be strictly ascending and
  // smaller than `universeSize`.
  BitVectorWithRank(ql::span<const uint64_t> sortedValues,
                    uint64_t universeSize)
      // `ceil(universeSize / numBitsPerBlock)`, written without an addition
      // that could overflow for a universe close to `UINT64_MAX`.
      : blocks_(universeSize / numBitsPerBlock +
                (universeSize % numBitsPerBlock != 0)),
        universeSize_{universeSize} {
    std::optional<uint64_t> previous;
    for (const uint64_t value : sortedValues) {
      AD_CONTRACT_CHECK(value < universeSize_);
      AD_CONTRACT_CHECK(!previous.has_value() || previous.value() < value);
      previous = value;
      const uint64_t offset = value % numBitsPerBlock;
      blocks_[value / numBitsPerBlock].bits_[offset / 64] |= uint64_t{1}
                                                             << (offset % 64);
    }
    uint64_t rank = 0;
    for (Block& block : blocks_) {
      block.rankOfMiddleBit_ = rank + popcountRange(block.bits_, 0, middleBit);
      rank += popcountRange(block.bits_, 0, numBitsPerBlock);
    }
    AD_CORRECTNESS_CHECK(rank == sortedValues.size());
  }

  // If `value` is contained, return its rank, that is the number of contained
  // values that are smaller than `value` (which is the position of `value` in
  // the `sortedValues` that the set was built from). Otherwise (also if
  // `value >= universeSize()`), return `std::nullopt`.
  std::optional<uint64_t> rankIfContained(uint64_t value) const {
    if (value >= universeSize_) {
      return std::nullopt;
    }
    const Block& block = blocks_[value / numBitsPerBlock];
    const uint64_t offset = value % numBitsPerBlock;
    if ((block.bits_[offset / 64] & (uint64_t{1} << (offset % 64))) == 0) {
      return std::nullopt;
    }
    // Count down from the middle bit in the first half of the block, up from
    // it in the second half.
    if (offset < middleBit) {
      return block.rankOfMiddleBit_ -
             popcountRange(block.bits_, offset, middleBit);
    }
    return block.rankOfMiddleBit_ +
           popcountRange(block.bits_, middleBit, offset);
  }

  // One more than the largest value that the set can contain.
  uint64_t universeSize() const { return universeSize_; }

  // The number of bytes of the bits and the stored ranks.
  size_t numBytes() const { return blocks_.size() * sizeof(Block); }

 private:
  // The number of set bits in `bits` at the offsets `[from, to)`. Touches at
  // most 4 words if `to - from <= middleBit`.
  static uint64_t popcountRange(const Bits& bits, uint64_t from, uint64_t to) {
    // Runs up to twice per query, so the check is only active in debug builds.
    AD_EXPENSIVE_CHECK(from <= to && to <= numBitsPerBlock);
    if (from == to) {
      return 0;
    }
    // The bits `[low, high)` of a single word, for `low < high <= 64`.
    const auto wordMask = [](uint64_t low, uint64_t high) {
      const uint64_t belowHigh =
          high == 64 ? ~uint64_t{0} : (uint64_t{1} << high) - 1;
      return belowHigh & (~uint64_t{0} << low);
    };
    const size_t firstWord = from / 64;
    const size_t lastWord = (to - 1) / 64;
    const uint64_t highInLastWord = to - lastWord * 64;
    const uint64_t highInFirstWord =
        firstWord == lastWord ? highInLastWord : 64;
    auto count = static_cast<uint64_t>(absl::popcount(
        bits[firstWord] & wordMask(from - firstWord * 64, highInFirstWord)));
    for (size_t word = firstWord + 1; word < lastWord; ++word) {
      count += static_cast<uint64_t>(absl::popcount(bits[word]));
    }
    if (lastWord > firstWord) {
      count += static_cast<uint64_t>(
          absl::popcount(bits[lastWord] & wordMask(0, highInLastWord)));
    }
    return count;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BITVECTORWITHRANK_H
