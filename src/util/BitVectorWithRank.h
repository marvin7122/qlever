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

namespace ad_utility {

// An immutable set of integers from `[0, universeSize())` that answers "is `i`
// contained, and if so, how many contained integers are smaller than `i`" in
// constant time (one cache line per query).
//
// Layout: one bit per integer, split into blocks of `bitsPerBlock` (448) bits.
// Each block occupies exactly one 64-byte cache line: the number of set bits in
// all previous blocks (8 bytes), followed by the block's 7 bit words. A query
// therefore reads one cache line and adds at most 7 popcounts. The memory is
// `64 * ceil(universeSize / 448)` bytes, that is 8/7 bits per integer of the
// universe, independent of how many integers are contained.
class BitVectorWithRank {
 public:
  static constexpr size_t wordsPerBlock = 7;
  static constexpr uint64_t bitsPerBlock = wordsPerBlock * 64;

 private:
  struct alignas(64) Block {
    uint64_t rankBefore_ = 0;
    std::array<uint64_t, wordsPerBlock> bits_{};
  };
  static_assert(sizeof(Block) == 64);

  std::vector<Block> blocks_;
  uint64_t universeSize_ = 0;

 public:
  // The empty set over the empty universe.
  BitVectorWithRank() = default;

  // Build the set of the given `sortedValues`, which must be strictly
  // ascending and smaller than `universeSize`.
  BitVectorWithRank(ql::span<const uint64_t> sortedValues,
                    uint64_t universeSize)
      : blocks_((universeSize + bitsPerBlock - 1) / bitsPerBlock),
        universeSize_{universeSize} {
    std::optional<uint64_t> previous;
    for (uint64_t value : sortedValues) {
      AD_CONTRACT_CHECK(value < universeSize_);
      AD_CONTRACT_CHECK(!previous.has_value() || previous.value() < value);
      previous = value;
      const uint64_t offset = value % bitsPerBlock;
      blocks_[value / bitsPerBlock].bits_[offset / 64] |= uint64_t{1}
                                                          << (offset % 64);
    }
    uint64_t rank = 0;
    for (Block& block : blocks_) {
      block.rankBefore_ = rank;
      for (uint64_t word : block.bits_) {
        rank += static_cast<uint64_t>(absl::popcount(word));
      }
    }
    AD_CORRECTNESS_CHECK(rank == sortedValues.size());
  }

  // If `value` is contained, return the number of contained values that are
  // smaller than `value` (its position in the sorted sequence of values the
  // set was built from). Otherwise (also if `value >= universeSize()`), return
  // `std::nullopt`.
  std::optional<uint64_t> rankIfContained(uint64_t value) const {
    if (value >= universeSize_) {
      return std::nullopt;
    }
    const Block& block = blocks_[value / bitsPerBlock];
    const uint64_t offset = value % bitsPerBlock;
    const size_t wordIdx = offset / 64;
    const uint64_t bit = uint64_t{1} << (offset % 64);
    const uint64_t word = block.bits_[wordIdx];
    if ((word & bit) == 0) {
      return std::nullopt;
    }
    uint64_t rank = block.rankBefore_ +
                    static_cast<uint64_t>(absl::popcount(word & (bit - 1)));
    for (size_t i = 0; i < wordIdx; ++i) {
      rank += static_cast<uint64_t>(absl::popcount(block.bits_[i]));
    }
    return rank;
  }

  // Hint the CPU to load the cache line that `rankIfContained(value)` reads,
  // so that a later call does not stall on it. No effect if `value >=
  // universeSize()`.
  void prefetch(uint64_t value) const {
    if (value < universeSize_) {
      __builtin_prefetch(&blocks_[value / bitsPerBlock]);
    }
  }

  // One more than the largest value that the set can contain.
  uint64_t universeSize() const { return universeSize_; }

  // The number of bytes of the bits and rank counters.
  size_t numBytes() const { return blocks_.size() * sizeof(Block); }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BITVECTORWITHRANK_H
