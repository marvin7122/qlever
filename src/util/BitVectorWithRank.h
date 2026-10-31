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
#include <sys/mman.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <new>
#include <optional>
#include <utility>

#include "backports/span.h"
#include "util/Exception.h"

namespace ad_utility {

// An immutable set of integers from `[0, universeSize())` that answers "is `i`
// contained, and if so, how many contained integers are smaller than `i`" in
// constant time (one cache line per query).
//
// Layout: one bit per integer, split into blocks of `bitsPerBlock` (448) bits.
// Each block occupies exactly one 64-byte cache line: the number of set bits
// before the middle of the block (8 bytes), followed by the block's 7 bit
// words. Anchoring the counter in the middle (rather than at the start, as in
// SDSL or Rank9) halves the worst case: a query reads one cache line and adds
// at most 4 popcounts, counting down from the anchor in the first half of the
// block and up from it in the second half. The memory is
// `64 * ceil(universeSize / 448)` bytes, that is 8/7 bits per integer of the
// universe, independent of how many integers are contained.
//
// With `useHugePages`, the blocks are allocated 2 MiB-aligned and marked with
// `madvise(MADV_HUGEPAGE)`, so that (if transparent huge pages are enabled at
// least in "madvise" mode) a large set occupies few TLB entries: a random query
// then costs one cache miss instead of one cache miss plus one TLB miss.
class BitVectorWithRank {
 public:
  static constexpr size_t wordsPerBlock = 7;
  static constexpr uint64_t bitsPerBlock = wordsPerBlock * 64;
  static constexpr size_t hugePageSize = size_t{1} << 21;
  // The bit offset of the anchor inside each block: queries before it count
  // down from the anchor, queries at or after it count up.
  static constexpr uint64_t middleBit = bitsPerBlock / 2;

 private:
  struct alignas(64) Block {
    // The number of contained values before `middleBit` of this block (in all
    // previous blocks plus the first half of this block).
    uint64_t rankAtMiddle_ = 0;
    std::array<uint64_t, wordsPerBlock> bits_{};
  };
  static_assert(sizeof(Block) == 64);

  // The number of set bits in `bits[from, to)`. At most 4 words are touched
  // when `to - from <= middleBit`.
  static uint64_t popcountRange(const std::array<uint64_t, wordsPerBlock>& bits,
                                uint64_t from, uint64_t to) {
    // Per-query hot path (up to twice per rank lookup): an expensive check —
    // active in debug/test builds, compiled out with NDEBUG.
    AD_EXPENSIVE_CHECK(from <= to && to <= bitsPerBlock);
    if (from == to) {
      return 0;
    }
    const size_t firstWord = from / 64;
    const size_t lastWord = (to - 1) / 64;
    // Bits `[lo, hi)` of a single word (`hi == 64` means the whole top).
    const auto wordMask = [](uint64_t lo, uint64_t hi) {
      return (hi == 64 ? ~uint64_t{0} : ((uint64_t{1} << hi) - 1)) &
             (~uint64_t{0} << lo);
    };
    const uint64_t lastHi = to - lastWord * 64;
    uint64_t count = static_cast<uint64_t>(absl::popcount(
        bits[firstWord] &
        wordMask(from - firstWord * 64, firstWord == lastWord ? lastHi : 64)));
    for (size_t i = firstWord + 1; i < lastWord; ++i) {
      count += static_cast<uint64_t>(absl::popcount(bits[i]));
    }
    if (lastWord > firstWord) {
      count += static_cast<uint64_t>(
          absl::popcount(bits[lastWord] & wordMask(0, lastHi)));
    }
    return count;
  }

  struct FreeDeleter {
    void operator()(Block* blocks) const { std::free(blocks); }
  };
  std::unique_ptr<Block[], FreeDeleter> blocks_;
  size_t numBlocks_ = 0;
  size_t numAllocatedBytes_ = 0;
  uint64_t universeSize_ = 0;

  // Allocate `numBlocks_` zero blocks, see `useHugePages` above.
  void allocateBlocks(bool useHugePages) {
    if (numBlocks_ == 0) {
      return;
    }
    const size_t alignment = useHugePages ? hugePageSize : sizeof(Block);
    // `std::aligned_alloc` requires the size to be a multiple of the alignment.
    numAllocatedBytes_ =
        (numBlocks_ * sizeof(Block) + alignment - 1) / alignment * alignment;
    void* memory = std::aligned_alloc(alignment, numAllocatedBytes_);
    if (memory == nullptr) {
      throw std::bad_alloc{};
    }
#ifdef MADV_HUGEPAGE
    if (useHugePages) {
      // Only a hint: if it fails, the blocks simply stay on regular pages.
      (void)madvise(memory, numAllocatedBytes_, MADV_HUGEPAGE);
    }
#endif
    blocks_.reset(static_cast<Block*>(memory));
    for (size_t i = 0; i < numBlocks_; ++i) {
      new (&blocks_[i]) Block{};
    }
  }

 public:
  // The empty set over the empty universe.
  BitVectorWithRank() = default;

  // Move-only. A moved-from object is the empty set over the empty universe.
  BitVectorWithRank(BitVectorWithRank&& other) noexcept
      : blocks_{std::move(other.blocks_)},
        numBlocks_{std::exchange(other.numBlocks_, 0)},
        numAllocatedBytes_{std::exchange(other.numAllocatedBytes_, 0)},
        universeSize_{std::exchange(other.universeSize_, 0)} {}
  BitVectorWithRank& operator=(BitVectorWithRank&& other) noexcept {
    blocks_ = std::move(other.blocks_);
    numBlocks_ = std::exchange(other.numBlocks_, 0);
    numAllocatedBytes_ = std::exchange(other.numAllocatedBytes_, 0);
    universeSize_ = std::exchange(other.universeSize_, 0);
    return *this;
  }

  // Build the set of the given `sortedValues`, which must be strictly
  // ascending and smaller than `universeSize`.
  BitVectorWithRank(ql::span<const uint64_t> sortedValues,
                    uint64_t universeSize, bool useHugePages = false)
      // Quotient plus remainder check (no addition, which would overflow for
      // universes near `UINT64_MAX` and allocate too few blocks).
      : numBlocks_{universeSize / bitsPerBlock +
                   (universeSize % bitsPerBlock != 0)},
        universeSize_{universeSize} {
    allocateBlocks(useHugePages);
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
    for (size_t i = 0; i < numBlocks_; ++i) {
      Block& block = blocks_[i];
      block.rankAtMiddle_ = rank + popcountRange(block.bits_, 0, middleBit);
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
    const uint64_t bit = uint64_t{1} << (offset % 64);
    if ((block.bits_[offset / 64] & bit) == 0) {
      return std::nullopt;
    }
    // Count down from the anchor in the first half, up in the second half.
    if (offset < middleBit) {
      return block.rankAtMiddle_ -
             popcountRange(block.bits_, offset, middleBit);
    }
    return block.rankAtMiddle_ + popcountRange(block.bits_, middleBit, offset);
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
  size_t numBytes() const { return numBlocks_ * sizeof(Block); }

  // The allocated memory (`numBytes()` rounded up to the alignment), for
  // example to check how much of it is backed by huge pages.
  const void* allocationBegin() const { return blocks_.get(); }
  size_t numAllocatedBytes() const { return numAllocatedBytes_; }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BITVECTORWITHRANK_H
