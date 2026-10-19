// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_PACKEDWORDOFFSETS_H
#define QLEVER_SRC_INDEX_VOCABULARY_PACKEDWORDOFFSETS_H

#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "util/Exception.h"

namespace ad_utility::vocabulary {

// A compact in-RAM copy of the ascending word offsets of a `VocabularyOnDisk`
// (the `.offsets` file: `n + 1` offsets of 8 bytes each, offset `i` is the
// start of word `i` in the words file, offset `n` the end of the last word).
//
// Layout: the offsets are cut into groups of `kOffsetsPerBlock` = 57. Each
// group is stored in one cache-line sized `Block` of 64 bytes: the first
// offset of the group as a full 64-bit `base_`, followed by the 56 differences
// between consecutive offsets of the group (the lengths of the group's first
// 56 words) as one byte each. Offset `j` of a group is `base_` plus the sum of
// the first `j` lengths, and the end of the word at position `j < 56` is its
// start plus `lengths_[j]`; the end of the group's last word is the next
// group's `base_`. So one lookup reads one cache line (two for the last word
// of a group), and its decode is a sum of at most 56 bytes.
//
// A group with a length of 255 or more cannot store its lengths in bytes (an
// "escape group"). Its `base_` then has the top bit set, and its low bits give
// the index of a slot of 57 full offsets in `exceptions_`. On the Wikidata
// index of 2025, 0.004 % of the words are 255 bytes or longer, and 0.1 % of
// the groups are escape groups.
//
// Memory: 64 bytes per 57 words (8.98 bits per word) plus 456 bytes per escape
// group, instead of 64 bits per word for the offsets file.
class PackedWordOffsets {
 public:
  static constexpr size_t kLengthsPerBlock = 56;
  static constexpr size_t kOffsetsPerBlock = kLengthsPerBlock + 1;
  // A length that does not fit into one byte. Lengths of 255 are escaped as
  // well, so that the byte 255 is free for future use as a marker.
  static constexpr uint64_t kMaxStoredLength = 254;
  static constexpr uint64_t kEscapeFlag = uint64_t{1} << 63;

 private:
  struct alignas(64) Block {
    uint64_t base_ = 0;
    std::array<uint8_t, kLengthsPerBlock> lengths_{};
  };
  static_assert(sizeof(Block) == 64);

  std::vector<Block> blocks_;
  std::vector<uint64_t> exceptions_;
  // The number of stored offsets (number of words + 1), 0 if empty.
  size_t numOffsets_ = 0;

 public:
  // Builds a `PackedWordOffsets` from the offsets (defined below).
  class Builder;

  PackedWordOffsets() = default;

  // The number of words (number of stored offsets minus one).
  size_t numWords() const { return numOffsets_ == 0 ? 0 : numOffsets_ - 1; }
  size_t numOffsets() const { return numOffsets_; }

  // The offset with index `i <= numWords()`.
  uint64_t offset(size_t i) const {
    AD_EXPENSIVE_CHECK(i < numOffsets_);
    const Block& block = blocks_[i / kOffsetsPerBlock];
    const size_t j = i % kOffsetsPerBlock;
    if (block.base_ & kEscapeFlag) [[unlikely]] {
      return exceptionOffset(block, j);
    }
    return block.base_ + prefixSum(block.lengths_.data(), j);
  }

  // The start and the end offset of word `i < numWords()`.
  std::pair<uint64_t, uint64_t> offsetPair(size_t i) const {
    AD_EXPENSIVE_CHECK(i + 1 < numOffsets_);
    const Block& block = blocks_[i / kOffsetsPerBlock];
    const size_t j = i % kOffsetsPerBlock;
    if (block.base_ & kEscapeFlag) [[unlikely]] {
      return {exceptionOffset(block, j), offset(i + 1)};
    }
    const uint64_t start = block.base_ + prefixSum(block.lengths_.data(), j);
    if (j < kLengthsPerBlock) [[likely]] {
      return {start, start + block.lengths_[j]};
    }
    return {start, offset(i + 1)};
  }

  // Prefetch the cache line that `offsetPair(i)` reads first.
  void prefetch(size_t i) const {
    __builtin_prefetch(&blocks_[i / kOffsetsPerBlock]);
  }

  // The bytes of RAM used by the structure (blocks plus exceptions).
  size_t memoryBytes() const {
    return blocks_.size() * sizeof(Block) +
           exceptions_.size() * sizeof(uint64_t);
  }
  size_t numBlocks() const { return blocks_.size(); }
  size_t numEscapeBlocks() const {
    return exceptions_.size() / kOffsetsPerBlock;
  }

  // The sum of the first `j <= kLengthsPerBlock` bytes at `lengths`, eight
  // bytes at a time (SWAR): each 8-byte word is split into four 16-bit lanes
  // of byte pairs, the lanes are accumulated (at most 7 * 2 * 255 = 3570 per
  // lane, no overflow), and one multiplication sums the four lanes.
  static uint64_t prefixSum(const uint8_t* lengths, size_t j) {
    if constexpr (std::endian::native != std::endian::little) {
      uint64_t sum = 0;
      for (size_t k = 0; k < j; ++k) {
        sum += lengths[k];
      }
      return sum;
    } else {
      constexpr uint64_t evenBytes = 0x00FF00FF00FF00FFULL;
      uint64_t lanes = 0;
      const size_t fullWords = j / 8;
      for (size_t w = 0; w < fullWords; ++w) {
        uint64_t x;
        std::memcpy(&x, lengths + 8 * w, sizeof(x));
        lanes += (x & evenBytes) + ((x >> 8) & evenBytes);
      }
      if (const size_t rest = j % 8; rest != 0) {
        uint64_t x;
        std::memcpy(&x, lengths + 8 * fullWords, sizeof(x));
        x &= (uint64_t{1} << (8 * rest)) - 1;
        lanes += (x & evenBytes) + ((x >> 8) & evenBytes);
      }
      return (lanes * 0x0001000100010001ULL) >> 48;
    }
  }

 private:
  uint64_t exceptionOffset(const Block& block, size_t j) const {
    return exceptions_[(block.base_ & ~kEscapeFlag) * kOffsetsPerBlock + j];
  }
};

// Builds a `PackedWordOffsets` from the offsets, which must be pushed in
// ascending order.
class PackedWordOffsets::Builder {
 public:
  // `expectedNumOffsets` is only used to reserve memory.
  explicit Builder(size_t expectedNumOffsets = 0);
  void push(uint64_t offset);
  // Finish the build. The builder must not be used afterwards.
  PackedWordOffsets finish() &&;

 private:
  void flushGroup();
  PackedWordOffsets result_;
  std::array<uint64_t, kOffsetsPerBlock> group_{};
  size_t groupSize_ = 0;
};

}  // namespace ad_utility::vocabulary

#endif  // QLEVER_SRC_INDEX_VOCABULARY_PACKEDWORDOFFSETS_H
