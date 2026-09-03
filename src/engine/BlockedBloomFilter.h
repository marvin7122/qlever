// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"

namespace ql::engine::filter {

// _____________________________________________________________________________
// Cache-Line Blocked Bloom Filter (Split-Block Bloom Filter):
// Sized in discrete 64-byte (512-bit) blocks matching hardware cache lines.
// Probing tests 8 bits in parallel inside a single L1 cache line, guaranteeing
// zero Last-Level Cache (LLC) thrashing.
class BlockedBloomFilter {
 public:
  static constexpr size_t BITS_PER_BLOCK = 512;
  static constexpr size_t BYTES_PER_BLOCK = 64;

  struct alignas(64) Block {
    uint32_t words[16] = {0};
  };

 private:
  std::vector<Block> blocks_;
  size_t numBlocks_ = 0;

  // Salt constants for generating 8 bit positions inside the 512-bit block
  static constexpr uint32_t SALTS[8] = {0x47b6137b, 0x44974d91, 0x8824ad5b,
                                        0xa2b7289d, 0x705495c7, 0x2df1424b,
                                        0x9efc4947, 0x5c6bfb31};

  [[nodiscard]] static constexpr uint64_t hashId(Id id) noexcept {
    uint64_t z = id.getBits() + 0x9e3779b97f4a7c15ULL;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
  }

 public:
  explicit BlockedBloomFilter(size_t expectedElements,
                              double falsePositiveRate = 0.01) {
    // Sizing: ~10 bits per element for ~1% FPR
    size_t targetBits = static_cast<size_t>(expectedElements * 10);
    numBlocks_ =
        std::max(1UL, (targetBits + BITS_PER_BLOCK - 1) / BITS_PER_BLOCK);
    blocks_.resize(numBlocks_);
  }

  void insert(Id id) noexcept {
    uint64_t hash = hashId(id);
    size_t blockIdx = (hash >> 32) % numBlocks_;
    uint32_t key = static_cast<uint32_t>(hash);

    Block& blk = blocks_[blockIdx];
    for (int i = 0; i < 8; ++i) {
      uint32_t bitPos = (key * SALTS[i]) >> 27;  // 0..31
      blk.words[i * 2] |= (1U << bitPos);
    }
  }

  [[nodiscard]] bool contains(Id id) const noexcept {
    uint64_t hash = hashId(id);
    size_t blockIdx = (hash >> 32) % numBlocks_;
    uint32_t key = static_cast<uint32_t>(hash);

    const Block& blk = blocks_[blockIdx];
    for (int i = 0; i < 8; ++i) {
      uint32_t bitPos = (key * SALTS[i]) >> 27;
      if ((blk.words[i * 2] & (1U << bitPos)) == 0) {
        return false;
      }
    }
    return true;
  }

  // Populate a BlockedBloomFilter from a column/span of Ids.
  static BlockedBloomFilter createFromColumn(ql::span<const Id> column,
                                             double falsePositiveRate = 0.01) {
    BlockedBloomFilter filter{column.size(), falsePositiveRate};
    for (Id id : column) {
      filter.insert(id);
    }
    return filter;
  }

  // Check whether a candidate key passes the filter.
  [[nodiscard]] bool passesFilter(Id id) const noexcept { return contains(id); }

  // Probe incoming candidate keys to prune non-matching row indices before
  // buffer materialization.
  [[nodiscard]] std::vector<size_t> pruneNonMatchingIndices(
      ql::span<const Id> candidateKeys) const {
    std::vector<size_t> matchingIndices;
    matchingIndices.reserve(candidateKeys.size());
    for (size_t i = 0; i < candidateKeys.size(); ++i) {
      if (contains(candidateKeys[i])) {
        matchingIndices.push_back(i);
      }
    }
    return matchingIndices;
  }

  [[nodiscard]] size_t numBlocks() const noexcept { return numBlocks_; }
  [[nodiscard]] size_t sizeBytes() const noexcept {
    return numBlocks_ * BYTES_PER_BLOCK;
  }
};

// _____________________________________________________________________________
// Semi-join pushdown helper: populates a BlockedBloomFilter from the build-side
// (smaller table's join column) during join preparation and probes probe-side
// candidate keys to prune non-matching rows before buffer materialization.
class SemiJoinPushdownHelper {
 private:
  BlockedBloomFilter filter_;

 public:
  explicit SemiJoinPushdownHelper(ql::span<const Id> buildSideKeys,
                                  double falsePositiveRate = 0.01)
      : filter_{BlockedBloomFilter::createFromColumn(buildSideKeys,
                                                     falsePositiveRate)} {}

  // Test whether a candidate key should be retained.
  [[nodiscard]] bool probe(Id candidateKey) const noexcept {
    return filter_.contains(candidateKey);
  }

  // Probe incoming candidate keys and return the indices of matching elements.
  [[nodiscard]] std::vector<size_t> pruneNonMatchingIndices(
      ql::span<const Id> candidateKeys) const {
    return filter_.pruneNonMatchingIndices(candidateKeys);
  }

  [[nodiscard]] const BlockedBloomFilter& filter() const noexcept {
    return filter_;
  }
};

}  // namespace ql::engine::filter
