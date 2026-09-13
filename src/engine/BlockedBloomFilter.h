// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <utility>
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

  // Salt constants for deriving 8 (lane, bit) positions inside the 512-bit
  // block. k = 8 fixed lanes is the split-block scheme (Putze et al.): each
  // probe touches a single cache line, so the hash count is a deliberate
  // design constant rather than a tunable parameter.
  static constexpr uint32_t SALTS[8] = {0x47b6137b, 0x44974d91, 0x8824ad5b,
                                        0xa2b7289d, 0x705495c7, 0x2df1424b,
                                        0x9efc4947, 0x5c6bfb31};

  // Derive the (lane, bit) position of the i-th hash of `key` inside one
  // block. All 16 words of the block are addressable, so no block capacity
  // is wasted.
  [[nodiscard]] static constexpr std::pair<uint32_t, uint32_t> laneAndBit(
      uint32_t key, int i) noexcept {
    uint32_t h = key * SALTS[i];
    return {(h >> 27) & 0xF, (h >> 22) & 0x1F};
  }

  [[nodiscard]] static constexpr uint64_t hashId(Id id) noexcept {
    uint64_t z = id.getBits() + 0x9e3779b97f4a7c15ULL;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
  }

 public:
  explicit BlockedBloomFilter(size_t expectedElements,
                              double falsePositiveRate = 0.01) {
    // Standard Bloom filter sizing: m = -n * ln(p) / ln(2)^2 bits, so the
    // requested rate controls the size (p = 0.01 needs ~9.6 bits/element).
    // The rate is clamped because ln(p) is undefined outside (0, 1).
    static constexpr double kLn2Squared = 0.4804530139182014;  // ln(2)^2
    double p = std::clamp(falsePositiveRate, 1e-9, 1.0 - 1e-9);
    size_t targetBits = static_cast<size_t>(std::ceil(
        -static_cast<double>(expectedElements) * std::log(p) / kLn2Squared));
    blocks_.resize(
        std::max(1UL, (targetBits + BITS_PER_BLOCK - 1) / BITS_PER_BLOCK));
  }

  void insert(Id id) noexcept {
    uint64_t hash = hashId(id);
    size_t blockIdx = (hash >> 32) % blocks_.size();
    uint32_t key = static_cast<uint32_t>(hash);

    Block& blk = blocks_[blockIdx];
    for (int i = 0; i < 8; ++i) {
      auto [wordIdx, bitPos] = laneAndBit(key, i);
      blk.words[wordIdx] |= (1U << bitPos);
    }
  }

  [[nodiscard]] bool contains(Id id) const noexcept {
    uint64_t hash = hashId(id);
    size_t blockIdx = (hash >> 32) % blocks_.size();
    uint32_t key = static_cast<uint32_t>(hash);

    const Block& blk = blocks_[blockIdx];
    for (int i = 0; i < 8; ++i) {
      auto [wordIdx, bitPos] = laneAndBit(key, i);
      if ((blk.words[wordIdx] & (1U << bitPos)) == 0) {
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

  [[nodiscard]] size_t numBlocks() const noexcept { return blocks_.size(); }
  [[nodiscard]] size_t sizeBytes() const noexcept {
    return blocks_.size() * BYTES_PER_BLOCK;
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
