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
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"

namespace ql::engine::filter {

// Approximate set of `Id`s: `contains` returns `true` for every inserted `Id`
// (no false negatives) and for a small fraction of other `Id`s (false
// positives). All memory is taken from the given `AllocatorWithLimit`, so it
// counts towards the memory limit of the query.
//
// Split-block layout (Putze et al., "Cache-, Hash- and Space-Efficient Bloom
// Filters"): the bit array consists of 64-byte blocks. An `Id` selects one
// block and sets or tests 8 bits inside it, so an `insert` or `contains`
// touches a single 64-byte block.
class BlockedBloomFilter {
 public:
  static constexpr size_t BITS_PER_BLOCK = 512;
  static constexpr size_t BYTES_PER_BLOCK = 64;

  struct alignas(BYTES_PER_BLOCK) Block {
    uint32_t words_[16] = {0};
  };
  static_assert(sizeof(Block) == BYTES_PER_BLOCK);

 private:
  std::vector<Block, ad_utility::AllocatorWithLimit<Block>> blocks_;

  // Odd multipliers from the split-block Bloom filter of Apache Parquet and
  // Impala. Each one derives an independent (word, bit) position from the
  // 32-bit in-block key. The number of positions per `Id` (`BITS_PER_KEY`) is
  // fixed by the split-block scheme and not a tuning parameter.
  static constexpr size_t BITS_PER_KEY = 8;
  static constexpr uint32_t SALTS[BITS_PER_KEY] = {
      0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
      0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

  // The (word, bit) position of the i-th bit for `key`. The top 9
  // bits of the product are the best mixed ones and address all 16 * 32 bits
  // of a block.
  static constexpr std::pair<uint32_t, uint32_t> wordAndBit(uint32_t key,
                                                            size_t i) {
    uint32_t product = key * SALTS[i];
    return {(product >> 27) & 0xF, (product >> 22) & 0x1F};
  }

  // splitmix64 finalizer: the bits of consecutive `Id`s differ only in the
  // low bits, but the block index and the in-block key need all 64 bits
  // mixed.
  static constexpr uint64_t hashId(Id id) {
    uint64_t z = id.getBits() + 0x9e3779b97f4a7c15ULL;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
  }

  // The block that `id` maps to and the 32-bit key for the in-block
  // positions.
  std::pair<size_t, uint32_t> blockAndKey(Id id) const {
    uint64_t hash = hashId(id);
    return {static_cast<size_t>((hash >> 32) % blocks_.size()),
            static_cast<uint32_t>(hash)};
  }

 public:
  // Create an empty filter for `expectedElements` elements. The number of bits
  // is m = -n * ln(p) / ln(2)^2 for the target false-positive rate `p`, which
  // must be in (0, 1). With the fixed 8 bits per element, the actual rate is
  // close to `p` for p around 0.01 (about 9.6 bits per element) and deviates
  // from it for much smaller or larger `p`.
  BlockedBloomFilter(size_t expectedElements,
                     const ad_utility::AllocatorWithLimit<Id>& allocator,
                     double falsePositiveRate = 0.01)
      : blocks_{ad_utility::AllocatorWithLimit<Block>{allocator}} {
    // Written as a positive range check, so that NaN is rejected as well.
    AD_CONTRACT_CHECK(falsePositiveRate > 0.0 && falsePositiveRate < 1.0,
                      "The false-positive rate of a `BlockedBloomFilter` must "
                      "be in (0, 1)");
    static constexpr double ln2Squared = 0.4804530139182014;
    double numBits = std::ceil(static_cast<double>(expectedElements) *
                               -std::log(falsePositiveRate) / ln2Squared);
    double numBlocks = std::ceil(numBits / BITS_PER_BLOCK);
    // Keep the conversion to `size_t` well-defined; the allocator enforces the
    // actual memory limit.
    AD_CONTRACT_CHECK(numBlocks < static_cast<double>(size_t{1} << 52),
                      "Too many elements for a `BlockedBloomFilter`");
    blocks_.resize(std::max(size_t{1}, static_cast<size_t>(numBlocks)));
  }

  // Create a filter that contains all `Id`s of `column`.
  static BlockedBloomFilter createFromColumn(
      ql::span<const Id> column,
      const ad_utility::AllocatorWithLimit<Id>& allocator,
      double falsePositiveRate = 0.01) {
    BlockedBloomFilter filter{column.size(), allocator, falsePositiveRate};
    for (Id id : column) {
      filter.insert(id);
    }
    return filter;
  }

  // Add `id`. Must not run concurrently with other `insert` or `contains`
  // calls; concurrent `contains` calls alone are safe.
  void insert(Id id) {
    auto [blockIdx, key] = blockAndKey(id);
    Block& block = blocks_[blockIdx];
    for (size_t i = 0; i < BITS_PER_KEY; ++i) {
      auto [word, bit] = wordAndBit(key, i);
      block.words_[word] |= (1U << bit);
    }
  }

  // `false` if `id` was definitely not inserted, `true` if it was inserted or
  // is a false positive.
  bool contains(Id id) const {
    auto [blockIdx, key] = blockAndKey(id);
    const Block& block = blocks_[blockIdx];
    for (size_t i = 0; i < BITS_PER_KEY; ++i) {
      auto [word, bit] = wordAndBit(key, i);
      if ((block.words_[word] & (1U << bit)) == 0) {
        return false;
      }
    }
    return true;
  }

  size_t numBlocks() const { return blocks_.size(); }
  size_t sizeBytes() const { return blocks_.size() * BYTES_PER_BLOCK; }
};

}  // namespace ql::engine::filter
