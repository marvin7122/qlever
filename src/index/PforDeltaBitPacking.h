// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <bit>
#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"

namespace ql::index::compression {

// _____________________________________________________________________________
// PFOR-DELTA Bit-Packing:
// Compresses monotonically increasing 64-bit ValueId sequences into minimal
// bit-widths (e.g. 4..16 bits per ID) by storing delta offsets.
class PforDeltaBitPacking {
 public:
  static constexpr size_t BLOCK_SIZE = 64;

  struct CompressedBlock {
    Id baseValue_{Id::makeUndefined()};
    uint8_t bitWidth_ = 0;
    std::vector<uint64_t> packedWords_;
  };

  // Compress a block of 64 sorted Ids using frame-of-reference deltas.
  static CompressedBlock compressBlock(ql::span<const Id> inputIds) {
    AD_CORRECTNESS_CHECK(!inputIds.empty());
    CompressedBlock block;
    block.baseValue_ = inputIds.front();

    const size_t n = inputIds.size();
    std::vector<uint64_t> deltas(n, 0);
    uint64_t maxDelta = 0;

    for (size_t i = 0; i < n; ++i) {
      deltas[i] = inputIds[i].getBits() - block.baseValue_.getBits();
      if (deltas[i] > maxDelta) {
        maxDelta = deltas[i];
      }
    }

    block.bitWidth_ = (maxDelta == 0) ? 1 : static_cast<uint8_t>(std::bit_width(maxDelta));
    if (block.bitWidth_ > 64) {
      block.bitWidth_ = 64;
    }

    // Pack deltas into 64-bit words
    size_t totalBits = n * block.bitWidth_;
    block.packedWords_.resize((totalBits + 63) / 64, 0);

    for (size_t i = 0; i < n; ++i) {
      size_t bitOffset = i * block.bitWidth_;
      size_t wordIdx = bitOffset / 64;
      size_t intraWordOffset = bitOffset % 64;

      block.packedWords_[wordIdx] |= (deltas[i] << intraWordOffset);
      if (intraWordOffset + block.bitWidth_ > 64 && (wordIdx + 1) < block.packedWords_.size()) {
        block.packedWords_[wordIdx + 1] |= (deltas[i] >> (64 - intraWordOffset));
      }
    }

    return block;
  }

  // Decompress a block back into full 64-bit ValueId integers.
  static void decompressBlock(
      const CompressedBlock& block, size_t numRows,
      ql::span<Id> outputIds) {
    AD_CORRECTNESS_CHECK(outputIds.size() >= numRows);
    const uint64_t mask = (block.bitWidth_ == 64) ? ~0ULL : ((1ULL << block.bitWidth_) - 1);

    for (size_t i = 0; i < numRows; ++i) {
      size_t bitOffset = i * block.bitWidth_;
      size_t wordIdx = bitOffset / 64;
      size_t intraWordOffset = bitOffset % 64;

      uint64_t delta = (block.packedWords_[wordIdx] >> intraWordOffset);
      if (intraWordOffset + block.bitWidth_ > 64 && (wordIdx + 1) < block.packedWords_.size()) {
        delta |= (block.packedWords_[wordIdx + 1] << (64 - intraWordOffset));
      }
      delta &= mask;

      outputIds[i] = Id::fromBits(block.baseValue_.getBits() + delta);
    }
  }
};

}  // namespace ql::index::compression
