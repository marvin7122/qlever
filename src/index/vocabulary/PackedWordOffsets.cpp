// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/PackedWordOffsets.h"

namespace ad_utility::vocabulary {

// _____________________________________________________________________________
PackedWordOffsets::Builder::Builder(size_t expectedNumOffsets) {
  result_.blocks_.reserve((expectedNumOffsets + kOffsetsPerBlock - 1) /
                          kOffsetsPerBlock);
}

// _____________________________________________________________________________
void PackedWordOffsets::Builder::push(uint64_t offset) {
  AD_CONTRACT_CHECK(offset < kEscapeFlag);
  AD_CONTRACT_CHECK(groupSize_ > 0 ? offset >= group_[groupSize_ - 1]
                                   : result_.numOffsets_ == 0 ||
                                         offset >= result_.offset(
                                                       result_.numOffsets_ - 1));
  group_[groupSize_++] = offset;
  if (groupSize_ == kOffsetsPerBlock) {
    flushGroup();
  }
}

// _____________________________________________________________________________
void PackedWordOffsets::Builder::flushGroup() {
  if (groupSize_ == 0) {
    return;
  }
  Block block;
  bool escape = false;
  for (size_t j = 0; j + 1 < groupSize_; ++j) {
    const uint64_t length = group_[j + 1] - group_[j];
    if (length > kMaxStoredLength) {
      escape = true;
      break;
    }
    block.lengths_[j] = static_cast<uint8_t>(length);
  }
  if (escape) {
    block.lengths_.fill(0);
    const size_t slot = result_.exceptions_.size() / kOffsetsPerBlock;
    block.base_ = kEscapeFlag | slot;
    for (size_t j = 0; j < kOffsetsPerBlock; ++j) {
      // Pad a partial last group with its last offset.
      result_.exceptions_.push_back(group_[std::min(j, groupSize_ - 1)]);
    }
  } else {
    // The lengths of a partial last group beyond its end stay 0.
    block.base_ = group_[0];
  }
  result_.blocks_.push_back(block);
  result_.numOffsets_ += groupSize_;
  groupSize_ = 0;
}

// _____________________________________________________________________________
PackedWordOffsets PackedWordOffsets::Builder::finish() && {
  flushGroup();
  result_.exceptions_.shrink_to_fit();
  return std::move(result_);
}

}  // namespace ad_utility::vocabulary
