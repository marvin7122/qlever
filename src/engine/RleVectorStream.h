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
#include <limits>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace ql::engine::rle {

// _____________________________________________________________________________
// Run-Length Encoded (RLE) Vector Stream for Late Materialization:
// Passes repeated predicate and subject IDs as (ValueId, RunLength) pairs,
// avoiding copying millions of redundant IDs across query execution tree nodes.
class RleVectorStream {
 public:
  struct Run {
    Id value_{Id::makeUndefined()};
    uint32_t length_ = 0;
  };

 private:
  std::vector<Run> runs_;
  size_t totalUncompressedRows_ = 0;

 public:
  void append(Id value, uint32_t length) {
    if (!runs_.empty() && runs_.back().value_ == value) {
      // Merging two `uint32_t` lengths can wrap past `UINT32_MAX`: saturate
      // the current run and spill the remainder into a fresh run instead.
      uint64_t merged = static_cast<uint64_t>(runs_.back().length_) + length;
      if (merged <= std::numeric_limits<uint32_t>::max()) {
        runs_.back().length_ = static_cast<uint32_t>(merged);
      } else {
        runs_.back().length_ = std::numeric_limits<uint32_t>::max();
        runs_.push_back(
            {value, static_cast<uint32_t>(
                        merged - std::numeric_limits<uint32_t>::max())});
      }
    } else {
      runs_.push_back({value, length});
    }
    totalUncompressedRows_ += length;
  }

  [[nodiscard]] size_t numRuns() const noexcept { return runs_.size(); }
  [[nodiscard]] size_t totalRows() const noexcept {
    return totalUncompressedRows_;
  }

  [[nodiscard]] ql::span<const Run> runs() const noexcept { return runs_; }

  // Late materialization: expands RLE runs directly into destination buffer
  void materialize(ql::span<Id> dest) const {
    AD_CORRECTNESS_CHECK(dest.size() >= totalUncompressedRows_);
    size_t outIdx = 0;
    for (const auto& run : runs_) {
      std::fill_n(dest.data() + outIdx, run.length_, run.value_);
      outIdx += run.length_;
    }
  }
};

}  // namespace ql::engine::rle
