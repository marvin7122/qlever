// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <cstdint>
#include <vector>

#include "backports/span.h"


namespace ql::engine::rle {

// Architecture invariants:
// 1. Heuristic 1: Builder manages tracking state; avoid AD_CONTRACT_CHECK on caller-supplied params
// 2. Heuristic 2: Buffer/view pairs are constructed via the Builder's append method
// 3. Heuristic 3: build() && returns RleVectorStream with validated invariants

// _____________________________________________________________________________
// Run-Length Encoded (RLE) Vector Stream for Late Materialization:
// Passes repeated predicate and subject IDs as (Id, RunLength) pairs,
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

  // Builder state management per Heuristic 1 - avoid AD_CONTRACT_CHECK on caller-supplied tracking params
  class Builder {
    RleVectorStream* stream_;
  public:
    explicit Builder(RleVectorStream* stream) : stream_(stream) {}
    Builder& add(Id value, uint32_t length) {
      stream_->append(value, length);
      return *this;
    }
    RleVectorStream build() && {
      // Validate all invariants before finalization per Heuristic 3
      stream_->validateInvariants();
      return std::move(*stream_);
    }
  };

 public:
  Builder beginBuild() { return Builder(this); }
  void append(Id value, uint32_t length) {
    if (!runs_.empty() && runs_.back().value_ == value) {
      runs_.back().length_ += length;
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

    // Late-materialize the RLE runs directly into the destination `span<Id>`.
  void materialize(ql::span<Id> dest) const {
    AD_CORRECTNESS_CHECK(dest.size() >= totalUncompressedRows_);
    size_t outIdx = 0;
    for (const auto& run : runs_) {
      for (uint32_t k = 0; k < run.length_; ++k) {
        dest[outIdx++] = run.value_;
      }
    }
  }
};

}  // namespace ql::engine::rle
