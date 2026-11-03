// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IO_URING_BATCHOFREADS_H
#define QLEVER_SRC_UTIL_IO_URING_BATCHOFREADS_H

#include <cstddef>
#include <cstdint>

#include "backports/span.h"
#include "util/Exception.h"

namespace ad_utility {

// The reads of one batch, as the argument of `ReadPolicy::readBatch`: read `i`
// reads `numberOfBytes()[i]` bytes, starting at byte `offsets()[i]` of the
// file, into `buffers()[i]`. A type of its own, so that the three spans have
// the same length by construction: the constructor is the one place that checks
// it, and a policy cannot be handed spans of different lengths. Views only: the
// spans and the buffers they point to must outlive the `readBatch` call.
class BatchOfReads {
 public:
  // Throw if the three spans differ in length.
  BatchOfReads(ql::span<const size_t> numberOfBytes,
               ql::span<const uint64_t> offsets, ql::span<char*> buffers)
      : numberOfBytes_{numberOfBytes}, offsets_{offsets}, buffers_{buffers} {
    AD_CONTRACT_CHECK(offsets_.size() == numberOfBytes_.size() &&
                          buffers_.size() == numberOfBytes_.size(),
                      "The three spans of a batch of reads describe the same "
                      "reads, so they must have the same length.");
  }

  [[nodiscard]] size_t size() const { return numberOfBytes_.size(); }
  [[nodiscard]] ql::span<const size_t> numberOfBytes() const {
    return numberOfBytes_;
  }
  [[nodiscard]] ql::span<const uint64_t> offsets() const { return offsets_; }
  [[nodiscard]] ql::span<char*> buffers() const { return buffers_; }

 private:
  ql::span<const size_t> numberOfBytes_;
  ql::span<const uint64_t> offsets_;
  ql::span<char*> buffers_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IO_URING_BATCHOFREADS_H
