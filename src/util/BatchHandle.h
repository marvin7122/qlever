// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BATCHHANDLE_H
#define QLEVER_SRC_UTIL_BATCHHANDLE_H

#include <cstdint>

#include "backports/three_way_comparison.h"

namespace ad_utility {

// The handle of a batch of reads, minted by `BatchManager::addBatch` and passed
// back to `wait` (see `IoUringManager.h`). A wrapper type, not an alias for
// `uint64_t`, so that a request id, a byte count or any other number cannot
// silently be passed where a batch handle is expected.
class BatchHandle {
 public:
  // The handle that wraps 0, so that handles can be stored in
  // default-constructed containers.
  BatchHandle() = default;

  // `explicit`, so that a bare integer cannot silently convert to a
  // `BatchHandle` where a handle is expected.
  explicit BatchHandle(uint64_t value) : value_{value} {}

  // Return the wrapped number, for diagnostics.
  [[nodiscard]] uint64_t value() const { return value_; }

  // Two handles are equal iff they wrap the same number.
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(BatchHandle, value_)

 private:
  uint64_t value_ = 0;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BATCHHANDLE_H
