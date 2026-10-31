// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_OUTSTANDINGREADCOUNTS_H
#define QLEVER_SRC_UTIL_OUTSTANDINGREADCOUNTS_H

#include <cstddef>

#include "util/BatchHandle.h"
#include "util/Exception.h"
#include "util/HashMap.h"

namespace ad_utility {

// The outstanding reads of a batch manager: reads that are prepared
// (submitted to a backend, not yet completed) or completed but not yet
// reaped, broken down per batch, plus their total.
//
// The total is a stored value (reading it is O(1)), but it is not a
// second, independently maintained fact: both numbers live only inside
// this class, and its two operations are the only way to change them,
// each updating the per-batch count and the total together. So the
// invariant "the total equals the sum over the batches" is enforced by
// encapsulation: no code outside this class can move one of the numbers
// without the other.
class OutstandingReadCounts {
 public:
  // Count one more prepared read of `handle` as outstanding.
  void countReadPrepared(BatchHandle handle) {
    ++total_;
    ++numberOfOutstandingReadRequestsPerBatch_[handle];
  }

  // Count one reaped read of `handle` as no longer outstanding.
  void countReadReaped(BatchHandle handle) {
    auto batchCountIterator =
        numberOfOutstandingReadRequestsPerBatch_.find(handle);

    AD_CORRECTNESS_CHECK(
        batchCountIterator != numberOfOutstandingReadRequestsPerBatch_.end(),
        "A reaped read must belong to a batch with outstanding "
        "reads; its completion is attributed to that batch.");

    --total_;
    if (--batchCountIterator->second == 0) {
      numberOfOutstandingReadRequestsPerBatch_.erase(batchCountIterator);
    }
  }

  // The total number of outstanding reads. O(1): the sum is kept, not
  // recomputed, and it cannot drift from the per-batch counts because
  // only the two operations above change either of them.
  [[nodiscard]] size_t total() const { return total_; }

  // The number of outstanding reads of `handle` (0 if the batch has
  // none left, or never had any).
  [[nodiscard]] size_t forBatch(BatchHandle handle) const {
    auto batchCountIterator =
        numberOfOutstandingReadRequestsPerBatch_.find(handle);
    return batchCountIterator == numberOfOutstandingReadRequestsPerBatch_.end()
               ? size_t{0}
               : batchCountIterator->second;
  }

 private:
  size_t total_ = 0;
  ad_utility::HashMap<BatchHandle, size_t>
      numberOfOutstandingReadRequestsPerBatch_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_OUTSTANDINGREADCOUNTS_H
