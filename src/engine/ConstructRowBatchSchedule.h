// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTROWBATCHSCHEDULE_H
#define QLEVER_SRC_ENGINE_CONSTRUCTROWBATCHSCHEDULE_H

#include <algorithm>
#include <cstddef>
#include <utility>

#include "util/Exception.h"

namespace qlever::constructExport {

// The row batches of a CONSTRUCT export of `numRows` rows. The first batch
// has `initialBatchSize` rows and every following batch twice as many as the
// one before, up to `maxBatchSize` rows; all later batches have
// `maxBatchSize` rows. A small first batch lets the first triples out early;
// the large later batches keep the per-batch cost low. With
// `initialBatchSize >= maxBatchSize`, every batch has `maxBatchSize` rows.
// The last batch may be smaller. Batch `k` is `[begin(k), end(k))`, relative
// to the first row; all functions are O(1) or O(log(maxBatchSize)) and do
// not overflow for any batch sizes.
class ConstructRowBatchSchedule {
 public:
  ConstructRowBatchSchedule(size_t numRows, size_t initialBatchSize,
                            size_t maxBatchSize)
      : numRows_{numRows},
        maxBatchSize_{maxBatchSize},
        initialBatchSize_{std::min(initialBatchSize, maxBatchSize)} {
    AD_CONTRACT_CHECK(initialBatchSize >= 1 && maxBatchSize >= 1);
    // The growing batches are those smaller than `maxBatchSize_`. Stop before
    // doubling could exceed `maxBatchSize_` (and overflow).
    for (size_t size = initialBatchSize_; size < maxBatchSize_; size *= 2) {
      ++numGrowingBatches_;
      rowsInGrowingBatches_ += size;
      if (size > maxBatchSize_ / 2) {
        break;
      }
    }
  }

  // The number of (non-empty) batches.
  size_t numBatches() const {
    if (numRows_ <= rowsInGrowingBatches_) {
      size_t k = 0;
      while (begin(k) < numRows_) {
        ++k;
      }
      return k;
    }
    const size_t rest = numRows_ - rowsInGrowingBatches_;
    return numGrowingBatches_ + rest / maxBatchSize_ +
           (rest % maxBatchSize_ != 0 ? 1 : 0);
  }

  // The first row of batch `k`. Precondition: `k <= numBatches()`.
  size_t begin(size_t k) const {
    if (k < numGrowingBatches_) {
      // initial * (1 + 2 + ... + 2^(k-1)).
      return initialBatchSize_ * ((size_t{1} << k) - 1);
    }
    return rowsInGrowingBatches_ + (k - numGrowingBatches_) * maxBatchSize_;
  }

  // One past the last row of batch `k`. Precondition: `k < numBatches()`.
  size_t end(size_t k) const {
    const size_t first = begin(k);
    const size_t size =
        k < numGrowingBatches_ ? initialBatchSize_ << k : maxBatchSize_;
    return first + std::min(size, numRows_ - first);
  }

  // The size of the first batch of a table when `rowsBefore` rows of the same
  // export were already split into batches (in earlier tables), so that the
  // batch size keeps growing across the tables of a lazy result instead of
  // restarting at `initialBatchSize` for every table: the size of the batch
  // that row `rowsBefore` would fall into if all rows were one table.
  static size_t initialBatchSizeAfter(size_t rowsBefore,
                                      size_t initialBatchSize,
                                      size_t maxBatchSize) {
    size_t size = std::min(initialBatchSize, maxBatchSize);
    size_t covered = 0;
    while (size < maxBatchSize && rowsBefore - covered >= size) {
      covered += size;
      size = size > maxBatchSize / 2 ? maxBatchSize : size * 2;
    }
    return size;
  }

 private:
  size_t numRows_;
  size_t maxBatchSize_;
  size_t initialBatchSize_;
  size_t numGrowingBatches_ = 0;
  size_t rowsInGrowingBatches_ = 0;
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTROWBATCHSCHEDULE_H
