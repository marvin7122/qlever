
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <algorithm>

#include <vector>

#include "backports/span.h"
#include "global/Id.h"
#include "util/Assertions.h"

namespace ql::engine::wcoj {

// _____________________________________________________________________________
// Provide sorted trie iterator interface for `LeapfrogTriejoin`.
// TODO<marvin7122> Prototype implementation; further optimization and integration needed.
class LeapfrogIterator {
 private:
  ql::span<const Id> sortedKeys_;
  size_t currentIndex_ = 0;

 public:
  explicit LeapfrogIterator(ql::span<const Id> sortedKeys)
      : sortedKeys_(sortedKeys), currentIndex_(0) {
    ADL_CORRECTNESS_CHECK(currentIndex_ <= sortedKeys_.size());
    AD_CHECK(std::is_sorted(sortedKeys_.begin(), sortedKeys_.end()));
  }

  /// @brief Check if the iterator has reached the end of the sorted keys.
  /// @return true if current position is past the last element, false otherwise.
  [[nodiscard]] bool atEnd() const noexcept {
    ADL_CORRECTNESS_CHECK(currentIndex_ <= sortedKeys_.size());
    return currentIndex_ >= sortedKeys_.size();
  }

  [[nodiscard]] Id key() const {
    ADL_CORRECTNESS_CHECK(!atEnd());
    return sortedKeys_[currentIndex_];
  }

  void next() {
    ADL_CORRECTNESS_CHECK(!atEnd());
    currentIndex_++;
  }

  // Fast forward to the first key >= targetKey using binary search
  void seek(Id targetKey) noexcept {
    if (atEnd() || key() >= targetKey) {
      return;
    }
    auto it = std::lower_bound(sortedKeys_.begin() + currentIndex_,
                               sortedKeys_.end(), targetKey);
    currentIndex_ = std::distance(sortedKeys_.begin(), it);
  }
};

// _____________________________________________________________________________
// Free function for intersecting K sorted variable iterators simultaneously.
// Theoretical worst-case optimal complexity for triangle queries (cf. Ngo et al.,
// WCOJ paper).
std::vector<Id> leapfrogIntersect(std::vector<LeapfrogIterator> iterators);

}  // namespace ql::engine::wcoj

// _____________________________________________________________________________
namespace ql::engine::wcoj {

std::vector<Id> leapfrogIntersect(std::vector<LeapfrogIterator> iterators) {
  std::vector<Id> result;
  if (iterators.empty()) {
    return result;
  }

  for (const auto& it : iterators) {
    if (it.atEnd()) {
      return result;
    }
  }

  const size_t k = iterators.size();

  while (true) {
    // Find iterator with smallest current key
    size_t minIndex = 0;
    for (size_t i = 1; i < k; ++i) {
      if (iterators[i].key() < iterators[minIndex].key()) {
        minIndex = i;
      }
    }

    Id minKey = iterators[minIndex].key();

    // Find maximum key across all iterators
    Id maxKey = minKey;
    for (const auto& it : iterators) {
      maxKey = std::max(maxKey, it.key());
    }

    if (minKey == maxKey) {
      // All iterators match on this key!
      result.push_back(minKey);
      iterators[minIndex].next();
      if (iterators[minIndex].atEnd()) {
        break;
      }
    } else {
      // Leapfrog forward to maxKey
      iterators[minIndex].seek(maxKey);
      if (iterators[minIndex].atEnd()) {
        break;
      }
    }
  }

  return result;
}

}  // namespace ql::engine::wcoj
