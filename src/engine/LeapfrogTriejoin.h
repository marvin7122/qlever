
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"

namespace ql::engine::wcoj {

// _____________________________________________________________________________
// Provide sorted trie iterator interface for `LeapfrogTriejoin`.
class LeapfrogIterator {
 private:
  ql::span<const Id> sortedKeys_;
  size_t currentIndex_ = 0;

 public:
  explicit LeapfrogIterator(ql::span<const Id> sortedKeys)
      : sortedKeys_(sortedKeys), currentIndex_(0) {}

  [[nodiscard]] bool atEnd() const noexcept {
    return currentIndex_ >= sortedKeys_.size();
  }

  [[nodiscard]] Id key() const noexcept { return sortedKeys_[currentIndex_]; }

  void next() noexcept { currentIndex_++; }

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

  // Note: iterators are mutated via seek/next; consider passing by value for explicit ownership.
  size_t p = 0;  // pointer to iterator with smallest key
  Id maxKey = iterators[0].key();
  for (const auto& it : iterators) {
    maxKey = std::max(maxKey, it.key());
  }

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
      // Leapfrog forward to minKey
      iterators[minIndex].seek(minKey);
      if (iterators[minIndex].atEnd()) {
        break;
      }
    }
  }

  return result;
}

}  // namespace ql::engine::wcoj
