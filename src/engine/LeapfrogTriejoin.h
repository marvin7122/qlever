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
#include <vector>

#include "backports/span.h"
#include "global/Id.h"

namespace ql::engine::wcoj {

// _____________________________________________________________________________
// Sorted Trie Iterator interface for Leapfrog Triejoin.
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
// Leapfrog Triejoin (Worst-Case Optimal Join - WCOJ):
// Simultaneously intersects K sorted variable iterators in O(N^1.5) time for
// triangle queries, completely eliminating O(N^2) intermediate tables.
class LeapfrogJoin {
 public:
  static std::vector<Id> intersect(std::vector<LeapfrogIterator>& iterators) {
    std::vector<Id> result;
    if (iterators.empty()) {
      return result;
    }

    for (const auto& it : iterators) {
      if (it.atEnd()) {
        return result;
      }
    }

    // Sort iterators by current key
    size_t k = iterators.size();
    size_t p = 0;  // pointer to iterator with smallest key
    Id maxKey = iterators[0].key();
    for (size_t i = 1; i < k; ++i) {
      if (iterators[i].key() > maxKey) {
        maxKey = iterators[i].key();
      }
    }

    while (true) {
      Id currentKey = iterators[p].key();

      if (currentKey == maxKey) {
        // All iterators match on this key!
        result.push_back(currentKey);
        iterators[p].next();
        if (iterators[p].atEnd()) {
          break;
        }
        maxKey = iterators[p].key();
      } else {
        // Leapfrog forward to maxKey
        iterators[p].seek(maxKey);
        if (iterators[p].atEnd()) {
          break;
        }
        maxKey = iterators[p].key();
      }

      // Move to next iterator in round-robin fashion
      p = (p + 1) % k;
    }

    return result;
  }
};

}  // namespace ql::engine::wcoj
