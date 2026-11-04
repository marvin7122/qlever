// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_SORTEDVOCABINDICES_H
#define QLEVER_SRC_INDEX_VOCABULARY_SORTEDVOCABINDICES_H

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <utility>

#include "backports/algorithm.h"
#include "util/BitVectorWithRank.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"
#include "util/OwnedOrViewedVector.h"

namespace ad_utility::vocabulary {

// How `SortedVocabIndices::positionOf` finds the position of an index.
enum class PositionLookup {
  // A binary search over the indices. No additional memory.
  BinarySearch,
  // One query to an `ad_utility::BitVectorWithRank` over `[0, endIndex())`,
  // built once by the constructor. Constant time (one cache line), but costs
  // 8/7 bits per index of `[0, endIndex())`, also for the holes.
  RankDirectory,
  // `RankDirectory` if the directory takes fewer bytes than the indices
  // themselves (8 bytes each), else `BinarySearch`. So the directory at most
  // doubles the memory of the indices. It is smaller if more than 1/56 (1.8 %)
  // of the indices in `[0, endIndex())` are contained.
  RankDirectoryIfSmaller,
};

// The strictly ascending vocabulary indices of the words of a vocabulary with
// holes (see `VocabularyInMemoryBinSearch`), together with the lookup from an
// index to its position among them (see `PositionLookup`). If there is a rank
// directory, it is built from the indices in the constructor, and the indices
// cannot be changed afterwards, so the two always agree.
class SortedVocabIndices : public ad_utility::NoCopy {
 public:
  using Storage = ad_utility::OwnedOrViewedVector<uint64_t>;
  using View = Storage::View;

 private:
  Storage storage_;
  // Only set if the constructor built a rank directory.
  std::optional<ad_utility::BitVectorWithRank> rankDirectory_;

 public:
  //____________________________________________________________________________
  // Create an object without indices, so that every index is a hole.
  SortedVocabIndices() = default;

  //____________________________________________________________________________
  // Take the strictly ascending `indices` (owned or viewed) and set up the
  // lookup of positions given by `positionLookup`. The ascending order is only
  // checked when a rank directory is built.
  SortedVocabIndices(Storage indices, PositionLookup positionLookup)
      : storage_{std::move(indices)} {
    if (positionLookup == PositionLookup::RankDirectoryIfSmaller) {
      positionLookup = isRankDirectorySmaller() ? PositionLookup::RankDirectory
                                                : PositionLookup::BinarySearch;
    }
    if (positionLookup == PositionLookup::RankDirectory) {
      const auto indexView = view();
      // `endIndex()` would overflow otherwise. Vocabulary indices are much
      // smaller.
      AD_CONTRACT_CHECK(indexView.empty() ||
                        indexView.back() <
                            std::numeric_limits<uint64_t>::max());
      rankDirectory_.emplace(indexView, endIndex());
    }
  }

  //____________________________________________________________________________
  // Return the indices, no matter whether they are owned or only viewed. For
  // viewed indices, the returned view is valid as long as the memory they were
  // viewed from (see `OwnedOrViewedVector`).
  View view() const { return storage_.view(); }

  //____________________________________________________________________________
  // Return the underlying storage, for serialization.
  const Storage& storage() const { return storage_; }

  //____________________________________________________________________________
  // Return one more than the largest index, or `0` if there are no indices.
  uint64_t endIndex() const {
    const auto indices = view();
    return indices.empty() ? 0 : indices.back() + 1;
  }

  //____________________________________________________________________________
  // Return the position of `index` among the indices (an offset into them, not
  // a vocabulary index), or `std::nullopt` if it is not contained (a hole, or
  // at least `endIndex()`).
  std::optional<size_t> positionOf(uint64_t index) const {
    if (rankDirectory_.has_value()) {
      const auto rank = rankDirectory_->rankIfContained(index);
      if (!rank.has_value()) {
        return std::nullopt;
      }
      return static_cast<size_t>(rank.value());
    }
    const auto indices = view();
    const auto it = ql::ranges::lower_bound(indices, index);
    if (it != indices.end() && *it == index) {
      return static_cast<size_t>(it - indices.begin());
    }
    return std::nullopt;
  }

  //____________________________________________________________________________
  // Return the number of bytes of the rank directory, `0` if there is none.
  size_t numberOfBytesOfRankDirectory() const {
    return rankDirectory_.has_value() ? rankDirectory_->numberOfBytes() : 0;
  }

 private:
  //____________________________________________________________________________
  // Return whether a rank directory over `[0, endIndex())` takes fewer bytes
  // than the indices (see `PositionLookup::RankDirectoryIfSmaller`).
  bool isRankDirectorySmaller() const {
    const auto indices = view();
    return !indices.empty() &&
           indices.back() < std::numeric_limits<uint64_t>::max() &&
           ad_utility::BitVectorWithRank::numberOfBytesForUniverse(endIndex()) <
               indices.size() * sizeof(uint64_t);
  }
};

}  // namespace ad_utility::vocabulary

#endif  // QLEVER_SRC_INDEX_VOCABULARY_SORTEDVOCABINDICES_H
