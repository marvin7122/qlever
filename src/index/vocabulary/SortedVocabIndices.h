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
};

// The strictly ascending vocabulary indices of the words of a vocabulary with
// holes (see `VocabularyInMemoryBinSearch`), together with the lookup from an
// index to its position among them (see `PositionLookup`). The rank directory
// is built from the indices in the constructor, and the indices cannot be
// changed afterwards, so the two always agree.
class SortedVocabIndices : public ad_utility::NoCopy {
 public:
  using Storage = ad_utility::OwnedOrViewedVector<uint64_t>;
  using View = Storage::View;

 private:
  Storage storage_;
  // Only set for `PositionLookup::RankDirectory`.
  std::optional<ad_utility::BitVectorWithRank> rankDirectory_;

 public:
  // No indices.
  SortedVocabIndices() = default;

  // The `storage` of the indices (strictly ascending, which is only checked
  // for `PositionLookup::RankDirectory`), with the given `positionLookup`.
  SortedVocabIndices(Storage storage, PositionLookup positionLookup)
      : storage_{std::move(storage)} {
    if (positionLookup == PositionLookup::RankDirectory) {
      const auto indices = view();
      // `endIndex()` would overflow otherwise. Vocabulary indices are much
      // smaller.
      AD_CONTRACT_CHECK(indices.empty() ||
                        indices.back() < std::numeric_limits<uint64_t>::max());
      rankDirectory_.emplace(indices, endIndex());
    }
  }

  // The indices, no matter whether they are owned or only viewed.
  View view() const { return storage_.view(); }

  // The underlying storage, for serialization.
  const Storage& storage() const { return storage_; }

  // One more than the largest index, or `0` if there are no indices.
  uint64_t endIndex() const {
    const auto indices = view();
    return indices.empty() ? 0 : indices.back() + 1;
  }

  // The position of `index` among the indices, or `std::nullopt` if it is not
  // contained (a hole, or at least `endIndex()`).
  std::optional<size_t> positionOf(uint64_t index) const {
    if (rankDirectory_.has_value()) {
      return rankDirectory_->rankIfContained(index);
    }
    const auto indices = view();
    const auto it = ql::ranges::lower_bound(indices, index);
    if (it != indices.end() && *it == index) {
      return static_cast<size_t>(it - indices.begin());
    }
    return std::nullopt;
  }

  // The number of bytes of the rank directory, `0` for
  // `PositionLookup::BinarySearch`.
  size_t numBytesOfRankDirectory() const {
    return rankDirectory_.has_value() ? rankDirectory_->numBytes() : 0;
  }
};

}  // namespace ad_utility::vocabulary

#endif  // QLEVER_SRC_INDEX_VOCABULARY_SORTEDVOCABINDICES_H
