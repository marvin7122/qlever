// Copyright 2024 - 2026, The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/VocabularyInternalExternal.h"

#include <absl/strings/str_cat.h>

#include <optional>
#include <range/v3/view/enumerate.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "global/RuntimeParameters.h"
#include "util/HugePages.h"

// _____________________________________________________________________________
std::string VocabularyInternalExternal::operator[](uint64_t i) const {
  auto fromInternal = internalVocab_[i];
  if (fromInternal.has_value()) {
    return std::string{fromInternal.value()};
  }
  return externalVocab_[i];
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInternalExternal::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  // One pass over `indices`: a word of the internal vocabulary is placed as a
  // view into that vocabulary (no copy); all other indices are collected, with
  // their positions in `indices`, for one batched lookup in the external
  // vocabulary. The internal vocabulary has "holes", so each index needs one
  // membership probe: one cache line with the rank directory (see
  // `vocabulary-internal-rank-lookup`), else a binary search; indices at or
  // past `internalVocab_.endIndex()` are known misses and skip the probe.
  // Results do not depend on the prefetch distance.
  MultiSourceVocabBatchAssembler assembler(indices.size());
  MarkerIndicesAndPositions externalSlots;
  const uint64_t internalEnd = internalVocab_.endIndex();
  auto internalPositionOf = [&](size_t index) -> std::optional<size_t> {
    return index < internalEnd ? internalVocab_.positionOfIndex(index)
                               : std::nullopt;
  };
  auto placeWord = [&](size_t position, size_t index,
                       std::optional<size_t> internalPosition) {
    if (internalPosition.has_value()) {
      assembler.assignUnownedViewAtPosition(
          position, internalVocab_.wordAtPosition(internalPosition.value()));
    } else {
      externalSlots.addPair(index, position);
    }
  };
  const size_t distance =
      internalVocab_.hasIndexRankDirectory()
          ? getRuntimeParameter<
                &RuntimeParameters::vocabularyInternalRankPrefetchDistance_>()
          : 0;
  if (distance == 0) {
    for (const auto& [position, index] : ::ranges::views::enumerate(indices)) {
      placeWord(position, index, internalPositionOf(index));
    }
  } else {
    // With the rank directory, each probe is one cache miss in the directory,
    // and each in-RAM word one more in its offsets and one in its bytes. Issue
    // these loads ahead (see `vocabulary-internal-rank-prefetch-distance`), so
    // that they overlap instead of stalling one after another.
    const size_t n = indices.size();
    std::vector<std::optional<size_t>> internalPositions(n);
    for (size_t i = 0; i < n; ++i) {
      if (i + distance < n) {
        internalVocab_.prefetchPositionOfIndex(indices[i + distance]);
      }
      internalPositions[i] = internalPositionOf(indices[i]);
    }
    for (size_t i = 0; i < n; ++i) {
      if (i + 2 * distance < n && internalPositions[i + 2 * distance]) {
        internalVocab_.prefetchWordOffsetsAtPosition(
            internalPositions[i + 2 * distance].value());
      }
      if (i + distance < n && internalPositions[i + distance]) {
        internalVocab_.prefetchWordAtPosition(
            internalPositions[i + distance].value());
      }
      placeWord(i, indices[i], internalPositions[i]);
    }
  }

  if (externalSlots.empty()) {
    return std::move(assembler).finalizeVocabBatchLookupResult();
  }
  auto external =
      externalVocab_.lookupBatch(externalSlots.getUnderlyingIndices());
  if (externalSlots.size() == indices.size()) {
    // No internal hit: the positions are `0, 1, ...`, so the external batch
    // already is the result.
    return external;
  }
  assembler.scatterSubBatchResultAtPositions(
      std::move(external), externalSlots.getResultPositions());
  return std::move(assembler).finalizeVocabBatchLookupResult();
}

// _____________________________________________________________________________
VocabularyInternalExternal::WordWriter::WordWriter(const std::string& filename,
                                                   size_t milestoneDistance)
    : internalWriter_{absl::StrCat(filename, internalSuffix)},
      externalWriter_{absl::StrCat(filename, externalSuffix)},
      milestoneDistance_{milestoneDistance} {}

// _____________________________________________________________________________
uint64_t VocabularyInternalExternal::WordWriter::operator()(
    std::string_view str, bool isExternal) {
  externalWriter_(str, true);
  if (!isExternal || sinceMilestone_ >= milestoneDistance_ || idx_ == 0) {
    internalWriter_(str, idx_);
    sinceMilestone_ = 0;
  }
  ++sinceMilestone_;
  return idx_++;
}

// _____________________________________________________________________________
void VocabularyInternalExternal::WordWriter::finishImpl() {
  internalWriter_.finish();
  externalWriter_.finish();
}

// _____________________________________________________________________________
VocabularyInternalExternal::WordWriter::~WordWriter() {
  if (!finishWasCalled()) {
    ad_utility::terminateIfThrows([this]() { this->finish(); },
                                  "Calling `finish` from the destructor of "
                                  "`VocabularyInternalExternal::WordWriter`");
  }
}

// _____________________________________________________________________________
void VocabularyInternalExternal::open(const std::string& filename) {
  AD_LOG_INFO << "Reading vocabulary from file " << filename << " ..."
              << std::endl;
  internalVocab_.open(absl::StrCat(filename, internalSuffix));
  externalVocab_.open(absl::StrCat(filename, externalSuffix));
  AD_LOG_INFO << "Done, number of words: " << size() << std::endl;
  AD_LOG_INFO << "Number of words in internal vocabulary (these are also part "
                 "of the external vocabulary): "
              << internalVocab_.size() << std::endl;
  if (getRuntimeParameter<
          &RuntimeParameters::vocabularyInternalRankLookup_>()) {
    const bool useHugePages = getRuntimeParameter<
        &RuntimeParameters::vocabularyInternalRankHugePages_>();
    internalVocab_.buildIndexRankDirectory(useHugePages);
    AD_LOG_INFO << "Rank directory of the internal vocabulary: "
                << internalVocab_.indexRankDirectoryNumBytes() << " bytes for "
                << internalVocab_.endIndex() << " vocabulary indices"
                << std::endl;
    if (useHugePages) {
      const auto [begin, size] = internalVocab_.indexRankDirectoryAllocation();
      const auto hugeBytes = ad_utility::anonHugePageBytes(begin, size);
      AD_LOG_INFO << "Huge pages for the rank directory requested "
                  << "(transparent huge pages: "
                  << ad_utility::transparentHugePagesMode() << "): "
                  << (hugeBytes.has_value() ? std::to_string(hugeBytes.value())
                                            : std::string{"unknown"})
                  << " of " << size
                  << " allocated bytes on huge pages (upper bound)"
                  << std::endl;
    }
  }
}
