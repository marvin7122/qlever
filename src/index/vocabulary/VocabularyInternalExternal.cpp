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

#include <range/v3/view/enumerate.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "global/RuntimeParameters.h"
#include "util/HugePages.h"

namespace ad_utility::vocabulary {

// _____________________________________________________________________________
std::string VocabularyInternalExternal::operator[](uint64_t i) const {
  auto fromInternal = internalVocab_[i];
  if (fromInternal.has_value()) {
    return std::string{fromInternal.value()};
  }
  return externalVocab_[i];
}

// _____________________________________________________________________________
// Partition input indices into internal-vocabulary hits and indices that must
// be resolved by the external vocabulary, while keeping their positions in the
// original input. Keeping the two groups separate allows each vocabulary to be
// batch-looked-up independently; the stored result positions are required to
// restore the original request order when the sub-results are assembled.
struct IndexPartition {
  MarkerIndicesAndPositions internalSlots_;
  MarkerIndicesAndPositions diskSlots_;
  // Words probed from the internal vocabulary while partitioning, in the same
  // order as `internalSlots_`. Reusing them below avoids looking each
  // internal hit up a second time inside `lookupBatch`.
  std::vector<std::string_view> internalWords_;
};

// _____________________________________________________________________________
// _____________________________________________________________________________
static IndexPartition partitionIndicesBySource(
    ql::span<const size_t> indices,
    const VocabularyInMemoryBinSearch& internalVocab) {
  IndexPartition result;
  result.internalSlots_.reserve(indices.size());
  result.diskSlots_.reserve(indices.size());
  result.internalWords_.reserve(indices.size());

  // Indices at or past `internalVocab.endIndex()` are known misses and skip
  // the membership probe. With the rank directory (see
  // `vocabulary-internal-rank-lookup`), the probe is one cache line; without
  // it, a binary search over the sorted indices of the in-RAM words.
  const uint64_t internalEnd = internalVocab.endIndex();
  auto internalPositionOf = [&](size_t index) -> std::optional<size_t> {
    return index < internalEnd ? internalVocab.positionOfIndex(index)
                               : std::nullopt;
  };
  auto place = [&](size_t i, size_t index,
                   std::optional<size_t> internalPosition) {
    if (internalPosition.has_value()) {
      result.internalSlots_.addPair(index, i);
      result.internalWords_.push_back(
          internalVocab.wordAtPosition(internalPosition.value()));
    } else {
      result.diskSlots_.addPair(index, i);
    }
  };

  // Results do not depend on the prefetch distance.
  const size_t distance =
      internalVocab.hasIndexRankDirectory()
          ? getRuntimeParameter<
                &RuntimeParameters::vocabularyInternalRankPrefetchDistance_>()
          : 0;
  if (distance == 0) {
    for (const auto& [i, idx] : ::ranges::views::enumerate(indices)) {
      place(i, idx, internalPositionOf(idx));
    }
    return result;
  }

  // With the rank directory, each probe is one cache miss in the directory,
  // and each in-RAM word one more in its offsets and one in its bytes. Issue
  // these loads ahead (see `vocabulary-internal-rank-prefetch-distance`), so
  // that they overlap instead of stalling one after another.
  const size_t n = indices.size();
  // Scratch buffer for the per-index probe results. `thread_local` (one buffer
  // per export helper thread) so that only growth is paid instead of one
  // allocation plus O(n) initialization per batch. Every element is overwritten
  // in the first loop below before it is read, so retained values never leak
  // across batches. This function never suspends, so a coroutine cannot migrate
  // threads between the writes and the reads.
  thread_local std::vector<std::optional<size_t>> internalPositions;
  internalPositions.resize(n);
  for (size_t i = 0; i < n; ++i) {
    if (i + distance < n) {
      internalVocab.prefetchPositionOfIndex(indices[i + distance]);
    }
    internalPositions[i] = internalPositionOf(indices[i]);
  }
  for (size_t i = 0; i < n; ++i) {
    if (i + 2 * distance < n && internalPositions[i + 2 * distance]) {
      internalVocab.prefetchWordOffsetsAtPosition(
          internalPositions[i + 2 * distance].value());
    }
    if (i + distance < n && internalPositions[i + distance]) {
      internalVocab.prefetchWordAtPosition(
          internalPositions[i + distance].value());
    }
    place(i, indices[i], internalPositions[i]);
  }
  return result;
}

// _____________________________________________________________________________
// Assemble a self-contained batch result from the internal words preserved
// during partitioning (no second vocabulary lookup for these hits).
static VocabBatchLookupResult makeInternalSubBatchResult(
    ql::span<const std::string_view> internalWords) {
  return makePmrVocabBatchLookupResult(internalWords);
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInternalExternal::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  auto partition = partitionIndicesBySource(indices, internalVocab_);

  // Take the fast path when all indices are resolved through the external
  // (disk) vocabulary.
  if (partition.internalSlots_.empty()) {
    return externalVocab_.lookupBatch(
        partition.diskSlots_.getUnderlyingIndices());
  }

  if (partition.diskSlots_.empty()) {
    return makeInternalSubBatchResult(partition.internalWords_);
  }

  // Handle mixed internal and external indices by assembling results from both
  // sources.
  MultiSourceVocabBatchAssembler assembler(indices.size());

  // 1. Pass the internal sub-result to the assembler, which takes ownership of
  // the result data so its string views remain valid, and place the values at
  // their original request positions.
  auto internal = makeInternalSubBatchResult(partition.internalWords_);
  assembler.scatterSubBatchResultAtPositions(
      internal, partition.internalSlots_.getResultPositions());

  // 2. Pass the external sub-result to the assembler and retain its result data
  // so the returned string views remain valid, placing the values at their
  // original request positions.
  auto disk =
      externalVocab_.lookupBatch(partition.diskSlots_.getUnderlyingIndices());
  assembler.scatterSubBatchResultAtPositions(
      std::move(disk), partition.diskSlots_.getResultPositions());

  return std::move(assembler).finalizeVocabBatchLookupResult();
}

// _____________________________________________________________________________
VocabularyInternalExternal::WordWriter::WordWriter(const std::string& filename,
                                                   size_t milestoneDistance)
    : internalWriter_{filename + ".internal"},
      externalWriter_{filename + ".external"},
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
  internalVocab_.open(filename + ".internal");
  externalVocab_.open(filename + ".external");
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
                  << " of " << size << " allocated bytes on huge pages"
                  << std::endl;
    }
  }
}
}  // namespace ad_utility::vocabulary
