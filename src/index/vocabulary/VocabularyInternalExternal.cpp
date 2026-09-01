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

#include <string>
#include <string_view>
#include <utility>

#include <range/v3/view/enumerate.hpp>

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
};

// _____________________________________________________________________________
// _____________________________________________________________________________
static IndexPartition partitionIndicesBySource(
    ql::span<const size_t> indices,
    const VocabularyInMemoryBinSearch& internalVocab) {
  IndexPartition result;
  result.internalSlots_.reserve(indices.size());
  result.diskSlots_.reserve(indices.size());

  for (const auto& [i, idx] : ::ranges::views::enumerate(indices)) {
    const auto& fromInternal = internalVocab[idx];
    if (fromInternal.has_value()) {
      result.internalSlots_.addPair(idx, i);
    } else {
      result.diskSlots_.addPair(idx, i);
    }
  }
  return result;
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInternalExternal::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  auto partition = partitionIndicesBySource(indices, internalVocab_);

  if (partition.internalSlots_.empty()) {
    return externalVocab_.lookupBatch(
        partition.diskSlots_.getUnderlyingIndices());
  }

  if (partition.diskSlots_.empty()) {
    return internalVocab_.lookupBatch(
        partition.internalSlots_.getUnderlyingIndices());
  }

  // Handle mixed internal and external indices by assembling results from both
  // sources.
  MultiSourceVocabBatchAssembler assembler(indices.size());

  // 1. Pass the internal sub-result to the assembler, which takes ownership of
  // the result data so its string views remain valid, and place the values at
  // their original request positions.
  auto internal = internalVocab_.lookupBatch(
      partition.internalSlots_.getUnderlyingIndices());
  assembler.scatterSubBatchResultAtPositions(
      std::move(internal), partition.internalSlots_.getResultPositions());

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
}
