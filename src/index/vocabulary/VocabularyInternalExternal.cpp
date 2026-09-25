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
#include <vector>

#include "backports/algorithm.h"

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

  // Partition the indices with a single RAM-cache probe per index. The
  // all-disk fast path below is derived from the partition instead of a
  // separate pre-scan, which would probe every index twice.
  std::vector<size_t> diskIndices;
  std::vector<size_t> diskSlots;
  std::vector<size_t> internalIndices;
  std::vector<size_t> internalSlots;
  diskIndices.reserve(indices.size());
  diskSlots.reserve(indices.size());
  internalIndices.reserve(indices.size());
  internalSlots.reserve(indices.size());

  for (auto [i, idx] : ::ranges::views::enumerate(indices)) {
    if (internalVocab_[idx].has_value()) {
      internalSlots.push_back(static_cast<size_t>(i));
      internalIndices.push_back(idx);
    } else {
      diskSlots.push_back(static_cast<size_t>(i));
      diskIndices.push_back(idx);
    }
  }

  // Fast path: every index misses the RAM cache, so hand the caller's span
  // straight through to the on-disk batch lookup without copying the indices
  // or allocating assembly buffers.
  if (internalIndices.empty()) {
    return externalVocab_.lookupBatch(indices);
  }

  std::vector<std::string_view> assembled(indices.size());
  std::vector<VocabBatchOwner> owners;
  owners.reserve(2);
  if (!diskIndices.empty()) {
    auto disk = externalVocab_.lookupBatch(diskIndices);
    scatterVocabBatchLookupResult(std::move(disk), diskSlots, assembled,
                                  owners);
  }
  if (!internalIndices.empty()) {
    // The internal words live in `internalVocab_`, which the result must not
    // reference directly: resolve them into an owning child batch and retain
    // it, so no view can dangle.
    auto internal = internalVocab_.lookupBatch(internalIndices);
    scatterVocabBatchLookupResult(std::move(internal), internalSlots, assembled,
                                  owners);
  }
  return keepAliveVocabBatch(std::move(owners), std::move(assembled));
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
