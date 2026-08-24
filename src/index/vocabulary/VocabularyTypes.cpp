// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/VocabularyTypes.h"

// _____________________________________________________________________________
// Allocate storage inside the arena and copy the given word into it.
void PmrVocabBatchBuilder::appendWord(std::string_view word) {
  auto guard = makeInvariantGuard();
  if (word.empty()) {
    views_.emplace_back("");
    ++numAppendedWords_;
    return;
  }
  ql::pmr::polymorphic_allocator<char> allocator{buffer_.get()};
  char* mem = allocator.allocate(word.size());
  ql::ranges::copy(word, mem);
  views_.emplace_back(mem, word.size());
  ++numAppendedWords_;
}

// _____________________________________________________________________________
// Finalize and return the immutable batch result.
[[nodiscard]] VocabBatchLookupResult PmrVocabBatchBuilder::finalize() && {
  // Full-coverage contract: the builder must have received exactly the
  // number of words it was constructed for (this also rejects a builder on
  // which no `append*` was ever called).
  AD_CONTRACT_CHECK(views_.size() == expectedSize_);
  // Ownership-transfer invariant: the arena must be alive here, otherwise the
  // finalized result would hold views into storage that dies immediately
  // (mirrors the `storage_ != nullptr` check of `BatchLookupData`).
  AD_CORRECTNESS_CHECK(buffer_ != nullptr);
  auto data =
      std::make_shared<VocabBatchLookupData<
          std::unique_ptr<ql::pmr::monotonic_buffer_resource>>>(
          std::move(buffer_), std::move(views_));
  return makeVocabBatchLookupResult(std::move(data));
}

// _____________________________________________________________________________
MultiSourceVocabBatchAssembler::MultiSourceVocabBatchAssembler(
    size_t totalExpectedWords)
    : assembledWordViews_(totalExpectedWords),
      slotIsFilled_(totalExpectedWords, false) {
  checkInvariants();
}

// _____________________________________________________________________________
void MultiSourceVocabBatchAssembler::checkInvariants() const {
  AD_CORRECTNESS_CHECK(assembledWordViews_.size() == slotIsFilled_.size());
  // Each accepted assignment increments the counter exactly once, so it can
  // never exceed the total number of slots.
  AD_CORRECTNESS_CHECK(filledCount_ <= assembledWordViews_.size());
  for (const auto& owner : storageOwners_) {
    AD_CORRECTNESS_CHECK(owner != nullptr);
  }
}

// ___________________________________________________________________________
// Scatter a child batch lookup result across the specified output positions
// and retain the child result's storage owner so its underlying string
// storage is kept alive.
void MultiSourceVocabBatchAssembler::scatterSubBatchResultAtPositions(
    VocabBatchLookupResult subBatchResult,
    ql::span<const size_t> targetPositions) {
  auto guard = makeInvariantGuard();
  AD_CONTRACT_CHECK(subBatchResult.size() == targetPositions.size());
  // Structural pairing: the retained owner is exactly the owner of the views
  // being scattered, so it cannot fail to back them. A null owner would mean
  // scattering views with no backing storage at all.
  AD_CORRECTNESS_CHECK(subBatchResult.owner_ != nullptr);

  // Strong exception guarantee: validate every target position (bounds and
  // not-yet-filled) before writing any slot, so a throwing call leaves the
  // assembler unchanged and the same scatter can simply be retried.
  for (size_t targetPosition : targetPositions) {
    AD_CONTRACT_CHECK(targetPosition < assembledWordViews_.size());
  }
  for (size_t targetPosition : targetPositions) {
    AD_CONTRACT_CHECK(!slotIsFilled_[targetPosition]);
  }
  for (auto [targetPosition, word] :
       ::ranges::views::zip(targetPositions, subBatchResult)) {
    assignWordAtPosition(targetPosition, word);
  }
  storageOwners_.push_back(subBatchResult.owner_);
}

// ___________________________________________________________________________
// Assign a single resolved string_view to its corresponding output position.
void MultiSourceVocabBatchAssembler::assignWordAtPosition(
    size_t resultPosition, std::string_view word) {
  auto guard = makeInvariantGuard();
  AD_CONTRACT_CHECK(resultPosition < assembledWordViews_.size());
  AD_CORRECTNESS_CHECK(!slotIsFilled_[resultPosition]);
  slotIsFilled_[resultPosition] = true;
  ++filledCount_;
  assembledWordViews_[resultPosition] = word;
}

// _____________________________________________________________________________
// Finalize the assembled batch and return a self-contained
// `VocabBatchLookupResult` (can be called only once).
[[nodiscard]] VocabBatchLookupResult MultiSourceVocabBatchAssembler::finalize()
    && {
  checkInvariants();
  AD_CORRECTNESS_CHECK(!assembledWordViews_.empty());
  AD_CORRECTNESS_CHECK(!storageOwners_.empty());
  AD_CORRECTNESS_CHECK(filledCount_ == assembledWordViews_.size());

  // The assembled views point into storage owned by the registered owners;
  // the owner vector is carried as the backing storage of the single
  // `VocabBatchLookupData` holder.
  using MultiOwnerStorage = std::vector<VocabBatchOwner>;
  auto multiOwnerData =
      std::make_shared<VocabBatchLookupData<MultiOwnerStorage>>(
          std::move(storageOwners_), std::move(assembledWordViews_));
  return makeVocabBatchLookupResult(std::move(multiOwnerData));
}
