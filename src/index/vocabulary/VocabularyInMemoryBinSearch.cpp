// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "index/vocabulary/VocabularyInMemoryBinSearch.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <numeric>

using std::string;

// _____________________________________________________________________________
VocabularyInMemoryBinSearch::IndicesView VocabularyInMemoryBinSearch::indices()
    const {
  return std::visit(
      [](const auto& indices) -> IndicesView {
        return {indices.data(), indices.size()};
      },
      indices_);
}

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::open(const string& fileName) {
  AD_CORRECTNESS_CHECK(
      words_.size() == 0 && indices().empty(),
      "Calling open on the same vocabulary twice is probably a bug");
  {
    ad_utility::serialization::FileReadSerializer file(fileName);
    file >> words_;
  }
  {
    ad_utility::serialization::FileReadSerializer idFile(
        absl::StrCat(fileName, idsSuffix));
    idFile >> ownedIndices();
  }
}

// _____________________________________________________________________________
std::optional<size_t> VocabularyInMemoryBinSearch::positionOfIndex(
    uint64_t index) const {
  if (indexRankDirectory_.has_value()) {
    return indexRankDirectory_->rankIfContained(index);
  }
  auto indices = this->indices();
  auto it = ql::ranges::lower_bound(indices, index);
  if (it != indices.end() && *it == index) {
    return static_cast<size_t>(it - indices.begin());
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::vector<std::optional<size_t>>
VocabularyInMemoryBinSearch::positionsOfIndices(
    ql::span<const size_t> indices) const {
  std::vector<std::optional<size_t>> result(indices.size());
  if (indexRankDirectory_.has_value()) {
    for (size_t i = 0; i < indices.size(); ++i) {
      result[i] = positionOfIndex(indices[i]);
    }
    return result;
  }
  std::vector<size_t> order(indices.size());
  std::iota(order.begin(), order.end(), size_t{0});
  ql::ranges::sort(order, std::less<>{},
                   [&indices](size_t i) { return indices[i]; });
  // Invariant: all vocabulary indices before position `lo` are smaller than
  // the current (and hence every later) requested index.
  auto all = this->indices();
  size_t lo = 0;
  for (size_t i : order) {
    const uint64_t index = indices[i];
    // Gallop forward from `lo` until `all[hi] >= index`, then binary search in
    // the last step.
    size_t hi = lo;
    size_t step = 1;
    while (hi < all.size() && all[hi] < index) {
      lo = hi + 1;
      hi += step;
      step *= 2;
    }
    hi = std::min(hi, all.size());
    auto it = std::lower_bound(all.begin() + lo, all.begin() + hi, index);
    lo = static_cast<size_t>(it - all.begin());
    if (it != all.end() && *it == index) {
      result[i] = lo;
    }
  }
  return result;
}

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::buildIndexRankDirectory() {
  indexRankDirectory_.reset();
  indexRankDirectory_.emplace(indices(), endIndex());
}

// _____________________________________________________________________________
uint64_t VocabularyInMemoryBinSearch::indexAtPosition(size_t position) const {
  auto indices = this->indices();
  AD_CORRECTNESS_CHECK(position < indices.size());
  return indices[position];
}

// _____________________________________________________________________________
uint64_t VocabularyInMemoryBinSearch::endIndex() const {
  auto indices = this->indices();
  return indices.empty() ? 0 : indices[indices.size() - 1] + 1;
}

// _____________________________________________________________________________
std::string_view VocabularyInMemoryBinSearch::wordAtPosition(
    size_t position) const {
  AD_CORRECTNESS_CHECK(position < words_.size());
  return words_[position];
}

// _____________________________________________________________________________
std::optional<std::string_view> VocabularyInMemoryBinSearch::operator[](
    uint64_t index) const {
  auto position = positionOfIndex(index);
  if (!position.has_value()) {
    return std::nullopt;
  }
  return wordAtPosition(position.value());
}

// _____________________________________________________________________________
WordAndIndex VocabularyInMemoryBinSearch::iteratorToWordAndIndex(
    ql::ranges::iterator_t<Words> it) const {
  if (it == words_.end()) {
    return WordAndIndex::end();
  }
  auto idx = static_cast<uint64_t>(it - words_.begin());
  auto indices = this->indices();
  WordAndIndex result{words_[idx], indices[idx]};
  if (idx > 0) {
    result.previousIndex() = indices[idx - 1];
  }
  return result;
}

// _____________________________________________________________________________
[[noreturn]] std::unique_ptr<WordWriterBase>
VocabularyInMemoryBinSearch::makeDiskWriterPtr(
    [[maybe_unused]] const std::string& filename) {
  AD_THROW(
      "A vocabulary with holes cannot be built word by word, because the "
      "`WordWriterBase` interface cannot express the explicit indices. Such a "
      "vocabulary can only be created by filtering an existing vocabulary.");
}

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::close() {
  words_.clear();
  indices_.emplace<Indices>();
  indexRankDirectory_.reset();
}

// _____________________________________________________________________________
VocabularyInMemoryBinSearch::WordWriter::WordWriter(const std::string& filename)
    : writer_{filename}, offsetWriter_{absl::StrCat(filename, idsSuffix)} {}

// _____________________________________________________________________________
uint64_t VocabularyInMemoryBinSearch::WordWriter::operator()(
    std::string_view str, uint64_t idx) {
  // Check that the indices are ascending.
  AD_CONTRACT_CHECK(!lastIndex_.has_value() || lastIndex_.value() < idx);
  lastIndex_ = idx;
  writer_.push(str.data(), str.size());
  offsetWriter_.push(idx);
  return idx;
}

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::WordWriter::finish() {
  writer_.finish();
  offsetWriter_.finish();
}
