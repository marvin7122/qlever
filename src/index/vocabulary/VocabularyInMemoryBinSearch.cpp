// Copyright 2024 - 2026, The QLever Authors, in particular:
//
// 2024        Johannes Kalmbach <johannes.kalmbach@gmail.com>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/VocabularyInMemoryBinSearch.h"

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
  Words words;
  {
    ad_utility::serialization::FileReadSerializer file(fileName);
    file >> words;
  }
  Indices indices;
  {
    ad_utility::serialization::FileReadSerializer idFile(fileName + ".ids");
    idFile >> indices;
  }
  AD_CORRECTNESS_CHECK(indices.size() == words.size());
  // Ensure that the deserialized indices are strictly ascending because binary
  // search relies on this property before publishing either buffer.
  for (size_t i = 1; i < indices.size(); ++i) {
    AD_CORRECTNESS_CHECK(
        indices[i - 1] < indices[i],
        "Deserialized vocabulary indices must be strictly ascending");
  }
  words_ = std::move(words);
  indices_ = std::move(indices);
}

// _____________________________________________________________________________
std::optional<size_t> VocabularyInMemoryBinSearch::positionOfIndex(
    uint64_t index) const {
  auto indices = this->indices();
  auto it = ql::ranges::lower_bound(indices, index);
  if (it != indices.end() && *it == index) {
    return static_cast<size_t>(it - indices.begin());
  }
  return std::nullopt;
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
  if (it == words().end()) {
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
}

// _____________________________________________________________________________
VocabularyInMemoryBinSearch::WordWriter::WordWriter(const std::string& filename)
    : writer_{filename}, offsetWriter_{filename + ".ids"} {}

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
