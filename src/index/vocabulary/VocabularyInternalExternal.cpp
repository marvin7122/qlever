// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (kalmbach@cs.uni-freiburg.de)

#include "index/vocabulary/VocabularyInternalExternal.h"

#include <absl/strings/str_cat.h>

#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <memory>
#include <string_view>
#include <vector>

#include "backports/algorithm.h"
#include "util/GlobalExecutor.h"

// _____________________________________________________________________________
std::string VocabularyInternalExternal::operator[](uint64_t i) const {
  auto fromInternal = internalVocab_[i];
  if (fromInternal.has_value()) {
    return std::string{fromInternal.value()};
  }
  return externalVocab_[i];
}

namespace {
// A word of the internal vocabulary that is part of a batch: its position in
// the batch and the view into the internal vocabulary.
struct InternalWord {
  size_t position;
  std::string_view word;
};

// Return the words of `indices` that are in `internalVocab`, in the order of
// `indices`.
std::vector<InternalWord> findInternalWords(
    const VocabularyInMemoryBinSearch& internalVocab,
    ql::span<const size_t> indices) {
  std::vector<InternalWord> internalWords;
  for (const size_t position : ql::views::iota(size_t{0}, indices.size())) {
    if (const auto word = internalVocab[indices[position]]) {
      internalWords.push_back({position, word.value()});
    }
  }
  return internalWords;
}

// Walk the positions `0 .. numPositions - 1` of a batch. For a position that
// has an internal word (`internalWords` is sorted by position) call
// `onInternal(internalWord)`, for every other position call
// `onExternal(position)`.
template <typename OnInternal, typename OnExternal>
void forEachPosition(size_t numPositions,
                     ql::span<const InternalWord> internalWords,
                     const OnInternal& onInternal,
                     const OnExternal& onExternal) {
  auto nextInternal = internalWords.begin();
  for (const size_t position : ql::views::iota(size_t{0}, numPositions)) {
    if (nextInternal != internalWords.end() &&
        nextInternal->position == position) {
      onInternal(*nextInternal);
      ++nextInternal;
    } else {
      onExternal(position);
    }
  }
}

// Return the indices of the words of the batch that are not in the internal
// vocabulary, in the order of `indices`.
std::vector<size_t> externalIndices(
    ql::span<const size_t> indices,
    ql::span<const InternalWord> internalWords) {
  std::vector<size_t> result;
  result.reserve(indices.size() - internalWords.size());
  forEachPosition(
      indices.size(), internalWords, [](const InternalWord&) {},
      [&](size_t position) { result.push_back(indices[position]); });
  return result;
}

// Return the views of the whole batch in the order of the batch: the internal
// words at their positions, the external words (which are in the order of
// `externalIndices`) at the remaining positions.
std::vector<std::string_view> mergeWords(
    size_t numWords, ql::span<const InternalWord> internalWords,
    ql::span<const std::string_view> externalWords) {
  AD_CORRECTNESS_CHECK(internalWords.size() + externalWords.size() == numWords);
  std::vector<std::string_view> views;
  views.reserve(numWords);
  auto nextExternal = externalWords.begin();
  forEachPosition(
      numWords, internalWords,
      [&](const InternalWord& internalWord) {
        views.push_back(internalWord.word);
      },
      [&](size_t) { views.push_back(*(nextExternal++)); });
  return views;
}
}  // namespace

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInternalExternal::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());
  AD_CONTRACT_CHECK(ql::ranges::all_of(
      indices, [this](size_t index) { return index < size(); }));

  // In the common case no word of the batch is in the internal vocabulary
  // (only the milestone words and the explicitly cached words are in RAM).
  const auto internalWords = findInternalWords(internalVocab_, indices);
  if (internalWords.empty()) {
    return externalVocab_.lookupBatch(indices);
  }

  // Read the remaining words from the external vocabulary in one batch. The
  // returned batch owns a share of the internal words and that sub-batch
  // result, so all its views stay valid as long as it lives.
  auto data = std::make_shared<SubBatchVocabBatchLookupData>();
  data->buffer().internalWords = internalVocab_.sharedWords();
  ql::span<const std::string_view> externalWords;
  if (const auto indicesToRead = externalIndices(indices, internalWords);
      !indicesToRead.empty()) {
    data->buffer().externalWords = externalVocab_.lookupBatch(indicesToRead);
    externalWords = *data->buffer().externalWords;
  }
  data->views() = mergeWords(indices.size(), internalWords, externalWords);
  return SubBatchVocabBatchLookupData::asResult(std::move(data));
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
}
