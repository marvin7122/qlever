// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPES_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPES_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Invariants.h"
#include "util/Iterators.h"
#include "util/TransparentFunctors.h"
#include "util/Views.h"

// The result type for a batch of vocabulary lookups.
using VocabBatchLookupResult = std::shared_ptr<ql::span<std::string_view>>;

// Type-erased input range of batches (each batch consists of a vector of
// indices into the underlying Vocabulary, specifying which terms' string
// representations need to be read from the underlying Vocabulary).
using VocabLookupInput = ad_utility::InputRangeTypeErased<std::vector<size_t>>;

// Type-erased output range of batch-lookup results (which are the string
// representations of the terms specified by `VocabLookupInput` to be read).
using VocabLookupOutput =
    ad_utility::InputRangeTypeErased<VocabBatchLookupResult>;

// Base class for a vocabulary batch-lookup result, shared by the different
// vocabulary implementations. Owns the materialized string data (`buffer()`,
// whose concrete type `BufferType` depends on the implementation) and one
// `string_view` per looked-up term (`views()`, each pointing into `buffer()`).
// //
// _____________________________________________________________________________
// Type-erased smart pointer holding whatever keeps word storage alive. Used
// to store child `VocabBatchLookupResult`s or references to vocabulary state
// (e.g., shared ownership of a vocabulary's in-memory word storage).
using VocabBatchOwner = std::shared_ptr<const void>;

// _____________________________________________________________________________
// Strong, self-contained batch-lookup result backed by a contiguous character
// buffer (used for reading fixed-size chunks from disk). All storage is
// private.
class ContiguousVocabBatchLookupData {
 private:
  std::vector<char> buffer_;
  std::vector<std::string_view> views_;
  ql::span<std::string_view> span_;

  friend class ContiguousVocabBatchBuilder;

 public:
  static VocabBatchLookupResult asResult(
      std::shared_ptr<ContiguousVocabBatchLookupData> self) {
    self->span_ = ql::span<std::string_view>{self->views_};
    auto* spanPtr = &self->span_;
    return std::shared_ptr<ql::span<std::string_view>>(std::move(self),
                                                       spanPtr);
  }
};

// _____________________________________________________________________________
// Builder for a contiguous batch lookup result. Allocates single contiguous
// memory for all requested word sizes, generates direct destination targets for
// asynchronous I/O (e.g. io_uring), and derives string_views atomically.
class ContiguousVocabBatchBuilder {
 private:
  std::shared_ptr<ContiguousVocabBatchLookupData> data_;
  std::vector<char*> targets_;

 public:
  explicit ContiguousVocabBatchBuilder(ql::span<const size_t> wordSizes)
      : data_{std::make_shared<ContiguousVocabBatchLookupData>()} {
    const size_t totalBytes = ::ranges::accumulate(wordSizes, size_t{0});
    data_->buffer_.resize(totalBytes);
    data_->views_.reserve(wordSizes.size());
    targets_.reserve(wordSizes.size());

    size_t offset = 0;
    for (size_t size : wordSizes) {
      char* target = data_->buffer_.data() + offset;
      targets_.push_back(target);
      data_->views_.emplace_back(target, size);
      offset += size;
    }
  }

  [[nodiscard]] const std::vector<char*>& targets() const noexcept {
    return targets_;
  }

  [[nodiscard]] VocabBatchLookupResult finalize() && {
    return ContiguousVocabBatchLookupData::asResult(std::move(data_));
  }
};

// _____________________________________________________________________________
// Strong, self-contained batch-lookup result backed by a PMR monotonic buffer
// resource.
class PmrVocabBatchLookupData {
 private:
  std::unique_ptr<ql::pmr::monotonic_buffer_resource> buffer_;
  std::vector<std::string_view> views_;
  ql::span<std::string_view> span_;

 public:
  PmrVocabBatchLookupData(
      std::unique_ptr<ql::pmr::monotonic_buffer_resource> buffer,
      std::vector<std::string_view> views)
      : buffer_{std::move(buffer)}, views_{std::move(views)} {
    span_ = ql::span<std::string_view>{views_};
  }

  static VocabBatchLookupResult asResult(
      std::shared_ptr<PmrVocabBatchLookupData> self) {
    auto* spanPtr = &self->span_;
    return std::shared_ptr<ql::span<std::string_view>>(std::move(self),
                                                       spanPtr);
  }
};

// _____________________________________________________________________________
// Strong, self-contained batch-lookup result backed by owning std::strings.
class StringVectorVocabBatchLookupData {
 private:
  std::vector<std::string> buffer_;
  std::vector<std::string_view> views_;
  ql::span<std::string_view> span_;

 public:
  explicit StringVectorVocabBatchLookupData(std::vector<std::string> words)
      : buffer_{std::move(words)} {
    views_ = ::ranges::to_vector(
        buffer_ |
        ql::views::transform(ad_utility::staticCast<std::string_view>));
    span_ = ql::span<std::string_view>{views_};
  }

  static VocabBatchLookupResult asResult(
      std::shared_ptr<StringVectorVocabBatchLookupData> self) {
    auto* spanPtr = &self->span_;
    return std::shared_ptr<ql::span<std::string_view>>(std::move(self),
                                                       spanPtr);
  }
};

// _____________________________________________________________________________
// Strong, self-contained batch-lookup result that owns multiple independent
// storage owners.
class MultiOwnerVocabBatchLookupData {
 private:
  std::vector<VocabBatchOwner> owners_;
  std::vector<std::string_view> views_;
  ql::span<std::string_view> span_;

 public:
  MultiOwnerVocabBatchLookupData(std::vector<VocabBatchOwner> owners,
                                 std::vector<std::string_view> views)
      : owners_{std::move(owners)}, views_{std::move(views)} {
    span_ = ql::span<std::string_view>{views_};
  }

  static VocabBatchLookupResult asResult(
      std::shared_ptr<MultiOwnerVocabBatchLookupData> self) {
    auto* spanPtr = &self->span_;
    return std::shared_ptr<ql::span<std::string_view>>(std::move(self),
                                                       spanPtr);
  }
};

// _____________________________________________________________________________
// A single entry yielded by a vocabulary's `scanAll`: a word together with its
// index in the vocabulary. For most vocabularies the indices are simply
// `0, 1, 2, ...`, but e.g. a `SplitVocabulary` yields the (non-contiguous)
// marker-encoded indices that its `operator[]` expects.
//
// IMPORTANT: `word_` is in general a view into a buffer that is reused when
// the range is advanced (e.g. for the on-disk and compressed vocabularies).
// It is therefore only valid until the next element is pulled from the range;
// consume each entry (or copy the word) before advancing.
struct IndexAndWord {
  uint64_t index_;
  std::string_view word_;
};

// A type-erased input range vocabularies can use for `scanAll()`, that yields
// all words of the vocabulary in order, together with their index.
using VocabularyScanRange = ad_utility::InputRangeTypeErased<IndexAndWord>;

// _____________________________________________________________________________
// Construct a result from owning strings and expose views into their storage.
inline VocabBatchLookupResult makeStringVectorVocabBatchLookupResult(
    std::vector<std::string> words) {
  auto data =
      std::make_shared<StringVectorVocabBatchLookupData>(std::move(words));
  return StringVectorVocabBatchLookupData::asResult(std::move(data));
}

// _____________________________________________________________________________
// Builder for a PMR arena-backed `VocabBatchLookupResult`.
// Encapsulates the `monotonic_buffer_resource`, allocates decompressed word
// storage directly in the arena, and guarantees that all exposed `string_view`s
// are safely backed by the arena storage without leaking memory resource
// accounting to callers.
class ArenaVocabBatchBuilder {
 private:
  std::unique_ptr<ql::pmr::monotonic_buffer_resource> buffer_;
  std::vector<std::string_view> views_;

 public:
  explicit ArenaVocabBatchBuilder(size_t expectedSize)
      : buffer_{std::make_unique<ql::pmr::monotonic_buffer_resource>()} {
    views_.reserve(expectedSize);
  }

  // Allocate storage inside the arena for up to `bound` bytes, invoke
  // `decompress(destinationSpan)` to write the bytes, and register the view.
  template <typename DecompressFunc>
  void appendDecompressedWord(size_t bound, DecompressFunc&& decompress) {
    if (bound == 0) {
      views_.emplace_back("");
      return;
    }

    ql::pmr::polymorphic_allocator<char> allocator{buffer_.get()};
    char* mem = allocator.allocate(bound);
    const size_t bytesWritten = decompress(ql::span<char>{mem, bound});

    AD_CORRECTNESS_CHECK(bytesWritten > 0 && bytesWritten <= bound);
    views_.emplace_back(mem, bytesWritten);
  }

  // Allocate storage inside the arena and copy the given word into it:
  void appendWord(std::string_view word) {
    if (word.empty()) {
      views_.emplace_back("");
      return;
    }
    ql::pmr::polymorphic_allocator<char> allocator{buffer_.get()};
    char* mem = allocator.allocate(word.size());
    std::memcpy(mem, word.data(), word.size());
    views_.emplace_back(mem, word.size());
  }

  // Atomically finalize and return the immutable batch result:
  [[nodiscard]] VocabBatchLookupResult finalize() && {
    AD_CONTRACT_CHECK(!views_.empty());
    auto data = std::make_shared<PmrVocabBatchLookupData>(std::move(buffer_),
                                                          std::move(views_));
    return PmrVocabBatchLookupData::asResult(std::move(data));
  }
};

// _____________________________________________________________________________
// Construct a PMR arena-backed `VocabBatchLookupResult` by copying words into a
// monotonic buffer arena.
inline VocabBatchLookupResult makePmrVocabBatchLookupResult(
    ql::span<const std::string_view> words) {
  ArenaVocabBatchBuilder builder(words.size());
  for (std::string_view word : words) {
    builder.appendWord(word);
  }
  return std::move(builder).finalize();
}

inline VocabBatchLookupResult makePmrVocabBatchLookupResult(
    std::initializer_list<std::string_view> words) {
  return makePmrVocabBatchLookupResult(
      ql::span<const std::string_view>{words.begin(), words.size()});
}

// _____________________________________________________________________________
// Helper struct that encapsulates assembling string_views from multiple
// independent vocabulary sources, verifying collision-free total coverage, and
// aggregating storage ownership into a self-contained `VocabBatchLookupResult`.
class MultiSourceVocabBatchAssembler
    : public ad_utility::WithInvariants<MultiSourceVocabBatchAssembler> {
 private:
  std::vector<std::string_view> assembledWordViews_;
  std::vector<bool> slotFilledTracking_;
  std::vector<VocabBatchOwner> storageOwners_;

 public:
  // ___________________________________________________________________________
  explicit MultiSourceVocabBatchAssembler(size_t totalExpectedWords)
      : assembledWordViews_(totalExpectedWords),
        slotFilledTracking_(totalExpectedWords, false) {}

  // ___________________________________________________________________________
  // Structural invariant enforcement mandated by `WithInvariants<Derived>`.
  void checkInvariants() const {
    AD_CORRECTNESS_CHECK(assembledWordViews_.size() ==
                         slotFilledTracking_.size());
    AD_CORRECTNESS_CHECK(storageOwners_.size() <= assembledWordViews_.size());
  }

  // ___________________________________________________________________________
  // Place a single resolved string_view into its corresponding output position.
  void assignWordAtPosition(size_t resultPosition, std::string_view word) {
    AD_CONTRACT_CHECK(resultPosition < assembledWordViews_.size());
    auto guard = makeInvariantGuard();

    AD_CORRECTNESS_CHECK(!slotFilledTracking_[resultPosition]);
    slotFilledTracking_[resultPosition] = true;
    assembledWordViews_[resultPosition] = word;
  }

  // ___________________________________________________________________________
  // Scatter a child batch lookup result across the specified output positions
  // and retain the child result object so its underlying string storage is kept
  // alive.
  void scatterSubBatchResultAtPositions(
      VocabBatchLookupResult subBatchResult,
      ql::span<const size_t> targetPositions) {
    AD_CONTRACT_CHECK(subBatchResult != nullptr);
    AD_CONTRACT_CHECK(subBatchResult->size() == targetPositions.size());
    auto guard = makeInvariantGuard();

    for (auto [targetPosition, word] :
         ::ranges::views::zip(targetPositions, *subBatchResult)) {
      AD_CORRECTNESS_CHECK(targetPosition < assembledWordViews_.size());
      AD_CORRECTNESS_CHECK(!slotFilledTracking_[targetPosition]);
      slotFilledTracking_[targetPosition] = true;
      assembledWordViews_[targetPosition] = word;
    }
    storageOwners_.push_back(std::move(subBatchResult));
  }

  // ___________________________________________________________________________
  // Register a shared storage owner (e.g. an in-memory vocabulary buffer)
  // that must outlive the assembled string_views.
  void registerStorageOwner(VocabBatchOwner storageOwner) {
    AD_CONTRACT_CHECK(storageOwner != nullptr);
    auto guard = makeInvariantGuard();

    storageOwners_.push_back(std::move(storageOwner));
  }

  // ___________________________________________________________________________
  // Finalize assembly into a self-contained `VocabBatchLookupResult`,
  // verifying that every slot has been populated exactly once.
  [[nodiscard]] VocabBatchLookupResult finalizeVocabBatchLookupResult() && {
    checkInvariants();
    AD_CONTRACT_CHECK(!assembledWordViews_.empty());
    AD_CONTRACT_CHECK(!storageOwners_.empty());
    AD_CORRECTNESS_CHECK(std::all_of(slotFilledTracking_.begin(),
                                     slotFilledTracking_.end(),
                                     [](bool isFilled) { return isFilled; }));

    auto multiOwnerData = std::make_shared<MultiOwnerVocabBatchLookupData>(
        std::move(storageOwners_), std::move(assembledWordViews_));
    return MultiOwnerVocabBatchLookupData::asResult(std::move(multiOwnerData));
  }
};

// _____________________________________________________________________________
// Paired lookup data for one vocabulary marker: for each position `i` in the
// arrays, `underlyingIndices[i]` is the index to look up, and
// `resultPositions[i]` is where the result goes in the final output. The
// arrays are always kept in sync (same size).
struct MarkerIndicesAndPositions {
 private:
  std::vector<size_t> underlyingIndices_;
  std::vector<size_t> resultPositions_;

 public:
  // ___________________________________________________________________________
  // Reserve capacity for the given number of pairs.
  void reserve(size_t capacity) {
    underlyingIndices_.reserve(capacity);
    resultPositions_.reserve(capacity);
  }

  // ___________________________________________________________________________
  // Add a (`underlyingIndex`, `resultPosition`) pair.
  void addPair(size_t underlyingIndex, size_t resultPosition) {
    underlyingIndices_.push_back(underlyingIndex);
    resultPositions_.push_back(resultPosition);
  }

  // ___________________________________________________________________________
  // Access the underlying indices for batch-lookup.
  ql::span<const size_t> getUnderlyingIndices() const {
    return underlyingIndices_;
  }

  // ___________________________________________________________________________
  // Access the result positions for scatter-back.
  ql::span<const size_t> getResultPositions() const { return resultPositions_; }

  // ___________________________________________________________________________
  // Check if this marker has any pairs.
  bool empty() const { return underlyingIndices_.empty(); }

  // ___________________________________________________________________________
  // Number of pairs.
  size_t size() const { return underlyingIndices_.size(); }
};

// _____________________________________________________________________________
// Paired lookup data for each of `NumVocabs` underlying vocabularies, indexed
// by the marker that identifies the vocabulary.
template <size_t NumVocabs>
using IndicesAndPositionsByMarker =
    std::array<MarkerIndicesAndPositions, NumVocabs>;

// _____________________________________________________________________________
// Partition marked indices into paired (`underlyingIndex`, `resultPosition`)
// lists per marker. For each input index, `getMarkerAndVocabIndex` extracts
// the marker that identifies the underlying vocabulary and the unmarked vocab
// index; the index is paired with its position in the input, and both are
// grouped by marker.
template <size_t NumVocabs, typename GetMarkerAndVocabIndex>
IndicesAndPositionsByMarker<NumVocabs> partitionMarkerIndicesAndPositions(
    ql::span<const size_t> indices, GetMarkerAndVocabIndex getMarkerAndIndex) {
  IndicesAndPositionsByMarker<NumVocabs> out;
  for (auto [resultPosition, markedIndex] :
       ::ranges::views::enumerate(indices)) {
    auto [marker, underlyingIndex] = getMarkerAndIndex(markedIndex);
    AD_CORRECTNESS_CHECK(marker < NumVocabs);
    out[marker].addPair(underlyingIndex, resultPosition);
  }
  return out;
}

// _____________________________________________________________________________
// Merge per-vocabulary lookup batches into a single combined
// `VocabBatchLookupResult` where each word is at the position of its original
// input index.
template <size_t NumVocabs, typename ReleaseLookupResultForMarker>
VocabBatchLookupResult mergeMarkerBatchesInInputOrder(
    const IndicesAndPositionsByMarker<NumVocabs>& markerIndicesAndPositions,
    ReleaseLookupResultForMarker releaseLookupResult) {
  size_t totalPositions = 0;
  for (const auto& markerIndices : markerIndicesAndPositions) {
    totalPositions += markerIndices.size();
  }
  MultiSourceVocabBatchAssembler assembler(totalPositions);

  for (const auto& [vocabMarker, markerIndices] :
       ::ranges::views::enumerate(markerIndicesAndPositions)) {
    if (markerIndices.empty()) {
      continue;
    }
    auto lookupResult = releaseLookupResult(vocabMarker);
    assembler.scatterSubBatchResultAtPositions(
        std::move(lookupResult), markerIndices.getResultPositions());
  }
  return std::move(assembler).finalizeVocabBatchLookupResult();
}

// _____________________________________________________________________________
// Generic sequential fallback implementations of the batch-lookup interface,
// used by all vocabularies that do not provide a specialized (e.g. io_uring)
// implementation. They simply loop over the indices and issue the ordinary
// single-word `operator[]` lookups one after another.
namespace ad_utility::vocabulary {

// _____________________________________________________________________________
// Sequential fallback for `lookupBatch`: look up each index individually via
// `vocab[idx]`, returning one `string_view` per index. Works for any vocabulary
// whose `operator[]` yields something convertible to `std::string`.
template <typename Vocab>
VocabBatchLookupResult sequentialLookupBatch(const Vocab& vocab,
                                             ql::span<const size_t> indices) {
  AD_CONTRACT_CHECK(!indices.empty());
  // Materialize the words as owning `std::string`s and move them into the
  // result's `std::vector<std::string>` buffer. The views then point at those
  // strings; no byte copying into a contiguous buffer is needed. Building the
  // views after the move is safe: moving the vector does not relocate the
  // contained strings.

  std::vector<std::string> words = ::ranges::to<std::vector<std::string>>(
      indices | ql::views::transform(
                    [&vocab](size_t idx) { return std::string{vocab[idx]}; }));

  return makeStringVectorVocabBatchLookupResult(std::move(words));
}

// _____________________________________________________________________________
// Streamed version of `lookupBatch`: lazily apply `vocab.lookupBatch` for the
// passed `vocab` to each batch of the (type-erased) input range.
// The referenced `vocab` must outlive the returned range.
template <typename Vocab>
VocabLookupOutput lookupBatchesStreamed(const Vocab& vocab,
                                        VocabLookupInput input) {
  return VocabLookupOutput{ad_utility::OwningView{std::move(input)} |
                           ql::views::transform([&vocab](const auto& indices) {
                             return vocab.lookupBatch(indices);
                           })};
}

}  // namespace ad_utility::vocabulary

// _____________________________________________________________________________
// A word and its index in the vocabulary from which it was obtained. Also
// contains a special state `end()` which can be queried by the `isEnd()`
// function. This can be used to represent words that are larger than the
// largest word in the vocabulary, similar to a typical `end()` iterator.
class WordAndIndex {
 private:
  std::optional<std::pair<std::string, uint64_t>> wordAndIndex_;
  // See the documentation for `previousIndex()` below.
  std::optional<uint64_t> previousIndex_ = std::nullopt;

 public:
  // ___________________________________________________________________________
  // Query for the special `end` semantics.
  bool isEnd() const { return !wordAndIndex_.has_value(); }

  // ___________________________________________________________________________
  // Return the word. Throws if `isEnd() == true`.
  const std::string& word() const {
    AD_CONTRACT_CHECK(wordAndIndex_.has_value());
    return wordAndIndex_.value().first;
  }

  // ___________________________________________________________________________
  // Return the index. Throws if `isEnd() == true`.
  uint64_t index() const {
    AD_CONTRACT_CHECK(wordAndIndex_.has_value());
    return wordAndIndex_.value().second;
  }

  // ___________________________________________________________________________
  uint64_t indexOrDefault(uint64_t defaultValue) const {
    return isEnd() ? defaultValue : index();
  }

  // ___________________________________________________________________________
  // The next valid index before `index()`. If `nullopt` either no
  // such index exists (because `index()` is already the first valid index),
  // or the `previousIndex_` simply wasn't set. This member is currently used to
  // communicate between the `VocabularyInMemoryBinSearch` and the
  // `InternalExternalVocabulary`.
  std::optional<uint64_t>& previousIndex() { return previousIndex_; }

  // ___________________________________________________________________________
  // Assuming this object holds a `lower_bound` result, check whether the word
  // is stored at this position and return an upper bound accordingly.
  template <typename T>
  std::optional<std::pair<uint64_t, uint64_t>> positionOfWord(
      const T& wordToCheck) {
    if (isEnd()) {
      return std::nullopt;
    }
    auto lower = index();
    auto upper = word() == wordToCheck ? lower + 1 : lower;
    return std::pair<uint64_t, uint64_t>{lower, upper};
  }

  // ___________________________________________________________________________
  // The default constructor creates a `WordAndIndex` with `isEnd() == true`.
  WordAndIndex() = default;

  // ___________________________________________________________________________
  // Explicit factory function for the end state.
  static WordAndIndex end() { return {}; }

  // ___________________________________________________________________________
  // Constructors for the ordinary non-end case.
  WordAndIndex(std::string word, uint64_t index)
      : wordAndIndex_{std::in_place, std::move(word), index} {}
  WordAndIndex(std::string_view word, uint64_t index)
      : wordAndIndex_{std::in_place, std::string{word}, index} {}
};

// _____________________________________________________________________________
// A common base class for the `WordWriter` types of different vocabulary
// implementations. It has to be called for each of the words (in the correct
// order).
class WordWriterBase {
 private:
  ad_utility::ThrowInDestructorIfSafe throwIfSafe_;
  std::string readableName_;
  std::atomic_bool finishWasCalled_ = false;

 public:
  // ___________________________________________________________________________
  // Write the next word. The `isExternal` flag is ignored for all the
  // vocabulary implementations but the `VocabularyInternalExternal`. Return the
  // index that was assigned to the word.
  virtual uint64_t operator()(std::string_view word, bool isExternal) = 0;

  // ___________________________________________________________________________
  // Destructor. If `finish` hasn't been called, an exception is thrown if it is
  // safe to do so. Derived classes have to make sure that their destructors
  // call `finish` if necessary. Note: It is unfortunately not possible to call
  // the virtual function `finish` directly from this base class destructor, as
  // at that point the derived class is already destroyed.
  virtual ~WordWriterBase() noexcept(false) {
    using namespace std::string_view_literals;
    if (!finishWasCalled_) {
      throwIfSafe_(
          []() {
            throw std::runtime_error{
                "WordWriterBase::finish was not called before the destructor."};
          },
          "this can happen when `finish` was not called before destroying a"
          " `WordWriter` that inherits from `WordWriterBase`. This is either a"
          " bug, or it can happen when an exception was thrown in the"
          " constructor of the subclass."sv);
    }
  }

  // ___________________________________________________________________________
  // Calling this function will signal that the last word has been pushed.
  // Implementations might e.g. flush all buffers to disk and close underlying
  // files. After calling `finish`, no more calls to `operator()` are allowed.
  // The destructor also calls `finish` if it wasn't called manually.
  virtual void finish() final {
    if (finishWasCalled_.exchange(true)) {
      return;
    }
    finishImpl();
  }

  // ___________________________________________________________________________
  bool finishWasCalled() const { return finishWasCalled_; }

  // ___________________________________________________________________________
  // Access to a `readableName` of the vocabulary that is written. Some
  // implementations use it to customize log messages.
  virtual std::string& readableName() { return readableName_; }

 private:
  // ___________________________________________________________________________
  // The base classes have to implement the actual logic for `finish` here.
  virtual void finishImpl() = 0;
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPES_H
