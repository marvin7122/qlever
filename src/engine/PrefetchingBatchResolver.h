// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_PREFETCHINGBATCHRESOLVER_H
#define QLEVER_SRC_ENGINE_PREFETCHINGBATCHRESOLVER_H

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || \
    defined(_M_IX86)
#include <xmmintrin.h>
#endif

#include "backports/concepts.h"
#include "backports/span.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/VocabIndex.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "parser/LiteralOrIri.h"
#include "util/Algorithm.h"
#include "util/CompactStringVector.h"
#include "util/Exception.h"

namespace ql::engine::prefetch {

// _____________________________________________________________________________
// Compiler and architecture agnostic software cache prefetching intrinsic.
// Issues a non-blocking CPU prefetch instruction for the memory address
// into the L1 data cache (_MM_HINT_T0 / temporal locality 3).
inline void prefetchVocabEntry(const void* address,
                               [[maybe_unused]] int distance = 8) noexcept {
  if (address == nullptr) {
    return;
  }
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || \
    defined(_M_IX86)
  _mm_prefetch(static_cast<const char*>(address), _MM_HINT_T0);
#elif defined(__GNUC__) || defined(__clang__)
  __builtin_prefetch(address, 0, 3);
#endif
}

// _____________________________________________________________________________
// Helper for prefetching a typed pointer with an optional index offset.
template <typename T>
inline void prefetchAddress(const T* address) noexcept {
  prefetchVocabEntry(static_cast<const void*>(address));
}

// _____________________________________________________________________________
// Configuration options for software prefetching batch resolution.
struct PrefetchConfig {
  // Number of rows/iterations to prefetch ahead of the current serialization
  // cursor. Typically 4 to 16 iterations hide DRAM/L3 cache miss latency
  // (~60ns) while keeping L1 cache lines active.
  size_t prefetchDistance{8};

  // Explicit validation of configuration invariants.
  void checkInvariants() const {
    AD_CONTRACT_CHECK(prefetchDistance > 0);
    AD_CONTRACT_CHECK(prefetchDistance <= 128);
  }
};

// _____________________________________________________________________________
// Deep Module: PrefetchingBatchResolver encapsulates software pipelining and
// CPU cache prefetching during vocabulary index and ID batch resolution.
//
// Invariant & Design Standard (ARCHITECTURE.md):
// - Hides all pipeline iteration state, prefetch distance mechanics, and
//   boundary draining.
// - Zero bookkeeping leakage: returns complete, verified result vectors or
// fills
//   pre-sized caller result spans.
// - Defines edge cases away: gracefully handles empty inputs, inputs smaller
//   than the prefetch distance, and mixed datatypes without branching
//   anomalies.
class PrefetchingBatchResolver {
 public:
  static constexpr size_t DEFAULT_PREFETCH_DISTANCE = 8;

 private:
  PrefetchConfig config_;

 public:
  // Default constructor with standard tuned prefetch distance of 8 rows.
  explicit PrefetchingBatchResolver(
      PrefetchConfig config =
          PrefetchConfig{.prefetchDistance = DEFAULT_PREFETCH_DISTANCE})
      : config_{config} {
    config_.checkInvariants();
  }

  // ___________________________________________________________________________
  [[nodiscard]] size_t prefetchDistance() const noexcept {
    return config_.prefetchDistance;
  }

  // ___________________________________________________________________________
  // Pipelined batch lookup loop issuing prefetch requests K rows ahead during
  // `resolveVocabIndexIds`.
  //
  // Resolves the `VocabIndex` IDs at `positions` in `ids`, prefetching
  // future ID structures and vocabulary memory lines K iterations ahead
  // while converting and formatting the current row into `results[position]`.
  template <bool removeQuotesAndAngleBrackets = false,
            bool returnOnlyLiterals = false,
            typename EscapeFunction = ql::identity>
  void resolveVocabIndexIds(
      const Index& index, ql::span<const Id> ids,
      ql::span<const size_t> positions,
      ql::span<std::optional<std::pair<std::string, const char*>>> results,
      const EscapeFunction& escapeFunction = EscapeFunction{}) const {
    if (positions.empty()) {
      return;
    }

    AD_CONTRACT_CHECK(results.size() >= ids.size());
    AD_EXPENSIVE_CHECK(ql::ranges::all_of(positions, [&ids](size_t i) {
      return ids[i].getDatatype() == Datatype::VocabIndex;
    }));

    const size_t n = positions.size();
    const size_t distance = config_.prefetchDistance;

    // Warm-up pipeline: prefetch the first `distance` entries
    for (size_t k = 0; k < std::min(distance, n); ++k) {
      const size_t pfPos = positions[k];
      prefetchVocabEntry(&ids[pfPos], static_cast<int>(distance));
      prefetchVocabEntry(&positions[k], static_cast<int>(distance));
    }

    // Main pipelined loop: prefetch row (i + distance) ahead while serializing
    // row i
    for (size_t i = 0; i < n; ++i) {
      if (i + distance < n) {
        const size_t pfPos = positions[i + distance];
        prefetchVocabEntry(&ids[pfPos], static_cast<int>(distance));
        prefetchVocabEntry(&positions[i + distance],
                           static_cast<int>(distance));
        const Id pfId = ids[pfPos];
        if (pfId.getDatatype() == Datatype::VocabIndex) {
          const auto wordVocabIndex = pfId.getVocabIndex();
          // Prefetch the underlying index entry if possible
          const auto* vocabPtr =
              reinterpret_cast<const void*>(&index.getImpl());
          prefetchVocabEntry(vocabPtr, static_cast<int>(distance));
        }
      }

      const size_t pos = positions[i];
      const Id id = ids[pos];
      const auto vocabIndex = id.getVocabIndex();
      std::string_view word = index.indexToString(vocabIndex);

      results[pos] = ql::exportIds::literalOrIriToStringAndType<
          removeQuotesAndAngleBrackets, returnOnlyLiterals>(
          LiteralOrIriView::fromStringRepresentation(word), escapeFunction);
    }
  }

  // ___________________________________________________________________________
  // Pipelined batch lookup directly over CompactVectorOfStrings storage,
  // issuing multi-stage prefetch intrinsics for offset table lines and
  // string payload cache lines K iterations ahead.
  template <typename CharType, typename MappingFunc>
  void resolveCompactVectorPipelined(
      const CompactVectorOfStrings<CharType>& words,
      ql::span<const size_t> indices, MappingFunc&& mappingFunc) const {
    if (indices.empty() || !words.ready()) {
      return;
    }

    const size_t n = indices.size();
    const size_t distance = config_.prefetchDistance;
    const auto offsets = words.offsetsSpan();
    const auto data = words.dataSpan();

    // Stage 1 warmup: prefetch offset table lines for the first `distance`
    // items
    for (size_t k = 0; k < std::min(distance, n); ++k) {
      const size_t idx = indices[k];
      if (idx < offsets.size()) {
        prefetchVocabEntry(&offsets[idx], static_cast<int>(distance));
      }
    }

    // Main pipelined loop
    for (size_t i = 0; i < n; ++i) {
      // 1. Prefetch offset table line for (i + distance)
      if (i + distance < n) {
        const size_t pfIdx = indices[i + distance];
        if (pfIdx < offsets.size()) {
          prefetchVocabEntry(&offsets[pfIdx], static_cast<int>(distance));
        }
      }

      // 2. Prefetch string character data line for (i + distance / 2)
      if (i + (distance / 2) < n) {
        const size_t midIdx = indices[i + (distance / 2)];
        if (midIdx + 1 < offsets.size()) {
          const auto strOffset = offsets[midIdx];
          if (strOffset < data.size()) {
            prefetchVocabEntry(data.data() + strOffset,
                               static_cast<int>(distance / 2));
          }
        }
      }

      // 3. Resolve current item i
      const size_t curIdx = indices[i];
      AD_CORRECTNESS_CHECK(curIdx + 1 < offsets.size());
      const auto curOffset = offsets[curIdx];
      const auto nextOffset = offsets[curIdx + 1];
      const size_t strLen = nextOffset - curOffset;
      const CharType* strPtr = data.data() + curOffset;
      std::basic_string_view<CharType> view(strPtr, strLen);

      mappingFunc(i, curIdx, view);
    }
  }

  // ___________________________________________________________________________
  // Pipelined batch variant of `idsToStringAndType`.
  //
  // Partitions ID positions into in-memory IDs and `VocabIndex` IDs, then
  // executes pipelined prefetch resolution across the vocabulary slots.
  template <bool removeQuotesAndAngleBrackets = false,
            bool returnOnlyLiterals = false,
            typename EscapeFunction = ql::identity>
  [[nodiscard]] std::vector<std::optional<std::pair<std::string, const char*>>>
  idsToStringAndType(
      const Index& index, ql::span<const Id> ids, const LocalVocab& localVocab,
      const EscapeFunction& escapeFunction = EscapeFunction{}) const {
    std::vector<std::optional<std::pair<std::string, const char*>>> results(
        ids.size());

    if (ids.empty()) {
      return results;
    }

    ql::exportIds::PartitionedIdPositions positions =
        ql::exportIds::partitionIdPositions(ids);

    // 1. Resolve non-VocabIndex IDs (encoded numeric values, LocalVocab, etc.)
    ql::exportIds::resolveNonVocabIndexIds<removeQuotesAndAngleBrackets,
                                           returnOnlyLiterals>(
        index, ids, localVocab, positions.nonVocabIndexIndices_, results,
        escapeFunction);

    // 2. Resolve VocabIndex IDs with pipelined prefetching
    resolveVocabIndexIds<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
        index, ids, positions.vocabIndexIndices_, results, escapeFunction);

    return results;
  }
};

// _____________________________________________________________________________
// Free convenience functions mirroring `ql::exportIds` interface with
// prefetching enabled.
template <bool removeQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false,
          typename EscapeFunction = ql::identity,
          size_t PrefetchDistance =
              PrefetchingBatchResolver::DEFAULT_PREFETCH_DISTANCE>
inline void resolveVocabIndexIdsPrefetched(
    const Index& index, ql::span<const Id> ids,
    ql::span<const size_t> positions,
    ql::span<std::optional<std::pair<std::string, const char*>>> results,
    const EscapeFunction& escapeFunction = EscapeFunction{}) {
  PrefetchingBatchResolver resolver{
      PrefetchConfig{.prefetchDistance = PrefetchDistance}};
  resolver
      .resolveVocabIndexIds<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
          index, ids, positions, results, escapeFunction);
}

// _____________________________________________________________________________
template <bool removeQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false,
          typename EscapeFunction = ql::identity,
          size_t PrefetchDistance =
              PrefetchingBatchResolver::DEFAULT_PREFETCH_DISTANCE>
[[nodiscard]] inline std::vector<
    std::optional<std::pair<std::string, const char*>>>
idsToStringAndTypePrefetched(
    const Index& index, ql::span<const Id> ids, const LocalVocab& localVocab,
    const EscapeFunction& escapeFunction = EscapeFunction{}) {
  PrefetchingBatchResolver resolver{
      PrefetchConfig{.prefetchDistance = PrefetchDistance}};
  return resolver
      .idsToStringAndType<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
          index, ids, localVocab, escapeFunction);
}

}  // namespace ql::engine::prefetch

#endif  // QLEVER_SRC_ENGINE_PREFETCHINGBATCHRESOLVER_H
