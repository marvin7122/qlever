// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_EXPORTIDS_H
#define QLEVER_SRC_INDEX_EXPORTIDS_H

#include <array>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/LocalVocab.h"
#include "parser/LiteralOrIri.h"
#include "util/Algorithm.h"
#include "util/CompilerExtensions.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/LruCacheWithStatistics.h"
#include "util/ValueIdentity.h"

namespace ql::exportIds {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
using LiteralOrIriView = ad_utility::triple_component::LiteralOrIriView;
using Iri = ad_utility::triple_component::Iri;
using IriView = ad_utility::triple_component::IriView;
using Literal = ad_utility::triple_component::Literal;

// Convert the `id` to a `Literal`. Datatypes are always stripped, so for
// literals (this includes IDs that directly store their value, like Doubles)
// the datatype is always empty. If 'onlyReturnLiteralsWithXsdString' is
// false, IRIs are converted to literals without a datatype, which is
// equivalent to the behavior of the SPARQL STR(...) function. If
// 'onlyReturnLiteralsWithXsdString' is true, all IRIs and literals with
// non-`xsd:string` datatypes (including encoded IDs) return `std::nullopt`.
// These semantics are useful for the string expressions in
// StringExpressions.cpp.
//
// All datatypes are handled, in one of the following ways. The words of the
// vocabularies (`VocabIndex`, `LocalVocabIndex`, `SecondaryVocabIndex`) and the
// encoded IRIs (`EncodedVal`) are resolved via `getLiteralOrIriFromVocabIndex`
// and `encodedIdToLiteralOrIri`, respectively, and then processed as described
// above. The words of the text index (`WordVocabIndex`, `TextRecordIndex`)
// are always plain literals. All remaining datatypes are handled by
// `idToLiteralForEncodedValue`, which turns the value that the `Id` stores
// into a plain literal (`Bool`, `Int`, `Double`, `Date`, `GeoPoint`), returns
// the label of the blank node (`BlankNodeIndex`), or returns `std::nullopt`
// (`Undefined`).
std::optional<Literal> idToLiteral(
    const IndexImpl& index, Id id, const LocalVocab& localVocab,
    bool onlyReturnLiteralsWithXsdString = false);

// Same as the previous function, but only handles the datatypes for which the
// value is encoded directly in the ID. For other datatypes an exception is
// thrown.
// If `onlyReturnLiteralsWithXsdString` is `true`, returns `std::nullopt`.
// If `onlyReturnLiteralsWithXsdString` is `false`, removes datatypes from
// literals (e.g. the integer `42` is converted to the plain literal `"42"`).
std::optional<Literal> idToLiteralForEncodedValue(
    Id id, bool onlyReturnLiteralsWithXsdString = false);

// A helper function for the `idToLiteral` function. Checks and processes
// a LiteralOrIri based on the given parameters.
std::optional<Literal> handleIriOrLiteral(LiteralOrIri word,
                                          bool onlyReturnLiteralsWithXsdString);

// The function resolves a given `ValueId` to a `LiteralOrIri` object. Unlike
// `idToLiteral` no further processing is applied to the string content.
std::optional<LiteralOrIri> idToLiteralOrIri(const IndexImpl& index, Id id,
                                             const LocalVocab& localVocab,
                                             bool skipEncodedValues = false);

// Helper for the `idToLiteralOrIri` function: Retrieves a string literal from
// a value encoded in the given ValueId.
std::optional<LiteralOrIri> idToLiteralOrIriForEncodedValue(Id id);

// Helper for the `idToLiteralOrIri` function: Retrieves a string literal for
// a word in the vocabulary.
LiteralOrIri getLiteralOrIriFromWordVocabIndex(const IndexImpl& index, Id id);

// Helper for the `idToLiteralOrIri` function: Retrieves a string literal for
// a word in the text index.
std::optional<LiteralOrIri> getLiteralOrIriFromTextRecordIndex(
    const IndexImpl& index, Id id);

// Helper for the `idToLiteral` function: get only literals from the
// `LiteralOrIri` object.
std::optional<Literal> getLiteralOrNullopt(
    std::optional<LiteralOrIri> litOrIri);

// Replaces the first character '<' and the last character '>' with double
// quotes '"' to convert an IRI to a Literal, ensuring only the angle brackets
// are replaced.
std::string replaceAnglesByQuotes(std::string iriString);

// Return the blank-node string representation if `iri` is a blank-node IRI,
// otherwise std::nullopt.
template <typename IriType>
std::optional<std::string_view> blankNodeIriToString(
    const IriType& iri AD_LIFETIMEBOUND);

// Acts as a helper to retrieve a LiteralOrIri object from an Id, where the Id
// is of type `VocabIndex`, `LocalVocabIndex`, `SecondaryVocabIndex`, or
// `EncodedVal`. This function should only be called with suitable `Datatype`
// Ids, otherwise `AD_FAIL()` is called. Note that an `Id` of type
// `SecondaryVocabIndex` requires the `index` to have a secondary vocabulary
// (see `IndexImpl::secondaryVocab`), which is checked.
LiteralOrIri getLiteralOrIriFromVocabIndex(const IndexImpl& index, Id id,
                                           const LocalVocab& localVocab);

// Convert an ID whose value is encoded in the ID bits to (string, XSD-type).
// Returns std::nullopt for Undefined. Throws for non-encoded-value datatypes.
std::optional<std::pair<std::string, const char*>>
idToStringAndTypeForEncodedValue(Id id);

// Convert an `EncodedVal` ID to a `LiteralOrIri` by looking up the encoded
// IRI via the `EncodedIriManager` in the index.
LiteralOrIri encodedIdToLiteralOrIri(Id id, const IndexImpl& index);

// Format a `LiteralOrIri` as a (string, XSD-type) pair applying the template
// options and `escapeFunction`. Return `std::nullopt` when `returnOnlyLiterals`
// is true and `word` is not a literal.
CPP_template(bool removeQuotesAndAngleBrackets = false,
             bool returnOnlyLiterals = false,
             typename LiteralOrIriType = LiteralOrIri,
             typename EscapeFunction = ql::identity)(
    requires ad_utility::SameAsAny<LiteralOrIriType, LiteralOrIri,
                                   LiteralOrIriView>) std::
    optional<std::pair<std::string, const char*>> literalOrIriToStringAndType(
        const LiteralOrIriType& word,
        EscapeFunction&& escapeFunction = EscapeFunction{}) {
  if constexpr (returnOnlyLiterals) {
    if (!word.isLiteral()) {
      return std::nullopt;
    }
  }
  if (word.isIri()) {
    if (auto blankNodeString = blankNodeIriToString(word.getIri())) {
      return std::pair{std::string{blankNodeString.value()}, nullptr};
    }
  }
  if constexpr (removeQuotesAndAngleBrackets) {
    // TODO<joka921> Can we get rid of the string copying here?
    return std::pair{
        escapeFunction(std::string{asStringViewUnsafe(word.getContent())}),
        nullptr};
  }
  // TODO<ms2144>: we unconditionally always materialize a string here, which
  // is wasteful and should be mitigated in the future.
  return std::pair{escapeFunction(std::string{word.toStringRepresentation()}),
                   nullptr};
}

// Convert the `id` to a human-readable string. The `index` is used to resolve
// the `Id`s that point into one of its data structures (`VocabIndex`,
// `SecondaryVocabIndex`, `WordVocabIndex`, `TextRecordIndex`, and
// `EncodedVal`). The `localVocab` is used to resolve `Id`s with datatype
// `LocalVocabIndex`. The `escapeFunction` is applied to the resulting string if
// it is not of a numeric type.
//
// Return value: If the `Id` encodes a numeric value (integer, double, etc.)
// then the `string` (first element of the pair) will be the number as a
// string without quotation marks, and the second element of the pair will
// contain the corresponding XSD-datatype as an URI. For all other values and
// datatypes, the second element of the pair will be empty and the first
// element will have the format `"stringContent"^^datatypeUri`. If the `id`
// holds the `Undefined` value, then `std::nullopt` is returned.
template <bool removeQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false,
          typename EscapeFunction = ql::identity>
std::optional<std::pair<std::string, const char*>> idToStringAndType(
    const Index& index, Id id, const LocalVocab& localVocab,
    EscapeFunction&& escapeFunction = EscapeFunction{}) {
  using enum Datatype;
  auto datatype = id.getDatatype();
  if constexpr (returnOnlyLiterals) {
    // In this mode, restrict the result to the words of the vocabularies that
    // store their words as strings. The words of the text index and the values
    // that are encoded in the `Id` are turned into a string directly below and
    // would bypass the `isLiteral()` check in `literalOrIriToStringAndType`,
    // so they have to be discarded here. An encoded IRI (`EncodedVal`) would
    // pass through that check, but can never be a literal, so discarding it
    // here as well is equivalent and cheaper.
    static constexpr std::array stringVocabDatatypes{
        VocabIndex, LocalVocabIndex, SecondaryVocabIndex};
    if (!ad_utility::contains(stringVocabDatatypes, datatype)) {
      return std::nullopt;
    }
  }

  auto formatLiteralOrIri = [&escapeFunction](const auto& word) {
    return literalOrIriToStringAndType<removeQuotesAndAngleBrackets,
                                       returnOnlyLiterals>(word,
                                                           escapeFunction);
  };

  switch (id.getDatatype()) {
    case WordVocabIndex: {
      std::string_view entity = index.indexToString(id.getWordVocabIndex());
      return std::pair{escapeFunction(std::string{entity}), nullptr};
    }
    case VocabIndex:
    case LocalVocabIndex:
    case SecondaryVocabIndex:
      return formatLiteralOrIri(
          getLiteralOrIriFromVocabIndex(index.getImpl(), id, localVocab));
    case EncodedVal:
      return formatLiteralOrIri(encodedIdToLiteralOrIri(id, index.getImpl()));
    case TextRecordIndex:
      return std::pair{
          escapeFunction(index.getTextExcerpt(id.getTextRecordIndex())),
          nullptr};
    default:
      return idToStringAndTypeForEncodedValue(id);
  }
}

// Bounded cache for the SELECT export path that maps an `Id` to the result of
// `idToStringAndType`. The cached strings are post-escaping, so one instance
// serves a single call site (fixed template arguments and escape function).
//
// `Id`s of type `LocalVocabIndex` are never cached: they point into a
// `LocalVocab` that can be destroyed while the export is still running (lazy
// results free each block's vocabulary after it is exported), after which the
// same address, and hence the same `Id`, can denote a different word. All
// other `Id`s resolve identically for the whole export (the on-disk
// vocabulary is static, the other datatypes are encoded in the `Id` bits).
class IdToStringAndTypeCache {
 public:
  using Value = std::optional<std::pair<std::string, const char*>>;

  // Default capacity: one cache per export call site shared by all columns.
  // Bounds memory on distinct-heavy results while keeping frequent terms
  // resident.
  static constexpr size_t DEFAULT_CAPACITY = 1 << 16;
  // Default admission window and threshold, see `Config`.
  static constexpr size_t DEFAULT_WINDOW_SIZE = 1 << 13;
  static constexpr double DEFAULT_MIN_HIT_RATE = 0.25;

  // `capacity_ == 0` disables the cache: every lookup is computed directly.
  // Otherwise the hit rate is checked after every `windowSize_` cached
  // lookups. If the hit rate of that window is below `minHitRate_`, the cache
  // is switched off for the rest of the export: on results with mostly
  // distinct terms, the hash lookup, the LRU bookkeeping and the copy of every
  // missed string into the cache cost more than the few hits save.
  // `windowSize_ == 0` or `minHitRate_ <= 0` disables this check.
  struct Config {
    size_t capacity_ = DEFAULT_CAPACITY;
    size_t windowSize_ = DEFAULT_WINDOW_SIZE;
    double minHitRate_ = DEFAULT_MIN_HIT_RATE;
  };

 private:
  // Empty iff the cache is disabled by `capacity_ == 0`.
  std::optional<ad_utility::util::LRUCacheWithStatistics<Id, Value>> cache_;
  size_t windowSize_;
  double minHitRate_;
  // False once a window's hit rate fell below `minHitRate_`.
  bool enabled_;
  // Number of hits at the start of the current window.
  uint64_t hitsAtWindowStart_ = 0;
  // Lookups computed directly because the cache is disabled (excludes the
  // `LocalVocabIndex` `Id`s, which are never cached).
  uint64_t bypassed_ = 0;
  // Holds the result of every lookup that does not go through the cache, so
  // that `cachedIdToStringAndType` can return a reference in every case. It is
  // overwritten by the next such lookup.
  Value uncached_;

 public:
  explicit IdToStringAndTypeCache(const Config& config)
      : windowSize_{config.windowSize_},
        minHitRate_{config.minHitRate_},
        enabled_{config.capacity_ > 0} {
    if (enabled_) {
      cache_.emplace(config.capacity_);
    }
  }
  explicit IdToStringAndTypeCache(size_t capacity)
      : IdToStringAndTypeCache{Config{capacity, 0, 0.0}} {}

  // Log the statistics of an export that used the cache.
  ~IdToStringAndTypeCache() {
    if (stats().totalLookups() + bypassed_ > 0) {
      AD_LOG_INFO << "SELECT export term cache: capacity "
                  << (cache_.has_value() ? cache_->capacity() : 0) << ", "
                  << stats().hits_ << " hits, " << stats().misses_
                  << " misses, " << bypassed_ << " bypassed, "
                  << (enabled_ ? "enabled" : "disabled") << " at the end"
                  << std::endl;
    }
  }

  IdToStringAndTypeCache(const IdToStringAndTypeCache&) = delete;
  IdToStringAndTypeCache& operator=(const IdToStringAndTypeCache&) = delete;

  // Return the cached value for `id`, computing it with `compute()` on a miss.
  // `LocalVocabIndex` `Id`s bypass the cache (see above), and so does every
  // `Id` once the cache is disabled. The reference is valid until the next
  // call.
  template <typename Compute>
  const Value& getOrCompute(Id id, const Compute& compute) {
    if (id.getDatatype() == Datatype::LocalVocabIndex) {
      uncached_ = compute();
      return uncached_;
    }
    if (!enabled_) {
      ++bypassed_;
      uncached_ = compute();
      return uncached_;
    }
    const Value& result =
        cache_->getOrCompute(id, [&compute](const Id&) { return compute(); });
    // Only the admission flag changes below, the cached entry and therefore
    // `result` stay valid.
    checkWindow();
    return result;
  }

  // True while lookups go through the cache.
  bool enabled() const { return enabled_; }

  // Lookups computed directly because the cache was disabled.
  uint64_t bypassed() const { return bypassed_; }

  const ad_utility::util::LRUCacheStats& stats() const {
    static constexpr ad_utility::util::LRUCacheStats noStats{};
    return cache_.has_value() ? cache_->stats() : noStats;
  }

 private:
  // At the end of each window of `windowSize_` cached lookups, disable the
  // cache if the window's hit rate is below `minHitRate_`.
  void checkWindow() {
    if (windowSize_ == 0 || minHitRate_ <= 0.0) {
      return;
    }
    const auto& s = cache_->stats();
    if (s.totalLookups() % windowSize_ != 0) {
      return;
    }
    auto windowHits = s.hits_ - hitsAtWindowStart_;
    hitsAtWindowStart_ = s.hits_;
    if (static_cast<double>(windowHits) <
        minHitRate_ * static_cast<double>(windowSize_)) {
      enabled_ = false;
    }
  }
};

// Cached variant of `idToStringAndType` for the SELECT export path. Returns a
// reference that is valid until the next lookup in `cache`.
template <bool removeQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false, typename EscapeFunction>
const IdToStringAndTypeCache::Value& cachedIdToStringAndType(
    IdToStringAndTypeCache& cache, const Index& index, Id id,
    const LocalVocab& localVocab, const EscapeFunction& escapeFunction) {
  return cache.getOrCompute(id, [&]() {
    return idToStringAndType<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
        index, id, localVocab, escapeFunction);
  });
}

// Overload without an escape function (identity escaping). It forwards to the
// function above with a persistent identity instance: a default-constructed
// temporary would bind to the `escapeFunction` const reference while the
// function returns a reference into the cache, which GCC flags as
// `-Wdangling-reference` under `-Werror`.
template <bool removeQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false>
const IdToStringAndTypeCache::Value& cachedIdToStringAndType(
    IdToStringAndTypeCache& cache, const Index& index, Id id,
    const LocalVocab& localVocab) {
  static constexpr ql::identity noEscape{};
  return cachedIdToStringAndType<removeQuotesAndAngleBrackets,
                                 returnOnlyLiterals>(cache, index, id,
                                                     localVocab, noEscape);
}

// Positions (indices into the `ids` span) split by datatype: `VocabIndex` ids
// (`vocabIndexIndices_`) need an on-disk vocabulary lookup and are batched
// together: all others (`nonVocabIndexIndices_`) are resolved from the id bits
// or `LocalVocab`. Each stored position is also the index of the `results` slot
// to scatter that id's resolved value back into.
struct PartitionedIdPositions {
  std::vector<size_t> vocabIndexIndices_;
  std::vector<size_t> nonVocabIndexIndices_;
};

// Partition the positions `0 ... ids.size()-1` by whether `ids[i]` is a
// `VocabIndex`.
PartitionedIdPositions partitionIdPositions(ql::span<const Id> ids);

// Resolve the IDs at `positions` (all non-`VocabIndex`) immediately via
// in-memory `idToStringAndType`, writing each result into its slot in
// `results`. These values are either encoded in the id bits or stored in the
// in-memory `LocalVocab`.
template <bool removeQuotesAndAngleBrackets, bool returnOnlyLiterals,
          typename EscapeFunction>
void resolveNonVocabIndexIds(
    const Index& index, ql::span<const Id> ids, const LocalVocab& localVocab,
    ql::span<const size_t> positions,
    ql::span<std::optional<std::pair<std::string, const char*>>> results,
    const EscapeFunction& escapeFunction) {
  AD_EXPENSIVE_CHECK(ql::ranges::all_of(positions, [&ids](size_t i) {
    return ids[i].getDatatype() != Datatype::VocabIndex;
  }));
  ql::ranges::for_each(positions, [&](size_t i) {
    results[i] =
        idToStringAndType<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
            index, ids[i], localVocab, escapeFunction);
  });
}

// Resolve the `VocabIndex` IDs at `positions` in a single batched vocabulary
// lookup, writing each result into its slot in `results`.
template <bool removeQuotesAndAngleBrackets, bool returnOnlyLiterals,
          typename EscapeFunction>
void resolveVocabIndexIds(
    const Index& index, ql::span<const Id> ids,
    ql::span<const size_t> positions,
    ql::span<std::optional<std::pair<std::string, const char*>>> results,
    const EscapeFunction& escapeFunction) {
  if (positions.empty()) {
    return;
  }

  AD_EXPENSIVE_CHECK(ql::ranges::all_of(positions, [&ids](size_t i) {
    return ids[i].getDatatype() == Datatype::VocabIndex;
  }));

  // NOTE: The batch is deliberately not sorted by vocabulary position: the
  // io_uring backend reorders the reads anyway, and only the synchronous
  // fallback could profit from sequential file access.
  auto rawIndices = ::ranges::to_vector(
      positions | ql::views::transform([&ids](size_t i) {
        return static_cast<size_t>(ids[i].getVocabIndex().get());
      }));
  auto vocabStrings = index.getImpl().getVocab().lookupBatch(rawIndices);

  // `vocabStrings` is in the same order as `positions`, so zip scatters each
  // looked-up string back to the position it came from.
  for (auto&& [sv, i] : ::ranges::views::zip(*vocabStrings, positions)) {
    results[i] = literalOrIriToStringAndType<removeQuotesAndAngleBrackets,
                                             returnOnlyLiterals>(
        LiteralOrIriView::fromStringRepresentation(sv), escapeFunction);
  }
}

// Batch variant of `idToStringAndType`. We cannot assume that the `VocabIndex`
// IDs form a single contiguous block: even when the `ids` are sorted, IDs of
// datatype `LocalVocabIndex` might be interspersed between the `VocabIndex`
// IDs. We therefore check each ID's datatype individually to partition the
// positions.
template <bool removeQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false,
          typename EscapeFunction = ql::identity>
std::vector<std::optional<std::pair<std::string, const char*>>>
idsToStringAndType(const Index& index, ql::span<const Id> ids,
                   const LocalVocab& localVocab,
                   const EscapeFunction& escapeFunction = EscapeFunction{}) {
  std::vector<std::optional<std::pair<std::string, const char*>>> results(
      ids.size());

  PartitionedIdPositions positions = partitionIdPositions(ids);

  resolveNonVocabIndexIds<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
      index, ids, localVocab, positions.nonVocabIndexIndices_, results,
      escapeFunction);
  resolveVocabIndexIds<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
      index, ids, positions.vocabIndexIndices_, results, escapeFunction);

  return results;
}

}  // namespace ql::exportIds

#endif  // QLEVER_SRC_INDEX_EXPORTIDS_H
