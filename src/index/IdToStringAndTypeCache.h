// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_IDTOSTRINGANDTYPECACHE_H
#define QLEVER_SRC_INDEX_IDTOSTRINGANDTYPECACHE_H

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "global/Id.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "util/Log.h"
#include "util/LruCacheWithStatistics.h"
#include "util/ValueIdentity.h"

namespace ql::exportIds {

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
  // The configured capacity, only used in the log message.
  size_t capacity_;
  // Empty iff the cache is disabled, either by `capacity_ == 0` or because a
  // window's hit rate fell below `minHitRate_`. In the second case the cached
  // values are destroyed right away, so that the memory is freed for the rest
  // of the export.
  std::optional<ad_utility::util::LRUCacheWithStatistics<Id, Value>> cache_;
  // The statistics of `cache_` at the moment it was switched off.
  ad_utility::util::LRUCacheStats statsWhenSwitchedOff_;
  size_t windowSize_;
  double minHitRate_;
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
      : capacity_{config.capacity_},
        windowSize_{config.windowSize_},
        minHitRate_{config.minHitRate_} {
    if (capacity_ > 0) {
      cache_.emplace(capacity_);
    }
  }
  explicit IdToStringAndTypeCache(size_t capacity)
      : IdToStringAndTypeCache{Config{capacity, 0, 0.0}} {}

  // Log the statistics of an export that used the cache.
  ~IdToStringAndTypeCache() {
    if (stats().totalLookups() + bypassed_ > 0) {
      AD_LOG_DEBUG << "SELECT export term cache: capacity " << capacity_ << ", "
                   << stats().hits_ << " hits, " << stats().misses_
                   << " misses, " << bypassed_ << " bypassed, "
                   << (enabled() ? "enabled" : "disabled") << " at the end"
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
    if (!enabled()) {
      ++bypassed_;
      uncached_ = compute();
      return uncached_;
    }
    const Value& result =
        cache_->getOrCompute(id, [&compute](const Id&) { return compute(); });
    if (!isWindowHitRateTooLow()) {
      return result;
    }
    // Switching off destroys `result`, so return it via `uncached_`.
    uncached_ = result;
    statsWhenSwitchedOff_ = cache_->stats();
    cache_.reset();
    return uncached_;
  }

  // True while lookups go through the cache.
  bool enabled() const { return cache_.has_value(); }

  // Lookups computed directly because the cache was disabled.
  uint64_t bypassed() const { return bypassed_; }

  // Return the numbers of hits and misses of the lookups that went through the
  // cache (including those before it was switched off).
  const ad_utility::util::LRUCacheStats& stats() const {
    return cache_.has_value() ? cache_->stats() : statsWhenSwitchedOff_;
  }

 private:
  // True iff the last lookup completed a window of `windowSize_` cached
  // lookups whose hit rate is below `minHitRate_`. A completed window also
  // starts the next one.
  bool isWindowHitRateTooLow() {
    if (windowSize_ == 0 || minHitRate_ <= 0.0) {
      return false;
    }
    const auto& s = cache_->stats();
    if (s.totalLookups() % windowSize_ != 0) {
      return false;
    }
    auto windowHits = s.hits_ - hitsAtWindowStart_;
    hitsAtWindowStart_ = s.hits_;
    return static_cast<double>(windowHits) <
           minHitRate_ * static_cast<double>(windowSize_);
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

}  // namespace ql::exportIds

#endif  // QLEVER_SRC_INDEX_IDTOSTRINGANDTYPECACHE_H
