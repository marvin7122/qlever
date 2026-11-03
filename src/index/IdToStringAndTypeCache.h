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
#include <functional>
#include <optional>
#include <string>
#include <utility>

#include "global/Id.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "util/Log.h"
#include "util/LruCache.h"
#include "util/NoCopyNoMove.h"
#include "util/ValueIdentity.h"

namespace ql::exportIds {

// The configuration of an `IdToStringAndTypeCache`, see the runtime parameters
// `select-export-term-cache-*` for the values used by the SELECT export.
struct IdToStringAndTypeCacheConfig {
  // The maximal number of cached terms. 0 disables the cache: every lookup is
  // computed directly.
  size_t capacity_;
  // The hit rate is checked after every `numberOfLookupsPerWindow_` cached
  // lookups. If the hit rate of that window is below `minHitRate_`, the cache
  // is switched off for the rest of the export: on results with mostly
  // distinct terms, the hash lookup, the LRU bookkeeping and the copy of every
  // missed string into the cache cost more than the few hits save. The switch
  // is one-way, because a switched-off cache no longer observes hits. 0 for
  // either value disables the check.
  size_t numberOfLookupsPerWindow_;
  double minHitRate_;
};

// Bounded cache for the SELECT export that maps an `Id` to the result of
// `idToStringAndType<removeQuotesAndAngleBrackets, returnOnlyLiterals>` with
// the given `EscapeFunction`. The cached strings are the formatted (escaped)
// output of exactly this conversion, which is why the conversion is part of
// the type and of the state of the cache: one instance can never return a term
// that was formatted for another output format.
//
// Only `Id`s for which `canBeCachedForWholeExport` holds are cached (this
// excludes `LocalVocabIndex`, see there for the reason).
template <bool removeQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false,
          typename EscapeFunction = ql::identity>
class IdToStringAndTypeCache : public ad_utility::NoCopyNoMove {
 public:
  using Value = std::optional<std::pair<std::string, const char*>>;

  // The number of lookups by how they were answered. Every lookup is counted
  // exactly once.
  struct Statistics {
    uint64_t numberOfHits_ = 0;
    uint64_t numberOfMisses_ = 0;
    // Computed directly because the cache was disabled.
    uint64_t numberOfLookupsWhileDisabled_ = 0;
    // Computed directly because `canBeCachedForWholeExport` was false.
    uint64_t numberOfLocalVocabLookups_ = 0;

    uint64_t numberOfLookups() const {
      return numberOfHits_ + numberOfMisses_ + numberOfLookupsWhileDisabled_ +
             numberOfLocalVocabLookups_;
    }
  };

 private:
  std::reference_wrapper<const Index> index_;
  EscapeFunction escapeFunction_;
  // The configured capacity, only used in the log message.
  size_t capacity_;
  // Empty iff the cache is disabled, either by `capacity_ == 0` or because a
  // window's hit rate fell below `minHitRate_`. In the second case the cached
  // values are destroyed right away, so that the memory is freed for the rest
  // of the export.
  std::optional<ad_utility::util::LRUCache<Id, Value>> cache_;
  size_t numberOfLookupsPerWindow_;
  double minHitRate_;
  // The number of cached lookups that are left in the current window, and the
  // number of hits in it so far.
  size_t numberOfLookupsLeftInWindow_;
  uint64_t numberOfHitsInWindow_ = 0;
  Statistics statistics_;
  // Holds the result of every lookup that does not go through the cache, so
  // that `get` can return a reference in every case. It is overwritten by the
  // next such lookup.
  Value uncached_;

 public:
  IdToStringAndTypeCache(const Index& index,
                         const IdToStringAndTypeCacheConfig& config,
                         EscapeFunction escapeFunction = EscapeFunction{})
      : index_{index},
        escapeFunction_{std::move(escapeFunction)},
        capacity_{config.capacity_},
        numberOfLookupsPerWindow_{config.numberOfLookupsPerWindow_},
        minHitRate_{config.minHitRate_},
        numberOfLookupsLeftInWindow_{config.numberOfLookupsPerWindow_} {
    if (capacity_ > 0) {
      cache_.emplace(capacity_);
    }
  }

  // Log the statistics of an export that used the cache.
  ~IdToStringAndTypeCache() {
    const auto& s = statistics_;
    if (s.numberOfLookups() > 0) {
      AD_LOG_DEBUG << "SELECT export term cache: capacity " << capacity_ << ", "
                   << s.numberOfHits_ << " hits, " << s.numberOfMisses_
                   << " misses, " << s.numberOfLookupsWhileDisabled_
                   << " lookups while disabled, "
                   << s.numberOfLocalVocabLookups_
                   << " local vocab lookups (never cached), "
                   << (isEnabled() ? "enabled" : "disabled") << " at the end"
                   << std::endl;
    }
  }

  // Return `idToStringAndType` for `id`, from the cache if possible. The
  // reference is only valid until the next call: lookups that do not go
  // through the cache share one slot. Every caller in the SELECT export uses
  // the value before the next call. A callback API would avoid this
  // restriction, but the TSV/CSV export has to `co_yield` the value, which a
  // callback cannot do.
  const Value& get(Id id, const LocalVocab& localVocab) {
    if (!canBeCachedForWholeExport(id)) {
      ++statistics_.numberOfLocalVocabLookups_;
      uncached_ = compute(id, localVocab);
      return uncached_;
    }
    if (!isEnabled()) {
      ++statistics_.numberOfLookupsWhileDisabled_;
      uncached_ = compute(id, localVocab);
      return uncached_;
    }
    bool isMiss = false;
    const Value& result =
        cache_->getOrCompute(id, [this, &isMiss, &localVocab](const Id& key) {
          isMiss = true;
          return compute(key, localVocab);
        });
    countLookup(!isMiss);
    if (!isWindowHitRateTooLow()) {
      return result;
    }
    // Switching off destroys `result`, so return it via `uncached_`.
    uncached_ = result;
    cache_.reset();
    return uncached_;
  }

  // True while lookups go through the cache.
  bool isEnabled() const { return cache_.has_value(); }

  const Statistics& statistics() const { return statistics_; }

 private:
  // The uncached conversion.
  Value compute(Id id, const LocalVocab& localVocab) const {
    return idToStringAndType<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
        index_.get(), id, localVocab, escapeFunction_);
  }

  // Count a lookup that went through the cache.
  void countLookup(bool isHit) {
    if (isHit) {
      ++statistics_.numberOfHits_;
      ++numberOfHitsInWindow_;
    } else {
      ++statistics_.numberOfMisses_;
    }
  }

  // True iff the last lookup completed a window of `numberOfLookupsPerWindow_`
  // cached lookups whose hit rate is below `minHitRate_`. A completed window
  // also starts the next one. A countdown instead of a modulo of the total
  // number of lookups avoids an integer division on every lookup.
  bool isWindowHitRateTooLow() {
    if (numberOfLookupsPerWindow_ == 0 || minHitRate_ <= 0.0) {
      return false;
    }
    if (--numberOfLookupsLeftInWindow_ > 0) {
      return false;
    }
    auto numberOfHitsInWindow = numberOfHitsInWindow_;
    numberOfLookupsLeftInWindow_ = numberOfLookupsPerWindow_;
    numberOfHitsInWindow_ = 0;
    return static_cast<double>(numberOfHitsInWindow) <
           minHitRate_ * static_cast<double>(numberOfLookupsPerWindow_);
  }
};

}  // namespace ql::exportIds

#endif  // QLEVER_SRC_INDEX_IDTOSTRINGANDTYPECACHE_H
