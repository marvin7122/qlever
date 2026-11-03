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
#include <list>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "global/Id.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "util/AllocatorWithLimit.h"
#include "util/Log.h"
#include "util/LruCache.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"
#include "util/ValueIdentity.h"

namespace ql::exportIds {

// The configuration of an `IdToStringAndTypeCache`, see the runtime parameters
// `select-export-term-cache-*` for the values used by the SELECT export.
struct IdToStringAndTypeCacheConfig {
  // The maximal memory of the cache, including its bookkeeping. A single term
  // may take at most 1/16 of it, so that one term cannot evict most of the
  // others. A size below 16 times the bookkeeping of one entry (about 3 KB)
  // disables the cache: every lookup is then computed directly.
  ad_utility::MemorySize maxSize_;
  // The hit rate is checked after every `numberOfLookupsPerWindow_` lookups
  // that are hits or misses (see `IdToStringAndTypeCache::Statistics`). If the
  // hit rate of that window is below `minHitRate_`, the cache destroys its
  // entries and computes every later term directly: on results with mostly
  // distinct terms, the hash lookup, the LRU bookkeeping and the copy of every
  // missed string into the cache cost more than the few hits save. This is
  // final for the export, because a switched-off cache no longer observes
  // hits. 0 for either value disables the check.
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
// Only the `Id`s whose conversion reads a vocabulary or the text index are
// cached (see `isWorthCaching`); the values that are formatted from the bits of
// the `Id` alone are computed directly.
//
// The memory of the cache is reserved from the memory budget of the queries
// (the `allocator` passed to the constructor) and returned when entries are
// evicted or the cache is destroyed. A term for which the budget has not
// enough memory left is returned, but not cached; the cache never makes a
// query fail.
template <bool removeQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false,
          typename EscapeFunction = ql::identity>
class IdToStringAndTypeCache : public ad_utility::NoCopyNoMove {
 public:
  using Value = std::optional<std::pair<std::string, const char*>>;

  // The number of lookups by how they were answered. Every lookup is counted
  // in exactly one of these. Only hits and misses count for the hit rate.
  struct Statistics {
    uint64_t numberOfHits_ = 0;
    uint64_t numberOfMisses_ = 0;
    // Computed directly because `canBeCachedForWholeExport` was false.
    uint64_t numberOfLocalVocabLookups_ = 0;
    // Computed directly because the value is formatted from the `Id` bits.
    uint64_t numberOfBitEncodedLookups_ = 0;
    // Computed directly because the cache was disabled.
    uint64_t numberOfLookupsWhileDisabled_ = 0;
    // Not found and not cached, because the term was larger than 1/16 of the
    // maximal size or the memory budget had not enough memory left.
    uint64_t numberOfTermsNotCachedForLackOfMemory_ = 0;

    uint64_t numberOfLookups() const {
      return numberOfHits_ + numberOfMisses_ + numberOfLocalVocabLookups_ +
             numberOfBitEncodedLookups_ + numberOfLookupsWhileDisabled_ +
             numberOfTermsNotCachedForLackOfMemory_;
    }
  };

 private:
  // The map is keyed by the bits of the `Id`. Comparing and hashing an `Id`
  // dereferences the `LocalVocabEntry` of a `LocalVocabIndex`, which a key of
  // bits can never do. With such keys, a cached `LocalVocabIndex` would be
  // found again for a different word at the same address, which is why
  // `get` never caches one.
  using Key = Id::T;
  using Cache = ad_utility::util::LRUCache<Key, Value>;

  // An upper estimate of the memory of one entry besides the heap memory of
  // its string: the slot in the `absl::flat_hash_map` of `LRUCache` (with its
  // control byte, times 16/7 because the table grows at a load factor of 7/8
  // to twice its size) and the node of its `std::list` (two pointers, the key,
  // and 16 bytes of overhead of the memory allocator).
  static constexpr size_t numberOfBytesOfBookkeepingPerEntry =
      (sizeof(std::pair<const Key,
                        std::pair<Value, typename std::list<Key>::iterator>>) +
       1) *
          16 / 7 +
      1 + 2 * sizeof(void*) + sizeof(Key) + 16;
  static constexpr size_t maxFractionOfOneEntry = 16;

  std::reference_wrapper<const Index> index_;
  EscapeFunction escapeFunction_;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  size_t maxNumberOfBytes_;
  // The memory of the entries in the cache, which is reserved from
  // `allocator_`.
  size_t numberOfBytes_ = 0;
  // Empty iff the cache is disabled, either by the configuration or because a
  // window's hit rate fell below `minHitRate_`.
  std::optional<Cache> cache_;
  size_t numberOfLookupsPerWindow_;
  double minHitRate_;
  // The number of hits and misses that are left in the current window, and the
  // number of hits in it so far.
  size_t numberOfLookupsLeftInWindow_;
  uint64_t numberOfHitsInWindow_ = 0;
  Statistics statistics_;
  // Holds the result of every lookup that is not answered from the cache, so
  // that `get` can return a reference in every case. It is overwritten by the
  // next such lookup.
  Value uncached_;

 public:
  IdToStringAndTypeCache(const Index& index,
                         const IdToStringAndTypeCacheConfig& config,
                         ad_utility::AllocatorWithLimit<Id> allocator,
                         EscapeFunction escapeFunction = EscapeFunction{})
      : index_{index},
        escapeFunction_{std::move(escapeFunction)},
        allocator_{std::move(allocator)},
        maxNumberOfBytes_{config.maxSize_.getBytes()},
        numberOfLookupsPerWindow_{config.numberOfLookupsPerWindow_},
        minHitRate_{config.minHitRate_},
        numberOfLookupsLeftInWindow_{config.numberOfLookupsPerWindow_} {
    if (maxNumberOfBytes_ / maxFractionOfOneEntry >=
        numberOfBytesOfBookkeepingPerEntry) {
      // Every entry takes at least `numberOfBytesOfBookkeepingPerEntry`, so
      // the bound on the bytes is reached before the bound on the number of
      // entries (see `makeRoomFor`).
      cache_.emplace(maxNumberOfBytes_ / numberOfBytesOfBookkeepingPerEntry);
    }
  }

  // Return the reserved memory, and log the statistics of an export that used
  // the cache. The log is here because the export functions are generators
  // that can be abandoned at any row (a client that disconnects, an error).
  ~IdToStringAndTypeCache() {
    switchOff();
    const auto& s = statistics_;
    if (s.numberOfLookups() > 0) {
      AD_LOG_DEBUG << "SELECT export term cache: " << s.numberOfHits_
                   << " hits, " << s.numberOfMisses_ << " misses, "
                   << s.numberOfTermsNotCachedForLackOfMemory_
                   << " terms not cached for lack of memory, "
                   << s.numberOfLocalVocabLookups_ << " local vocab lookups, "
                   << s.numberOfBitEncodedLookups_ << " bit-encoded values, "
                   << s.numberOfLookupsWhileDisabled_
                   << " lookups while disabled" << std::endl;
    }
  }

  // Return `idToStringAndType` for `id`, from the cache if possible. The
  // reference is only valid until the next call: lookups that are not
  // answered from the cache share one slot, and an insertion can evict or move
  // a cached value. Every caller in the SELECT export uses the value before
  // the next call. A callback API would avoid this restriction, but the
  // TSV/CSV export has to `co_yield` the value, which a callback cannot do.
  const Value& get(Id id, const LocalVocab& localVocab) {
    if (!canBeCachedForWholeExport(id)) {
      ++statistics_.numberOfLocalVocabLookups_;
      return computeUncached(id, localVocab);
    }
    if (!isWorthCaching(id)) {
      ++statistics_.numberOfBitEncodedLookups_;
      return computeUncached(id, localVocab);
    }
    if (!isEnabled()) {
      ++statistics_.numberOfLookupsWhileDisabled_;
      return computeUncached(id, localVocab);
    }
    Key key = id.getBits();
    if (auto cached = cache_->tryGet(key)) {
      ++statistics_.numberOfHits_;
      ++numberOfHitsInWindow_;
      return endOfWindowLookup(cached.value());
    }
    Value value = compute(id, localVocab);
    size_t numberOfBytes = numberOfBytesOfEntry(value);
    if (!makeRoomFor(numberOfBytes)) {
      ++statistics_.numberOfTermsNotCachedForLackOfMemory_;
      uncached_ = std::move(value);
      return uncached_;
    }
    ++statistics_.numberOfMisses_;
    numberOfBytes_ += numberOfBytes;
    return endOfWindowLookup(cache_->insertAbsent(key, std::move(value)));
  }

  // True while lookups can be answered from the cache.
  bool isEnabled() const { return cache_.has_value(); }

  const Statistics& statistics() const { return statistics_; }

  // The memory of the entries in the cache, including their bookkeeping.
  ad_utility::MemorySize memoryUsed() const {
    return ad_utility::MemorySize::bytes(numberOfBytes_);
  }

  // True iff the conversion of `id` reads a vocabulary or the text index. The
  // other values (`Undefined`, `Bool`, `Int`, `Double`, `Date`, `GeoPoint`,
  // `BlankNodeIndex`) are formatted from the bits of the `Id` without reading
  // any vocabulary, so caching them saves no read; they would only take the
  // place of terms that need one and distort the hit rate.
  static bool isWorthCaching(Id id) {
    using enum Datatype;
    switch (id.getDatatype()) {
      case VocabIndex:
      case SecondaryVocabIndex:
      case EncodedVal:
      case WordVocabIndex:
      case TextRecordIndex:
        return true;
      default:
        return false;
    }
  }

 private:
  // The uncached conversion.
  Value compute(Id id, const LocalVocab& localVocab) const {
    return idToStringAndType<removeQuotesAndAngleBrackets, returnOnlyLiterals>(
        index_.get(), id, localVocab, escapeFunction_);
  }

  const Value& computeUncached(Id id, const LocalVocab& localVocab) {
    uncached_ = compute(id, localVocab);
    return uncached_;
  }

  // The memory of an entry with `value`: the bookkeeping, and the heap memory
  // of the string unless it is stored inside the `std::string` object (small
  // string optimization).
  static size_t numberOfBytesOfEntry(const Value& value) {
    size_t numberOfBytes = numberOfBytesOfBookkeepingPerEntry;
    if (value.has_value()) {
      const std::string& string = value->first;
      const char* object = reinterpret_cast<const char*>(&string);
      std::less<const char*> less;
      bool isStoredInObject = !less(string.data(), object) &&
                              less(string.data(), object + sizeof(string));
      if (!isStoredInObject) {
        numberOfBytes += string.capacity() + 1;
      }
    }
    return numberOfBytes;
  }

  // Evict least recently used entries until an entry of `numberOfBytes` fits
  // into the maximal size, and reserve its memory from `allocator_`. Return
  // false if the entry is larger than 1/16 of the maximal size or the memory
  // budget has not enough memory left; nothing is reserved then.
  bool makeRoomFor(size_t numberOfBytes) {
    if (numberOfBytes > maxNumberOfBytes_ / maxFractionOfOneEntry) {
      return false;
    }
    while (numberOfBytes_ + numberOfBytes > maxNumberOfBytes_) {
      auto evicted = cache_->popLeastRecentlyUsed();
      AD_CORRECTNESS_CHECK(evicted.has_value());
      release(numberOfBytesOfEntry(evicted.value()));
    }
    // The bound on the bytes implies the bound on the number of entries (see
    // the constructor), so `LRUCache` never evicts an entry on its own, which
    // would leave its memory reserved.
    AD_CORRECTNESS_CHECK(cache_->size() < cache_->capacity());
    auto memorySize = ad_utility::MemorySize::bytes(numberOfBytes);
    if (allocator_.amountMemoryLeft() < memorySize) {
      return false;
    }
    try {
      // Lowering the limit of the shared budget reserves the memory.
      allocator_.decreaseMemoryLimit(memorySize);
    } catch (const std::runtime_error&) {
      // Another query took the memory since the check above.
      return false;
    }
    return true;
  }

  // Return the reservation of an entry of `numberOfBytes` to `allocator_`.
  void release(size_t numberOfBytes) {
    AD_CORRECTNESS_CHECK(numberOfBytes_ >= numberOfBytes);
    numberOfBytes_ -= numberOfBytes;
    allocator_.increaseMemoryLimit(
        ad_utility::MemorySize::bytes(numberOfBytes));
  }

  // Destroy all entries and return their memory.
  void switchOff() {
    cache_.reset();
    allocator_.increaseMemoryLimit(
        ad_utility::MemorySize::bytes(numberOfBytes_));
    numberOfBytes_ = 0;
  }

  // Complete a hit or a miss with the `result` that is stored in the cache:
  // check the hit rate if the lookup completed a window, and switch the cache
  // off if it is too low. Switching off destroys `result`, so it is then
  // returned via `uncached_`.
  const Value& endOfWindowLookup(const Value& result) {
    if (!isWindowHitRateTooLow()) {
      return result;
    }
    uncached_ = result;
    switchOff();
    return uncached_;
  }

  // True iff the last lookup completed a window of `numberOfLookupsPerWindow_`
  // hits and misses whose hit rate is below `minHitRate_`. A completed window
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
