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

#include <gtest/gtest_prod.h>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <utility>

#include "global/Id.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "util/AllocatorWithLimit.h"
#include "util/ExceptionHandling.h"
#include "util/Log.h"
#include "util/LruCache.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"
#include "util/ValueIdentity.h"

namespace ql::exportIds {

// The configuration of an `IdToStringAndTypeCache`, see the runtime parameters
// `select-export-term-cache-*` for the values used by the SELECT export.
struct IdToStringAndTypeCacheConfig {
  // The maximal memory of the cache: its entries (strings and list nodes) and
  // its hash table, including the old table while the table grows. A single
  // entry may take at most 1/16 of it, so that one term cannot evict most of
  // the others. A size that is too small for 16 small entries with their share
  // of the hash table (about 3 KB) disables the cache: every lookup is then
  // computed directly.
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

// The conversion that an `IdToStringAndTypeCache` applies: the template
// arguments of `idToStringAndType` and the type of its escape function. They
// form one type so that a new conversion option only changes this type, not
// every instantiation of the cache.
template <bool removeQuotesAndAngleBrackets_ = false,
          bool returnOnlyLiterals_ = false,
          typename EscapeFunction_ = ql::identity>
struct IdToStringAndTypeConversion {
  static constexpr bool removeQuotesAndAngleBrackets =
      removeQuotesAndAngleBrackets_;
  static constexpr bool returnOnlyLiterals = returnOnlyLiterals_;
  using EscapeFunction = EscapeFunction_;
};

// Bounded cache for the SELECT export that maps an `Id` to the result of
// `idToStringAndType` with the template arguments and the escape function of
// `Conversion` (an `IdToStringAndTypeConversion`). The cached strings are the
// formatted (escaped) output of exactly this conversion, which is why the
// conversion is part of the type and of the state of the cache: one instance
// can never return a term that was formatted for another output format.
//
// Only the `Id`s whose conversion reads a vocabulary or the text index are
// cached (see `isWorthCaching`); the values that are formatted from the bits of
// the `Id` alone are computed directly.
//
// The memory of the cache is reserved from the memory budget of the queries
// (the `allocator` passed to the constructor) and returned when entries are
// evicted, when the hash table shrinks, and when the cache is switched off or
// destroyed. While the hash table grows, its old and new table coexist; this
// peak is reserved and within the maximal size. While it shrinks, the new,
// smaller table briefly coexists with the old one without being reserved. A
// lookup never fails for lack of memory: a term that does not fit into the
// budget is returned, but not cached. While the memory is reserved, it is not
// available to other allocations from the same budget.
template <typename Conversion = IdToStringAndTypeConversion<>>
class IdToStringAndTypeCache : public ad_utility::NoCopyNoMove {
 public:
  using EscapeFunction = typename Conversion::EscapeFunction;
  using Value = std::optional<std::pair<std::string, const char*>>;

  // The number of lookups by how they were answered. Every lookup is counted
  // in exactly one of these. Only hits and misses count for the hit rate.
  // (`LRUCacheWithStatistics` counts only hits and misses.)
  struct Statistics {
    uint64_t numberOfHits_ = 0;
    uint64_t numberOfMisses_ = 0;
    // Computed directly because `canBeCachedForWholeExport` was false.
    uint64_t numberOfLocalVocabLookups_ = 0;
    // Computed directly because the value is formatted from the `Id` bits.
    uint64_t numberOfBitEncodedLookups_ = 0;
    // Computed directly because the cache was disabled.
    uint64_t numberOfLookupsWhileDisabled_ = 0;
    // Not found and not cached, because the entry was larger than 1/16 of the
    // maximal size or the memory budget had not enough memory left. Like the
    // other fields, this counts lookups: a term that is looked up twice counts
    // twice.
    uint64_t numberOfLookupsNotCachedForLackOfMemory_ = 0;

    uint64_t numberOfLookups() const noexcept {
      return numberOfHits_ + numberOfMisses_ + numberOfLocalVocabLookups_ +
             numberOfBitEncodedLookups_ + numberOfLookupsWhileDisabled_ +
             numberOfLookupsNotCachedForLackOfMemory_;
    }
  };

 private:
  // The map is keyed by the bits of the `Id`. Comparing and hashing an `Id`
  // dereferences the `LocalVocabEntry` of a `LocalVocabIndex`, which a key of
  // bits can never do. With such keys, a cached `LocalVocabIndex` would be
  // found again for a different word at the same address, which is why
  // `isWorthCaching` excludes it.
  using Key = Id::T;
  using Cache = ad_utility::util::LRUCache<Key, Value>;

  // An upper estimate of the memory of a node of the `std::list` of
  // `LRUCache`: two pointers, the key, and 16 bytes of overhead of the memory
  // allocator.
  static constexpr size_t numberOfBytesPerListNode =
      2 * sizeof(void*) + sizeof(Key) + 16;

 public:
  // The smallest maximal size that enables the cache: 16 entries without heap
  // memory, each with a list node and its share of a hash table that has just
  // grown (at most 16/7 slots per entry, see `LRUCache::hashTableCapacity`).
  static constexpr size_t minNumberOfBytesToEnable =
      16 * (numberOfBytesPerListNode +
            Cache::numberOfBytesPerHashTableSlot * 16 / 7 + 1);

 private:
  std::reference_wrapper<const Index> index_;
  EscapeFunction escapeFunction_;
  ad_utility::AllocatorWithLimit<Id> allocator_;
  ad_utility::MemorySize maxMemory_;
  ad_utility::MemorySize maxMemoryOfOneEntry_;
  // The memory of the entries (list nodes and heap memory of the strings) and
  // of the hash table, both reserved from `allocator_`.
  ad_utility::MemorySize memoryOfEntries_;
  ad_utility::MemorySize memoryOfHashTable_;
  // Empty iff the cache is disabled, either by the configuration or because a
  // window's hit rate fell below `minHitRate_`.
  std::optional<Cache> cache_;
  size_t numberOfLookupsPerWindow_;
  double minHitRate_;
  // The number of hits and misses that are left in the current window, and the
  // number of hits in it so far. Both stay unchanged if the hit rate is not
  // checked (see `checksHitRate`).
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
        maxMemory_{config.maxSize_},
        // `MemorySize::operator/` rounds, so divide the bytes to round down.
        maxMemoryOfOneEntry_{
            ad_utility::MemorySize::bytes(maxMemory_.getBytes() / 16)},
        numberOfLookupsPerWindow_{config.numberOfLookupsPerWindow_},
        minHitRate_{config.minHitRate_},
        numberOfLookupsLeftInWindow_{config.numberOfLookupsPerWindow_} {
    if (maxMemory_.getBytes() >= minNumberOfBytesToEnable) {
      // Every entry takes at least `numberOfBytesPerListNode`, so the bound on
      // the bytes is reached before this bound on the number of entries, and
      // `LRUCache` never evicts an entry on its own (which would leave its
      // memory reserved).
      cache_.emplace(maxMemory_.getBytes() / numberOfBytesPerListNode + 1);
    }
  }

  // Return the reserved memory, and log the statistics of an export that used
  // the cache. The log is here because the export functions are generators
  // that can be abandoned at any row (a client that disconnects, an error).
  // Neither step is expected to throw; if one does, the reservation could not
  // be returned, so the program terminates.
  ~IdToStringAndTypeCache() {
    ad_utility::terminateIfThrows(
        [this] {
          switchOff();
          const auto& s = statistics_;
          if (s.numberOfHits_ + s.numberOfMisses_ +
                  s.numberOfLookupsNotCachedForLackOfMemory_ >
              0) {
            AD_LOG_DEBUG << "SELECT export term cache: " << s.numberOfHits_
                         << " hits, " << s.numberOfMisses_ << " misses, "
                         << s.numberOfLookupsNotCachedForLackOfMemory_
                         << " lookups not cached for lack of memory, "
                         << s.numberOfLocalVocabLookups_
                         << " local vocab lookups, "
                         << s.numberOfBitEncodedLookups_
                         << " bit-encoded values, "
                         << s.numberOfLookupsWhileDisabled_
                         << " lookups while disabled" << std::endl;
          }
        },
        "the destructor of `IdToStringAndTypeCache`");
  }

  // Return `idToStringAndType` for `id`, from the cache if possible. The
  // reference is only valid until the next call: a lookup that is not
  // answered from the cache returns the member `uncached_`, which the next
  // such lookup overwrites; a later miss can evict the cached value
  // (`popLeastRecentlyUsed`) or move it when `insertAbsent` grows the hash
  // table; and a switch-off destroys all cached values. Every caller in the
  // SELECT export uses the value before the next call.
  const Value& get(Id id, const LocalVocab& localVocab) {
    if (!isWorthCaching(id)) {
      if (canBeCachedForWholeExport(id)) {
        ++statistics_.numberOfBitEncodedLookups_;
      } else {
        ++statistics_.numberOfLocalVocabLookups_;
      }
      return computeUncached(id, localVocab);
    }
    if (!isEnabled()) {
      ++statistics_.numberOfLookupsWhileDisabled_;
      return computeUncached(id, localVocab);
    }
    Key key = id.getBits();
    if (auto cached = cache_->tryGet(key)) {
      ++statistics_.numberOfHits_;
      if (checksHitRate()) {
        ++numberOfHitsInWindow_;
      }
      return endOfWindowLookup(cached.value());
    }
    Value value = compute(id, localVocab);
    if (!reserveAndMakeRoomForEntry(memoryOfEntry(value))) {
      ++statistics_.numberOfLookupsNotCachedForLackOfMemory_;
      uncached_ = std::move(value);
      return uncached_;
    }
    ++statistics_.numberOfMisses_;
    const Value& result = cache_->insertAbsent(key, std::move(value));
    // Return the reservation of the old table, if the table has grown, and
    // of the growth that did not happen otherwise.
    if (!setHashTableCharge(ad_utility::MemorySize::bytes(
            Cache::numberOfBytesOfHashTable(cache_->hashTableCapacity())))) {
      // Only if the table has grown more than
      // `hashTableCapacityAfterOneMoreInsertion` says, and the budget has no
      // memory left for the difference.
      uncached_ = result;
      switchOff();
      return uncached_;
    }
    return endOfWindowLookup(result);
  }

  // True while lookups can be answered from the cache.
  bool isEnabled() const noexcept { return cache_.has_value(); }

  const Statistics& statistics() const noexcept { return statistics_; }

  // The reserved memory of the entries and the hash table. Both are reserved
  // from one budget of at most `MemorySize::max()`, so the sum cannot
  // overflow.
  ad_utility::MemorySize memoryUsed() const noexcept {
    return ad_utility::MemorySize::bytes(memoryOfEntries_.getBytes() +
                                         memoryOfHashTable_.getBytes());
  }

  // True iff `id` may be cached (see `canBeCachedForWholeExport`) and its
  // conversion reads a vocabulary or the text index. The other values
  // (`Undefined`, `Bool`, `Int`, `Double`, `Date`, `GeoPoint`,
  // `BlankNodeIndex`) are formatted from the bits of the `Id` without reading
  // any vocabulary, so caching them saves no read; they would only take the
  // place of terms that need one and distort the hit rate.
  static constexpr bool isWorthCaching(Id id) noexcept {
    if (!canBeCachedForWholeExport(id)) {
      return false;
    }
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
    return idToStringAndType<Conversion::removeQuotesAndAngleBrackets,
                             Conversion::returnOnlyLiterals>(
        index_.get(), id, localVocab, escapeFunction_);
  }

  const Value& computeUncached(Id id, const LocalVocab& localVocab) {
    uncached_ = compute(id, localVocab);
    return uncached_;
  }

  // The memory of an entry with `value` besides its slot in the hash table:
  // the list node, and the heap memory of the string unless it is stored
  // inside the `std::string` object (small string optimization).
  static ad_utility::MemorySize memoryOfEntry(const Value& value) noexcept {
    size_t numberOfBytes = numberOfBytesPerListNode;
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
    return ad_utility::MemorySize::bytes(numberOfBytes);
  }

  // The memory of the hash table at the peak of one more insertion: if the
  // table may grow (see `LRUCache::hashTableCapacityAfterOneMoreInsertion`),
  // its old and its new table coexist while the entries are moved.
  ad_utility::MemorySize memoryOfHashTableForOneMoreInsertion() const {
    size_t capacity = cache_->hashTableCapacity();
    size_t capacityAfter = cache_->hashTableCapacityAfterOneMoreInsertion();
    return ad_utility::MemorySize::bytes(
        Cache::numberOfBytesOfHashTable(capacityAfter) +
        (capacityAfter != capacity ? Cache::numberOfBytesOfHashTable(capacity)
                                   : 0));
  }

  // Prepare the insertion of an entry of `memory`: reserve its memory
  // and the growth of the hash table (including the old table while it is
  // moved) together, then evict least recently used entries until the
  // entries and the hash table at the peak of the insertion fit into the
  // maximal size. Shrink the hash table when it is less than 1/4 full. Return
  // false, with nothing reserved for the entry, if the entry is larger than
  // 1/16 of the maximal size or the budget has not enough memory left. In the
  // second case, entries may already have been evicted and the hash table
  // shrunk: after a shrink, the peak of the insertion can exceed the
  // reservation, and the budget may not hold the difference.
  bool reserveAndMakeRoomForEntry(ad_utility::MemorySize memory) {
    if (memory > maxMemoryOfOneEntry_) {
      return false;
    }
    auto hashTablePeak = memoryOfHashTableForOneMoreInsertion();
    auto hashTableGrowth = hashTablePeak > memoryOfHashTable_
                               ? hashTablePeak - memoryOfHashTable_
                               : ad_utility::MemorySize::bytes(0);
    if (!allocator_.tryReserveWithoutAllocating(memory + hashTableGrowth)) {
      return false;
    }
    memoryOfEntries_ += memory;
    memoryOfHashTable_ += hashTableGrowth;
    while (memoryOfEntries_ + memoryOfHashTableForOneMoreInsertion() >
               maxMemory_ &&
           cache_->size() > 0) {
      auto evicted = cache_->popLeastRecentlyUsed();
      AD_CORRECTNESS_CHECK(evicted.has_value());
      releaseEntry(memoryOfEntry(evicted.value()));
      if (cache_->size() * 4 < cache_->hashTableCapacity()) {
        cache_->shrinkHashTableToFit();
        // The smaller table can grow again with the insertion, so its peak can
        // be larger than the current reservation.
        if (!setHashTableCharge(memoryOfHashTableForOneMoreInsertion())) {
          releaseEntry(memory);
          return false;
        }
      }
    }
    AD_CORRECTNESS_CHECK(cache_->size() < cache_->capacity());
    return true;
  }

  // Reserve or return memory so that the reservation for the hash table is
  // `memory`. Return false and change nothing if the budget has not enough
  // memory left.
  bool setHashTableCharge(ad_utility::MemorySize memory) {
    if (memory > memoryOfHashTable_) {
      if (!allocator_.tryReserveWithoutAllocating(memory -
                                                  memoryOfHashTable_)) {
        return false;
      }
    } else if (memory < memoryOfHashTable_) {
      allocator_.releaseReservation(memoryOfHashTable_ - memory);
    }
    memoryOfHashTable_ = memory;
    return true;
  }

  // Return the reservation of `memory` for one entry: either an evicted entry,
  // or the entry that `reserveAndMakeRoomForEntry` reserved but does not
  // insert.
  void releaseEntry(ad_utility::MemorySize memory) {
    AD_CORRECTNESS_CHECK(memoryOfEntries_ >= memory);
    memoryOfEntries_ -= memory;
    allocator_.releaseReservation(memory);
  }

  // Destroy all entries and return their memory.
  void switchOff() {
    cache_.reset();
    auto reserved = memoryUsed();
    if (reserved > ad_utility::MemorySize::bytes(0)) {
      allocator_.releaseReservation(reserved);
    }
    memoryOfEntries_ = ad_utility::MemorySize::bytes(0);
    memoryOfHashTable_ = ad_utility::MemorySize::bytes(0);
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
  bool isWindowHitRateTooLow() noexcept {
    if (!checksHitRate()) {
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

  // True iff the hit rate is checked: 0 for either value of the window
  // disables the check (see `IdToStringAndTypeCacheConfig`).
  bool checksHitRate() const noexcept {
    return numberOfLookupsPerWindow_ > 0 && minHitRate_ > 0.0;
  }

  FRIEND_TEST(IdToStringAndTypeCache, HashTableCountsForTheMaxSize);
};

}  // namespace ql::exportIds

#endif  // QLEVER_SRC_INDEX_IDTOSTRINGANDTYPECACHE_H
