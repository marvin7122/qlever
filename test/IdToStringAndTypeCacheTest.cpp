// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "index/IdToStringAndTypeCache.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/AllocatorTestHelpers.h"
#include "util/IndexTestHelpers.h"

namespace {

using ad_utility::MemorySize;
using ad_utility::testing::makeAllocator;
using ql::exportIds::idToStringAndType;
using Cache = ql::exportIds::IdToStringAndTypeCache<>;
using Config = ql::exportIds::IdToStringAndTypeCacheConfig;

// A cache that is large enough for every test and never switches itself off.
const Config largeCache{MemorySize::megabytes(64), 0, 0.0};

// An index with the IRIs `<a0>`, ..., `<a{n-1}>`, and a function that returns
// the `Id` of `<ai>`.
auto makeIndexWithIris(size_t n) {
  std::string kg;
  for (size_t i = 0; i < n; ++i) {
    absl::StrAppend(&kg, "<s> <p> <a", i, "> .\n");
  }
  auto qec = ad_utility::testing::getQec(kg);
  auto getId = [getId = ad_utility::testing::makeGetId(qec->getIndex())](
                   size_t i) { return getId(absl::StrCat("<a", i, ">")); };
  return std::pair{qec, getId};
}

// _____________________________________________________________________________
// The cache returns the same values as direct lookups and serves repeated
// vocabulary terms from the cache. Values that are encoded in the `Id` bits
// are computed directly.
TEST(IdToStringAndTypeCache, MatchesDirectAndHits) {
  std::string kg =
      "<s> <p> <o> . "
      "<s> <q> \"hello\" . "
      "<s> <p> 42 . "
      "<s> <p> 3.14 .";
  auto qec = ad_utility::testing::getQec(kg);
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  auto getId = ad_utility::testing::makeGetId(index);
  std::vector<Id> vocabIds{getId("<s>"), getId("<p>"), getId("<o>"),
                           getId("<q>"), getId("\"hello\"")};
  std::vector<Id> bitEncodedIds{Id::makeFromInt(42), Id::makeFromDouble(3.14),
                                Id::makeFromBool(true), Id::makeUndefined()};

  Cache cache{index, largeCache, makeAllocator()};
  // The first pass populates the cache, the second pass must hit on every
  // vocabulary term.
  for (int pass = 0; pass < 2; ++pass) {
    for (const auto& ids : {vocabIds, bitEncodedIds}) {
      for (Id id : ids) {
        EXPECT_EQ(cache.get(id, localVocab),
                  idToStringAndType(index, id, localVocab))
            << "Mismatch, pass " << pass;
      }
    }
  }
  const auto& statistics = cache.statistics();
  EXPECT_EQ(statistics.numberOfHits_, vocabIds.size());
  EXPECT_EQ(statistics.numberOfMisses_, vocabIds.size());
  EXPECT_EQ(statistics.numberOfBitEncodedLookups_, 2 * bitEncodedIds.size());
  EXPECT_EQ(statistics.numberOfLookups(),
            2 * (vocabIds.size() + bitEncodedIds.size()));
}

// _____________________________________________________________________________
// Exactly the datatypes whose conversion reads a vocabulary or the text index
// are worth caching.
TEST(IdToStringAndTypeCache, IsWorthCaching) {
  using enum Datatype;
  auto worth = [](Datatype datatype) {
    return Cache::isWorthCaching(Id::fromBits(datatype, 0));
  };
  for (Datatype datatype : {VocabIndex, SecondaryVocabIndex, EncodedVal,
                            WordVocabIndex, TextRecordIndex}) {
    EXPECT_TRUE(worth(datatype)) << toString(datatype);
  }
  for (Datatype datatype : {Undefined, Bool, Int, Double, Date, GeoPoint,
                            BlankNodeIndex, LocalVocabIndex}) {
    EXPECT_FALSE(worth(datatype)) << toString(datatype);
  }
}

// _____________________________________________________________________________
// Terms of the secondary vocabulary (`SecondaryVocabIndex`) are cached like
// the terms of the main vocabulary.
TEST(IdToStringAndTypeCache, CachesSecondaryVocabTerms) {
  using namespace ad_utility::testing;
  // Use a fresh index (and not the shared one of `getQec`), because the
  // secondary vocabulary must not leak into other tests.
  TestIndexConfig config{"<a> <b> <c> ."};
  config.secondaryVocabWords = std::vector<std::string>{"<zzz>"};
  Index index = makeTestIndex(std::move(config));
  LocalVocab localVocab{};
  Id id = Id::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(0));
  Cache cache{index, largeCache, makeAllocator()};
  for (int pass = 0; pass < 2; ++pass) {
    const auto& value = cache.get(id, localVocab);
    EXPECT_EQ(value, idToStringAndType(index, id, localVocab));
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->first, "<zzz>");
  }
  EXPECT_EQ(cache.statistics().numberOfHits_, 1u);
  EXPECT_EQ(cache.statistics().numberOfMisses_, 1u);
}

// _____________________________________________________________________________
// The conversion (template arguments and escape function) is part of the
// cache, so the cached strings are those of exactly that conversion.
TEST(IdToStringAndTypeCache, AppliesItsConversion) {
  auto qec = ad_utility::testing::getQec("<s> <p> \"a\\tb\" .");
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  Id id = ad_utility::testing::makeGetId(index)("\"a\tb\"");
  auto escape = &RdfEscaping::escapeForTsv;
  ql::exportIds::IdToStringAndTypeCache<false, false, decltype(escape)> cache{
      index, largeCache, makeAllocator(), escape};
  for (int pass = 0; pass < 2; ++pass) {
    EXPECT_EQ(cache.get(id, localVocab),
              (idToStringAndType<false, false>(index, id, localVocab, escape)));
  }
  EXPECT_NE(cache.get(id, localVocab),
            idToStringAndType(index, id, localVocab));
  EXPECT_EQ(cache.statistics().numberOfHits_, 2u);
}

// _____________________________________________________________________________
// `LocalVocabIndex` IDs bypass the cache: a `LocalVocab` can be destroyed
// during a lazy export, and the same `Id` can then denote a different word.
TEST(IdToStringAndTypeCache, BypassesLocalVocabIds) {
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");
  const Index& index = qec->getIndex();
  Cache cache{index, largeCache, makeAllocator()};

  LocalVocab localVocab{};
  Id localVocabId =
      Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
          LocalVocabEntry::literalWithoutQuotes("localLit",
                                                qec->getLocalVocabContext())));
  for (int pass = 0; pass < 2; ++pass) {
    const auto& cached = cache.get(localVocabId, localVocab);
    EXPECT_EQ(cached, idToStringAndType(index, localVocabId, localVocab))
        << "Mismatch, pass " << pass;
    ASSERT_TRUE(cached.has_value());
    EXPECT_THAT(cached->first, ::testing::HasSubstr("localLit"));
  }
  EXPECT_EQ(cache.statistics().numberOfLocalVocabLookups_, 2u);
  EXPECT_EQ(cache.statistics().numberOfLookups(), 2u);
}

// _____________________________________________________________________________
// A `LocalVocabIndex` `Id` whose word is also in the index vocabulary compares
// equal to the `VocabIndex` `Id` of that word (see `ValueId::compareThreeWay`).
// It still does not hit the cached `VocabIndex` entry, because it never reaches
// the cache.
TEST(IdToStringAndTypeCache, LocalVocabIdEqualToCachedIdDoesNotHit) {
  auto qec = ad_utility::testing::getQec("<s> <p> \"hello\" .");
  const Index& index = qec->getIndex();
  Cache cache{index, largeCache, makeAllocator()};
  LocalVocab localVocab{};
  Id vocabId = ad_utility::testing::makeGetId(index)("\"hello\"");
  Id localVocabId =
      Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
          LocalVocabEntry::literalWithoutQuotes("hello",
                                                qec->getLocalVocabContext())));
  ASSERT_EQ(localVocabId, vocabId);
  EXPECT_EQ(cache.get(vocabId, localVocab),
            idToStringAndType(index, vocabId, localVocab));
  EXPECT_EQ(cache.get(localVocabId, localVocab),
            idToStringAndType(index, localVocabId, localVocab));
  EXPECT_EQ(cache.statistics().numberOfHits_, 0u);
  EXPECT_EQ(cache.statistics().numberOfMisses_, 1u);
  EXPECT_EQ(cache.statistics().numberOfLocalVocabLookups_, 1u);
}

// _____________________________________________________________________________
// A maximal size of 0 (the default of the runtime parameter), or one that is
// too small for 16 entries, disables the cache: every lookup is computed
// directly.
TEST(IdToStringAndTypeCache, SmallMaxSizeDisables) {
  auto [qec, getId] = makeIndexWithIris(1);
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  for (auto maxSize : {MemorySize::bytes(0), MemorySize::bytes(1000)}) {
    Cache cache{index, Config{maxSize, 0, 0.0}, makeAllocator()};
    EXPECT_FALSE(cache.isEnabled());
    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(cache.get(getId(0), localVocab),
                idToStringAndType(index, getId(0), localVocab));
    }
    EXPECT_EQ(cache.statistics().numberOfLookupsWhileDisabled_, 3u);
    EXPECT_EQ(cache.statistics().numberOfLookups(), 3u);
  }
  Cache enabled{index, Config{MemorySize::kilobytes(64), 0, 0.0},
                makeAllocator()};
  EXPECT_TRUE(enabled.isEnabled());
}

// _____________________________________________________________________________
// The memory of the entries never exceeds the maximal size: the least
// recently used entries are evicted to make room.
TEST(IdToStringAndTypeCache, EvictsToStayWithinMaxSize) {
  auto [qec, getId] = makeIndexWithIris(200);
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  const auto maxSize = MemorySize::kilobytes(8);
  Cache cache{index, Config{maxSize, 0, 0.0}, makeAllocator()};
  ASSERT_TRUE(cache.isEnabled());
  for (size_t i = 0; i < 200; ++i) {
    EXPECT_EQ(cache.get(getId(i), localVocab),
              idToStringAndType(index, getId(i), localVocab));
    EXPECT_LE(cache.memoryUsed(), maxSize);
  }
  EXPECT_GT(cache.memoryUsed(), MemorySize::bytes(0));
  // The most recent term is still cached, the first one was evicted.
  cache.get(getId(199), localVocab);
  EXPECT_EQ(cache.statistics().numberOfHits_, 1u);
  cache.get(getId(0), localVocab);
  EXPECT_EQ(cache.statistics().numberOfHits_, 1u);
  EXPECT_EQ(cache.statistics().numberOfMisses_, 201u);
}

// _____________________________________________________________________________
// The memory of the entries and of the hash table stays within the maximal
// size, also when large terms evict many small ones (the hash table does not
// shrink by itself when entries are removed).
TEST(IdToStringAndTypeCache, HashTableCountsForTheMaxSize) {
  std::string kg;
  for (size_t i = 0; i < 300; ++i) {
    absl::StrAppend(&kg, "<s> <p> <a", i, "> .\n");
  }
  for (size_t i = 0; i < 20; ++i) {
    absl::StrAppend(&kg, "<s> <q> \"", std::string(400, 'x'), i, "\" .\n");
  }
  auto qec = ad_utility::testing::getQec(kg);
  const Index& index = qec->getIndex();
  auto getId = ad_utility::testing::makeGetId(index);
  LocalVocab localVocab{};
  const auto maxSize = MemorySize::kilobytes(8);
  auto allocator = makeAllocator(MemorySize::megabytes(1));
  Cache cache{index, Config{maxSize, 0, 0.0}, allocator};
  auto check = [&](Id id) {
    EXPECT_EQ(cache.get(id, localVocab),
              idToStringAndType(index, id, localVocab));
    EXPECT_LE(cache.memoryUsed(), maxSize);
    EXPECT_EQ(allocator.amountMemoryLeft(),
              MemorySize::megabytes(1) - cache.memoryUsed());
  };
  for (size_t i = 0; i < 300; ++i) {
    check(getId(absl::StrCat("<a", i, ">")));
  }
  for (size_t i = 0; i < 20; ++i) {
    check(getId(absl::StrCat("\"", std::string(400, 'x'), i, "\"")));
  }
  EXPECT_EQ(cache.statistics().numberOfTermsNotCachedForLackOfMemory_, 0u);
  EXPECT_EQ(cache.statistics().numberOfMisses_, 320u);
}

// _____________________________________________________________________________
// A term that is larger than 1/16 of the maximal size is returned but not
// cached, and does not count for the hit rate.
TEST(IdToStringAndTypeCache, DoesNotCacheTermsLargerThanOneSixteenth) {
  std::string longLiteral(600, 'x');
  auto qec = ad_utility::testing::getQec(
      absl::StrCat("<s> <p> \"short\" .\n<s> <p> \"", longLiteral, "\" .\n"));
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  auto getId = ad_utility::testing::makeGetId(index);
  Id shortId = getId("\"short\"");
  Id longId = getId(absl::StrCat("\"", longLiteral, "\""));
  // 1/16 of 8 kB is 500 bytes. A window of one lookup with a minimal hit rate
  // of 1 switches the cache off on the first miss.
  Cache cache{index, Config{MemorySize::kilobytes(8), 1, 1.0}, makeAllocator()};
  for (int pass = 0; pass < 2; ++pass) {
    EXPECT_EQ(cache.get(longId, localVocab),
              idToStringAndType(index, longId, localVocab));
  }
  EXPECT_TRUE(cache.isEnabled());
  EXPECT_EQ(cache.statistics().numberOfTermsNotCachedForLackOfMemory_, 2u);
  EXPECT_EQ(cache.memoryUsed(), MemorySize::bytes(0));
  // The short term is cached; its miss completes the window and switches the
  // cache off, and its value is still returned correctly.
  EXPECT_EQ(cache.get(shortId, localVocab),
            idToStringAndType(index, shortId, localVocab));
  EXPECT_FALSE(cache.isEnabled());
  EXPECT_EQ(cache.memoryUsed(), MemorySize::bytes(0));
}

// _____________________________________________________________________________
// The memory of the entries is reserved from the allocator's budget and
// returned when the cache switches off or is destroyed. Without enough budget,
// terms are not cached.
TEST(IdToStringAndTypeCache, ReservesMemoryFromTheBudget) {
  auto [qec, getId] = makeIndexWithIris(10);
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  const auto budget = MemorySize::megabytes(1);
  auto allocator = makeAllocator(budget);
  {
    Cache cache{index, largeCache, allocator};
    for (size_t i = 0; i < 10; ++i) {
      cache.get(getId(i), localVocab);
    }
    EXPECT_GT(cache.memoryUsed(), MemorySize::bytes(0));
    EXPECT_EQ(allocator.amountMemoryLeft(), budget - cache.memoryUsed());
  }
  EXPECT_EQ(allocator.amountMemoryLeft(), budget);

  // A budget that holds the first entry, but not the hash table that it
  // needs: the term is not cached, the cache stays enabled, and nothing stays
  // reserved.
  {
    auto smallAllocator = makeAllocator(MemorySize::bytes(60));
    Cache cache{index, largeCache, smallAllocator};
    EXPECT_EQ(cache.get(getId(0), localVocab),
              idToStringAndType(index, getId(0), localVocab));
    EXPECT_TRUE(cache.isEnabled());
    EXPECT_EQ(cache.statistics().numberOfTermsNotCachedForLackOfMemory_, 1u);
    EXPECT_EQ(cache.memoryUsed(), MemorySize::bytes(0));
    EXPECT_EQ(smallAllocator.amountMemoryLeft(), MemorySize::bytes(60));
  }

  // A budget that is used up: nothing is cached, and nothing is reserved.
  auto emptyAllocator = makeAllocator(MemorySize::bytes(0));
  Cache cache{index, largeCache, emptyAllocator};
  for (int pass = 0; pass < 2; ++pass) {
    EXPECT_EQ(cache.get(getId(0), localVocab),
              idToStringAndType(index, getId(0), localVocab));
  }
  EXPECT_EQ(cache.statistics().numberOfTermsNotCachedForLackOfMemory_, 2u);
  EXPECT_EQ(cache.memoryUsed(), MemorySize::bytes(0));
  EXPECT_EQ(emptyAllocator.amountMemoryLeft(), MemorySize::bytes(0));
}

// _____________________________________________________________________________
// A window whose hit rate is below the minimum switches the cache off for the
// rest of the export, and returns its memory; the values stay correct.
TEST(IdToStringAndTypeCache, SwitchesOffOnLowHitRate) {
  auto [qec, getId] = makeIndexWithIris(10);
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  auto allocator = makeAllocator(MemorySize::megabytes(1));
  Cache cache{index, Config{MemorySize::megabytes(1), 4, 0.5}, allocator};
  // All distinct: the first window of 4 lookups has no hits.
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(cache.get(getId(i), localVocab),
              idToStringAndType(index, getId(i), localVocab));
    EXPECT_EQ(cache.isEnabled(), i < 3) << i;
  }
  EXPECT_EQ(cache.statistics().numberOfMisses_, 4u);
  EXPECT_EQ(cache.statistics().numberOfLookupsWhileDisabled_, 6u);
  EXPECT_EQ(cache.memoryUsed(), MemorySize::bytes(0));
  EXPECT_EQ(allocator.amountMemoryLeft(), MemorySize::megabytes(1));
}

// _____________________________________________________________________________
// Windows with a hit rate at or above the minimum keep the cache on, and a
// later window with a low hit rate still switches it off. Bit-encoded values
// do not count for the window.
TEST(IdToStringAndTypeCache, ChecksEveryWindow) {
  auto [qec, getId] = makeIndexWithIris(10);
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  Cache cache{index, Config{MemorySize::megabytes(1), 4, 0.5}, makeAllocator()};
  // Two distinct terms in 12 lookups: 2 misses in the first window, then hits.
  // Distinct integers in between do not count.
  for (size_t i = 0; i < 12; ++i) {
    EXPECT_EQ(cache.get(getId(i % 2), localVocab),
              idToStringAndType(index, getId(i % 2), localVocab));
    Id integer = Id::makeFromInt(static_cast<int64_t>(i));
    EXPECT_EQ(cache.get(integer, localVocab),
              idToStringAndType(index, integer, localVocab));
  }
  EXPECT_TRUE(cache.isEnabled());
  EXPECT_EQ(cache.statistics().numberOfHits_, 10u);
  EXPECT_EQ(cache.statistics().numberOfMisses_, 2u);
  EXPECT_EQ(cache.statistics().numberOfBitEncodedLookups_, 12u);
  // A window with one hit (25 % < 50 %) switches the cache off at its end.
  for (size_t i : {0, 5, 6, 7}) {
    EXPECT_TRUE(cache.isEnabled());
    EXPECT_EQ(cache.get(getId(i), localVocab),
              idToStringAndType(index, getId(i), localVocab));
  }
  EXPECT_FALSE(cache.isEnabled());
  EXPECT_EQ(cache.statistics().numberOfHits_, 11u);
  EXPECT_EQ(cache.statistics().numberOfMisses_, 5u);
}

}  // namespace
