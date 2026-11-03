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
#include "util/IndexTestHelpers.h"

namespace {

using ql::exportIds::idToStringAndType;
using ql::exportIds::IdToStringAndTypeCache;
using Config = ql::exportIds::IdToStringAndTypeCacheConfig;

using ad_utility::MemorySize;

// A cache that is large enough for every test and never switches itself off.
const Config largeCache{1 << 16, MemorySize::megabytes(64), 0, 0.0};

// _____________________________________________________________________________
// The cache returns the same values as direct lookups and serves repeats from
// the cache (visible in the statistics).
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
  std::vector<Id> ids{
      getId("<s>"),
      getId("<p>"),
      getId("<o>"),
      getId("<q>"),
      getId("\"hello\""),
      Id::makeFromInt(42),
      Id::makeFromDouble(3.14),
      Id::makeUndefined(),
  };

  IdToStringAndTypeCache<> cache{index, largeCache};
  // First pass populates the cache, second pass must hit on every id.
  for (int pass = 0; pass < 2; ++pass) {
    for (const Id& id : ids) {
      EXPECT_EQ(cache.get(id, localVocab),
                idToStringAndType(index, id, localVocab))
          << "Mismatch, pass " << pass;
    }
  }
  EXPECT_EQ(cache.statistics().numberOfLookups(), 2 * ids.size());
  EXPECT_EQ(cache.statistics().numberOfHits_, ids.size());
  EXPECT_EQ(cache.statistics().numberOfMisses_, ids.size());
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
  IdToStringAndTypeCache<false, false, decltype(escape)> cache{
      index, largeCache, escape};
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
  IdToStringAndTypeCache<> cache{index, largeCache};

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
  IdToStringAndTypeCache<> cache{index, largeCache};
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
// Capacity 0 disables the cache: every lookup is computed directly.
TEST(IdToStringAndTypeCache, CapacityZeroBypasses) {
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  IdToStringAndTypeCache<> cache{index,
                                 Config{0, MemorySize::megabytes(64), 0, 0.0}};
  EXPECT_FALSE(cache.isEnabled());
  for (int i = 0; i < 3; ++i) {
    Id id = Id::makeFromInt(7);
    EXPECT_EQ(cache.get(id, localVocab),
              idToStringAndType(index, id, localVocab));
  }
  EXPECT_EQ(cache.statistics().numberOfLookupsWhileDisabled_, 3u);
  EXPECT_EQ(cache.statistics().numberOfLookups(), 3u);
}

// _____________________________________________________________________________
// A window whose hit rate is below the minimum switches the cache off for the
// rest of the export; the values stay correct.
TEST(IdToStringAndTypeCache, SwitchesOffOnLowHitRate) {
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  IdToStringAndTypeCache<> cache{
      index, Config{100, MemorySize::megabytes(64), 4, 0.5}};
  // All distinct: the first window of 4 lookups has no hits.
  for (int i = 0; i < 10; ++i) {
    Id id = Id::makeFromInt(i);
    EXPECT_EQ(cache.get(id, localVocab),
              idToStringAndType(index, id, localVocab));
    EXPECT_EQ(cache.isEnabled(), i < 3) << i;
  }
  EXPECT_EQ(cache.statistics().numberOfMisses_, 4u);
  EXPECT_EQ(cache.statistics().numberOfLookupsWhileDisabled_, 6u);
}

// _____________________________________________________________________________
// Windows with a hit rate at or above the minimum keep the cache on, and a
// later window with a low hit rate still switches it off.
TEST(IdToStringAndTypeCache, ChecksEveryWindow) {
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  IdToStringAndTypeCache<> cache{
      index, Config{100, MemorySize::megabytes(64), 4, 0.5}};
  // Two distinct ids in 12 lookups: 2 misses in the first window, then hits.
  for (int i = 0; i < 12; ++i) {
    Id id = Id::makeFromInt(i % 2);
    EXPECT_EQ(cache.get(id, localVocab),
              idToStringAndType(index, id, localVocab));
  }
  EXPECT_TRUE(cache.isEnabled());
  EXPECT_EQ(cache.statistics().numberOfHits_, 10u);
  EXPECT_EQ(cache.statistics().numberOfMisses_, 2u);
  // A window with one hit (25 % < 50 %) switches the cache off at its end.
  for (int i : {0, 100, 101, 102}) {
    Id id = Id::makeFromInt(i);
    EXPECT_TRUE(cache.isEnabled());
    EXPECT_EQ(cache.get(id, localVocab),
              idToStringAndType(index, id, localVocab));
  }
  EXPECT_FALSE(cache.isEnabled());
  EXPECT_EQ(cache.statistics().numberOfHits_, 11u);
  EXPECT_EQ(cache.statistics().numberOfMisses_, 5u);
}

// _____________________________________________________________________________
// A term whose string is larger than its share of the maximal size (here
// 1000 bytes / 10 entries = 100 bytes) is returned, but not cached.
TEST(IdToStringAndTypeCache, DoesNotCacheTermsLargerThanTheirShare) {
  std::string longLiteral(200, 'x');
  auto qec = ad_utility::testing::getQec(
      absl::StrCat("<s> <p> \"short\" . <s> <p> \"", longLiteral, "\" ."));
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  auto getId = ad_utility::testing::makeGetId(index);
  Id shortId = getId("\"short\"");
  Id longId = getId(absl::StrCat("\"", longLiteral, "\""));
  IdToStringAndTypeCache<> cache{index,
                                 Config{10, MemorySize::bytes(1000), 0, 0.0}};
  for (int pass = 0; pass < 2; ++pass) {
    for (Id id : {shortId, longId}) {
      EXPECT_EQ(cache.get(id, localVocab),
                idToStringAndType(index, id, localVocab));
    }
  }
  EXPECT_EQ(cache.statistics().numberOfHits_, 1u);
  EXPECT_EQ(cache.statistics().numberOfMisses_, 3u);
  EXPECT_EQ(cache.statistics().numberOfTermsTooLargeToCache_, 2u);

  // With a maximal size of 0, no term is cached.
  IdToStringAndTypeCache<> nothingFits{
      index, Config{10, MemorySize::bytes(0), 0, 0.0}};
  for (int pass = 0; pass < 2; ++pass) {
    EXPECT_EQ(nothingFits.get(shortId, localVocab),
              idToStringAndType(index, shortId, localVocab));
  }
  EXPECT_EQ(nothingFits.statistics().numberOfHits_, 0u);
  EXPECT_EQ(nothingFits.statistics().numberOfTermsTooLargeToCache_, 2u);
}

// _____________________________________________________________________________
// A switch-off on a lookup whose term was too large to cache returns that
// term correctly (it is already in the slot for uncached lookups).
TEST(IdToStringAndTypeCache, SwitchOffOnTermTooLargeToCache) {
  auto qec = ad_utility::testing::getQec("<s> <p> \"short\" .");
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  Id id = ad_utility::testing::makeGetId(index)("\"short\"");
  IdToStringAndTypeCache<> cache{index,
                                 Config{10, MemorySize::bytes(0), 1, 1.0}};
  EXPECT_EQ(cache.get(id, localVocab),
            idToStringAndType(index, id, localVocab));
  EXPECT_FALSE(cache.isEnabled());
  EXPECT_EQ(cache.get(id, localVocab),
            idToStringAndType(index, id, localVocab));
}

}  // namespace
