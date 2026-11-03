// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "index/IdToStringAndTypeCache.h"
#include "util/IndexTestHelpers.h"

namespace {

// _____________________________________________________________________________
// The SELECT export cache returns the same values as direct lookups and
// serves repeats from the cache (visible in the hit statistics).
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

  ql::exportIds::IdToStringAndTypeCache cache{
      ql::exportIds::IdToStringAndTypeCache::DEFAULT_CAPACITY};
  // First pass populates the cache, second pass must hit on every id.
  for (int pass = 0; pass < 2; ++pass) {
    for (const Id& id : ids) {
      const auto& cached =
          ql::exportIds::cachedIdToStringAndType(cache, index, id, localVocab);
      EXPECT_EQ(cached, ql::exportIds::idToStringAndType(index, id, localVocab))
          << "Mismatch, pass " << pass;
    }
  }
  EXPECT_EQ(cache.stats().totalLookups(), 2 * ids.size());
  EXPECT_EQ(cache.stats().hits_, ids.size());
}

// _____________________________________________________________________________
// `LocalVocabIndex` IDs bypass the cache: a `LocalVocab` can be destroyed
// during a lazy export, and the same `Id` can then denote a different word.
TEST(IdToStringAndTypeCache, BypassesLocalVocabIds) {
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");
  const Index& index = qec->getIndex();
  ql::exportIds::IdToStringAndTypeCache cache{
      ql::exportIds::IdToStringAndTypeCache::DEFAULT_CAPACITY};

  LocalVocab localVocab{};
  Id localVocabId =
      Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
          LocalVocabEntry::literalWithoutQuotes("localLit",
                                                qec->getLocalVocabContext())));
  for (int pass = 0; pass < 2; ++pass) {
    const auto& cached = ql::exportIds::cachedIdToStringAndType(
        cache, index, localVocabId, localVocab);
    EXPECT_EQ(cached,
              ql::exportIds::idToStringAndType(index, localVocabId, localVocab))
        << "Mismatch, pass " << pass;
    ASSERT_TRUE(cached.has_value());
    EXPECT_THAT(cached->first, ::testing::HasSubstr("localLit"));
  }
  EXPECT_EQ(cache.stats().totalLookups(), 0u);
}

// _____________________________________________________________________________
// Capacity 0 disables the cache: every lookup is computed directly.
TEST(IdToStringAndTypeCache, CapacityZeroBypasses) {
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  ql::exportIds::IdToStringAndTypeCache cache{
      ql::exportIds::IdToStringAndTypeCache::Config{0, 0, 0.0}};
  EXPECT_FALSE(cache.enabled());
  for (int i = 0; i < 3; ++i) {
    Id id = Id::makeFromInt(7);
    EXPECT_EQ(
        ql::exportIds::cachedIdToStringAndType(cache, index, id, localVocab),
        ql::exportIds::idToStringAndType(index, id, localVocab));
  }
  EXPECT_EQ(cache.stats().totalLookups(), 0u);
  EXPECT_EQ(cache.bypassed(), 3u);
}

// _____________________________________________________________________________
// A window whose hit rate is below the minimum switches the cache off for the
// rest of the export; the values stay correct.
TEST(IdToStringAndTypeCache, SwitchesOffOnLowHitRate) {
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  ql::exportIds::IdToStringAndTypeCache cache{
      ql::exportIds::IdToStringAndTypeCache::Config{100, 4, 0.5}};
  // All distinct: the first window of 4 lookups has no hits.
  for (int i = 0; i < 10; ++i) {
    Id id = Id::makeFromInt(i);
    EXPECT_EQ(
        ql::exportIds::cachedIdToStringAndType(cache, index, id, localVocab),
        ql::exportIds::idToStringAndType(index, id, localVocab));
    EXPECT_EQ(cache.enabled(), i < 3) << i;
  }
  EXPECT_EQ(cache.stats().totalLookups(), 4u);
  EXPECT_EQ(cache.stats().misses_, 4u);
  EXPECT_EQ(cache.bypassed(), 6u);
}

// _____________________________________________________________________________
// Windows with a hit rate at or above the minimum keep the cache on.
TEST(IdToStringAndTypeCache, StaysOnOnHighHitRate) {
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  ql::exportIds::IdToStringAndTypeCache cache{
      ql::exportIds::IdToStringAndTypeCache::Config{100, 4, 0.5}};
  // Two distinct ids in 12 lookups: 2 misses in the first window, then hits.
  for (int i = 0; i < 12; ++i) {
    Id id = Id::makeFromInt(i % 2);
    EXPECT_EQ(
        ql::exportIds::cachedIdToStringAndType(cache, index, id, localVocab),
        ql::exportIds::idToStringAndType(index, id, localVocab));
  }
  EXPECT_TRUE(cache.enabled());
  EXPECT_EQ(cache.stats().hits_, 10u);
  EXPECT_EQ(cache.stats().misses_, 2u);
  EXPECT_EQ(cache.bypassed(), 0u);
}

}  // namespace
