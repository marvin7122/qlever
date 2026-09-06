// Copyright 2026, The QLever Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "engine/PrefetchingBatchResolver.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/VocabIndex.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "util/CompactStringVector.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"

using namespace ql::engine::prefetch;
using namespace std::string_literals;

namespace {

TEST(PrefetchingBatchResolver, ConfigurationInvariants) {
  EXPECT_NO_THROW(PrefetchConfig{.prefetchDistance = 1}.checkInvariants());
  EXPECT_NO_THROW(PrefetchConfig{.prefetchDistance = 8}.checkInvariants());
  EXPECT_NO_THROW(PrefetchConfig{.prefetchDistance = 64}.checkInvariants());

  EXPECT_ANY_THROW(PrefetchConfig{.prefetchDistance = 0}.checkInvariants());
  EXPECT_ANY_THROW(PrefetchConfig{.prefetchDistance = 129}.checkInvariants());
}

TEST(PrefetchingBatchResolver, PrefetchIntrinsicSmokeTest) {
  int x = 42;
    // Verify that prefetchVocabEntry and prefetchAddress do not throw
  // for null pointers, valid pointers, and explicit prefetch distances.
  EXPECT_NO_THROW(prefetchVocabEntry(nullptr));
  EXPECT_NO_THROW(prefetchVocabEntry(&x));
  EXPECT_NO_THROW(prefetchVocabEntry(&x, 16));
  EXPECT_NO_THROW(prefetchAddress(&x));
}

TEST(PrefetchingBatchResolver, EquivalenceWithStandardBatchResolution) {
  std::string kg =
      "<s> <p> \"first\" . <s> <p> \"second\" . <s> <p> \"third\" . <s> <p> "
      "\"fourth\" . <s> <p> 123 . <s> <p> <http://example.org/resource> .";
  auto qec = ad_utility::testing::getQec(kg);
  const auto& index = qec->getIndex();
  auto getId = ad_utility::testing::makeGetId(index);

  std::vector<Id> testIds = {
      getId("\"first\""),
      getId("<s>"),
      ad_utility::testing::IntId(123),
      getId("\"second\""),
      getId("<http://example.org/resource>"),
      getId("\"third\""),
      getId("\"fourth\""),
      ad_utility::testing::UndefId()};

  LocalVocab localVocab;

    // Resolve standard baseline
  auto baselineResults = ql::exportIds::idsToStringAndType(
      index, testIds, localVocab, ql::identity{});

  // Prefetched resolution with various prefetch distances
  for (size_t distance : {1, 2, 4, 8, 16}) {
    PrefetchingBatchResolver resolver(
        PrefetchConfig{.prefetchDistance = distance});
    auto prefetchedResults = resolver.idsToStringAndType(
        index, testIds, localVocab, ql::identity{});

    ASSERT_EQ(baselineResults.size(), prefetchedResults.size());
    for (size_t i = 0; i < baselineResults.size(); ++i) {
      EXPECT_EQ(baselineResults[i], prefetchedResults[i])
          << "Mismatch at index " << i << " with prefetch distance "
          << distance;
    }
  }
}

TEST(PrefetchingBatchResolver, CompactVectorPipelinedResolution) {
  CompactVectorOfStrings<char> words;
  std::vector<std::string> rawWords = {
      "<iri1>", "<iri2>", "\"literal1\"", "\"literal2\"", "\"longer_literal_3\""};
  words.build(rawWords);

  PrefetchingBatchResolver resolver(PrefetchConfig{.prefetchDistance = 4});

  std::vector<size_t> queryIndices = {0, 4, 1, 3, 2, 4, 0, 1};
  std::vector<std::string> resolvedWords(queryIndices.size());

  resolver.resolveCompactVectorPipelined(
      words, queryIndices,
      [&resolvedWords](size_t slot, size_t, std::string_view view) {
        resolvedWords[slot] = std::string(view);
      });

  for (size_t i = 0; i < queryIndices.size(); ++i) {
    EXPECT_EQ(resolvedWords[i], rawWords[queryIndices[i]]);
  }
}

TEST(PrefetchingBatchResolver, EmptyAndBoundaryInputs) {
  std::string kg = "<s> <p> <o> .";
  auto qec = ad_utility::testing::getQec(kg);
  const auto& index = qec->getIndex();

  PrefetchingBatchResolver resolver;
  LocalVocab localVocab;

    // Test empty IDs span
  auto emptyResults = resolver.idsToStringAndType(
      index, ql::span<const Id>{}, localVocab);
  EXPECT_TRUE(emptyResults.empty());

    // Test empty positions
  std::vector<std::optional<std::pair<std::string, const char*>>> results(1);
  std::vector<Id> ids = {ad_utility::testing::IntId(1)};
  EXPECT_NO_THROW(resolver.resolveVocabIndexIds(
      index, ids, ql::span<const size_t>{}, results));
}

}  // namespace
