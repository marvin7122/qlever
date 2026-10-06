//   Copyright 2024 - 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Authors: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//            Johannes Kalmbach <kalmbach@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"

using namespace sparqlExpression::detail;

// _____________________________________________________________________________
TEST(SparqlExpressionGenerators, makeStringResultGetter) {
  using ad_utility::triple_component::LiteralOrIri;
  auto* qec = ad_utility::testing::getQec();
  auto literal = LocalVocabEntry::literalWithoutQuotes(
      "Test String", qec->getLocalVocabContext());
  LocalVocab localVocab{};

  auto function = makeStringResultGetter(&localVocab);

  auto result = function(literal);

  EXPECT_EQ(result.getLocalVocabIndex()->toStringRepresentation(),
            "\"Test String\"");
}

// _____________________________________________________________________________
TEST(SparqlExpressionGenerators, idOrLiteralOrIriToId) {
  using ad_utility::triple_component::LiteralOrIri;
  auto* qec = ad_utility::testing::getQec();
  auto literal = LocalVocabEntry::literalWithoutQuotes(
      "Test String", qec->getLocalVocabContext());
  LocalVocab localVocab{};

  auto result = idOrLiteralOrIriToId(literal, &localVocab);

  EXPECT_EQ(result.getLocalVocabIndex()->toStringRepresentation(),
            "\"Test String\"");
  // Should be the identity function for regular ids.
  EXPECT_EQ(result.getBits(),
            idOrLiteralOrIriToId(result, &localVocab).getBits());
}

// _____________________________________________________________________________
TEST(SparqlExpressionGenerators, resultGeneratorSetOfIntervals) {
  auto t = Id::makeFromBool(true);
  auto f = Id::makeFromBool(false);
  {
    ad_utility::SetOfIntervals s{{{1, 3}, {3, 3}, {3, 4}, {5, 6}}};
    auto generator = sparqlExpression::detail::resultGenerator(s, 10);
    std::vector<Id> res;
    ql::ranges::copy(generator, std::back_inserter(res));
    EXPECT_THAT(res, ::testing::ElementsAre(f, t, t, t, f, t, f, f, f, f));
  }
  {
    ad_utility::SetOfIntervals s{{{0, 3}, {3, 3}, {3, 4}, {8, 10}}};
    auto generator = sparqlExpression::detail::resultGenerator(s, 10);
    std::vector<Id> res;
    ql::ranges::copy(generator, std::back_inserter(res));
    EXPECT_THAT(res, ::testing::ElementsAre(t, t, t, t, f, f, f, f, t, t));
  }
  {
    ad_utility::SetOfIntervals s{{{3, 11}}};
    auto generator = sparqlExpression::detail::resultGenerator(s, 10);
    std::vector<Id> res;
    ql::ranges::copy(generator, std::back_inserter(res));
    EXPECT_THAT(res, ::testing::ElementsAre(f, f, f, t, t, t, t, t, t, t));
  }

  using S = ad_utility::SetOfIntervals;
  auto checkPrefix = [](const S& set, size_t size,
                        const std::vector<Id>& expected) {
    auto generator = sparqlExpression::detail::resultGenerator(set, size);
    std::vector<Id> res;
    ql::ranges::copy(generator, std::back_inserter(res));
    EXPECT_EQ(res, expected);
  };
  const auto complement = S::Complement{}(S{{{1, 2}}});
  checkPrefix(complement, 3, {t, f, t});
  checkPrefix(S::Complement{}(S{}), 3, {t, t, t});
  checkPrefix(complement, 0, {});
  checkPrefix(S{{{3, 5}}}, 3, {f, f, f});
  checkPrefix(S{{{4, 6}}}, 3, {f, f, f});

  auto transformed = sparqlExpression::detail::resultGenerator(
      complement, 3, [](Id id) { return id.getBool() ? 42 : -1; });
  std::vector<int> transformedRes;
  ql::ranges::copy(transformed, std::back_inserter(transformedRes));
  EXPECT_THAT(transformedRes, ::testing::ElementsAre(42, -1, 42));
}
