// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

// Byte equality of the live V2 SELECT CSV/TSV serializer
// (`ExportEngineV2::appendSerializedRows`, vocabulary resolution included)
// with Legacy V1 on a real test index: escaping, empty literals, unbound
// cells, blank nodes, encoded IRIs, local-vocab words, encoded values, and
// enough rows for multi-window morsels.

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryPlanner.h"
#include "engine/export_v2/ExportEngineV2.h"
#include "index/IndexImpl.h"
#include "parser/SparqlParser.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"

namespace {
using ad_utility::MediaType;
using ad_utility::VocabularyType;
using ql::engine::export_v2::ExportEngineV2;

struct Exports {
  std::string legacy_;
  std::string v2String_;
  std::string v2Chunks_;
};

// V2 streams the lazy result blocks (`Result::idTables`), so it needs a root
// operation that is computed lazily. A fully materialized root result (e.g.
// a cached result, or VALUES) is outside this test's scope.
bool rootResultIsLazy(QueryExecutionContext* qec, ParsedQuery& parsed) {
  qec->clearCacheUnpinnedOnly();
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, handle};
  auto qet = qp.createExecutionTree(parsed);
  return !qet.getResult(true)->isFullyMaterialized();
}

Exports runAllEngines(ad_utility::testing::TestIndexConfig config,
                      const std::string& query, MediaType mediaType) {
  auto qec = ad_utility::testing::getQec(std::move(config));
  const auto& encodedIriManager = qec->getIndex().getImpl().encodedIriManager();
  auto parsed = SparqlParser::parseQuery(&encodedIriManager, query, {});
  // Every engine run plans afresh on an empty cache: a cached (fully
  // materialized) result from the previous run would not be lazy.
  auto plan = [&]() {
    qec->clearCacheUnpinnedOnly();
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    QueryPlanner qp{qec, handle};
    return std::pair{qp.createExecutionTree(parsed), handle};
  };
  Exports result;
  {
    auto [qet, handle] = plan();
    ad_utility::Timer timer{ad_utility::Timer::Started};
    for (const auto& block : ExportQueryExecutionTrees::computeResult(
             parsed, qet, mediaType, timer, handle)) {
      result.legacy_ += block;
    }
  }
  if (!rootResultIsLazy(qec, parsed)) {
    ADD_FAILURE() << "root result is fully materialized, V2 cannot stream it: "
                  << query;
    return result;
  }
  {
    auto [qet, handle] = plan();
    EXPECT_TRUE(ExportEngineV2::canHandle(parsed, qet, mediaType)) << query;
    for (const auto& block :
         ExportEngineV2::computeResult(parsed, qet, mediaType, handle)) {
      result.v2String_ += block;
    }
  }
  {
    auto [qet, handle] = plan();
    for (const auto& chunk :
         ExportEngineV2::computeResultChunks(parsed, qet, mediaType, handle)) {
      result.v2Chunks_ += chunk.toString();
    }
  }
  return result;
}

void expectV2EqualsLegacy(
    const ad_utility::testing::TestIndexConfig& config,
    const std::string& query,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  for (auto mediaType : {MediaType::csv, MediaType::tsv}) {
    auto exports = runAllEngines(config, query, mediaType);
    EXPECT_FALSE(exports.legacy_.empty());
    EXPECT_EQ(exports.v2String_, exports.legacy_)
        << ad_utility::toString(mediaType) << ": " << query;
    EXPECT_EQ(exports.v2Chunks_, exports.legacy_)
        << ad_utility::toString(mediaType) << ": " << query;
  }
}

const std::string kg = R"(
<http://example.org/1> <http://example.org/label> "plain" .
<http://example.org/2> <http://example.org/label> "with,comma" .
<http://example.org/3> <http://example.org/label> "with \"quote\"" .
<http://example.org/4> <http://example.org/label> "line\nbreak" .
<http://example.org/5> <http://example.org/label> "tab\there"@en .
<http://example.org/6> <http://example.org/label> "" .
<http://example.org/7> <http://example.org/label> 42 .
<http://example.org/8> <http://example.org/label> 3.5 .
<http://example.org/9> <http://example.org/label> _:b1 .
<http://example.org/10> <http://example.org/label> <http://other.org/x,y> .
<http://example.org/11> <http://example.org/label> "2024-01-01"^^<http://www.w3.org/2001/XMLSchema#date> .
<http://example.org/12> <http://example.org/label> "custom"^^<http://example.org/dt> .
<http://example.org/13> <http://example.org/label> "Ein ziemlich langes Literal, das die externe Vokabel trifft"@de .
<http://example.org/14> <http://example.org/label> "short"@en .
<http://example.org/15> <http://example.org/label> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
<http://other.org/a> <http://example.org/label> "subject not encoded" .
_:b2 <http://example.org/label> "blank subject" .
)";

ad_utility::testing::TestIndexConfig makeConfig(
    std::string turtle, std::optional<VocabularyType> vocabularyType) {
  ad_utility::testing::TestIndexConfig config{std::move(turtle)};
  config.encodedPrefixesWithoutAngleBrackets =
      std::vector<std::string>{"http://example.org/"};
  config.vocabularyType = vocabularyType;
  return config;
}

const std::vector<std::optional<VocabularyType>> vocabularyTypes{
    std::nullopt, VocabularyType{VocabularyType::Enum::OnDiskCompressed},
    VocabularyType{VocabularyType::Enum::InMemoryUncompressed}};

const std::vector<std::string> queries{
    // Vocabulary words, encoded IRIs, blank nodes, encoded values, escaping.
    "SELECT ?s ?o WHERE { ?s <http://example.org/label> ?o }",
    // Column order differs from the table order.
    "SELECT ?o ?s WHERE { ?s <http://example.org/label> ?o }",
    // Unbound cells (UNDEF) and local-vocab words that need escaping.
    "SELECT ?s ?x ?o WHERE { ?s <http://example.org/label> ?o . "
    "VALUES ?x { \"v,1\" UNDEF <http://example.org/77> 5 \"\" } }",
    // Words computed by BIND live in the local vocabulary.
    "SELECT ?s ?y WHERE { ?s <http://example.org/label> ?o "
    "BIND(CONCAT(STR(?o), \"\\\"x\\\"\") AS ?y) }",
    // A uniformly encoded (integer) column.
    "SELECT ?s ?n WHERE { ?s <http://example.org/label> ?o "
    "BIND(STRLEN(STR(?o)) AS ?n) }",
    // Language filter, as in the Wikidata benchmark query.
    "SELECT ?s ?o WHERE { ?s <http://example.org/label> ?o "
    "FILTER(LANG(?o) = \"en\") }",
    // All columns.
    "SELECT * WHERE { ?s ?p ?o }",
};

TEST(ExportEngineV2LiveTest, SmallIndexMatchesLegacy) {
  for (const auto& vocabularyType : vocabularyTypes) {
    for (const auto& query : queries) {
      expectV2EqualsLegacy(makeConfig(kg, vocabularyType), query);
    }
  }
}

// More rows than one revocation window (1024) and one morsel (8192): 300
// triples times 40 VALUES rows. The test index builder uses two triples per
// partial vocabulary, so the row count comes from the cartesian product, not
// from more triples (which would exhaust the file descriptors).
TEST(ExportEngineV2LiveTest, ManyRowsMatchLegacy) {
  std::string turtle;
  for (size_t i = 0; i < 300; ++i) {
    const std::string n = std::to_string(i);
    turtle += "<http://example.org/" + n + "> <http://example.org/label> ";
    switch (i % 5) {
      case 0:
        turtle += "\"label " + n + "\"@en .\n";
        break;
      case 1:
        turtle += "\"needs,quoting " + n + "\" .\n";
        break;
      case 2:
        turtle += "<http://other.org/" + n + "> .\n";
        break;
      case 3:
        turtle += n + " .\n";
        break;
      default:
        turtle += "\"\" .\n";
    }
  }
  std::string values;
  for (size_t k = 0; k < 40; ++k) {
    values +=
        k % 2 == 0 ? std::to_string(k) : "\"v," + std::to_string(k) + "\"";
    values += " ";
  }
  for (const auto& vocabularyType : vocabularyTypes) {
    expectV2EqualsLegacy(
        makeConfig(turtle, vocabularyType),
        "SELECT ?s ?o ?k WHERE { ?s <http://example.org/label> "
        "?o . VALUES ?k { " +
            values + "} }");
  }
}

}  // namespace
