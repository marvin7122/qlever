// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

// Byte equality of the live V2 SELECT CSV/TSV serializer
// (`ExportEngineV2::appendSerializedRows`, vocabulary resolution included)
// and of the V2 CONSTRUCT Turtle/N-Triples serializer
// (`ConstructRowSerializer`) with Legacy V1 on a real test index: escaping,
// empty literals, unbound cells, blank nodes, encoded IRIs, local-vocab words,
// encoded values, and enough rows for multi-window morsels. With a scheduler
// (unordered helper threads) the CONSTRUCT lines must be the same multiset, and
// the same bytes for LIMIT/OFFSET (ordered session).

#include <absl/strings/match.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <algorithm>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryPlanner.h"
#include "engine/export_v2/ExportEngineV2.h"
#include "index/IndexImpl.h"
#include "parser/SparqlParser.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

namespace {
using ad_utility::MediaType;
using ad_utility::VocabularyType;
using ql::engine::export_v2::ExportEngineV2;

struct Exports {
  std::string legacy_;
  std::string v2String_;
  std::string v2Chunks_;
};

// `scheduler` (optional) runs the V2 morsels on helper threads.
Exports runAllEngines(
    ad_utility::testing::TestIndexConfig config, const std::string& query,
    MediaType mediaType,
    ad_utility::export_v2::ElasticExportScheduler* scheduler = nullptr) {
  auto qec = ad_utility::testing::getQec(std::move(config));
  qec->clearCacheUnpinnedOnly();
  const auto& encodedIriManager = qec->getIndex().getImpl().encodedIriManager();
  auto parsed = SparqlParser::parseQuery(&encodedIriManager, query, {});
  Exports result;
  {
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    QueryPlanner qp{qec, handle};
    auto qet = qp.createExecutionTree(parsed);
    ad_utility::Timer timer{ad_utility::Timer::Started};
    for (const auto& block : ExportQueryExecutionTrees::computeResult(
             parsed, qet, mediaType, timer, handle)) {
      result.legacy_ += block;
    }
  }
  {
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    QueryPlanner qp{qec, handle};
    auto qet = qp.createExecutionTree(parsed);
    EXPECT_TRUE(ExportEngineV2::canHandle(parsed, qet, mediaType)) << query;
    for (const auto& block : ExportEngineV2::computeResult(
             parsed, qet, mediaType, handle, scheduler)) {
      result.v2String_ += block;
    }
  }
  {
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    QueryPlanner qp{qec, handle};
    auto qet = qp.createExecutionTree(parsed);
    for (const auto& chunk : ExportEngineV2::computeResultChunks(
             parsed, qet, mediaType, handle, scheduler)) {
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
    // The `IndexScan` applies the OFFSET itself; V2 must not skip the rows a
    // second time.
    "SELECT ?s ?o WHERE { ?s <http://example.org/label> ?o } OFFSET 3",
    "SELECT ?s ?o WHERE { ?s <http://example.org/label> ?o } LIMIT 4 "
    "OFFSET 2",
    // An OFFSET that the root operation (a join) does not apply.
    "SELECT ?s ?o WHERE { ?s <http://example.org/label> ?o . ?s ?p ?x } "
    "LIMIT 5 OFFSET 1",
};

TEST(ExportEngineV2LiveTest, SmallIndexMatchesLegacy) {
  for (const auto& vocabularyType : vocabularyTypes) {
    for (const auto& query : queries) {
      expectV2EqualsLegacy(makeConfig(kg, vocabularyType), query);
    }
  }
}

// More rows than one revocation window (1024) and one morsel (8192), with a
// tiny permutation block size, so a morsel spans many blocks and windows
// (exercises the per-morsel reservation and in-place window appends).
TEST(ExportEngineV2LiveTest, ManyRowsMatchLegacy) {
  std::string turtle;
  for (size_t i = 0; i < 10'000; ++i) {
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
  for (const auto& vocabularyType : vocabularyTypes) {
    expectV2EqualsLegacy(
        makeConfig(turtle, vocabularyType),
        "SELECT ?s ?o WHERE { ?s <http://example.org/label> ?o }");
  }
}

// The lines of `text`, sorted: CONSTRUCT output as a triple multiset.
std::vector<std::string> sortedLines(const std::string& text) {
  std::vector<std::string> lines = absl::StrSplit(text, '\n');
  ql::ranges::sort(lines);
  return lines;
}

// CONSTRUCT in Turtle and N-Triples: without a scheduler V2 writes the same
// bytes as Legacy. With helper threads the morsels of an unbounded query may
// arrive in any order, so only the triple multiset must match; with LIMIT or
// OFFSET the session is ordered and the bytes must match again.
void expectConstructV2EqualsLegacy(
    const ad_utility::testing::TestIndexConfig& config,
    const std::string& query, bool expectEmpty = false,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  ad_utility::export_v2::ElasticExportScheduler scheduler{4};
  const bool ordered =
      absl::StrContains(query, "LIMIT") || absl::StrContains(query, "OFFSET");
  for (auto mediaType : {MediaType::turtle, MediaType::ntriples}) {
    auto exports = runAllEngines(config, query, mediaType);
    EXPECT_EQ(exports.legacy_.empty(), expectEmpty)
        << ad_utility::toString(mediaType) << ": " << query;
    EXPECT_EQ(exports.v2String_, exports.legacy_)
        << ad_utility::toString(mediaType) << ": " << query;
    EXPECT_EQ(exports.v2Chunks_, exports.legacy_)
        << ad_utility::toString(mediaType) << ": " << query;

    auto parallel = runAllEngines(config, query, mediaType, &scheduler);
    EXPECT_EQ(parallel.legacy_, exports.legacy_);
    if (ordered) {
      EXPECT_EQ(parallel.v2String_, exports.legacy_)
          << ad_utility::toString(mediaType) << ": " << query;
      EXPECT_EQ(parallel.v2Chunks_, exports.legacy_)
          << ad_utility::toString(mediaType) << ": " << query;
    } else {
      EXPECT_EQ(sortedLines(parallel.v2String_), sortedLines(exports.legacy_))
          << ad_utility::toString(mediaType) << ": " << query;
      EXPECT_EQ(sortedLines(parallel.v2Chunks_), sortedLines(exports.legacy_))
          << ad_utility::toString(mediaType) << ": " << query;
    }
  }
}

const std::vector<std::string> constructQueries{
    // Vocabulary words, encoded IRIs, blank nodes in the data, language tags,
    // literals that need escaping, numbers, dates, booleans, custom types.
    "CONSTRUCT { ?s <http://example.org/label> ?o } "
    "WHERE { ?s <http://example.org/label> ?o }",
    // Variable in every position.
    "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
    // Constants: IRIs (`a`), a literal that needs escaping, a language-tagged
    // literal, and a number (short in Turtle, typed in N-Triples).
    "CONSTRUCT { ?s a <http://example.org/C> . "
    "?s <http://example.org/c> \"say \\\"hi\\\"\\n\" . "
    "?s <http://example.org/l> \"Hallo\"@de . "
    "?s <http://example.org/n> 42 . ?o <http://example.org/back> ?s } "
    "WHERE { ?s <http://example.org/label> ?o }",
    // A literal in subject position and a variable that the WHERE clause does
    // not bind drop their template triples; the other triple stays.
    "CONSTRUCT { \"lit\" <http://example.org/p> ?o . "
    "?s <http://example.org/p> ?nowhere . ?s <http://example.org/q> ?o } "
    "WHERE { ?s <http://example.org/label> ?o }",
    // Unbound (UNDEF) values skip only the triples that use them.
    "CONSTRUCT { ?s <http://example.org/x> ?x . ?s <http://example.org/o> ?o } "
    "WHERE { ?s <http://example.org/label> ?o . "
    "VALUES ?x { \"v,1\" UNDEF <http://example.org/77> 5 \"\" } }",
    // Local-vocab words (BIND) that need escaping, encoded integers, doubles
    // and booleans.
    "CONSTRUCT { ?s <http://example.org/y> ?y . ?s <http://example.org/n> ?n . "
    "?s <http://example.org/d> ?d . ?s <http://example.org/b> ?b } "
    "WHERE { ?s <http://example.org/label> ?o "
    "BIND(CONCAT(STR(?o), \"\\n\\\"x\\\"\\\\\") AS ?y) "
    "BIND(STRLEN(STR(?o)) AS ?n) BIND(?n * 0.5 AS ?d) BIND(?n > 3 AS ?b) }",
    // Template blank nodes: one label per row, shared within the row.
    "CONSTRUCT { ?s <http://example.org/has> _:b . _:b <http://example.org/v> "
    "?o } WHERE { ?s <http://example.org/label> ?o }",
    // LIMIT/OFFSET applied by the `IndexScan`, and by the export (a join):
    // order and blank-node labels as in Legacy.
    "CONSTRUCT { ?s <http://example.org/has> _:b . _:b <http://example.org/v> "
    "?o } WHERE { ?s <http://example.org/label> ?o } LIMIT 4 OFFSET 2",
    "CONSTRUCT { ?s <http://example.org/has> _:b . _:b <http://example.org/v> "
    "?o } WHERE { ?s <http://example.org/label> ?o . ?s ?p ?x } "
    "LIMIT 5 OFFSET 1",
    // Language filter, as in the Wikidata benchmark query.
    "CONSTRUCT { ?s <http://www.w3.org/2000/01/rdf-schema#label> ?o } "
    "WHERE { ?s <http://example.org/label> ?o FILTER(LANG(?o) = \"en\") }",
};

TEST(ExportEngineV2LiveTest, ConstructMatchesLegacy) {
  for (const auto& vocabularyType : vocabularyTypes) {
    for (const auto& query : constructQueries) {
      expectConstructV2EqualsLegacy(makeConfig(kg, vocabularyType), query);
    }
  }
}

TEST(ExportEngineV2LiveTest, ConstructEmptyResult) {
  for (const auto& vocabularyType : vocabularyTypes) {
    expectConstructV2EqualsLegacy(
        makeConfig(kg, vocabularyType),
        "CONSTRUCT { ?s <http://example.org/p> ?o } "
        "WHERE { ?s <http://example.org/nothing> ?o }",
        true);
    expectConstructV2EqualsLegacy(
        makeConfig(kg, vocabularyType),
        "CONSTRUCT { ?s <http://example.org/p> ?o } "
        "WHERE { ?s <http://example.org/label> ?o } LIMIT 0",
        true);
  }
}

// CONSTRUCT CSV/TSV and deduplicating CONSTRUCT stay on Legacy.
TEST(ExportEngineV2LiveTest, ConstructRoutingStaysOnLegacyWhenUnsupported) {
  auto qec = ad_utility::testing::getQec(makeConfig(kg, std::nullopt));
  const auto& encodedIriManager = qec->getIndex().getImpl().encodedIriManager();
  auto parsed = SparqlParser::parseQuery(
      &encodedIriManager, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", {});
  EXPECT_TRUE(ExportEngineV2::canHandle(parsed, MediaType::turtle));
  EXPECT_TRUE(ExportEngineV2::canHandle(parsed, MediaType::ntriples));
  EXPECT_FALSE(ExportEngineV2::canHandle(parsed, MediaType::csv));
  EXPECT_FALSE(ExportEngineV2::canHandle(parsed, MediaType::tsv));
  EXPECT_FALSE(ExportEngineV2::canHandle(parsed, MediaType::qleverJson));
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::constructDeduplication_>(
          ad_utility::DeduplicationMode::full());
  EXPECT_FALSE(ExportEngineV2::canHandle(parsed, MediaType::turtle));
}

// Many rows, tiny blocks: morsels span blocks and revocation windows, and the
// blank-node labels count the rows of earlier blocks.
TEST(ExportEngineV2LiveTest, ConstructManyRowsMatchLegacy) {
  std::string turtle;
  for (size_t i = 0; i < 10'000; ++i) {
    const std::string n = std::to_string(i);
    turtle += "<http://example.org/" + n + "> <http://example.org/label> ";
    switch (i % 4) {
      case 0:
        turtle += "\"Name " + n + "\"@de .\n";
        break;
      case 1:
        turtle += "\"quote \\\" " + n + "\" .\n";
        break;
      case 2:
        turtle += "<http://other.org/" + n + "> .\n";
        break;
      default:
        turtle += n + " .\n";
    }
  }
  for (const auto& vocabularyType : vocabularyTypes) {
    const auto config = makeConfig(turtle, vocabularyType);
    expectConstructV2EqualsLegacy(
        config,
        "CONSTRUCT { ?s <http://www.w3.org/2000/01/rdf-schema#label> ?o . "
        "?s <http://example.org/node> _:b } "
        "WHERE { ?s <http://example.org/label> ?o }");
    expectConstructV2EqualsLegacy(
        config,
        "CONSTRUCT { ?s <http://example.org/node> _:b . _:b "
        "<http://example.org/v> ?o } WHERE { ?s <http://example.org/label> ?o "
        ". ?s ?p ?x } LIMIT 9000 OFFSET 333");
  }
}

}  // namespace
