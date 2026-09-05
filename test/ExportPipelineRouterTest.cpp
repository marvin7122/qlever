// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/ExportPipelineRouter.h"
#include "parser/SparqlParser.h"

using namespace ql::engine;
using ad_utility::url_parser::ParamValueMap;

namespace {

ParsedQuery parse(std::string_view queryStr) {
  return SparqlParser::parseQuery(nullptr, std::string(queryStr));
}

TEST(ExportPipelineRouterTest, ToStringFunction) {
  EXPECT_EQ(toString(ExportEngineMode::LegacyV1), "LegacyV1");
  EXPECT_EQ(toString(ExportEngineMode::FastStreamingV2), "FastStreamingV2");
  EXPECT_EQ(toString(static_cast<ExportEngineMode>(999)), "Unknown");
  EXPECT_EQ(toString(ExportSendMode::ConcatenatedString), "ConcatenatedString");
  EXPECT_EQ(toString(ExportSendMode::ScatterGather), "ScatterGather");
}

TEST(ExportPipelineRouterTest, DefaultModeIsLegacyV1) {
  auto query = parse("SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
  ParamValueMap params;

  auto mode = ExportPipelineRouter::selectEngine(query, params);
  EXPECT_EQ(mode, ExportEngineMode::LegacyV1);
}

TEST(ExportPipelineRouterTest, UrlParamFastExportTruthySelectsV2) {
  auto query = parse("SELECT * WHERE { ?s ?p ?o }");

  std::vector<std::string> truthyValues = {"1",   "true", "TRUE",
                                           "yes", "YES",  "on"};
  for (const auto& val : truthyValues) {
    ParamValueMap params;
    params["fast-export"] = {val};
    auto mode = ExportPipelineRouter::selectEngine(query, params);
    EXPECT_EQ(mode, ExportEngineMode::FastStreamingV2)
        << "Failed for val: " << val;
  }
}

TEST(ExportPipelineRouterTest, UrlParamFastExportFalsySelectsV1) {
  auto query = parse("SELECT * WHERE { ?s ?p ?o }");

  std::vector<std::string> falsyValues = {"0",  "false", "FALSE",
                                          "no", "NO",    "off"};
  for (const auto& val : falsyValues) {
    ParamValueMap params;
    params["fast-export"] = {val};
    auto mode = ExportPipelineRouter::selectEngine(
        query, params, std::nullopt, ExportEngineMode::FastStreamingV2);
    EXPECT_EQ(mode, ExportEngineMode::LegacyV1) << "Failed for val: " << val;
  }
}

TEST(ExportPipelineRouterTest, UrlParamExportEngineV2) {
  auto query = parse("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }");

  ParamValueMap params;
  params["export-engine"] = {"v2"};
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params),
            ExportEngineMode::FastStreamingV2);

  params["export-engine"] = {"fast"};
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params),
            ExportEngineMode::FastStreamingV2);

  params["export-engine"] = {"legacy"};
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params),
            ExportEngineMode::LegacyV1);

  params["export-engine"] = {"v1"};
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params),
            ExportEngineMode::LegacyV1);
}

TEST(ExportPipelineRouterTest, HttpHeaderOverrides) {
  auto query = parse("SELECT ?s WHERE { ?s ?p ?o }");
  ParamValueMap params;

  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, "v2"),
            ExportEngineMode::FastStreamingV2);
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, "fast"),
            ExportEngineMode::FastStreamingV2);
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, "streaming"),
            ExportEngineMode::FastStreamingV2);
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, "legacy"),
            ExportEngineMode::LegacyV1);
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, "v1"),
            ExportEngineMode::LegacyV1);
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, "unknown-header"),
            ExportEngineMode::LegacyV1);
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, ""),
            ExportEngineMode::LegacyV1);
}

TEST(ExportPipelineRouterTest, ServerDefaultModeConfiguration) {
  auto query = parse("SELECT * WHERE { ?s ?p ?o }");
  ParamValueMap params;

  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, std::nullopt,
                                               ExportEngineMode::LegacyV1),
            ExportEngineMode::LegacyV1);

  EXPECT_EQ(ExportPipelineRouter::selectEngine(
                query, params, std::nullopt, ExportEngineMode::FastStreamingV2),
            ExportEngineMode::FastStreamingV2);
}

TEST(ExportPipelineRouterTest, AskQueryNotEligibleForFastStreaming) {
  auto query = parse("ASK WHERE { ?s ?p ?o }");
  ParamValueMap params;
  params["fast-export"] = {"1"};

  EXPECT_FALSE(ExportPipelineRouter::isEligibleForFastStreaming(query));
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params),
            ExportEngineMode::LegacyV1);
}

TEST(ExportPipelineRouterTest, DescribeDecisionDiagnostics) {
  auto selectQuery = parse("SELECT * WHERE { ?s ?p ?o }");
  auto askQuery = parse("ASK WHERE { ?s ?p ?o }");

  // 1. Fast path selected
  {
    ParamValueMap params;
    params["fast-export"] = {"1"};
    std::string desc =
        ExportPipelineRouter::describeDecision(selectQuery, params);
    EXPECT_THAT(desc, testing::HasSubstr("FastStreamingV2"));
    EXPECT_THAT(desc, testing::HasSubstr("Fast-Path V2 selected"));
  }

  // 2. Ineligible query fallback
  {
    ParamValueMap params;
    params["fast-export"] = {"1"};
    std::string desc = ExportPipelineRouter::describeDecision(askQuery, params);
    EXPECT_THAT(desc, testing::HasSubstr("LegacyV1"));
    EXPECT_THAT(desc, testing::HasSubstr("ineligible for V2 streaming"));
  }

  // 3. Explicit V1 override
  {
    ParamValueMap params;
    params["fast-export"] = {"0"};
    std::string desc = ExportPipelineRouter::describeDecision(
        selectQuery, params, std::nullopt, ExportEngineMode::FastStreamingV2);
    EXPECT_THAT(desc, testing::HasSubstr("LegacyV1"));
    EXPECT_THAT(desc, testing::HasSubstr("explicitly requested"));
  }

  // 4. Default standard relational pipeline
  {
    ParamValueMap params;
    std::string desc =
        ExportPipelineRouter::describeDecision(selectQuery, params);
    EXPECT_THAT(desc, testing::HasSubstr("LegacyV1"));
    EXPECT_THAT(desc,
                testing::HasSubstr("default standard relational pipeline"));
  }
}

TEST(ExportPipelineRouterTest, SelectSendModeDefaultIsConcatenatedString) {
  ParamValueMap params;
  EXPECT_EQ(ExportPipelineRouter::selectSendMode(params),
            ExportSendMode::ConcatenatedString);
  EXPECT_EQ(ExportPipelineRouter::selectSendMode(params, "unknown"),
            ExportSendMode::ConcatenatedString);
}

TEST(ExportPipelineRouterTest, SelectSendModeUrlParam) {
  ParamValueMap params;
  params["export-send"] = {"iovec"};
  EXPECT_EQ(ExportPipelineRouter::selectSendMode(params),
            ExportSendMode::ScatterGather);
  params["export-send"] = {"string"};
  EXPECT_EQ(ExportPipelineRouter::selectSendMode(params),
            ExportSendMode::ConcatenatedString);
  params["export-send"] = {"writev"};
  EXPECT_EQ(ExportPipelineRouter::selectSendMode(params),
            ExportSendMode::ScatterGather);
}

TEST(ExportPipelineRouterTest, UnsupportedConstructsFallBackToV1) {
  // Each query requests V2 explicitly; every one must still route to Legacy.
  const std::vector<std::string> ineligibleQueries = {
      // Federated execution leaves this machine.
      "SELECT * WHERE { SERVICE <http://example.org/sparql> { ?s ?p ?o } }",
      // Aggregation, grouping, and modifiers V2 does not implement.
      "SELECT ?s (COUNT(?o) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?s",
      "SELECT * WHERE { ?s ?p ?o } ORDER BY ?s",
      "SELECT DISTINCT * WHERE { ?s ?p ?o }",
      "SELECT REDUCED * WHERE { ?s ?p ?o }",
      // Graph shapes outside Scan/Join/Filter.
      "SELECT * WHERE { { ?s ?p ?o } UNION { ?a ?b ?c } }",
      "SELECT * WHERE { ?s ?p ?o . OPTIONAL { ?s ?q ?r } }",
      "SELECT * WHERE { ?s ?p ?o . MINUS { ?s ?q ?r } }",
      "SELECT * WHERE { { SELECT ?s WHERE { ?s ?p ?o } } }",
      "SELECT * WHERE { ?s <http://example.org/p>+ ?o }",
  };
  for (const auto& queryStr : ineligibleQueries) {
    auto query = parse(queryStr);
    ParamValueMap params;
    params["fast-export"] = {"1"};
    EXPECT_FALSE(ExportPipelineRouter::isEligibleForFastStreaming(query))
        << "Failed for query: " << queryStr;
    EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params),
              ExportEngineMode::LegacyV1)
        << "Failed for query: " << queryStr;
  }
}

TEST(ExportPipelineRouterTest, EligibleShapesStillSelectV2) {
  const std::vector<std::string> eligibleQueries = {
      "SELECT * WHERE { ?s ?p ?o }",
      "SELECT * WHERE { ?s ?p ?o . ?a ?b ?c . FILTER(?o > 5) }",
      "SELECT * WHERE { ?s ?p ?o . BIND(1 AS ?x) }",
      "SELECT * WHERE { ?s ?p ?o . VALUES ?v { 1 2 3 } }",
      "SELECT * WHERE { ?s ?p ?o } LIMIT 10 OFFSET 5",
  };
  for (const auto& queryStr : eligibleQueries) {
    auto query = parse(queryStr);
    ParamValueMap params;
    params["fast-export"] = {"1"};
    EXPECT_TRUE(ExportPipelineRouter::isEligibleForFastStreaming(query))
        << "Failed for query: " << queryStr;
    EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params),
              ExportEngineMode::FastStreamingV2)
        << "Failed for query: " << queryStr;
  }
}

TEST(ExportPipelineRouterTest, IneligibleQueryExplainsFallback) {
  auto query = parse(
      "SELECT * WHERE { SERVICE <http://example.org/sparql> "
      "{ ?s ?p ?o } }");
  ParamValueMap params;
  params["fast-export"] = {"1"};
  std::string desc = ExportPipelineRouter::describeDecision(query, params);
  EXPECT_THAT(desc, testing::HasSubstr("LegacyV1"));
  EXPECT_THAT(desc, testing::HasSubstr("ineligible for V2 streaming"));
}

TEST(ExportPipelineRouterTest, SelectSendModeHeader) {
  ParamValueMap params;
  EXPECT_EQ(ExportPipelineRouter::selectSendMode(params, "iovec"),
            ExportSendMode::ScatterGather);
  params["export-send"] = {"string"};
  EXPECT_EQ(ExportPipelineRouter::selectSendMode(params, "iovec"),
            ExportSendMode::ConcatenatedString);
}

}  // namespace
