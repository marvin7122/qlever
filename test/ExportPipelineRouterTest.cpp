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
using ad_utility::httpUtils::UrlParser;

namespace {

ParsedQuery parse(std::string_view queryStr) {
  return SparqlParser::parseQuery(std::string(queryStr));
}

TEST(ExportPipelineRouterTest, DefaultModeIsLegacyV1) {
  auto query = parse("SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
  UrlParser::ParamValueMap params;

  auto mode = ExportPipelineRouter::selectEngine(query, params);
  EXPECT_EQ(mode, ExportEngineMode::LegacyV1);
}

TEST(ExportPipelineRouterTest, UrlParamFastExportTruthySelectsV2) {
  auto query = parse("SELECT * WHERE { ?s ?p ?o }");

  std::vector<std::string> truthyValues = {"1", "true", "TRUE", "yes", "YES", "on"};
  for (const auto& val : truthyValues) {
    UrlParser::ParamValueMap params;
    params["fast-export"] = val;
    auto mode = ExportPipelineRouter::selectEngine(query, params);
    EXPECT_EQ(mode, ExportEngineMode::FastStreamingV2) << "Failed for val: " << val;
  }
}

TEST(ExportPipelineRouterTest, UrlParamFastExportFalsySelectsV1) {
  auto query = parse("SELECT * WHERE { ?s ?p ?o }");

  std::vector<std::string> falsyValues = {"0", "false", "FALSE", "no", "NO", "off"};
  for (const auto& val : falsyValues) {
    UrlParser::ParamValueMap params;
    params["fast-export"] = val;
    auto mode = ExportPipelineRouter::selectEngine(
        query, params, std::nullopt, ExportEngineMode::FastStreamingV2);
    EXPECT_EQ(mode, ExportEngineMode::LegacyV1) << "Failed for val: " << val;
  }
}

TEST(ExportPipelineRouterTest, UrlParamExportEngineV2) {
  auto query = parse("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }");

  UrlParser::ParamValueMap params;
  params["export-engine"] = "v2";
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params), ExportEngineMode::FastStreamingV2);

  params["export-engine"] = "fast";
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params), ExportEngineMode::FastStreamingV2);

  params["export-engine"] = "legacy";
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params), ExportEngineMode::LegacyV1);

  params["export-engine"] = "v1";
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params), ExportEngineMode::LegacyV1);
}

TEST(ExportPipelineRouterTest, HttpHeaderOverrides) {
  auto query = parse("SELECT ?s WHERE { ?s ?p ?o }");
  UrlParser::ParamValueMap params;

  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, "v2"),
            ExportEngineMode::FastStreamingV2);
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, "fast"),
            ExportEngineMode::FastStreamingV2);
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, "streaming"),
            ExportEngineMode::FastStreamingV2);
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params, "legacy"),
            ExportEngineMode::LegacyV1);
}

TEST(ExportPipelineRouterTest, ServerDefaultModeConfiguration) {
  auto query = parse("SELECT * WHERE { ?s ?p ?o }");
  UrlParser::ParamValueMap params;

  EXPECT_EQ(ExportPipelineRouter::selectEngine(
                query, params, std::nullopt, ExportEngineMode::LegacyV1),
            ExportEngineMode::LegacyV1);

  EXPECT_EQ(ExportPipelineRouter::selectEngine(
                query, params, std::nullopt, ExportEngineMode::FastStreamingV2),
            ExportEngineMode::FastStreamingV2);
}

TEST(ExportPipelineRouterTest, AskQueryNotEligibleForFastStreaming) {
  auto query = parse("ASK WHERE { ?s ?p ?o }");
  UrlParser::ParamValueMap params;
  params["fast-export"] = "1";

  // ASK query is not eligible for streaming export -> transparent fallback to LegacyV1
  EXPECT_FALSE(ExportPipelineRouter::isEligibleForFastStreaming(query));
  EXPECT_EQ(ExportPipelineRouter::selectEngine(query, params), ExportEngineMode::LegacyV1);
}

TEST(ExportPipelineRouterTest, DescribeDecisionDiagnostics) {
  auto query = parse("SELECT * WHERE { ?s ?p ?o }");
  UrlParser::ParamValueMap params;
  params["fast-export"] = "1";

  std::string desc = ExportPipelineRouter::describeDecision(query, params);
  EXPECT_THAT(desc, testing::HasSubstr("FastStreamingV2"));
  EXPECT_THAT(desc, testing::HasSubstr("Fast-Path V2 selected"));
}

}  // namespace
