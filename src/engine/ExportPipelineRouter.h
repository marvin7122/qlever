// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORTPIPELINEROUTER_H
#define QLEVER_SRC_ENGINE_EXPORTPIPELINEROUTER_H

#include <optional>
#include <string>
#include <string_view>

#include "parser/ParsedQuery.h"
#include "util/http/UrlParser.h"

namespace ql::engine {

// _____________________________________________________________________________
// Execution engine mode for query results and data exports.
enum class ExportEngineMode {
  LegacyV1 = 0,        // Proven pull-based Volcano iterator using `IdTable`.
  FastStreamingV2 = 1  // Push-based streaming export engine.
};

// Return a human-readable representation of `ExportEngineMode`.
[[nodiscard]] constexpr std::string_view toString(
    ExportEngineMode mode) noexcept {
  switch (mode) {
    case ExportEngineMode::LegacyV1:
      return "LegacyV1";
    case ExportEngineMode::FastStreamingV2:
      return "FastStreamingV2";
  }
  return "Unknown";
}

// An explicitly requested engine override, parsed from the request's URL
// parameters and HTTP headers (see `ExportPipelineRouter::selectEngine`).
enum class ExplicitEngineRequest { None, WantV1, WantV2 };

// _____________________________________________________________________________
// Route an incoming SPARQL request either to the standard relational export
// pipeline (Legacy V1) or to the push-based streaming export engine (Fast-Path
// V2). Queries that V2 cannot handle fall back to V1 instead of failing.
//
// Precedence of explicit overrides: the `X-QLever-Export-Engine` HTTP header
// wins over the URL parameter `fast-export`, which wins over the URL parameter
// `export-engine`, which wins over the server default. A value that a source
// does not recognize is ignored, so the next source in this order decides.
//
// All functions are synchronous and do not retain `parameters` or
// `exportHeader` beyond the call.
class ExportPipelineRouter {
 public:
  using ParamValueMap = ad_utility::url_parser::ParamValueMap;

  // Return the export engine mode for `query`, given the request's URL
  // `parameters`, the value of the `X-QLever-Export-Engine` header (if any),
  // and the server-wide `serverDefault`. A request for V2 returns V2 only if
  // `isEligibleForFastStreaming(query)` holds.
  [[nodiscard]] static ExportEngineMode selectEngine(
      const ParsedQuery& query, const ParamValueMap& parameters,
      std::optional<std::string_view> exportHeader = std::nullopt,
      ExportEngineMode serverDefault = ExportEngineMode::LegacyV1);

  // Return true if `query` is a SELECT or CONSTRUCT query without constructs
  // that V2 cannot execute (see `hasUnsupportedConstructs`). Return false for
  // all other queries, in particular ASK and DESCRIBE.
  [[nodiscard]] static bool isEligibleForFastStreaming(
      const ParsedQuery& query);

  // Return the routing decision of `selectEngine` for the same arguments
  // together with a human-readable reason for logging.
  [[nodiscard]] static std::string describeDecision(
      const ParsedQuery& query, const ParamValueMap& parameters,
      std::optional<std::string_view> exportHeader = std::nullopt,
      ExportEngineMode serverDefault = ExportEngineMode::LegacyV1);

 private:
  // Parse the explicit per-request override shared by `selectEngine` and
  // `describeDecision`, so that the logged reason and the decision cannot
  // diverge.
  [[nodiscard]] static ExplicitEngineRequest parseExplicitRequest(
      const ParamValueMap& parameters,
      std::optional<std::string_view> exportHeader);

  // Return V2 if `query` is eligible for it, and V1 otherwise.
  [[nodiscard]] static ExportEngineMode fastStreamingIfEligible(
      const ParsedQuery& query);

  // Return true if `query` uses aggregation, HAVING, or ORDER BY, which need
  // the materialized grouping or sorting of Legacy V1.
  // TODO<Marvin Stoetzel> Also exclude SERVICE clauses, subqueries, property
  // paths, and MINUS before `selectEngine` is called from the `Server`.
  [[nodiscard]] static bool hasUnsupportedConstructs(const ParsedQuery& query);
};

}  // namespace ql::engine

#endif  // QLEVER_SRC_ENGINE_EXPORTPIPELINEROUTER_H
