// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORTPIPELINEROUTER_H
#define QLEVER_SRC_ENGINE_EXPORTPIPELINEROUTER_H

#include <absl/strings/str_cat.h>

#include <cctype>
#include <optional>
#include <string>
#include <string_view>

#include "parser/ParsedQuery.h"
#include "util/http/UrlParser.h"

namespace ql::engine {

// _____________________________________________________________________________
// Execution engine mode for query results and data exports.
enum class ExportEngineMode {
  LegacyV1 = 0,        // Proven pull-based Volcano iterator using IdTable
  FastStreamingV2 = 1  // Push-based zero-copy streaming engine
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
// parameters and HTTP headers (see `parseExplicitRequest`).
enum class ExplicitEngineRequest { None, WantV1, WantV2 };

// _____________________________________________________________________________
// Deep module routing incoming SPARQL requests between the standard relational
// execution pipeline (Legacy V1) and the specialized push-based streaming
// export engine (Fast-Path V2).
//
// Precedence of explicit overrides: the `X-QLever-Export-Engine` HTTP header
// wins over URL parameters, which win over the server default.
//
// Adheres to the 7 Universal Laws:
// - Law 1: Deep Module (concise public interface, encapsulated plan analysis)
// - Law 2: Zero Bookkeeping Leakage (caller never manages routing internals)
// - Law 4: Defining Errors Out of Existence (unsupported shapes safely
// fallback)
class ExportPipelineRouter {
 public:
  using ParamValueMap = ad_utility::url_parser::ParamValueMap;

  // ___________________________________________________________________________
  // Determine the appropriate export engine mode based on request metadata,
  // query AST eligibility, and server configuration defaults.
  [[nodiscard]] static ExportEngineMode selectEngine(
      const ParsedQuery& query, const ParamValueMap& parameters,
      std::optional<std::string_view> exportHeader = std::nullopt,
      ExportEngineMode serverDefault = ExportEngineMode::LegacyV1) {
    switch (parseExplicitRequest(parameters, exportHeader)) {
      case ExplicitEngineRequest::WantV2:
        return evaluateEligibility(query, ExportEngineMode::FastStreamingV2);
      case ExplicitEngineRequest::WantV1:
        return ExportEngineMode::LegacyV1;
      case ExplicitEngineRequest::None:
        break;
    }

    // Check server-wide default mode
    if (serverDefault == ExportEngineMode::FastStreamingV2) {
      return evaluateEligibility(query, ExportEngineMode::FastStreamingV2);
    }

    return ExportEngineMode::LegacyV1;
  }

  // ___________________________________________________________________________
  // Inspect the `ParsedQuery` AST to determine whether it is eligible for
  // `FastStreamingV2`. Return true for standard scan, join, projection, and
  // construct queries without unsupported constructs (aggregation, ordering,
  // ...). Return false otherwise (e.g. ASK and DESCRIBE, which currently use
  // standard evaluation).
  [[nodiscard]] static bool isEligibleForFastStreaming(
      const ParsedQuery& query) {
    // CONSTRUCT and SELECT queries are currently eligible.
    if (query.hasConstructClause() || query.hasSelectClause()) {
      if (hasUnsupportedConstructs(query)) {
        return false;
      }
      return true;
    }

    // ASK and DESCRIBE currently use standard evaluation
    return false;
  }

  // ___________________________________________________________________________
  // Return a detailed diagnostic string explaining the routing decision.
  [[nodiscard]] static std::string describeDecision(
      const ParsedQuery& query, const ParamValueMap& parameters,
      std::optional<std::string_view> exportHeader = std::nullopt,
      ExportEngineMode serverDefault = ExportEngineMode::LegacyV1) {
    ExportEngineMode selected =
        selectEngine(query, parameters, exportHeader, serverDefault);
    bool eligible = isEligibleForFastStreaming(query);

    std::string reason;
    if (selected == ExportEngineMode::FastStreamingV2) {
      reason =
          "Fast-Path V2 selected (eligible export query with explicit or "
          "default opt-in)";
    } else {
      const auto request = parseExplicitRequest(parameters, exportHeader);
      const bool explicitlyRequestedV2 =
          request == ExplicitEngineRequest::WantV2;
      const bool explicitlyRequestedV1 =
          request == ExplicitEngineRequest::WantV1;

      if (explicitlyRequestedV2 && !eligible) {
        reason =
            "Fallback to Legacy V1 (fast-path requested but query is "
            "ineligible for V2 streaming)";
      } else if (explicitlyRequestedV1) {
        reason =
            "Legacy V1 selected (explicitly requested via query parameter or "
            "header override)";
      } else if (serverDefault == ExportEngineMode::FastStreamingV2 &&
                 !eligible) {
        reason =
            "Fallback to Legacy V1 (server default is V2 but query is "
            "ineligible for V2 streaming)";
      } else {
        reason = "Legacy V1 selected (default standard relational pipeline)";
      }
    }

    return absl::StrCat("ExportEngine: ", toString(selected),
                        " [Reason: ", reason, "]");
  }

 private:
  // ASCII case-insensitive equality without heap allocation, so the
  // request-path helpers below can stay `noexcept`.
  [[nodiscard]] static bool equalsAsciiCaseInsensitive(
      std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size()) {
      return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
      if (std::tolower(static_cast<unsigned char>(a[i])) !=
          std::tolower(static_cast<unsigned char>(b[i]))) {
        return false;
      }
    }
    return true;
  }

  // Parse the explicit per-request override shared by `selectEngine` and
  // `describeDecision`, so wording and action cannot diverge. The HTTP header
  // wins over URL parameters when both are set.
  [[nodiscard]] static ExplicitEngineRequest parseExplicitRequest(
      const ParamValueMap& parameters,
      std::optional<std::string_view> exportHeader) noexcept {
    if (exportHeader.has_value()) {
      const auto headerVal = exportHeader.value();
      if (equalsAsciiCaseInsensitive(headerVal, "v2") ||
          equalsAsciiCaseInsensitive(headerVal, "fast") ||
          equalsAsciiCaseInsensitive(headerVal, "streaming")) {
        return ExplicitEngineRequest::WantV2;
      }
      if (equalsAsciiCaseInsensitive(headerVal, "v1") ||
          equalsAsciiCaseInsensitive(headerVal, "legacy")) {
        return ExplicitEngineRequest::WantV1;
      }
    }

    const auto optFastExport = getParameterValue(parameters, "fast-export");
    if (optFastExport.has_value()) {
      if (isTruthy(optFastExport.value())) {
        return ExplicitEngineRequest::WantV2;
      }
      if (isFalsy(optFastExport.value())) {
        return ExplicitEngineRequest::WantV1;
      }
    }

    const auto optExportEngine = getParameterValue(parameters, "export-engine");
    if (optExportEngine.has_value()) {
      const auto engineVal = optExportEngine.value();
      if (equalsAsciiCaseInsensitive(engineVal, "v2") ||
          equalsAsciiCaseInsensitive(engineVal, "fast")) {
        return ExplicitEngineRequest::WantV2;
      }
      if (equalsAsciiCaseInsensitive(engineVal, "v1") ||
          equalsAsciiCaseInsensitive(engineVal, "legacy")) {
        return ExplicitEngineRequest::WantV1;
      }
    }

    return ExplicitEngineRequest::None;
  }

  // Heterogeneous, zero-allocation parameter lookup on ParamValueMap. When a
  // key carries multiple values (e.g. `?fast-export=true&fast-export=false`),
  // the first value wins; this is documented here and pinned by test (unlike
  // `getParameterCheckAtMostOnce` in `UrlParser.h`, which throws).
  [[nodiscard]] static std::optional<std::string_view> getParameterValue(
      const ParamValueMap& parameters, std::string_view key) noexcept {
    auto it = parameters.find(key);
    if (it != parameters.end() && !it->second.empty()) {
      return it->second.front();
    }
    return std::nullopt;
  }

  [[nodiscard]] static ExportEngineMode evaluateEligibility(
      const ParsedQuery& query, ExportEngineMode targetMode) {
    if (targetMode == ExportEngineMode::FastStreamingV2) {
      if (isEligibleForFastStreaming(query)) {
        return ExportEngineMode::FastStreamingV2;
      }
      // Transparent fallback to Legacy V1
      return ExportEngineMode::LegacyV1;
    }
    return targetMode;
  }

  [[nodiscard]] static bool isTruthy(std::string_view val) noexcept {
    return val == "1" || equalsAsciiCaseInsensitive(val, "true") ||
           equalsAsciiCaseInsensitive(val, "yes") ||
           equalsAsciiCaseInsensitive(val, "on");
  }

  [[nodiscard]] static bool isFalsy(std::string_view val) noexcept {
    return val == "0" || equalsAsciiCaseInsensitive(val, "false") ||
           equalsAsciiCaseInsensitive(val, "no") ||
           equalsAsciiCaseInsensitive(val, "off");
  }

  // Conservative unsupported-construct detection: aggregation, HAVING, and
  // ORDER BY need materialized grouping/sorting that the streaming engine
  // cannot provide, so such queries fall back to Legacy V1 (the safe
  // direction: over-approximation only loses the fast path, never correctness).
  // TODO(WP-followup): extend to SERVICE clauses, subqueries, property paths,
  // MINUS, and FILTER-streamability before the WP that wires `selectEngine`
  // into `src/` callers. Not `noexcept`: the aggregate walk can throw via
  // `AD_CORRECTNESS_CHECK`, so neither is the eligibility chain above.
  [[nodiscard]] static bool hasUnsupportedConstructs(const ParsedQuery& query) {
    if (query.isAggregatingQuery() || !query._havingClauses.empty() ||
        !query._orderBy.empty()) {
      return true;
    }
    return false;
  }
};

}  // namespace ql::engine

#endif  // QLEVER_SRC_ENGINE_EXPORTPIPELINEROUTER_H
