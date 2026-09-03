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

#include <optional>
#include <string>
#include <string_view>

#include "parser/ParsedQuery.h"
#include "util/Invariants.h"
#include "util/StringUtils.h"
#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"

namespace ql::engine {

// _____________________________________________________________________________
// Execution engine mode for query results and data exports.
enum class ExportEngineMode {
  LegacyV1 = 0,     // Proven pull-based Volcano iterator using IdTable
  FastStreamingV2 = 1  // Push-based zero-copy streaming engine
};

// Return a human-readable representation of `ExportEngineMode`.
[[nodiscard]] constexpr std::string_view toString(ExportEngineMode mode) noexcept {
  switch (mode) {
    case ExportEngineMode::LegacyV1:
      return "LegacyV1";
    case ExportEngineMode::FastStreamingV2:
      return "FastStreamingV2";
  }
  return "Unknown";
}

// _____________________________________________________________________________
// Deep module routing incoming SPARQL requests between the standard relational
// execution pipeline (Legacy V1) and the specialized push-based streaming
// export engine (Fast-Path V2).
//
// Adheres to the 7 Universal Laws:
// - Law 1: Deep Module (concise public interface, encapsulated plan analysis)
// - Law 2: Zero Bookkeeping Leakage (caller never manages routing internals)
// - Law 4: Defining Errors Out of Existence (unsupported shapes safely fallback)
class ExportPipelineRouter {
 public:
  using ParamValueMap = ad_utility::url_parser::ParamValueMap;

  // ___________________________________________________________________________
  // Determine the appropriate export engine mode based on request metadata,
  // query AST eligibility, and server configuration defaults.
  [[nodiscard]] static ExportEngineMode selectEngine(
      const ParsedQuery& query,
      const ParamValueMap& parameters,
      std::optional<std::string_view> exportHeader = std::nullopt,
      ExportEngineMode serverDefault = ExportEngineMode::LegacyV1) noexcept {

    // 1. Check explicit query parameter overrides
    const auto optFastExport = getParameterValue(parameters, "fast-export");
    const auto optExportEngine = getParameterValue(parameters, "export-engine");

    if (optFastExport.has_value()) {
      if (isTruthy(optFastExport.value())) {
        return evaluateEligibility(query, ExportEngineMode::FastStreamingV2);
      } else if (isFalsy(optFastExport.value())) {
        return ExportEngineMode::LegacyV1;
      }
    }

    if (optExportEngine.has_value()) {
      const auto optVal = ad_utility::getLowercase(optExportEngine.value());
      if (optVal == "v2" || optVal == "fast") {
        return evaluateEligibility(query, ExportEngineMode::FastStreamingV2);
      } else if (optVal == "v1" || optVal == "legacy") {
        return ExportEngineMode::LegacyV1;
      }
    }

    // 2. Check explicit HTTP Header override (e.g. X-QLever-Export-Engine: v2)
    if (exportHeader.has_value()) {
      const auto headerVal = ad_utility::getLowercase(exportHeader.value());
      if (headerVal == "v2" || headerVal == "fast" || headerVal == "streaming") {
        return evaluateEligibility(query, ExportEngineMode::FastStreamingV2);
      } else if (headerVal == "v1" || headerVal == "legacy") {
        return ExportEngineMode::LegacyV1;
      }
    }

    // 3. Check server-wide default mode
    if (serverDefault == ExportEngineMode::FastStreamingV2) {
      return evaluateEligibility(query, ExportEngineMode::FastStreamingV2);
    }

    return ExportEngineMode::LegacyV1;
  }

  // ___________________________________________________________________________
  // Inspect the `ParsedQuery` AST to determine whether it is eligible for `FastStreamingV2`.
  // Return true for standard scan, join, projection, and construct queries.
  // Return false for queries containing unsupported constructs (e.g. distributed
  // federated queries or complex custom service endpoints).
  [[nodiscard]] static bool isEligibleForFastStreaming(const ParsedQuery& query) noexcept {
    // CONSTRUCT and SELECT queries are currently eligible.
    if (query._clause.isConstructClause() || query._clause.isSelectClause()) {
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
      const ParsedQuery& query,
      const ParamValueMap& parameters,
      std::optional<std::string_view> exportHeader = std::nullopt,
      ExportEngineMode serverDefault = ExportEngineMode::LegacyV1) {
    ExportEngineMode selected = selectEngine(query, parameters, exportHeader, serverDefault);
    bool eligible = isEligibleForFastStreaming(query);

    std::string reason;
    if (selected == ExportEngineMode::FastStreamingV2) {
      reason = "Fast-Path V2 selected (eligible export query with explicit or default opt-in)";
    } else {
      const auto optFastExport = getParameterValue(parameters, "fast-export");
      const auto optExportEngine = getParameterValue(parameters, "export-engine");

      bool explicitlyRequestedV2 = false;
      bool explicitlyRequestedV1 = false;

      if (optFastExport.has_value()) {
        if (isTruthy(optFastExport.value())) {
          explicitlyRequestedV2 = true;
        } else if (isFalsy(optFastExport.value())) {
          explicitlyRequestedV1 = true;
        }
      }

      if (optExportEngine.has_value()) {
        const auto val = ad_utility::getLowercase(optExportEngine.value());
        if (val == "v2" || val == "fast") {
          explicitlyRequestedV2 = true;
        } else if (val == "v1" || val == "legacy") {
          explicitlyRequestedV1 = true;
        }
      }

      if (exportHeader.has_value()) {
        const auto val = ad_utility::getLowercase(exportHeader.value());
        if (val == "v2" || val == "fast" || val == "streaming") {
          explicitlyRequestedV2 = true;
        } else if (val == "v1" || val == "legacy") {
          explicitlyRequestedV1 = true;
        }
      }

      if (explicitlyRequestedV2 && !eligible) {
        reason = "Fallback to Legacy V1 (fast-path requested but query is ineligible for V2 streaming)";
      } else if (explicitlyRequestedV1) {
        reason = "Legacy V1 selected (explicitly requested via query parameter or header override)";
      } else if (serverDefault == ExportEngineMode::FastStreamingV2 && !eligible) {
        reason = "Fallback to Legacy V1 (server default is V2 but query is ineligible for V2 streaming)";
      } else {
        reason = "Legacy V1 selected (default standard relational pipeline)";
      }
    }

    return absl::StrCat("ExportEngine: ", toString(selected), " [Reason: ", reason, "]");
  }

 private:
  // Heterogeneous, zero-allocation parameter lookup on ParamValueMap.
  [[nodiscard]] static std::optional<std::string_view> getParameterValue(
      const ParamValueMap& parameters,
      std::string_view key) noexcept {
    auto it = parameters.find(key);
    if (it != parameters.end() && !it->second.empty()) {
      return it->second.front();
    }
    return std::nullopt;
  }

  [[nodiscard]] static ExportEngineMode evaluateEligibility(
      const ParsedQuery& query, ExportEngineMode targetMode) noexcept {
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
    auto lower = ad_utility::getLowercase(val);
    return lower == "1" || lower == "true" || lower == "yes" || lower == "on";
  }

  [[nodiscard]] static bool isFalsy(std::string_view val) noexcept {
    auto lower = ad_utility::getLowercase(val);
    return lower == "0" || lower == "false" || lower == "no" || lower == "off";
  }

  // Unsupported-construct detection is not implemented yet; all SELECT and
  // CONSTRUCT queries are currently treated as eligible for the fast path.
  [[nodiscard]] static bool hasUnsupportedConstructs(const ParsedQuery&) noexcept {
    return false;
  }
};

}  // namespace ql::engine

#endif  // QLEVER_SRC_ENGINE_EXPORTPIPELINEROUTER_H
