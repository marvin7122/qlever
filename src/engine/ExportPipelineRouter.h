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

#include "util/StringUtils.h"

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
    case ExportEngineMode::FastStreamingV2:
      return "LegacyV1";
    case ExportEngineMode::FastStreamingV2:
      return "FastStreamingV2";
  }
  return "Unknown";
}

// _____________________________________________________________________________
// Deep module routing incoming SPARQL requests between the standard relational
// execution pipeline (Legacy V1) and the specialized push-based streaming

//
// Adheres to the 7 Universal Laws:
// - Law 1: Deep Module (concise public interface, encapsulated plan analysis)
// - Law 2: Zero Bookkeeping Leakage (caller never manages routing internals)
// - Law 4: Defining Errors Out of Existence (unsupported shapes safely
// fall back)
class ExportPipelineRouter {
 public:
  using ParamValueMap = ad_utility::url_parser::ParamValueMap;

  
      ExportEngineMode serverDefault = ExportEngineMode::LegacyV1) noexcept {
    // 1. Check explicit query parameter overrides
    const auto optFastExport = getParameterValue(parameters, "fast-export");
    const auto optExportEngine = getParameterValue(parameters, "export-engine");

    if (optFastExport.has_value()) {
      if (isTruthy(optFastExport.value())) {
        return evaluateEligibility(query);
      }
      return ExportEngineMode::LegacyV1;
    }

    if (optExportEngine.has_value()) {
      const auto optVal =
          ad_utility::getLowercase(std::string(optExportEngine.value()));
      if (optVal == "v2" || optVal == "fast") {
        return evaluateEligibility(query, ExportEngineMode::FastStreamingV2);
      } else if (optVal == "v1" || optVal == "legacy") {
        return ExportEngineMode::LegacyV1;
      }
    }

    // 2. Check explicit HTTP Header override (e.g. X-QLever-Export-Engine: v2)
    if (exportHeader.has_value()) {
      const auto headerVal =
          ad_utility::getLowercase(std::string(exportHeader.value()));
      if (headerVal == "v2" || headerVal == "fast" ||
          headerVal == "streaming") {
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
    // Inspect the `ParsedQuery` to determine whether it is eligible for
  // `FastStreamingV2`. Return true for SELECT and CONSTRUCT queries that do
  // not contain unsupported constructs. Return false for ASK and DESCRIBE
  // queries or when unsupported constructs are detected.
  [[nodiscard]] static bool isEligibleForFastStreaming(
      const ParsedQuery& query) noexcept {
    // All CONSTRUCT and SELECT queries are currently eligible (unsupported-construct detection is not yet implemented).
    if (!(query.hasConstructClause() || query.hasSelectClause())) {
        // ASK and DESCRIBE currently use standard evaluation.
        return false;
    }
    return !hasUnsupportedConstructs(query);
  }

  // ___________________________________________________________________________
  // Return a detailed diagnostic string explaining the routing decision.
[[nodiscard]] static std::string describeDecision(
      const ParsedQuery& query, const ParamValueMap& parameters,
      std::optional<std::string_view> exportHeader = std::nullopt,
      ExportEngineMode serverDefault = ExportEngineMode::LegacyV1);

 private:
    // Look up `key` in `ParamValueMap` and return its value.
  [[nodiscard]] static std::optional<std::string> getParameterValue(
      const ParamValueMap& parameters, std::string_view key) noexcept {
    auto it = parameters.find(key);
    if (it != parameters.end() && !it->second.empty()) {
      return it->second.front();
    }
    return std::nullopt;
  }

  [[nodiscard]] static ExportEngineMode evaluateEligibility(
      const ParsedQuery& query) noexcept {
    if (isEligibleForFastStreaming(query)) {
      return ExportEngineMode::FastStreamingV2;
    }
    // Transparent fallback to Legacy V1
    return ExportEngineMode::LegacyV1;
  }

  [[nodiscard]] static ExportEngineMode evaluateEligibility(
      const ParsedQuery& query, ExportEngineMode requestedMode) noexcept {
    if (isEligibleForFastStreaming(query)) {
      return requestedMode;
    }
    // Transparent fallback to Legacy V1
    return ExportEngineMode::LegacyV1;
  }

  
    auto lower = ad_utility::getLowercase(std::string(val));
    return lower == "0" || lower == "false" || lower == "no" || lower == "off";
  }

  // Unsupported-construct detection is not implemented yet; all SELECT and
  // CONSTRUCT queries are currently treated as eligible for the fast path.
  // A SERVICE clause requires contacting a remote endpoint and streaming
  // results from it, which is incompatible with FastStreamingV2's zero-copy
  // push-based model until proper service forwarding is implemented.
  [[nodiscard]] static bool hasUnsupportedConstructs(
      const ParsedQuery& query) noexcept {
    return hasUnsupportedConstructsImpl(query.children());
  }

  // ___________________________________________________________________________
  // Traverse the GraphPattern recursively and return true if any
  // SERVICE clause is found. SERVICE clauses require a remote HTTP call and
  // are thus not compatible with FastStreamingV2's streaming architecture.
  [[nodiscard]] static bool hasUnsupportedConstructsImpl(
      const parsedQuery::GraphPattern& graphPattern) noexcept {
    for (const auto& operation : graphPattern._graphPatterns) {
      if (std::holds_alternative<parsedQuery::Service>(operation)) {
        return true;
      }
      // Recurse into nested graph patterns (Subquery, GroupGraphPattern,
      // Optional, Minus, Union, TransPath).
      if (std::holds_alternative<parsedQuery::Subquery>(operation)) {
        if (hasUnsupportedConstructsImpl(
                operation.get<parsedQuery::Subquery>().get().children())) {
          return true;
        }
      }
      if (std::holds_alternative<parsedQuery::GroupGraphPattern>(operation)) {
        if (hasUnsupportedConstructsImpl(
                operation.get<parsedQuery::GroupGraphPattern>()._child)) {
          return true;
        }
      }
      if (std::holds_alternative<parsedQuery::Optional>(operation)) {
        if (hasUnsupportedConstructsImpl(
                operation.get<parsedQuery::Optional>()._child)) {
          return true;
        }
      }
      if (std::holds_alternative<parsedQuery::Minus>(operation)) {
        const auto& minus = operation.get<parsedQuery::Minus>();
        if (hasUnsupportedConstructsImpl(minus._child)) {
          return true;
        }
      }
      if (std::holds_alternative<parsedQuery::Union>(operation)) {
        const auto& uni = operation.get<parsedQuery::Union>();
        if (hasUnsupportedConstructsImpl(uni._child1) ||
            hasUnsupportedConstructsImpl(uni._child2)) {
          return true;
        }
      }
      if (std::holds_alternative<parsedQuery::TransPath>(operation) &&
          hasUnsupportedConstructsImpl(
              operation.get<parsedQuery::TransPath>()._childGraphPattern)) {
        return true;
      }
    }
    return false;
  }
};

}  // namespace ql::engine

#endif  // QLEVER_SRC_ENGINE_EXPORTPIPELINEROUTER_H
