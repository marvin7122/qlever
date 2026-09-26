// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ExportPipelineRouter.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <initializer_list>
#include <variant>

#include "backports/algorithm.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"

namespace ql::engine {

namespace {

// _____________________________________________________________________________
// Return true if `value` equals one of `candidates`, ignoring ASCII case. Does
// not allocate.
bool equalsAnyIgnoreCase(std::string_view value,
                         std::initializer_list<std::string_view> candidates) {
  return ql::ranges::any_of(candidates, [value](std::string_view candidate) {
    return absl::EqualsIgnoreCase(value, candidate);
  });
}

// _____________________________________________________________________________
bool isTruthy(std::string_view value) {
  return equalsAnyIgnoreCase(value, {"1", "true", "yes", "on"});
}

// _____________________________________________________________________________
bool isFalsy(std::string_view value) {
  return equalsAnyIgnoreCase(value, {"0", "false", "no", "off"});
}

// _____________________________________________________________________________
// Return the first value of the URL parameter `key`, or `std::nullopt` if it
// is not set. When a key carries multiple values (for example
// `?fast-export=true&fast-export=false`), the first value wins, unlike in
// `getParameterCheckAtMostOnce` in `UrlParser.h`, which throws. The returned
// view points into `parameters`.
std::optional<std::string_view> getFirstParameterValue(
    const ExportPipelineRouter::ParamValueMap& parameters,
    std::string_view key) {
  auto it = parameters.find(key);
  if (it == parameters.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second.front();
}

// _____________________________________________________________________________
// Return true if `pattern` (recursing into plain groups) contains a `BIND`
// operation. Used to detect scalar `SELECT` aliases, which the parser rewrites
// to `BIND` (see `ExportPipelineRouter::hasUnsupportedConstructs`).
bool graphPatternContainsBind(const parsedQuery::GraphPattern& pattern) {
  namespace pq = parsedQuery;
  return ql::ranges::any_of(pattern._graphPatterns, [](const auto& operation) {
    if (std::holds_alternative<pq::Bind>(operation)) {
      return true;
    }
    return std::holds_alternative<pq::GroupGraphPattern>(operation) &&
           graphPatternContainsBind(
               std::get<pq::GroupGraphPattern>(operation)._child);
  });
}

// _____________________________________________________________________________
// Return true if `pattern` (including its FILTER and BIND expressions and its
// nested groups) contains anything that V2 does not support.
bool graphPatternHasUnsupportedConstructs(
    const parsedQuery::GraphPattern& pattern);

// _____________________________________________________________________________
// Return true if `operation` or anything nested in it is not supported by V2.
// Only `BasicGraphPattern` without property paths, `Bind` without `EXISTS`,
// `Values`, and plain (non-GRAPH) groups of these are supported.
bool operationIsUnsupported(
    const parsedQuery::GraphPatternOperation& operation) {
  namespace pq = parsedQuery;
  if (std::holds_alternative<pq::GroupGraphPattern>(operation)) {
    const auto& group = std::get<pq::GroupGraphPattern>(operation);
    return !std::holds_alternative<std::monostate>(group.graphSpec_) ||
           graphPatternHasUnsupportedConstructs(group._child);
  }
  if (std::holds_alternative<pq::Bind>(operation)) {
    // `EXISTS` carries a nested query and fails closed like a subquery.
    return !std::get<pq::Bind>(operation)
                ._expression.getExistsExpressions()
                .empty();
  }
  if (std::holds_alternative<pq::BasicGraphPattern>(operation)) {
    // Property paths (e.g. `?s <p>+ ?o`) need the transitive-path machinery
    // that the V2 engine does not implement yet. Plain IRIs and predicate
    // variables stay eligible.
    return ql::ranges::any_of(
        std::get<pq::BasicGraphPattern>(operation)._triples,
        [](const SparqlTriple& triple) {
          return std::holds_alternative<PropertyPath>(triple.p_) &&
                 !std::get<PropertyPath>(triple.p_).isIri();
        });
  }
  return !std::holds_alternative<pq::Values>(operation);
}

// _____________________________________________________________________________
bool graphPatternHasUnsupportedConstructs(
    const parsedQuery::GraphPattern& pattern) {
  const bool hasFilterExists =
      ql::ranges::any_of(pattern._filters, [](const SparqlFilter& filter) {
        return !filter.expression_.getExistsExpressions().empty();
      });
  return hasFilterExists ||
         ql::ranges::any_of(pattern._graphPatterns, &operationIsUnsupported);
}

}  // namespace

// _____________________________________________________________________________
ExportEngineMode ExportPipelineRouter::selectEngine(
    const ParsedQuery& query, const ParamValueMap& parameters,
    std::optional<std::string_view> exportHeader,
    ExportEngineMode serverDefault) {
  switch (parseExplicitRequest(parameters, exportHeader)) {
    case ExplicitEngineRequest::WantV2:
      return fastStreamingIfEligible(query);
    case ExplicitEngineRequest::WantV1:
      return ExportEngineMode::LegacyV1;
    case ExplicitEngineRequest::None:
      break;
  }
  if (serverDefault == ExportEngineMode::FastStreamingV2) {
    return fastStreamingIfEligible(query);
  }
  return ExportEngineMode::LegacyV1;
}

// _____________________________________________________________________________
bool ExportPipelineRouter::isEligibleForFastStreaming(
    const ParsedQuery& query) {
  if (!query.hasConstructClause() && !query.hasSelectClause()) {
    return false;
  }
  return !hasUnsupportedConstructs(query);
}

// _____________________________________________________________________________
std::string ExportPipelineRouter::describeDecision(
    const ParsedQuery& query, const ParamValueMap& parameters,
    std::optional<std::string_view> exportHeader,
    ExportEngineMode serverDefault) {
  const ExportEngineMode selected =
      selectEngine(query, parameters, exportHeader, serverDefault);
  const ExplicitEngineRequest request =
      parseExplicitRequest(parameters, exportHeader);
  const bool eligible = isEligibleForFastStreaming(query);

  std::string_view reason;
  if (selected == ExportEngineMode::FastStreamingV2) {
    reason =
        "Fast-Path V2 selected (eligible export query with explicit or "
        "default opt-in)";
  } else if (request == ExplicitEngineRequest::WantV2) {
    reason =
        "Fallback to Legacy V1 (fast-path requested but query is ineligible "
        "for V2 streaming)";
  } else if (request == ExplicitEngineRequest::WantV1) {
    reason =
        "Legacy V1 selected (explicitly requested via query parameter or "
        "header override)";
  } else if (serverDefault == ExportEngineMode::FastStreamingV2 && !eligible) {
    reason =
        "Fallback to Legacy V1 (server default is V2 but query is ineligible "
        "for V2 streaming)";
  } else {
    reason = "Legacy V1 selected (default standard relational pipeline)";
  }
  return absl::StrCat("ExportEngine: ", toString(selected),
                      " [Reason: ", reason, "]");
}

// _____________________________________________________________________________
ExplicitEngineRequest ExportPipelineRouter::parseExplicitRequest(
    const ParamValueMap& parameters,
    std::optional<std::string_view> exportHeader) {
  if (exportHeader.has_value()) {
    if (equalsAnyIgnoreCase(exportHeader.value(),
                            {"v2", "fast", "streaming"})) {
      return ExplicitEngineRequest::WantV2;
    }
    if (equalsAnyIgnoreCase(exportHeader.value(), {"v1", "legacy"})) {
      return ExplicitEngineRequest::WantV1;
    }
  }

  if (auto fastExport = getFirstParameterValue(parameters, "fast-export")) {
    if (isTruthy(fastExport.value())) {
      return ExplicitEngineRequest::WantV2;
    }
    if (isFalsy(fastExport.value())) {
      return ExplicitEngineRequest::WantV1;
    }
  }

  if (auto engine = getFirstParameterValue(parameters, "export-engine")) {
    if (equalsAnyIgnoreCase(engine.value(), {"v2", "fast"})) {
      return ExplicitEngineRequest::WantV2;
    }
    if (equalsAnyIgnoreCase(engine.value(), {"v1", "legacy"})) {
      return ExplicitEngineRequest::WantV1;
    }
  }

  return ExplicitEngineRequest::None;
}

// _____________________________________________________________________________
ExportEngineMode ExportPipelineRouter::fastStreamingIfEligible(
    const ParsedQuery& query) {
  return isEligibleForFastStreaming(query) ? ExportEngineMode::FastStreamingV2
                                           : ExportEngineMode::LegacyV1;
}

// _____________________________________________________________________________
bool ExportPipelineRouter::hasUnsupportedConstructs(const ParsedQuery& query) {
  // Solution modifiers that require blocking operators or aggregation.
  if (!query._groupByVariables.empty() || !query._havingClauses.empty() ||
      !query._orderBy.empty()) {
    return true;
  }
  // The V2 engine reads the implicit default graph.
  if (!query.datasetClauses_.isUnconstrainedOrWithClause()) {
    return true;
  }
  if (query.hasSelectClause()) {
    const auto& selectClause = query.selectClause();
    // Aliases cover aggregate select expressions and GROUP BY queries for
    // now; DISTINCT and REDUCED require post-hoc deduplication state.
    if (selectClause.distinct_ || selectClause.reduced_ ||
        !selectClause.getAliases().empty()) {
      return true;
    }
    // Scalar `SELECT` aliases like `SELECT (?o AS ?x)` are rewritten to
    // `BIND` during parsing (`ParsedQuery::addSolutionModifiers`), so the
    // check above cannot see them. Projecting a computed binding needs V2
    // projection support that does not exist yet, hence fail closed.
    // Plain `SELECT * ... BIND ...` stays eligible.
    if (!selectClause.isAsterisk() &&
        graphPatternContainsBind(query._rootGraphPattern)) {
      return true;
    }
  }
  return graphPatternHasUnsupportedConstructs(query._rootGraphPattern);
}

}  // namespace ql::engine
