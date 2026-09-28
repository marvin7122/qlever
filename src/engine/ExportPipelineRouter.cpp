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
// Return true if `pattern` (recursing into nested groups) contains a `BIND`
// operation. Use it to detect scalar `SELECT` aliases, which the parser
// rewrites to `BIND` (see `ExportPipelineRouter::hasUnsupportedConstructs`).
bool graphPatternContainsBind(
    const parsedQuery::GraphPattern& pattern) noexcept {
  namespace pq = parsedQuery;
  return ql::ranges::any_of(pattern._graphPatterns, [](const auto& operation) {
    return std::holds_alternative<pq::Bind>(operation) ||
           (std::holds_alternative<pq::GroupGraphPattern>(operation) &&
            graphPatternContainsBind(
                std::get<pq::GroupGraphPattern>(operation)._child));
  });
}

// _____________________________________________________________________________
// Return true if `pattern` (including `EXISTS` inside its `FILTER` and `BIND`
// expressions, and its nested groups) contains anything that V2 does not
// support. Plain `FILTER`/`BIND` without `EXISTS` stay eligible.
bool graphPatternHasUnsupportedConstructs(
    const parsedQuery::GraphPattern& pattern);

// _____________________________________________________________________________
// TODO<Marvin Stoetzel>: support OPTIONAL, UNION, MINUS, SERVICE, subqueries,
// GRAPH, DESCRIBE, and proper property paths in V2 (currently fail closed to
// V1).
// Return true if `operation` or anything nested in it is not supported by V2.
// Only `BasicGraphPattern` without proper property paths, `Bind` without
// `EXISTS`, `Values`, and plain (non-`GRAPH`) groups of these are supported.
bool operationIsUnsupported(
    const parsedQuery::GraphPatternOperation& operation) {
  namespace pq = parsedQuery;
  if (std::holds_alternative<pq::GroupGraphPattern>(operation)) {
    const auto& group = std::get<pq::GroupGraphPattern>(operation);
    // A plain group holds `std::monostate` in `graphSpec_`; anything else is
    // a GRAPH clause, which needs named-graph routing that V2 lacks.
    return !std::holds_alternative<std::monostate>(group.graphSpec_) ||
           graphPatternHasUnsupportedConstructs(group._child);
  }
  if (std::holds_alternative<pq::Bind>(operation)) {
    // Reject `EXISTS`: it carries a nested query, so fail closed like a
    // subquery.
    return !std::get<pq::Bind>(operation)
                ._expression.getExistsExpressions()
                .empty();
  }
  if (std::holds_alternative<pq::BasicGraphPattern>(operation)) {
    // Reject any proper property path (not just transitive closure): V2 does
    // not implement the path machinery yet. Plain IRIs
    // (`PropertyPath::isIri()`, which also wraps simple IRIs via `fromIri`)
    // and predicate variables stay eligible.
    return ql::ranges::any_of(
        std::get<pq::BasicGraphPattern>(operation)._triples,
        [](const SparqlTriple& triple) {
          // `Variable` predicates never match this branch and stay eligible.
          return std::holds_alternative<PropertyPath>(triple.p_) &&
                 !std::get<PropertyPath>(triple.p_).isIri();
        });
  }
  // Fail-closed allowlist: `Values` is the only other supported operation, so
  // everything else (OPTIONAL, UNION, MINUS, SERVICE, subqueries, DESCRIBE,
  // text/spatial search, ...) is routed to V1, including future alternatives.
  return !std::holds_alternative<pq::Values>(operation);
}

// _____________________________________________________________________________
bool graphPatternHasUnsupportedConstructs(
    const parsedQuery::GraphPattern& pattern) {
  // Reject `EXISTS` in filters just like in `Bind` above: it carries a
  // nested query that V2 cannot evaluate.
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
  // Reject solution modifiers that require blocking operators or aggregation
  // (`ParsedQuery::_groupByVariables`, `_havingClauses`, `_orderBy`).
  if (!query._groupByVariables.empty() || !query._havingClauses.empty() ||
      !query._orderBy.empty()) {
    return true;
  }
  // V2 only reads the implicit default graph: reject `FROM`/`FROM NAMED`
  // (`DatasetClauses::isUnconstrainedOrWithClause()` is false). A `WITH`
  // clause stays eligible (it counts as unconstrained).
  if (!query.datasetClauses_.isUnconstrainedOrWithClause()) {
    return true;
  }
  if (query.hasSelectClause()) {
    const auto& selectClause = query.selectClause();
    // TODO<Marvin Stoetzel>: support `DISTINCT`/`REDUCED` deduplication and
    // `SELECT`-expression projection in V2.
    // Aliases cover aggregate `SELECT` expressions and `GROUP BY` queries for
    // now; `DISTINCT` and `REDUCED` require post-hoc deduplication state.
    if (selectClause.distinct_ || selectClause.reduced_ ||
        !selectClause.getAliases().empty()) {
      return true;
    }
    // Only scalar aliases without `GROUP BY` are rewritten to `BIND` during
    // parsing (`ParsedQuery::addSolutionModifiers` appends them to the root
    // graph pattern); grouped/aggregate aliases stay in `getAliases()` and
    // are rejected above. Projecting any computed `BIND` (alias-derived or
    // user-written) with an explicit projection (`!isAsterisk()`) needs V2
    // projection support that does not exist yet, hence fail closed. Plain
    // `SELECT * ... BIND ...` (without `EXISTS`) stays eligible.
    if (!selectClause.isAsterisk() &&
        graphPatternContainsBind(query._rootGraphPattern)) {
      return true;
    }
  }
  return graphPatternHasUnsupportedConstructs(query._rootGraphPattern);
}

}  // namespace ql::engine
