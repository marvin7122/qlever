// Copyright 2026, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_COUNTSTARCARDINALITY_H
#define QLEVER_SRC_ENGINE_COUNTSTARCARDINALITY_H

#include <cstddef>
#include <optional>

class QueryExecutionTree;

// Exact `COUNT(*)` cardinalities derived from index metadata, without
// evaluating the child operation. Handles a bag `UNION` of two eligible
// index scans (sum of exact sizes), an `OPTIONAL` of two eligible scans (left
// multiplicities times `max(1, right)` per join key, empty right side
// included), a `MINUS` of two eligible scans (left keys absent on the right),
// a correlated `FILTER EXISTS` over two eligible scans (left keys present on
// the right), and an inner join of eligible bound-predicate scans on a single
// common variable (two-scan join or k-star: sum of multiplicity products from
// distinct col1 counts). Returns `std::nullopt` when the root operation has
// any other shape or a precondition fails, in which case the caller falls back
// to counting the materialized child result.
std::optional<size_t> computeCountStarCardinality(
    const QueryExecutionTree& tree);

#endif  // QLEVER_SRC_ENGINE_COUNTSTARCARDINALITY_H
