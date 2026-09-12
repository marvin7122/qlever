// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_JITSTRINGFOLD_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_JITSTRINGFOLD_H

#include <optional>

#include "engine/VariableToColumnMap.h"
#include "engine/sparqlExpressions/JitExpressionBytecodeVm.h"

class Index;

namespace sparqlExpression {
class SparqlExpression;
}

namespace ql::engine::jit {

// Try to compile a FILTER expression with constant string patterns into a
// `JitBytecodeProgram` by resolving the constants against the vocabulary of
// `index` once at planning time (String Optimization Cases A and B of the
// expression JIT master specification):
// * `?var = <IRI or literal constant>`: the constant is resolved to a single
//   `Id` (vocabulary entry or inlined value like an encoded IRI) and the
//   check becomes a raw `ValueId` bit comparison (`CMP_EQ_ID`). This is
//   exactly equivalent to the legacy evaluation, which resolves literal
//   constants via `toValueId` and compares IDs by bits. Only `EQ` is folded:
//   for `NE`, rows with undefined values are dropped by the legacy
//   evaluation, which a bitwise comparison cannot express.
// * `REGEX(?var, "^prefix")` (a `PrefixRegexExpression`): the prefix is
//   lowered to vocabulary ranges (`IN_ID_RANGE`, combined with `OR_BOOL`),
//   mirroring `PrefixRegexExpression::evaluate` including the `<`-prefixed
//   range for `STR()`-wrapped variables.
// Return `std::nullopt` if the expression is not a supported pattern, if a
// variable is unbound, or if a constant cannot be resolved without eagerly
// extending the local vocabulary (the caller then falls back to the generic
// JIT or legacy evaluation, which remain correct).
[[nodiscard]] std::optional<JitBytecodeProgram> tryFoldStringFilterToJit(
    const sparqlExpression::SparqlExpression& expr,
    const VariableToColumnMap& varColMap, const Index& index);

}  // namespace ql::engine::jit

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_JITSTRINGFOLD_H
