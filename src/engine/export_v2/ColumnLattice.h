// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/ValueId.h"
#include "parser/Alias.h"
#include "parser/ParsedQuery.h"
#include "rdfTypes/Variable.h"

namespace ql::engine::export_v2 {

// _____________________________________________________________________________
// Plan-time datatype lattice for one selected output column (Export V2 WP-A).
//
// A SPARQL column has no SQL-style schema: the same variable can hold IRIs,
// literals, numbers, and UNDEF in different rows. The lattice records what the
// query plan already proves about a column, so the serializer (WP-C) can skip
// its per-chunk `getDatatype()` uniformity scan when the answer is known:
//
// * `Int/Double/Bool/Date/GeoPoint` — every row holds this trivial `ValueId`.
//   Only produced for `BIND` constants today.
// * `Encoded` — every row holds an `EncodedVal`.
// * `Vocab` — values resolve through a vocabulary (IRIs, literals).
// * `Union` — mixed or unknown at plan time. The chunk decides at runtime,
//   exactly like the live V2 loop does today.
// * `Undef` — the column is unbound (`nullopt` in `selectedColumns`); the
//   serializer already emits no bytes for these.
//
// `BlankNodeIndex` and `Undefined` map to `Union` on purpose: the live loop
// treats uniform blank-node columns with the encoded formatter, so only the
// runtime check may pick that path until a bit-equal blank-node writer exists.
enum class ColumnLattice {
  Int,
  Double,
  Bool,
  Date,
  GeoPoint,
  Encoded,
  Vocab,
  Union,
  Undef
};

// Return a human-readable representation of `ColumnLattice`.
[[nodiscard]] constexpr std::string_view toString(
    ColumnLattice lattice) noexcept {
  switch (lattice) {
    case ColumnLattice::Int:
      return "Int";
    case ColumnLattice::Double:
      return "Double";
    case ColumnLattice::Bool:
      return "Bool";
    case ColumnLattice::Date:
      return "Date";
    case ColumnLattice::GeoPoint:
      return "GeoPoint";
    case ColumnLattice::Encoded:
      return "Encoded";
    case ColumnLattice::Vocab:
      return "Vocab";
    case ColumnLattice::Union:
      return "Union";
    case ColumnLattice::Undef:
      return "Undef";
  }
  return "Unknown";
}

// Immutable per-output-column lattice, parallel to `selectedColumns`.
struct ColumnLatticeResult {
  std::vector<ColumnLattice> columns_;
  [[nodiscard]] size_t size() const { return columns_.size(); }
  [[nodiscard]] ColumnLattice at(size_t outputColumn) const {
    return columns_.at(outputColumn);
  }
};

// Map one constant `ValueId` datatype onto the lattice.
[[nodiscard]] inline ColumnLattice latticeForDatatype(Datatype datatype) {
  using enum Datatype;
  switch (datatype) {
    case Int:
      return ColumnLattice::Int;
    case Double:
      return ColumnLattice::Double;
    case Bool:
      return ColumnLattice::Bool;
    case Date:
      return ColumnLattice::Date;
    case GeoPoint:
      return ColumnLattice::GeoPoint;
    case EncodedVal:
      return ColumnLattice::Encoded;
    case VocabIndex:
    case LocalVocabIndex:
    case SecondaryVocabIndex:
    case WordVocabIndex:
    case TextRecordIndex:
      return ColumnLattice::Vocab;
    case BlankNodeIndex:
    case Undefined:
      return ColumnLattice::Union;
  }
  // Unreachable while the switch covers every enumerator; a future datatype
  // conservatively becomes `Union` (runtime check) instead of a wrong proof.
  return ColumnLattice::Union;
}

// Lattice for one variable from the query plan: a `BIND` constant yields its
// datatype (e.g. `BIND(1 AS ?x)` is `Int`); anything else is `Union`.
[[nodiscard]] inline ColumnLattice latticeForVariable(
    const ParsedQuery& query, const Variable& variable) {
  for (const auto& alias : query.getAliases()) {
    if (alias._target == variable) {
      const auto* idExpression =
          dynamic_cast<const sparqlExpression::IdExpression*>(
              alias._expression.getPimpl());
      if (idExpression == nullptr) {
        return ColumnLattice::Union;
      }
      return latticeForDatatype(idExpression->value().getDatatype());
    }
  }
  return ColumnLattice::Union;
}

// Full output lattice for the SELECT list. `selectedColumns` parallels the
// selected variables (`std::nullopt` = unbound column = `Undef`).
[[nodiscard]] inline ColumnLatticeResult compileColumnLattice(
    const ParsedQuery& query,
    const QueryExecutionTree::ColumnIndicesAndTypes& selectedColumns) {
  ColumnLatticeResult result;
  result.columns_.reserve(selectedColumns.size());
  for (const auto& entry : selectedColumns) {
    if (!entry.has_value()) {
      result.columns_.push_back(ColumnLattice::Undef);
      continue;
    }
    result.columns_.push_back(
        latticeForVariable(query, Variable{entry.value().variable_}));
  }
  return result;
}

}  // namespace ql::engine::export_v2
