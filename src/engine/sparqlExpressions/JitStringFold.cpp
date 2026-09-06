// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "engine/sparqlExpressions/JitStringFold.h"

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "global/Id.h"
#include "global/ValueId.h"
#include "index/Index.h"
#include "index/TripleComponentConversions.h"
#include "parser/TripleComponent.h"

namespace ql::engine::jit {
namespace {

// Emit `column == idBits` for a bound variable column.
void emitIdEquality(JitBytecodeProgram& program, ColumnIndex column,
                    uint64_t idBits) {
  program.addInstruction(OpCode::LOAD_COL_ID, static_cast<int64_t>(column));
  program.addInstruction(OpCode::LOAD_CONST_INT, static_cast<int64_t>(idBits));
  program.addInstruction(OpCode::CMP_EQ_ID);
  program.addReferencedColumn(column);
}

// Resolve a constant IRI or literal against `index` without modifying the
// local vocabulary. Return the single `Id` if the constant is stored in the
// vocabulary or encoded directly in the ID (e.g. an encoded IRI), and
// `std::nullopt` otherwise (the caller falls back to legacy evaluation,
// which may resolve the constant via the local vocabulary).
std::optional<Id> resolveConstantToId(const TripleComponent& tripleComponent,
                                      const Index& index) {
  AD_CONTRACT_CHECK(!tripleComponent.isString());
  if (std::optional<Id> id =
          toValueIdIfNotString(tripleComponent, &index.encodedIriManager())) {
    return id;
  }
  AD_CORRECTNESS_CHECK(tripleComponent.isLiteral() || tripleComponent.isIri());
  const std::string& word =
      tripleComponent.isLiteral()
          ? tripleComponent.getLiteral().toStringRepresentation()
          : tripleComponent.getIri().toStringRepresentation();
  Index::Vocab::IndexType vocabIndex;
  if (index.getVocab().getId(word, &vocabIndex)) {
    return Id::makeFromVocabIndex(vocabIndex);
  }
  return std::nullopt;
}

// If `expr` is a literal expression holding a constant of type `T`, return
// its value, else `std::nullopt`.
template <typename T>
std::optional<T> getLiteralValue(
    const sparqlExpression::SparqlExpression& expr) {
  const auto* literal =
      dynamic_cast<const sparqlExpression::detail::LiteralExpression<T>*>(
          &expr);
  if (literal == nullptr) {
    return std::nullopt;
  }
  return literal->value();
}

// True if an ID resolved from a string/IRI constant is backed by the
// vocabulary or inline-encoded (and therefore bitwise-comparable): plain
// literals and IRIs resolve to `VocabIndex` or `EncodedVal`, never to a
// numeric payload.
bool isStringBackedId(Id id) {
  const auto datatype = id.getDatatype();
  return datatype == Datatype::VocabIndex || datatype == Datatype::EncodedVal;
}

// Try `?var = <constant>` (in either argument order). Only `EQ` is folded,
// see the header for why `NE` is excluded.
std::optional<JitBytecodeProgram> tryFoldEquality(
    const sparqlExpression::relational::RelationalExpression<
        sparqlExpression::relational::Comparison::EQ>& eq,
    const VariableToColumnMap& varColMap, const Index& index) {
  auto children = eq.children();
  for (size_t varSide = 0; varSide < 2; ++varSide) {
    const auto& varChild = children[varSide];
    const auto& constChild = children[1 - varSide];
    std::optional<Variable> variable = varChild->getVariableOrNullopt();
    if (!variable.has_value() || !varColMap.contains(variable.value())) {
      continue;
    }
    const ColumnIndex column = varColMap.at(variable.value()).columnIndex_;
    // IRI or string literal constant (String Optimization Cases A and B).
    // Only vocabulary-backed or inline-encoded IDs fold: a bitwise
    // comparison is exact for those (no legacy cross-type equality with
    // strings exists), while e.g. boolean or decimal literals would compare
    // numerically in the legacy evaluation.
    if (auto literal = getLiteralValue<TripleComponent::Literal>(*constChild)) {
      auto id = resolveConstantToId(TripleComponent{literal.value()}, index);
      if (!id.has_value() || !isStringBackedId(id.value())) {
        return std::nullopt;
      }
      JitBytecodeProgram program;
      emitIdEquality(program, column, id.value().getBits());
      program.addInstruction(OpCode::RET);
      program.setCellRule(CellRule::BitwiseExact);
      return program;
    }
    if (auto iri = getLiteralValue<TripleComponent::Iri>(*constChild)) {
      auto id = resolveConstantToId(TripleComponent{iri.value()}, index);
      if (!id.has_value() || !isStringBackedId(id.value())) {
        return std::nullopt;
      }
      JitBytecodeProgram program;
      emitIdEquality(program, column, id.value().getBits());
      program.addInstruction(OpCode::RET);
      program.setCellRule(CellRule::BitwiseExact);
      return program;
    }
    // Already-resolved `Id` constant. Floating-point payloads are excluded:
    // a bitwise comparison would consider two NaNs equal, while the legacy
    // evaluation does not. `Undefined` payloads are excluded because the
    // legacy evaluation drops them while a bitwise comparison would keep
    // `UNDEF` cells. `LocalVocabIndex` payloads are excluded because the
    // legacy evaluation compares them string-aware across vocabularies.
    // `Bool`, `Date` and `GeoPoint` payloads are conservatively excluded
    // (rare as filter constants). `Int` payloads fold with a
    // `FoldIntEquality` precondition (no `Double` cells); all
    // vocabulary-backed payloads are bitwise-exact.
    if (auto id = getLiteralValue<ValueId>(*constChild)) {
      const auto datatype = id.value().getDatatype();
      // `LocalVocabIndex` payloads are excluded like `Bool`: the legacy
      // evaluation compares them string-aware across vocabularies, unlike
      // a bitwise comparison.
      if (datatype == Datatype::Double || datatype == Datatype::GeoPoint ||
          datatype == Datatype::Bool || datatype == Datatype::Undefined ||
          datatype == Datatype::Date || datatype == Datatype::LocalVocabIndex) {
        return std::nullopt;
      }
      JitBytecodeProgram program;
      emitIdEquality(program, column, id.value().getBits());
      program.addInstruction(OpCode::RET);
      program.setCellRule(datatype == Datatype::Int ? CellRule::FoldIntEquality
                                                    : CellRule::BitwiseExact);
      return program;
    }
    return std::nullopt;
  }
  return std::nullopt;
}

// Try `REGEX(?var, "^prefix")` via vocabulary prefix ranges.
std::optional<JitBytecodeProgram> tryFoldPrefixRegex(
    const sparqlExpression::PrefixRegexExpression& regex,
    const VariableToColumnMap& varColMap, const Index& index) {
  const Variable& variable = regex.getVariable();
  if (!varColMap.contains(variable)) {
    return std::nullopt;
  }
  const ColumnIndex column = varColMap.at(variable).columnIndex_;
  // Same prefixes as `PrefixRegexExpression::evaluate`: the quoted literal
  // prefix, plus the `<`-prefixed IRI range if the variable is `STR()`-wrapped.
  std::vector<std::string> prefixes{"\"" + regex.getPrefix()};
  if (regex.isChildStrExpression()) {
    prefixes.push_back("<" + regex.getPrefix());
  }
  struct Range {
    uint64_t lo;
    uint64_t hi;
  };
  std::vector<Range> ranges;
  for (const auto& prefix : prefixes) {
    // NOTE: bind the returned `PrefixRanges` to a local: iterating
    // `index.prefixRanges(prefix).ranges()` directly would read through a
    // reference into a temporary that is destroyed after the range
    // initializer (undefined behavior).
    const auto prefixRange = index.prefixRanges(prefix);
    for (const auto& [begin, end] : prefixRange.ranges()) {
      const uint64_t lo = Id::makeFromVocabIndex(begin).getBits();
      const uint64_t hi = Id::makeFromVocabIndex(end).getBits();
      if (lo != hi) {
        ranges.push_back({lo, hi});
      }
    }
  }
  JitBytecodeProgram program;
  program.addReferencedColumn(column);
  if (ranges.empty()) {
    // No vocabulary entry carries the prefix: always false (like the legacy
    // evaluation, which finds no matching range).
    program.addInstruction(OpCode::LOAD_CONST_INT, 0);
  } else {
    for (size_t i = 0; i < ranges.size(); ++i) {
      program.addInstruction(OpCode::LOAD_COL_ID, static_cast<int64_t>(column));
      const size_t rangeIdx = program.addIdRange(ranges[i].lo, ranges[i].hi);
      program.addInstruction(OpCode::IN_ID_RANGE,
                             static_cast<int64_t>(rangeIdx));
      if (i > 0) {
        program.addInstruction(OpCode::OR_BOOL);
      }
    }
  }
  program.addInstruction(OpCode::RET);
  // Same bitwise range test as the legacy `PrefixRegexExpression::evaluate`
  // over the same vocabulary ranges: exact for every cell datatype.
  program.setCellRule(CellRule::BitwiseExact);
  return program;
}

}  // namespace

// _____________________________________________________________________________
std::optional<JitBytecodeProgram> tryFoldStringFilterToJit(
    const sparqlExpression::SparqlExpression& expr,
    const VariableToColumnMap& varColMap, const Index& index) {
  using sparqlExpression::relational::Comparison;
  using sparqlExpression::relational::RelationalExpression;
  if (const auto* eq =
          dynamic_cast<const RelationalExpression<Comparison::EQ>*>(&expr)) {
    return tryFoldEquality(*eq, varColMap, index);
  }
  if (const auto* regex =
          dynamic_cast<const sparqlExpression::PrefixRegexExpression*>(&expr)) {
    return tryFoldPrefixRegex(*regex, varColMap, index);
  }
  return std::nullopt;
}

}  // namespace ql::engine::jit
