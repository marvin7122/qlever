// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <optional>
#include <vector>

#include "./util/IdTableHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/VariableToColumnMap.h"
#include "engine/sparqlExpressions/JitExpressionBytecodeVm.h"
#include "engine/sparqlExpressions/JitStringFold.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "global/Id.h"
#include "global/ValueId.h"
#include "index/TripleComponentConversions.h"
#include "parser/TripleComponent.h"
#include "util/IndexTestHelpers.h"

namespace {

using namespace sparqlExpression;
using ql::engine::jit::JitBytecodeProgram;
using ql::engine::jit::OpCode;
using ql::engine::jit::tryFoldStringFilterToJit;

// Build a small index with IRIs that share a prefix and plain literals.
Index makeStringFoldTestIndex() {
  return ad_utility::testing::makeTestIndex(
      "<http://example.org/alice> <http://example.org/name> \"Alice\" . "
      "<http://example.org/alicia> <http://example.org/name> \"Alicia\" . "
      "<http://example.org/bob> <http://example.org/name> \"Bob\" . ");
}

// Resolve a word via the test index, failing the test if it is missing.
Id idForWord(const Index& index, const TripleComponent& tc) {
  auto id = toValueId(tc, index);
  EXPECT_TRUE(id.has_value());
  return id.value();
}

VariableToColumnMap singleColumnMap(const Variable& var) {
  VariableToColumnMap map;
  map[var] = ColumnIndexAndTypeInfo{
      0, ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined};
  return map;
}

std::unique_ptr<VariableExpression> testVar() {
  return std::make_unique<VariableExpression>(Variable{"?x"});
}

// Execute a single-row program via the scalar interpreter. `bits` holds the
// raw `ValueId` bits of the single input column.
int64_t executeSingleRow(const JitBytecodeProgram& program, uint64_t bits) {
  std::vector<int64_t> row{static_cast<int64_t>(bits)};
  return program.execute(row);
}

}  // namespace

// `?x = <IRI>` folds to a raw ID comparison (Cases A and B).
TEST(JitStringFoldTest, EqualityWithIriConstant) {
  const Index index = makeStringFoldTestIndex();
  const Variable var{"?x"};
  const auto map = singleColumnMap(var);
  const Id alice =
      idForWord(index, TripleComponent{TripleComponent::Iri::fromIriref(
                           "<http://example.org/alice>")});
  const Id alicia =
      idForWord(index, TripleComponent{TripleComponent::Iri::fromIriref(
                           "<http://example.org/alicia>")});

  EqualExpression expr{{testVar(), std::make_unique<IriExpression>(
                                       TripleComponent::Iri::fromIriref(
                                           "<http://example.org/alice>"))}};
  auto program = tryFoldStringFilterToJit(expr, map, index);
  ASSERT_TRUE(program.has_value());
  EXPECT_EQ(executeSingleRow(program.value(), alice.getBits()), 1);
  EXPECT_EQ(executeSingleRow(program.value(), alicia.getBits()), 0);
  // Undefined and incompatible values never match, like the legacy
  // evaluation (which drops those rows).
  EXPECT_EQ(executeSingleRow(program.value(), Id::makeUndefined().getBits()),
            0);
  EXPECT_EQ(executeSingleRow(program.value(), Id::makeFromInt(42).getBits()),
            0);
}

// The variable may also be on the right-hand side.
TEST(JitStringFoldTest, EqualityWithSwappedArguments) {
  const Index index = makeStringFoldTestIndex();
  const Variable var{"?x"};
  const auto map = singleColumnMap(var);
  const Id bob = idForWord(
      index, TripleComponent{
                 TripleComponent::Iri::fromIriref("<http://example.org/bob>")});
  EqualExpression expr{
      {std::make_unique<IriExpression>(
           TripleComponent::Iri::fromIriref("<http://example.org/bob>")),
       testVar()}};
  auto program = tryFoldStringFilterToJit(expr, map, index);
  ASSERT_TRUE(program.has_value());
  EXPECT_EQ(executeSingleRow(program.value(), bob.getBits()), 1);
  EXPECT_EQ(executeSingleRow(program.value(), Id::makeUndefined().getBits()),
            0);
}

// `?x = "literal"` folds to the ID of the literal.
TEST(JitStringFoldTest, EqualityWithStringLiteral) {
  const Index index = makeStringFoldTestIndex();
  const Variable var{"?x"};
  const auto map = singleColumnMap(var);
  const Id aliceName = idForWord(
      index, TripleComponent{
                 ad_utility::testing::tripleComponentLiteral("\"Alice\"")});
  const Id aliciaName = idForWord(
      index, TripleComponent{
                 ad_utility::testing::tripleComponentLiteral("\"Alicia\"")});
  EqualExpression expr{
      {testVar(),
       std::make_unique<StringLiteralExpression>(
           ad_utility::testing::tripleComponentLiteral("\"Alice\""))}};
  auto program = tryFoldStringFilterToJit(expr, map, index);
  ASSERT_TRUE(program.has_value());
  EXPECT_EQ(executeSingleRow(program.value(), aliceName.getBits()), 1);
  EXPECT_EQ(executeSingleRow(program.value(), aliciaName.getBits()), 0);
}

// Already-resolved `Id` constants fold, except floating-point payloads (NaN
// must not compare equal to itself, unlike a bitwise comparison).
TEST(JitStringFoldTest, EqualityWithIdConstant) {
  const Index index = makeStringFoldTestIndex();
  const Variable var{"?x"};
  const auto map = singleColumnMap(var);
  EqualExpression intExpr{
      {testVar(), std::make_unique<IdExpression>(Id::makeFromInt(42))}};
  auto intProgram = tryFoldStringFilterToJit(intExpr, map, index);
  ASSERT_TRUE(intProgram.has_value());
  EXPECT_EQ(executeSingleRow(intProgram.value(), Id::makeFromInt(42).getBits()),
            1);
  EXPECT_EQ(executeSingleRow(intProgram.value(), Id::makeFromInt(43).getBits()),
            0);

  EqualExpression doubleExpr{
      {testVar(), std::make_unique<IdExpression>(Id::makeFromDouble(1.5))}};
  EXPECT_FALSE(tryFoldStringFilterToJit(doubleExpr, map, index).has_value());
}

// Unresolvable constants, `!=`, unbound variables, and non-patterns fall
// back to the generic evaluation.
TEST(JitStringFoldTest, UnsupportedPatternsFallBack) {
  const Index index = makeStringFoldTestIndex();
  const Variable var{"?x"};
  const auto map = singleColumnMap(var);
  // Unknown IRI: not in the vocabulary and not inline-encodable.
  EqualExpression unknownIri{
      {testVar(),
       std::make_unique<IriExpression>(
           TripleComponent::Iri::fromIriref("<http://example.org/unknown>"))}};
  EXPECT_FALSE(tryFoldStringFilterToJit(unknownIri, map, index).has_value());
  // Unknown literal.
  EqualExpression unknownLiteral{
      {testVar(),
       std::make_unique<StringLiteralExpression>(
           ad_utility::testing::tripleComponentLiteral("\"Unknown\""))}};
  EXPECT_FALSE(
      tryFoldStringFilterToJit(unknownLiteral, map, index).has_value());
  // `!=` is deliberately not folded (undefined rows are dropped, not kept).
  NotEqualExpression ne{{testVar(), std::make_unique<IriExpression>(
                                        TripleComponent::Iri::fromIriref(
                                            "<http://example.org/alice>"))}};
  EXPECT_FALSE(tryFoldStringFilterToJit(ne, map, index).has_value());
  // Unbound variable.
  EqualExpression unbound{
      {std::make_unique<VariableExpression>(Variable{"?y"}),
       std::make_unique<IriExpression>(
           TripleComponent::Iri::fromIriref("<http://example.org/alice>"))}};
  EXPECT_FALSE(tryFoldStringFilterToJit(unbound, map, index).has_value());
}

// `REGEX(?x, "^prefix")` folds to vocabulary range checks.
TEST(JitStringFoldTest, PrefixRegex) {
  const Index index = makeStringFoldTestIndex();
  const Variable var{"?x"};
  const auto map = singleColumnMap(var);
  const Id aliceName = idForWord(
      index, TripleComponent{
                 ad_utility::testing::tripleComponentLiteral("\"Alice\"")});
  const Id aliciaName = idForWord(
      index, TripleComponent{
                 ad_utility::testing::tripleComponentLiteral("\"Alicia\"")});
  const Id bobName = idForWord(
      index,
      TripleComponent{ad_utility::testing::tripleComponentLiteral("\"Bob\"")});
  PrefixRegexExpression regex{testVar(), "Ali", var};
  auto program = tryFoldStringFilterToJit(regex, map, index);
  ASSERT_TRUE(program.has_value());
  EXPECT_EQ(executeSingleRow(program.value(), aliceName.getBits()), 1);
  EXPECT_EQ(executeSingleRow(program.value(), aliciaName.getBits()), 1);
  EXPECT_EQ(executeSingleRow(program.value(), bobName.getBits()), 0);
  EXPECT_EQ(executeSingleRow(program.value(), Id::makeUndefined().getBits()),
            0);
}

// A prefix without any vocabulary entry matches nothing.
TEST(JitStringFoldTest, PrefixRegexWithoutMatch) {
  const Index index = makeStringFoldTestIndex();
  const Variable var{"?x"};
  const auto map = singleColumnMap(var);
  const Id aliceName = idForWord(
      index, TripleComponent{
                 ad_utility::testing::tripleComponentLiteral("\"Alice\"")});
  PrefixRegexExpression regex{testVar(), "Xyz", var};
  auto program = tryFoldStringFilterToJit(regex, map, index);
  ASSERT_TRUE(program.has_value());
  EXPECT_EQ(executeSingleRow(program.value(), aliceName.getBits()), 0);
}

// The folded program filters a whole `IdTable` like the legacy evaluation.
TEST(JitStringFoldTest, ExecuteFilterOverIdTable) {
  const Index index = makeStringFoldTestIndex();
  const Variable var{"?x"};
  const auto map = singleColumnMap(var);
  const Id alice =
      idForWord(index, TripleComponent{TripleComponent::Iri::fromIriref(
                           "<http://example.org/alice>")});
  const Id alicia =
      idForWord(index, TripleComponent{TripleComponent::Iri::fromIriref(
                           "<http://example.org/alicia>")});
  const Id bob = idForWord(
      index, TripleComponent{
                 TripleComponent::Iri::fromIriref("<http://example.org/bob>")});
  EqualExpression expr{{testVar(), std::make_unique<IriExpression>(
                                       TripleComponent::Iri::fromIriref(
                                           "<http://example.org/alice>"))}};
  auto program = tryFoldStringFilterToJit(expr, map, index);
  ASSERT_TRUE(program.has_value());

  IdTable input =
      makeIdTableFromVector({{alice}, {alicia}, {bob}, {Id::makeUndefined()}});
  IdTableStatic<1> result{ad_utility::testing::makeAllocator()};
  ql::engine::jit::JitExpressionBytecodeVm::executeFilter<1>(
      program.value(), input, result, nullptr);
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result(0, 0), alice);
}
