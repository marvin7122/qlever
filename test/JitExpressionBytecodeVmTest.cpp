// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <vector>

#include "./util/RuntimeParametersTestHelpers.h"
#include "engine/Bind.h"
#include "engine/Filter.h"
#include "engine/ValuesForTesting.h"
#include "engine/sparqlExpressions/JitExpressionBytecodeVm.h"
#include "engine/sparqlExpressions/JitExpressionCompiler.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "index/LocalVocabEntry.h"
#include "parser/GraphPatternOperation.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

using namespace ql::engine::jit;

TEST(JitExpressionBytecodeVmTest, ArithmeticExpressionEvaluation) {
  // Expression: (col[0] * 2) + col[1] > 100
  JitBytecodeProgram program;
  program.addInstruction(OpCode::LOAD_COL_INT, 0);
  program.addInstruction(OpCode::LOAD_CONST_INT, 2);
  program.addInstruction(OpCode::MUL_INT);
  program.addInstruction(OpCode::LOAD_COL_INT, 1);
  program.addInstruction(OpCode::ADD_INT);
  program.addInstruction(OpCode::LOAD_CONST_INT, 100);
  program.addInstruction(OpCode::CMP_GT_INT);
  program.addInstruction(OpCode::RET);

  // Row 1: col0 = 40, col1 = 30 -> (40*2) + 30 = 110 > 100 -> true (1)
  std::vector<int64_t> row1 = {40, 30};
  EXPECT_EQ(program.execute(row1), 1);

  // Row 2: col0 = 30, col1 = 20 -> (30*2) + 20 = 80 > 100 -> false (0)
  std::vector<int64_t> row2 = {30, 20};
  EXPECT_EQ(program.execute(row2), 0);
}

TEST(JitExpressionBytecodeVmTest, AllArithmeticAndComparisonOpcodes) {
  // Subtraction & Comparison GE: (col[0] - col[1]) >= 15
  {
    JitBytecodeProgram program;
    program.addInstruction(OpCode::LOAD_COL_INT, 0);
    program.addInstruction(OpCode::LOAD_COL_INT, 1);
    program.addInstruction(OpCode::SUB_INT);
    program.addInstruction(OpCode::LOAD_CONST_INT, 15);
    program.addInstruction(OpCode::CMP_GE_INT);
    program.addInstruction(OpCode::RET);

    std::vector<int64_t> r1 = {30, 15};
    EXPECT_EQ(program.execute(r1), 1);
    std::vector<int64_t> r2 = {30, 16};
    EXPECT_EQ(program.execute(r2), 0);
  }

  // Division & Comparison LT: (col[0] / col[1]) < 5
  {
    JitBytecodeProgram program;
    program.addInstruction(OpCode::LOAD_COL_INT, 0);
    program.addInstruction(OpCode::LOAD_COL_INT, 1);
    program.addInstruction(OpCode::DIV_INT);
    program.addInstruction(OpCode::LOAD_CONST_INT, 5);
    program.addInstruction(OpCode::CMP_LT_INT);
    program.addInstruction(OpCode::RET);

    std::vector<int64_t> r1 = {12, 3};  // 4 < 5 -> true
    EXPECT_EQ(program.execute(r1), 1);
    std::vector<int64_t> r2 = {20, 4};  // 5 < 5 -> false
    EXPECT_EQ(program.execute(r2), 0);
    std::vector<int64_t> r3 = {20, 0};  // div by 0 -> 0 < 5 -> true
    EXPECT_EQ(program.execute(r3), 1);
  }

  // Modulo & Comparison EQ: (col[0] % col[1]) == 2
  {
    JitBytecodeProgram program;
    program.addInstruction(OpCode::LOAD_COL_INT, 0);
    program.addInstruction(OpCode::LOAD_COL_INT, 1);
    program.addInstruction(OpCode::MOD_INT);
    program.addInstruction(OpCode::LOAD_CONST_INT, 2);
    program.addInstruction(OpCode::CMP_EQ_INT);
    program.addInstruction(OpCode::RET);

    std::vector<int64_t> r1 = {17, 5};  // 17 % 5 = 2 == 2 -> true
    EXPECT_EQ(program.execute(r1), 1);
    std::vector<int64_t> r2 = {16, 5};  // 16 % 5 = 1 == 2 -> false
    EXPECT_EQ(program.execute(r2), 0);
  }

  // Comparison LE & NE: col[0] <= 10 && col[0] != 7
  {
    JitBytecodeProgram progLE;
    progLE.addInstruction(OpCode::LOAD_COL_INT, 0);
    progLE.addInstruction(OpCode::LOAD_CONST_INT, 10);
    progLE.addInstruction(OpCode::CMP_LE_INT);
    progLE.addInstruction(OpCode::RET);

    std::vector<int64_t> r1 = {10};
    EXPECT_EQ(progLE.execute(r1), 1);
    std::vector<int64_t> r2 = {11};
    EXPECT_EQ(progLE.execute(r2), 0);

    JitBytecodeProgram progNE;
    progNE.addInstruction(OpCode::LOAD_COL_INT, 0);
    progNE.addInstruction(OpCode::LOAD_CONST_INT, 7);
    progNE.addInstruction(OpCode::CMP_NE_INT);
    progNE.addInstruction(OpCode::RET);

    std::vector<int64_t> r3 = {8};
    EXPECT_EQ(progNE.execute(r3), 1);
    std::vector<int64_t> r4 = {7};
    EXPECT_EQ(progNE.execute(r4), 0);
  }
}

TEST(JitExpressionBytecodeVmTest, VectorMorselKernelExecution) {
  // Test 128 rows across two 64-row vector morsels
  constexpr size_t NUM_ROWS = 128;
  std::vector<int64_t> col0(NUM_ROWS);
  std::vector<int64_t> col1(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    col0[i] = static_cast<int64_t>(i);
    col1[i] = 50;
  }

  // Program: col[0] > col[1] (i.e. i > 50)
  JitBytecodeProgram program;
  program.addInstruction(OpCode::LOAD_COL_INT, 0);
  program.addInstruction(OpCode::LOAD_COL_INT, 1);
  program.addInstruction(OpCode::CMP_GT_INT);
  program.addInstruction(OpCode::RET);

  const int64_t* inputColumns[2] = {col0.data(), col1.data()};
  uint64_t outFilterMask[2] = {0, 0};

  JitExpressionBytecodeVm::executeVectorMorsel(program, inputColumns, NUM_ROWS,
                                               outFilterMask);

  // First morsel: i in [0..63]. Match condition i > 50 -> bits 51..63 set
  uint64_t expectedMorsel0 = 0;
  for (size_t i = 51; i < 64; ++i) {
    expectedMorsel0 |= (1ULL << i);
  }
  EXPECT_EQ(outFilterMask[0], expectedMorsel0);

  // Second morsel: i in [64..127]. All i > 50 -> all 64 bits set
  uint64_t expectedMorsel1 = ~0ULL;
  EXPECT_EQ(outFilterMask[1], expectedMorsel1);
}

TEST(JitExpressionBytecodeVmTest, AstLoweringAndFilterExecution) {
  using namespace sparqlExpression;
  auto I = ad_utility::testing::IntId;

  // AST: (?x * 2) + ?y > 100
  auto expr = std::make_unique<GreaterThanExpression>(
      std::array<SparqlExpression::Ptr, 2>{
          makeAddExpression(
              makeMultiplyExpression(
                  std::make_unique<VariableExpression>(Variable{"?x"}),
                  std::make_unique<IdExpression>(I(2))),
              std::make_unique<VariableExpression>(Variable{"?y"})),
          std::make_unique<IdExpression>(I(100))});

  VariableToColumnMap varColMap;
  varColMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  varColMap[Variable{"?y"}] = makeAlwaysDefinedColumn(1);

  auto optProgram = JitExpressionBytecodeVm::compile(*expr, varColMap);
  ASSERT_TRUE(optProgram.has_value());

  const auto& prog = optProgram.value();
  std::vector<int64_t> row1 = {40, 30};  // 40*2 + 30 = 110 > 100 -> true (1)
  EXPECT_EQ(prog.execute(row1), 1);
  std::vector<int64_t> row2 = {30, 20};  // 30*2 + 20 = 80 > 100 -> false (0)
  EXPECT_EQ(prog.execute(row2), 0);

  // End-to-end table filtering with executeFilter
  IdTable inputTable =
      makeIdTableFromVector({{40, 30}, {30, 20}, {45, 15}, {20, 10}}, I);
  IdTableStatic<2> resultTable =
      IdTable{2, ad_utility::makeUnlimitedAllocator<Id>()}.toStatic<2>();

  JitExpressionBytecodeVm::executeFilter<2>(prog, inputTable, resultTable);

  IdTable dynamicResult = std::move(resultTable).toDynamic();
  EXPECT_EQ(dynamicResult, makeIdTableFromVector({{40, 30}, {45, 15}}, I));
}

TEST(JitExpressionBytecodeVmTest, FilterOperationIntegration) {
  using namespace sparqlExpression;
  auto I = ad_utility::testing::IntId;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  // Input table with ?x and ?y
  IdTable inputTable =
      makeIdTableFromVector({{10, 5}, {20, 15}, {5, 2}, {30, 25}, {15, 8}}, I);

  ValuesForTesting values{qec,
                          std::move(inputTable),
                          {Variable{"?x"}, Variable{"?y"}},
                          false,
                          {},
                          LocalVocab{},
                          std::nullopt,
                          true};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};

  // Filter: (?x - ?y) > 8
  // Rows:
  // (10, 5) -> 5 > 8 (false)
  // (20, 15) -> 5 > 8 (false)
  // (5, 2) -> 3 > 8 (false)
  // (30, 25) -> 5 > 8 (false)
  // (15, 8) -> 7 > 8 (false)
  // Let's test (?x - ?y) > 4:
  // (10, 5) -> 5 > 4 (true)
  // (20, 15) -> 5 > 4 (true)
  // (5, 2) -> 3 > 4 (false)
  // (30, 25) -> 5 > 4 (true)
  // (15, 8) -> 7 > 4 (true)
  auto expr = std::make_unique<GreaterThanExpression>(
      std::array<SparqlExpression::Ptr, 2>{
          makeSubtractExpression(
              std::make_unique<VariableExpression>(Variable{"?x"}),
              std::make_unique<VariableExpression>(Variable{"?y"})),
          std::make_unique<IdExpression>(I(4))});

  Filter filter{qec,
                std::make_shared<QueryExecutionTree>(std::move(subTree)),
                {std::move(expr), "(?x - ?y) > 4"}};

  auto result = filter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());

  EXPECT_EQ(result->idTableView(),
            makeIdTableFromVector({{10, 5}, {20, 15}, {30, 25}, {15, 8}}, I));
}

TEST(JitExpressionBytecodeVmTest, NativeAsmJitFilterCompilationAndExecution) {
  using namespace sparqlExpression;
  auto I = ad_utility::testing::IntId;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  // Test with a larger table (100 rows) to test morsel processing
  VectorTable data;
  for (int64_t i = 0; i < 100; ++i) {
    data.push_back({i, i * 2});
  }
  IdTable inputTable = makeIdTableFromVector(data, I);

  ValuesForTesting values{qec,
                          std::move(inputTable),
                          {Variable{"?a"}, Variable{"?b"}},
                          false,
                          {},
                          LocalVocab{},
                          std::nullopt,
                          true};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};

  // Filter: (?a * 2) == ?b  (All 100 rows should match)
  auto expr =
      std::make_unique<EqualExpression>(std::array<SparqlExpression::Ptr, 2>{
          makeMultiplyExpression(
              std::make_unique<VariableExpression>(Variable{"?a"}),
              std::make_unique<IdExpression>(I(2))),
          std::make_unique<VariableExpression>(Variable{"?b"})});

  Filter filter{qec,
                std::make_shared<QueryExecutionTree>(std::move(subTree)),
                {std::move(expr), "(?a * 2) == ?b"}};

  auto result = filter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());
  EXPECT_EQ(result->idTableView().size(), 100u);
}

TEST(JitExpressionBytecodeVmTest, IdEqualityAndRangeOpcodes) {
  const uint64_t vocabBase =
      Id::makeFromVocabIndex(VocabIndex::make(100)).getBits();
  const uint64_t lo = vocabBase;
  const uint64_t hi = Id::makeFromVocabIndex(VocabIndex::make(200)).getBits();

  // col == const via raw ID bits.
  {
    JitBytecodeProgram program;
    program.addInstruction(OpCode::LOAD_COL_ID, 0);
    program.addInstruction(OpCode::LOAD_CONST_INT,
                           static_cast<int64_t>(vocabBase + 7));
    program.addInstruction(OpCode::CMP_EQ_ID);
    program.addInstruction(OpCode::RET);
    EXPECT_EQ(program.execute(
                  std::vector<int64_t>{static_cast<int64_t>(vocabBase + 7)}),
              1);
    EXPECT_EQ(program.execute(
                  std::vector<int64_t>{static_cast<int64_t>(vocabBase + 8)}),
              0);
    // Undefined (all-zero bits) never matches a defined constant.
    EXPECT_EQ(program.execute(std::vector<int64_t>{0}), 0);
  }

  // Half-open unsigned range check, combined with OR_BOOL.
  {
    JitBytecodeProgram program;
    const size_t range = program.addIdRange(lo, hi);
    program.addInstruction(OpCode::LOAD_COL_ID, 0);
    program.addInstruction(OpCode::IN_ID_RANGE, static_cast<int64_t>(range));
    program.addInstruction(OpCode::LOAD_COL_ID, 0);
    program.addInstruction(OpCode::IN_ID_RANGE, static_cast<int64_t>(range));
    program.addInstruction(OpCode::OR_BOOL);
    program.addInstruction(OpCode::RET);
    EXPECT_EQ(program.execute(std::vector<int64_t>{static_cast<int64_t>(lo)}),
              1);
    EXPECT_EQ(
        program.execute(std::vector<int64_t>{static_cast<int64_t>(hi - 1)}), 1);
    EXPECT_EQ(program.execute(std::vector<int64_t>{static_cast<int64_t>(hi)}),
              0);
    EXPECT_EQ(program.execute(std::vector<int64_t>{0}), 0);
  }
}

TEST(JitExpressionBytecodeVmTest, DivisionFallsBackToLegacyEvaluation) {
  // Regression test: the JIT backends implement truncating integer division,
  // but the legacy evaluation divides via doubles (see `DivideImpl`), so
  // programs containing `DIV_INT` must fall back. Rows where the true
  // quotient is nonzero but truncates to zero (1/2, 2/4) or infinite (1/0)
  // are kept by the legacy evaluation and must also be kept here.
  using namespace sparqlExpression;
  auto I = ad_utility::testing::IntId;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();
  // Pin division-by-zero to NaN/inf (rather than UNDEF): only then does the
  // legacy evaluation keep the `(1, 0)` row via +inf, so the test genuinely
  // verifies that the JIT falls back instead of truncating.
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::divisionByZeroIsUndef_>(
          false);

  IdTable inputTable =
      makeIdTableFromVector({{1, 2}, {2, 4}, {4, 2}, {1, 0}, {0, 5}}, I);
  ValuesForTesting values{qec,
                          std::move(inputTable),
                          {Variable{"?x"}, Variable{"?y"}},
                          false,
                          {},
                          LocalVocab{},
                          std::nullopt,
                          true};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};

  auto expr = makeDivideExpression(
      std::make_unique<VariableExpression>(Variable{"?x"}),
      std::make_unique<VariableExpression>(Variable{"?y"}));
  Filter filter{qec,
                std::make_shared<QueryExecutionTree>(std::move(subTree)),
                {std::move(expr), "?x / ?y"}};

  auto result = filter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  // `cleanup` must outlive the evaluation.
  (void)cleanup;
  ASSERT_TRUE(result->isFullyMaterialized());
  // All rows except (0, 5) have a nonzero (possibly infinite) quotient.
  EXPECT_EQ(result->idTableView(),
            makeIdTableFromVector({{1, 2}, {2, 4}, {4, 2}, {1, 0}}, I));
}

TEST(JitExpressionBytecodeVmTest, FoldIntEqualityFallsBackOnDoubles) {
  // Regression test: a folded `?x = <int>` compares raw `ValueId` bits,
  // but the legacy evaluation compares integers and doubles numerically
  // (`1.0 == 1`). The fold must only run when no `Double` cells occur,
  // otherwise the legacy evaluation takes over and keeps the `1.0` row.
  // (`Bool` and `Undefined` cells need no guard: the legacy `=` drops them
  // just like the bitwise comparison.)
  using namespace sparqlExpression;
  using namespace sparqlExpression::relational;
  auto I = ad_utility::testing::IntId;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  IdTable inputTable = makeIdTableFromVector({{I(1)},
                                              {Id::makeFromDouble(1.0)},
                                              {Id::makeFromBool(true)},
                                              {I(2)},
                                              {Id::makeUndefined()}});
  ValuesForTesting values{qec, std::move(inputTable), {Variable{"?x"}}, false,
                          {},  LocalVocab{},          std::nullopt,     true};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};

  auto expr = std::make_unique<EqualExpression>(EqualExpression::Children{
      std::make_unique<VariableExpression>(Variable{"?x"}),
      std::make_unique<IdExpression>(I(1))});
  Filter filter{qec,
                std::make_shared<QueryExecutionTree>(std::move(subTree)),
                {std::move(expr), "?x = 1"}};

  auto result = filter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());
  // `1` and `1.0` are equal to `1` in the legacy evaluation; `true`, `2`
  // and `UNDEF` are dropped (strict `=` typing, mismatch, error).
  EXPECT_EQ(result->idTableView(),
            makeIdTableFromVector({{I(1)}, {Id::makeFromDouble(1.0)}}));
}

TEST(JitExpressionBytecodeVmTest, ArithmeticFilterFallsBackOnDoubles) {
  // Regression test: `FILTER(?x * 2 > 3)` with `Double` cells. The legacy
  // evaluation computes doubles, while the integer kernels yield `UNDEF`
  // for `Double` cells and would wrongly drop the `2.5` row.
  using namespace sparqlExpression;
  using namespace sparqlExpression::relational;
  auto I = ad_utility::testing::IntId;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  IdTable inputTable = makeIdTableFromVector(
      {{I(2)}, {Id::makeFromDouble(1.5)}, {Id::makeFromDouble(2.5)}, {I(1)}});
  ValuesForTesting values{qec, std::move(inputTable), {Variable{"?x"}}, false,
                          {},  LocalVocab{},          std::nullopt,     true};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};

  auto expr =
      std::make_unique<GreaterThanExpression>(GreaterThanExpression::Children{
          makeMultiplyExpression(
              std::make_unique<VariableExpression>(Variable{"?x"}),
              std::make_unique<IdExpression>(I(2))),
          std::make_unique<IdExpression>(I(3))});
  Filter filter{qec,
                std::make_shared<QueryExecutionTree>(std::move(subTree)),
                {std::move(expr), "?x * 2 > 3"}};

  auto result = filter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());
  EXPECT_EQ(result->idTableView(),
            makeIdTableFromVector({{I(2)}, {Id::makeFromDouble(2.5)}}));
}

TEST(JitExpressionBytecodeVmTest, BindFallsBackOnDoubles) {
  // Regression test: `BIND(?x * 2 AS ?z)` with a `Double` cell. The legacy
  // evaluation materializes the double `3.0`, while the integer column
  // execution yields `UNDEF` for `Double` cells.
  using namespace sparqlExpression;
  auto I = ad_utility::testing::IntId;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  IdTable inputTable =
      makeIdTableFromVector({{Id::makeFromDouble(1.5)}, {I(3)}});
  ValuesForTesting values{qec, std::move(inputTable), {Variable{"?x"}}, false,
                          {},  LocalVocab{},          std::nullopt,     true};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};

  auto expr = makeMultiplyExpression(
      std::make_unique<VariableExpression>(Variable{"?x"}),
      std::make_unique<IdExpression>(I(2)));
  parsedQuery::Bind bind{{std::move(expr), "?x * 2"}, Variable{"?z"}};
  Bind bindOp{qec, std::make_shared<QueryExecutionTree>(std::move(subTree)),
              std::move(bind)};

  auto result = bindOp.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());
  const auto& table = result->idTableView();
  ASSERT_EQ(table.size(), 2u);
  ASSERT_EQ(table.numColumns(), 2u);
  EXPECT_EQ(table(0, 1), Id::makeFromDouble(3.0));
  EXPECT_EQ(table(1, 1), I(6));
}

TEST(JitExpressionBytecodeVmTest, ProgramPredicates) {
  // `containsDivision` and `hasExactIntegerSemantics` gate the JIT backends.
  JitBytecodeProgram arith;
  arith.addInstruction(OpCode::LOAD_COL_INT, 0);
  arith.addInstruction(OpCode::LOAD_CONST_INT, 2);
  arith.addInstruction(OpCode::MUL_INT);
  arith.addInstruction(OpCode::RET);
  EXPECT_FALSE(JitExpressionBytecodeVm::containsDivision(arith));
  EXPECT_TRUE(JitExpressionBytecodeVm::hasExactIntegerSemantics(arith));

  JitBytecodeProgram withDiv;
  withDiv.addInstruction(OpCode::LOAD_COL_INT, 0);
  withDiv.addInstruction(OpCode::LOAD_CONST_INT, 2);
  withDiv.addInstruction(OpCode::DIV_INT);
  withDiv.addInstruction(OpCode::RET);
  EXPECT_TRUE(JitExpressionBytecodeVm::containsDivision(withDiv));
  EXPECT_FALSE(JitExpressionBytecodeVm::hasExactIntegerSemantics(withDiv));

  JitBytecodeProgram withCmp;
  withCmp.addInstruction(OpCode::LOAD_COL_INT, 0);
  withCmp.addInstruction(OpCode::LOAD_CONST_INT, 2);
  withCmp.addInstruction(OpCode::CMP_GT_INT);
  withCmp.addInstruction(OpCode::RET);
  EXPECT_FALSE(JitExpressionBytecodeVm::containsDivision(withCmp));
  // Comparisons yield booleans, not integers, in legacy evaluation.
  EXPECT_FALSE(JitExpressionBytecodeVm::hasExactIntegerSemantics(withCmp));

  JitBytecodeProgram withIdOp;
  withIdOp.addInstruction(OpCode::LOAD_COL_ID, 0);
  withIdOp.addInstruction(OpCode::RET);
  EXPECT_FALSE(JitExpressionBytecodeVm::hasExactIntegerSemantics(withIdOp));
}

TEST(JitExpressionBytecodeVmTest, ScanColumnKindsAndCellRules) {
  // Unit test for the runtime exactness guard (see `CellRule`): datatype
  // presence detection over referenced columns and the per-rule predicates.
  auto I = ad_utility::testing::IntId;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  LocalVocabEntry local =
      LocalVocabEntry::literalWithoutQuotes("xyz", qec->getLocalVocabContext());

  JitBytecodeProgram program;
  program.addInstruction(OpCode::LOAD_COL_ID, 0);
  program.addReferencedColumn(0);

  // Mixed column: every constrained kind is present.
  IdTable mixed =
      makeIdTableFromVector({{I(1)},
                             {Id::makeFromDouble(1.5)},
                             {Id::makeFromBool(true)},
                             {Id::makeFromLocalVocabIndex(&local)},
                             {Id::makeFromVocabIndex(VocabIndex::make(3))},
                             {Id::makeUndefined()}});
  auto kinds =
      JitExpressionBytecodeVm::scanColumnKinds(program, mixed, 0, mixed.size());
  EXPECT_TRUE(kinds.hasDouble);
  EXPECT_TRUE(kinds.hasBool);
  EXPECT_TRUE(kinds.hasLocalVocab);
  EXPECT_TRUE(kinds.hasOther);
  EXPECT_FALSE(kinds.allInt);
  EXPECT_FALSE(JitExpressionBytecodeVm::satisfiesCellRule(
      CellRule::BitwiseExact, program, kinds));
  EXPECT_FALSE(JitExpressionBytecodeVm::satisfiesCellRule(
      CellRule::FoldIntEquality, program, kinds));
  EXPECT_FALSE(JitExpressionBytecodeVm::satisfiesCellRule(
      CellRule::IntegerArithmetic, program, kinds));
  EXPECT_FALSE(JitExpressionBytecodeVm::satisfiesCellRule(
      CellRule::OrderedComparison, program, kinds));

  // Pure integers satisfy every rule (the native backend additionally
  // requires `allInt`, which holds here).
  IdTable ints = makeIdTableFromVector({{I(1)}, {I(2)}});
  auto intKinds =
      JitExpressionBytecodeVm::scanColumnKinds(program, ints, 0, ints.size());
  EXPECT_TRUE(intKinds.allInt);
  EXPECT_FALSE(intKinds.hasDouble);
  EXPECT_TRUE(JitExpressionBytecodeVm::satisfiesCellRule(CellRule::BitwiseExact,
                                                         program, intKinds));
  EXPECT_TRUE(JitExpressionBytecodeVm::satisfiesCellRule(
      CellRule::FoldIntEquality, program, intKinds));
  EXPECT_TRUE(JitExpressionBytecodeVm::satisfiesCellRule(
      CellRule::IntegerArithmetic, program, intKinds));
  EXPECT_TRUE(JitExpressionBytecodeVm::satisfiesCellRule(
      CellRule::OrderedComparison, program, intKinds));

  // An empty range scans nothing and satisfies every rule.
  auto emptyKinds =
      JitExpressionBytecodeVm::scanColumnKinds(program, mixed, 0, 0);
  EXPECT_TRUE(emptyKinds.allInt);
  EXPECT_TRUE(JitExpressionBytecodeVm::satisfiesCellRule(
      CellRule::OrderedComparison, program, emptyKinds));
}

TEST(JitExpressionBytecodeVmTest, ExecuteIntColumn) {
  // Program: (?x * 2) + ?y, evaluated to a value column across morsels.
  // UNDEF and non-integer inputs propagate UNDEF, like legacy evaluation.
  auto I = ad_utility::testing::IntId;
  constexpr size_t NUM_ROWS = 70;
  VectorTable raw;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    raw.push_back({static_cast<int64_t>(i), static_cast<int64_t>(100 - i)});
  }
  IdTable inputTable = makeIdTableFromVector(raw, I);
  inputTable(3, 0) = Id::makeUndefined();
  inputTable(5, 1) = Id::makeFromVocabIndex(VocabIndex::make(7));

  JitBytecodeProgram program;
  program.addInstruction(OpCode::LOAD_COL_INT, 0);
  program.addInstruction(OpCode::LOAD_CONST_INT, 2);
  program.addInstruction(OpCode::MUL_INT);
  program.addInstruction(OpCode::LOAD_COL_INT, 1);
  program.addInstruction(OpCode::ADD_INT);
  program.addInstruction(OpCode::RET);

  IdTable outputTable = inputTable.clone();
  outputTable.addEmptyColumn();
  const ColumnIndex outCol = outputTable.numColumns() - 1;
  JitExpressionBytecodeVm::executeIntColumn(program, inputTable, outputTable,
                                            outCol, nullptr);
  ASSERT_EQ(outputTable.size(), NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if (i == 3 || i == 5) {
      EXPECT_EQ(outputTable(i, outCol), Id::makeUndefined()) << "row " << i;
    } else {
      const int64_t expected =
          static_cast<int64_t>(i) * 2 + (100 - static_cast<int64_t>(i));
      EXPECT_EQ(outputTable(i, outCol), Id::makeFromInt(expected))
          << "row " << i;
    }
  }
}

TEST(JitExpressionBytecodeVmTest, BindOperationIntegration) {
  // `BIND((?x * 2) + 1 AS ?z)` evaluates through `executeIntColumn`, with
  // UNDEF propagation for undefined inputs.
  using namespace sparqlExpression;
  auto I = ad_utility::testing::IntId;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  IdTable inputTable = makeIdTableFromVector({{1}, {2}, {3}, {4}, {5}, {6}}, I);
  inputTable(5, 0) = Id::makeUndefined();
  ValuesForTesting values{qec, std::move(inputTable), {Variable{"?x"}}, false,
                          {},  LocalVocab{},          std::nullopt,     true};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};

  auto expr = makeAddExpression(
      makeMultiplyExpression(
          std::make_unique<VariableExpression>(Variable{"?x"}),
          std::make_unique<IdExpression>(I(2))),
      std::make_unique<IdExpression>(I(1)));
  parsedQuery::Bind bind{{std::move(expr), "(?x * 2) + 1"}, Variable{"?z"}};
  Bind bindOp{qec, std::make_shared<QueryExecutionTree>(std::move(subTree)),
              std::move(bind)};

  auto result = bindOp.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());
  ASSERT_EQ(result->idTableView().numColumns(), 2u);
  const auto& table = result->idTableView();
  ASSERT_EQ(table.size(), 6u);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(table(i, 1), I(static_cast<int64_t>(i + 1) * 2 + 1))
        << "row " << i;
  }
  EXPECT_EQ(table(5, 1), Id::makeUndefined());
}

TEST(JitExpressionBytecodeVmTest, AndOrLoweringWithKleeneSemantics) {
  using namespace sparqlExpression;
  auto I = ad_utility::testing::IntId;
  auto U = Id::makeUndefined();

  // AST: (?x > 1) && (?y < 10). Conjunction with an UNDEF side drops the
  // row, like the legacy `AndLambda`.
  auto andExpr = makeAndExpression(
      std::make_unique<GreaterThanExpression>(
          std::array<SparqlExpression::Ptr, 2>{
              std::make_unique<VariableExpression>(Variable{"?x"}),
              std::make_unique<IdExpression>(I(1))}),
      std::make_unique<LessThanExpression>(std::array<SparqlExpression::Ptr, 2>{
          std::make_unique<VariableExpression>(Variable{"?y"}),
          std::make_unique<IdExpression>(I(10))}));

  VariableToColumnMap varColMap;
  varColMap[Variable{"?x"}] = makeAlwaysDefinedColumn(0);
  varColMap[Variable{"?y"}] = makeAlwaysDefinedColumn(1);

  auto optAnd = JitExpressionBytecodeVm::compile(*andExpr, varColMap);
  ASSERT_TRUE(optAnd.has_value());
  const auto& andProg = optAnd.value();
  EXPECT_TRUE(std::any_of(
      andProg.instructions().begin(), andProg.instructions().end(),
      [](const auto& inst) { return inst.op == OpCode::AND_BOOL; }));

  IdTable andInput =
      makeIdTableFromVector({{I(2), I(5)},   // true && true -> keep
                             {I(0), I(5)},   // false && true -> drop
                             {I(2), I(50)},  // true && false -> drop
                             {U, I(5)},      // undef && true -> drop
                             {I(0), U},      // false && undef -> drop
                             {U, U}});       // undef && undef -> drop
  IdTableStatic<2> andResult =
      IdTable{2, ad_utility::makeUnlimitedAllocator<Id>()}.toStatic<2>();
  JitExpressionBytecodeVm::executeFilter<2>(andProg, andInput, andResult);
  EXPECT_EQ(std::move(andResult).toDynamic(),
            makeIdTableFromVector({{I(2), I(5)}}));

  // AST: (?x > 100) || (?y < 10). Kleene disjunction: a `True` side keeps
  // the row even when the other side is UNDEF (matches `OrLambda`).
  auto orExpr = makeOrExpression(
      std::make_unique<GreaterThanExpression>(
          std::array<SparqlExpression::Ptr, 2>{
              std::make_unique<VariableExpression>(Variable{"?x"}),
              std::make_unique<IdExpression>(I(100))}),
      std::make_unique<LessThanExpression>(std::array<SparqlExpression::Ptr, 2>{
          std::make_unique<VariableExpression>(Variable{"?y"}),
          std::make_unique<IdExpression>(I(10))}));

  auto optOr = JitExpressionBytecodeVm::compile(*orExpr, varColMap);
  ASSERT_TRUE(optOr.has_value());
  const auto& orProg = optOr.value();

  IdTable orInput =
      makeIdTableFromVector({{I(2), I(5)},     // false || true -> keep
                             {I(200), I(50)},  // true || false -> keep
                             {I(200), I(5)},   // true || true -> keep
                             {I(2), I(50)},    // false || false -> drop
                             {U, I(5)},        // undef || true -> keep (Kleene)
                             {I(200), U},      // true || undef -> keep (Kleene)
                             {U, I(50)},       // undef || false -> drop
                             {U, U}});         // undef || undef -> drop
  IdTableStatic<2> orResult =
      IdTable{2, ad_utility::makeUnlimitedAllocator<Id>()}.toStatic<2>();
  JitExpressionBytecodeVm::executeFilter<2>(orProg, orInput, orResult);
  EXPECT_EQ(std::move(orResult).toDynamic(),
            makeIdTableFromVector({{I(2), I(5)},
                                   {I(200), I(50)},
                                   {I(200), I(5)},
                                   {U, I(5)},
                                   {I(200), U}}));
}

TEST(JitExpressionBytecodeVmTest, MultiRangeOrKeepsSingleRangeRows) {
  // Regression test: `OR_BOOL` over several `IN_ID_RANGE` checks (as
  // emitted by `tryFoldPrefixRegex` for multiple vocabulary ranges) must
  // keep rows that match exactly one range. Each range check is invalid
  // outside its own range, so a naive validity conjunction drops such rows
  // while the legacy disjunction keeps them.
  const uint64_t baseA =
      Id::makeFromVocabIndex(VocabIndex::make(100)).getBits();
  const uint64_t baseB =
      Id::makeFromVocabIndex(VocabIndex::make(300)).getBits();
  JitBytecodeProgram program;
  program.addReferencedColumn(0);
  const size_t rangeA = program.addIdRange(baseA, baseA + 100);
  const size_t rangeB = program.addIdRange(baseB, baseB + 100);
  program.addInstruction(OpCode::LOAD_COL_ID, 0);
  program.addInstruction(OpCode::IN_ID_RANGE, static_cast<int64_t>(rangeA));
  program.addInstruction(OpCode::LOAD_COL_ID, 0);
  program.addInstruction(OpCode::IN_ID_RANGE, static_cast<int64_t>(rangeB));
  program.addInstruction(OpCode::OR_BOOL);
  program.addInstruction(OpCode::RET);

  auto V = [](uint64_t bits) { return Id::fromBits(bits); };
  // NOTE: the datatype tag lives in the high `ValueId` bits, so `baseA + 200`
  // is exactly `baseB` (and thus inside range B). The neither-row uses +150
  // (vocabulary index 250), which is inside neither range.
  IdTable input =
      makeIdTableFromVector({{V(baseA + 10)},          // only range A -> keep
                             {V(baseB + 10)},          // only range B -> keep
                             {V(baseA + 150)},         // neither -> drop
                             {Id::makeUndefined()}});  // neither -> drop
  IdTableStatic<1> result =
      IdTable{1, ad_utility::makeUnlimitedAllocator<Id>()}.toStatic<1>();
  JitExpressionBytecodeVm::executeFilter<1>(program, input, result);
  EXPECT_EQ(std::move(result).toDynamic(),
            makeIdTableFromVector({{V(baseA + 10)}, {V(baseB + 10)}}));
}

TEST(JitExpressionBytecodeVmTest, YearLoweringAndRule) {
  using namespace sparqlExpression;
  auto I = ad_utility::testing::IntId;
  auto U = Id::makeUndefined();
  auto D = [](int year, int month, int day) {
    return Id::makeFromDate(DateYearOrDuration(Date(year, month, day)));
  };

  VariableToColumnMap varColMap;
  varColMap[Variable{"?d"}] = makeAlwaysDefinedColumn(0);

  // AST: YEAR(?d) >= 1800 (the date-range benchmark shape without the
  // conjunction).
  auto yearGe = std::make_unique<GreaterEqualExpression>(
      std::array<SparqlExpression::Ptr, 2>{
          makeYearExpression(
              std::make_unique<VariableExpression>(Variable{"?d"})),
          std::make_unique<IdExpression>(I(1800))});
  auto optYear = JitExpressionBytecodeVm::compile(*yearGe, varColMap);
  ASSERT_TRUE(optYear.has_value());
  const auto& yearProg = optYear.value();
  EXPECT_EQ(yearProg.cellRule(), CellRule::YearExtraction);

  IdTable dates = makeIdTableFromVector({{D(1850, 5, 1)},    // keep
                                         {D(1800, 1, 1)},    // keep (boundary)
                                         {D(1799, 12, 31)},  // drop
                                         {D(2000, 6, 30)},   // keep
                                         {I(1850)},  // not a date -> drop
                                         {U}});      // drop
  auto dateKinds = JitExpressionBytecodeVm::scanColumnKinds(yearProg, dates, 0,
                                                            dates.size());
  EXPECT_TRUE(dateKinds.hasDate);
  EXPECT_EQ(dateKinds.perColumn.size(), 1u);
  EXPECT_TRUE(dateKinds.perColumn.at(0).second.hasDate);
  EXPECT_TRUE(JitExpressionBytecodeVm::satisfiesCellRule(
      CellRule::YearExtraction, yearProg, dateKinds));

  IdTableStatic<1> yearResult =
      IdTable{1, ad_utility::makeUnlimitedAllocator<Id>()}.toStatic<1>();
  JitExpressionBytecodeVm::executeFilter<1>(yearProg, dates, yearResult);
  EXPECT_EQ(std::move(yearResult).toDynamic(),
            makeIdTableFromVector(
                {{D(1850, 5, 1)}, {D(1800, 1, 1)}, {D(2000, 6, 30)}}));

  // A date column that is also loaded as `Int` rejects the rule: integer
  // loads of dates diverge from the legacy date arithmetic and ordering.
  JitBytecodeProgram intLoad;
  intLoad.addInstruction(OpCode::LOAD_COL_INT, 0);
  intLoad.addInstruction(OpCode::LOAD_CONST_INT, 1);
  intLoad.addInstruction(OpCode::ADD_INT);
  intLoad.addInstruction(OpCode::RET);
  intLoad.addReferencedColumn(0);
  intLoad.setCellRule(CellRule::YearExtraction);
  auto intKinds =
      JitExpressionBytecodeVm::scanColumnKinds(intLoad, dates, 0, dates.size());
  EXPECT_FALSE(JitExpressionBytecodeVm::satisfiesCellRule(
      CellRule::YearExtraction, intLoad, intKinds));

  // Doubles keep rejecting the rule even next to dates.
  IdTable withDoubles =
      makeIdTableFromVector({{D(1850, 5, 1)}, {Id::makeFromDouble(1.5)}});
  auto doubleKinds = JitExpressionBytecodeVm::scanColumnKinds(
      yearProg, withDoubles, 0, withDoubles.size());
  EXPECT_FALSE(JitExpressionBytecodeVm::satisfiesCellRule(
      CellRule::YearExtraction, yearProg, doubleKinds));
}

TEST(JitExpressionBytecodeVmTest, LifespanAndDateRangeEndToEnd) {
  using namespace sparqlExpression;
  auto I = ad_utility::testing::IntId;
  auto U = Id::makeUndefined();
  auto D = [](int year, int month, int day) {
    return Id::makeFromDate(DateYearOrDuration(Date(year, month, day)));
  };
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  // Lifespan shape: YEAR(?death) - YEAR(?birth) > 90.
  IdTable lifespanInput = makeIdTableFromVector({{D(1900, 1, 1), D(1800, 1, 1)},
                                                 {D(1850, 1, 1), D(1800, 1, 1)},
                                                 {U, D(1800, 1, 1)},
                                                 {D(1900, 1, 1), I(1800)}});
  ValuesForTesting lifespanValues{qec,
                                  std::move(lifespanInput),
                                  {Variable{"?death"}, Variable{"?birth"}},
                                  false,
                                  {},
                                  LocalVocab{},
                                  std::nullopt,
                                  true};
  QueryExecutionTree lifespanSubTree{
      qec, std::make_shared<ValuesForTesting>(std::move(lifespanValues))};
  auto lifespanExpr = std::make_unique<GreaterThanExpression>(
      std::array<SparqlExpression::Ptr, 2>{
          makeSubtractExpression(
              makeYearExpression(
                  std::make_unique<VariableExpression>(Variable{"?death"})),
              makeYearExpression(
                  std::make_unique<VariableExpression>(Variable{"?birth"}))),
          std::make_unique<IdExpression>(I(90))});
  Filter lifespanFilter{
      qec,
      std::make_shared<QueryExecutionTree>(std::move(lifespanSubTree)),
      {std::move(lifespanExpr), "YEAR(?death) - YEAR(?birth) > 90"}};
  auto lifespanResult =
      lifespanFilter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(lifespanResult->isFullyMaterialized());
  EXPECT_EQ(lifespanResult->idTableView(),
            makeIdTableFromVector({{D(1900, 1, 1), D(1800, 1, 1)}}));

  // Date-range shape: YEAR(?d) >= 1800 && YEAR(?d) < 1900.
  IdTable rangeInput = makeIdTableFromVector({{D(1850, 6, 15)},
                                              {D(1799, 12, 31)},
                                              {D(1900, 1, 1)},
                                              {D(1800, 1, 1)},
                                              {I(1850)},
                                              {U}});
  ValuesForTesting rangeValues{
      qec, std::move(rangeInput), {Variable{"?d"}}, false,
      {},  LocalVocab{},          std::nullopt,     true};
  QueryExecutionTree rangeSubTree{
      qec, std::make_shared<ValuesForTesting>(std::move(rangeValues))};
  auto rangeExpr = makeAndExpression(
      std::make_unique<GreaterEqualExpression>(
          std::array<SparqlExpression::Ptr, 2>{
              makeYearExpression(
                  std::make_unique<VariableExpression>(Variable{"?d"})),
              std::make_unique<IdExpression>(I(1800))}),
      std::make_unique<LessThanExpression>(std::array<SparqlExpression::Ptr, 2>{
          makeYearExpression(
              std::make_unique<VariableExpression>(Variable{"?d"})),
          std::make_unique<IdExpression>(I(1900))}));
  Filter rangeFilter{
      qec,
      std::make_shared<QueryExecutionTree>(std::move(rangeSubTree)),
      {std::move(rangeExpr), "YEAR(?d) >= 1800 && YEAR(?d) < 1900"}};
  auto rangeResult =
      rangeFilter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(rangeResult->isFullyMaterialized());
  EXPECT_EQ(rangeResult->idTableView(),
            makeIdTableFromVector({{D(1850, 6, 15)}, {D(1800, 1, 1)}}));
}

TEST(JitExpressionBytecodeVmTest, NativeAndFilterEndToEnd) {
  // All-`Int` conjunction: the `Filter` engages the native AsmJit backend
  // (`allInt`), which validates the `AND_BOOL` machine-code lowering.
  using namespace sparqlExpression;
  auto I = ad_utility::testing::IntId;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  VectorTable data;
  for (int64_t i = 0; i < 100; ++i) {
    data.push_back({i, 100 - i});
  }
  IdTable inputTable = makeIdTableFromVector(data, I);
  ValuesForTesting values{qec,
                          std::move(inputTable),
                          {Variable{"?a"}, Variable{"?b"}},
                          false,
                          {},
                          LocalVocab{},
                          std::nullopt,
                          true};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};

  // (?a > 10) && (?b < 90): rows 11..99 except where mirrored, i.e. rows
  // with a in [11, 99] and b = 100 - a < 90, i.e. a in [11, 99].
  auto expr = makeAndExpression(
      std::make_unique<GreaterThanExpression>(
          std::array<SparqlExpression::Ptr, 2>{
              std::make_unique<VariableExpression>(Variable{"?a"}),
              std::make_unique<IdExpression>(I(10))}),
      std::make_unique<LessThanExpression>(std::array<SparqlExpression::Ptr, 2>{
          std::make_unique<VariableExpression>(Variable{"?b"}),
          std::make_unique<IdExpression>(I(90))}));
  Filter filter{qec,
                std::make_shared<QueryExecutionTree>(std::move(subTree)),
                {std::move(expr), "(?a > 10) && (?b < 90)"}};
  auto result = filter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());
  // a in [11, 99] (89 rows): b = 100 - a in [1, 89], all < 90.
  EXPECT_EQ(result->idTableView().size(), 89u);
  EXPECT_EQ(result->idTableView()(0, 0), I(11));
}

TEST(JitExpressionBytecodeVmTest, NativeYearFilterEndToEnd) {
  // `YEAR(?d) >= 1800` over a mixed date/int column: the native AsmJit
  // backend compiles the program (no longer declining date loads) and the
  // `Filter` engages it through the relaxed `YearExtraction` gate, which
  // validates the `LOAD_COL_DATE`/`YEAR_DATE` machine-code lowering and
  // the per-row validity flag (non-`Date` cells drop, even the `Int`
  // whose value would compare true as a year).
  using namespace sparqlExpression;
  auto I = ad_utility::testing::IntId;
  auto D = [](int year, int month, int day) {
    return Id::makeFromDate(DateYearOrDuration(Date(year, month, day)));
  };

  VariableToColumnMap varColMap;
  varColMap[Variable{"?d"}] = makeAlwaysDefinedColumn(0);
  auto yearGe = std::make_unique<GreaterEqualExpression>(
      std::array<SparqlExpression::Ptr, 2>{
          makeYearExpression(
              std::make_unique<VariableExpression>(Variable{"?d"})),
          std::make_unique<IdExpression>(I(1800))});
  // The native compiler accepts the date program (previously nullopt).
  auto optNative = JitExpressionCompiler::compile(*yearGe, varColMap);
  ASSERT_TRUE(optNative.has_value());

  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();
  IdTable inputTable =
      makeIdTableFromVector({{D(1850, 5, 1)},    // keep
                             {D(1800, 1, 1)},    // keep (boundary)
                             {D(1799, 12, 31)},  // drop
                             {D(2000, 6, 30)},   // keep
                             {I(1900)}});        // not a date -> drop
  ValuesForTesting values{qec, std::move(inputTable), {Variable{"?d"}}, false,
                          {},  LocalVocab{},          std::nullopt,     true};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};
  auto expr = std::make_unique<GreaterEqualExpression>(
      std::array<SparqlExpression::Ptr, 2>{
          makeYearExpression(
              std::make_unique<VariableExpression>(Variable{"?d"})),
          std::make_unique<IdExpression>(I(1800))});
  Filter filter{qec,
                std::make_shared<QueryExecutionTree>(std::move(subTree)),
                {std::move(expr), "YEAR(?d) >= 1800"}};
  auto result = filter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());
  EXPECT_EQ(result->idTableView().size(), 3u);
  EXPECT_EQ(result->idTableView()(0, 0), D(1850, 5, 1));
  EXPECT_EQ(result->idTableView()(1, 0), D(1800, 1, 1));
  EXPECT_EQ(result->idTableView()(2, 0), D(2000, 6, 30));
}
