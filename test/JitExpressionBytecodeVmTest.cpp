// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <vector>

#include "engine/Filter.h"
#include "engine/ValuesForTesting.h"
#include "engine/sparqlExpressions/JitExpressionBytecodeVm.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
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
  auto I = ad_utility::testing::IntId;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  // Test with a larger table (100 rows) to test morsel processing
  std::vector<std::vector<int64_t>> data;
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
  auto expr = std::make_unique<EqualExpression>(
      std::array<SparqlExpression::Ptr, 2>{
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
