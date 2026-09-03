// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <vector>

#include "engine/sparqlExpressions/JitExpressionBytecodeVm.h"

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
