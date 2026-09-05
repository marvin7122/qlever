// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "engine/sparqlExpressions/JitExpressionBytecodeVm.h"

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace ql::engine::jit {

// _____________________________________________________________________________
std::optional<JitBytecodeProgram> JitExpressionBytecodeVm::compile(
    const sparqlExpression::SparqlExpression& expr,
    const VariableToColumnMap& varColMap) {
  JitBytecodeProgram program;
  if (expr.compileToJit(program, varColMap)) {
    program.addInstruction(OpCode::RET);
    // Comparisons are only exact over `Int`/`Bool`/`Undefined` cells
    // (legacy compares strings, dates and mixed numerics), pure integer
    // arithmetic additionally tolerates all non-numeric cells except
    // `Date` (see `CellRule`).
    const auto& instructions = program.instructions();
    const bool hasComparison = std::any_of(
        instructions.begin(), instructions.end(), [](const Instruction& inst) {
          switch (inst.op) {
            case OpCode::CMP_GT_INT:
            case OpCode::CMP_GE_INT:
            case OpCode::CMP_LT_INT:
            case OpCode::CMP_LE_INT:
            case OpCode::CMP_EQ_INT:
            case OpCode::CMP_NE_INT:
              return true;
            default:
              return false;
          }
        });
    program.setCellRule(hasComparison ? CellRule::OrderedComparison
                                      : CellRule::IntegerArithmetic);
    return program;
  }
  return std::nullopt;
}

}  // namespace ql::engine::jit
