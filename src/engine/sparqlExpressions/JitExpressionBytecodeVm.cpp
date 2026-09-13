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
    // Statically bound the interpreter stack depth: right-nested
    // expressions (e.g. `1 + (1 + (...))`) grow the stack linearly, and the
    // kernels use fixed `MAX_STACK_SLOTS` stacks. Overly deep programs fall
    // back to the legacy evaluation instead of overflowing.
    size_t depth = 0;
    size_t maxDepth = 0;
    for (const auto& inst : program.instructions()) {
      switch (inst.op) {
        case OpCode::LOAD_COL_INT:
        case OpCode::LOAD_CONST_INT:
        case OpCode::LOAD_COL_ID:
        case OpCode::LOAD_COL_DATE:
          ++depth;
          break;
        case OpCode::ADD_INT:
        case OpCode::SUB_INT:
        case OpCode::MUL_INT:
        case OpCode::DIV_INT:
        case OpCode::MOD_INT:
        case OpCode::CMP_GT_INT:
        case OpCode::CMP_GE_INT:
        case OpCode::CMP_LT_INT:
        case OpCode::CMP_LE_INT:
        case OpCode::CMP_EQ_INT:
        case OpCode::CMP_NE_INT:
        case OpCode::CMP_EQ_ID:
        case OpCode::OR_BOOL:
        case OpCode::AND_BOOL:
          if (depth < 2) {
            return std::nullopt;
          }
          --depth;
          break;
        case OpCode::IN_ID_RANGE:
        case OpCode::YEAR_DATE:
          if (depth < 1) {
            return std::nullopt;
          }
          break;
        default:
          // `RET` is appended below, so any other unrecognized instruction
          // means a malformed program: refuse it rather than execute it.
          return std::nullopt;
      }
      maxDepth = std::max(maxDepth, depth);
      if (maxDepth > MAX_STACK_SLOTS) {
        return std::nullopt;
      }
    }
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
    const bool hasYearExtraction = std::any_of(
        instructions.begin(), instructions.end(),
        [](const Instruction& inst) { return inst.op == OpCode::YEAR_DATE; });
    program.setCellRule(hasYearExtraction ? CellRule::YearExtraction
                        : hasComparison   ? CellRule::OrderedComparison
                                          : CellRule::IntegerArithmetic);
    return program;
  }
  return std::nullopt;
}

}  // namespace ql::engine::jit
