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
    return program;
  }
  return std::nullopt;
}

}  // namespace ql::engine::jit
