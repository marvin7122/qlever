// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <chrono>
#include <iostream>
#include <vector>

#include "engine/sparqlExpressions/JitExpressionBytecodeVm.h"

using namespace ql::engine::jit;

int main() {
  constexpr size_t NUM_ROWS = 10'000'000;
  std::cout << "Benchmarking JIT Expression Bytecode VM over " << NUM_ROWS << " rows...\n";

  std::vector<int64_t> col0(NUM_ROWS);
  std::vector<int64_t> col1(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    col0[i] = static_cast<int64_t>(i % 100);
    col1[i] = static_cast<int64_t>((i * 3) % 100);
  }

  // Program: (col0 * 2) + col1 > 100
  JitBytecodeProgram program;
  program.addInstruction(OpCode::LOAD_COL_INT, 0);
  program.addInstruction(OpCode::LOAD_CONST_INT, 2);
  program.addInstruction(OpCode::MUL_INT);
  program.addInstruction(OpCode::LOAD_COL_INT, 1);
  program.addInstruction(OpCode::ADD_INT);
  program.addInstruction(OpCode::LOAD_CONST_INT, 100);
  program.addInstruction(OpCode::CMP_GT_INT);
  program.addInstruction(OpCode::RET);

  auto t0 = std::chrono::high_resolution_clock::now();
  size_t matches = 0;
  int64_t row[2];
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    row[0] = col0[i];
    row[1] = col1[i];
    matches += program.execute(row);
  }
  auto t1 = std::chrono::high_resolution_clock::now();

  double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
  std::cout << "Bytecode VM Evaluation: " << ms << " ms ("
            << (NUM_ROWS / (ms / 1000.0)) / 1e6 << " M evaluations/sec, matches: " << matches << ")\n";

  return 0;
}
