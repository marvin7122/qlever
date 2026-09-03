// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"

namespace ql::engine::jit {

// _____________________________________________________________________________
// Lightweight Bytecode VM for SPARQL Expression Evaluation:
// Flattens expression trees into a contiguous instruction stream, executing in
// tight register/stack loops without virtual function calls or heap allocations.
enum class OpCode : uint8_t {
  LOAD_COL_INT,
  LOAD_CONST_INT,
  ADD_INT,
  SUB_INT,
  MUL_INT,
  CMP_GT_INT,
  CMP_EQ_INT,
  RET
};

struct Instruction {
  OpCode op;
  int64_t arg;
};

class JitBytecodeProgram {
 private:
  std::vector<Instruction> code_;

 public:
  void addInstruction(OpCode op, int64_t arg = 0) {
    code_.push_back({op, arg});
  }

  // Execute bytecode program over a single row's column inputs
  [[nodiscard]] int64_t execute(ql::span<const int64_t> rowColumns) const noexcept {
    int64_t stack[16];
    size_t sp = 0;

    for (const auto& inst : code_) {
      switch (inst.op) {
        case OpCode::LOAD_COL_INT:
          stack[sp++] = rowColumns[inst.arg];
          break;
        case OpCode::LOAD_CONST_INT:
          stack[sp++] = inst.arg;
          break;
        case OpCode::ADD_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = a + b;
          break;
        }
        case OpCode::SUB_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = a - b;
          break;
        }
        case OpCode::MUL_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = a * b;
          break;
        }
        case OpCode::CMP_GT_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (a > b) ? 1 : 0;
          break;
        }
        case OpCode::CMP_EQ_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (a == b) ? 1 : 0;
          break;
        }
        case OpCode::RET:
          return (sp > 0) ? stack[sp - 1] : 0;
      }
    }
    return (sp > 0) ? stack[sp - 1] : 0;
  }
};

}  // namespace ql::engine::jit
