// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <bit>
#include <cstdint>
#include <optional>
#include <vector>

#include "backports/span.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/CancellationHandle.h"

namespace sparqlExpression {
class SparqlExpression;
}

namespace ql::engine::jit {

// _____________________________________________________________________________
// Lightweight Bytecode VM for SPARQL Expression Evaluation:
// Flattens expression trees into a contiguous instruction stream, executing in
// tight register/stack loops without virtual function calls or heap
// allocations.
enum class OpCode : uint8_t {
  LOAD_COL_INT,
  LOAD_CONST_INT,
  ADD_INT,
  SUB_INT,
  MUL_INT,
  DIV_INT,
  MOD_INT,
  CMP_GT_INT,
  CMP_GE_INT,
  CMP_LT_INT,
  CMP_LE_INT,
  CMP_EQ_INT,
  CMP_NE_INT,
  RET
};

struct Instruction {
  OpCode op;
  int64_t arg = 0;
};

class JitBytecodeProgram {
 private:
  std::vector<Instruction> code_;
  std::vector<ColumnIndex> referencedColumns_;

 public:
  void addInstruction(OpCode op, int64_t arg = 0) {
    code_.push_back({op, arg});
  }

  void addReferencedColumn(ColumnIndex col) {
    referencedColumns_.push_back(col);
  }

  [[nodiscard]] const std::vector<Instruction>& instructions() const noexcept {
    return code_;
  }

  [[nodiscard]] const std::vector<ColumnIndex>& referencedColumns()
      const noexcept {
    return referencedColumns_;
  }

  // Execute bytecode program over a single row's column inputs
  [[nodiscard]] int64_t execute(
      ql::span<const int64_t> rowColumns) const noexcept {
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
        case OpCode::DIV_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (b != 0) ? (a / b) : 0;
          break;
        }
        case OpCode::MOD_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (b != 0) ? (a % b) : 0;
          break;
        }
        case OpCode::CMP_GT_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (a > b) ? 1 : 0;
          break;
        }
        case OpCode::CMP_GE_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (a >= b) ? 1 : 0;
          break;
        }
        case OpCode::CMP_LT_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (a < b) ? 1 : 0;
          break;
        }
        case OpCode::CMP_LE_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (a <= b) ? 1 : 0;
          break;
        }
        case OpCode::CMP_EQ_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (a == b) ? 1 : 0;
          break;
        }
        case OpCode::CMP_NE_INT: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (a != b) ? 1 : 0;
          break;
        }
        case OpCode::RET:
          return (sp > 0) ? stack[sp - 1] : 0;
      }
    }
    return (sp > 0) ? stack[sp - 1] : 0;
  }
};

class JitExpressionBytecodeVm {
 public:
  // Compile a SPARQL expression AST to a JitBytecodeProgram. Returns nullopt
  // if the expression contains unsupported operators, types, or unbound vars.
  static std::optional<JitBytecodeProgram> compile(
      const sparqlExpression::SparqlExpression& expr,
      const VariableToColumnMap& varColMap);

  // Vectorized Morsel Kernel: executes bytecode program over 64-row morsels
  // of contiguous columnar memory.
  static void executeVectorMorsel(const JitBytecodeProgram& program,
                                  const int64_t* const* inputColumns,
                                  size_t numRows,
                                  uint64_t* outFilterMask) noexcept {
    constexpr size_t MORSEL_SIZE = 64;
    alignas(64) int64_t stack[16][MORSEL_SIZE];
    alignas(64) uint64_t validity[16];

    for (size_t rowOffset = 0; rowOffset < numRows; rowOffset += MORSEL_SIZE) {
      size_t batchSize = std::min<size_t>(MORSEL_SIZE, numRows - rowOffset);
      uint64_t batchMask =
          (batchSize == 64) ? ~0ULL : ((1ULL << batchSize) - 1);
      size_t sp = 0;

      for (const auto& inst : program.instructions()) {
        switch (inst.op) {
          case OpCode::LOAD_COL_INT: {
            const int64_t* colData = inputColumns[inst.arg] + rowOffset;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = colData[i];
            }
            validity[sp] = batchMask;
            sp++;
            break;
          }
          case OpCode::LOAD_CONST_INT: {
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = inst.arg;
            }
            validity[sp] = batchMask;
            sp++;
            break;
          }
          case OpCode::ADD_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = stack[aIdx][i] + stack[bIdx][i];
            }
            validity[sp] = validity[aIdx] & validity[bIdx];
            sp++;
            break;
          }
          case OpCode::SUB_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = stack[aIdx][i] - stack[bIdx][i];
            }
            validity[sp] = validity[aIdx] & validity[bIdx];
            sp++;
            break;
          }
          case OpCode::MUL_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = stack[aIdx][i] * stack[bIdx][i];
            }
            validity[sp] = validity[aIdx] & validity[bIdx];
            sp++;
            break;
          }
          case OpCode::DIV_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t divMask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              int64_t b = stack[bIdx][i];
              if (b != 0) {
                stack[sp][i] = stack[aIdx][i] / b;
                divMask |= (1ULL << i);
              } else {
                stack[sp][i] = 0;
              }
            }
            validity[sp] = validity[aIdx] & validity[bIdx] & divMask;
            sp++;
            break;
          }
          case OpCode::MOD_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t modMask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              int64_t b = stack[bIdx][i];
              if (b != 0) {
                stack[sp][i] = stack[aIdx][i] % b;
                modMask |= (1ULL << i);
              } else {
                stack[sp][i] = 0;
              }
            }
            validity[sp] = validity[aIdx] & validity[bIdx] & modMask;
            sp++;
            break;
          }
          case OpCode::CMP_GT_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] > stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::CMP_GE_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] >= stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::CMP_LT_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] < stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::CMP_LE_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] <= stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::CMP_EQ_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] == stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::CMP_NE_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] != stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::RET:
            break;
        }
      }

      uint64_t filterMask = 0;
      if (sp > 0) {
        uint64_t valid = validity[sp - 1] & batchMask;
#pragma GCC unroll 8
        for (size_t i = 0; i < batchSize; ++i) {
          if ((valid & (1ULL << i)) && stack[sp - 1][i] != 0) {
            filterMask |= (1ULL << i);
          }
        }
      }
      outFilterMask[rowOffset / MORSEL_SIZE] = filterMask;
    }
  }

  // Execute filter over an input IdTable and append matching rows to
  // resultTable
  template <int WIDTH, typename Table>
  static void executeFilter(
      const JitBytecodeProgram& program, const Table& inputTable,
      IdTableStatic<WIDTH>& resultTable,
      ad_utility::SharedCancellationHandle cancellationHandle = nullptr) {
    size_t numRows = inputTable.size();
    if (numRows == 0) {
      return;
    }

    constexpr size_t MORSEL_SIZE = 64;
    alignas(64) int64_t stack[16][MORSEL_SIZE];
    alignas(64) uint64_t validity[16];

    for (size_t rowOffset = 0; rowOffset < numRows; rowOffset += MORSEL_SIZE) {
      if (cancellationHandle && (rowOffset % (MORSEL_SIZE * 1024) == 0)) {
        cancellationHandle->throwIfCancelled();
      }
      size_t batchSize = std::min<size_t>(MORSEL_SIZE, numRows - rowOffset);
      uint64_t batchMask =
          (batchSize == 64) ? ~0ULL : ((1ULL << batchSize) - 1);
      size_t sp = 0;

      for (const auto& inst : program.instructions()) {
        switch (inst.op) {
          case OpCode::LOAD_COL_INT: {
            auto colSpan = inputTable.getColumn(inst.arg);
            const Id* colData = colSpan.data() + rowOffset;
            uint64_t valMask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              Id id = colData[i];
              if (id.getDatatype() == Datatype::Int) {
                stack[sp][i] = id.getInt();
                valMask |= (1ULL << i);
              } else if (id.getDatatype() == Datatype::Bool) {
                stack[sp][i] = id.getBool() ? 1 : 0;
                valMask |= (1ULL << i);
              } else {
                stack[sp][i] = 0;
              }
            }
            validity[sp] = valMask;
            sp++;
            break;
          }
          case OpCode::LOAD_CONST_INT: {
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = inst.arg;
            }
            validity[sp] = batchMask;
            sp++;
            break;
          }
          case OpCode::ADD_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = stack[aIdx][i] + stack[bIdx][i];
            }
            validity[sp] = validity[aIdx] & validity[bIdx];
            sp++;
            break;
          }
          case OpCode::SUB_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = stack[aIdx][i] - stack[bIdx][i];
            }
            validity[sp] = validity[aIdx] & validity[bIdx];
            sp++;
            break;
          }
          case OpCode::MUL_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = stack[aIdx][i] * stack[bIdx][i];
            }
            validity[sp] = validity[aIdx] & validity[bIdx];
            sp++;
            break;
          }
          case OpCode::DIV_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t divMask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              int64_t b = stack[bIdx][i];
              if (b != 0) {
                stack[sp][i] = stack[aIdx][i] / b;
                divMask |= (1ULL << i);
              } else {
                stack[sp][i] = 0;
              }
            }
            validity[sp] = validity[aIdx] & validity[bIdx] & divMask;
            sp++;
            break;
          }
          case OpCode::MOD_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t modMask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              int64_t b = stack[bIdx][i];
              if (b != 0) {
                stack[sp][i] = stack[aIdx][i] % b;
                modMask |= (1ULL << i);
              } else {
                stack[sp][i] = 0;
              }
            }
            validity[sp] = validity[aIdx] & validity[bIdx] & modMask;
            sp++;
            break;
          }
          case OpCode::CMP_GT_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] > stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::CMP_GE_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] >= stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::CMP_LT_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] < stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::CMP_LE_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] <= stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::CMP_EQ_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] == stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::CMP_NE_INT: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (stack[aIdx][i] != stack[bIdx][i]) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & validity[bIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::RET:
            break;
        }
      }

      uint64_t filterMask = 0;
      if (sp > 0) {
        uint64_t valid = validity[sp - 1] & batchMask;
#pragma GCC unroll 8
        for (size_t i = 0; i < batchSize; ++i) {
          if ((valid & (1ULL << i)) && stack[sp - 1][i] != 0) {
            filterMask |= (1ULL << i);
          }
        }
      }

      while (filterMask != 0) {
        uint32_t idx = std::countr_zero(filterMask);
        resultTable.push_back(inputTable[rowOffset + idx]);
        filterMask &= filterMask - 1;
      }
    }
  }
};

}  // namespace ql::engine::jit
