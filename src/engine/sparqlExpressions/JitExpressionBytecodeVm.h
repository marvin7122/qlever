// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <algorithm>
#include <bit>
#include <cstdint>
#include <optional>
#include <vector>

#include "backports/span.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"

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
  // Raw `ValueId` bit operations for index-folded string filters (see
  // `tryFoldStringFilterToJit`). `LOAD_COL_ID` pushes the uninterpreted
  // 64-bit ID of a column, `CMP_EQ_ID` compares raw ID bits for equality,
  // and `IN_ID_RANGE` checks `lo <= id < hi` in unsigned `ValueId` bit
  // order, where `arg` indexes the program's ID range table. `OR_BOOL`
  // pops two boolean values and pushes their disjunction, `AND_BOOL` their
  // conjunction. Both implement Kleene three-valued logic together with the
  // kernels' validity masks: a `True` operand decides the `OR` (a `False`
  // operand the `AND`) even when the other operand is invalid, exactly like
  // the legacy `AndLambda`/`OrLambda` (where invalid means dropped rows).
  LOAD_COL_ID,
  CMP_EQ_ID,
  IN_ID_RANGE,
  OR_BOOL,
  AND_BOOL,
  // Date support for `YEAR(?var)`. `LOAD_COL_DATE` pushes the uninterpreted
  // 64-bit ID of a column (invalid unless the cell holds a `Date`),
  // `YEAR_DATE` replaces raw date bits by the year and is invalid for
  // non-date payloads (including durations), mirroring `ExtractYear`.
  LOAD_COL_DATE,
  YEAR_DATE,
  RET
};

struct Instruction {
  OpCode op;
  int64_t arg = 0;
};

// Exactness precondition for executing a program over table cells whose
// datatypes are only known at runtime. The integer morsel kernels treat
// every non-`Int` (non-`Bool`) cell as invalid (`UNDEF`), which matches the
// legacy evaluation except in the following cases:
// * `Double` cells: legacy arithmetic, comparisons and `=` compute doubles.
// * `Date` cells under `ADD`/`SUB`: legacy computes dates.
// * Any non-`Int` cell under the native machine-code backend, which has no
//   validity concept and reinterprets raw `ValueId` bits.
enum class CellRule {
  // Bitwise `ID` programs (string/IRI folds, prefix ranges): exact for
  // every cell datatype except `LocalVocabIndex`, for which the legacy
  // evaluation compares string-aware while the kernels compare raw bits.
  BitwiseExact,
  // Folded `?var = <int>`: legacy `=` compares integers and doubles
  // numerically (`5.0 == 5`), so `Double` cells must be absent. (`Bool`
  // and `Undefined` cells need no guard: the legacy `=` drops them just
  // like the bitwise comparison.)
  FoldIntEquality,
  // Integer arithmetic (`ADD`/`SUB`/`MUL`/`MOD`): requires the absence of
  // `Double` and `Date` cells.
  IntegerArithmetic,
  // Programs with comparisons: only `Int`, `Bool` and `Undefined` cells
  // are exact (legacy compares strings, dates and mixed numerics).
  OrderedComparison,
  // Programs with `YEAR_DATE`: like `OrderedComparison`, but `Date` cells
  // are additionally allowed. Exactness then requires that every `Date`
  // cell only reaches `YEAR_DATE` positions (a bare integer load or
  // comparison of a date diverges: the kernels drop the row while the
  // legacy evaluation computes date arithmetic or ordering). This is
  // enforced per column by `satisfiesCellRule`, using the load kinds
  // derived from the program's instructions.
  YearExtraction,
};

// How a program loads a column, derived from its `LOAD_*` instructions.
// Used to constrain `Date` cells to `YEAR_DATE` positions (see
// `CellRule::YearExtraction`).
enum class ColumnLoadKind { Int, Id, Date };

// Decode the year from raw `ValueId` bits, mirroring `ExtractYear` (see
// `DateExpressions.cpp`) exactly: the legacy `DateValueGetter` yields the
// stored `DateYearOrDuration` for every `Date` cell (including durations)
// and `ExtractYear` applies `DateYearOrDuration::getYear` to it, so calling
// the same two functions here is bit-identical by construction, including
// for directly stored large years and durations. Returns `nullopt` for
// non-`Date` cells, for which the legacy evaluation yields `UNDEF` (dropped
// rows, like the kernels' invalid lanes). Implemented once so all backends
// share it.
inline std::optional<int64_t> decodeYearFromDateBits(uint64_t rawBits) {
  const ValueId id = ValueId::fromBits(rawBits);
  if (id.getDatatype() != Datatype::Date) {
    return std::nullopt;
  }
  return id.getDate().getYear();
}

class JitBytecodeProgram {
 private:
  std::vector<Instruction> code_;
  std::vector<ColumnIndex> referencedColumns_;
  // Half-open `[lo, hi)` ranges of raw `ValueId` bits for `IN_ID_RANGE`.
  // Bounds use unsigned bit order, matching
  // `valueIdComparators::compareByBits`.
  std::vector<std::pair<uint64_t, uint64_t>> idRanges_;
  // Strictest rule by default, so a lowering site that forgets to set the
  // rule over-falls-back (performance) instead of diverging (correctness).
  CellRule cellRule_ = CellRule::OrderedComparison;

 public:
  void setCellRule(CellRule rule) noexcept { cellRule_ = rule; }
  [[nodiscard]] CellRule cellRule() const noexcept { return cellRule_; }
  void addInstruction(OpCode op, int64_t arg = 0) {
    code_.push_back({op, arg});
  }

  void addReferencedColumn(ColumnIndex col) {
    referencedColumns_.push_back(col);
  }

  // Store an ID range and return its index for `IN_ID_RANGE`.
  size_t addIdRange(uint64_t loBits, uint64_t hiBits) {
    idRanges_.emplace_back(loBits, hiBits);
    return idRanges_.size() - 1;
  }

  [[nodiscard]] const std::vector<Instruction>& instructions() const noexcept {
    return code_;
  }

  [[nodiscard]] const std::vector<ColumnIndex>& referencedColumns()
      const noexcept {
    return referencedColumns_;
  }

  [[nodiscard]] const std::vector<std::pair<uint64_t, uint64_t>>& idRanges()
      const noexcept {
    return idRanges_;
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
        case OpCode::LOAD_COL_ID:
          stack[sp++] = rowColumns[inst.arg];
          break;
        case OpCode::CMP_EQ_ID: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (a == b) ? 1 : 0;
          break;
        }
        case OpCode::IN_ID_RANGE: {
          uint64_t a = static_cast<uint64_t>(stack[--sp]);
          const auto& [lo, hi] = idRanges_.at(static_cast<size_t>(inst.arg));
          stack[sp++] = (lo <= a && a < hi) ? 1 : 0;
          break;
        }
        case OpCode::OR_BOOL: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (a || b) ? 1 : 0;
          break;
        }
        case OpCode::AND_BOOL: {
          int64_t b = stack[--sp];
          int64_t a = stack[--sp];
          stack[sp++] = (a && b) ? 1 : 0;
          break;
        }
        case OpCode::LOAD_COL_DATE:
          stack[sp++] = rowColumns[inst.arg];
          break;
        case OpCode::YEAR_DATE: {
          const auto year =
              decodeYearFromDateBits(static_cast<uint64_t>(stack[--sp]));
          stack[sp++] = year.value_or(0);
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
          case OpCode::LOAD_COL_ID: {
            const int64_t* colData = inputColumns[inst.arg] + rowOffset;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = colData[i];
            }
            validity[sp] = batchMask;
            sp++;
            break;
          }
          case OpCode::CMP_EQ_ID: {
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
          case OpCode::IN_ID_RANGE: {
            sp--;
            size_t aIdx = sp;
            const auto& [lo, hi] =
                program.idRanges().at(static_cast<size_t>(inst.arg));
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              auto bits = static_cast<uint64_t>(stack[aIdx][i]);
              if (lo <= bits && bits < hi) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::OR_BOOL: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            // Kleene logic: a `True` operand decides the disjunction even
            // when the other operand is invalid (matches `OrLambda`, where
            // invalid rows are dropped, i.e. only `True` keeps a row).
            uint64_t mask = 0;
            uint64_t decided = 0;
            const uint64_t validA = validity[aIdx];
            const uint64_t validB = validity[bIdx];
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              const bool a = stack[aIdx][i] != 0;
              const bool b = stack[bIdx][i] != 0;
              const bool knownA = (validA >> i) & 1;
              const bool knownB = (validB >> i) & 1;
              if ((a && knownA) || (b && knownB)) {
                mask |= (1ULL << i);
                decided |= (1ULL << i);
              } else if (knownA && knownB) {
                decided |= (1ULL << i);
              }
            }
            validity[sp] = decided & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::AND_BOOL: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            // Kleene logic: a `False` operand decides the conjunction even
            // when the other operand is invalid (matches `AndLambda`).
            uint64_t mask = 0;
            uint64_t decided = 0;
            const uint64_t validA = validity[aIdx];
            const uint64_t validB = validity[bIdx];
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              const bool a = stack[aIdx][i] != 0;
              const bool b = stack[bIdx][i] != 0;
              const bool knownA = (validA >> i) & 1;
              const bool knownB = (validB >> i) & 1;
              if ((!a && knownA) || (!b && knownB)) {
                decided |= (1ULL << i);
              } else if (knownA && knownB) {
                decided |= (1ULL << i);
                if (a && b) {
                  mask |= (1ULL << i);
                }
              }
            }
            validity[sp] = decided & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::LOAD_COL_DATE: {
            const int64_t* colData = inputColumns[inst.arg] + rowOffset;
            uint64_t dateMask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = colData[i];
              if (ValueId::fromBits(static_cast<uint64_t>(colData[i]))
                      .getDatatype() == Datatype::Date) {
                dateMask |= (1ULL << i);
              }
            }
            validity[sp] = dateMask;
            sp++;
            break;
          }
          case OpCode::YEAR_DATE: {
            sp--;
            size_t aIdx = sp;
            uint64_t yearMask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              const auto year =
                  decodeYearFromDateBits(static_cast<uint64_t>(stack[aIdx][i]));
              if (year.has_value()) {
                stack[sp][i] = year.value();
                yearMask |= (1ULL << i);
              } else {
                stack[sp][i] = 0;
              }
            }
            validity[sp] = yearMask & validity[aIdx] & batchMask;
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
          case OpCode::LOAD_COL_ID: {
            auto colSpan = inputTable.getColumn(inst.arg);
            const Id* colData = colSpan.data() + rowOffset;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = static_cast<int64_t>(colData[i].getBits());
            }
            validity[sp] = batchMask;
            sp++;
            break;
          }
          case OpCode::CMP_EQ_ID: {
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
          case OpCode::IN_ID_RANGE: {
            sp--;
            size_t aIdx = sp;
            const auto& [lo, hi] =
                program.idRanges().at(static_cast<size_t>(inst.arg));
            uint64_t mask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              auto bits = static_cast<uint64_t>(stack[aIdx][i]);
              if (lo <= bits && bits < hi) {
                mask |= (1ULL << i);
              }
            }
            validity[sp] = mask & validity[aIdx] & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::OR_BOOL: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            // Kleene logic: a `True` operand decides the disjunction even
            // when the other operand is invalid (matches `OrLambda`, where
            // invalid rows are dropped, i.e. only `True` keeps a row).
            uint64_t mask = 0;
            uint64_t decided = 0;
            const uint64_t validA = validity[aIdx];
            const uint64_t validB = validity[bIdx];
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              const bool a = stack[aIdx][i] != 0;
              const bool b = stack[bIdx][i] != 0;
              const bool knownA = (validA >> i) & 1;
              const bool knownB = (validB >> i) & 1;
              if ((a && knownA) || (b && knownB)) {
                mask |= (1ULL << i);
                decided |= (1ULL << i);
              } else if (knownA && knownB) {
                decided |= (1ULL << i);
              }
            }
            validity[sp] = decided & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::AND_BOOL: {
            sp--;
            size_t bIdx = sp;
            sp--;
            size_t aIdx = sp;
            // Kleene logic: a `False` operand decides the conjunction even
            // when the other operand is invalid (matches `AndLambda`).
            uint64_t mask = 0;
            uint64_t decided = 0;
            const uint64_t validA = validity[aIdx];
            const uint64_t validB = validity[bIdx];
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              const bool a = stack[aIdx][i] != 0;
              const bool b = stack[bIdx][i] != 0;
              const bool knownA = (validA >> i) & 1;
              const bool knownB = (validB >> i) & 1;
              if ((!a && knownA) || (!b && knownB)) {
                decided |= (1ULL << i);
              } else if (knownA && knownB) {
                decided |= (1ULL << i);
                if (a && b) {
                  mask |= (1ULL << i);
                }
              }
            }
            validity[sp] = decided & batchMask;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = (mask >> i) & 1;
            }
            sp++;
            break;
          }
          case OpCode::LOAD_COL_DATE: {
            auto colSpan = inputTable.getColumn(inst.arg);
            const Id* colData = colSpan.data() + rowOffset;
            uint64_t dateMask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              Id id = colData[i];
              stack[sp][i] = id.getBits();
              if (id.getDatatype() == Datatype::Date) {
                dateMask |= (1ULL << i);
              }
            }
            validity[sp] = dateMask;
            sp++;
            break;
          }
          case OpCode::YEAR_DATE: {
            sp--;
            size_t aIdx = sp;
            uint64_t yearMask = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              const auto year =
                  decodeYearFromDateBits(static_cast<uint64_t>(stack[aIdx][i]));
              if (year.has_value()) {
                stack[sp][i] = year.value();
                yearMask |= (1ULL << i);
              } else {
                stack[sp][i] = 0;
              }
            }
            validity[sp] = yearMask & validity[aIdx] & batchMask;
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

  // True if the program contains a `DIV_INT` instruction. The JIT backends
  // implement truncating integer division, while the legacy evaluation
  // divides via doubles (see `DivideImpl` in `NumericBinaryExpressions.cpp`):
  // `FILTER(?x / ?y)` keeps rows where the true quotient is nonzero but
  // truncates to zero (e.g. `1 / 2`), and keeps rows with infinite quotient
  // (e.g. `1 / 0`), so such programs must fall back to legacy evaluation.
  static bool containsDivision(const JitBytecodeProgram& program) {
    return std::any_of(
        program.instructions().begin(), program.instructions().end(),
        [](const Instruction& inst) { return inst.op == OpCode::DIV_INT; });
  }

  // True if the program evaluates to plain integers with legacy-identical
  // semantics: integer arithmetic over integer inputs only. Excluded are
  // `DIV_INT` (see `containsDivision`) as well as all comparison and ID
  // opcodes, which produce boolean (not integer) results in the legacy
  // evaluation and therefore must not be materialized as value columns.
  static bool hasExactIntegerSemantics(const JitBytecodeProgram& program) {
    return std::all_of(program.instructions().begin(),
                       program.instructions().end(),
                       [](const Instruction& inst) {
                         switch (inst.op) {
                           case OpCode::LOAD_COL_INT:
                           case OpCode::LOAD_CONST_INT:
                           case OpCode::ADD_INT:
                           case OpCode::SUB_INT:
                           case OpCode::MUL_INT:
                           case OpCode::MOD_INT:
                           case OpCode::RET:
                             return true;
                           default:
                             return false;
                         }
                       });
  }

  // Datatype presence in a program's referenced columns over a row range.
  // `Undefined` cells are always exact (both sides drop them) and are not
  // tracked.
  // Datatype presence flags shared by the program-wide `ColumnKinds` and
  // the per-column breakdown below.
  struct ColumnKindFlags {
    bool hasDouble = false;
    bool hasBool = false;
    bool hasDate = false;
    // `LocalVocabIndex` cells compare string-aware in the legacy evaluation
    // (against vocabulary-backed bounds), but bitwise in the kernels.
    bool hasLocalVocab = false;
    // Any cell that is not `Int`, `Bool`, `Undefined`, `Double`, `Date` or
    // `LocalVocabIndex` (vocabulary indices, literals, geo points, ...).
    bool hasOther = false;
  };

  struct ColumnKinds : ColumnKindFlags {
    bool allInt = true;
    // Per-column presence in first-use order without duplicates, used by
    // `CellRule::YearExtraction` to constrain `Date` cells to `YEAR_DATE`
    // positions (see `satisfiesCellRule`).
    std::vector<std::pair<ColumnIndex, ColumnKindFlags>> perColumn;
  };

  // Classify one cell into `flags`. Returns true for `Int` cells (used for
  // `ColumnKinds::allInt`).
  static bool classifyCellInto(Id id, ColumnKindFlags& flags) {
    switch (id.getDatatype()) {
      case Datatype::Int:
        return true;
      case Datatype::Double:
        flags.hasDouble = true;
        return false;
      case Datatype::Bool:
        flags.hasBool = true;
        return false;
      case Datatype::Date:
        flags.hasDate = true;
        return false;
      case Datatype::LocalVocabIndex:
        flags.hasLocalVocab = true;
        flags.hasOther = true;
        return false;
      case Datatype::Undefined:
        return false;
      default:
        flags.hasOther = true;
        return false;
    }
  }

  // Scan the program's referenced columns over `[begin, begin + numRows)`
  // for the datatypes that constrain exact execution (see `CellRule`).
  // Short-circuits once every kind is present. NOTE: the short-circuit only
  // fires when `hasDouble`, `hasBool`, `hasDate` and `hasOther` are all set,
  // which rejects every rule on the aggregate flags alone, so the possibly
  // incomplete `perColumn` breakdown stays sound.
  template <typename Table>
  static ColumnKinds scanColumnKinds(const JitBytecodeProgram& program,
                                     const Table& inputTable, size_t begin,
                                     size_t numRows,
                                     const ad_utility::SharedCancellationHandle&
                                         cancellationHandle = nullptr) {
    ColumnKinds kinds;
    size_t checked = 0;
    for (ColumnIndex col : program.referencedColumns()) {
      auto colSpan = inputTable.getColumn(col);
      const Id* colData = colSpan.data() + begin;
      ColumnKindFlags colFlags;
      for (size_t i = 0; i < numRows; ++i) {
        if (cancellationHandle && (++checked % (64 * 1024) == 0)) {
          cancellationHandle->throwIfCancelled();
        }
        if (!classifyCellInto(colData[i], colFlags)) {
          kinds.allInt = false;
        }
      }
      kinds.hasDouble = kinds.hasDouble || colFlags.hasDouble;
      kinds.hasBool = kinds.hasBool || colFlags.hasBool;
      kinds.hasDate = kinds.hasDate || colFlags.hasDate;
      kinds.hasLocalVocab = kinds.hasLocalVocab || colFlags.hasLocalVocab;
      kinds.hasOther = kinds.hasOther || colFlags.hasOther;
      auto sameColumn = [&col](const auto& entry) {
        return entry.first == col;
      };
      if (std::find_if(kinds.perColumn.begin(), kinds.perColumn.end(),
                       sameColumn) == kinds.perColumn.end()) {
        kinds.perColumn.emplace_back(col, colFlags);
      } else {
        // Duplicate references merge flags (idempotent for the aggregate).
        for (auto& entry : kinds.perColumn) {
          if (entry.first == col) {
            entry.second.hasDouble =
                entry.second.hasDouble || colFlags.hasDouble;
            entry.second.hasBool = entry.second.hasBool || colFlags.hasBool;
            entry.second.hasDate = entry.second.hasDate || colFlags.hasDate;
            entry.second.hasLocalVocab =
                entry.second.hasLocalVocab || colFlags.hasLocalVocab;
            entry.second.hasOther = entry.second.hasOther || colFlags.hasOther;
          }
        }
      }
      if (kinds.hasDouble && kinds.hasBool && kinds.hasDate && kinds.hasOther) {
        return kinds;
      }
    }
    return kinds;
  }

  // How a program loads each column, derived from its `LOAD_*` instructions
  // (at most one entry per load; columns may appear multiple times).
  static std::vector<std::pair<ColumnIndex, ColumnLoadKind>> columnLoadKinds(
      const JitBytecodeProgram& program) {
    std::vector<std::pair<ColumnIndex, ColumnLoadKind>> loads;
    for (const auto& inst : program.instructions()) {
      switch (inst.op) {
        case OpCode::LOAD_COL_INT:
          loads.emplace_back(static_cast<ColumnIndex>(inst.arg),
                             ColumnLoadKind::Int);
          break;
        case OpCode::LOAD_COL_ID:
          loads.emplace_back(static_cast<ColumnIndex>(inst.arg),
                             ColumnLoadKind::Id);
          break;
        case OpCode::LOAD_COL_DATE:
          loads.emplace_back(static_cast<ColumnIndex>(inst.arg),
                             ColumnLoadKind::Date);
          break;
        default:
          break;
      }
    }
    return loads;
  }

  // True if executing a program with the given `CellRule` over cells of the
  // scanned kinds matches the legacy evaluation (see `CellRule`). The
  // `YearExtraction` rule additionally constrains `Date` cells to
  // `YEAR_DATE` positions using the program's loads, so it takes the
  // program as well.
  static bool satisfiesCellRule(CellRule rule,
                                const JitBytecodeProgram& program,
                                const ColumnKinds& kinds) {
    switch (rule) {
      case CellRule::BitwiseExact:
        return !kinds.hasLocalVocab;
      case CellRule::FoldIntEquality:
        return !kinds.hasDouble;
      case CellRule::IntegerArithmetic:
        return !kinds.hasDouble && !kinds.hasDate;
      case CellRule::OrderedComparison:
        return !kinds.hasDouble && !kinds.hasBool && !kinds.hasDate &&
               !kinds.hasOther;
      case CellRule::YearExtraction:
        return satisfiesYearExtractionRule(program, kinds);
    }
    AD_FAIL();
  }

  // The `YearExtraction` rule (see `CellRule`): like `OrderedComparison`
  // but with `Date` cells allowed, provided every load of a date-containing
  // column is a `DATE` load. `Bool` cells stay excluded: integer loads map
  // them to 0/1 while the legacy numeric evaluation rejects them.
  static bool satisfiesYearExtractionRule(const JitBytecodeProgram& program,
                                          const ColumnKinds& kinds) {
    if (kinds.hasDouble || kinds.hasBool || kinds.hasOther) {
      return false;
    }
    if (!kinds.hasDate) {
      return true;
    }
    const auto loads = columnLoadKinds(program);
    for (const auto& [col, flags] : kinds.perColumn) {
      if (!flags.hasDate) {
        continue;
      }
      bool dateLoaded = false;
      for (const auto& [loadCol, kind] : loads) {
        if (loadCol == col) {
          if (kind != ColumnLoadKind::Date) {
            return false;
          }
          dateLoaded = true;
        }
      }
      // A date-containing column that the program never loads cannot affect
      // execution, but that contradicts the scan over referenced columns, so
      // reject defensively (performance, never correctness).
      if (!dateLoaded) {
        return false;
      }
    }
    return true;
  }

  // Evaluate an integer-valued program (see `hasExactIntegerSemantics`) over
  // `numRows` rows of `inputTable` starting at `inputBegin` and write the
  // results as `Id`s into `output` (`Int` where the row is valid, `UNDEF`
  // otherwise, matching the legacy evaluation).
  template <typename Table>
  static void executeIntColumnInto(
      const JitBytecodeProgram& program, const Table& inputTable,
      size_t inputBegin, size_t numRows, Id* output,
      ad_utility::SharedCancellationHandle cancellationHandle = nullptr) {
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
          batchSize == MORSEL_SIZE ? ~0ULL : ((1ULL << batchSize) - 1);
      size_t sp = 0;

      for (const auto& inst : program.instructions()) {
        switch (inst.op) {
          case OpCode::LOAD_COL_INT: {
            auto colSpan = inputTable.getColumn(inst.arg);
            const Id* colData = colSpan.data() + inputBegin + rowOffset;
            uint64_t valid = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              if (colData[i].getDatatype() == Datatype::Int) {
                stack[sp][i] = colData[i].getInt();
                valid |= (1ULL << i);
              } else if (colData[i].getDatatype() == Datatype::Bool) {
                stack[sp][i] = colData[i].getBool() ? 1 : 0;
                valid |= (1ULL << i);
              } else {
                stack[sp][i] = 0;
              }
            }
            validity[sp] = valid & batchMask;
            sp++;
            break;
          }
          case OpCode::LOAD_CONST_INT:
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = inst.arg;
            }
            validity[sp] = batchMask;
            sp++;
            break;
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
          case OpCode::RET:
            break;
          default:
            // Unreachable for programs satisfying `hasExactIntegerSemantics`
            // (see above): invalidate the slot.
            validity[sp] = 0;
#pragma GCC unroll 8
            for (size_t i = 0; i < batchSize; ++i) {
              stack[sp][i] = 0;
            }
            sp++;
            break;
        }
      }

      if (sp > 0) {
        uint64_t valid = validity[sp - 1] & batchMask;
#pragma GCC unroll 8
        for (size_t i = 0; i < batchSize; ++i) {
          output[rowOffset + i] = (valid & (1ULL << i))
                                      ? Id::makeFromInt(stack[sp - 1][i])
                                      : Id::makeUndefined();
        }
      }
    }
  }

  // Evaluate over all rows of `inputTable` and write the results into
  // `outputColumn` of `outputTable` (see `executeIntColumnInto`).
  // `outputTable` may be the same table as `inputTable` as long as
  // `outputColumn` holds no referenced input column.
  template <typename Table>
  static void executeIntColumn(
      const JitBytecodeProgram& program, const Table& inputTable,
      IdTable& outputTable, ColumnIndex outputColumn,
      ad_utility::SharedCancellationHandle cancellationHandle = nullptr) {
    if (inputTable.size() == 0) {
      return;
    }
    executeIntColumnInto(program, inputTable, 0, inputTable.size(),
                         &outputTable(0, outputColumn), cancellationHandle);
  }
};

}  // namespace ql::engine::jit
