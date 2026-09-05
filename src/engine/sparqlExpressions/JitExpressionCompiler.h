// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <asmjit/asmjit.h>

#include <bit>
#include <cstdint>
#include <memory>
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
// Represents an in-memory executable native machine-code filter kernel.
// Evaluates a contiguous morsel of 64 rows, writing a 64-bit match bitmask.
//
// Signature:
//   size_t (*)(const uint64_t* const* columnPtrs, size_t numRows, uint64_t*
//   outMatchMask)
// Returns total number of matching rows in the morsel.
using NativeFilterMorselFn = size_t (*)(const uint64_t* const*, size_t,
                                        uint64_t*);

// _____________________________________________________________________________
class JitCompiledExpression {
 private:
  std::shared_ptr<asmjit::JitRuntime> runtime_;
  NativeFilterMorselFn fn_ = nullptr;
  std::vector<ColumnIndex> referencedColumns_;

 public:
  JitCompiledExpression(std::shared_ptr<asmjit::JitRuntime> runtime,
                        NativeFilterMorselFn fn,
                        std::vector<ColumnIndex> referencedColumns)
      : runtime_(std::move(runtime)),
        fn_(fn),
        referencedColumns_(std::move(referencedColumns)) {}

  ~JitCompiledExpression() {
    if (runtime_ && fn_) {
      runtime_->release(fn_);
    }
  }

  // Non-copyable, move-only (RAII for JIT executable page allocation)
  JitCompiledExpression(const JitCompiledExpression&) = delete;
  JitCompiledExpression& operator=(const JitCompiledExpression&) = delete;
  JitCompiledExpression(JitCompiledExpression&& other) noexcept
      : runtime_(std::move(other.runtime_)),
        fn_(other.fn_),
        referencedColumns_(std::move(other.referencedColumns_)) {
    other.fn_ = nullptr;
  }
  JitCompiledExpression& operator=(JitCompiledExpression&& other) noexcept {
    if (this != &other) {
      if (runtime_ && fn_) {
        runtime_->release(fn_);
      }
      runtime_ = std::move(other.runtime_);
      fn_ = other.fn_;
      referencedColumns_ = std::move(other.referencedColumns_);
      other.fn_ = nullptr;
    }
    return *this;
  }

  [[nodiscard]] NativeFilterMorselFn function() const noexcept { return fn_; }
  [[nodiscard]] const std::vector<ColumnIndex>& referencedColumns()
      const noexcept {
    return referencedColumns_;
  }

  // Execute native compiled filter over an input IdTable and append matching
  // rows into resultTable.
  template <int WIDTH, typename Table>
  void executeFilter(
      const Table& inputTable, IdTableStatic<WIDTH>& resultTable,
      ad_utility::SharedCancellationHandle cancellationHandle = nullptr) const {
    size_t numRows = inputTable.size();
    if (numRows == 0 || !fn_) {
      return;
    }

    constexpr size_t MORSEL_SIZE = 64;

    // Collect raw column pointer array for referenced columns
    std::vector<const uint64_t*> rawColumns;
    rawColumns.reserve(referencedColumns_.size());
    for (ColumnIndex col : referencedColumns_) {
      auto span = inputTable.getColumn(col);
      rawColumns.push_back(reinterpret_cast<const uint64_t*>(span.data()));
    }

    std::vector<const uint64_t*> morselColPtrs(referencedColumns_.size());

    for (size_t rowOffset = 0; rowOffset < numRows; rowOffset += MORSEL_SIZE) {
      if (cancellationHandle && (rowOffset % (MORSEL_SIZE * 1024) == 0)) {
        cancellationHandle->throwIfCancelled();
      }

      size_t batchSize = std::min<size_t>(MORSEL_SIZE, numRows - rowOffset);
      for (size_t i = 0; i < referencedColumns_.size(); ++i) {
        morselColPtrs[i] = rawColumns[i] + rowOffset;
      }

      uint64_t matchMask = 0;
      fn_(morselColPtrs.data(), batchSize, &matchMask);

      // Fast compaction: copy matching rows into result table
      while (matchMask != 0) {
        uint32_t idx = std::countr_zero(matchMask);
        resultTable.push_back(inputTable[rowOffset + idx]);
        matchMask &= matchMask - 1;
      }
    }
  }
};

// _____________________________________________________________________________
// Master compiler from SPARQL Expression AST to native x86-64 machine code.
class JitExpressionCompiler {
 public:
  // Compile expression AST to native machine code. Returns std::nullopt if
  // expression has unsupported constructs or types.
  static std::optional<JitCompiledExpression> compile(
      const sparqlExpression::SparqlExpression& expr,
      const VariableToColumnMap& varColMap);
};

}  // namespace ql::engine::jit
