// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H

#include <absl/container/inlined_vector.h>

#include <memory>
#include <optional>
#include <vector>

#include "engine/ConstructTypes.h"
#include "engine/idTable/IdTable.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/LruCacheWithStatistics.h"
#include "util/Synchronized.h"

namespace qlever::constructExport {

// `EvaluatedVariableValues` is used to store the evaluation results
// (`EvaluatedTerm`s) for the values of a single variable across all rows in a
// batch. The i-th element corresponds to the i-th row in the batch, and is
// `nullopt` iff the variable was unbound for that row.
using EvaluatedVariableValues = std::vector<std::optional<EvaluatedTerm>>;

// Result of batch-evaluating all variables for a batch of rows. Stores the
// evaluated values per variable column and the number of rows in the batch.
struct BatchEvaluationResult {
  // `variablesByColumn_` maps a column index of the `Result` that is being
  // evaluated to the `EvaluatedVariableValues` for the variable that is stored
  // in that column. We use a hash map (instead of a dense vector) because the
  // set of evaluated columns may be sparse: some variables in the WHERE-clause
  // (in the `IdTable`) may not appear in the CONSTRUCT template and are thus
  // not evaluated.
  ad_utility::HashMap<ColumnIndex, EvaluatedVariableValues> variablesByColumn_;
  size_t numRows_ = 0;

  const std::optional<EvaluatedTerm>& getVariable(ColumnIndex columnIndex,
                                                  size_t rowInBatch) const {
    return variablesByColumn_.at(columnIndex).at(rowInBatch);
  }
};

using IdCache =
    ad_utility::util::LRUCacheWithStatistics<Id, std::optional<EvaluatedTerm>>;

// An `IdCache` that the two evaluating threads of the three-stage CONSTRUCT
// export pipeline share (see `ConstructBatchEvaluator::prepareBatch`).
using SynchronizedIdCache = ad_utility::Synchronized<IdCache>;

// The first part of the evaluation of a batch (see
// `ConstructBatchEvaluator::prepareBatch`): the values of all `Id`s that were
// found in the `IdCache` or need no vocabulary read, and the submitted
// vocabulary lookup of the remaining `VocabIndex` `Id`s. It owns everything it
// refers to, so it stays valid after the result block it was prepared from has
// been destroyed.
struct PreparedBatch {
  struct Column {
    ColumnIndex columnIndex_ = 0;
    // One entry per row of the batch; the rows of `pendingIds_` are filled by
    // `completeBatch`.
    EvaluatedVariableValues values_;
    // The `VocabIndex` `Id`s of the column that still have to be looked up
    // (unique, sorted), and for each of them the rows of the batch that hold
    // it.
    std::vector<Id> pendingIds_;
    std::vector<absl::InlinedVector<size_t, 3>> pendingRows_;
  };
  size_t numRows_ = 0;
  // The row id of the first row of the batch, used for blank node labels.
  size_t blankNodeBaseId_ = 0;
  std::vector<Column> columns_;
  // One lookup for the `pendingIds_` of all columns, in the order of
  // `columns_`; null iff no column has a pending `Id`.
  std::unique_ptr<VocabLookupHandleBase> lookup_;
};

// Identifies a contiguous sub-range of rows of an `IdTable` that forms one
// batch.
struct BatchEvaluationContext {
  // A non-owning view over the batch's `IdTable`. Stored by value: an
  // `IdTableView` is a lightweight handle (like `string_view`), so copying it
  // is cheap, and every caller already hands us a view
  // (`TableWithVocab::idTable()` returns `const IdTableView<0>&`). The viewed
  // table must outlive the context.
  IdTableView<0> idTable_;
  size_t firstRow_;
  size_t endRow_;  // exclusive

  BatchEvaluationContext(IdTableView<0> idTable, size_t firstRow, size_t endRow)
      : idTable_(std::move(idTable)), firstRow_(firstRow), endRow_(endRow) {
    AD_CONTRACT_CHECK(firstRow <= endRow);
    AD_CONTRACT_CHECK(endRow <= idTable_.numRows());
  }

  size_t numRows() const { return endRow_ - firstRow_; }
};

// Resolves `Id` values in variable columns to their string representations
// (IRI, literal, etc.) via `ConstructQueryEvaluator::evaluateId`.
//
// The evaluation is column-oriented: for each variable (identified by their
// `IdTable` column), all rows in the batch are evaluated before moving to the
// next variable.
//
// An `IdCache` (LRU cache keyed by `Id`) avoids redundant evaluation of the
// same `Id` across rows and batches.
class ConstructBatchEvaluator {
 public:
  // Evaluates the variables identified by `variableColumnIndices` for all rows
  // in `evaluationContext`. Each entry in `variableColumnIndices` is an
  // `IdTable` column index representing a variable in the CONSTRUCT template.
  static BatchEvaluationResult evaluateBatch(
      ql::span<const ColumnIndex> variableColumnIndices,
      const BatchEvaluationContext& evaluationContext,
      const LocalVocab& localVocab, const Index& index, IdCache& idCache);

  // `evaluateBatch` split into two parts that can run on different threads.
  // `prepareBatch` needs the result block of `evaluationContext` (and
  // `localVocab`) only while it runs: it looks up every `Id` in `idCache`,
  // resolves the misses that need no vocabulary read (encoded values, local
  // vocabulary entries), and submits one vocabulary lookup for the remaining
  // `VocabIndex` `Id`s without waiting for it. `completeBatch` waits for that
  // lookup, inserts the new values into `idCache`, and returns the same
  // result as `evaluateBatch`.
  static PreparedBatch prepareBatch(
      ql::span<const ColumnIndex> variableColumnIndices,
      const BatchEvaluationContext& evaluationContext,
      const LocalVocab& localVocab, const Index& index,
      SynchronizedIdCache& idCache);
  static BatchEvaluationResult completeBatch(PreparedBatch& prepared,
                                             const Index& index,
                                             SynchronizedIdCache& idCache);

 private:
  // Evaluate a single variable (identified by its `IdTable` column index)
  // across all rows in the batch.
  static EvaluatedVariableValues evaluateVariableByColumn(
      size_t idTableColumnIdx, const BatchEvaluationContext& ctx,
      const LocalVocab& localVocab, const Index& index, IdCache& idCache);

  // Convert the result of `ExportIds::idToStringAndType` to an `EvaluatedTerm`.
  static std::optional<EvaluatedTerm> stringAndTypeToEvaluatedTerm(
      std::optional<std::pair<std::string, const char*>>&& optStringAndType);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
