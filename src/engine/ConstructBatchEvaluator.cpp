// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructBatchEvaluator.h"

#include <absl/strings/str_cat.h>

#include "engine/ExportQueryExecutionTrees.h"
#include "global/Constants.h"
#include "util/Exception.h"
#include "util/Views.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
BatchEvaluationResult ConstructBatchEvaluator::evaluateBatch(
    ql::span<const size_t> variableColumnIndices,
    const BatchEvaluationContext& evaluationContext,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = evaluationContext.numRows();
  batchResult.variablesByColumn_.reserve(variableColumnIndices.size());

  for (size_t variableColumnIdx : variableColumnIndices) {
    batchResult.variablesByColumn_.push_back(evaluateVariableByColumn(
        variableColumnIdx, evaluationContext, localVocab, index, idCache));
  }

  return batchResult;
}

// _____________________________________________________________________________
std::optional<EvaluatedTerm>
ConstructBatchEvaluator::stringAndTypeToEvaluatedTerm(
    std::optional<std::pair<std::string, const char*>> optStringAndType) {
  if (!optStringAndType.has_value()) return std::nullopt;
  auto& [str, type] = optStringAndType.value();
  const char* i = XSD_INT_TYPE;
  const char* d = XSD_DECIMAL_TYPE;
  const char* b = XSD_BOOLEAN_TYPE;
  // Note: If `type` is `XSD_DOUBLE_TYPE`, `str` is always "NaN", "INF" or
  // "-INF", which doesn't have a short form notation.
  if (type == nullptr || type == i || type == d ||
      (type == b && str.length() > 1)) {
    return std::make_shared<const std::string>(std::move(str));
  }
  return std::make_shared<const std::string>(
      absl::StrCat("\"", str, "\"^^<", type, ">"));
}

// _____________________________________________________________________________
std::optional<EvaluatedTerm> ConstructBatchEvaluator::idToEvaluatedTerm(
    const Index& index, Id id, const LocalVocab& localVocab) {
  return stringAndTypeToEvaluatedTerm(
      ExportQueryExecutionTrees::idToStringAndType(index, id, localVocab));
}

// _____________________________________________________________________________
EvaluatedVariableValues ConstructBatchEvaluator::evaluateVariableByColumn(
    size_t idTableColumnIdx, const BatchEvaluationContext& ctx,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  decltype(auto) col = ctx.idTable_.getColumn(idTableColumnIdx);
  const size_t numRows = ctx.numRows();

  // Build a `(Id, rowInBatch)` index vector and sort by `Id`. This ensures
  // that `VocabIndex` IDs form a contiguous, sorted block (see
  // `idsToStringAndType`), converting vocabulary lookups from random-access
  // reads to sequential reads for I/O locality.
  std::vector<std::pair<Id, size_t>> sortedIndices;
  sortedIndices.reserve(numRows);
  for (size_t i = 0; i < numRows; ++i) {
    sortedIndices.emplace_back(col[ctx.firstRow_ + i], i);
  }
  ql::ranges::sort(sortedIndices, [](const auto& a, const auto& b) {
    return a.first < b.first;
  });

  // Phase 1: check the cache for each sorted ID. Scatter hits directly to
  // `result`; collect misses for batch resolution.
  EvaluatedVariableValues result(numRows);
  std::vector<Id> missIds;
  std::vector<size_t> missRows;
  for (const auto& [id, rowInBatch] : sortedIndices) {
    auto cached = idCache.tryGet(id);
    if (cached) {
      result[rowInBatch] = *cached;
    } else {
      missIds.push_back(id);
      missRows.push_back(rowInBatch);
    }
  }

  // Phase 2: batch-resolve cache misses. `missIds` is sorted (collected from
  // `sortedIndices`), satisfying the `idsToStringAndType` precondition for
  // sequential VocabIndex I/O. Each miss is inserted into the cache via
  // `getOrCompute`; duplicate IDs in `missIds` are handled correctly: the
  // second occurrence is already in cache and the lambda is not called.
  auto missResolved =
      ExportQueryExecutionTrees::idsToStringAndType(index, missIds, localVocab);
  for (size_t i = 0; i < missIds.size(); ++i) {
    result[missRows[i]] =
        idCache.getOrCompute(missIds[i], [&missResolved, i](const Id&) {
          return stringAndTypeToEvaluatedTerm(std::move(missResolved[i]));
        });
  }
  return result;
}

// _____________________________________________________________________________

}  // namespace qlever::constructExport
