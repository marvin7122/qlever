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

namespace {

// Convert an already-resolved `optional<pair<string, type>>` (as returned by
// `ExportQueryExecutionTrees::idsToStringAndType`) to an `EvaluatedTerm`.
// Returns `std::nullopt` if the input is `std::nullopt` (undefined Id).
std::optional<EvaluatedTerm> toEvaluatedTerm(
    std::optional<std::pair<std::string, const char*>> optStringAndType) {
  if (!optStringAndType.has_value()) return std::nullopt;
  auto [str, type] = std::move(*optStringAndType);
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

}  // namespace

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
EvaluatedVariableValues ConstructBatchEvaluator::evaluateVariableByColumn(
    size_t idTableColumnIdx, const BatchEvaluationContext& ctx,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  decltype(auto) col = ctx.idTable_.getColumn(idTableColumnIdx);
  const size_t numRows = ctx.numRows();

  EvaluatedVariableValues result{numRows};

  // First pass: serve cache hits immediately; collect cache-miss positions and
  // their `Id`s for batch resolution.
  std::vector<size_t> cacheMissRowIndices;
  std::vector<Id> cacheMisses;
  for (size_t i = 0; i < numRows; ++i) {
    Id id = col[ctx.firstRow_ + i];
    if (auto cached = idCache.tryGet(id)) {
      result[i] = *cached;
    } else {
      cacheMissRowIndices.push_back(i);
      cacheMisses.push_back(id);
    }
  }

  // Batch-resolve cache misses. `idsToStringAndType` sorts `VocabIndex` `Id`s
  // internally for I/O locality.
  auto resolved = ExportQueryExecutionTrees::idsToStringAndType(
      index, cacheMisses, localVocab);

  // Convert, insert into the cache, and scatter into the result. For repeated
  // miss IDs (same Id appears twice in `cacheMisses`), subsequent
  // `getOrCompute` calls will be cache hits and return the shared_ptr stored by
  // the first.
  for (size_t j = 0; j < cacheMissRowIndices.size(); ++j) {
    auto evaluatedTerm = toEvaluatedTerm(std::move(resolved[j]));
    result[cacheMissRowIndices[j]] = idCache.getOrCompute(
        cacheMisses[j], [&evaluatedTerm](const Id&) { return evaluatedTerm; });
  }
  return result;
}

}  // namespace qlever::constructExport
