// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructBatchEvaluator.h"

#include <type_traits>
#include <variant>

#include "index/ExportIds.h"

namespace qlever::constructExport {
namespace {

// Convert one borrowed-or-owned batch-lookup result to an `EvaluatedTerm`. A
// borrowed vocabulary term takes over the shared owner, so a word read from
// the on-disk vocabulary remains shared instead of being copied. An owned term
// is materialized inline as before, without extra ownership bookkeeping.
std::optional<EvaluatedTerm> borrowedOrOwnedToEvaluatedTerm(
    std::optional<ql::exportIds::BorrowedOrOwnedStringAndType>&&
        optStringAndType) {
  if (!optStringAndType.has_value()) {
    return std::nullopt;
  }
  return std::visit(
      [](auto&& arg) -> EvaluatedTerm {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::pair<std::string, const char*>>) {
          return std::make_shared<const EvaluatedTermData>(std::move(arg.first),
                                                           arg.second);
        } else {
          return std::make_shared<const EvaluatedTermData>(
              arg.value_, arg.type_, std::move(arg.owner_));
        }
      },
      std::move(optStringAndType.value()));
}
}  // namespace

// _____________________________________________________________________________
BatchEvaluationResult ConstructBatchEvaluator::evaluateBatch(
    ql::span<const ColumnIndex> variableColumnIndices,
    const BatchEvaluationContext& evaluationContext,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = evaluationContext.numRows();

  for (size_t variableColumnIdx : variableColumnIndices) {
    auto [it, wasNew] = batchResult.variablesByColumn_.emplace(
        variableColumnIdx,
        evaluateVariableByColumn(variableColumnIdx, evaluationContext,
                                 localVocab, index, idCache));
    AD_CORRECTNESS_CHECK(wasNew);
  }

  return batchResult;
}

// _____________________________________________________________________________
EvaluatedVariableValues ConstructBatchEvaluator::evaluateVariableByColumn(
    size_t idTableColumnIdx, const BatchEvaluationContext& ctx,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  decltype(auto) col = ctx.idTable_.getColumn(idTableColumnIdx)
                           .subspan(ctx.firstRow_, ctx.numRows());

  const size_t numRows = ctx.numRows();

  // Build a `(rowInBatch, Id)` index vector and sort by `Id`. This ensures
  // that `VocabIndex` IDs form a contiguous, sorted block (see
  // `idsToBorrowedStringAndType`), converting vocabulary lookups from
  // random-access reads to sequential reads for I/O locality.
  auto sortedIndices = ::ranges::to_vector(::ranges::views::enumerate(col));

  ql::ranges::sort(sortedIndices, {}, ad_utility::second);

  // Phase 1: check the cache for each sorted ID. Scatter hits directly to
  // `result`; collect misses for batch resolution.
  EvaluatedVariableValues result(numRows);
  // Unique `Id`s not found in `idCache`, in sorted order (inherited from
  // `sortedIndices`). Each entry corresponds to the entry at the same index
  // in `missRows`.
  std::vector<Id> missIds;
  // For each entry in `missIds`, the batch row indices that hold that `Id`.
  std::vector<absl::InlinedVector<size_t, 3>> missRows;
  for (const auto& [rowInBatch, id] : sortedIndices) {
    auto cached = idCache.tryGet(id);
    if (cached) {
      // Note that a `LocalVocabIndex` Id may well produce a hit here, even
      // though such Ids are never inserted into `idCache` (see the comment in
      // Phase 2). `Id`s do not compare and hash bitwise: a `LocalVocabIndex`
      // Id whose term also exists in the index vocabulary compares equal to,
      // and hashes like, the corresponding `VocabIndex` Id (see
      // `ValueId::compareThreeWay` and `AbslHashValue` in `ValueId.h`). Such a
      // hit is safe, because the matched entry was inserted under a
      // `VocabIndex` key and therefore does not point into any block-local
      // `LocalVocab`; and it is correct, because equal `Id`s denote the same
      // RDF term.
      result[rowInBatch] = cached.value();
    } else if (!missIds.empty() && missIds.back() == id) {
      missRows.back().push_back(static_cast<size_t>(rowInBatch));
    } else {
      missIds.push_back(id);
      missRows.push_back({static_cast<size_t>(rowInBatch)});
    }
  }

  // Phase 2: batch-resolve cache misses. `missIds` is deduplicated and sorted
  // (inherited from `sortedIndices`), satisfying the
  // `idsToBorrowedStringAndType` precondition for sequential VocabIndex I/O.
  // Vocabulary terms borrow their bytes from the shared batch-lookup storage,
  // which each cached `EvaluatedTerm` keeps alive via its owner.
  // `LocalVocabIndex` Ids are
  // resolved per block but never inserted into `idCache`: the
  // `LocalVocabEntry` they point to is owned by the current result block's
  // `LocalVocab` and would dangle once the export advances to the next block,
  // making a later hash-colliding lookup compare against freed memory
  // (heap-use-after-free in `LocalVocabEntry::compareThreeWay`). Resolving
  // them per block is fine performance-wise: `LocalVocabEntry`s live in RAM,
  // so there is no disk I/O to amortize across batches.
  auto missResolved =
      ql::exportIds::idsToBorrowedStringAndType(index, missIds, localVocab);
  for (auto&& [id, resolved, rows] :
       ::ranges::views::zip(missIds, missResolved, missRows)) {
    auto evaluate = [&resolved](const Id&) {
      return borrowedOrOwnedToEvaluatedTerm(std::move(resolved));
    };
    const std::optional<EvaluatedTerm> evaluated =
        id.getDatatype() == Datatype::LocalVocabIndex
            ? evaluate(id)
            : idCache.getOrCompute(id, evaluate);
    for (const size_t row : rows) {
      result[row] = evaluated;
    }
  }
  return result;
}

}  // namespace qlever::constructExport
