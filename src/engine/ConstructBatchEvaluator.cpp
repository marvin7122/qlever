// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructBatchEvaluator.h"

#include <algorithm>
#include <functional>
#include <vector>

#include "global/Constants.h"
#include "index/ExportIds.h"
#include "util/FiberIoScheduler.h"

namespace qlever::constructExport {

namespace {

// ColumnWork holds intermediate state between evaluation phases.
// - columnIdx_: the column index being evaluated
// - result_: resolved values per row (cache hits from phase A, misses from
//   phase C)
// - missIds_: unique IDs not found in idCache, in sorted order
// - missRows_: batch row indices for each missed ID
// - missResolved_: resolved string representations of missed IDs
struct ColumnWork {
  ColumnIndex columnIdx_;
  EvaluatedVariableValues result_;
  std::vector<Id> missIds_;
  std::vector<absl::InlinedVector<size_t, 3>> missRows_;
  std::vector<std::optional<std::pair<std::string, const char*>>> missResolved_;
};

// Phase A: sort the column, check the cache, scatter hits to
// `work.result_`, and collect misses into `work.missIds_`/`missRows_`. Pure
// CPU work, always runs on the calling thread.
void collectColumnMisses(size_t idTableColumnIdx,
                         const BatchEvaluationContext& ctx, IdCache& idCache,
                         ColumnWork& work) {
  decltype(auto) col = ctx.idTable_.getColumn(idTableColumnIdx)
                           .subspan(ctx.firstRow_, ctx.numRows());

  const size_t numRows = ctx.numRows();

  // Build a `(rowInBatch, Id)` index vector and sort by `Id`. This ensures
  // that `VocabIndex` IDs form a contiguous, sorted block (see
  // `idsToStringAndType`), converting vocabulary lookups from random-access
  // reads to sequential reads for I/O locality.
  auto sortedIndices = ::ranges::to_vector(::ranges::views::enumerate(col));

  ql::ranges::sort(sortedIndices, {}, ad_utility::second);

  // Check the cache for each sorted ID. Scatter hits directly to `result_`;
  // collect misses for batch resolution.
  work.result_ = EvaluatedVariableValues(numRows);
  // Unique `Id`s not found in `idCache`, in sorted order (inherited from
  // `sortedIndices`). Each entry corresponds to the entry at the same index
  // in `missRows`.
  for (const auto& [rowInBatch, id] : sortedIndices) {
    auto cached = idCache.tryGet(id);
    if (cached) {
      // Note that a `LocalVocabIndex` Id may well produce a hit here, even
      // though such Ids are never inserted into `idCache` (see the comment in
      // phase C). `Id`s do not compare and hash bitwise: a `LocalVocabIndex`
      // Id whose term also exists in the index vocabulary compares equal to,
      // and hashes like, the corresponding `VocabIndex` Id (see
      // `ValueId::compareThreeWay` and `AbslHashValue` in `ValueId.h`). Such a
      // hit is safe, because the matched entry was inserted under a
      // `VocabIndex` key and therefore does not point into any block-local
      // `LocalVocab`; and it is correct, because equal `Id`s denote the same
      // RDF term.
      work.result_[rowInBatch] = cached.value();
    } else if (!work.missIds_.empty() && work.missIds_.back() == id) {
      work.missRows_.back().push_back(static_cast<size_t>(rowInBatch));
    } else {
      work.missIds_.push_back(id);
      work.missRows_.push_back({static_cast<size_t>(rowInBatch)});
    }
  }
}

// Phase B: batch-resolve the collected misses. `missIds_` is deduplicated
// and sorted (inherited from the phase A sort), satisfying the
// `idsToStringAndType` precondition for sequential VocabIndex I/O. Reads
// only `index`/`localVocab` (plus a pooled I/O manager, one per caller), so
// concurrent phase B bodies share no mutable state and may run as fibers.
void resolveColumnMisses(const Index& index, const LocalVocab& localVocab,
                         ColumnWork& work) {
  if (work.missIds_.empty()) {
    return;
  }
  work.missResolved_ =
      ql::exportIds::idsToStringAndType(index, work.missIds_, localVocab);
}

// Phase C: insert the resolved misses into `idCache` and scatter them to
// `work.result_`. Runs on the calling thread in column order, exactly as the
// sequential evaluation would, so cache insertion order (and hence LRU
// eviction) is unaffected by phase B concurrency.
void scatterColumnResolved(ColumnWork& work, IdCache& idCache) {
  for (auto&& [id, resolved, rows] : ::ranges::views::zip(
           work.missIds_, work.missResolved_, work.missRows_)) {
    // Init-capture (not a reference capture): the factory moves from the
    // lambda's own member, so a repeated invocation could never observe a
    // moved-from outer element. `getOrCompute` invokes the factory at most
    // once, but the capture makes that a non-requirement.
    auto evaluate = [resolved = std::move(resolved)](const Id&) mutable {
      return ConstructBatchEvaluator::stringAndTypeToEvaluatedTerm(
          std::move(resolved));
    };
    // `LocalVocabIndex` Ids are resolved per block but never inserted into
    // `idCache`: the `LocalVocabEntry` they point to is owned by the current
    // result block's `LocalVocab` and would dangle once the export advances
    // to the next block, making a later hash-colliding lookup compare against
    // freed memory (heap-use-after-free in `LocalVocabEntry::compareThreeWay`).
    // Resolving them per block is fine performance-wise: `LocalVocabEntry`s
    // live in RAM, so there is no disk I/O to amortize across batches.
    const std::optional<EvaluatedTerm> evaluated =
        id.getDatatype() == Datatype::LocalVocabIndex
            ? evaluate(id)
            : idCache.getOrCompute(id, evaluate);
    for (const size_t row : rows) {
      work.result_[row] = evaluated;
    }
  }
}

}  // namespace

// Phase A for every column of one batch, sequentially.
void collectBatchMisses(ql::span<const ColumnIndex> variableColumnIndices,
                        const BatchEvaluationContext& evaluationContext,
                        IdCache& idCache, std::vector<ColumnWork>& columns) {
  columns.reserve(columns.size() + variableColumnIndices.size());
  for (size_t variableColumnIdx : variableColumnIndices) {
    ColumnWork& work = columns.emplace_back();
    work.columnIdx_ = variableColumnIdx;
    collectColumnMisses(variableColumnIdx, evaluationContext, idCache, work);
  }
}

// Phase B over an arbitrary set of columns (possibly from two batches) in
// waves of concurrent fibers, so one thread keeps several lookup batches in
// flight (design step 1). Each in-flight column pops one I/O manager from
// the pool, and `pop()` blocks once the pool is empty, so a wave holds at
// most `NUM_VOCAB_BATCH_IO_MANAGERS` columns: more concurrent fibers than
// managers would deadlock the thread. A lone resolvable column skips fibers
// (no overlap possible, avoid the setup). The pointees must outlive the
// call; phase B only reads `index`/`localVocab` (plus a pooled I/O manager),
// so concurrent bodies share no mutable state.
void resolveColumnsInWaves(const Index& index, const LocalVocab& localVocab,
                           const std::vector<ColumnWork*>& columns) {
  constexpr size_t kMaxConcurrentColumns = NUM_VOCAB_BATCH_IO_MANAGERS;
  for (size_t begin = 0; begin < columns.size();
       begin += kMaxConcurrentColumns) {
    const size_t end = std::min(begin + kMaxConcurrentColumns, columns.size());
    std::vector<ColumnWork*> resolvable;
    for (size_t i = begin; i < end; ++i) {
      if (!columns[i]->missIds_.empty()) {
        resolvable.push_back(columns[i]);
      }
    }
    if (resolvable.empty()) {
      continue;
    }
    if (resolvable.size() == 1) {
      resolveColumnMisses(index, localVocab, *resolvable[0]);
      continue;
    }
    std::vector<std::function<void()>> bodies;
    bodies.reserve(resolvable.size());
    for (ColumnWork* work : resolvable) {
      bodies.emplace_back([&index, &localVocab, work]() {
        resolveColumnMisses(index, localVocab, *work);
      });
    }
    ad_utility::FiberIoScheduler::runAsFibers(std::move(bodies));
  }
}

// Phase C for every column of one batch in order, then publish. Identical
// to the sequential evaluation, including the duplicate-column contract
// check.
void scatterBatchResolved(std::vector<ColumnWork>& columns, IdCache& idCache,
                          BatchEvaluationResult& batchResult) {
  for (ColumnWork& work : columns) {
    scatterColumnResolved(work, idCache);
    auto [it, wasNew] = batchResult.variablesByColumn_.emplace(
        work.columnIdx_, std::move(work.result_));
    AD_CORRECTNESS_CHECK(wasNew);
  }
}

// Pointers into `columns`, in order. Valid until `columns` is mutated.
std::vector<ColumnWork*> columnPointers(std::vector<ColumnWork>& columns) {
  std::vector<ColumnWork*> pointers;
  pointers.reserve(columns.size());
  for (ColumnWork& work : columns) {
    pointers.push_back(&work);
  }
  return pointers;
}

// _____________________________________________________________________________
BatchEvaluationResult ConstructBatchEvaluator::evaluateBatch(
    ql::span<const ColumnIndex> variableColumnIndices,
    const BatchEvaluationContext& evaluationContext,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = evaluationContext.numRows();

  std::vector<ColumnWork> columns;
  collectBatchMisses(variableColumnIndices, evaluationContext, idCache,
                     columns);
  resolveColumnsInWaves(index, localVocab, columnPointers(columns));
  scatterBatchResolved(columns, idCache, batchResult);

  return batchResult;
}

// _____________________________________________________________________________
std::pair<BatchEvaluationResult, BatchEvaluationResult>
ConstructBatchEvaluator::evaluateBatchPair(
    ql::span<const ColumnIndex> variableColumnIndices,
    const BatchEvaluationContext& firstContext,
    const BatchEvaluationContext& secondContext,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  // Phase A for both batches, sequentially. No I/O happens here, so no
  // fiber can yield between the two batches' cache checks.
  std::vector<ColumnWork> firstColumns;
  std::vector<ColumnWork> secondColumns;
  collectBatchMisses(variableColumnIndices, firstContext, idCache,
                     firstColumns);
  collectBatchMisses(variableColumnIndices, secondContext, idCache,
                     secondColumns);

  // Phase B for both batches' columns in shared waves, so the second
  // batch's I/O waits overlap the first batch's. Phase B bodies share no
  // mutable state (see `resolveColumnMisses`).
  auto firstPointers = columnPointers(firstColumns);
  auto secondPointers = columnPointers(secondColumns);
  std::vector<ColumnWork*> bothPointers;
  bothPointers.reserve(firstPointers.size() + secondPointers.size());
  bothPointers.insert(bothPointers.end(), firstPointers.begin(),
                      firstPointers.end());
  bothPointers.insert(bothPointers.end(), secondPointers.begin(),
                      secondPointers.end());
  resolveColumnsInWaves(index, localVocab, bothPointers);

  // Phase C per batch in order, exactly as the sequential evaluation would
  // run it, so cache insertion order (and hence LRU eviction) matches the
  // serial schedule batch for batch.
  BatchEvaluationResult firstResult;
  firstResult.numRows_ = firstContext.numRows();
  BatchEvaluationResult secondResult;
  secondResult.numRows_ = secondContext.numRows();
  scatterBatchResolved(firstColumns, idCache, firstResult);
  scatterBatchResolved(secondColumns, idCache, secondResult);

  return {std::move(firstResult), std::move(secondResult)};
}

// _____________________________________________________________________________
std::optional<EvaluatedTerm>
ConstructBatchEvaluator::stringAndTypeToEvaluatedTerm(
    std::optional<std::pair<std::string, const char*>>&& optStringAndType) {
  if (!optStringAndType.has_value()) return std::nullopt;
  auto& [str, type] = optStringAndType.value();
  return std::make_shared<const EvaluatedTermData>(std::move(str), type);
}

}  // namespace qlever::constructExport
