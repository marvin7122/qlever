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
#include "global/RuntimeParameters.h"
#include "index/ExportIds.h"
#include "util/FiberIoScheduler.h"

namespace qlever::constructExport {

namespace {

// Per-column intermediate state between the three evaluation phases below.
struct ColumnWork {
  // The `IdTable` column index being evaluated.
  ColumnIndex columnIdx_;
  // Resolved values per batch row; cache hits are scattered in phase A, cache
  // misses in phase C.
  EvaluatedVariableValues result_;
  // Unique `Id`s not found in `idCache`, in sorted order; entry `i`
  // corresponds to `missRows_[i]` and (after phase B) to `missResolved_[i]`.
  std::vector<Id> missIds_;
  // For each entry in `missIds_`, the batch row indices holding that `Id`.
  std::vector<absl::InlinedVector<size_t, 3>> missRows_;
  // Phase B output: the resolved miss strings, parallel to `missIds_`.
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
// `idsToStringAndType` precondition for sequential VocabIndex I/O. The
// depth-2 variant keeps the lookup of the next vocabulary sub-batch in flight
// while the current one is consumed. Reads only `index`/`localVocab` (plus at
// most two pooled I/O managers per caller), so concurrent phase B bodies share
// no mutable state and may run as fibers. Only called for columns with
// misses (phase B skips the others).
void resolveColumnMisses(const Index& index, const LocalVocab& localVocab,
                         ColumnWork& work) {
  AD_CORRECTNESS_CHECK(!work.missIds_.empty());
  work.missResolved_ =
      ql::exportIds::idsToStringAndTypeDepth2(index, work.missIds_, localVocab);
}

// Resolve the slice `[begin, end)` of `work.missIds_` into the same slice of
// the pre-sized `work.missResolved_`. Disjoint slices of one column share no
// mutable state, so they may run as concurrent fibers.
void resolveColumnMissSlice(const Index& index, const LocalVocab& localVocab,
                            ColumnWork& work, size_t begin, size_t end) {
  auto ids = ql::span<const Id>{work.missIds_}.subspan(begin, end - begin);
  auto resolved =
      ql::exportIds::idsToStringAndTypeDepth2(index, ids, localVocab);
  std::move(resolved.begin(), resolved.end(),
            work.missResolved_.begin() + begin);
}

// Convert the result of `ExportIds::idToStringAndType` to an `EvaluatedTerm`.
std::optional<EvaluatedTerm> stringAndTypeToEvaluatedTerm(
    std::optional<std::pair<std::string, const char*>>&& optStringAndType) {
  if (!optStringAndType.has_value()) return std::nullopt;
  auto& [str, type] = optStringAndType.value();
  return std::make_shared<const EvaluatedTermData>(std::move(str), type);
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
      return stringAndTypeToEvaluatedTerm(std::move(resolved));
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

// _____________________________________________________________________________
BatchEvaluationResult ConstructBatchEvaluator::evaluateBatch(
    ql::span<const ColumnIndex> variableColumnIndices,
    const BatchEvaluationContext& evaluationContext,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = evaluationContext.numRows();

  // Phase A for every column, sequentially.
  std::vector<ColumnWork> columns;
  columns.reserve(variableColumnIndices.size());
  for (size_t variableColumnIdx : variableColumnIndices) {
    ColumnWork& work = columns.emplace_back();
    work.columnIdx_ = variableColumnIdx;
    collectColumnMisses(variableColumnIdx, evaluationContext, idCache, work);
  }

  // Phase B in waves of concurrent fibers, so one thread keeps several
  // lookup batches in flight. Only columns with misses take part. Research
  // knobs: each column's misses are split into up to
  // `construct-export-fibers-per-column` contiguous slices of at least
  // `maxVocabIndicesPerSubBatch` ids, and at most
  // `construct-export-max-fibers` slices run concurrently. The defaults (1
  // slice per column, 4 fibers) are the column waves of 4: each column holds
  // up to two pooled I/O managers (depth-2 lookup), so a wave uses the 8
  // managers the vocabulary creates up front. The pool does not block when
  // it is empty (it creates a manager). A lone slice skips fibers.
  const size_t slicesPerColumn = getRuntimeParameter<
      &RuntimeParameters::constructExportFibersPerColumn_>();
  const size_t maxFibers =
      getRuntimeParameter<&RuntimeParameters::constructExportMaxFibers_>();
  struct Slice {
    size_t column_;
    size_t begin_;
    size_t end_;
    bool whole_;
  };
  std::vector<Slice> slices;
  for (size_t i = 0; i < columns.size(); ++i) {
    const size_t n = columns[i].missIds_.size();
    if (n == 0) {
      continue;
    }
    const size_t k = std::max<size_t>(
        1, std::min(slicesPerColumn,
                    n / ql::exportIds::maxVocabIndicesPerSubBatch));
    if (k == 1) {
      slices.push_back({i, 0, n, true});
      continue;
    }
    columns[i].missResolved_.resize(n);
    for (size_t j = 0; j < k; ++j) {
      slices.push_back({i, n * j / k, n * (j + 1) / k, false});
    }
  }
  auto runSlice = [&index, &localVocab, &columns](const Slice& slice) {
    if (slice.whole_) {
      resolveColumnMisses(index, localVocab, columns[slice.column_]);
    } else {
      resolveColumnMissSlice(index, localVocab, columns[slice.column_],
                             slice.begin_, slice.end_);
    }
  };
  for (size_t begin = 0; begin < slices.size(); begin += maxFibers) {
    const size_t end = std::min(begin + maxFibers, slices.size());
    if (end - begin == 1) {
      runSlice(slices[begin]);
      continue;
    }
    std::vector<std::function<void()>> bodies;
    bodies.reserve(end - begin);
    for (size_t s = begin; s < end; ++s) {
      bodies.emplace_back([&runSlice, &slices, s]() { runSlice(slices[s]); });
    }
    ad_utility::FiberIoScheduler::runAsFibers(std::move(bodies));
  }

  // Phase C for every column in order, then publish. Identical to the
  // sequential evaluation, including the duplicate-column contract check.
  for (ColumnWork& work : columns) {
    scatterColumnResolved(work, idCache);
    auto [it, wasNew] = batchResult.variablesByColumn_.emplace(
        work.columnIdx_, std::move(work.result_));
    AD_CORRECTNESS_CHECK(wasNew);
  }

  return batchResult;
}

}  // namespace qlever::constructExport
