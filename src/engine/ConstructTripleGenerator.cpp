// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleGenerator.h"

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructDeduplicator.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ConstructTripleInstantiator.h"
#include "global/RuntimeParameters.h"

#include <iterator>

namespace qlever::constructExport {

using ad_utility::InputRangeTypeErased;
using StringTriple = QueryExecutionTree::StringTriple;

//______________________________________________________________________________
IdCache ConstructTripleGenerator::makeIdCache(
    const PreprocessedConstructTemplate& tmpl) {
  return IdCache{std::max(tmpl.uniqueVariableColumns_.size(), size_t{1}) *
                 CACHE_ENTRIES_PER_VARIABLE};
}

namespace {

// Bundles the pieces `computeBatch` needs beyond the batch itself.
struct BatchEvalContext {
  BatchEvalContext(const PreprocessedConstructTemplate& preprocessedTemplate,
                   const Index& index, IdCache& cache,
                   const CancellationHandle& cancellationHandle,
                   std::shared_ptr<ConstructDeduplicator> deduplicator)
      : preprocessedTemplate_{preprocessedTemplate},
        index_{index},
        cache_{cache},
        cancellationHandle_{cancellationHandle},
        deduplicator_{std::move(deduplicator)} {}

  std::reference_wrapper<const PreprocessedConstructTemplate>
      preprocessedTemplate_;
  std::reference_wrapper<const Index> index_;
  std::reference_wrapper<IdCache> cache_;
  std::reference_wrapper<const CancellationHandle> cancellationHandle_;
  std::shared_ptr<ConstructDeduplicator> deduplicator_;
};

// The rows of one chunk form a contiguous index range, so the batch is
// [first index, first index + chunk size). Chunks from `views::chunk` are
// never empty.
CPP_template(typename ChunkView)(requires ranges::range<ChunkView>)
    BatchEvaluationContext batchContextFor(
        const TableConstRefWithVocab& tableWithVocab,
        const ChunkView& batch) {
  AD_CORRECTNESS_CHECK(!ql::ranges::empty(batch));
  const size_t batchBegin = *ql::ranges::begin(batch);
  const size_t batchEnd =
      batchBegin + static_cast<size_t>(ql::ranges::size(batch));
  return BatchEvaluationContext{tableWithVocab.idTable(), batchBegin,
                                batchEnd};
}

// Instantiate one evaluated batch. When `context.deduplicator_` is set,
// duplicate triples are dropped as they are instantiated (see
// `instantiateBatch`'s `DeduplicationParams`). Runs serially in row order;
// callers pair only the evaluation above and keep one call per batch in
// batch order, so deduplication decisions match the serial schedule.
std::vector<EvaluatedTriple> instantiateOne(
    const BatchEvalContext& context, const BatchEvaluationResult& batchResult,
    const BatchEvaluationContext& ctx, size_t tableRowOffset) {
  const size_t blankNodeBaseId = tableRowOffset + ctx.firstRow_;
  std::optional<DeduplicationParams> deduplication{std::nullopt};
  if (context.deduplicator_) {
    deduplication.emplace(DeduplicationParams{*context.deduplicator_, ctx});
  }
  return instantiateBatch(context.preprocessedTemplate_.get(), batchResult,
                          blankNodeBaseId, deduplication);
}

// Evaluate the rows covered by `batch.view_`. Cancellation is checked once at
// the start.
CPP_template(typename ChunkView)(requires ranges::range<ChunkView>)
    std::vector<EvaluatedTriple> computeBatch(
        const TableConstRefWithVocab& tableWithVocab, ChunkView batch,
        const BatchEvalContext& context, size_t tableRowOffset) {
  context.cancellationHandle_.get()->throwIfCancelled();
  const BatchEvaluationContext ctx = batchContextFor(tableWithVocab, batch);

  auto batchResult = ConstructBatchEvaluator::evaluateBatch(
      context.preprocessedTemplate_.get().uniqueVariableColumns_, ctx,
      tableWithVocab.localVocab(), context.index_, context.cache_);
  return instantiateOne(context, batchResult, ctx, tableRowOffset);
}

// Evaluate two consecutive chunks with shared fiber overlap (see
// `ConstructBatchEvaluator::evaluateBatchPair`) and instantiate both
// serially in batch order. Cancellation is checked once at the start, so a
// cancelled export still throws between batches, never mid-batch. The
// emitted triples are identical to two sequential `computeBatch` calls:
// evaluation results match by construction and instantiation keeps the
// serial order, including deduplication decisions.
CPP_template(typename FirstView, typename SecondView)(
    requires ranges::range<FirstView>&& ranges::range<SecondView>)
    std::vector<EvaluatedTriple> computeBatchPair(
        const TableConstRefWithVocab& tableWithVocab, FirstView first,
        SecondView second, const BatchEvalContext& context,
        size_t tableRowOffset) {
  context.cancellationHandle_.get()->throwIfCancelled();
  const BatchEvaluationContext firstCtx =
      batchContextFor(tableWithVocab, first);
  const BatchEvaluationContext secondCtx =
      batchContextFor(tableWithVocab, second);

  auto [firstResult, secondResult] =
      ConstructBatchEvaluator::evaluateBatchPair(
          context.preprocessedTemplate_.get().uniqueVariableColumns_,
          firstCtx, secondCtx, tableWithVocab.localVocab(), context.index_,
          context.cache_);
  std::vector<EvaluatedTriple> triples =
      instantiateOne(context, firstResult, firstCtx, tableRowOffset);
  auto secondTriples =
      instantiateOne(context, secondResult, secondCtx, tableRowOffset);
  triples.insert(triples.end(),
                 std::make_move_iterator(secondTriples.begin()),
                 std::make_move_iterator(secondTriples.end()));
  return triples;
}

// Chunks `table` into batches and evaluates each one. Takes `TableWithRange` by
// value and stores only value-captures in the returned view so the pipeline is
// self-contained w.r.t. the `table` handle (no reference to a caller's
// `TableWithRange` / parameter can dangle). `TableWithRange` itself is a cheap
// non-owning handle (`IdTableView` + `LocalVocab` ref); the underlying result
// storage must still outlive the whole export, as with every other CONSTRUCT
// export path.
auto processTableBatches(TableWithRange table, BatchEvalContext context,
                         size_t tableRowOffset) {
  // Copy the cheap pieces out first so neither `chunk` nor the transform
  // lambda retain a reference into the by-value `table` parameter.
  auto rowView = table.view_;
  const TableConstRefWithVocab tableWithVocab = table.tableWithVocab_;
  auto chunks = ranges::views::chunk(std::move(rowView),
                                     ConstructTripleGenerator::BATCH_SIZE);
  auto computeOne = [tableWithVocab, context = std::move(context),
                     tableRowOffset](auto chunkView) mutable {
    return computeBatch(tableWithVocab, chunkView, context, tableRowOffset);
  };
  if (!getRuntimeParameter<&RuntimeParameters::exportFiberOverlap_>()) {
    return InputRangeTypeErased<EvaluatedTriple>(
        std::move(chunks) | ql::views::transform(std::move(computeOne)) |
        ql::views::join);
  }
  // Overlap consecutive chunks in pairs (depth 2, the depth the NVMe
  // optimization arc saturates at): both chunks' miss resolutions share one
  // fiber wave while instantiation stays serial and ordered. A lone
  // trailing chunk evaluates alone. `view_` is an `iota_view`, so holding
  // both chunk views while the pair evaluates is safe.
  auto computePair = [tableWithVocab, context = std::move(context),
                      tableRowOffset](auto chunkPair) mutable {
    auto pairIt = ql::ranges::begin(chunkPair);
    auto first = *pairIt;
    ++pairIt;
    if (pairIt == ql::ranges::end(chunkPair)) {
      return computeBatch(tableWithVocab, first, context, tableRowOffset);
    }
    return computeBatchPair(tableWithVocab, first, *pairIt, context,
                            tableRowOffset);
  };
  return InputRangeTypeErased<EvaluatedTriple>(
      std::move(chunks) | ranges::views::chunk(2) |
      ql::views::transform(std::move(computePair)) | ql::views::join);
}
}  // namespace

//______________________________________________________________________________
InputRangeTypeErased<EvaluatedTriple> ConstructTripleGenerator::evaluateTables(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    const EvaluationConfig& config) {
  auto preprocessedTemplate = ConstructTemplatePreprocessor::preprocess(
      templateTriples, variableColumns, config.index_);
  IdCache cache = makeIdCache(preprocessedTemplate);

  std::shared_ptr<ConstructDeduplicator> deduplicator;
  if (!std::holds_alternative<DeduplicationMode::None>(config.mode_.value_)) {
    deduplicator =
        std::make_shared<ConstructDeduplicator>(config.mode_, config.qec_);
  }

  auto preprocessedTemplatePtr =
      std::make_shared<const PreprocessedConstructTemplate>(
          std::move(preprocessedTemplate));

  auto processTable =
      [preprocessedTemplate = std::move(preprocessedTemplatePtr),
       index = config.index_, cancellationHandle = config.cancellationHandle_,
       cache = std::move(cache), deduplicator = std::move(deduplicator),
       accumulatedRowOffset = rowOffset](const TableWithRange& table) mutable {
        const size_t numRowsOfTable = ql::ranges::size(table.view_);

        const size_t tableRowOffset = accumulatedRowOffset;
        accumulatedRowOffset += numRowsOfTable;

        const BatchEvalContext context{*preprocessedTemplate, index, cache,
                                       cancellationHandle, deduplicator};
        return processTableBatches(table, context, tableRowOffset);
      };

  auto pipeline = std::move(rowIndices) |
                  ql::views::transform(std::move(processTable)) |
                  ql::views::join;
  return InputRangeTypeErased(std::move(pipeline));
}

//______________________________________________________________________________
InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateFormattedTriples(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    ad_utility::MediaType mediaType, const EvaluationConfig& config) {
  auto evaluatedTriples =
      evaluateTables(templateTriples, variableColumns, std::move(rowIndices),
                     rowOffset, config);

  auto transformer = [mediaType](const EvaluatedTriple& triple) {
    return formatTriple(triple, mediaType);
  };
  return InputRangeTypeErased(std::move(evaluatedTriples) |
                              ql::views::transform(transformer));
}

//______________________________________________________________________________
InputRangeTypeErased<StringTriple>
ConstructTripleGenerator::generateStringTriples(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    const EvaluationConfig& config) {
  auto evaluatedTriples =
      evaluateTables(templateTriples, variableColumns, std::move(rowIndices),
                     rowOffset, config);

  auto transformer = [](const EvaluatedTriple& triple) {
    return createStringTriple(triple);
  };
  return InputRangeTypeErased(std::move(evaluatedTriples) |
                              ql::views::transform(transformer));
}

}  // namespace qlever::constructExport
