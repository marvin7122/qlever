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
#include "util/AsyncStream.h"

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

// Evaluate the rows covered by `batch.view_`. Cancellation is checked once at
// the start. When `context.deduplicator_` is set, duplicate triples are
// dropped as they are instantiated (see `instantiateBatch`'s
// `DeduplicationParams`).
CPP_template(typename ChunkView)(requires ranges::range<ChunkView>)
    std::vector<EvaluatedTriple> computeBatch(
        const TableConstRefWithVocab& tableWithVocab, ChunkView batch,
        const BatchEvalContext& context, size_t tableRowOffset) {
  context.cancellationHandle_.get()->throwIfCancelled();
  AD_CORRECTNESS_CHECK(!ql::ranges::empty(batch));

  const size_t batchBegin = *ql::ranges::begin(batch);
  const size_t batchEnd =
      batchBegin + static_cast<size_t>(ql::ranges::size(batch));

  const BatchEvaluationContext ctx{tableWithVocab.idTable(), batchBegin,
                                   batchEnd};

  auto batchResult = ConstructBatchEvaluator::evaluateBatch(
      context.preprocessedTemplate_.get().uniqueVariableColumns_, ctx,
      tableWithVocab.localVocab(), context.index_, context.cache_);

  const size_t blankNodeBaseId = tableRowOffset + batchBegin;

  std::optional<DeduplicationParams> deduplication{std::nullopt};
  if (context.deduplicator_) {
    deduplication.emplace(DeduplicationParams{*context.deduplicator_, ctx});
  }
  return instantiateBatch(context.preprocessedTemplate_.get(), batchResult,
                          blankNodeBaseId, deduplication);
}

// Chunks `table` into batches and evaluates each one; the returned view yields
// one `std::vector<EvaluatedTriple>` per batch. Takes `TableWithRange` by
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
  return ranges::views::chunk(std::move(rowView),
                              ConstructTripleGenerator::BATCH_SIZE) |
         ql::views::transform([tableWithVocab, context = std::move(context),
                               tableRowOffset](auto chunkView) {
           return computeBatch(tableWithVocab, chunkView, context,
                               tableRowOffset);
         });
}

// Stage 1 of the three-stage pipeline (see `evaluateTables`): chunks `table`
// into batches and prepares each one (`ConstructBatchEvaluator::prepareBatch`).
// The same lifetime rules as for `processTableBatches` apply.
auto prepareTableBatches(
    TableWithRange table,
    std::shared_ptr<const PreprocessedConstructTemplate> preprocessedTemplate,
    std::reference_wrapper<const Index> index,
    std::shared_ptr<SynchronizedIdCache> cache,
    ad_utility::SharedCancellationHandle cancellationHandle,
    size_t tableRowOffset) {
  auto rowView = table.view_;
  const TableConstRefWithVocab tableWithVocab = table.tableWithVocab_;
  return ranges::views::chunk(std::move(rowView),
                              ConstructTripleGenerator::BATCH_SIZE) |
         ql::views::transform(
             [tableWithVocab,
              preprocessedTemplate = std::move(preprocessedTemplate), index,
              cache = std::move(cache),
              cancellationHandle = std::move(cancellationHandle),
              tableRowOffset](auto chunkView) {
               cancellationHandle->throwIfCancelled();
               AD_CORRECTNESS_CHECK(!ql::ranges::empty(chunkView));
               const size_t batchBegin = *ql::ranges::begin(chunkView);
               const size_t batchEnd =
                   batchBegin +
                   static_cast<size_t>(ql::ranges::size(chunkView));
               const BatchEvaluationContext ctx{tableWithVocab.idTable(),
                                                batchBegin, batchEnd};
               PreparedBatch prepared = ConstructBatchEvaluator::prepareBatch(
                   preprocessedTemplate->uniqueVariableColumns_, ctx,
                   tableWithVocab.localVocab(), index.get(), *cache);
               prepared.blankNodeBaseId_ = tableRowOffset + batchBegin;
               return prepared;
             });
}

// Stage 2 of the three-stage pipeline: completes the prepared batches
// (`ConstructBatchEvaluator::completeBatch`) and instantiates their triples.
class CompletedBatches
    : public ad_utility::InputRangeFromGet<std::vector<EvaluatedTriple>> {
 public:
  CompletedBatches(
      InputRangeTypeErased<PreparedBatch> preparedBatches,
      std::shared_ptr<const PreprocessedConstructTemplate> preprocessedTemplate,
      std::reference_wrapper<const Index> index,
      std::shared_ptr<SynchronizedIdCache> cache)
      : preparedBatches_{std::move(preparedBatches)},
        preprocessedTemplate_{std::move(preprocessedTemplate)},
        index_{index},
        cache_{std::move(cache)} {}

  std::optional<std::vector<EvaluatedTriple>> get() override {
    auto prepared = preparedBatches_.get();
    if (!prepared.has_value()) {
      return std::nullopt;
    }
    const BatchEvaluationResult batchResult =
        ConstructBatchEvaluator::completeBatch(prepared.value(), index_.get(),
                                               *cache_);
    return instantiateBatch(*preprocessedTemplate_, batchResult,
                            prepared->blankNodeBaseId_);
  }

 private:
  InputRangeTypeErased<PreparedBatch> preparedBatches_;
  std::shared_ptr<const PreprocessedConstructTemplate> preprocessedTemplate_;
  std::reference_wrapper<const Index> index_;
  std::shared_ptr<SynchronizedIdCache> cache_;
};

// Yields the triples of `batches` one by one, in order.
class FlattenedBatches : public ad_utility::InputRangeFromGet<EvaluatedTriple> {
 public:
  explicit FlattenedBatches(
      InputRangeTypeErased<std::vector<EvaluatedTriple>> batches)
      : batches_{std::move(batches)} {}

  std::optional<EvaluatedTriple> get() override {
    while (next_ == currentBatch_.size()) {
      auto batch = batches_.get();
      if (!batch.has_value()) {
        return std::nullopt;
      }
      currentBatch_ = std::move(batch.value());
      next_ = 0;
    }
    return std::move(currentBatch_[next_++]);
  }

 private:
  InputRangeTypeErased<std::vector<EvaluatedTriple>> batches_;
  std::vector<EvaluatedTriple> currentBatch_;
  size_t next_ = 0;
};
}  // namespace

//______________________________________________________________________________
InputRangeTypeErased<EvaluatedTriple> ConstructTripleGenerator::evaluateTables(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    const EvaluationConfig& config) {
  auto preprocessedTemplate = ConstructTemplatePreprocessor::preprocess(
      templateTriples, variableColumns, config.index_);
  IdCache cache = makeIdCache(preprocessedTemplate);

  const QueryExecutionContext& qec = config.qec_;
  std::shared_ptr<ConstructDeduplicator> deduplicator;
  if (!std::holds_alternative<DeduplicationMode::None>(config.mode_.value_)) {
    deduplicator =
        qec.makeShared<ConstructDeduplicator>(config.mode_, config.qec_);
  }

  auto preprocessedTemplatePtr =
      qec.makeShared<const PreprocessedConstructTemplate>(
          std::move(preprocessedTemplate));

  // With a pipeline depth N > 0, the batches are evaluated on other threads
  // than the consuming one, which formats them, and each stage hands its
  // batches to the next one through a queue of N batches. With
  // `construct-export-pipeline-split-lookup` (only without deduplication),
  // the export has three stages: thread 1 computes the result blocks, looks up
  // the `Id`s in the cache and submits the vocabulary lookup of each batch
  // (`prepareBatch`); thread 2 waits for the lookup, decodes the words and
  // instantiates the triples (`completeBatch`); the consumer formats. The two
  // evaluating threads share the `IdCache` through a mutex. Otherwise one
  // thread evaluates the batches; the `IdCache` and the deduplicator are only
  // used by that thread.
  const size_t pipelineDepth =
      getRuntimeParameter<&RuntimeParameters::constructExportPipelineDepth_>();
  if (pipelineDepth > 0 && !deduplicator &&
      getRuntimeParameter<
          &RuntimeParameters::constructExportPipelineSplitLookup_>()) {
    auto sharedCache = std::make_shared<SynchronizedIdCache>(std::move(cache));
    auto prepareTable = [preprocessedTemplate = preprocessedTemplatePtr,
                         index = config.index_,
                         cancellationHandle = config.cancellationHandle_,
                         cache = sharedCache, accumulatedRowOffset = rowOffset](
                            const TableWithRange& table) mutable {
      const size_t tableRowOffset = accumulatedRowOffset;
      accumulatedRowOffset += ql::ranges::size(table.view_);
      return prepareTableBatches(table, preprocessedTemplate, index, cache,
                                 cancellationHandle, tableRowOffset);
    };
    InputRangeTypeErased<PreparedBatch> prepared{
        std::move(rowIndices) | ql::views::transform(std::move(prepareTable)) |
        ql::views::join};
    prepared =
        ad_utility::streams::runStreamAsync(std::move(prepared), pipelineDepth);
    InputRangeTypeErased<std::vector<EvaluatedTriple>> completed{
        CompletedBatches{std::move(prepared), preprocessedTemplatePtr,
                         config.index_, std::move(sharedCache)}};
    completed = ad_utility::streams::runStreamAsync(std::move(completed),
                                                    pipelineDepth);
    return InputRangeTypeErased<EvaluatedTriple>{
        FlattenedBatches{std::move(completed)}};
  }

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

  InputRangeTypeErased<std::vector<EvaluatedTriple>> batches{
      std::move(rowIndices) | ql::views::transform(std::move(processTable)) |
      ql::views::join};

  if (pipelineDepth > 0) {
    batches =
        ad_utility::streams::runStreamAsync(std::move(batches), pipelineDepth);
  }
  return InputRangeTypeErased<EvaluatedTriple>{
      FlattenedBatches{std::move(batches)}};
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
