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
#include "util/OrderedTaskWindow.h"

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
  return ranges::views::chunk(std::move(rowView),
                              ConstructTripleGenerator::BATCH_SIZE) |
         ql::views::transform([tableWithVocab, context = std::move(context),
                               tableRowOffset](auto chunkView) {
           return computeBatch(tableWithVocab, chunkView, context,
                               tableRowOffset);
         }) |
         ql::views::join;
}
// Formats the triples of one batch into one string in `mediaType`. The result
// equals the concatenation of the strings that the sequential export yields
// for these triples.
std::string formatBatch(ql::span<const EvaluatedTriple> triples,
                        ad_utility::MediaType mediaType, bool fastTurtle) {
  if (fastTurtle) {
    return formatTriplesAsTurtle(triples);
  }
  std::string result;
  for (const auto& triple : triples) {
    result += formatTriple(triple, mediaType);
  }
  return result;
}

// The state of `formatTablesInParallel`: the batches of the current table are
// evaluated and formatted by the workers of `window_`, and their strings are
// returned in order. The next table is only requested from `tables_` once all
// batches of the current one have been returned, because advancing a lazy
// result may free the current table.
class ParallelFormatState {
 public:
  ParallelFormatState(
      std::shared_ptr<const PreprocessedConstructTemplate> preprocessedTemplate,
      const Index& index, CancellationHandle cancellationHandle,
      InputRangeTypeErased<TableWithRange> tables, size_t rowOffset,
      ad_utility::MediaType mediaType, bool fastTurtle, size_t numWorkers,
      size_t cacheCapacity)
      : preprocessedTemplate_{std::move(preprocessedTemplate)},
        index_{index},
        cancellationHandle_{std::move(cancellationHandle)},
        tables_{std::move(tables)},
        accumulatedRowOffset_{rowOffset},
        mediaType_{mediaType},
        fastTurtle_{fastTurtle} {
    caches_.reserve(numWorkers);
    for (size_t i = 0; i < numWorkers; ++i) {
      caches_.push_back(std::make_unique<IdCache>(cacheCapacity));
    }
    // Two batches per worker keep every worker busy while the consumer takes
    // the oldest result.
    window_.emplace(numWorkers, 2 * numWorkers);
  }

  // Return the string of the next non-empty batch, `std::nullopt` at the end.
  std::optional<std::string> next() {
    while (true) {
      while (table_.has_value() && nextRow_ < endRow_ && !window_->full()) {
        submitNextBatch();
      }
      if (!window_->empty()) {
        std::string batch = window_->popFront();
        if (batch.empty()) {
          continue;
        }
        return batch;
      }
      // All batches of the current table are done, advance to the next one.
      table_.reset();
      auto table = tables_.get();
      if (!table.has_value()) {
        return std::nullopt;
      }
      table_ = table->tableWithVocab_;
      nextRow_ = *ql::ranges::begin(table->view_);
      endRow_ = nextRow_ + ql::ranges::size(table->view_);
      tableRowOffset_ = accumulatedRowOffset_;
      accumulatedRowOffset_ += ql::ranges::size(table->view_);
    }
  }

 private:
  // Submit the batch `[nextRow_, nextRow_ + BATCH_SIZE)` of the current table.
  void submitNextBatch() {
    const uint64_t batchBegin = nextRow_;
    const uint64_t batchEnd = std::min(
        endRow_, batchBegin + uint64_t{ConstructTripleGenerator::BATCH_SIZE});
    nextRow_ = batchEnd;
    window_->submit(ad_utility::OrderedTaskWindow<std::string>::Task{
        [this, table = table_.value(), batchBegin, batchEnd,
         tableRowOffset = tableRowOffset_](size_t workerIndex) {
          const BatchEvalContext context{*preprocessedTemplate_, index_.get(),
                                         *caches_.at(workerIndex),
                                         cancellationHandle_, nullptr};
          auto triples =
              computeBatch(table, ::ranges::views::iota(batchBegin, batchEnd),
                           context, tableRowOffset);
          return formatBatch(triples, mediaType_, fastTurtle_);
        }});
  }

  std::shared_ptr<const PreprocessedConstructTemplate> preprocessedTemplate_;
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;
  InputRangeTypeErased<TableWithRange> tables_;
  size_t accumulatedRowOffset_;
  ad_utility::MediaType mediaType_;
  bool fastTurtle_;
  // One `IdCache` per worker, so the workers share no mutable state.
  std::vector<std::unique_ptr<IdCache>> caches_;
  std::optional<TableConstRefWithVocab> table_;
  size_t tableRowOffset_ = 0;
  uint64_t nextRow_ = 0;
  uint64_t endRow_ = 0;
  // Declared last, so the workers (and with them all tasks, which reference
  // the members above) are joined first on destruction.
  std::optional<ad_utility::OrderedTaskWindow<std::string>> window_;
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
  // The runtime parameters are read once per export, not once per triple.
  const bool fastTurtle =
      mediaType == ad_utility::MediaType::turtle &&
      getRuntimeParameter<&RuntimeParameters::useFastExportStreamFormatter_>();
#ifdef __EMSCRIPTEN__
  // No worker threads in the WebAssembly build.
  const size_t numThreads = 1;
#else
  const size_t numThreads = ad_utility::resolveNumThreads(
      getRuntimeParameter<&RuntimeParameters::constructExportNumThreads_>());
#endif
  // Deduplication depends on the order in which the triples are seen, so it
  // keeps the sequential path.
  if (numThreads > 1 &&
      std::holds_alternative<DeduplicationMode::None>(config.mode_.value_)) {
    return formatTablesInParallel(templateTriples, variableColumns,
                                  std::move(rowIndices), rowOffset, mediaType,
                                  fastTurtle, config, numThreads);
  }

  auto evaluatedTriples =
      evaluateTables(templateTriples, variableColumns, std::move(rowIndices),
                     rowOffset, config);

  if (fastTurtle) {
    return formatTriplesAsTurtleInBatches(std::move(evaluatedTriples));
  }
  auto transformer = [mediaType](const EvaluatedTriple& triple) {
    return formatTriple(triple, mediaType);
  };
  return InputRangeTypeErased(std::move(evaluatedTriples) |
                              ql::views::transform(transformer));
}

//______________________________________________________________________________
InputRangeTypeErased<std::string>
ConstructTripleGenerator::formatTablesInParallel(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    ad_utility::MediaType mediaType, bool fastTurtle,
    const EvaluationConfig& config, size_t numThreads) {
  AD_CONTRACT_CHECK(numThreads > 1);
  AD_CONTRACT_CHECK(
      std::holds_alternative<DeduplicationMode::None>(config.mode_.value_));
  AD_CONTRACT_CHECK(!fastTurtle || mediaType == ad_utility::MediaType::turtle);
  auto preprocessedTemplate = ConstructTemplatePreprocessor::preprocess(
      templateTriples, variableColumns, config.index_);
  const size_t cacheCapacity = makeIdCache(preprocessedTemplate).capacity();
  auto state = std::make_unique<ParallelFormatState>(
      std::make_shared<const PreprocessedConstructTemplate>(
          std::move(preprocessedTemplate)),
      config.index_.get(), config.cancellationHandle_, std::move(rowIndices),
      rowOffset, mediaType, fastTurtle, numThreads, cacheCapacity);
  return InputRangeTypeErased<std::string>{
      ad_utility::InputRangeFromGetCallable{
          [state = std::move(state)]() { return state->next(); }}};
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
