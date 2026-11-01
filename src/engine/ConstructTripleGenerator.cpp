// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleGenerator.h"

#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <deque>
#include <future>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructDeduplicator.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ConstructTripleInstantiator.h"
#include "global/RuntimeParameters.h"
#include "util/GlobalExecutor.h"
#include "util/Log.h"

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

// Reads the vocabulary words of the next batches of one table on the global
// executor while the current batch is evaluated, so that their pages are
// already in the vocabulary's buffer pool (or the page cache) when the batch
// is evaluated (see `export-vocabulary-prefetch-batches`). The lookups are
// only for their side effect; their results are discarded, and the batch
// itself still looks up its words as before.
class VocabularyPrefetcher {
 public:
  // Prefetch for the batches of `ConstructTripleGenerator::BATCH_SIZE` rows
  // of `table` in `[tableBegin, tableEnd)`, reading the `columns`, up to
  // `distance` batches ahead of the evaluated one.
  VocabularyPrefetcher(const Index& index, TableConstRefWithVocab table,
                       std::vector<ColumnIndex> columns, uint64_t tableBegin,
                       uint64_t tableEnd, size_t distance)
      : index_{index},
        table_{table},
        columns_{std::move(columns)},
        tableEnd_{tableEnd},
        distance_{distance},
        nextToLaunch_{tableBegin + ConstructTripleGenerator::BATCH_SIZE} {}

  // Wait for the prefetches that are still running: they would otherwise
  // outlive the export.
  ~VocabularyPrefetcher() {
    for (auto& [batchBegin, prefetch] : inFlight_) {
      prefetch.wait();
    }
  }

  VocabularyPrefetcher(const VocabularyPrefetcher&) = delete;
  VocabularyPrefetcher& operator=(const VocabularyPrefetcher&) = delete;

  // Call before the batch starting at row `batchBegin` is evaluated: wait for
  // its own prefetch (the batch would otherwise read the same pages a second
  // time), then start the prefetches of the next `distance` batches.
  void beforeBatch(uint64_t batchBegin) {
    while (!inFlight_.empty() && inFlight_.front().first <= batchBegin) {
      inFlight_.front().second.wait();
      inFlight_.pop_front();
    }
    const uint64_t lastToLaunch =
        batchBegin + distance_ * ConstructTripleGenerator::BATCH_SIZE;
    for (; nextToLaunch_ <= lastToLaunch && nextToLaunch_ < tableEnd_;
         nextToLaunch_ += ConstructTripleGenerator::BATCH_SIZE) {
      launch(nextToLaunch_);
    }
  }

 private:
  // Start the prefetch of the batch starting at row `batchBegin`. The indices
  // are collected here, so the task only uses the index, which outlives the
  // export, and not the result table.
  void launch(uint64_t batchBegin) {
    const uint64_t batchEnd =
        std::min(batchBegin + ConstructTripleGenerator::BATCH_SIZE, tableEnd_);
    std::vector<size_t> indices;
    for (ColumnIndex column : columns_) {
      for (Id id : table_.idTable().getColumn(column).subspan(
               batchBegin, batchEnd - batchBegin)) {
        if (id.getDatatype() == Datatype::VocabIndex) {
          indices.push_back(id.getVocabIndex().get());
        }
      }
    }
    ql::ranges::sort(indices);
    indices.erase(std::unique(indices.begin(), indices.end()), indices.end());
    if (indices.empty()) {
      return;
    }
    inFlight_.emplace_back(
        batchBegin,
        boost::asio::post(
            ad_utility::globalExecutor(),
            boost::asio::use_future(
                [&index = index_.get(), indices = std::move(indices)]() {
                  // A failed prefetch only costs its speedup: the batch
                  // reads (and reports a failure of) its words itself.
                  try {
                    index.getImpl().getVocab().lookupBatch(indices);
                  } catch (const std::exception& e) {
                    AD_LOG_DEBUG << "Vocabulary prefetch failed: " << e.what()
                                 << std::endl;
                  }
                })));
  }

  std::reference_wrapper<const Index> index_;
  TableConstRefWithVocab table_;
  std::vector<ColumnIndex> columns_;
  uint64_t tableEnd_;
  size_t distance_;
  // The first row of the next batch to prefetch.
  uint64_t nextToLaunch_;
  // The running prefetches, by the first row of their batch (ascending).
  std::deque<std::pair<uint64_t, std::future<void>>> inFlight_;
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
  // Shared by the copies of the transform below (null = no prefetching).
  std::shared_ptr<VocabularyPrefetcher> prefetcher;
  if (const size_t distance = getRuntimeParameter<
          &RuntimeParameters::exportVocabularyPrefetchBatches_>();
      distance > 0 && !ql::ranges::empty(rowView)) {
    prefetcher = std::make_shared<VocabularyPrefetcher>(
        context.index_.get(), tableWithVocab,
        context.preprocessedTemplate_.get().uniqueVariableColumns_,
        *ql::ranges::begin(rowView),
        *ql::ranges::begin(rowView) + ql::ranges::size(rowView), distance);
  }
  return ranges::views::chunk(std::move(rowView),
                              ConstructTripleGenerator::BATCH_SIZE) |
         ql::views::transform([tableWithVocab, context = std::move(context),
                               tableRowOffset, prefetcher](auto chunkView) {
           if (prefetcher != nullptr) {
             prefetcher->beforeBatch(*ql::ranges::begin(chunkView));
           }
           return computeBatch(tableWithVocab, chunkView, context,
                               tableRowOffset);
         }) |
         ql::views::join;
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
