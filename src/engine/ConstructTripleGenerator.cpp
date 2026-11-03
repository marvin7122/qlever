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
#include "util/views/AsyncTransformView.h"

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

// The range of `ConstructTripleGenerator::makeRowChunks`.
class RowChunker : public ad_utility::InputRangeFromGet<RowChunk> {
  InputRangeTypeErased<TableWithRange> tables_;
  // The rows of the current table that are not yet in a chunk, or
  // `std::nullopt` before the first table has been requested.
  std::optional<TableWithRange> currentTable_;
  // The blank node row id of row 0 of the `currentTable_`, and that of the next
  // table (see `rowOffsetAccumulatesAcrossTables` in the test).
  size_t tableRowOffset_ = 0;
  size_t nextTableRowOffset_;
  size_t maxNumberOfRowsPerChunk_;
  ad_utility::AllocatorWithLimit<Id> allocator_;

 public:
  RowChunker(InputRangeTypeErased<TableWithRange> tables, size_t rowOffset,
             size_t maxNumberOfRowsPerChunk,
             ad_utility::AllocatorWithLimit<Id> allocator)
      : tables_{std::move(tables)},
        nextTableRowOffset_{rowOffset},
        maxNumberOfRowsPerChunk_{maxNumberOfRowsPerChunk},
        allocator_{std::move(allocator)} {
    AD_CONTRACT_CHECK(maxNumberOfRowsPerChunk > 0);
  }

  std::optional<RowChunk> get() override {
    RowChunk chunk;
    size_t numberOfRows = 0;
    while (numberOfRows < maxNumberOfRowsPerChunk_) {
      // Only request the next table (which may invalidate the current one)
      // when all rows of the current table are in a chunk.
      if (!currentTable_.has_value() || currentTable_->view_.empty()) {
        currentTable_ = tables_.get();
        if (!currentTable_.has_value()) {
          break;
        }
        tableRowOffset_ = nextTableRowOffset_;
        nextTableRowOffset_ += currentTable_->view_.size();
        continue;
      }
      auto& [tableWithVocab, rows] = currentTable_.value();
      const uint64_t begin = *rows.begin();
      const uint64_t tableEnd = begin + rows.size();
      const uint64_t end =
          std::min(tableEnd, begin + (maxNumberOfRowsPerChunk_ - numberOfRows));
      IdTable idTable{tableWithVocab.idTable().numColumns(), allocator_};
      idTable.insertAtEnd(tableWithVocab.idTable(), begin, end);
      chunk.parts_.push_back(CopiedRows{std::move(idTable),
                                        tableWithVocab.localVocab().clone(),
                                        tableRowOffset_ + begin});
      numberOfRows += end - begin;
      rows = ql::views::iota(end, tableEnd);
    }
    if (chunk.parts_.empty()) {
      return std::nullopt;
    }
    return chunk;
  }
};
}  // namespace

//______________________________________________________________________________
InputRangeTypeErased<RowChunk> ConstructTripleGenerator::makeRowChunks(
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    size_t maxNumberOfRowsPerChunk,
    ad_utility::AllocatorWithLimit<Id> allocator) {
  return InputRangeTypeErased<RowChunk>{
      RowChunker{std::move(rowIndices), rowOffset, maxNumberOfRowsPerChunk,
                 std::move(allocator)}};
}

//______________________________________________________________________________
FormattedChunk ConstructTripleGenerator::formatRowChunk(
    const PreprocessedConstructTemplate& preprocessedTemplate,
    const RowChunk& chunk, ad_utility::MediaType mediaType, const Index& index,
    const CancellationHandle& cancellationHandle,
    const ad_utility::AllocatorWithLimit<char>& allocator,
    ad_utility::AsyncTransformStopFlag stop) {
  // Each chunk has a cache of its own, because the cache is not thread-safe.
  IdCache cache = makeIdCache(preprocessedTemplate);
  const BatchEvalContext context{preprocessedTemplate, index, cache,
                                 cancellationHandle, nullptr};
  FormattedChunk formatted{allocator};
  for (const CopiedRows& part : chunk.parts_) {
    const TableConstRefWithVocab table{part.idTable_.asStaticView<0>(),
                                       part.localVocab_};
    // The rows are numbered from zero in the copy, so the blank node row id of
    // row `i` is `part.blankNodeRowIdOfFirstRow_ + i`, as in `computeBatch`.
    for (auto batch : ranges::views::chunk(
             ql::views::iota(size_t{0}, part.idTable_.numRows()), BATCH_SIZE)) {
      if (stop.isRequested()) {
        return formatted;
      }
      for (const EvaluatedTriple& triple : computeBatch(
               table, batch, context, part.blankNodeRowIdOfFirstRow_)) {
        formatted.append(formatTriple(triple, mediaType));
      }
    }
  }
  return formatted;
}

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
InputRangeTypeErased<FormattedChunk>
ConstructTripleGenerator::generateFormattedChunksInParallel(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    ad_utility::MediaType mediaType, const EvaluationConfig& config,
    size_t maxNumberOfChunksInFlight) {
  AD_CONTRACT_CHECK(
      std::holds_alternative<DeduplicationMode::None>(config.mode_.value_));
  const QueryExecutionContext& qec = config.qec_;
  auto preprocessedTemplate =
      qec.makeShared<const PreprocessedConstructTemplate>(
          ConstructTemplatePreprocessor::preprocess(
              templateTriples, variableColumns, config.index_));
  auto chunks = makeRowChunks(std::move(rowIndices), rowOffset,
                              MAX_NUMBER_OF_ROWS_PER_CHUNK, qec.getAllocator());
  // The tasks only read what the lambda captures by value.
  auto formatChunk =
      [preprocessedTemplate = std::move(preprocessedTemplate), mediaType,
       index = config.index_, cancellationHandle = config.cancellationHandle_,
       allocator = ad_utility::AllocatorWithLimit<char>{qec.getAllocator()}](
          RowChunk chunk, ad_utility::AsyncTransformStopFlag stop) {
        return formatRowChunk(*preprocessedTemplate, chunk, mediaType, index,
                              cancellationHandle, allocator, stop);
      };
  using View =
      ad_utility::AsyncTransformView<decltype(chunks), decltype(formatChunk)>;
  return InputRangeTypeErased<FormattedChunk>{
      std::make_unique<View>(std::move(chunks), std::move(formatChunk),
                             maxNumberOfChunksInFlight, qec.executor())};
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
