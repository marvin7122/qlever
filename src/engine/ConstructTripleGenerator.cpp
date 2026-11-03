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
#include "engine/ConstructRowChunks.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ConstructTripleInstantiator.h"
#include "util/OverloadCallOperator.h"
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

// Evaluate the rows covered by `batch.view_`. The blank nodes of the first row
// get the row id `blankNodeRowIdOfFirstRow`, those of the following rows
// consecutive ids. Cancellation is checked once at the start. When
// `context.deduplicator_` is set, duplicate triples are dropped as they are
// instantiated (see `instantiateBatch`'s `DeduplicationParams`).
CPP_template(typename ChunkView)(requires ranges::range<ChunkView>)
std::vector<EvaluatedTriple> computeBatch(
    const TableConstRefWithVocab& tableWithVocab, ChunkView batch,
    const BatchEvalContext& context, size_t blankNodeRowIdOfFirstRow) {
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

  std::optional<DeduplicationParams> deduplication{std::nullopt};
  if (context.deduplicator_) {
    deduplication.emplace(DeduplicationParams{*context.deduplicator_, ctx});
  }
  return instantiateBatch(context.preprocessedTemplate_.get(), batchResult,
                          blankNodeRowIdOfFirstRow, deduplication);
}

// Chunks `table` into batches and evaluates each one. Takes `TableWithRange` by
// value and stores only value-captures in the returned view so the pipeline is
// self-contained w.r.t. the `table` handle (no reference to a caller's
// `TableWithRange` / parameter can dangle). `TableWithRange` itself is a cheap
// non-owning handle (`IdTableView` + `LocalVocab` ref); the underlying result
// storage must still outlive the whole export, as with every other CONSTRUCT
// export path. The blank nodes of the first row of `table.view_` get the row id
// `blankNodeRowIdOfFirstRow`.
auto processTableBatches(TableWithRange table, BatchEvalContext context,
                         size_t blankNodeRowIdOfFirstRow) {
  // Copy the cheap pieces out first so neither `chunk` nor the transform
  // lambda retain a reference into the by-value `table` parameter.
  auto rowView = table.view_;
  const TableConstRefWithVocab tableWithVocab = table.tableWithVocab_;
  // The view does not have to start at row 0 of the table (for example, if
  // the OFFSET ends inside the table), so the row id of a batch is counted
  // from the first row of the view.
  const size_t firstRow = *ql::ranges::begin(rowView);
  return ranges::views::chunk(std::move(rowView),
                              ConstructTripleGenerator::BATCH_SIZE) |
         ql::views::transform([tableWithVocab, context = std::move(context),
                               blankNodeRowIdOfFirstRow,
                               firstRow](auto chunkView) {
           const size_t batchBegin = *ql::ranges::begin(chunkView);
           return computeBatch(
               tableWithVocab, chunkView, context,
               blankNodeRowIdOfFirstRow + (batchBegin - firstRow));
         }) |
         ql::views::join;
}

using namespace detail;

// The range of `detail::makeRowChunks`.
class RowChunker : public ad_utility::InputRangeFromGet<RowChunkOrInputError> {
  // The table of which some rows are not yet in a chunk.
  struct CurrentTable {
    // The rows that are not yet in a chunk.
    TableWithRange rows_;
    // The blank node row id of the first of these rows.
    size_t blankNodeRowIdOfFirstRow_;
    // The clone of the `LocalVocab` for the copies of the rows (only with
    // `CopyColumns`), created on the first copy.
    std::shared_ptr<const LocalVocab> localVocab_;
  };
  // Reading the tables, the exception that reading them threw (the last
  // element that is still to be returned), or done.
  struct Reading {
    std::optional<CurrentTable> currentTable_;
  };
  struct Failed {
    InputError error_;
  };
  struct Done {};

  InputRangeTypeErased<TableWithRange> tables_;
  RowChunkingConfig config_;
  std::variant<Reading, Failed, Done> state_ = Reading{};
  size_t blankNodeRowIdOfNextTable_;

 public:
  RowChunker(InputRangeTypeErased<TableWithRange> tables,
             RowChunkingConfig config)
      : tables_{std::move(tables)},
        config_{std::move(config)},
        blankNodeRowIdOfNextTable_{config_.blankNodeRowIdOfFirstRow_} {
    AD_CONTRACT_CHECK(config_.maxNumberOfRowsPerChunk_ > 0);
  }

  std::optional<RowChunkOrInputError> get() override {
    RowChunk chunk;
    if (auto* reading = std::get_if<Reading>(&state_)) {
      try {
        fill(chunk, *reading);
      } catch (...) {
        // `tables_` is not read again after it threw.
        state_ = Failed{InputError{std::current_exception()}};
      }
    }
    if (!chunk.parts_.empty()) {
      return chunk;
    }
    if (auto* failed = std::get_if<Failed>(&state_)) {
      InputError error = std::move(failed->error_);
      state_ = Done{};
      return error;
    }
    return std::nullopt;
  }

 private:
  // Add rows to the `chunk` until it is full or the tables are exhausted (then
  // the `state_` becomes `Done`).
  void fill(RowChunk& chunk, Reading& reading) {
    size_t numberOfRows = 0;
    while (numberOfRows < config_.maxNumberOfRowsPerChunk_) {
      auto& current = reading.currentTable_;
      // Only request the next table (which may invalidate the current one)
      // when all rows of the current table are in a chunk.
      if (!current.has_value() || current->rows_.view_.empty()) {
        current.reset();
        auto next = tables_.get();
        if (!next.has_value()) {
          state_ = Done{};
          return;
        }
        const size_t numberOfRowsOfTable = next->view_.size();
        current.emplace(CurrentTable{std::move(next.value()),
                                     blankNodeRowIdOfNextTable_, nullptr});
        blankNodeRowIdOfNextTable_ += numberOfRowsOfTable;
        continue;
      }
      numberOfRows += takeRows(chunk, current.value(),
                               config_.maxNumberOfRowsPerChunk_ - numberOfRows);
    }
  }

  // Move at most `maxNumberOfRows` (but at least one) rows of the `current`
  // table into a new part of the `chunk`, and return their number.
  size_t takeRows(RowChunk& chunk, CurrentTable& current,
                  size_t maxNumberOfRows) {
    auto& [tableWithVocab, rows] = current.rows_;
    const uint64_t begin = *rows.begin();
    const uint64_t tableEnd = begin + rows.size();
    const uint64_t end =
        std::min(tableEnd, begin + static_cast<uint64_t>(maxNumberOfRows));
    auto part = std::visit(
        ad_utility::OverloadCallOperator{
            [&](const BorrowRows&) -> std::variant<TableWithRange, CopiedRows> {
              return TableWithRange{tableWithVocab,
                                    ql::views::iota(begin, end)};
            },
            [&](const CopyColumns& copy)
                -> std::variant<TableWithRange, CopiedRows> {
              if (current.localVocab_ == nullptr) {
                current.localVocab_ = std::make_shared<const LocalVocab>(
                    tableWithVocab.localVocab().clone());
              }
              IdTable idTable{copy.columns_.size(), config_.allocator_};
              idTable.insertAtEnd(tableWithVocab.idTable(), begin, end,
                                  copy.columns_);
              return CopiedRows{std::move(idTable), current.localVocab_};
            }},
        config_.storage_);
    chunk.parts_.push_back(
        ChunkPart{std::move(part), current.blankNodeRowIdOfFirstRow_});
    current.blankNodeRowIdOfFirstRow_ += end - begin;
    rows = ql::views::iota(end, tableEnd);
    return end - begin;
  }
};
}  // namespace

namespace detail {
//______________________________________________________________________________
InputRangeTypeErased<RowChunkOrInputError> makeRowChunks(
    InputRangeTypeErased<TableWithRange> tables, RowChunkingConfig config) {
  return InputRangeTypeErased<RowChunkOrInputError>{
      RowChunker{std::move(tables), std::move(config)}};
}

//______________________________________________________________________________
FormattedChunk ChunkFormatter::format(
    const RowChunk& chunk, ad_utility::AsyncTransformStopFlag stop) const {
  const PreprocessedConstructTemplate& preprocessedTemplate =
      *preprocessedTemplate_;
  // Each chunk has a cache of its own, because the cache is not thread-safe.
  IdCache cache = ConstructTripleGenerator::makeIdCache(preprocessedTemplate);
  const BatchEvalContext context{preprocessedTemplate, index_, cache,
                                 cancellationHandle_, nullptr};
  size_t numberOfRowsOfChunk = 0;
  for (const ChunkPart& part : chunk.parts_) {
    numberOfRowsOfChunk += std::visit(
        ad_utility::OverloadCallOperator{
            [](const TableWithRange& rows) -> size_t {
              return rows.view_.size();
            },
            [](const CopiedRows& rows) { return rows.idTable_.numRows(); }},
        part.rows_);
  }
  FormattedChunk formatted{allocator_};
  size_t numberOfFormattedRows = 0;
  for (const ChunkPart& part : chunk.parts_) {
    // The table and the rows of the part.
    auto [table, rows] = std::visit(
        ad_utility::OverloadCallOperator{
            [](const TableWithRange& borrowed) {
              return std::pair{borrowed.tableWithVocab_, borrowed.view_};
            },
            [](const CopiedRows& copied) {
              return std::pair{
                  TableConstRefWithVocab{copied.idTable_.asStaticView<0>(),
                                         *copied.localVocab_},
                  ql::views::iota(uint64_t{0}, static_cast<uint64_t>(
                                                   copied.idTable_.numRows()))};
            }},
        part.rows_);
    const uint64_t firstRow = *rows.begin();
    for (auto batch :
         ranges::views::chunk(rows, ConstructTripleGenerator::BATCH_SIZE)) {
      if (stop.isRequested()) {
        return formatted;
      }
      const uint64_t batchBegin = *ql::ranges::begin(batch);
      for (const EvaluatedTriple& triple : computeBatch(
               table, batch, context,
               part.blankNodeRowIdOfFirstRow_ + (batchBegin - firstRow))) {
        formatted.append(formatTriple(triple, mediaType_));
      }
      // After the first batch, reserve the memory for the whole chunk,
      // estimated from the size of that batch.
      if (numberOfFormattedRows == 0) {
        const size_t numberOfRowsOfBatch = ql::ranges::size(batch);
        formatted.reserve(formatted.size() * numberOfRowsOfChunk /
                          numberOfRowsOfBatch);
      }
      numberOfFormattedRows += ql::ranges::size(batch);
    }
  }
  return formatted;
}

//______________________________________________________________________________
FormattedChunk ChunkFormatter::operator()(
    RowChunkOrInputError chunkOrError,
    ad_utility::AsyncTransformStopFlag stop) const {
  return std::visit(
      ad_utility::OverloadCallOperator{
          [this, stop](const RowChunk& chunk) { return format(chunk, stop); },
          [](const InputError& error) -> FormattedChunk {
            std::rethrow_exception(error.exception_);
          }},
      chunkOrError);
}
}  // namespace detail

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

        const size_t blankNodeRowIdOfFirstRow = accumulatedRowOffset;
        accumulatedRowOffset += numRowsOfTable;

        const BatchEvalContext context{*preprocessedTemplate, index, cache,
                                       cancellationHandle, deduplicator};
        return processTableBatches(table, context, blankNodeRowIdOfFirstRow);
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
    InputRangeTypeErased<TableWithRange> rowIndices,
    TableLifetime tableLifetime, size_t rowOffset,
    ad_utility::MediaType mediaType, const EvaluationConfig& config,
    size_t maxNumberOfChunksInFlight) {
  using namespace detail;
  AD_CONTRACT_CHECK(
      std::holds_alternative<DeduplicationMode::None>(config.mode_.value_));
  const QueryExecutionContext& qec = config.qec_;
  auto preprocessedTemplate = ConstructTemplatePreprocessor::preprocess(
      templateTriples, variableColumns, config.index_);
  RowStorage storage = BorrowRows{};
  if (tableLifetime == TableLifetime::UntilNextTable) {
    // Copy only the columns of the template, and preprocess the template
    // again for the columns of the copies.
    std::vector<ColumnIndex> columns =
        preprocessedTemplate.uniqueVariableColumns_;
    VariableToColumnMap columnsOfCopies;
    for (const auto& [variable, columnInfo] : variableColumns) {
      auto it = ql::ranges::find(columns, columnInfo.columnIndex_);
      if (it != columns.end()) {
        auto columnInfoOfCopy = columnInfo;
        columnInfoOfCopy.columnIndex_ =
            static_cast<ColumnIndex>(it - columns.begin());
        columnsOfCopies.emplace(variable, columnInfoOfCopy);
      }
    }
    preprocessedTemplate = ConstructTemplatePreprocessor::preprocess(
        templateTriples, columnsOfCopies, config.index_);
    storage = CopyColumns{std::move(columns)};
  }
  auto chunks =
      makeRowChunks(std::move(rowIndices),
                    RowChunkingConfig{rowOffset, MAX_NUMBER_OF_ROWS_PER_CHUNK,
                                      std::move(storage), qec.getAllocator()});
  ChunkFormatter formatter{
      qec.makeShared<const PreprocessedConstructTemplate>(
          std::move(preprocessedTemplate)),
      mediaType, config.index_, config.cancellationHandle_,
      ad_utility::AllocatorWithLimit<char>{qec.getAllocator()}};
  using View = ad_utility::AsyncTransformView<decltype(chunks), ChunkFormatter>;
  return InputRangeTypeErased<FormattedChunk>{
      std::make_unique<View>(std::move(chunks), std::move(formatter),
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
