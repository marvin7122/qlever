// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "engine/export_v2/ExportEngineV2.h"

#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "engine/Bind.h"
#include "engine/CartesianProductJoin.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/Filter.h"
#include "engine/HasPredicateScan.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Sort.h"
#include "engine/Values.h"
#include "engine/export_v2/ColumnLattice.h"
#include "engine/export_v2/ConstructRowSerializer.h"
#include "engine/export_v2/ExportMorselPlanner.h"
#include "engine/export_v2/ResolvedColumn.h"
#include "global/Id.h"
#include "global/RuntimeParameters.h"
#include "index/ExportIds.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"
#include "util/Timer.h"

namespace ql::engine::export_v2 {
namespace {

// True when `text` needs CSV/TSV escaping. SimdEscapeClassifier is the fast
// reject filter; the rare positive goes through the legacy RdfEscaping path
// (required for CSV quoting).
template <RowFormat Format>
bool needsEscaping(std::string_view text) {
  constexpr auto escapeFormat =
      Format == RowFormat::Csv ? EscapeFormat::Csv : EscapeFormat::Tsv;
  return !text.empty() &&
         SimdEscapeClassifier::findFirstEscapeSimd<escapeFormat>(text) !=
             std::string_view::npos;
}

template <RowFormat Format>
std::string escapeForFormat(std::string_view text) {
  if constexpr (Format == RowFormat::Csv) {
    return RdfEscaping::escapeForCsv(std::string{text});
  } else {
    static_assert(Format == RowFormat::Tsv);
    return RdfEscaping::escapeForTsv(std::string{text});
  }
}

// Escape `input` for CSV/TSV (the `escapeFunction` of `idToStringAndType`).
template <RowFormat Format>
std::string escapeCell(std::string input) {
  if (!needsEscaping<Format>(input)) {
    return input;
  }
  return escapeForFormat<Format>(input);
}

// Write the cell for `word` into `column`, with the same text as
// `literalOrIriToStringAndType<Format == Csv>(word, escapeCell<Format>)`:
// blank nodes unescaped, CSV the bare content, TSV the full representation,
// escaped if needed. Without escaping the cell is a view into `word`'s
// storage, so `word` must outlive `column` unless `copy` is set.
template <RowFormat Format>
void setWordCell(ResolvedColumn& column, size_t row,
                 const ad_utility::triple_component::LiteralOrIriView& word,
                 bool copy) {
  auto set = [&](std::string_view text) {
    copy ? column.setCopy(row, text) : column.setView(row, text);
  };
  if (word.isIri()) {
    if (auto blankNode = ql::exportIds::blankNodeIriToString(word.getIri())) {
      set(blankNode.value());
      return;
    }
  }
  std::string_view text;
  if constexpr (Format == RowFormat::Csv) {
    text = asStringViewUnsafe(word.getContent());
  } else {
    text = word.toStringRepresentation();
  }
  if (needsEscaping<Format>(text)) {
    column.setCopy(row, escapeForFormat<Format>(text));
  } else {
    set(text);
  }
}

// Resolve a column that is not uniformly encoded. `VocabIndex` ids go through
// one batched vocabulary lookup and become views into the decoded batch.
// Encoded IRIs are decoded once and copied into the scratch buffer. All other
// ids take the generic `idToStringAndType` path.
template <RowFormat Format>
void resolveMixedColumn(ResolvedColumn& column, const Index& index,
                        ql::span<const Id> ids, const LocalVocab& localVocab) {
  constexpr bool removeQuotes = Format == RowFormat::Csv;
  using LiteralOrIriView = ad_utility::triple_component::LiteralOrIriView;
  std::vector<size_t> vocabRows;
  std::vector<size_t> vocabIndices;
  for (size_t row = 0; row < ids.size(); ++row) {
    const Id id = ids[row];
    switch (id.getDatatype()) {
      case Datatype::VocabIndex:
        vocabRows.push_back(row);
        vocabIndices.push_back(id.getVocabIndex().get());
        break;
      case Datatype::EncodedVal: {
        const std::string iri =
            index.getImpl().encodedIriManager().toString(id);
        setWordCell<Format>(
            column, row, LiteralOrIriView::fromStringRepresentation(iri), true);
        break;
      }
      default: {
        auto cell = ql::exportIds::idToStringAndType<removeQuotes>(
            index, id, localVocab, escapeCell<Format>);
        if (cell.has_value()) {
          column.setCopy(row, cell.value().first);
        }
      }
    }
  }
  if (!vocabRows.empty()) {
    ad_utility::vocabulary::ArenaVocabBatchBuilder builder(
        vocabIndices.size(), index.getImpl().allocator());
    auto words = index.getImpl().getVocab().lookupBatch(vocabIndices, builder);
    AD_CORRECTNESS_CHECK(words.size() == vocabRows.size());
    auto word = words.begin();
    for (size_t row : vocabRows) {
      setWordCell<Format>(column, row,
                          LiteralOrIriView::fromStringRepresentation(*word++),
                          false);
    }
    column.keepAlive(std::move(words));
  }
}

std::string makeHeaderLine(const parsedQuery::SelectClause& selectClause,
                           RowFormat format) {
  std::vector<std::string> variables =
      selectClause.getSelectedVariablesAsStrings();
  if (format == RowFormat::Csv) {
    ql::ranges::for_each(variables,
                         [](std::string& var) { var = var.substr(1); });
  }
  const char separator = format == RowFormat::Csv ? ',' : '\t';
  return absl::StrCat(absl::StrJoin(variables, std::string_view{&separator, 1}),
                      "\n");
}

// The selected output columns with their plan-time datatype lattice (WP-A).
// `indices` parallels the SELECT list (`std::nullopt` = unbound column);
// `lattice` proves per-column types so the serializer can skip its per-chunk
// uniformity scan for proven-trivial columns (WP-C).
struct SelectedColumns {
  std::vector<std::optional<ColumnIndex>> indices_;
  ColumnLatticeResult lattice_;
};

SelectedColumns selectedColumns(const ParsedQuery& parsedQuery,
                                const QueryExecutionTree& qet,
                                const parsedQuery::SelectClause& selectClause) {
  auto selected = qet.selectedVariablesToColumnIndices(selectClause, true);
  SelectedColumns columns;
  columns.indices_.reserve(selected.size());
  for (const auto& entry : selected) {
    if (entry.has_value()) {
      columns.indices_.emplace_back(entry.value().columnIndex_);
    } else {
      columns.indices_.emplace_back(std::nullopt);
    }
  }
  columns.lattice_ = compileColumnLattice(parsedQuery, selected);
  return columns;
}

// Checkpoint interval for cooperative revocation: an in-flight morsel
// abandons its remainder at most this many rows after a new query arrives,
// so a foreground query waits for pool threads no longer than one
// checkpoint of serialization. Ordered sessions ignore checkpoints and run
// each morsel to completion for deterministic prefixes.
constexpr uint64_t kRevocationCheckRows = 1024;

// A morsel must have serialized this many rows before its size is
// extrapolated: a few rows give a noisy bytes-per-row estimate.
constexpr uint64_t kMinRowsForMorselEstimate = 256;

// Once the first `rowsDone` of a morsel's `rowsTotal` rows are serialized,
// grow the copy buffer to the extrapolated morsel size (plus 1/8 slack), so
// the remaining windows append without reallocating and re-copying the bytes
// already written. A low estimate only costs the usual geometric growth.
// Returns true when no further call is needed for this morsel.
bool reserveForMorsel(ScatterGatherChunkBuilder& builder, uint64_t rowsDone,
                      uint64_t rowsTotal) {
  if (rowsDone >= rowsTotal) {
    return true;
  }
  if (rowsDone < kMinRowsForMorselEstimate || builder.empty()) {
    return false;
  }
  const auto estimate = static_cast<size_t>(
      static_cast<double>(builder.size()) * static_cast<double>(rowsTotal) /
      static_cast<double>(rowsDone));
  builder.reserveCopied(estimate + estimate / 8);
  return true;
}

// The OFFSET that is still to be applied to the result of `qet`. Like Legacy
// `compensateForLimitOffsetClause`: when the root operation already applied
// the OFFSET (e.g. an `IndexScan`), its result starts at the first row to
// export. Re-applying the LIMIT is harmless, so it is never compensated.
uint64_t offsetLeftToApply(const QueryExecutionTree& qet,
                           const LimitOffsetClause& limitOffset) {
  return qet.handlesLimitOffset() == LimitOffsetHandling::NONE
             ? limitOffset._offset
             : 0;
}

// The blocks of `result` as owned pairs, which morsel segments keep alive.
// A lazy result is streamed. A fully materialized result (for example one
// from the query cache, or an operation that always materializes) is one
// block: its table is cloned, because the segments own their blocks and the
// result keeps its own copy for the cache. `Result::idTables` would reject it.
Result::LazyResult resultBlocks(std::shared_ptr<const Result> result) {
  if (!result->isFullyMaterialized()) {
    return result->idTables();
  }
  return Result::LazyResult{
      ad_utility::lazySingleValueRange([result = std::move(result)]() {
        return Result::IdTableVocabPair{result->idTableView().clone(),
                                        result->localVocab().clone()};
      })};
}

// Serializes the rows `[begin, end)` of `segment`'s block into `builder`.
// There is one per request (SELECT CSV/TSV or CONSTRUCT), shared by all morsel
// tasks of that request, so calls must be safe from concurrent threads. It
// owns everything it reads besides the block (shared ownership): closing the
// session does not join running helpers, so a helper may still serialize after
// the request's context is gone.
using SegmentSerializer =
    std::function<void(const ExportMorselSegment& segment, uint64_t begin,
                       uint64_t end, ScatterGatherChunkBuilder& builder)>;

// Builds morsel tasks with cooperative revocation checkpoints (unordered
// sessions only). On revocation the task returns its partial builder and
// resubmits the unprocessed tail as an ordinary morsel, so no row is lost
// and no row is emitted twice; on cancellation the tail is dropped with the
// job. Partial builders are just smaller builders: unordered emission
// accepts them, ordered sessions never produce them.
struct CheckpointMorselRunner {
  std::shared_ptr<
      ad_utility::export_v2::ExportJobState<ScatterGatherChunkBuilder>>
      state_;
  std::shared_ptr<const SegmentSerializer> serialize_;
  bool checkpoints_ = false;

  absl::AnyInvocable<ScatterGatherChunkBuilder()> makeTask(
      ExportMorsel plan) const {
    const uint64_t epoch = state_->currentEpoch();
    return [*this, plan = std::move(plan), epoch]() mutable {
      return run(std::move(plan), epoch);
    };
  }

  ScatterGatherChunkBuilder run(ExportMorsel plan, uint64_t epoch) const {
    ScatterGatherChunkBuilder builder;
    const size_t numSegments = plan.segments_.size();
    uint64_t rowsDone = 0;
    bool reserved = false;
    for (size_t s = 0; s < numSegments; ++s) {
      auto& seg = plan.segments_[s];
      uint64_t pos = seg.begin_;
      while (pos < seg.end_) {
        const uint64_t windowEnd =
            std::min(seg.end_, pos + kRevocationCheckRows);
        (*serialize_)(seg, pos, windowEnd, builder);
        rowsDone += windowEnd - pos;
        if (!reserved) {
          reserved = reserveForMorsel(builder, rowsDone, plan.numRows_);
        }
        pos = windowEnd;
        if (pos < seg.end_ || s + 1 < numSegments) {
          if (state_->isCancelled()) {
            return builder;
          }
          if (checkpoints_ && state_->currentEpoch() != epoch) {
            ExportMorsel remainder;
            if (pos < seg.end_) {
              auto tail = seg;
              tail.begin_ = pos;
              remainder.segments_.push_back(std::move(tail));
              remainder.numRows_ += seg.end_ - pos;
            }
            for (size_t r = s + 1; r < numSegments; ++r) {
              remainder.numRows_ +=
                  plan.segments_[r].end_ - plan.segments_[r].begin_;
              remainder.segments_.push_back(std::move(plan.segments_[r]));
            }
            state_->trySubmitMorsel(makeTask(std::move(remainder)));
            return builder;
          }
        }
      }
    }
    return builder;
  }
};

// One builder per header / 8192-row morsel. Callers choose finalizeToString
// (default HTTP) or finalize (scatter-gather HTTP). When `scheduler` is set
// (live V2 default), CPU serialize runs on `queryThreadPool_` and the
// coordinator consumes here; helpers never write the socket. Without
// LIMIT/OFFSET/export limits the row order is semantically irrelevant, so
// morsels emit in completion order; bounded queries keep deterministic slot
// order.
//
// The morsel plans stream straight from the lazy result blocks: every segment
// serializes from its block in place, so there is no `sliced` copy and no
// second copy through a rechunker.
cppcoro::generator<ScatterGatherChunkBuilder> serializeMorsels(
    const QueryExecutionTree& qet, LimitOffsetClause limitOffset,
    std::optional<std::string> header,
    std::shared_ptr<const SegmentSerializer> serialize,
    ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  if (header.has_value()) {
    ScatterGatherChunkBuilder builder;
    builder.appendOwned(std::move(header).value());
    co_yield std::move(builder);
  }

  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();

  // The order of the morsels follows the clause the user wrote, the rows to
  // skip follow the plan (see `offsetLeftToApply`).
  const bool ordered = limitOffset._limit.has_value() ||
                       limitOffset._offset != 0 ||
                       limitOffset.textLimit_.has_value() ||
                       limitOffset.exportLimit_.has_value();
  limitOffset._offset = offsetLeftToApply(qet, limitOffset);

  constexpr uint64_t rowsPerMorsel = 8192;
  if (scheduler == nullptr) {
    // No session exists here, so nothing can revoke: serialize each plan
    // directly without checkpoints.
    for (auto&& plan :
         planExportMorsels(resultBlocks(result), limitOffset, rowsPerMorsel)) {
      cancellationHandle->throwIfCancelled();
      ScatterGatherChunkBuilder builder;
      uint64_t rowsDone = 0;
      bool reserved = false;
      for (const auto& segment : plan.segments_) {
        cancellationHandle->throwIfCancelled();
        (*serialize)(segment, segment.begin_, segment.end_, builder);
        rowsDone += segment.end_ - segment.begin_;
        if (!reserved) {
          reserved = reserveForMorsel(builder, rowsDone, plan.numRows_);
        }
      }
      if (!builder.empty()) {
        co_yield std::move(builder);
      }
    }
    co_return;
  }

  AD_LOG_INFO << "ExportEngineV2 streaming lazy result blocks to morsels on "
                 "queryThreadPool_ (no extra V2 threads)"
              << std::endl;
  auto session = scheduler->createSession<ScatterGatherChunkBuilder>();
  session.setOrdered(ordered);
  // The plans own their blocks, so workers can serialize after the driver
  // moved on. No table or vocabulary clone: one shared owner per block.
  // Unordered tasks checkpoint revocation mid-morsel (see above); ordered
  // tasks run each morsel to completion.
  const CheckpointMorselRunner runner{session.sharedState(),
                                      std::move(serialize), !ordered};
  for (auto&& plan :
       planExportMorsels(resultBlocks(result), limitOffset, rowsPerMorsel)) {
    cancellationHandle->throwIfCancelled();
    session.submitMorsel(runner.makeTask(std::move(plan)));
  }
  while (session.hasMoreResults()) {
    auto builder = session.consumeNextResult();
    if (!builder.empty()) {
      co_yield std::move(builder);
    }
  }
}

// The SELECT CSV/TSV morsels: a header line, then the selected columns of
// every row.
cppcoro::generator<ScatterGatherChunkBuilder> buildSelectMorsels(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    RowFormat format, ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  const auto& selectClause = parsedQuery.selectClause();
  auto columns = selectedColumns(parsedQuery, qet, selectClause);
  auto serialize = std::make_shared<const SegmentSerializer>(
      [indices = std::move(columns.indices_),
       lattice = std::move(columns.lattice_.columns_),
       index = qet.getQec()->getIndexSharedPtr(),
       format](const ExportMorselSegment& segment, uint64_t begin, uint64_t end,
               ScatterGatherChunkBuilder& builder) {
        ExportEngineV2::appendSerializedRows(
            segment.block_->idTable_.asStaticView<0>(),
            segment.block_->localVocab_, format, builder, *index, indices,
            begin, end, lattice);
      });
  return serializeMorsels(
      qet, parsedQuery._limitOffset, makeHeaderLine(selectClause, format),
      std::move(serialize), std::move(cancellationHandle), scheduler);
}

// The CONSTRUCT Turtle/N-Triples morsels: the instantiated template of every
// row, no header. The serializer numbers blank nodes from the OFFSET that is
// still applied, exactly like Legacy.
cppcoro::generator<ScatterGatherChunkBuilder> buildConstructMorsels(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    ad_utility::MediaType mediaType,
    ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  const auto& limitOffset = parsedQuery._limitOffset;
  auto index = qet.getQec()->getIndexSharedPtr();
  auto serializer = std::make_shared<const ConstructRowSerializer>(
      parsedQuery.constructClause().triples_, qet.getVariableColumns(), *index,
      mediaType, offsetLeftToApply(qet, limitOffset));
  auto serialize = std::make_shared<const SegmentSerializer>(
      [serializer = std::move(serializer), index = std::move(index)](
          const ExportMorselSegment& segment, uint64_t begin, uint64_t end,
          ScatterGatherChunkBuilder& builder) {
        serializer->appendRows(segment.block_->idTable_.asStaticView<0>(),
                               segment.block_->localVocab_, *index, begin, end,
                               segment.rowsExportedBeforeBlock_, builder);
      });
  return serializeMorsels(qet, limitOffset, std::nullopt, std::move(serialize),
                          std::move(cancellationHandle), scheduler);
}

// The morsels of `parsedQuery` in `mediaType`. Requires `canHandle`.
cppcoro::generator<ScatterGatherChunkBuilder> buildMorsels(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    ad_utility::MediaType mediaType,
    ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  if (parsedQuery.hasConstructClause()) {
    return buildConstructMorsels(parsedQuery, qet, mediaType,
                                 std::move(cancellationHandle), scheduler);
  }
  return buildSelectMorsels(parsedQuery, qet,
                            ExportEngineV2::rowFormatFor(mediaType).value(),
                            std::move(cancellationHandle), scheduler);
}

}  // namespace

// True when every operation in the tree rooted at `operation` is one the V2
// serializer understands: triple scans, joins, filters, BIND, inline VALUES,
// and internal sorts. Internal `Sort` (e.g. over `VALUES` below a join) only
// orders the join input; V2 formats yielded rows in order, so bytes are
// unaffected. User `ORDER BY` never reaches this check: the router rejects it
// at the query-shape level. A null or foreign child falls back to Legacy V1
// (safe direction).
static bool operationTreeIsSupported(const ::Operation& operation) {
  const bool supported =
      dynamic_cast<const ::IndexScan*>(&operation) != nullptr ||
      dynamic_cast<const ::HasPredicateScan*>(&operation) != nullptr ||
      dynamic_cast<const ::Join*>(&operation) != nullptr ||
      dynamic_cast<const ::CartesianProductJoin*>(&operation) != nullptr ||
      dynamic_cast<const ::Filter*>(&operation) != nullptr ||
      dynamic_cast<const ::Bind*>(&operation) != nullptr ||
      dynamic_cast<const ::Sort*>(&operation) != nullptr ||
      dynamic_cast<const ::Values*>(&operation) != nullptr;
  if (!supported) {
    return false;
  }
  for (const auto* child : operation.getChildren()) {
    if (child == nullptr ||
        !operationTreeIsSupported(*child->getRootOperation())) {
      return false;
    }
  }
  return true;
}

// _____________________________________________________________________________
bool ExportEngineV2::canHandle(const ParsedQuery& parsedQuery,
                               ad_utility::MediaType mediaType) noexcept {
  using enum ad_utility::MediaType;
  if (parsedQuery.hasSelectClause()) {
    return mediaType == csv || mediaType == tsv;
  }
  if (parsedQuery.hasConstructClause()) {
    // CONSTRUCT CSV/TSV and a deduplicating CONSTRUCT stay on Legacy.
    return ConstructRowSerializer::supportsMediaType(mediaType) &&
           std::holds_alternative<ad_utility::DeduplicationMode::None>(
               getRuntimeParameter<
                   &RuntimeParameters::constructDeduplication_>()
                   .value_);
  }
  return false;
}

// _____________________________________________________________________________
bool ExportEngineV2::canHandle(const ParsedQuery& parsedQuery,
                               const QueryExecutionTree& qet,
                               ad_utility::MediaType mediaType) noexcept {
  return canHandle(parsedQuery, mediaType) &&
         operationTreeIsSupported(*qet.getRootOperation());
}

// _____________________________________________________________________________
std::optional<RowFormat> ExportEngineV2::rowFormatFor(
    ad_utility::MediaType mediaType) noexcept {
  using enum ad_utility::MediaType;
  if (mediaType == csv) {
    return RowFormat::Csv;
  }
  if (mediaType == tsv) {
    return RowFormat::Tsv;
  }
  return std::nullopt;
}

// _____________________________________________________________________________
void ExportEngineV2::appendSerializedRows(
    const IdTableView<0>& idTable, const LocalVocab& localVocab,
    RowFormat format, ScatterGatherChunkBuilder& builder, const Index& index,
    ql::span<const std::optional<ColumnIndex>> selectedColumns,
    uint64_t rowBegin, uint64_t rowEnd, ql::span<const ColumnLattice> lattice) {
  const uint64_t numRows = idTable.numRows();
  rowEnd = std::min(rowEnd, numRows);
  if (rowBegin >= rowEnd) {
    return;
  }
  const size_t numOutputCols =
      selectedColumns.empty() ? idTable.numColumns() : selectedColumns.size();

  auto columnAt = [&](size_t outCol) -> std::optional<ColumnIndex> {
    if (selectedColumns.empty()) {
      return outCol;
    }
    return selectedColumns[outCol];
  };

  // Uniform encoded columns (Int/Double/Bool/Date/...) use the same
  // idToStringAndTypeForEncodedValue bytes as Legacy. Mixed or vocab columns
  // go through `resolveMixedColumn` (one batched `lookupBatch` for the
  // `VocabIndex` ids, the `idToStringAndType` bytes for the rest). Compile-time
  // MonomorphicRowSerializer stays off: its to_chars doubles and IRI brackets
  // do not match SELECT CSV (MonomorphicSerializersTest).
  const size_t n = static_cast<size_t>(rowEnd - rowBegin);
  auto isVocabLike = [](Datatype d) {
    using enum Datatype;
    return d == VocabIndex || d == LocalVocabIndex ||
           d == SecondaryVocabIndex || d == WordVocabIndex ||
           d == TextRecordIndex || d == EncodedVal;
  };
  std::vector<std::optional<ResolvedColumn>> resolved(numOutputCols);
  for (size_t outCol = 0; outCol < numOutputCols; ++outCol) {
    const auto col = columnAt(outCol);
    if (!col.has_value()) {
      continue;
    }
    // A proven-trivial plan-time lattice (WP-A `BIND` constants) already
    // settles the column: every row holds this datatype, so the per-chunk
    // uniformity scan below is redundant. All other lattices (`Union`,
    // `Vocab`, `Encoded`, short spans) keep the runtime check, which is the
    // only path that may select the vocab resolvers.
    // The lattice parallels the SELECT list, so it only applies when the
    // caller selected explicit columns. With an empty `selectedColumns` the
    // output follows table order and the lattice would misalign.
    const ColumnLattice colLattice =
        !selectedColumns.empty() && outCol < lattice.size()
            ? lattice[outCol]
            : ColumnLattice::Union;
    const bool latticeTrivial = colLattice == ColumnLattice::Int ||
                                colLattice == ColumnLattice::Double ||
                                colLattice == ColumnLattice::Bool ||
                                colLattice == ColumnLattice::Date ||
                                colLattice == ColumnLattice::GeoPoint;
    const auto ids = idTable.getColumn(col.value()).subspan(rowBegin, n);
    bool uniformEncoded =
        latticeTrivial || (!ids.empty() && !isVocabLike(ids[0].getDatatype()));
    if (uniformEncoded && !latticeTrivial) {
      const Datatype dt = ids[0].getDatatype();
      for (Id id : ids) {
        if (id.getDatatype() != dt) {
          uniformEncoded = false;
          break;
        }
      }
    }
    auto& column = resolved[outCol].emplace(n);
    if (uniformEncoded) {
      for (size_t i = 0; i < n; ++i) {
        auto cell = ql::exportIds::idToStringAndTypeForEncodedValue(ids[i]);
        if (cell.has_value()) {
          column.setCopy(i, cell.value().first);
        }
      }
    } else if (format == RowFormat::Csv) {
      resolveMixedColumn<RowFormat::Csv>(column, index, ids, localVocab);
    } else {
      resolveMixedColumn<RowFormat::Tsv>(column, index, ids, localVocab);
    }
    column.finish();
  }

  // Write the rows straight into the builder's copy buffer: the exact window
  // size is known, so the buffer grows at most once per window and every
  // cell's bytes are copied exactly once (from the decoded vocabulary batch or
  // the column's scratch buffer). This also keeps one builder segment per
  // window run instead of one per cell.
  const char separator = format == RowFormat::Csv ? ',' : '\t';
  size_t windowBytes = n * numOutputCols;  // separators and newlines
  for (const auto& column : resolved) {
    if (column.has_value()) {
      windowBytes += column->totalBytes();
    }
  }
  builder.appendCopiedWith(windowBytes, [&](std::string& out) {
    for (size_t i = 0; i < n; ++i) {
      for (size_t outCol = 0; outCol < numOutputCols; ++outCol) {
        if (outCol > 0) {
          out.push_back(separator);
        }
        if (resolved[outCol].has_value()) {
          out.append((*resolved[outCol])[i]);
        }
      }
      out.push_back('\n');
    }
  });
}

ScatterGatherChunk ExportEngineV2::serializeTableChunk(
    const IdTableView<0>& idTable, const LocalVocab& localVocab,
    RowFormat format, ScatterGatherChunkBuilder& builder, const Index& index,
    ql::span<const std::optional<ColumnIndex>> selectedColumns,
    uint64_t rowBegin, uint64_t rowEnd) {
  appendSerializedRows(idTable, localVocab, format, builder, index,
                       selectedColumns, rowBegin, rowEnd);
  return std::move(builder).finalize();
}

ScatterGatherChunk ExportEngineV2::serializeTableChunk(
    const IdTable& idTable, const LocalVocab& localVocab, RowFormat format,
    ScatterGatherChunkBuilder& builder, const Index& index,
    ql::span<const std::optional<ColumnIndex>> selectedColumns,
    uint64_t rowBegin, uint64_t rowEnd) {
  return serializeTableChunk(idTable.asStaticView<0>(), localVocab, format,
                             builder, index, selectedColumns, rowBegin, rowEnd);
}

// _____________________________________________________________________________
cppcoro::generator<std::string> ExportEngineV2::computeResult(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    ad_utility::MediaType mediaType,
    ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  ad_utility::Timer timer{ad_utility::Timer::Started};

  if (!canHandle(parsedQuery, qet, mediaType)) {
    // Backstop: the server routes op-unsupported plans to Legacy upfront, so
    // reaching this branch means the gates disagree. Say so loudly instead of
    // serving Legacy bytes under a V2 log line.
    AD_LOG_INFO << "ExportEngineV2 falls back to Legacy V1 "
                   "(operation tree not supported)"
                << std::endl;
    for (auto& chunk : ExportQueryExecutionTrees::computeResult(
             parsedQuery, qet, mediaType, timer,
             std::move(cancellationHandle))) {
      co_yield std::move(chunk);
    }
    co_return;
  }

  for (auto builder : buildMorsels(parsedQuery, qet, mediaType,
                                   cancellationHandle, scheduler)) {
    co_yield std::move(builder).finalizeToString();
  }
}

// _____________________________________________________________________________
cppcoro::generator<ScatterGatherChunk> ExportEngineV2::computeResultChunks(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    ad_utility::MediaType mediaType,
    ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  AD_CONTRACT_CHECK(canHandle(parsedQuery, qet, mediaType));
  // Serialize on the caller thread. Do not spawn a producer thread here:
  // GCC rewrites this function as a coroutine frame and rejected
  // `std::thread` + `AsyncChunkPipeline` locals (91a9a7845). Overlap with
  // HTTP send is `runStreamAsync` in `Server::sendStreamableResponse`.
  for (auto builder : buildMorsels(parsedQuery, qet, mediaType,
                                   std::move(cancellationHandle), scheduler)) {
    auto chunk = std::move(builder).finalize();
    if (!chunk.empty()) {
      co_yield std::move(chunk);
    }
  }
}

}  // namespace ql::engine::export_v2
