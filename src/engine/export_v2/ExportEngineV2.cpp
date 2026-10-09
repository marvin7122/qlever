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
#include <array>
#include <charconv>
#include <limits>
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
#include "engine/export_v2/ExportMorselPlanner.h"
#include "engine/export_v2/MonomorphicSerializers.h"
#include "engine/export_v2/SimdEscapeClassifier.h"
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

// The text of one output column for a window of rows. A cell views either a
// decoded vocabulary word (kept alive by `batch_`, no copy) or `scratch_`
// (escaped words, values encoded in the `Id`, local-vocab words). Undefined
// and unbound cells are empty views: Legacy and V2 both serialize them as an
// empty field, exactly like an empty string.
class ResolvedColumn {
 private:
  std::vector<std::string_view> cells_;
  std::string scratch_;
  // Cells whose text lives in `scratch_`: (row, offset, size). `scratch_` may
  // reallocate while the column is resolved, so these become views only in
  // `finish`.
  struct ScratchCell {
    size_t row_;
    size_t offset_;
    size_t size_;
  };
  std::vector<ScratchCell> scratchCells_;
  ad_utility::vocabulary::VocabBatchLookupResult batch_;
  size_t totalBytes_ = 0;

 public:
  explicit ResolvedColumn(size_t numRows) : cells_(numRows) {}
  // Pinned: after `finish`, cells view `scratch_`, whose bytes may live inside
  // the object (small-string buffer), so a moved or copied column would
  // dangle.
  ResolvedColumn(const ResolvedColumn&) = delete;
  ResolvedColumn& operator=(const ResolvedColumn&) = delete;
  ResolvedColumn(ResolvedColumn&&) = delete;
  ResolvedColumn& operator=(ResolvedColumn&&) = delete;

  // Point `row` at `text`, which must outlive this column (vocabulary batch).
  void setView(size_t row, std::string_view text) {
    cells_[row] = text;
    totalBytes_ += text.size();
  }

  // Copy `text` into the column's scratch buffer.
  void setCopy(size_t row, std::string_view text) {
    if (text.empty()) {
      return;
    }
    scratchCells_.push_back({row, scratch_.size(), text.size()});
    scratch_.append(text);
    totalBytes_ += text.size();
  }

  void keepAlive(ad_utility::vocabulary::VocabBatchLookupResult batch) {
    batch_ = std::move(batch);
  }

  // Turn the scratch cells into views. Call once, after the last `set*`.
  void finish() {
    for (const auto& cell : scratchCells_) {
      cells_[cell.row_] =
          std::string_view{scratch_.data() + cell.offset_, cell.size_};
    }
  }

  [[nodiscard]] std::string_view operator[](size_t row) const {
    return cells_[row];
  }
  [[nodiscard]] size_t size() const { return cells_.size(); }
  [[nodiscard]] size_t totalBytes() const { return totalBytes_; }
};

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
    index.getImpl().getVocab().lookupBatch(vocabIndices, builder);
    auto words = std::move(builder).finalize();
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

// The writer that `MonomorphicRowSerializer` renders into: appends to one
// window string. Only the operations of the column types that
// `appendSerializedRows` selects (`Integer`, `Double`, `Boolean`,
// `Preformatted`, `Undefined`) are needed.
class StringRowWriter {
 public:
  explicit StringRowWriter(std::string& out) : out_{out} {}
  void writeChar(char c) { out_.push_back(c); }
  void writeRaw(std::string_view bytes) { out_.append(bytes); }
  // Same digits as the Legacy `std::to_string`, without the temporary string.
  template <typename Integer>
  void writeInteger(Integer value) {
    std::array<char, std::numeric_limits<Integer>::digits10 + 3> buffer;
    const auto [end, error] =
        std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
    AD_CORRECTNESS_CHECK(error == std::errc{});
    out_.append(buffer.data(), end);
  }

 private:
  std::string& out_;
};

// One output column of a window, as `MonomorphicRowSerializer` consumes it:
// direct `Id`s for uniform `Int`/`Double`/`Bool` columns, the Legacy-resolved
// (already escaped) cells for everything else, nothing for unbound columns.
struct MonomorphicColumn {
  ColumnType type_ = ColumnType::Undefined;
  ql::span<const Id> ids_;
  const ResolvedColumn* resolved_ = nullptr;
};

// SELECTs with more columns keep the generic assembly loop: the dispatch below
// instantiates one row loop per schema (5^k for k columns).
constexpr size_t kMaxMonomorphicColumns = 3;

// The column type for a window column that holds only `datatype`, if the
// serializer renders it directly from the `Id` (Legacy bytes, see
// `idToStringAndTypeForEncodedValue`).
std::optional<ColumnType> directColumnType(Datatype datatype) {
  switch (datatype) {
    case Datatype::Int:
      return ColumnType::Integer;
    case Datatype::Double:
      return ColumnType::Double;
    case Datatype::Bool:
      return ColumnType::Boolean;
    case Datatype::Undefined:
      return ColumnType::Undefined;
    default:
      return std::nullopt;
  }
}

template <ColumnType Type>
auto monomorphicCell(const MonomorphicColumn& column, size_t row) {
  if constexpr (Type == ColumnType::Integer) {
    return column.ids_[row].getInt();
  } else if constexpr (Type == ColumnType::Double) {
    return column.ids_[row].getDouble();
  } else if constexpr (Type == ColumnType::Boolean) {
    return column.ids_[row];
  } else if constexpr (Type == ColumnType::Preformatted) {
    // `ResolvedColumn` cells are views (empty view = empty field), already
    // escaped for the target format.
    return (*column.resolved_)[row];
  } else {
    static_assert(Type == ColumnType::Undefined);
    return UndefinedCell{};
  }
}

template <RowFormat Format, ColumnType... Types, size_t... Indices>
void writeMonomorphicRows(ql::span<const MonomorphicColumn> columns,
                          size_t numRows, std::string& out,
                          std::index_sequence<Indices...>) {
  StringRowWriter writer{out};
  for (size_t row = 0; row < numRows; ++row) {
    MonomorphicRowSerializer<Types...>::template serializeRow<Format>(
        writer, monomorphicCell<Types>(columns[Indices], row)...);
  }
}

// Turn the runtime column types of one window into a compile-time schema,
// one column at a time, and write all rows with that instantiation.
template <RowFormat Format, ColumnType... Chosen>
void dispatchMonomorphicRows(ql::span<const MonomorphicColumn> columns,
                             size_t numRows, std::string& out) {
  constexpr size_t numChosen = sizeof...(Chosen);
  if constexpr (numChosen > 0) {
    if (columns.size() == numChosen) {
      writeMonomorphicRows<Format, Chosen...>(
          columns, numRows, out, std::make_index_sequence<numChosen>{});
      return;
    }
  }
  if constexpr (numChosen < kMaxMonomorphicColumns) {
    AD_CORRECTNESS_CHECK(columns.size() > numChosen);
    auto next = [&](auto type) {
      dispatchMonomorphicRows<Format, Chosen..., decltype(type)::value>(
          columns, numRows, out);
    };
    using enum ColumnType;
    switch (columns[numChosen].type_) {
      case Integer:
        return next(std::integral_constant<ColumnType, Integer>{});
      case Double:
        return next(std::integral_constant<ColumnType, Double>{});
      case Boolean:
        return next(std::integral_constant<ColumnType, Boolean>{});
      case Preformatted:
        return next(std::integral_constant<ColumnType, Preformatted>{});
      case Undefined:
        return next(std::integral_constant<ColumnType, Undefined>{});
      default:
        AD_FAIL();
    }
  } else {
    AD_FAIL();
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

// The blocks of `result` for `planExportMorsels`. A fully materialized result
// (an index scan below `lazy-index-scan-max-size-materialization`, a cached
// result) has no generator, so it is served as one block. The block is a copy
// because morsels own their blocks and may outlive the caller's reference.
// Ownership moves in here: the lazy range from `idTables()` is self-sustaining
// (see `Result::idTables`), and the materialized path drops the original right
// after cloning, so a fully materialized table is never held twice.
Result::LazyResult resultBlocks(std::shared_ptr<const Result> result) {
  if (!result->isFullyMaterialized()) {
    return result->idTables();
  }
  return Result::LazyResult{
      ad_utility::lazySingleValueRange([result = std::move(result)]() mutable {
        auto pair = Result::IdTableVocabPair{result->cloneIdTable(),
                                             result->localVocab().clone()};
        result.reset();
        return pair;
      })};
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
  // Cap the extrapolation at a bounded multiple of the bytes written so
  // far: the sampled rows may be atypically large, and a revoked or
  // cancelled partial builder would otherwise keep a reservation sized for
  // a full morsel. A low estimate only costs the usual geometric growth.
  builder.reserveCopied(std::min(estimate + estimate / 8, builder.size() * 16));
  return true;
}

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
  std::shared_ptr<std::vector<std::optional<ColumnIndex>>> columnsPtr_;
  std::shared_ptr<std::vector<ColumnLattice>> latticePtr_;
  // Shared ownership: closing the session does not join running helpers, so
  // a helper may still serialize after the request's context is gone.
  std::shared_ptr<const Index> index_;
  RowFormat format_ = RowFormat::Csv;
  bool checkpoints_ = false;
  bool monomorphicRows_ = false;

  absl::AnyInvocable<ScatterGatherChunkBuilder()> makeTask(
      ExportMorsel plan) const {
    return [*this, plan = std::move(plan)]() mutable {
      return run(std::move(plan));
    };
  }

  ScatterGatherChunkBuilder run(ExportMorsel plan) const {
    // The epoch is sampled when the morsel starts, not when it is planned: a
    // morsel that starts after a revocation already runs within the new
    // quota and must not be split again.
    const uint64_t epoch = state_->currentEpoch();
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
        ExportEngineV2::appendSerializedRows(
            seg.block_->idTable_.asStaticView<0>(), seg.block_->localVocab_,
            format_, builder, *index_, *columnsPtr_, pos, windowEnd,
            *latticePtr_, monomorphicRows_);
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
              remainder.segments_.push_back({seg.block_, pos, seg.end_});
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
cppcoro::generator<ScatterGatherChunkBuilder> buildSerializedMorsels(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    RowFormat format, ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  const auto& selectClause = parsedQuery.selectClause();
  const auto columns = selectedColumns(parsedQuery, qet, selectClause);
  const Index& index = qet.getQec()->getIndex();
  const bool monomorphicRows =
      getRuntimeParameter<&RuntimeParameters::exportV2MonomorphicRows_>();

  {
    ScatterGatherChunkBuilder header;
    header.appendOwned(makeHeaderLine(selectClause, format));
    co_yield std::move(header);
  }

  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();

  // The root operation may already have applied LIMIT/OFFSET (e.g. an
  // `IndexScan` handles it `FULL` while scanning, see
  // `QueryPlanner::createExecutionTrees`). Compensate exactly like Legacy
  // (`ExportQueryExecutionTrees::computeResult`) so `planExportMorsels`
  // applies each exactly once; without this, OFFSET rows would be skipped a
  // second time for such roots.
  auto plannedLimitOffset = parsedQuery._limitOffset;
  ExportQueryExecutionTrees::compensateForLimitOffsetClause(plannedLimitOffset,
                                                            qet);

  constexpr uint64_t rowsPerMorsel = 8192;
  if (scheduler == nullptr) {
    // No session exists here, so nothing can revoke: serialize each plan
    // directly without checkpoints.
    for (auto&& plan : planExportMorsels(resultBlocks(std::move(result)),
                                         plannedLimitOffset, rowsPerMorsel)) {
      cancellationHandle->throwIfCancelled();
      ScatterGatherChunkBuilder builder;
      uint64_t rowsDone = 0;
      bool reserved = false;
      for (const auto& segment : plan.segments_) {
        ExportEngineV2::appendSerializedRows(
            segment.block_->idTable_.asStaticView<0>(),
            segment.block_->localVocab_, format, builder, index,
            columns.indices_, segment.begin_, segment.end_,
            columns.lattice_.columns_, monomorphicRows);
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
  const auto policy =
      getRuntimeParameter<&RuntimeParameters::exportV2HelperPolicy_>() ==
              "exclusive"
          ? ad_utility::export_v2::HelperPolicy::Exclusive
          : ad_utility::export_v2::HelperPolicy::Fair;
  auto session = scheduler->createSession<ScatterGatherChunkBuilder>(policy);
  // `ordered` deliberately uses the original clause: a query that was bounded
  // (LIMIT and/or OFFSET) keeps deterministic slot order like Legacy V1, even
  // when compensation above already zeroed the offset for a root that applied
  // it. Only the planner consumes the compensated clause.
  const auto& limitOffset = parsedQuery._limitOffset;
  const bool ordered = limitOffset._limit.has_value() ||
                       limitOffset._offset != 0 ||
                       limitOffset.textLimit_.has_value() ||
                       limitOffset.exportLimit_.has_value();
  session.setOrdered(ordered);
  auto columnsPtr = std::make_shared<std::vector<std::optional<ColumnIndex>>>(
      columns.indices_);
  auto latticePtr =
      std::make_shared<std::vector<ColumnLattice>>(columns.lattice_.columns_);
  // The plans own their blocks, so workers can serialize after the driver
  // moved on. No table or vocabulary clone: one shared owner per block.
  // Unordered tasks checkpoint revocation mid-morsel (see above); ordered
  // tasks run each morsel to completion.
  const CheckpointMorselRunner runner{session.sharedState(),
                                      columnsPtr,
                                      latticePtr,
                                      qet.getQec()->getIndexSharedPtr(),
                                      format,
                                      !ordered,
                                      monomorphicRows};
  // Optional helper trace for concurrency measurements.
  const auto logInterval = std::chrono::milliseconds{
      getRuntimeParameter<&RuntimeParameters::exportV2HelperLogIntervalMs_>()};
  auto nextLog = std::chrono::steady_clock::now();
  auto logHelpers = [&session, logInterval, policy, &nextLog](bool force) {
    if (logInterval.count() == 0) {
      return;
    }
    const auto now = std::chrono::steady_clock::now();
    if (!force && now < nextLog) {
      return;
    }
    nextLog = now + logInterval;
    AD_LOG_INFO << "ExportEngineV2 helpers job=" << session.jobId()
                << " policy=" << ad_utility::export_v2::toString(policy)
                << " active=" << session.activeHelpers()
                << " quota=" << session.helperQuota()
                << " state=" << ad_utility::export_v2::toString(session.state())
                << " consumed=" << session.consumedSlots() << "/"
                << session.totalSlots() << std::endl;
  };
  logHelpers(true);
  // Keep a small window of work available to the pool without pulling the
  // entire lazy result or retaining all serialized builders. Use the session
  // counters so remainders submitted at revocation checkpoints also count.
  const size_t maxInFlight = std::max(size_t{1}, 2 * scheduler->poolSize());
  auto consume = [&]() {
    cancellationHandle->throwIfCancelled();
    auto builder = session.consumeNextResult();
    cancellationHandle->throwIfCancelled();
    logHelpers(false);
    return builder;
  };
  for (auto&& plan : planExportMorsels(resultBlocks(std::move(result)),
                                       plannedLimitOffset, rowsPerMorsel)) {
    cancellationHandle->throwIfCancelled();
    session.submitMorsel(runner.makeTask(std::move(plan)));
    while (session.totalSlots() - session.consumedSlots() >= maxInFlight) {
      auto builder = consume();
      if (!builder.empty()) {
        co_yield std::move(builder);
      }
    }
  }
  while (session.hasMoreResults()) {
    auto builder = consume();
    if (!builder.empty()) {
      co_yield std::move(builder);
    }
  }
  logHelpers(true);
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
  if (!parsedQuery.hasSelectClause()) {
    return false;
  }
  if (mediaType != csv && mediaType != tsv) {
    return false;
  }
  return true;
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
    uint64_t rowBegin, uint64_t rowEnd, ql::span<const ColumnLattice> lattice,
    bool monomorphicRows) {
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
  // still go through idsToStringAndType / lookupBatch. With
  // `monomorphicRows`, uniform Int/Double/Bool/Undefined columns are not
  // resolved to strings: `MonomorphicRowSerializer` renders them from the
  // `Id`s with the Legacy formatting, and the resolved columns are written
  // verbatim (`ColumnType::Preformatted`). Either way the resolved bytes are
  // copied exactly once into the builder (see below).
  const size_t n = static_cast<size_t>(rowEnd - rowBegin);
  const bool monomorphic = monomorphicRows && numOutputCols > 0 &&
                           numOutputCols <= kMaxMonomorphicColumns;
  std::vector<MonomorphicColumn> monomorphicColumns(monomorphic ? numOutputCols
                                                                : 0);
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
      uniformEncoded = std::all_of(ids.begin(), ids.end(), [dt](Id id) {
        return id.getDatatype() == dt;
      });
    }
    if (monomorphic && uniformEncoded) {
      if (auto type = directColumnType(ids[0].getDatatype())) {
        monomorphicColumns[outCol] = {type.value(), ids, nullptr};
        continue;
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
    AD_CORRECTNESS_CHECK(resolved[outCol]->size() == n);
    if (monomorphic) {
      monomorphicColumns[outCol] = {ColumnType::Preformatted, ids,
                                    &resolved[outCol].value()};
    }
  }

  // Assemble the whole window with a single coalesced append into the
  // builder's copy buffer: the exact window size is known, so the buffer
  // grows at most once per window and every cell's bytes are copied exactly
  // once (from the decoded vocabulary batch or the column's scratch buffer).
  // Per-cell appends would create one builder segment per cell (millions of
  // segments per morsel, ~600 s for 1M H-size rows, measured); one append
  // per window keeps segments per morsel in the single digits, and the extra
  // coalescing copy is linear and far cheaper than that segment overhead.
  const char separator = format == RowFormat::Csv ? ',' : '\t';
  size_t windowBytes = n * numOutputCols;  // separators and newlines
  for (const auto& column : resolved) {
    if (column.has_value()) {
      windowBytes += column->totalBytes();
    }
  }
  if (monomorphic) {
    std::string out;
    out.reserve(windowBytes);
    if (format == RowFormat::Csv) {
      dispatchMonomorphicRows<RowFormat::Csv>(monomorphicColumns, n, out);
    } else {
      dispatchMonomorphicRows<RowFormat::Tsv>(monomorphicColumns, n, out);
    }
    builder.appendCopy(out);
    return;
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

  const auto format = rowFormatFor(mediaType).value();
  for (auto builder : buildSerializedMorsels(parsedQuery, qet, format,
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
  const auto format = rowFormatFor(mediaType).value();
  for (auto builder :
       buildSerializedMorsels(parsedQuery, qet, format,
                              std::move(cancellationHandle), scheduler)) {
    auto chunk = std::move(builder).finalize();
    if (!chunk.empty()) {
      co_yield std::move(chunk);
    }
  }
}

}  // namespace ql::engine::export_v2
