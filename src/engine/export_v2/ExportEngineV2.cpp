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
#include "global/Id.h"
#include "global/RuntimeParameters.h"
#include "index/ExportIds.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Timer.h"

namespace ql::engine::export_v2 {
namespace {

// Escape `input` for CSV/TSV. Uses SimdEscapeClassifier as a fast reject filter
// before falling back to the legacy RdfEscaping path (required for CSV
// quoting).
template <RowFormat Format>
std::string escapeCell(std::string input) {
  if (input.empty()) {
    return input;
  }
  if constexpr (Format == RowFormat::Csv) {
    if (SimdEscapeClassifier::findFirstEscapeSimd<EscapeFormat::Csv>(input) ==
        std::string_view::npos) {
      return input;
    }
    return RdfEscaping::escapeForCsv(std::move(input));
  } else {
    static_assert(Format == RowFormat::Tsv);
    if (SimdEscapeClassifier::findFirstEscapeSimd<EscapeFormat::Tsv>(input) ==
        std::string_view::npos) {
      return input;
    }
    return RdfEscaping::escapeForTsv(std::move(input));
  }
}

using ResolvedCell = std::optional<std::pair<std::string, const char*>>;

template <RowFormat Format>
std::vector<ResolvedCell> resolveColumn(const Index& index,
                                        ql::span<const Id> ids,
                                        const LocalVocab& localVocab) {
  constexpr bool removeQuotes = Format == RowFormat::Csv;
  return ql::exportIds::idsToStringAndType<removeQuotes>(index, ids, localVocab,
                                                         escapeCell<Format>);
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
  const std::vector<ResolvedCell>* resolved_ = nullptr;
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
    const auto& cell = (*column.resolved_)[row];
    return cell.has_value() ? std::string_view{cell.value().first}
                            : std::string_view{};
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

// Checkpoint interval for cooperative revocation: an in-flight morsel
// abandons its remainder at most this many rows after a new query arrives,
// so a foreground query waits for pool threads no longer than one
// checkpoint of serialization. Ordered sessions ignore checkpoints and run
// each morsel to completion for deterministic prefixes.
constexpr uint64_t kRevocationCheckRows = 1024;

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
    const uint64_t epoch = state_->currentEpoch();
    return [*this, plan = std::move(plan), epoch]() mutable {
      return run(std::move(plan), epoch);
    };
  }

  ScatterGatherChunkBuilder run(ExportMorsel plan, uint64_t epoch) const {
    ScatterGatherChunkBuilder builder;
    const size_t numSegments = plan.segments_.size();
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

  constexpr uint64_t rowsPerMorsel = 8192;
  if (scheduler == nullptr) {
    // No session exists here, so nothing can revoke: serialize each plan
    // directly without checkpoints.
    for (auto&& plan : planExportMorsels(
             result->idTables(), parsedQuery._limitOffset, rowsPerMorsel)) {
      cancellationHandle->throwIfCancelled();
      ScatterGatherChunkBuilder builder;
      for (const auto& segment : plan.segments_) {
        ExportEngineV2::appendSerializedRows(
            segment.block_->idTable_.asStaticView<0>(),
            segment.block_->localVocab_, format, builder, index,
            columns.indices_, segment.begin_, segment.end_,
            columns.lattice_.columns_, monomorphicRows);
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
  for (auto&& plan : planExportMorsels(
           result->idTables(), parsedQuery._limitOffset, rowsPerMorsel)) {
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

// The morsels of `buildSerializedMorsels` as the chunk types of the two HTTP
// send modes: one string per morsel (`computeResult`) or the scatter-gather
// chunk without empty ones (`computeResultChunks`).
cppcoro::generator<std::string> serializedMorselStrings(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    RowFormat format, ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  for (auto builder :
       buildSerializedMorsels(parsedQuery, qet, format,
                              std::move(cancellationHandle), scheduler)) {
    co_yield std::move(builder).finalizeToString();
  }
}

cppcoro::generator<ScatterGatherChunk> serializedMorselChunks(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    RowFormat format, ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  for (auto builder :
       buildSerializedMorsels(parsedQuery, qet, format,
                              std::move(cancellationHandle), scheduler)) {
    auto chunk = std::move(builder).finalize();
    if (!chunk.empty()) {
      co_yield std::move(chunk);
    }
  }
}

// Chunks prefetched by a dedicated producer thread (runtime parameter
// `export-v2-async-pipeline`): `makeChunks` runs on that thread, so the
// generator frame is created, resumed and destroyed there; the calling
// coroutine only pops finished chunks by value.
//
// Deliberately a plain function, so the lambda and the thread are created
// outside of any coroutine body. The first version (a `std::thread` and an
// `AsyncChunkPipeline` as locals of `computeResult`, reverted in 91a9a7845)
// failed to compile with GCC, and was also unsafe: destroying the suspended
// generator (the client abandons the download) destroyed a joinable
// `std::thread` without running the `catch` that joined it, which calls
// `std::terminate`. `AsyncChunkProducer` cancels and joins in its destructor.
constexpr size_t kAsyncPipelineCapacity = 2;

template <typename ChunkType>
using MorselChunkGenerator = cppcoro::generator<ChunkType> (*)(
    const ParsedQuery&, const QueryExecutionTree&, RowFormat,
    ad_utility::SharedCancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler*);

template <typename ChunkType>
std::unique_ptr<qlever::export_v2::AsyncChunkProducer<ChunkType>>
startAsyncProducer(MorselChunkGenerator<ChunkType> makeChunks,
                   const ParsedQuery& parsedQuery,
                   const QueryExecutionTree& qet, RowFormat format,
                   ad_utility::SharedCancellationHandle cancellationHandle,
                   ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  return std::make_unique<qlever::export_v2::AsyncChunkProducer<ChunkType>>(
      [makeChunks, &parsedQuery, &qet, format,
       cancellationHandle = std::move(cancellationHandle), scheduler] {
        return makeChunks(parsedQuery, qet, format, cancellationHandle,
                          scheduler);
      },
      kAsyncPipelineCapacity);
}

template <typename ChunkType>
void logAsyncPipelineStats(
    const qlever::export_v2::AsyncChunkProducer<ChunkType>& producer) {
  const auto stats = producer.stats();
  AD_LOG_INFO << "ExportEngineV2 async pipeline: " << stats.chunksConsumed_
              << " chunks, " << stats.bytesConsumed_ << " bytes, "
              << stats.producerWaits_ << " producer waits, "
              << stats.consumerWaits_ << " consumer waits" << std::endl;
}

bool useAsyncPipeline() {
  return getRuntimeParameter<&RuntimeParameters::exportV2AsyncPipeline_>();
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
  // verbatim (`ColumnType::Preformatted`).
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
  std::vector<std::vector<ResolvedCell>> resolved(numOutputCols);
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
    if (monomorphic && uniformEncoded) {
      if (auto type = directColumnType(ids[0].getDatatype())) {
        monomorphicColumns[outCol] = {type.value(), ids, nullptr};
        continue;
      }
    }
    if (uniformEncoded) {
      resolved[outCol].resize(n);
      for (size_t i = 0; i < n; ++i) {
        resolved[outCol][i] =
            ql::exportIds::idToStringAndTypeForEncodedValue(ids[i]);
      }
    } else if (format == RowFormat::Csv) {
      resolved[outCol] = resolveColumn<RowFormat::Csv>(index, ids, localVocab);
    } else {
      resolved[outCol] = resolveColumn<RowFormat::Tsv>(index, ids, localVocab);
    }
    AD_CORRECTNESS_CHECK(resolved[outCol].size() == n);
    if (monomorphic) {
      monomorphicColumns[outCol] = {ColumnType::Preformatted, ids,
                                    &resolved[outCol]};
    }
  }

  // Assemble the whole window into one string with a single coalesced append.
  // Per-cell appends would create one builder segment per cell (millions of
  // segments per morsel, ~600 s for 1M H-size rows, measured). One append
  // per window keeps segments per morsel in the single digits; the extra
  // coalescing copy is linear and far cheaper than that segment overhead.
  std::string out;
  if (monomorphic) {
    if (format == RowFormat::Csv) {
      dispatchMonomorphicRows<RowFormat::Csv>(monomorphicColumns, n, out);
    } else {
      dispatchMonomorphicRows<RowFormat::Tsv>(monomorphicColumns, n, out);
    }
    builder.appendCopy(out);
    return;
  }
  const char separator = format == RowFormat::Csv ? ',' : '\t';
  for (size_t i = 0; i < n; ++i) {
    for (size_t outCol = 0; outCol < numOutputCols; ++outCol) {
      if (outCol > 0) {
        out.push_back(separator);
      }
      if (resolved[outCol].empty()) {
        continue;
      }
      auto& cell = resolved[outCol][i];
      if (cell.has_value()) {
        out.append(std::move(cell.value().first));
      }
    }
    out.push_back('\n');
  }
  builder.appendCopy(out);
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
  if (!useAsyncPipeline()) {
    for (auto& chunk :
         serializedMorselStrings(parsedQuery, qet, format,
                                 std::move(cancellationHandle), scheduler)) {
      co_yield std::move(chunk);
    }
    co_return;
  }
  auto producer = startAsyncProducer<std::string>(
      &serializedMorselStrings, parsedQuery, qet, format,
      std::move(cancellationHandle), scheduler);
  while (auto chunk = producer->pop()) {
    co_yield std::move(chunk.value());
  }
  logAsyncPipelineStats(*producer);
}

// _____________________________________________________________________________
cppcoro::generator<ScatterGatherChunk> ExportEngineV2::computeResultChunks(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    ad_utility::MediaType mediaType,
    ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  AD_CONTRACT_CHECK(canHandle(parsedQuery, qet, mediaType));
  // Without `export-v2-async-pipeline`, serialize on the caller thread; the
  // overlap with the HTTP send then comes from `runStreamAsync` in
  // `Server::sendStreamableResponse`.
  const auto format = rowFormatFor(mediaType).value();
  if (!useAsyncPipeline()) {
    for (auto& chunk :
         serializedMorselChunks(parsedQuery, qet, format,
                                std::move(cancellationHandle), scheduler)) {
      co_yield std::move(chunk);
    }
    co_return;
  }
  auto producer = startAsyncProducer<ScatterGatherChunk>(
      &serializedMorselChunks, parsedQuery, qet, format,
      std::move(cancellationHandle), scheduler);
  while (auto chunk = producer->pop()) {
    co_yield std::move(chunk.value());
  }
  logAsyncPipelineStats(*producer);
}

}  // namespace ql::engine::export_v2
