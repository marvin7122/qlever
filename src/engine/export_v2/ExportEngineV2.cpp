// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "engine/export_v2/ExportEngineV2.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
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
#include "global/Id.h"
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

  {
    ScatterGatherChunkBuilder header;
    header.appendOwned(makeHeaderLine(selectClause, format));
    co_yield std::move(header);
  }

  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();

  // Serialize one morsel plan: every segment straight from its lazy block.
  auto serializePlan = [&](const ExportMorsel& plan) {
    ScatterGatherChunkBuilder builder;
    for (const auto& segment : plan.segments_) {
      ExportEngineV2::appendSerializedRows(
          segment.block_->idTable_.asStaticView<0>(),
          segment.block_->localVocab_, format, builder, index, columns.indices_,
          segment.begin_, segment.end_, columns.lattice_.columns_);
    }
    return builder;
  };

  constexpr uint64_t rowsPerMorsel = 8192;
  if (scheduler == nullptr) {
    for (auto&& plan : planExportMorsels(
             result->idTables(), parsedQuery._limitOffset, rowsPerMorsel)) {
      cancellationHandle->throwIfCancelled();
      auto builder = serializePlan(plan);
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
  session.setOrdered(limitOffset._limit.has_value() || limitOffset._offset != 0 ||
                     limitOffset.textLimit_.has_value() ||
                     limitOffset.exportLimit_.has_value());
  auto columnsPtr = std::make_shared<std::vector<std::optional<ColumnIndex>>>(
      columns.indices_);
  auto latticePtr =
      std::make_shared<std::vector<ColumnLattice>>(columns.lattice_.columns_);
  const Index* indexPtr = &index;

  for (auto&& plan : planExportMorsels(
           result->idTables(), parsedQuery._limitOffset, rowsPerMorsel)) {
    cancellationHandle->throwIfCancelled();
    // The plan owns its blocks, so the worker can serialize after the driver
    // moved on. No table or vocabulary clone: one shared owner per block.
    session.submitMorsel([morsel = std::move(plan), columnsPtr, latticePtr,
                          indexPtr, format, cancellationHandle]() {
      cancellationHandle->throwIfCancelled();
      ScatterGatherChunkBuilder builder;
      for (const auto& segment : morsel.segments_) {
        ExportEngineV2::appendSerializedRows(
            segment.block_->idTable_.asStaticView<0>(),
            segment.block_->localVocab_, format, builder, *indexPtr,
            *columnsPtr, segment.begin_, segment.end_, *latticePtr);
      }
      return builder;
    });
  }
  while (session.hasMoreResults()) {
    auto builder = session.consumeNextResult();
    if (!builder.empty()) {
      co_yield std::move(builder);
    }
  }
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
  // still go through idsToStringAndType / lookupBatch. Compile-time
  // MonomorphicRowSerializer stays off: its to_chars doubles and IRI brackets
  // do not match SELECT CSV (MonomorphicSerializersTest).
  const size_t n = static_cast<size_t>(rowEnd - rowBegin);
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
  }

  // Assemble the whole window into one string with a single coalesced append.
  // Per-cell appends would create one builder segment per cell, and the
  // builder's invariant guard scans every segment on each append: quadratic
  // in the window size (~600 s for 1M H-size rows, measured). One append per
  // window keeps segments per morsel in the single digits.
  const char separator = format == RowFormat::Csv ? ',' : '\t';
  std::string out;
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
