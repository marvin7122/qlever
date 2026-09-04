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
#include "engine/ExportQueryExecutionTrees.h"
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

const qlever::export_v2::ImmutableByteBuffer& csvComma() {
  static const qlever::export_v2::ImmutableByteBuffer buf{std::string(",")};
  return buf;
}

const qlever::export_v2::ImmutableByteBuffer& tsvTab() {
  static const qlever::export_v2::ImmutableByteBuffer buf{std::string("\t")};
  return buf;
}

const qlever::export_v2::ImmutableByteBuffer& newline() {
  static const qlever::export_v2::ImmutableByteBuffer buf{std::string("\n")};
  return buf;
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

std::vector<std::optional<ColumnIndex>> selectedColumnIndices(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause) {
  auto selected = qet.selectedVariablesToColumnIndices(selectClause, true);
  std::vector<std::optional<ColumnIndex>> columns;
  columns.reserve(selected.size());
  for (const auto& entry : selected) {
    if (entry.has_value()) {
      columns.emplace_back(entry.value().columnIndex_);
    } else {
      columns.emplace_back(std::nullopt);
    }
  }
  return columns;
}

// One builder per header / 8192-row morsel. Callers choose finalizeToString
// (default HTTP) or finalize (scatter-gather HTTP). When `scheduler` is set
// (live V2 default), CPU serialize runs on the isolated helper pool with
// ordered consume on this thread; helpers never write the socket.
cppcoro::generator<ScatterGatherChunkBuilder> buildSerializedMorsels(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    RowFormat format, ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  const auto& selectClause = parsedQuery.selectClause();
  const auto columns = selectedColumnIndices(qet, selectClause);
  const Index& index = qet.getQec()->getIndex();

  {
    ScatterGatherChunkBuilder header;
    header.appendOwned(makeHeaderLine(selectClause, format));
    co_yield std::move(header);
  }

  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();

  constexpr uint64_t morselRows = 8192;
  auto serializeOnThisThread = [&](const IdTableView<0>& table,
                                   const LocalVocab& localVocab, uint64_t begin,
                                   uint64_t end) {
    ScatterGatherChunkBuilder builder;
    ExportEngineV2::appendSerializedRows(table, localVocab, format, builder,
                                         index, columns, begin, end);
    return builder;
  };

  if (scheduler == nullptr) {
    uint64_t resultSize = 0;
    for (const auto& tableWithRange : ExportQueryExecutionTrees::getRowIndices(
             parsedQuery._limitOffset, *result, resultSize)) {
      cancellationHandle->throwIfCancelled();
      if (tableWithRange.view_.empty()) {
        continue;
      }
      const auto& table = tableWithRange.tableWithVocab_.idTable();
      const auto& localVocab = tableWithRange.tableWithVocab_.localVocab();
      const uint64_t rowBegin = tableWithRange.view_.front();
      const uint64_t rowEnd = rowBegin + tableWithRange.view_.size();
      for (uint64_t begin = rowBegin; begin < rowEnd; begin += morselRows) {
        cancellationHandle->throwIfCancelled();
        const uint64_t end = std::min(rowEnd, begin + morselRows);
        auto builder = serializeOnThisThread(table, localVocab, begin, end);
        if (!builder.empty()) {
          co_yield std::move(builder);
        }
      }
    }
    co_return;
  }

  AD_LOG_INFO << "ExportEngineV2 using ElasticExportScheduler ("
              << scheduler->workerThreadCount() << " helper threads)"
              << std::endl;
  auto session = scheduler->createSession<ScatterGatherChunkBuilder>();
  auto columnsPtr =
      std::make_shared<std::vector<std::optional<ColumnIndex>>>(columns);
  const Index* indexPtr = &index;
  constexpr size_t kSubmitWindow = 8;
  size_t inflight = 0;

  auto consumeOne = [&]() {
    auto builder = session.consumeNextResult();
    --inflight;
    return builder;
  };

  uint64_t resultSize = 0;
  for (const auto& tableWithRange : ExportQueryExecutionTrees::getRowIndices(
           parsedQuery._limitOffset, *result, resultSize)) {
    cancellationHandle->throwIfCancelled();
    if (tableWithRange.view_.empty()) {
      continue;
    }
    const uint64_t rowBegin = tableWithRange.view_.front();
    const uint64_t rowEnd = rowBegin + tableWithRange.view_.size();
    std::shared_ptr<const IdTable> tablePtr;
    std::shared_ptr<const LocalVocab> vocabPtr;
    const bool shareMaterialized = result->isFullyMaterialized();
    if (!shareMaterialized) {
      tablePtr = std::make_shared<IdTable>(
          tableWithRange.tableWithVocab_.idTable().clone());
      vocabPtr = std::make_shared<LocalVocab>(
          tableWithRange.tableWithVocab_.localVocab().clone());
    }
    for (uint64_t begin = rowBegin; begin < rowEnd; begin += morselRows) {
      cancellationHandle->throwIfCancelled();
      const uint64_t end = std::min(rowEnd, begin + morselRows);
      if (shareMaterialized) {
        session.submitMorsel([result, columnsPtr, indexPtr, format, begin, end,
                              cancellationHandle]() {
          cancellationHandle->throwIfCancelled();
          ScatterGatherChunkBuilder builder;
          ExportEngineV2::appendSerializedRows(
              result->idTableView(), result->localVocab(), format, builder,
              *indexPtr, *columnsPtr, begin, end);
          return builder;
        });
      } else {
        session.submitMorsel([tablePtr, vocabPtr, columnsPtr, indexPtr, format,
                              begin, end, cancellationHandle]() {
          cancellationHandle->throwIfCancelled();
          ScatterGatherChunkBuilder builder;
          ExportEngineV2::appendSerializedRows(
              tablePtr->asStaticView<0>(), *vocabPtr, format, builder,
              *indexPtr, *columnsPtr, begin, end);
          return builder;
        });
      }
      ++inflight;
      if (inflight >= kSubmitWindow) {
        auto builder = consumeOne();
        if (!builder.empty()) {
          co_yield std::move(builder);
        }
      }
    }
  }
  while (session.hasMoreResults()) {
    auto builder = consumeOne();
    if (!builder.empty()) {
      co_yield std::move(builder);
    }
  }
}

}  // namespace

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
    uint64_t rowBegin, uint64_t rowEnd) {
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

  // Batch-resolve each bound column with the same idToStringAndType contract
  // as Legacy (CSV quoting, vocab strings). MonomorphicRowSerializer is not
  // a drop-in: it needs a compile-time column schema, does not resolve Ids,
  // formats doubles with to_chars ("1" vs Legacy "1.0"), and keeps IRI
  // brackets that SELECT CSV strips. See MonomorphicSerializersTest.
  const size_t n = static_cast<size_t>(rowEnd - rowBegin);
  std::vector<std::vector<ResolvedCell>> resolved(numOutputCols);
  for (size_t outCol = 0; outCol < numOutputCols; ++outCol) {
    const auto col = columnAt(outCol);
    if (!col.has_value()) {
      continue;
    }
    const auto ids = idTable.getColumn(col.value()).subspan(rowBegin, n);
    if (format == RowFormat::Csv) {
      resolved[outCol] = resolveColumn<RowFormat::Csv>(index, ids, localVocab);
    } else {
      resolved[outCol] = resolveColumn<RowFormat::Tsv>(index, ids, localVocab);
    }
    AD_CORRECTNESS_CHECK(resolved[outCol].size() == n);
  }

  const auto& separator = format == RowFormat::Csv ? csvComma() : tsvTab();
  for (size_t i = 0; i < n; ++i) {
    for (size_t outCol = 0; outCol < numOutputCols; ++outCol) {
      if (outCol > 0) {
        builder.appendOwned(separator.slice(0, separator.size()));
      }
      if (resolved[outCol].empty()) {
        continue;
      }
      auto& cell = resolved[outCol][i];
      if (cell.has_value()) {
        builder.appendOwned(std::move(cell.value().first));
      }
    }
    builder.appendOwned(newline().slice(0, newline().size()));
  }
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

  if (!canHandle(parsedQuery, mediaType)) {
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
  AD_CONTRACT_CHECK(canHandle(parsedQuery, mediaType));
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
