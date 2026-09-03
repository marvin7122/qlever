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
// before falling back to the legacy RdfEscaping path (required for CSV quoting).
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

template <RowFormat Format>
void appendResolvedId(ScatterGatherChunkBuilder& builder, const Index& index,
                      Id id, const LocalVocab& localVocab) {
  constexpr bool removeQuotes = Format == RowFormat::Csv;
  auto optionalStringAndType = ql::exportIds::idToStringAndType<removeQuotes>(
      index, id, localVocab, escapeCell<Format>);
  if (optionalStringAndType.has_value()) {
    builder.appendCopy(optionalStringAndType.value().first);
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
  return absl::StrCat(
      absl::StrJoin(variables, std::string_view{&separator, 1}), "\n");
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
  // LIMIT/OFFSET / export-limit handling is still on the Legacy path.
  // TODO(export-v2): wire getRowIndices-equivalent limiting into V2.
  return parsedQuery._limitOffset.isUnconstrained() &&
         !parsedQuery._limitOffset.exportLimit_.has_value();
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
ScatterGatherChunk ExportEngineV2::serializeTableChunk(
    const IdTable& idTable, const LocalVocab& localVocab, RowFormat format,
    ScatterGatherChunkBuilder& builder, const Index& index,
    ql::span<const std::optional<ColumnIndex>> selectedColumns) {
  const size_t numRows = idTable.numRows();
  const size_t numOutputCols =
      selectedColumns.empty() ? idTable.numColumns() : selectedColumns.size();

  auto columnAt = [&](size_t outCol) -> std::optional<ColumnIndex> {
    if (selectedColumns.empty()) {
      return outCol;
    }
    return selectedColumns[outCol];
  };

  for (size_t row = 0; row < numRows; ++row) {
    for (size_t outCol = 0; outCol < numOutputCols; ++outCol) {
      if (outCol > 0) {
        builder.appendCopy(format == RowFormat::Csv ? "," : "\t");
      }
      const auto col = columnAt(outCol);
      if (!col.has_value()) {
        continue;
      }
      Id id = idTable(row, col.value());
      if (format == RowFormat::Csv) {
        appendResolvedId<RowFormat::Csv>(builder, index, id, localVocab);
      } else {
        appendResolvedId<RowFormat::Tsv>(builder, index, id, localVocab);
      }
    }
    builder.appendCopy("\n");
  }

  return std::move(builder).finalize();
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
  const auto& selectClause = parsedQuery.selectClause();
  const auto columns = selectedColumnIndices(qet, selectClause);
  const Index& index = qet.getQec()->getIndex();

  co_yield makeHeaderLine(selectClause, format);

  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();

  // Synchronous vertical slice: VectorStreamSource → escape → ScatterGather →
  // co_yield. A producer-thread + AsyncChunkPipeline nested in this coroutine
  // fails to compile under GCC (coroutine frame rewrite); async ring is a
  // follow-up seam. `co_yield` cannot appear inside the forEach/run lambdas, so
  // we buffer one Result block's serialized chunks then yield them here.
  (void)scheduler;
  VectorStreamSource source{VectorStreamConfig{RowsPerChunk{8192}}};
  std::vector<Result::IdTableVocabPair> pendingBlocks;
  std::vector<std::string> pendingChunks;

  auto serializeBlock = [&](Result::IdTableVocabPair block) {
    cancellationHandle->throwIfCancelled();
    pendingBlocks.clear();
    pendingBlocks.push_back(std::move(block));
    pendingChunks.clear();
    source.run(pendingBlocks, [&](const Result::IdTableVocabPair& chunk) {
      cancellationHandle->throwIfCancelled();
      ScatterGatherChunkBuilder builder;
      auto sgChunk = serializeTableChunk(chunk.idTable_, chunk.localVocab_,
                                         format, builder, index, columns);
      pendingChunks.push_back(sgChunk.toString());
    });
  };

  if (result->isFullyMaterialized()) {
    serializeBlock(Result::IdTableVocabPair{result->cloneIdTable(),
                                            result->localVocab().clone()});
    for (auto& serialized : pendingChunks) {
      co_yield std::move(serialized);
    }
  } else {
    for (auto& pair : result->idTables()) {
      serializeBlock(std::move(pair));
      for (auto& serialized : pendingChunks) {
        co_yield std::move(serialized);
      }
    }
  }
}

}  // namespace ql::engine::export_v2
