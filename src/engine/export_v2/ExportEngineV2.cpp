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
#include <thread>
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

template <typename Sink>
void forEachResultBlock(const Result& result, Sink&& sink) {
  if (result.isFullyMaterialized()) {
    Result::IdTableVocabPair pair{result.cloneIdTable(),
                                  result.localVocab().clone()};
    std::invoke(sink, std::move(pair));
    return;
  }
  for (auto& pair : result.idTables()) {
    std::invoke(sink, std::move(pair));
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

  VectorStreamSource source{VectorStreamConfig{RowsPerChunk{8192}}};

  AsyncChunkPipeline<std::string> pipeline{AsyncChunkPipelineConfig{
      .capacity_ = 2,
      .runtimeEnabled_ = true,
  }};

  // Optional scheduler reserved for WP7 morsel offload; overnight slice uses a
  // dedicated producer thread so the HTTP coroutine can stream.
  (void)scheduler;

  std::exception_ptr producerError;
  std::thread producer{[&]() {
    try {
      std::vector<Result::IdTableVocabPair> pendingBlocks;
      forEachResultBlock(*result, [&](Result::IdTableVocabPair block) {
        cancellationHandle->throwIfCancelled();
        pendingBlocks.clear();
        pendingBlocks.push_back(std::move(block));
        source.run(pendingBlocks, [&](const Result::IdTableVocabPair& chunk) {
          cancellationHandle->throwIfCancelled();
          ScatterGatherChunkBuilder builder;
          auto sgChunk =
              serializeTableChunk(chunk.idTable_, chunk.localVocab_, format,
                                  builder, index, columns);
          if (pipeline.push(sgChunk.toString()) ==
              qlever::export_v2::PushResult::Closed) {
            throw std::runtime_error{
                "ExportEngineV2: async chunk pipeline closed while producing"};
          }
        });
      });
      pipeline.finish();
    } catch (...) {
      producerError = std::current_exception();
      pipeline.fail(producerError);
    }
  }};

  try {
    while (auto chunk = pipeline.pop()) {
      cancellationHandle->throwIfCancelled();
      co_yield std::move(*chunk);
    }
  } catch (...) {
    pipeline.cancel();
    producer.join();
    throw;
  }

  producer.join();
  if (producerError) {
    std::rethrow_exception(producerError);
  }
}

}  // namespace ql::engine::export_v2
