// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "engine/export_v2/SelectCsvStreamer.h"

#include <absl/strings/str_join.h>

#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "engine/export_v2/CsvChunkSink.h"
#include "engine/export_v2/VectorStreamSource.h"

namespace ql::engine::export_v2 {

namespace {

// Generate the raw CSV chunks: header line plus one string per rechunked
// result block. Mirrors the V1 `selectQueryResultToStream<csv>` traversal
// (same block iteration, same header, same per-cell conversion via the
// sink), so only the chunking differs.
STREAMABLE_GENERATOR_TYPE streamChunks(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
    SelectCsvStreamer::CancellationHandle cancellationHandle,
    [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  AD_LOG_DEBUG << "Converting V2 result IDs to CSV ..." << std::endl;
  auto selectedColumns =
      qet.selectedVariablesToColumnIndices(selectClause, true);

  // Print header line (without the question marks, like V1).
  std::vector<std::string> variables =
      selectClause.getSelectedVariablesAsStrings();
  ql::ranges::for_each(
      variables, [](std::string& variable) { variable = variable.substr(1); });
  STREAMABLE_YIELD(absl::StrJoin(variables, ","));
  STREAMABLE_YIELD('\n');

  VectorStreamSource source{};
  CsvChunkSink sink{qet.getQec()->getIndex(), selectedColumns};
  std::vector<Id> row;
  std::vector<Result::IdTableVocabPair> staging;
  std::vector<std::string> chunks;
  auto collectChunk = [&chunks, &sink](const Result::IdTableVocabPair& block) {
    chunks.emplace_back();
    sink.appendChunk(block.idTable_, block.localVocab_, chunks.back());
  };

  uint64_t resultSize = 0;
  for (const auto& [pair, range] : ExportQueryExecutionTrees::getRowIndices(
           limitAndOffset, *result, resultSize)) {
    // Materialize the limit/offset slice into an owned block. This single
    // copy is removed by the zero-copy arena work package; the `LocalVocab`
    // clone preserves ID indices by construction.
    const auto& view = pair.idTable();
    const size_t numColumns = view.numColumns();
    IdTable slice{numColumns, qet.getQec()->getAllocator()};
    row.clear();
    row.reserve(numColumns);
    for (uint64_t i : range) {
      row.clear();
      for (size_t column = 0; column < numColumns; ++column) {
        row.push_back(view(i, column));
      }
      slice.push_back(row);
    }
    staging.clear();
    chunks.clear();
    staging.emplace_back(std::move(slice), pair.localVocab().clone());
    source.run(staging, collectChunk);
    for (auto& chunk : chunks) {
      STREAMABLE_YIELD(std::move(chunk));
    }
    cancellationHandle->throwIfCancelled();
  }
  AD_LOG_DEBUG << "Done creating V2 CSV result.\n";
  STREAMABLE_RETURN;
}

}  // namespace

// _____________________________________________________________________________
ExportQueryExecutionTrees::ComputeResultReturnType SelectCsvStreamer::run(
    const QueryExecutionTree& qet, const ParsedQuery& parsedQuery,
    CancellationHandle cancellationHandle,
    [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  auto limit = parsedQuery._limitOffset;
  ExportQueryExecutionTrees::compensateForLimitOffsetClause(limit, qet);

  // This call triggers the possibly expensive computation of the query
  // result unless the result is already cached (same as V1).
  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  auto inner =
      streamChunks(qet, parsedQuery.selectClause(), limit, std::move(result),
                   std::move(cancellationHandle), streamableYielder);

  return [](auto range) -> cppcoro::generator<std::string> {
    for (auto&& item : range) {
      co_yield item;
    }
  }(ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
                            std::move(inner)));

#else
  streamChunks(qet, parsedQuery.selectClause(), limit, std::move(result),
               std::move(cancellationHandle), streamableYielder);
#endif
}

}  // namespace ql::engine::export_v2
