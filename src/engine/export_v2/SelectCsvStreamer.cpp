// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/export_v2/SelectCsvStreamer.h"

#include <absl/strings/str_join.h>

#include <algorithm>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "engine/export_v2/CsvChunkSink.h"
#include "util/Exception.h"

namespace ql::engine::export_v2 {

namespace {

// Yield the CSV header line and then the rows selected by `limitAndOffset`
// from `result`, `rowsPerChunk` rows per yielded string. The header and the
// row selection are the same as in the V1 CSV export
// (`ExportQueryExecutionTrees::selectQueryResultToStream`).
STREAMABLE_GENERATOR_TYPE streamChunks(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
    size_t rowsPerChunk,
    SelectCsvStreamer::CancellationHandle cancellationHandle,
    [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  // In the CSV format, the variables don't include the question mark.
  std::vector<std::string> variables =
      selectClause.getSelectedVariablesAsStrings();
  ql::ranges::for_each(
      variables, [](std::string& variable) { variable = variable.substr(1); });
  STREAMABLE_YIELD(absl::StrJoin(variables, ","));
  STREAMABLE_YIELD('\n');

  const CsvChunkSink sink{
      qet.getQec()->getIndex(),
      qet.selectedVariablesToColumnIndices(selectClause, true)};
  std::string chunk;
  uint64_t resultSize = 0;
  for (const auto& [tableWithVocab, rows] :
       ExportQueryExecutionTrees::getRowIndices(limitAndOffset, *result,
                                                resultSize)) {
    if (rows.empty()) {
      continue;
    }
    const uint64_t rowsEnd = *rows.begin() + rows.size();
    for (uint64_t chunkBegin = *rows.begin(); chunkBegin < rowsEnd;
         chunkBegin += rowsPerChunk) {
      const uint64_t chunkEnd =
          std::min<uint64_t>(rowsEnd, chunkBegin + rowsPerChunk);
      chunk.clear();
      sink.appendRows(
          tableWithVocab.idTable(), tableWithVocab.localVocab(),
          ql::ranges::iota_view<uint64_t, uint64_t>{chunkBegin, chunkEnd},
          chunk);
      STREAMABLE_YIELD(chunk);
      cancellationHandle->throwIfCancelled();
    }
  }
  AD_LOG_DEBUG << "Done creating V2 CSV result, result size is " << resultSize
               << "." << std::endl;
  STREAMABLE_RETURN;
}

}  // namespace

// _____________________________________________________________________________
ExportQueryExecutionTrees::ComputeResultReturnType SelectCsvStreamer::run(
    const QueryExecutionTree& qet, const ParsedQuery& parsedQuery,
    size_t rowsPerChunk, CancellationHandle cancellationHandle,
    [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  AD_CONTRACT_CHECK(parsedQuery.hasSelectClause());
  AD_CONTRACT_CHECK(rowsPerChunk > 0);
  AD_CONTRACT_CHECK(cancellationHandle != nullptr);
  auto limitAndOffset = parsedQuery._limitOffset;
  ExportQueryExecutionTrees::compensateForLimitOffsetClause(limitAndOffset,
                                                            qet);

  // This call triggers the possibly expensive computation of the query
  // result unless the result is already cached (same as V1).
  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  auto csvChunks = streamChunks(
      qet, parsedQuery.selectClause(), limitAndOffset, std::move(result),
      rowsPerChunk, std::move(cancellationHandle), streamableYielder);

  // Same conversion as at the end of
  // `ExportQueryExecutionTrees::computeResult`.
  return [](auto range) -> cppcoro::generator<std::string> {
    for (auto&& csvChunk : range) {
      co_yield csvChunk;
    }
  }(ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
                            std::move(csvChunks)));
#else
  streamChunks(qet, parsedQuery.selectClause(), limitAndOffset,
               std::move(result), rowsPerChunk, std::move(cancellationHandle),
               streamableYielder);
#endif
}

}  // namespace ql::engine::export_v2
