// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "engine/export_v2/ExportEngineV2.h"

#include <chrono>
#include <iostream>
#include <string>

#include "engine/ExportQueryExecutionTrees.h"
#include "util/Exception.h"

namespace ql::engine::export_v2 {

// _____________________________________________________________________________
ScatterGatherChunk ExportEngineV2::serializeTableChunk(
    const IdTable& idTable,
    [[maybe_unused]] const LocalVocab& localVocab,
    RowFormat format,
    ScatterGatherChunkBuilder& builder) {
  const size_t numRows = idTable.numRows();
  const size_t numCols = idTable.numColumns();

  for (size_t row = 0; row < numRows; ++row) {
    for (size_t col = 0; col < numCols; ++col) {
      if (col > 0) {
        builder.appendCopy(format == RowFormat::Csv ? "," : "\t");
      }
      Id id = idTable(row, col);
      if (id.getDatatype() == Datatype::Int) {
        builder.appendCopy(std::to_string(id.getInt()));
      } else if (id.getDatatype() == Datatype::Double) {
        builder.appendCopy(std::to_string(id.getDouble()));
      } else if (id.getDatatype() == Datatype::Undefined) {
        // empty string for undef
      } else {
        // Fallback literal representation
        std::string raw = "<val>";
        if (format == RowFormat::Csv) {
          auto escaped = SimdEscapeClassifier::classifyAndEscape<RowFormat::Csv>(raw);
          builder.appendCopy(escaped);
        } else {
          auto escaped = SimdEscapeClassifier::classifyAndEscape<RowFormat::Tsv>(raw);
          builder.appendCopy(escaped);
        }
      }
    }
    builder.appendCopy("\n");
  }

  return std::move(builder).finalize();
}

// _____________________________________________________________________________
cppcoro::generator<std::string> ExportEngineV2::computeResult(
    const ParsedQuery& parsedQuery,
    const QueryExecutionTree& qet,
    ad_utility::MediaType mediaType,
    ad_utility::SharedCancellationHandle cancellationHandle,
    [[maybe_unused]] ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  // Delegate through ExportPipelineRouter
  return ExportPipelineRouter::computeResult(parsedQuery, qet, mediaType,
                                             cancellationHandle);
}

}  // namespace ql::engine::export_v2
