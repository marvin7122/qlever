// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_V2_SELECTCSVSTREAMER_H
#define QLEVER_SRC_ENGINE_EXPORT_V2_SELECTCSVSTREAMER_H

#include <cstddef>

#include "engine/ExportQueryExecutionTrees.h"
#include "parser/ParsedQuery.h"
#include "util/stream_generator.h"

namespace ql::engine::export_v2 {

// V2 export of a SELECT result as CSV. The response bytes are identical to
// those of the V1 CSV export (`ExportQueryExecutionTrees::computeResult`); the
// difference is that V2 serializes `rowsPerChunk` rows at a time into one
// string instead of yielding every cell and separator separately.
class SelectCsvStreamer {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;

  static constexpr size_t defaultRowsPerChunk = 8192;

  // Compute the result of `qet` and return it as CSV, with the header line
  // derived from the SELECT clause of `parsedQuery` and the LIMIT and OFFSET
  // of `parsedQuery` applied. `parsedQuery` must be a SELECT query that
  // `ExportPipelineRouter` routes to V2, and `rowsPerChunk` must be positive.
  // As for `ExportQueryExecutionTrees::computeResult`, `qet` and `parsedQuery`
  // must outlive the returned generator. Cancellation is checked after every
  // chunk.
  static ExportQueryExecutionTrees::ComputeResultReturnType run(
      const QueryExecutionTree& qet, const ParsedQuery& parsedQuery,
      size_t rowsPerChunk, CancellationHandle cancellationHandle,
      STREAMABLE_YIELDER_ARG_DECL);
};

}  // namespace ql::engine::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_SELECTCSVSTREAMER_H
