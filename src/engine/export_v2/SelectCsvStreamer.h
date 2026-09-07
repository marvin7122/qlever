// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_V2_SELECTCSVSTREAMER_H
#define QLEVER_SRC_ENGINE_EXPORT_V2_SELECTCSVSTREAMER_H

#include "engine/ExportQueryExecutionTrees.h"
#include "parser/ParsedQuery.h"
#include "util/stream_generator.h"

namespace ql::engine::export_v2 {

// First real V2 execution path: stream a SELECT result as CSV through
// `VectorStreamSource` rechunking and the `CsvChunkSink` serializer. The
// result computation, limit/offset handling, and cell conversion are shared
// with (not duplicated from) the V1 pipeline, so V1/V2 bytes agree by
// construction. Later work packages replace the sink and the block sourcing
// without touching the call site.
class SelectCsvStreamer {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;

  // The query must be eligible for V2 (see `ExportPipelineRouter`) and use
  // the CSV media type; this is checked by the caller.
  static ExportQueryExecutionTrees::ComputeResultReturnType run(
      const QueryExecutionTree& qet, const ParsedQuery& parsedQuery,
      CancellationHandle cancellationHandle,
      [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder);
};

}  // namespace ql::engine::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_SELECTCSVSTREAMER_H
