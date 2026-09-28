// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <string>

#include "engine/QueryExecutionTree.h"
#include "engine/export_v2/ElasticExportScheduler.h"
#include "engine/export_v2/ExportEngineV2Serialize.h"
#include "parser/ParsedQuery.h"
#include "util/CancellationHandle.h"
#include "util/Generator.h"
#include "util/http/MediaTypes.h"

namespace ql::engine::export_v2 {

// _____________________________________________________________________________
// Unified Export Engine V2 (WP8): Coordinates push-based streaming export
// by wiring VectorStreamSource, SIMD escaping, ScatterGather chunk builders,
// and ElasticExportScheduler into a single pipeline.
class ExportEngineV2 {
 public:
  // Serialize a single tabular block into a ScatterGatherChunk.
  static ScatterGatherChunk serializeTableChunk(
      const IdTable& idTable, const LocalVocab& localVocab, RowFormat format,
      ScatterGatherChunkBuilder& builder) {
    return ql::engine::export_v2::serializeTableChunk(idTable, localVocab,
                                                      format, builder);
  }

  // Compute streamed query export results using the push-driven V2 pipeline.
  static cppcoro::generator<std::string> computeResult(
      const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
      ad_utility::MediaType mediaType,
      ad_utility::SharedCancellationHandle cancellationHandle,
      ad_utility::export_v2::ElasticExportScheduler* scheduler = nullptr);
};

}  // namespace ql::engine::export_v2
