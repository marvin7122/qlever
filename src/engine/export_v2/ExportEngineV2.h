// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <limits>
#include <optional>
#include <string>

#include "backports/span.h"
#include "engine/ExportPipelineRouter.h"
#include "engine/QueryExecutionTree.h"
#include "engine/export_v2/AsyncChunkPipeline.h"
#include "engine/export_v2/ElasticExportScheduler.h"
#include "engine/export_v2/ExportEngineV2Serialize.h"
#include "engine/export_v2/VectorStreamSource.h"
#include "index/Index.h"
#include "parser/ParsedQuery.h"
#include "util/CancellationHandle.h"
#include "util/http/MediaTypes.h"
#include "util/stream_generator.h"

namespace ql::engine::export_v2 {

using qlever::export_v2::AsyncChunkPipeline;

// _____________________________________________________________________________
// Unified Export Engine V2 (WP8): Coordinates push-based streaming export
// by wiring VectorStreamSource, SIMD escaping, ScatterGather chunk builders,
// and ElasticExportScheduler into a single pipeline.
class ExportEngineV2 {
 public:
  // True when this engine can serve `mediaType` for `parsedQuery` without
  // falling back to Legacy V1. Currently: SELECT + CSV/TSV (LIMIT/OFFSET
  // included). CONSTRUCT and other media types still use Legacy.
  [[nodiscard]] static bool canHandle(const ParsedQuery& parsedQuery,
                                      ad_utility::MediaType mediaType) noexcept;

  // Same as above, but additionally requires the planned operation tree to
  // contain only scans, joins, filters, BIND, and inline VALUES (the serving
  // path, where the query execution tree is available). Anything else falls
  // back to Legacy V1, including FILTER EXISTS (which plans an `ExistsJoin`).
  [[nodiscard]] static bool canHandle(const ParsedQuery& parsedQuery,
                                      const QueryExecutionTree& qet,
                                      ad_utility::MediaType mediaType) noexcept;

  // Map CSV/TSV media types onto the V2 row format. Returns nullopt otherwise.
  [[nodiscard]] static std::optional<RowFormat> rowFormatFor(
      ad_utility::MediaType mediaType) noexcept;

  // Integer-only / unit-test serialize path (header-only, no Index TU).
  static ScatterGatherChunk serializeTableChunk(
      const IdTable& idTable, const LocalVocab& localVocab, RowFormat format,
      ScatterGatherChunkBuilder& builder) {
    return ql::engine::export_v2::serializeTableChunk(idTable, localVocab,
                                                      format, builder);
  }

  // Live-path serialize with vocabulary resolution and selected columns.
  // `selectedColumns` empty means all IdTable columns; nullopt entry = unbound.
  // `[rowBegin, rowEnd)` selects a half-open row range (default: all rows).
  static void appendSerializedRows(
      const IdTableView<0>& idTable, const LocalVocab& localVocab,
      RowFormat format, ScatterGatherChunkBuilder& builder, const Index& index,
      ql::span<const std::optional<ColumnIndex>> selectedColumns,
      uint64_t rowBegin = 0,
      uint64_t rowEnd = std::numeric_limits<uint64_t>::max());

  static ScatterGatherChunk serializeTableChunk(
      const IdTableView<0>& idTable, const LocalVocab& localVocab,
      RowFormat format, ScatterGatherChunkBuilder& builder, const Index& index,
      ql::span<const std::optional<ColumnIndex>> selectedColumns,
      uint64_t rowBegin = 0,
      uint64_t rowEnd = std::numeric_limits<uint64_t>::max());

  static ScatterGatherChunk serializeTableChunk(
      const IdTable& idTable, const LocalVocab& localVocab, RowFormat format,
      ScatterGatherChunkBuilder& builder, const Index& index,
      ql::span<const std::optional<ColumnIndex>> selectedColumns,
      uint64_t rowBegin = 0,
      uint64_t rowEnd = std::numeric_limits<uint64_t>::max());

  // Compute streamed query export results using the push-driven V2 pipeline
  // for eligible SELECT CSV/TSV requests; otherwise delegates to Legacy V1.
  // Default HTTP path: one `std::string` per morsel (`export-send=string`).
  static cppcoro::generator<std::string> computeResult(
      const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
      ad_utility::MediaType mediaType,
      ad_utility::SharedCancellationHandle cancellationHandle,
      ad_utility::export_v2::ElasticExportScheduler* scheduler = nullptr);

  // Same serialize as `computeResult`, but each morsel is a ScatterGatherChunk
  // for `export-send=iovec`. Requires `canHandle`; does not fall back to
  // Legacy.
  static cppcoro::generator<ScatterGatherChunk> computeResultChunks(
      const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
      ad_utility::MediaType mediaType,
      ad_utility::SharedCancellationHandle cancellationHandle,
      ad_utility::export_v2::ElasticExportScheduler* scheduler = nullptr);
};

}  // namespace ql::engine::export_v2
