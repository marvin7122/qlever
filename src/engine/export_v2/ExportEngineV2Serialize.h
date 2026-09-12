// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <array>
#include <string>
#include <string_view>

#include "engine/export_v2/MonomorphicSerializers.h"
#include "engine/export_v2/ScatterGatherArenaStreamer.h"
#include "engine/export_v2/SimdEscapeClassifier.h"
#include "engine/idTable/IdTable.h"
#include "index/LocalVocab.h"

namespace ql::engine::export_v2 {

using qlever::export_v2::ScatterGatherChunk;
using qlever::export_v2::ScatterGatherChunkBuilder;

// Lightweight tabular chunk serialization used by ExportEngineV2 and by
// ExportEngineV2Test. Kept free of QueryExecutionTree / index TUs so cluster
// GCC 11 builds can compile the unit test without IndexImpl/range-v3.
inline ScatterGatherChunk serializeTableChunk(
    const IdTable& idTable, [[maybe_unused]] const LocalVocab& localVocab,
    RowFormat format, ScatterGatherChunkBuilder& builder) {
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
        std::string raw = "<val>";
        std::array<char, 256> buf{};
        if (format == RowFormat::Csv) {
          auto escaped = SimdEscapeClassifier::copyAndEscape<EscapeFormat::Csv>(
              raw, {buf.data(), buf.size()});
          builder.appendCopy(std::string_view(escaped.data(), escaped.size()));
        } else {
          auto escaped = SimdEscapeClassifier::copyAndEscape<EscapeFormat::Tsv>(
              raw, {buf.data(), buf.size()});
          builder.appendCopy(std::string_view(escaped.data(), escaped.size()));
        }
      }
    }
    builder.appendCopy("\n");
  }

  return std::move(builder).finalize();
}

}  // namespace ql::engine::export_v2
