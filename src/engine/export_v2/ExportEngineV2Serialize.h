// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <string>
#include <string_view>

#include "engine/export_v2/MonomorphicSerializers.h"
#include "engine/export_v2/ScatterGatherArenaStreamer.h"
#include "engine/export_v2/SimdEscapeClassifier.h"
#include "engine/idTable/IdTable.h"
#include "index/LocalVocab.h"
#include "util/Exception.h"

namespace ql::engine::export_v2 {

using qlever::export_v2::ScatterGatherChunk;
using qlever::export_v2::ScatterGatherChunkBuilder;

// Lightweight tabular chunk serialization used by ExportEngineV2 and by
// ExportEngineV2Test. Kept free of QueryExecutionTree / index TUs so cluster
// GCC 11 builds can compile the unit test without IndexImpl/range-v3. Only
// index-free `Id` datatypes can therefore be resolved here; index-backed IDs
// fail loudly instead of being silently replaced by placeholder text (the
// full `Id` -> string conversion lives in `index/ExportIds.h`). Consumes
// `builder`: it is finalized into the returned chunk and left empty.
inline ScatterGatherChunk serializeTableChunk(
    const IdTable& idTable, const LocalVocab& localVocab, RowFormat format,
    ScatterGatherChunkBuilder& builder) {
  const size_t numRows = idTable.numRows();
  const size_t numCols = idTable.numColumns();

  // Escaping at most doubles the input, plus two CSV quotes. The buffer is
  // reused across cells, so it only grows to the longest escaped entry.
  std::string buf;
  auto appendEscaped = [&builder, &buf, format](std::string_view raw) {
    if (buf.size() < 2 * raw.size() + 2) {
      buf.resize(2 * raw.size() + 2);
    }
    if (format == RowFormat::Csv) {
      auto escaped = SimdEscapeClassifier::copyAndEscape<EscapeFormat::Csv>(
          raw, {buf.data(), buf.size()});
      builder.appendCopy(std::string_view(escaped.data(), escaped.size()));
    } else {
      auto escaped = SimdEscapeClassifier::copyAndEscape<EscapeFormat::Tsv>(
          raw, {buf.data(), buf.size()});
      builder.appendCopy(std::string_view(escaped.data(), escaped.size()));
    }
  };

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
      } else if (id.getDatatype() == Datatype::Bool) {
        builder.appendCopy(id.getBoolLiteral());
      } else if (id.getDatatype() == Datatype::Undefined) {
        // empty string for undef
      } else if (id.getDatatype() == Datatype::LocalVocabIndex) {
        // Same source as `ql::exportIds::getLiteralOrIriFromVocabIndex`: the
        // entry already is a `LiteralOrIri`, so its string representation is
        // the exported cell content.
        std::string_view raw = localVocab.getWord(id.getLocalVocabIndex())
                                   .toStringRepresentation();
        appendEscaped(raw);
      } else {
        AD_THROW(
            "ExportEngineV2 cannot serialize index-backed `Id` without the "
            "index vocabulary");
      }
    }
    builder.appendCopy("\n");
  }

  return std::move(builder).finalize();
}

}  // namespace ql::engine::export_v2
