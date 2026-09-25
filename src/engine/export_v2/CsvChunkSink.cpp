// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/export_v2/CsvChunkSink.h"

#include <utility>

#include "index/ExportIds.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"

namespace ql::engine::export_v2 {

// _____________________________________________________________________________
CsvChunkSink::CsvChunkSink(
    const Index& index,
    QueryExecutionTree::ColumnIndicesAndTypes selectedColumns)
    : index_{index}, selectedColumns_{std::move(selectedColumns)} {}

// _____________________________________________________________________________
void CsvChunkSink::appendRows(const IdTableView<0>& table,
                              const LocalVocab& vocab,
                              ql::ranges::iota_view<uint64_t, uint64_t> rows,
                              std::string& out) const {
  for (const auto& column : selectedColumns_) {
    AD_CONTRACT_CHECK(!column.has_value() ||
                      column.value().columnIndex_ < table.numColumns());
  }
  for (uint64_t row : rows) {
    bool isFirstColumn = true;
    for (const auto& column : selectedColumns_) {
      if (!isFirstColumn) {
        out += ',';
      }
      isFirstColumn = false;
      if (!column.has_value()) {
        continue;
      }
      auto cell = ql::exportIds::idToStringAndType<true>(
          index_, table(row, column.value().columnIndex_), vocab,
          RdfEscaping::escapeForCsv);
      if (cell.has_value()) [[likely]] {
        out += cell.value().first;
      }
    }
    out += '\n';
  }
}

}  // namespace ql::engine::export_v2
