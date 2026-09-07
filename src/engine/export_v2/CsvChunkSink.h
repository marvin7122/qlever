// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_V2_CSVCHUNKSINK_H
#define QLEVER_SRC_ENGINE_EXPORT_V2_CSVCHUNKSINK_H

#include <string>

#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
#include "index/ExportIds.h"
#include "index/LocalVocab.h"
#include "rdfTypes/RdfEscaping.h"

namespace ql::engine::export_v2 {

// Serialize one rechunked result block to CSV. The cell conversion is
// exactly the V1 conversion (`idToStringAndType` with `escapeForCsv`), so
// V1 and V2 bytes agree by construction. Later work packages replace this
// sink with vectorized serializers without touching the call site.
class CsvChunkSink {
 public:
  CsvChunkSink(const IndexImpl& index,
               const QueryExecutionTree::ColumnIndicesAndTypes& selectedColumns)
      : index_{index}, selectedColumns_{selectedColumns} {}

  // Append the CSV rendering of every row in `table` (resolved against
  // `vocab`) to `out`.
  void appendChunk(const IdTable& table, const LocalVocab& vocab,
                   std::string& out) const {
    for (size_t i = 0; i < table.numRows(); ++i) {
      for (size_t j = 0; j < selectedColumns_.size(); ++j) {
        if (selectedColumns_[j].has_value()) {
          const auto& column = selectedColumns_[j].value();
          Id id = table(i, column.columnIndex_);
          auto cell = ql::exportIds::idToStringAndType<true>(
              index_, id, vocab, RdfEscaping::escapeForCsv);
          if (cell.has_value()) [[likely]] {
            out += cell.value().first;
          }
        }
        if (j + 1 < selectedColumns_.size()) {
          out += ',';
        }
      }
      out += '\n';
    }
  }

 private:
  std::reference_wrapper<const IndexImpl> index_;
  std::reference_wrapper<const QueryExecutionTree::ColumnIndicesAndTypes>
      selectedColumns_;
};

}  // namespace ql::engine::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_CSVCHUNKSINK_H
