// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_V2_CSVCHUNKSINK_H
#define QLEVER_SRC_ENGINE_EXPORT_V2_CSVCHUNKSINK_H

#include <cstdint>
#include <string>

#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "engine/idTable/IdTable.h"
#include "index/Index.h"
#include "index/LocalVocab.h"

namespace ql::engine::export_v2 {

// Serializer for rows of a SELECT result to CSV. The cell conversion is the
// one of the V1 CSV export (`idToStringAndType` with `escapeForCsv`), so both
// produce identical bytes.
class CsvChunkSink {
 private:
  const Index& index_;
  QueryExecutionTree::ColumnIndicesAndTypes selectedColumns_;

 public:
  // `index` must outlive the sink. Every column index in `selectedColumns`
  // must be valid for all tables passed to `appendRows`.
  CsvChunkSink(const Index& index,
               QueryExecutionTree::ColumnIndicesAndTypes selectedColumns);

  // Append the CSV rendering of the rows `rows` of `table`, whose local
  // vocabulary entries are resolved against `vocab`, to `out`. Each row ends
  // with a newline; unbound and unselectable cells are empty.
  void appendRows(const IdTableView<0>& table, const LocalVocab& vocab,
                  ql::ranges::iota_view<uint64_t, uint64_t> rows,
                  std::string& out) const;
};

}  // namespace ql::engine::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_CSVCHUNKSINK_H
