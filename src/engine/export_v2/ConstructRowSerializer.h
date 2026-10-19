// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "engine/VariableToColumnMap.h"
#include "engine/export_v2/ScatterGatherArenaStreamer.h"
#include "engine/idTable/IdTable.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "parser/data/Types.h"
#include "util/http/MediaTypes.h"

namespace ql::engine::export_v2 {

using qlever::export_v2::ScatterGatherChunkBuilder;

// The vocabulary lookups of one window of rows, started ahead of serializing
// that window (see `export-v2-pipeline-lookups`). Destroying it without
// serializing the window waits for its reads.
class PreparedWindowBase {
 public:
  virtual ~PreparedWindowBase() = default;
  // Wait for the first phase of the lookups and start the second (see
  // `VocabLookupHandleBase::advance`).
  virtual void advance() = 0;
};

// Instantiates a CONSTRUCT template for a window of result rows and writes one
// Turtle or N-Triples line per instantiated triple, with the same bytes as
// Legacy (`ConstructTripleGenerator::generateFormattedTriples` with
// deduplication `none`):
// - rows in order, and for each row the template triples in template order;
// - a triple with an unbound (UNDEF) variable is skipped;
// - template triples that Legacy drops at preprocessing (a literal as subject
//   or predicate, a variable that the WHERE clause does not bind) never
//   appear;
// - a blank node `_:x` becomes `_:u<rowId>_x` (`_:g...` if generated), where
//   `rowId` is the OFFSET plus the rows that earlier blocks exported plus the
//   row index in its block;
// - literals in object position are escaped with
//   `RdfEscaping::validRDFLiteralFromNormalized`;
// - N-Triples writes every typed value with its datatype, Turtle writes
//   integers, decimals and `true`/`false` in short form.
//
// Vocabulary words are read with one `lookupBatch` per column and window and
// written straight from the decoded batch, without a per-term string.
//
// The object is immutable after construction, so one instance serves all
// morsel tasks of a request concurrently.
class ConstructRowSerializer {
 public:
  // True for the media types this serializer writes: Turtle and N-Triples.
  [[nodiscard]] static bool supportsMediaType(
      ad_utility::MediaType mediaType) noexcept;

  // `rowOffset` is the OFFSET that is still applied to the WHERE result (it
  // is part of every blank-node label). Requires
  // `supportsMediaType(mediaType)`.
  ConstructRowSerializer(
      const ad_utility::sparql_types::Triples& templateTriples,
      const VariableToColumnMap& variableColumns, const Index& index,
      ad_utility::MediaType mediaType, uint64_t rowOffset);

  // Append the triples of the rows `[rowBegin, rowEnd)` of `idTable` to
  // `builder`. `rowsExportedBeforeBlock` is the number of rows that the blocks
  // before `idTable` exported (see `ExportMorselSegment`).
  // `prepared`, if not null, must come from `prepareRows` with the same
  // `idTable`, `index`, `rowBegin` and `rowEnd`; its lookups are then
  // completed instead of looking the words up here. The bytes are the same.
  void appendRows(const IdTableView<0>& idTable, const LocalVocab& localVocab,
                  const Index& index, uint64_t rowBegin, uint64_t rowEnd,
                  uint64_t rowsExportedBeforeBlock,
                  ScatterGatherChunkBuilder& builder,
                  PreparedWindowBase* prepared = nullptr) const;

  // Start the vocabulary lookups of the rows `[rowBegin, rowEnd)` of
  // `idTable` (one split-phase lookup per template variable), for a later
  // `appendRows` of the same rows. Returns null if there is nothing to look
  // up.
  std::unique_ptr<PreparedWindowBase> prepareRows(const IdTableView<0>& idTable,
                                                  const Index& index,
                                                  uint64_t rowBegin,
                                                  uint64_t rowEnd) const;

 private:
  // One position of a template triple.
  struct Term {
    enum class Kind { Constant, Variable, BlankNode };
    Kind kind_;
    // `Constant`: the final text (already escaped in object position).
    // `BlankNode`: the label prefix (`_:u` or `_:g`).
    std::string text_;
    // `BlankNode`: the label suffix (`_` + label).
    std::string suffix_;
    // `Variable`: the index into `columns_`.
    size_t slot_ = 0;
  };
  using Triple = std::array<Term, 3>;

  std::vector<Triple> triples_;
  // The result columns of the template's variables, by slot.
  std::vector<ColumnIndex> columns_;
  // How often each slot occurs in the template (for the size estimate).
  std::vector<size_t> slotUses_;
  bool includeDataType_;
  // Like Legacy (`ConstructTripleGenerator::generateFormattedTriples`): Turtle
  // with `use-fast-export-stream-formatter` (the default) escapes every
  // literal, also in subject and predicate position; N-Triples, and Turtle
  // without the flag, escape the object only (`formatTriple`).
  bool escapeAllPositions_;
  uint64_t rowOffset_;
};

}  // namespace ql::engine::export_v2
