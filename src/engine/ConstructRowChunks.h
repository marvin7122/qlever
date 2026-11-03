// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTROWCHUNKS_H
#define QLEVER_SRC_ENGINE_CONSTRUCTROWCHUNKS_H

#include <exception>
#include <functional>
#include <memory>
#include <variant>
#include <vector>

#include "engine/ConstructTripleGenerator.h"
#include "engine/ConstructTypes.h"
#include "engine/QueryExportTypes.h"
#include "util/AllocatorWithLimit.h"
#include "util/Iterators.h"
#include "util/http/MediaTypes.h"
#include "util/views/AsyncTransformStopFlag.h"

// The building blocks of `ConstructTripleGenerator::
// generateFormattedChunksInParallel`. They are only in a header of their own so
// that they can be tested; they are defined in `ConstructTripleGenerator.cpp`.
namespace qlever::constructExport::detail {

// Rows of one table that are copied, so that they stay valid when the export
// has moved on to later tables.
struct CopiedRows {
  // Only the columns that the template uses (see `CopyColumns`).
  IdTable idTable_;
  // A clone of the `LocalVocab` of the table, which keeps the entries that the
  // `idTable_` refers to alive. It is shared by all copies from that table.
  std::shared_ptr<const LocalVocab> localVocab_;
};

// Consecutive rows of one table: borrowed (a `TableWithRange` of a table that
// stays valid during the whole export) or copied.
struct ChunkPart {
  std::variant<TableWithRange, CopiedRows> rows_;
  // The blank nodes of the first row get this row id, those of the following
  // rows consecutive ids.
  size_t blankNodeRowIdOfFirstRow_;
};

// The unit of work of a parallel export: consecutive rows of the result, which
// may come from several tables. It is never empty.
struct RowChunk {
  std::vector<ChunkPart> parts_;
};

// An exception that was thrown while the rows were read. It is passed on in the
// order of the rows, after the chunks before it.
struct InputError {
  std::exception_ptr exception_;
};

// An element of `makeRowChunks`.
using RowChunkOrInputError = std::variant<RowChunk, InputError>;

// How a chunk holds its rows: it borrows them (`TableLifetime::WholeExport`),
// or it copies the `columns_` of the tables into the columns 0, 1, ... of a
// `CopiedRows` (`TableLifetime::UntilNextTable`).
struct BorrowRows {};
struct CopyColumns {
  std::vector<ColumnIndex> columns_;
};
using RowStorage = std::variant<BorrowRows, CopyColumns>;

// The parameters of `makeRowChunks`.
struct RowChunkingConfig {
  // The blank node row id of the first row of the first table (the OFFSET).
  size_t blankNodeRowIdOfFirstRow_;
  size_t maxNumberOfRowsPerChunk_;
  RowStorage storage_;
  ad_utility::AllocatorWithLimit<Id> allocator_;
};

// Cut the rows of the `tables` into chunks of `maxNumberOfRowsPerChunk_` rows
// (only the last chunk may have fewer), with the blank node row ids of
// `ConstructTripleGenerator::evaluateTables`. A table is only requested when
// its rows are needed for a chunk. If requesting a table (or copying its rows)
// throws, the rows before are returned as a last chunk, followed by the
// `InputError`, and then the range ends.
ad_utility::InputRangeTypeErased<RowChunkOrInputError> makeRowChunks(
    ad_utility::InputRangeTypeErased<TableWithRange> tables,
    RowChunkingConfig config);

// Instantiates and formats the triples of a chunk. This is the transformation
// of the `AsyncTransformView` of a parallel export, so it only reads its
// members and may be called concurrently for different chunks.
struct ChunkFormatter {
  // Preprocessed for the columns of the chunks (see `CopyColumns`).
  std::shared_ptr<const PreprocessedConstructTemplate> preprocessedTemplate_;
  ad_utility::MediaType mediaType_;
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;
  ad_utility::AllocatorWithLimit<char> allocator_;

  // Return the formatted triples of the `chunk`. Throw a
  // `CancellationException` if the `cancellationHandle_` is cancelled. Stop
  // early and return the triples so far as soon as `stop` is requested (the
  // export then no longer needs them).
  FormattedChunk format(const RowChunk& chunk,
                        ad_utility::AsyncTransformStopFlag stop) const;

  // Format a `RowChunk`, and rethrow an `InputError`.
  FormattedChunk operator()(RowChunkOrInputError chunkOrError,
                            ad_utility::AsyncTransformStopFlag stop) const;
};

}  // namespace qlever::constructExport::detail

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTROWCHUNKS_H
