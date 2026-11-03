// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include <gtest/gtest_prod.h>

#include <string>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTypes.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "engine/VariableToColumnMap.h"
#include "index/Index.h"
#include "parser/data/Types.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"
#include "util/ConstructDeduplicationMode.h"
#include "util/Iterators.h"
#include "util/http/MediaTypes.h"
#include "util/views/AsyncTransformStopFlag.h"

namespace qlever::constructExport {

using ad_utility::InputRangeTypeErased;
using CancellationHandle = ad_utility::SharedCancellationHandle;
using Triples = ad_utility::sparql_types::Triples;
using IdCache =
    ad_utility::util::LRUCacheWithStatistics<Id, std::optional<EvaluatedTerm>>;
using StringTriple = QueryExecutionTree::StringTriple;

// Bundles the pieces shared by `evaluateTables`, `generateStringTriples`, and
// `generateFormattedTriples`.
struct EvaluationConfig {
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;
  std::reference_wrapper<const QueryExecutionContext> qec_;
  ad_utility::DeduplicationMode mode_ = ad_utility::DeduplicationMode::none();
};

// Rows of one block of the result of the WHERE clause, copied, so that they
// stay valid when the export has moved on to later blocks.
struct CopiedRows {
  // All columns of the rows.
  IdTable idTable_;
  // A clone of the `LocalVocab` of the block, which keeps the entries that the
  // `idTable_` refers to alive.
  LocalVocab localVocab_;
  // The row id from which the blank nodes of the first row are generated (the
  // ids of the following rows are consecutive).
  size_t blankNodeRowIdOfFirstRow_;
};

// The unit of work of a parallel CONSTRUCT export: consecutive rows of the
// result, which may come from several blocks.
struct RowChunk {
  std::vector<CopiedRows> parts_;
};

// The formatted triples of one `RowChunk`. The memory is allocated with the
// allocator of the query, so it counts towards the `memory-for-queries` budget
// that all queries share. An allocation that does not fit first evicts
// unpinned entries of the query cache and then fails the export.
using FormattedChunk = std::basic_string<char, std::char_traits<char>,
                                         ad_utility::AllocatorWithLimit<char>>;

// Generates triples from the CONSTRUCT query results by instantiating the
// template triple patterns with the values from the result table produced by
// the WHERE clause of the CONSTRUCT query.
class ConstructTripleGenerator {
  friend class ConstructTripleGeneratorTest;

 public:
  // the number of `IdTable` rows that one batch consists of.
  static constexpr size_t BATCH_SIZE = 1024;
  // the number of entries in the `IdCache` for each variable in the construct
  // clause template.
  static constexpr size_t CACHE_ENTRIES_PER_VARIABLE = 2048;
  // The maximal number of rows of a `RowChunk` of a parallel export (see
  // `generateFormattedChunksInParallel`). A chunk has to be large enough that
  // scheduling a task costs little compared to its formatting. This value has
  // not been tuned yet.
  static constexpr size_t MAX_NUMBER_OF_ROWS_PER_CHUNK = 8 * BATCH_SIZE;

  // Instantiates `templateTriples` for each row in `rowIndices` and returns a
  // lazy range of triples serialized according to `mediaType`.
  // Duplicate triples are handled according to `config.mode_`.
  static InputRangeTypeErased<std::string> generateFormattedTriples(
      const Triples& templateTriples,
      const VariableToColumnMap& variableColumns,
      InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
      ad_utility::MediaType mediaType, const EvaluationConfig& config);

  // Like `generateFormattedTriples`, but the rows of `rowIndices` are copied
  // into chunks of `MAX_NUMBER_OF_ROWS_PER_CHUNK` rows (the last one may be
  // smaller), across the boundaries of the tables, and the triples of these
  // chunks are instantiated and formatted concurrently on the executor of
  // `config.qec_`. The result has one element per chunk, in the order of the
  // rows, and their concatenation is the same as that of the elements of
  // `generateFormattedTriples`. At most `maxNumberOfChunksInFlight` chunks
  // are in flight (copied, but not yet yielded). Destroying the result stops
  // the chunks in flight and waits for them. This must not be consumed from a
  // thread of the executor (see `TaskQueueOnExecutor`).
  //
  // Precondition: `config.mode_` is `DeduplicationMode::None` (the
  // deduplication depends on the order in which the triples are seen).
  static InputRangeTypeErased<FormattedChunk> generateFormattedChunksInParallel(
      const Triples& templateTriples,
      const VariableToColumnMap& variableColumns,
      InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
      ad_utility::MediaType mediaType, const EvaluationConfig& config,
      size_t maxNumberOfChunksInFlight);

  // Instantiates `templateTriples` for each row in `rowIndices` and returns a
  // lazy range of `StringTriple`.
  // Duplicate triples are handled according to `config.mode_`.
  static InputRangeTypeErased<StringTriple> generateStringTriples(
      const Triples& templateTriples,
      const VariableToColumnMap& variableColumns,
      InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
      const EvaluationConfig& config);

 private:
  // Returns an `IdCache` sized for `tmpl` (minimum one slot to handle
  // blank-node-only templates).
  static IdCache makeIdCache(const PreprocessedConstructTemplate& tmpl);

  // Lazily evaluates all `TableWithRange` values from `rowIndices`, processes
  // them in batches of `BATCH_SIZE` rows, and returns a flat range of
  // `EvaluatedTriple`. Duplicate triples are handled according to
  // `config.mode_`. The blank nodes of the `i`-th row of `rowIndices` (counted
  // over all tables, from 0) get the row id `rowOffset + i`.
  static InputRangeTypeErased<EvaluatedTriple> evaluateTables(
      const Triples& templateTriples,
      const VariableToColumnMap& variableColumns,
      ad_utility::InputRangeTypeErased<TableWithRange> rowIndices,
      size_t rowOffset, const EvaluationConfig& config);

  // Copy the rows of `rowIndices` into chunks of `maxNumberOfRowsPerChunk`
  // rows (only the last chunk may have fewer), with the blank node row ids of
  // `evaluateTables` (which start at `rowOffset`). A table is only requested
  // from `rowIndices` when its rows are needed for a chunk.
  static InputRangeTypeErased<RowChunk> makeRowChunks(
      InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
      size_t maxNumberOfRowsPerChunk,
      ad_utility::AllocatorWithLimit<Id> allocator);

  // Instantiate the `preprocessedTemplate` for all rows of the `chunk` and
  // return the triples formatted as `mediaType`, allocated with the
  // `allocator`. This is the unit of work of
  // `generateFormattedChunksInParallel`, so it only reads its arguments and
  // may be called concurrently for different chunks. Throw a
  // `CancellationException` if the `cancellationHandle` is cancelled. Stop
  // early and return the triples so far as soon as `stop` is requested (the
  // export then no longer needs them).
  static FormattedChunk formatRowChunk(
      const PreprocessedConstructTemplate& preprocessedTemplate,
      const RowChunk& chunk, ad_utility::MediaType mediaType,
      const Index& index, const CancellationHandle& cancellationHandle,
      const ad_utility::AllocatorWithLimit<char>& allocator,
      ad_utility::AsyncTransformStopFlag stop);

  FRIEND_TEST(MakeIdCache, emptyTemplate);
  FRIEND_TEST(MakeIdCache, singleVariable);
  FRIEND_TEST(MakeIdCache, multipleVariables);
  FRIEND_TEST(ConstructTripleGeneratorTest, rowOffsetAccumulatesAcrossTables);
  FRIEND_TEST(ConstructTripleGeneratorTest,
              blankNodeRowIdsWithOffsetInsideTable);
  FRIEND_TEST(ConstructTripleGeneratorTest, cannotCancelDuringBatch);
  FRIEND_TEST(ConstructTripleGeneratorTest, cancellationThrowsBetweenBatches);
  FRIEND_TEST(ConstructTripleGeneratorTest, makeRowChunks);
  FRIEND_TEST(ConstructTripleGeneratorTest, formatRowChunk);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
