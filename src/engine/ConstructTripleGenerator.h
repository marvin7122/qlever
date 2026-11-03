// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include <gtest/gtest_prod.h>

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

namespace qlever::constructExport {

namespace detail {
struct ChunkFormatter;
}

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

// The formatted triples of one chunk of rows of a parallel export (see
// `generateFormattedChunksInParallel`). The memory is allocated with the
// allocator of the query, so it counts towards the `memory-for-queries` budget
// that all queries share. An allocation that does not fit first evicts
// unpinned entries of the query cache and then fails the export.
using FormattedChunk = std::basic_string<char, std::char_traits<char>,
                                         ad_utility::AllocatorWithLimit<char>>;

// How long the tables of the `rowIndices` of a parallel export stay valid:
// until the export is done (the tables of a fully materialized result, which
// the export keeps alive), or only until the next table is requested (the
// blocks of a lazy result).
enum class TableLifetime { WholeExport, UntilNextTable };

// Generates triples from the CONSTRUCT query results by instantiating the
// template triple patterns with the values from the result table produced by
// the WHERE clause of the CONSTRUCT query.
class ConstructTripleGenerator {
  friend class ConstructTripleGeneratorTest;
  // For `makeIdCache`.
  friend struct detail::ChunkFormatter;

 public:
  // the number of `IdTable` rows that one batch consists of.
  static constexpr size_t BATCH_SIZE = 1024;
  // the number of entries in the `IdCache` for each variable in the construct
  // clause template.
  static constexpr size_t CACHE_ENTRIES_PER_VARIABLE = 2048;
  // The maximal number of rows of a chunk of a parallel export (see
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

  // Like `generateFormattedTriples`, but the rows of `rowIndices` are cut into
  // chunks of `MAX_NUMBER_OF_ROWS_PER_CHUNK` rows (the last one may be
  // smaller), across the boundaries of the tables, and the triples of these
  // chunks are instantiated and formatted concurrently on the executor of
  // `config.qec_`. The result has one element per chunk, in the order of the
  // rows, and their concatenation is the same as that of the elements of
  // `generateFormattedTriples`. If reading `rowIndices` throws, the chunks
  // before the exception are yielded first. With `TableLifetime::WholeExport`,
  // the chunks refer to the rows of the tables; otherwise, the columns of the
  // tables that the template uses are copied.
  //
  // Memory: at most `maxNumberOfChunksInFlight` chunks are submitted but not
  // yet yielded (each with its rows and its formatted triples), plus the rows
  // of the next chunk, which are read (and, for a lazy result, computed) one
  // chunk ahead, plus the chunk that is being yielded. Destroying the result
  // stops the chunks in flight and waits for them. This must not be consumed
  // from a thread of the executor (see `TaskQueueOnExecutor`).
  //
  // Precondition: `config.mode_` is `DeduplicationMode::None` (the
  // deduplication depends on the order in which the triples are seen).
  static InputRangeTypeErased<FormattedChunk> generateFormattedChunksInParallel(
      const Triples& templateTriples,
      const VariableToColumnMap& variableColumns,
      InputRangeTypeErased<TableWithRange> rowIndices,
      TableLifetime tableLifetime, size_t rowOffset,
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

  FRIEND_TEST(MakeIdCache, emptyTemplate);
  FRIEND_TEST(MakeIdCache, singleVariable);
  FRIEND_TEST(MakeIdCache, multipleVariables);
  FRIEND_TEST(ConstructTripleGeneratorTest, rowOffsetAccumulatesAcrossTables);
  FRIEND_TEST(ConstructTripleGeneratorTest,
              blankNodeRowIdsWithOffsetInsideTable);
  FRIEND_TEST(ConstructTripleGeneratorTest, cannotCancelDuringBatch);
  FRIEND_TEST(ConstructTripleGeneratorTest, cancellationThrowsBetweenBatches);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
