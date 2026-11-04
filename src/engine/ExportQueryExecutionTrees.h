// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORTQUERYEXECUTIONTREES_H
#define QLEVER_SRC_ENGINE_EXPORTQUERYEXECUTIONTREES_H

#include <string>

#include "engine/ConstructTripleGenerator.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"
#include "util/http/MediaTypes.h"
#include "util/stream_generator.h"
#include "util/views/AsyncTransformStopFlag.h"

// Class for computing the result of an already parsed and planned query and
// exporting it in different formats (TSV, CSV, Turtle, JSON, Binary).
//
// TODO<joka921> Also implement a streaming JSON serializer to reduce the RAM
// consumption of large JSON exports and to make this interface even simpler.
class ExportQueryExecutionTrees {
 public:
  using enum ad_utility::MediaType;
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  using Literal = ad_utility::triple_component::Literal;

  // The media types QLever can serialize for each SPARQL query form. These are
  // the per-query-form runtime contract.
  // `qleverJson` is supported for every query form (it is special-cased in
  // `computeResult` via `computeResultAsQLeverJSON`), so it appears in all
  // three.
  static constexpr std::array supportedMediaTypesForConstructQueries{
      turtle, csv, tsv, ntriples, qleverJson};
  static constexpr std::array supportedMediaTypesForAskQueries{
      qleverJson, sparqlJson, sparqlXml};
  // NOTE: The binary format is not supported in the reduced feature mode for
  // C++17, such that clients fall back to a different format.
  static constexpr std::array supportedMediaTypesForSelectQueries{
      octetStream,        csv, tsv, sparqlXml, sparqlJson, qleverJson,
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
      binaryQleverExport,
#endif
  };
  // The media types the result-serialization templates may be instantiated
  // with.
  static constexpr std::array staticallySupportedMediaTypes{
      csv,        tsv,        octetStream,       turtle, ntriples, sparqlXml,
      sparqlJson, qleverJson, binaryQleverExport};

  // The maximal number of rows in one chunk of a parallel CSV or TSV export
  // (see the runtime parameter `parallel-csv-tsv-export`). A chunk is the unit
  // of work of the parallel export, so it has to be large enough that
  // scheduling a task costs little compared to its serialization. This value
  // has not been tuned yet.
  static constexpr uint64_t MAX_NUMBER_OF_ROWS_PER_CSV_OR_TSV_CHUNK = 8192;

  // The serialized lines (each terminated by a newline) of one chunk of a
  // parallel CSV or TSV export. The memory is allocated with the allocator of
  // the query, so it counts towards the `memory-for-queries` budget that all
  // queries share. An allocation that does not fit first evicts unpinned
  // entries of the query cache and then fails the export.
  using CsvOrTsvChunk = std::basic_string<char, std::char_traits<char>,
                                          ad_utility::AllocatorWithLimit<char>>;

  // Compute the result of the given `parsedQuery` (created by the
  // `SparqlParser`) for which the `QueryExecutionTree` has been previously
  // created by the `QueryPlanner`. The result is converted into a sequence of
  // bytes that represents the result of the computed query in the format
  // specified by the `mediaType`. Supported formats for this function are CSV,
  // TSV, Turtle, NTriples, Binary, SparqlJSON, QLeverJSON. Note that the Binary
  // format can only be used with SELECT queries and the Turtle and NTriples
  // formats can only be used with CONSTRUCT queries. Invalid `mediaType`s and
  // invalid combinations of `mediaType` and the query type will throw. The
  // result is returned as a `generator` that lazily computes the serialized
  // result in large chunks of bytes.
  using ComputeResultReturnType =
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
      cppcoro::generator<std::string>;
#else
      void;
#endif
  static ComputeResultReturnType computeResult(
      const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
      ad_utility::MediaType mediaType, const ad_utility::Timer& requestTimer,
      CancellationHandle cancellationHandle, STREAMABLE_YIELDER_ARG_DECL);

  // Convert a `stream_generator` to an "ordinary" `InputRange<string>` that
  // yields exactly the same chunks as the `stream_generator`. Exceptions that
  // happen during the creation of the first chunk (default chunk size is 1MB)
  // will be immediately thrown when calling this function. Exceptions that
  // happen later will be caught and their exception message will be  yielded by
  // the resulting `generator<string>` together with a message, that explains,
  // that there is no good mechanism for handling errors during a chunked HTTP
  // response transfer.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  static ad_utility::InputRangeTypeErased<std::string>
  convertStreamGeneratorForChunkedTransfer(
      STREAMABLE_GENERATOR_TYPE streamGenerator);
#endif

 private:
  // Make sure that the offset is not applied again when exporting the
  // result (it is already applied by the root operation in the query
  // execution tree). Note that we don't need this for the limit because
  // applying a fixed limit is idempotent. This only works because the query
  // planner does the exact same `handlesLimitOffset()` check.
  static void compensateForLimitOffsetClause(
      LimitOffsetClause& limitOffsetClause, const QueryExecutionTree& qet);

  // Generate the bindings of the result of a SELECT or CONSTRUCT query in the
  // `application/qlever-results+json` format.
  //
  // NOTE: This calls `selectQueryResultBindingsToQLeverJSON` or
  // `constructQueryResultBindingsToQLeverJSON` for the bindings and adds the
  // remaining (meta) fields needed for the `application/qlever-results+json`
  // format.
  static STREAMABLE_GENERATOR_TYPE computeResultAsQLeverJSON(
      const ParsedQuery& query, const QueryExecutionTree& qet,
      const LimitOffsetClause& limitOffset,
      const ad_utility::Timer& requestTimer,
      CancellationHandle cancellationHandle, STREAMABLE_YIELDER_ARG_DECL);

  // Generate the bindings of the result of a SELECT query in the
  // `application/ qlever+json` format.
  static ad_utility::InputRangeTypeErased<std::string>
  selectQueryResultBindingsToQLeverJSON(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> result, uint64_t& resultSize,
      CancellationHandle cancellationHandle);

  // Generate the bindings of the result of a CONSTRUCT query in the
  // `application/ qlever+json` format.
  static ad_utility::InputRangeTypeErased<std::string>
  constructQueryResultBindingsToQLeverJSON(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> result, uint64_t& resultSize,
      CancellationHandle cancellationHandle);

  // Helper function that generates the individual bindings for the
  // `application/ qlever+json` format.
  static auto idTableToQLeverJSONBindings(
      const QueryExecutionTree& qet, LimitOffsetClause limitAndOffset,
      const QueryExecutionTree::ColumnIndicesAndTypes columns,
      std::shared_ptr<const Result> result, uint64_t& resultSize,
      CancellationHandle cancellationHandle);

  // Return a `qlever::constructExport::EvaluationConfig` for
  // `ConstructTripleGenerator`, including
  // `RuntimeParameters::constructDeduplication_`.
  static qlever::constructExport::EvaluationConfig
  makeConstructEvaluationConfig(const QueryExecutionTree& qet,
                                CancellationHandle cancellationHandle);

  // Helper function that generates the result of a CONSTRUCT query as
  // `StringTriple`s.
  static auto constructQueryResultToStringTriples(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
      uint64_t& resultSize, CancellationHandle cancellationHandle);

  // Helper function that generates the result of a CONSTRUCT query as a
  // CSV or TSV stream.
  template <ad_utility::MediaType format>
  static STREAMABLE_GENERATOR_TYPE constructQueryResultToStream(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
      CancellationHandle cancellationHandle, STREAMABLE_YIELDER_ARG_DECL);

  // Generate the result of a SELECT query as a CSV or TSV or binary stream.
  template <ad_utility::MediaType format>
  static STREAMABLE_GENERATOR_TYPE selectQueryResultToStream(
      const QueryExecutionTree& qet,
      const parsedQuery::SelectClause& selectClause,
      LimitOffsetClause limitAndOffset, CancellationHandle cancellationHandle,
      const ad_utility::Timer& requestTimer, STREAMABLE_YIELDER_ARG_DECL);

  // Return the CSV or TSV lines (each terminated by a newline) of the `rows`
  // of the `table`, with one field per entry of `columns` (an empty field for
  // a column that is `std::nullopt` or an unbound value), allocated with the
  // `allocator`. This is the unit of work of the parallel CSV or TSV export,
  // so it only reads its arguments and may be called concurrently for
  // different `rows`. Throw a `CancellationException` if the
  // `cancellationHandle` is cancelled. Stop early and return the lines so far
  // as soon as `stop` is requested (the export then no longer needs them).
  // Every row in `rows` and every column index in `columns` must be valid for
  // `table` (checked).
  template <ad_utility::MediaType format>
  static CsvOrTsvChunk csvOrTsvRows(
      const Index& index,
      ql::span<const std::optional<QueryExecutionTree::VariableAndColumnIndex>>
          columns,
      const TableConstRefWithVocab& table,
      ql::ranges::iota_view<uint64_t, uint64_t> rows,
      const ad_utility::AllocatorWithLimit<char>& allocator,
      ad_utility::CancellationHandle<>& cancellationHandle,
      ad_utility::AsyncTransformStopFlag stop);

  // Yield all `IdTables` provided by the given `result`.
  static ad_utility::InputRangeTypeErased<TableConstRefWithVocab> getIdTables(
      const Result& result);

  // Generate the result in "blocks" and, when iterating over the generator
  // from beginning to end, return the total number of rows in the result
  // in `totalResultSize`.
  //
  // Blocks, where all rows are before OFFSET, are requested (and hence
  // computed), but skipped.
  //
  // Blocks, where at least one row is after OFFSET but before the effective
  // export limit (minimum of the LIMIT and the value of the `send` parameter),
  // are requested and yielded (together with the corresponding `LocalVocab`
  // and the range from that `IdTable` that belongs to the result).
  //
  // Blocks after the effective export limit until the LIMIT are requested, and
  // counted towards the `totalResultSize`, but not yielded.
  //
  // Blocks after the LIMIT are not even requested.
 public:
  static ad_utility::InputRangeTypeErased<TableWithRange> getRowIndices(
      const LimitOffsetClause& limitOffset, const Result& result,
      uint64_t& resutSizeTotal, uint64_t resultSizeMultiplicator = 1);

 private:
  FRIEND_TEST(ExportQueryExecutionTrees, getIdTablesReturnsSingletonIterator);
  FRIEND_TEST(ExportQueryExecutionTrees, getIdTablesMirrorsGenerator);
  FRIEND_TEST(ExportQueryExecutionTrees, ensureCorrectSlicingOfSingleIdTable);
  FRIEND_TEST(ExportQueryExecutionTrees,
              ensureCorrectSlicingOfIdTablesWhenFirstIsSkipped);
  FRIEND_TEST(ExportQueryExecutionTrees,
              ensureCorrectSlicingOfIdTablesWhenLastIsSkipped);
  FRIEND_TEST(ExportQueryExecutionTrees,
              ensureCorrectSlicingOfIdTablesWhenFirstAndSecondArePartial);
  FRIEND_TEST(ExportQueryExecutionTrees,
              ensureCorrectSlicingOfIdTablesWhenFirstAndLastArePartial);
  FRIEND_TEST(ExportQueryExecutionTrees,
              ensureGeneratorIsNotConsumedWhenNotRequired);
  FRIEND_TEST(ExportQueryExecutionTrees, verifyQleverJsonContainsValidMetadata);
  FRIEND_TEST(ExportQueryExecutionTrees, compensateForLimitOffsetClause);
  FRIEND_TEST(ExportQueryExecutionTrees, csvOrTsvRows);
};

#endif  // QLEVER_SRC_ENGINE_EXPORTQUERYEXECUTIONTREES_H
