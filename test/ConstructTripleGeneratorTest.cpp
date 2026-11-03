// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "./util/IdTableHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/ConstructRowChunks.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ConstructTripleGenerator.h"
#include "engine/ConstructTripleInstantiator.h"
#include "engine/Result.h"
#include "util/AllocatorTestHelpers.h"
#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/OverloadCallOperator.h"

namespace {

using namespace qlever::constructExport;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Field;
using ::testing::ResultOf;
using Triples = ad_utility::sparql_types::Triples;

auto iriV = ad_utility::testing::iriV;

static auto matchTriple(const std::string& s, const std::string& p,
                        const std::string& o) {
  auto termStr = [](const EvaluatedTerm& t) {
    return formatTerm(*t, /*includeDataType=*/false);
  };
  return AllOf(Field(&EvaluatedTriple::subject_, ResultOf(termStr, s)),
               Field(&EvaluatedTriple::predicate_, ResultOf(termStr, p)),
               Field(&EvaluatedTriple::object_, ResultOf(termStr, o)));
}

static auto matchStringTriple(const std::string& s, const std::string& p,
                              const std::string& o) {
  return AllOf(Field(&StringTriple::subject_, s),
               Field(&StringTriple::predicate_, p),
               Field(&StringTriple::object_, o));
}

static constexpr auto U = Id::makeUndefined();

}  // namespace

namespace qlever::constructExport {

// =============================================================================
// Test fixture.
// Builds a small index from:
//   <s> <p> <o> .
//   <s> <q> "hello" .
//
// Provides helpers to build `IdTable`s, template triples, and `TableWithRange`
// values.
// =============================================================================

class ConstructTripleGeneratorTest : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ =
      ad_utility::testing::getQec("<s> <p> <o> . <s> <q> \"hello\" .");
  const Index& index_ = qec_->getIndex();

  Id idS_ = ad_utility::testing::makeGetId(index_)("<s>");
  Id idP_ = ad_utility::testing::makeGetId(index_)("<p>");
  Id idO_ = ad_utility::testing::makeGetId(index_)("<o>");
  Id idQ_ = ad_utility::testing::makeGetId(index_)("<q>");

  // Create a non-cancelled `CancellationHandle`.
  static ad_utility::SharedCancellationHandle makeHandle() {
    return std::make_shared<
        ad_utility::SharedCancellationHandle::element_type>();
  }

  // Wrap an `IdTable` in a shared Result (moves the table in).
  static std::shared_ptr<const Result> makeResult(IdTable table) {
    return std::make_shared<const Result>(
        std::move(table), std::vector<ColumnIndex>{}, LocalVocab{});
  }

  // Create a `TableWithRange` referencing the Result's `IdTable`, covering rows
  // [start, end).
  static TableWithRange makeTableWithRange(const Result& result, uint64_t start,
                                           uint64_t end) {
    return {TableConstRefWithVocab{result.idTableView(), result.localVocab()},
            ql::views::iota(start, end)};
  }

  // Wrap a single `TableWithRange` in an `InputRangeTypeErased`.
  static ad_utility::InputRangeTypeErased<TableWithRange> singleTableRange(
      TableWithRange table) {
    return ad_utility::InputRangeTypeErased{
        std::vector<TableWithRange>{std::move(table)}};
  }

  // Build the shared evaluation config used by generator APIs.
  EvaluationConfig makeConfig(
      ad_utility::SharedCancellationHandle handle = makeHandle()) const {
    return EvaluationConfig{index_, std::move(handle), *qec_};
  }

  // Run `ConstructTripleGenerator::evaluateTables` over a single
  // `TableWithRange` and collect `EvaluatedTriple`s.
  std::vector<EvaluatedTriple> run(
      Triples triples, VariableToColumnMap varMap, TableWithRange table,
      ad_utility::SharedCancellationHandle handle = makeHandle()) {
    auto stringTriples = ConstructTripleGenerator::evaluateTables(
        triples, varMap, singleTableRange(std::move(table)), 0,
        makeConfig(std::move(handle)));

    return ::ranges::to_vector(stringTriples);
  }

  // Build a single-triple CONSTRUCT template.
  static Triples oneTriple(GraphTerm s, GraphTerm p, GraphTerm o) {
    return {std::array{std::move(s), std::move(p), std::move(o)}};
  }
};

// =============================================================================
// Tests
// =============================================================================

// No rows in the table view -> no triples emitted, regardless of the template.
TEST_F(ConstructTripleGeneratorTest, emptyTable) {
  auto result = makeResult(makeIdTableFromVector({}));
  auto table = makeTableWithRange(*result, 0, 0);  // empty table
  auto templateTriples = oneTriple(iriV("<s>"), iriV("<p>"), iriV("<o>"));

  EXPECT_TRUE(run(templateTriples, {}, table).empty());
}

// All-constants template: every result row emits one identical triple,
// regardless of `IdTable` cell contents.
TEST_F(ConstructTripleGeneratorTest, allConstantsYieldsOneTriplePerRow) {
  //              col 0
  // row 0:       undefined
  // row 1:       undefined
  // row 2:       undefined
  auto result = makeResult(makeIdTableFromVector({{U}, {U}, {U}}));
  auto table = makeTableWithRange(*result, 0, 3);
  auto templateTriples = oneTriple(iriV("<s>"), iriV("<p>"), iriV("<o>"));

  EXPECT_THAT(run(templateTriples, {}, table),
              ElementsAre(matchTriple("<s>", "<p>", "<o>"),
                          matchTriple("<s>", "<p>", "<o>"),
                          matchTriple("<s>", "<p>", "<o>")));
}

// Variable in subject position: correctly resolved from the `IdTable` column.
TEST_F(ConstructTripleGeneratorTest, variableInSubjectResolved) {
  //              col 0
  // row 0:       <s>
  // row 1:       <o>
  auto result = makeResult(makeIdTableFromVector({{idS_}, {idO_}}));
  auto table = makeTableWithRange(*result, 0, 2);
  auto triples = oneTriple(Variable{"?sub"}, iriV("<p>"), iriV("<o>"));
  VariableToColumnMap varMap;
  // values of variable ?sub are located in column 0 of `Idtable`.
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);

  EXPECT_THAT(run(triples, varMap, table),
              ElementsAre(matchTriple("<s>", "<p>", "<o>"),
                          matchTriple("<o>", "<p>", "<o>")));
}

// A row where a variable resolves to an undefined `Id` -> that triple is
// dropped.
// Rows before and after the undefined row are unaffected.
TEST_F(ConstructTripleGeneratorTest, undefDropsTriple) {
  auto result = makeResult(makeIdTableFromVector({{idS_}, {U}, {idO_}}));
  auto table = makeTableWithRange(*result, 0, 3);
  auto triples = oneTriple(Variable{"?sub"}, iriV("<p>"), iriV("<o>"));
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);

  EXPECT_THAT(run(triples, varMap, table),
              ElementsAre(matchTriple("<s>", "<p>", "<o>"),
                          matchTriple("<o>", "<p>", "<o>")));
}

// Multiple template triples: for each result row all template triples are
// emitted in row-major order (all template triples for row 0, then row 1, ...).
TEST_F(ConstructTripleGeneratorTest, multipleTemplateTriples) {
  auto result = makeResult(makeIdTableFromVector({{idS_}, {idO_}}));
  auto table = makeTableWithRange(*result, 0, 2);
  Triples templateTriples{
      std::array<GraphTerm, 3>{Variable{"?sub"}, iriV("<p>"), iriV("<o1>")},
      std::array<GraphTerm, 3>{Variable{"?sub"}, iriV("<q>"), iriV("<o2>")},
  };
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);

  EXPECT_THAT(run(templateTriples, varMap, table),
              ElementsAre(matchTriple("<s>", "<p>", "<o1>"),
                          matchTriple("<s>", "<q>", "<o2>"),
                          matchTriple("<o>", "<p>", "<o1>"),
                          matchTriple("<o>", "<q>", "<o2>")));
}

// Blank-node row IDs combine rowOffset (accumulated over prior tables) and
// the index of the row in the view of its table:
//   blankNodeRowId = rowOffset + (row - firstRow of the view)
TEST_F(ConstructTripleGeneratorTest, blankNodeUsesCorrectRowId) {
  auto result = makeResult(makeIdTableFromVector({{U}, {U}, {U}}));

  // View starts at absolute row 1 of the `IdTable`: firstRow=1, numRows=2.
  auto table = makeTableWithRange(*result, 1, 3);
  // Template: _:u<rowId>_node <p> <o>  (rowOffset=0)
  // row 1 of the table -> blankNodeRowId = 0 + (1 - 1) = 0
  // row 2 of the table -> blankNodeRowId = 0 + (2 - 1) = 1
  auto templateTriples =
      oneTriple(BlankNode{false, "node"}, iriV("<p>"), iriV("<o>"));

  EXPECT_THAT(run(templateTriples, {}, table),
              ElementsAre(matchTriple("_:u0_node", "<p>", "<o>"),
                          matchTriple("_:u1_node", "<p>", "<o>")));
}

// rowOffset accumulates across multiple tables: blank node IDs from the second
// table incorporate the row count of the first table.
TEST_F(ConstructTripleGeneratorTest, rowOffsetAccumulatesAcrossTables) {
  // Table 1: 3 rows (view 0..3)
  auto result1 = makeResult(makeIdTableFromVector({{U}, {U}, {U}}));
  auto table1 = makeTableWithRange(*result1, 0, 3);

  // Table 2: 2 rows (view 5..7, i.e. rows 5 and 6 of its IdTable)
  auto result2 =
      makeResult(makeIdTableFromVector({{U}, {U}, {U}, {U}, {U}, {U}, {U}}));
  auto table2 = makeTableWithRange(*result2, 5, 7);

  auto templateTriples =
      oneTriple(BlankNode{false, "x"}, iriV("<p>"), iriV("<o>"));

  std::vector<TableWithRange> tables{table1, table2};

  auto tableRange =
      ad_utility::InputRangeTypeErased<TableWithRange>{std::move(tables)};

  auto range = ConstructTripleGenerator::evaluateTables(
      templateTriples, {}, std::move(tableRange), 0, makeConfig());

  // Table 1: rowOffset=0, firstRow=0
  //   row 0: rowId = 0+(0-0) = 0
  //   row 1: rowId = 0+(1-0) = 1
  //   row 2: rowId = 0+(2-0) = 2
  // Table 2: rowOffset=3 (3 rows processed from table1), firstRow=5
  //   row 5: rowId = 3+(5-5) = 3
  //   row 6: rowId = 3+(6-5) = 4
  EXPECT_THAT(::ranges::to_vector(std::move(range)),
              ElementsAre(matchTriple("_:u0_x", "<p>", "<o>"),
                          matchTriple("_:u1_x", "<p>", "<o>"),
                          matchTriple("_:u2_x", "<p>", "<o>"),
                          matchTriple("_:u3_x", "<p>", "<o>"),
                          matchTriple("_:u4_x", "<p>", "<o>")));
}

// An OFFSET of 2 that ends inside the first table, as `getRowIndices` produces
// it: the view of the first table starts at row 2, and the `rowOffset` is the
// OFFSET. The rows get the consecutive ids 2, 3 and 4, so that the blank nodes
// of different rows are different. (The ids used to be `rowOffset + row`,
// which gave 4 for the first row and then 3 and 4 for the second table.)
TEST_F(ConstructTripleGeneratorTest, blankNodeRowIdsWithOffsetInsideTable) {
  auto result1 = makeResult(makeIdTableFromVector({{U}, {U}, {U}}));
  auto result2 = makeResult(makeIdTableFromVector({{U}, {U}}));
  std::vector<TableWithRange> tables{makeTableWithRange(*result1, 2, 3),
                                     makeTableWithRange(*result2, 0, 2)};
  auto templateTriples =
      oneTriple(BlankNode{false, "x"}, iriV("<p>"), iriV("<o>"));
  auto range = ConstructTripleGenerator::evaluateTables(
      templateTriples, {},
      ad_utility::InputRangeTypeErased<TableWithRange>{std::move(tables)}, 2,
      makeConfig());
  EXPECT_THAT(::ranges::to_vector(std::move(range)),
              ElementsAre(matchTriple("_:u2_x", "<p>", "<o>"),
                          matchTriple("_:u3_x", "<p>", "<o>"),
                          matchTriple("_:u4_x", "<p>", "<o>")));
}

// A view starting at a non-zero index reads the correct rows from the
// `IdTable`.
TEST_F(ConstructTripleGeneratorTest, viewSubrangeReadsCorrectRowsOfIdTable) {
  //              col 0
  // row 0:       <s>     <- not in the view
  // row 1:       <p>     <- firstRow (view starts here)
  // row 2:       <o>
  // row 3:       <q>     <- endRow (exclusive)
  auto result =
      makeResult(makeIdTableFromVector({{idS_}, {idP_}, {idO_}, {idQ_}}));
  auto table = makeTableWithRange(*result, 1, 3);
  auto templateTriples =
      oneTriple(Variable{"?sub"}, iriV("<pred>"), iriV("<obj>"));
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);

  EXPECT_THAT(run(templateTriples, varMap, table),
              ElementsAre(matchTriple("<p>", "<pred>", "<obj>"),
                          matchTriple("<o>", "<pred>", "<obj>")));
}

// More than `ConstructTripleGenerator::BATCH_SIZE` rows: the generator
// correctly crosses the internal batch boundary and yields triples for all
// rows.
TEST_F(ConstructTripleGeneratorTest, acrossBatchBoundary) {
  constexpr size_t N = ConstructTripleGenerator::BATCH_SIZE + 1;

  std::vector<std::vector<IntOrId>> rows(N, std::vector<IntOrId>{idS_});
  auto result = makeResult(makeIdTableFromVector(rows));
  auto table = makeTableWithRange(*result, 0, N);
  auto templateTriples = oneTriple(Variable{"?sub"}, iriV("<p>"), iriV("<o>"));
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);

  auto collected = run(templateTriples, varMap, table);

  ASSERT_EQ(collected.size(), N);
  for (const auto& triple : collected) {
    EXPECT_THAT(triple, matchTriple("<s>", "<p>", "<o>"));
  }
}

// After consuming all `ConstructTripleGenerator::BATCH_SIZE` triples in
// batch 0, cancelling the handle causes the next get() call (which would start
// batch 1) to throw.
TEST_F(ConstructTripleGeneratorTest, cancellationThrowsBetweenBatches) {
  constexpr size_t N = ConstructTripleGenerator::BATCH_SIZE + 1;

  std::vector<std::vector<IntOrId>> rows(N, std::vector<IntOrId>{idS_});
  auto result = makeResult(makeIdTableFromVector(rows));
  auto table = makeTableWithRange(*result, 0, N);
  auto templateTriples = oneTriple(iriV("<s>"), iriV("<p>"), iriV("<o>"));

  auto handle = makeHandle();
  auto range = ConstructTripleGenerator::evaluateTables(
      templateTriples, {}, singleTableRange(table), 0, makeConfig(handle));

  // Drain all ConstructTripleGenerator::BATCH_SIZE triples from batch
  // 0.
  for (size_t i = 0; i < ConstructTripleGenerator::BATCH_SIZE; ++i) {
    ASSERT_TRUE(range.get().has_value());
  }

  // Cancel before the next get() triggers batch 1.
  handle->cancel(ad_utility::CancellationState::MANUAL);

  EXPECT_ANY_THROW(range.get());
}

// Cancellation is only checked at the start of each batch, not within one.
// Cancelling mid-batch does not interrupt the current batch: the remaining
// triples of that batch are still returned.
TEST_F(ConstructTripleGeneratorTest, cannotCancelDuringBatch) {
  // Two rows. Both should fit inside a single batch (make sure via assert).
  static_assert(2 < ConstructTripleGenerator::BATCH_SIZE);
  auto result = makeResult(makeIdTableFromVector({{idS_}, {idO_}}));
  auto table = makeTableWithRange(*result, 0, 2);
  auto templateTriples = oneTriple(iriV("<s>"), iriV("<p>"), iriV("<o>"));

  auto handle = makeHandle();
  auto range = ConstructTripleGenerator::evaluateTables(
      templateTriples, {}, singleTableRange(std::move(table)), 0,
      makeConfig(handle));

  // First triple succeeds.
  ASSERT_TRUE(range.get().has_value());

  // Cancel mid-batch.
  handle->cancel(ad_utility::CancellationState::MANUAL);

  // Second triple is still returned: the batch was already computed and
  // cancellation is not re-checked until the next batch starts.
  EXPECT_TRUE(range.get().has_value());

  // Range exhausted normally, no second batch means no throw.
  EXPECT_FALSE(range.get().has_value());
}

// The `IdCache` is shared across batches: for the same `Id`, the
// `EvaluatedTerm` `shared_ptr` returned in batch 1 is pointer-identical to the
// one from batch 0, proving the cache was not reset between batches.
TEST_F(ConstructTripleGeneratorTest, idCacheIsSharedAcrossBatches) {
  constexpr size_t N = ConstructTripleGenerator::BATCH_SIZE + 1;

  // All rows hold the same ID so the second batch is guaranteed to be a cache
  // hit if the cache is carried over. This relies on the cache capacity being
  // larger than `BATCH_SIZE`. The minimum cache capacity is
  // `CACHE_ENTRIES_PER_VARIABLE` (due to the std::max floor in `makeIdCache`),
  // which currently exceeds `BATCH_SIZE`.
  std::vector<std::vector<IntOrId>> rows(N, std::vector<IntOrId>{idS_});
  auto result = makeResult(makeIdTableFromVector(rows));
  auto table = makeTableWithRange(*result, 0, N);
  auto templateTriples = oneTriple(Variable{"?sub"}, iriV("<p>"), iriV("<o>"));
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);

  auto collected = run(templateTriples, varMap, std::move(table));
  ASSERT_EQ(collected.size(), N);

  // Batch 0: row 0. Batch 1: last row. Check pointer equality here.
  const EvaluatedTerm& fromBatch0 = collected.front().subject_;
  const EvaluatedTerm& fromBatch1 = collected.back().subject_;
  EXPECT_EQ(fromBatch0.get(), fromBatch1.get());
}

// =============================================================================
// Tests for `ConstructTripleGenerator::makeIdCache`
// =============================================================================

// Zero variables: the `std::max` floor ensures we never create a zero-capacity
// cache, so the capacity equals exactly `CACHE_ENTRIES_PER_VARIABLE`.
TEST(MakeIdCache, emptyTemplate) {
  PreprocessedConstructTemplate tmpl;
  auto cache = ConstructTripleGenerator::makeIdCache(tmpl);
  EXPECT_EQ(cache.capacity(),
            ConstructTripleGenerator::CACHE_ENTRIES_PER_VARIABLE);
}

// One variable: the floor is not doing work; result is still
// `CACHE_ENTRIES_PER_VARIABLE`.
TEST(MakeIdCache, singleVariable) {
  PreprocessedConstructTemplate tmpl;
  // Note: Assigning an `initializer_list` of size 1 to a `std::vector` here
  // would trigger a false-positive `-Warray-bounds` warning with GCC 12 in
  // optimized builds, hence the explicit `std::vector`.
  tmpl.uniqueVariableColumns_ = std::vector<ColumnIndex>{0};
  auto cache = ConstructTripleGenerator::makeIdCache(tmpl);
  EXPECT_EQ(cache.capacity(),
            ConstructTripleGenerator::CACHE_ENTRIES_PER_VARIABLE);
}

// Multiple variables: capacity scales linearly with the number of unique
// variable columns.
TEST(MakeIdCache, multipleVariables) {
  PreprocessedConstructTemplate tmpl;
  tmpl.uniqueVariableColumns_ = {0, 1, 2};
  auto cache = ConstructTripleGenerator::makeIdCache(tmpl);
  EXPECT_EQ(cache.capacity(),
            3 * ConstructTripleGenerator::CACHE_ENTRIES_PER_VARIABLE);
}

// =============================================================================
// Tests for `ConstructTripleGenerator::generateStringTriples`
// =============================================================================

// Smoke test: constants are emitted as plain IRI strings in the StringTriple.
TEST_F(ConstructTripleGeneratorTest, generateStringTriplesFormatsAsStrings) {
  auto result = makeResult(makeIdTableFromVector({{U}}));
  auto table = makeTableWithRange(*result, 0, 1);
  auto templateTriples = oneTriple(iriV("<s>"), iriV("<p>"), iriV("<o>"));

  auto range = ConstructTripleGenerator::generateStringTriples(
      templateTriples, {}, singleTableRange(std::move(table)), 0, makeConfig());

  auto triples = ::ranges::to_vector(range);

  EXPECT_THAT(triples, ElementsAre(matchStringTriple("<s>", "<p>", "<o>")));
}

// =============================================================================
// Tests for `ConstructTripleGenerator::generateFormattedTriples`
// =============================================================================

// Helper: collect all strings from a formatted-triples range.
static std::vector<std::string> collectFormatted(
    ad_utility::InputRangeTypeErased<std::string> range) {
  std::vector<std::string> result;
  while (auto s = range.get()) {
    result.push_back(std::move(*s));
  }
  return result;
}

struct FormattedTriplesParam {
  ad_utility::MediaType mediaType;
  std::string expected;
};

class GenerateFormattedTriplesTest
    : public ConstructTripleGeneratorTest,
      public ::testing::WithParamInterface<FormattedTriplesParam> {};

TEST_P(GenerateFormattedTriplesTest, formatsCorrectly) {
  auto result = makeResult(makeIdTableFromVector({{U}}));
  auto table = makeTableWithRange(*result, 0, 1);
  auto templateTriples = oneTriple(iriV("<s>"), iriV("<p>"), iriV("<o>"));

  auto range = ConstructTripleGenerator::generateFormattedTriples(
      templateTriples, {}, singleTableRange(std::move(table)), 0,
      GetParam().mediaType, makeConfig());

  EXPECT_THAT(collectFormatted(std::move(range)),
              ElementsAre(GetParam().expected));
}

INSTANTIATE_TEST_SUITE_P(
    FormattedTripleFormats, GenerateFormattedTriplesTest,
    ::testing::Values(
        FormattedTriplesParam{ad_utility::MediaType::turtle, "<s> <p> <o> .\n"},
        FormattedTriplesParam{ad_utility::MediaType::csv, "<s>,<p>,<o>\n"},
        FormattedTriplesParam{ad_utility::MediaType::tsv, "<s>\t<p>\t<o>\n"}));

// Only turtle, csv, and tsv are supported. Any other media type triggers a
// contract check failure when the first triple is pulled from the range.
TEST_F(ConstructTripleGeneratorTest,
       generateFormattedTriplesRejectsUnsupportedMediaType) {
  auto result = makeResult(makeIdTableFromVector({{U}}));
  auto table = makeTableWithRange(*result, 0, 1);
  auto templateTriples = oneTriple(iriV("<s>"), iriV("<p>"), iriV("<o>"));

  static constexpr std::array supported{
      ad_utility::MediaType::turtle,
      ad_utility::MediaType::csv,
      ad_utility::MediaType::tsv,
      ad_utility::MediaType::ntriples,
  };

  // expect that unsupported mediatypes throw, expect that supported mediatypes
  // don't throw.
  for (const auto& [mediaType, _] : ad_utility::detail::getAllMediaTypes()) {
    auto range = ConstructTripleGenerator::generateFormattedTriples(
        templateTriples, {}, singleTableRange(table), 0, mediaType,
        makeConfig());

    if (ad_utility::contains(supported, mediaType)) {
      EXPECT_NO_THROW(range.get());
    } else {
      EXPECT_ANY_THROW(range.get());
    }
  }
}

// _____________________________________________________________________________
// Helpers for the tests of the parallel export.
namespace {
using detail::BorrowRows;
using detail::ChunkPart;
using detail::CopiedRows;
using detail::CopyColumns;
using detail::InputError;
using detail::RowChunk;
using detail::RowChunkOrInputError;

// A range of the `tables` that throws a `std::runtime_error` with the message
// "table failed" when the table after them is requested, if `throwAtEnd`.
ad_utility::InputRangeTypeErased<TableWithRange> tablesThatMayThrow(
    std::vector<TableWithRange> tables, bool throwAtEnd,
    size_t* numberOfRequestedTables = nullptr) {
  return ad_utility::InputRangeTypeErased<TableWithRange>{
      ad_utility::InputRangeFromGetCallable{
          [tables = std::move(tables), throwAtEnd, numberOfRequestedTables,
           i = size_t{0}]() mutable -> std::optional<TableWithRange> {
            if (numberOfRequestedTables != nullptr) {
              ++*numberOfRequestedTables;
            }
            if (i < tables.size()) {
              return tables.at(i++);
            }
            if (throwAtEnd) {
              throw std::runtime_error{"table failed"};
            }
            return std::nullopt;
          }}};
}

// The values of column 0 of the rows of a `ChunkPart` (which must be
// `IntId`s), and their blank node row id.
using PartValues = std::pair<std::vector<int64_t>, size_t>;
PartValues partValues(const ChunkPart& part) {
  std::vector<int64_t> values;
  std::visit(
      ad_utility::OverloadCallOperator{
          [&values](const TableWithRange& rows) {
            for (uint64_t row : rows.view_) {
              values.push_back(rows.tableWithVocab_.idTable()(row, 0).getInt());
            }
          },
          [&values](const CopiedRows& rows) {
            EXPECT_EQ(rows.idTable_.numColumns(), 1);
            for (Id id : rows.idTable_.getColumn(0)) {
              values.push_back(id.getInt());
            }
          }},
      part.rows_);
  return {std::move(values), part.blankNodeRowIdOfFirstRow_};
}

// The `PartValues` of each part of the `RowChunk` of `element`.
std::vector<PartValues> chunkValues(
    const std::optional<RowChunkOrInputError>& element) {
  std::vector<PartValues> result;
  EXPECT_TRUE(element.has_value());
  if (!element.has_value() ||
      !std::holds_alternative<RowChunk>(element.value())) {
    ADD_FAILURE() << "Expected a `RowChunk`";
    return result;
  }
  for (const ChunkPart& part : std::get<RowChunk>(element.value()).parts_) {
    result.push_back(partValues(part));
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
// The rows of all tables are cut into chunks of the maximal number of rows
// (only the last one is smaller), across table boundaries, with the blank node
// row ids of `evaluateTables`. The rows are borrowed, or only the requested
// column is copied, with a clone of the `LocalVocab` that is shared by the
// copies of a table. A table is only requested when its rows are needed.
TEST_F(ConstructTripleGeneratorTest, makeRowChunks) {
  using ad_utility::testing::IntId;
  // Table `a` contributes its rows [0, 3), `b` its rows [2, 7), and `c` its
  // rows [0, 2), so there are 3 + 5 + 2 = 10 rows, and chunks of at most 4
  // rows. Column 0 has the values that the test checks; with `CopyColumns`, it
  // is the copy of column 1.
  auto a = makeResult(makeIdTableFromVector({{0, 0}, {-1, 1}, {-2, 2}}, IntId));
  auto b = makeResult(makeIdTableFromVector({{-10, 10},
                                             {-11, 11},
                                             {-12, 12},
                                             {-13, 13},
                                             {-14, 14},
                                             {-15, 15},
                                             {-16, 16}},
                                            IntId));
  LocalVocab localVocabOfC;
  Id localId =
      Id::makeFromLocalVocabIndex(localVocabOfC.getIndexAndAddIfNotContained(
          LocalVocabEntry::literalWithoutQuotes("local",
                                                qec_->getLocalVocabContext())));
  auto c = std::make_shared<const Result>(
      makeIdTableFromVector({{-20, 20}, {-21, 21}}, IntId),
      std::vector<ColumnIndex>{}, std::move(localVocabOfC));
  auto tables = [&]() {
    return std::vector<TableWithRange>{makeTableWithRange(*a, 0, 3),
                                       makeTableWithRange(*b, 2, 7),
                                       makeTableWithRange(*c, 0, 2)};
  };
  // `a` starts at the row offset 100, `b` at 100 + 3, and `c` at 103 + 5.
  auto check = [&](detail::RowStorage storage,
                   std::vector<int64_t> (*values)(std::vector<int64_t>)) {
    size_t numberOfRequestedTables = 0;
    auto chunks = detail::makeRowChunks(
        tablesThatMayThrow(tables(), false, &numberOfRequestedTables),
        {.blankNodeRowIdOfFirstRow_ = 100,
         .maxNumberOfRowsPerChunk_ = 4,
         .storage_ = std::move(storage),
         .allocator_ = ad_utility::testing::makeAllocator()});
    std::vector<std::optional<RowChunkOrInputError>> elements;
    elements.push_back(chunks.get());
    EXPECT_THAT(chunkValues(elements.back()),
                ElementsAre(PartValues{values({0, 1, 2}), 100},
                            PartValues{values({12}), 103}));
    EXPECT_EQ(numberOfRequestedTables, 2);
    elements.push_back(chunks.get());
    EXPECT_THAT(chunkValues(elements.back()),
                ElementsAre(PartValues{values({13, 14, 15, 16}), 104}));
    EXPECT_EQ(numberOfRequestedTables, 2);
    elements.push_back(chunks.get());
    EXPECT_THAT(chunkValues(elements.back()),
                ElementsAre(PartValues{values({20, 21}), 108}));
    EXPECT_FALSE(chunks.get().has_value());
    return elements;
  };
  // Borrowed rows are read from column 0, so their values are negative.
  auto negate = +[](std::vector<int64_t> values) {
    for (auto& value : values) {
      value = -value;
    }
    return values;
  };
  auto unchanged = +[](std::vector<int64_t> values) { return values; };
  check(BorrowRows{}, negate);
  auto copies = check(CopyColumns{{1}}, unchanged);

  // The copies of `b` in the first and second chunk share one clone of its
  // `LocalVocab`, and the clone of `c` keeps its entries alive after the result
  // of `c` is gone.
  auto copiedPart = [&copies](size_t chunk, size_t part) -> const CopiedRows& {
    return std::get<CopiedRows>(
        std::get<RowChunk>(copies.at(chunk).value()).parts_.at(part).rows_);
  };
  EXPECT_EQ(copiedPart(0, 1).localVocab_, copiedPart(1, 0).localVocab_);
  EXPECT_NE(copiedPart(0, 0).localVocab_, copiedPart(0, 1).localVocab_);
  c.reset();
  const CopiedRows& copyOfC = copiedPart(2, 0);
  EXPECT_EQ(copyOfC.localVocab_->size(), 1);
  EXPECT_EQ(localId.getLocalVocabIndex()->toStringRepresentation(),
            "\"local\"");

  // No tables, no chunks.
  auto noChunks = detail::makeRowChunks(
      tablesThatMayThrow({}, false),
      {.blankNodeRowIdOfFirstRow_ = 0,
       .maxNumberOfRowsPerChunk_ = 4,
       .storage_ = BorrowRows{},
       .allocator_ = ad_utility::testing::makeAllocator()});
  EXPECT_FALSE(noChunks.get().has_value());
}

// _____________________________________________________________________________
// If requesting a table throws, the rows before are returned as a last
// (partial) chunk, then the exception as an `InputError`, and then the range
// ends without requesting another table.
TEST_F(ConstructTripleGeneratorTest, makeRowChunksWithInputError) {
  using ad_utility::testing::IntId;
  auto a = makeResult(makeIdTableFromVector({{0}, {1}, {2}, {3}, {4}}, IntId));
  size_t numberOfRequestedTables = 0;
  auto chunks = detail::makeRowChunks(
      tablesThatMayThrow({makeTableWithRange(*a, 0, 5)}, true,
                         &numberOfRequestedTables),
      {.blankNodeRowIdOfFirstRow_ = 0,
       .maxNumberOfRowsPerChunk_ = 4,
       .storage_ = BorrowRows{},
       .allocator_ = ad_utility::testing::makeAllocator()});
  EXPECT_THAT(chunkValues(chunks.get()),
              ElementsAre(PartValues{{0, 1, 2, 3}, 0}));
  EXPECT_THAT(chunkValues(chunks.get()), ElementsAre(PartValues{{4}, 4}));
  auto error = chunks.get();
  ASSERT_TRUE(error.has_value());
  ASSERT_TRUE(std::holds_alternative<InputError>(error.value()));
  AD_EXPECT_THROW_WITH_MESSAGE(
      std::rethrow_exception(std::get<InputError>(error.value()).exception_),
      ::testing::HasSubstr("table failed"));
  EXPECT_FALSE(chunks.get().has_value());
  EXPECT_EQ(numberOfRequestedTables, 2);
}

// _____________________________________________________________________________
// The parallel export gives the same output as `generateFormattedTriples`, for
// every format and both table lifetimes: two tables with more rows than one
// chunk, blank nodes, an unbound value, a `LocalVocab` entry, an unused
// column, and an OFFSET that ends inside the first table. If requesting a
// table throws, the output before the exception is also the same: the chunks
// before it are not dropped.
TEST_F(ConstructTripleGeneratorTest, generateFormattedChunksInParallel) {
  LocalVocab localVocab;
  Id localId =
      Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
          LocalVocabEntry::literalWithoutQuotes("local",
                                                qec_->getLocalVocabContext())));
  // Column 0 is not used by the template, column 1 is `?y`.
  constexpr size_t numberOfRows =
      ConstructTripleGenerator::MAX_NUMBER_OF_ROWS_PER_CHUNK + 1000;
  IdTable firstTable{2, ad_utility::testing::makeAllocator()};
  IdTable secondTable{2, ad_utility::testing::makeAllocator()};
  for (size_t i = 0; i < numberOfRows; ++i) {
    std::array values{idS_, U, idO_, localId};
    firstTable.push_back({idQ_, values.at(i % 4)});
    secondTable.push_back({idQ_, values.at((i + 1) % 4)});
  }
  auto first = makeResult(std::move(firstTable));
  auto second = std::make_shared<const Result>(std::move(secondTable),
                                               std::vector<ColumnIndex>{},
                                               std::move(localVocab));
  auto tables = [&]() {
    return std::vector<TableWithRange>{
        makeTableWithRange(*first, 7, numberOfRows),
        makeTableWithRange(*second, 0, numberOfRows)};
  };
  Triples templateTriples{
      std::array<GraphTerm, 3>{Variable{"?y"}, iriV("<p>"), iriV("<o>")},
      std::array<GraphTerm, 3>{BlankNode{false, "b"}, iriV("<q>"),
                               Variable{"?y"}}};
  VariableToColumnMap variableColumns{
      {Variable{"?x"}, makeAlwaysDefinedColumn(0)},
      {Variable{"?y"}, makePossiblyUndefinedColumn(1)}};
  // Concatenate the output, and the message of the exception, if any.
  auto consume = [](auto range) {
    std::string output;
    try {
      for (const auto& element : range) {
        output.append(std::string_view{element});
      }
    } catch (const std::exception& e) {
      output.append(absl::StrCat("ERROR: ", e.what()));
    }
    return output;
  };
  using enum ad_utility::MediaType;
  for (auto mediaType : {turtle, ntriples, csv, tsv}) {
    for (bool throwAtEnd : {false, true}) {
      auto expected =
          consume(ConstructTripleGenerator::generateFormattedTriples(
              templateTriples, variableColumns,
              tablesThatMayThrow(tables(), throwAtEnd), 7, mediaType,
              makeConfig()));
      EXPECT_THAT(expected, ::testing::HasSubstr("\"local\""));
      EXPECT_EQ(absl::StrContains(expected, "ERROR: table failed"), throwAtEnd);
      for (auto tableLifetime :
           {TableLifetime::WholeExport, TableLifetime::UntilNextTable}) {
        EXPECT_EQ(
            consume(ConstructTripleGenerator::generateFormattedChunksInParallel(
                templateTriples, variableColumns,
                tablesThatMayThrow(tables(), throwAtEnd), tableLifetime, 7,
                mediaType, makeConfig(), 4)),
            expected);
      }
    }
  }
}

// _____________________________________________________________________________
// `ChunkFormatter`: once `stop` is requested, no more triples are formatted; a
// cancellation throws; an `InputError` is rethrown.
TEST_F(ConstructTripleGeneratorTest, chunkFormatter) {
  auto result = makeResult(makeIdTableFromVector({{idS_}, {idO_}}));
  Triples templateTriples{
      std::array<GraphTerm, 3>{Variable{"?x"}, iriV("<p>"), iriV("<o>")}};
  VariableToColumnMap variableColumns{
      {Variable{"?x"}, makeAlwaysDefinedColumn(0)}};
  auto handle = makeHandle();
  detail::ChunkFormatter formatter{
      std::make_shared<const PreprocessedConstructTemplate>(
          ConstructTemplatePreprocessor::preprocess(templateTriples,
                                                    variableColumns, index_)),
      ad_utility::MediaType::turtle, index_, handle,
      ad_utility::AllocatorWithLimit<char>{
          ad_utility::testing::makeAllocator()}};
  RowChunk chunk;
  chunk.parts_.push_back(ChunkPart{makeTableWithRange(*result, 0, 2), 0});
  std::atomic<bool> stop = false;
  auto format = [&](const RowChunk& rowChunk) {
    return std::string{
        formatter.format(rowChunk, ad_utility::AsyncTransformStopFlag{stop})};
  };
  EXPECT_EQ(format(chunk), "<s> <p> <o> .\n<o> <p> <o> .\n");
  stop = true;
  EXPECT_EQ(format(chunk), "");
  stop = false;
  AD_EXPECT_THROW_WITH_MESSAGE(
      formatter(InputError{std::make_exception_ptr(
                    std::runtime_error{"table failed"})},
                ad_utility::AsyncTransformStopFlag{stop}),
      ::testing::HasSubstr("table failed"));
  handle->cancel(ad_utility::CancellationState::MANUAL);
  EXPECT_THROW(format(chunk), ad_utility::CancellationException);
}

}  // namespace qlever::constructExport
