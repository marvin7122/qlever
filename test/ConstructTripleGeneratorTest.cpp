// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <limits>
#include <stdexcept>

#include "./util/IdTableHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/ConstructRowBatchSchedule.h"
#include "engine/ConstructTripleGenerator.h"
#include "engine/ConstructTripleInstantiator.h"
#include "engine/Result.h"
#include "global/RuntimeParameters.h"
#include "util/CancellationHandle.h"
#include "util/RuntimeParametersTestHelpers.h"

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

// Blank-node row IDs combine rowOffset (accumulated over prior tables),
// firstRow (view start), and the in-batch row index:
//   blankNodeRowId = rowOffset + firstRow + rowInBatch
TEST_F(ConstructTripleGeneratorTest, blankNodeUsesCorrectRowId) {
  auto result = makeResult(makeIdTableFromVector({{U}, {U}, {U}}));

  // View starts at absolute row 1 of the `IdTable`: firstRow=1, numRows=2.
  auto table = makeTableWithRange(*result, 1, 3);
  // Template: _:u<rowId>_node <p> <o>  (rowOffset=0)
  // row 0 of batch -> blankNodeRowId = 0 + 1 + 0 = 1
  // row 1 of batch -> blankNodeRowId = 0 + 1 + 1 = 2
  auto templateTriples =
      oneTriple(BlankNode{false, "node"}, iriV("<p>"), iriV("<o>"));

  EXPECT_THAT(run(templateTriples, {}, table),
              ElementsAre(matchTriple("_:u1_node", "<p>", "<o>"),
                          matchTriple("_:u2_node", "<p>", "<o>")));
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
  //   row 0: rowId = 0+0+0 = 0
  //   row 1: rowId = 0+0+1 = 1
  //   row 2: rowId = 0+0+2 = 2
  // Table 2: rowOffset=3 (3 rows processed from table1), firstRow=5
  //   row 0: rowId = 3+5+0 = 8
  //   row 1: rowId = 3+5+1 = 9
  EXPECT_THAT(::ranges::to_vector(std::move(range)),
              ElementsAre(matchTriple("_:u0_x", "<p>", "<o>"),
                          matchTriple("_:u1_x", "<p>", "<o>"),
                          matchTriple("_:u2_x", "<p>", "<o>"),
                          matchTriple("_:u8_x", "<p>", "<o>"),
                          matchTriple("_:u9_x", "<p>", "<o>")));
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

// The runtime parameter `construct-export-row-batch-size` sets the batch size.
// Batches of one row, batches that do not divide the number of rows, and one
// batch larger than the table all yield every row.
TEST_F(ConstructTripleGeneratorTest, rowBatchSizeIsConfigurable) {
  constexpr size_t N = 5;
  std::vector<std::vector<IntOrId>> rows(N, std::vector<IntOrId>{idS_});
  auto result = makeResult(makeIdTableFromVector(rows));
  auto templateTriples = oneTriple(Variable{"?sub"}, iriV("<p>"), iriV("<o>"));
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);
  for (size_t batchSize : {size_t{1}, size_t{3}, size_t{4096}}) {
    SCOPED_TRACE(batchSize);
    auto reset = setRuntimeParameterForTest<
        &RuntimeParameters::constructExportRowBatchSize_>(batchSize);
    auto table = makeTableWithRange(*result, 0, N);
    auto collected = run(templateTriples, varMap, table);
    ASSERT_EQ(collected.size(), N) << "batch size " << batchSize;
    for (const auto& triple : collected) {
      EXPECT_THAT(triple, matchTriple("<s>", "<p>", "<o>"));
    }

    if (batchSize < N) {
      auto handle = makeHandle();
      auto range = ConstructTripleGenerator::generateStringTriples(
          templateTriples, varMap, singleTableRange(table), 0,
          makeConfig(handle));
      ASSERT_TRUE(range.get().has_value());
      handle->cancel(ad_utility::CancellationState::MANUAL);

      // Cancellation leaves the remaining rows of the first batch readable.
      for (size_t row = 1; row < batchSize; ++row) {
        ASSERT_TRUE(range.get().has_value());
      }
      // The next row starts a new batch and must observe the cancellation.
      EXPECT_THROW(range.get(), ad_utility::CancellationException);
    }
  }
}

// With `construct-export-initial-row-batch-size` smaller than
// `construct-export-row-batch-size`, the batches grow (here 1, 2, 4, 4, ...
// rows) and every row is still evaluated exactly once, in order.
TEST_F(ConstructTripleGeneratorTest, growingRowBatchesYieldEveryRow) {
  auto resetMax = setRuntimeParameterForTest<
      &RuntimeParameters::constructExportRowBatchSize_>(size_t{4});
  auto resetInitial = setRuntimeParameterForTest<
      &RuntimeParameters::constructExportInitialRowBatchSize_>(size_t{1});
  constexpr size_t N = 13;
  std::vector<std::vector<IntOrId>> rows(N, std::vector<IntOrId>{idS_});
  auto result = makeResult(makeIdTableFromVector(rows));
  auto templateTriples = oneTriple(Variable{"?sub"}, iriV("<p>"), iriV("<o>"));
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);
  auto collected =
      run(templateTriples, varMap, makeTableWithRange(*result, 0, N));
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

}  // namespace qlever::constructExport

// _____________________________________________________________________________
namespace {
using qlever::constructExport::ConstructRowBatchSchedule;
// The batch sizes of `schedule`, in order.
std::vector<size_t> batchSizes(const ConstructRowBatchSchedule& schedule) {
  std::vector<size_t> sizes;
  size_t expectedBegin = 0;
  for (size_t k = 0; k < schedule.numBatches(); ++k) {
    EXPECT_EQ(schedule.begin(k), expectedBegin) << "batch " << k;
    sizes.push_back(schedule.end(k) - schedule.begin(k));
    expectedBegin = schedule.end(k);
  }
  return sizes;
}
}  // namespace

// _____________________________________________________________________________
TEST(ConstructRowBatchSchedule, growsByDoublingUpToTheMaximum) {
  using ::testing::ElementsAre;
  EXPECT_THAT(batchSizes({100, 4, 32}),
              ElementsAre(4, 8, 16, 32, 32, 8));  // 4 + 8 + 16 + 32 + 32 + 8
  // A maximum that is not a power-of-two multiple of the initial size.
  EXPECT_THAT(batchSizes({30, 4, 10}), ElementsAre(4, 8, 10, 8));
  // The last batch is cut at the number of rows, also while growing.
  EXPECT_THAT(batchSizes({10, 4, 32}), ElementsAre(4, 6));
  EXPECT_THAT(batchSizes({4, 4, 32}), ElementsAre(4));
  EXPECT_THAT(batchSizes({0, 4, 32}), ElementsAre());
}

// _____________________________________________________________________________
TEST(ConstructRowBatchSchedule, fixedSizeWithoutGrowth) {
  using ::testing::ElementsAre;
  EXPECT_THAT(batchSizes({10, 4, 4}), ElementsAre(4, 4, 2));
  // An initial size above the maximum is clamped to the maximum.
  EXPECT_THAT(batchSizes({10, 100, 4}), ElementsAre(4, 4, 2));
  EXPECT_THAT(batchSizes({3, 1, 1}), ElementsAre(1, 1, 1));
}

// _____________________________________________________________________________
TEST(ConstructRowBatchSchedule, extremeSizesDoNotOverflow) {
  constexpr size_t maxSize = std::numeric_limits<size_t>::max();
  EXPECT_THAT(batchSizes({5, 1, maxSize}), ::testing::ElementsAre(1, 2, 2));
  EXPECT_THAT(batchSizes({5, maxSize, maxSize}), ::testing::ElementsAre(5));
  ConstructRowBatchSchedule huge{maxSize, 1, maxSize};
  // 64 growing batches (1, 2, ..., 2^63) cover all `2^64 - 1` rows.
  EXPECT_EQ(huge.numBatches(), 64u);
  EXPECT_EQ(huge.end(63), maxSize);
  EXPECT_THROW((ConstructRowBatchSchedule{5, 0, 4}), ad_utility::Exception);
  EXPECT_THROW((ConstructRowBatchSchedule{5, 4, 0}), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(ConstructRowBatchSchedule, unrepresentableGrowingBatchesAreRejected) {
  constexpr size_t maxSize = std::numeric_limits<size_t>::max();
  // The growing-batch totals 3 * (2^63 - 1) and 4294967298 * (2^32 - 1) both
  // wrap `size_t`, so the configurations are rejected fail-fast.
  EXPECT_THROW((ConstructRowBatchSchedule{5, 3, maxSize}),
               std::invalid_argument);
  EXPECT_THROW((ConstructRowBatchSchedule{5, 4294967298ULL, maxSize}),
               std::invalid_argument);
  // The largest representable prefix, 2^64 - 1, is still accepted.
  EXPECT_NO_THROW((ConstructRowBatchSchedule{maxSize, 1, maxSize}));
}

// _____________________________________________________________________________
TEST(ConstructRowBatchSchedule, initialBatchSizeAfterContinuesTheGrowth) {
  // Batches 4, 8, 16, 32, 32, ... start at rows 0, 4, 12, 28, 60, ...
  EXPECT_EQ(ConstructRowBatchSchedule::initialBatchSizeAfter(0, 4, 32), 4u);
  EXPECT_EQ(ConstructRowBatchSchedule::initialBatchSizeAfter(3, 4, 32), 4u);
  EXPECT_EQ(ConstructRowBatchSchedule::initialBatchSizeAfter(4, 4, 32), 8u);
  EXPECT_EQ(ConstructRowBatchSchedule::initialBatchSizeAfter(11, 4, 32), 8u);
  EXPECT_EQ(ConstructRowBatchSchedule::initialBatchSizeAfter(12, 4, 32), 16u);
  EXPECT_EQ(ConstructRowBatchSchedule::initialBatchSizeAfter(28, 4, 32), 32u);
  EXPECT_EQ(ConstructRowBatchSchedule::initialBatchSizeAfter(1000, 4, 32), 32u);
  // Without growth, and with a maximum that is not a power-of-two multiple.
  EXPECT_EQ(ConstructRowBatchSchedule::initialBatchSizeAfter(0, 64, 32), 32u);
  EXPECT_EQ(ConstructRowBatchSchedule::initialBatchSizeAfter(12, 4, 10), 10u);
  constexpr size_t maxSize = std::numeric_limits<size_t>::max();
  EXPECT_EQ(
      ConstructRowBatchSchedule::initialBatchSizeAfter(maxSize, 1, maxSize),
      maxSize);
}
