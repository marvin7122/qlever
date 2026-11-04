// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <robintf@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "./WordsAndDocsFileLineCreator.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/MaterializedViews.h"
#include "engine/NamedResultCache.h"
#include "engine/QueryPlanner.h"
#include "index/ExportIds.h"
#include "parser/SparqlParser.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/AllocatorTestHelpers.h"
#include "util/ConcurrencyLimitedExecutor.h"
#include "util/Forward.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/NoCopyNoMove.h"
#include "util/ParseableDuration.h"
#include "util/ParsedQueryTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

using namespace std::string_literals;
using namespace std::chrono_literals;
using ::testing::ElementsAre;
using ::testing::EndsWith;
using ::testing::Eq;
using ::testing::HasSubstr;

namespace {
using ad_utility::testing::parseQuery;

// Run the given SPARQL `query` on the index that is built from the `config` and
// export the result as the `mediaType`.
std::string runQueryStreamableResult(
    QueryExecutionContext* qec, const std::string& query,
    ad_utility::MediaType mediaType,
    std::optional<size_t> exportLimit = std::nullopt) {
  // TODO<joka921> There is a bug in the caching that we have yet to trace.
  // This cache clearing should not be necessary.
  qec->clearCacheUnpinnedOnly();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = parseQuery(query);
  pq._limitOffset.exportLimit_ = exportLimit;
  auto qet = qp.createExecutionTree(pq);
  ad_utility::Timer timer(ad_utility::Timer::Started);
  auto strGenerator = ExportQueryExecutionTrees::computeResult(
      pq, *qet, mediaType, timer, std::move(cancellationHandle));

  std::string result;
  for (const auto& block : strGenerator) {
    result += block;
  }
  return result;
}

// Overload that runs the `query` on the index of the given `config`.
std::string runQueryStreamableResult(
    ad_utility::testing::TestIndexConfig config, const std::string& query,
    ad_utility::MediaType mediaType,
    std::optional<size_t> exportLimit = std::nullopt) {
  return runQueryStreamableResult(
      ad_utility::testing::getQec(std::move(config)), query, mediaType,
      exportLimit);
}

// Run the given SPARQL `query` on the given Turtle `kg` and export the result
// as the `mediaType`.
std::string runQueryStreamableResult(
    const std::string& kg, const std::string& query,
    ad_utility::MediaType mediaType, bool useTextIndex = false,
    std::optional<size_t> exportLimit = std::nullopt) {
  ad_utility::testing::TestIndexConfig config{kg};
  config.createTextIndex = useTextIndex;
  return runQueryStreamableResult(std::move(config), query, mediaType,
                                  exportLimit);
}

// Run the given SPARQL `query` on the given Turtle `kg` and export the result
// as JSON. `mediaType` must be `sparqlJSON` or `qleverJSON`.
nlohmann::json runJSONQuery(const std::string& kg, const std::string& query,
                            ad_utility::MediaType mediaType,
                            bool useTextIndex = false,
                            std::optional<size_t> exportLimit = std::nullopt) {
  ad_utility::testing::TestIndexConfig config{kg};
  config.createTextIndex = useTextIndex;
  auto qec = ad_utility::testing::getQec(std::move(config));
  // TODO<joka921> There is a bug in the caching that we have yet to trace.
  // This cache clearing should not be necessary.
  qec->clearCacheUnpinnedOnly();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = parseQuery(query);
  pq._limitOffset.exportLimit_ = exportLimit;
  auto qet = qp.createExecutionTree(pq);
  ad_utility::Timer timer{ad_utility::Timer::Started};
  std::string resStr;
  for (auto c : ExportQueryExecutionTrees::computeResult(
           pq, *qet, mediaType, timer, std::move(cancellationHandle))) {
    resStr += c;
  }
  return nlohmann::json::parse(resStr);
}

// A test case that tests the correct execution and exporting of a SELECT query
// in various formats.
struct TestCaseSelectQuery {
  std::string kg;                   // The knowledge graph (TURTLE)
  std::string query;                // The query (SPARQL)
  uint64_t resultSize;              // The expected number of results.
  std::string resultTsv;            // The expected result in TSV format.
  std::string resultCsv;            // The expected result in CSV format
  nlohmann::json resultQLeverJSON;  // The expected result in QLeverJSOn format.
                                    // Note: this member only contains the inner
                                    // result array with the bindings and NOT
                                    // the metadata.
  nlohmann::json resultSparqlJSON;  // The expected result in SparqlJSON format.
  std::string resultXml;
};

// A test case that tests the correct execution and exporting of an ASK query
// in various formats.
struct TestCaseAskQuery {
  std::string kg;                   // The knowledge graph (TURTLE)
  std::string query;                // The query (SPARQL)
  nlohmann::json resultQLeverJSON;  // The expected result in QLeverJSON format.
  // Note: this member only contains the inner
  // result array with the bindings and NOT
  // the metadata.
  nlohmann::json resultSparqlJSON;  // The expected result in SparqlJSON format.
  std::string resultXml;
};

// For a CONSTRUCT query, the `resultSize` of the QLever JSON is the number of
// results of the WHERE clause.
struct TestCaseConstructQuery {
  std::string kg;                   // The knowledge graph (TURTLE)
  std::string query;                // The query (SPARQL)
  uint64_t resultSizeTotal;         // The expected number of results,
                                    // including triples with UNDEF values.
  uint64_t resultSizeExported;      // The expected number of results exported.
  std::string resultTsv;            // The expected result in TSV format.
  std::string resultCsv;            // The expected result in CSV format.
  std::string resultTurtle;         // The expected result in Turtle format.
  std::string resultNtriples;       // The expected result in NTriples format.
  nlohmann::json resultQLeverJSON;  // The expected result in QLeverJSOn format.
                                    // Note: this member only contains the inner
                                    // result array with the bindings and NOT
                                    // the metadata.
  // How many triples the construct query contains.
  size_t numTriples = 1;
};

// Run a single test case for a SELECT query.
void runSelectQueryTestCase(
    const TestCaseSelectQuery& testCase, bool useTextIndex = false,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::sparqlResultsJsonWithTime_>(false);
  auto trace = generateLocationTrace(l, "runSelectQueryTestCase");
  using enum ad_utility::MediaType;
  EXPECT_EQ(
      runQueryStreamableResult(testCase.kg, testCase.query, tsv, useTextIndex),
      testCase.resultTsv);
  EXPECT_EQ(
      runQueryStreamableResult(testCase.kg, testCase.query, csv, useTextIndex),
      testCase.resultCsv);

  auto resultJSON = nlohmann::json::parse(runQueryStreamableResult(
      testCase.kg, testCase.query, qleverJson, useTextIndex));
  // TODO<joka921> Test other members of the JSON result (e.g. the selected
  // variables).
  ASSERT_EQ(resultJSON["query"], testCase.query);
  ASSERT_EQ(resultJSON["resultSizeTotal"], testCase.resultSize);
  ASSERT_EQ(resultJSON["resultSizeExported"], testCase.resultSize);
  EXPECT_EQ(resultJSON["res"], testCase.resultQLeverJSON);

  EXPECT_EQ(nlohmann::json::parse(runQueryStreamableResult(
                testCase.kg, testCase.query, sparqlJson, useTextIndex)),
            testCase.resultSparqlJSON);

  // TODO<joka921> Use this for proper testing etc.
  auto xmlAsString = runQueryStreamableResult(testCase.kg, testCase.query,
                                              sparqlXml, useTextIndex);
  EXPECT_EQ(testCase.resultXml, xmlAsString);

  // Test the interaction of normal limit (the LIMIT of the query) and export
  // limit (the value of the `send` parameter).
  for (uint64_t exportLimit{0}; exportLimit < uint64_t{4}; ++exportLimit) {
    auto resultJson = nlohmann::json::parse(runQueryStreamableResult(
        testCase.kg, testCase.query, qleverJson, useTextIndex, exportLimit));
    ASSERT_EQ(resultJson["resultSizeTotal"], testCase.resultSize);
    ASSERT_EQ(resultJson["resultSizeExported"],
              std::min(exportLimit, testCase.resultSize));
  }
}

// Run a single test case for a CONSTRUCT query.
void runConstructQueryTestCase(
    const TestCaseConstructQuery& testCase,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::sparqlResultsJsonWithTime_>(false);
  auto trace = generateLocationTrace(l, "runConstructQueryTestCase");
  using enum ad_utility::MediaType;
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, tsv),
            testCase.resultTsv);
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, csv),
            testCase.resultCsv);
  auto resultJson = nlohmann::json::parse(
      runQueryStreamableResult(testCase.kg, testCase.query, qleverJson));
  ASSERT_EQ(resultJson["query"], testCase.query);
  ASSERT_EQ(resultJson["resultSizeTotal"], testCase.resultSizeTotal);
  ASSERT_EQ(resultJson["resultSizeExported"], testCase.resultSizeExported);
  EXPECT_EQ(resultJson["res"], testCase.resultQLeverJSON);
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, turtle),
            testCase.resultTurtle);
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, ntriples),
            testCase.resultNtriples);

  // Test the interaction of normal limit (the LIMIT of the query) and export
  // limit (the value of the `send` parameter).
  for (uint64_t exportLimit{0}; exportLimit < uint64_t{4}; ++exportLimit) {
    auto resultJson = nlohmann::json::parse(runQueryStreamableResult(
        testCase.kg, testCase.query, qleverJson, false, exportLimit));
    ASSERT_EQ(resultJson["resultSizeTotal"], testCase.resultSizeTotal);
    ASSERT_EQ(resultJson["resultSizeExported"],
              std::min(exportLimit * testCase.numTriples,
                       testCase.resultSizeExported));
  }
}

// Run a single test case for an ASK query.
void runAskQueryTestCase(
    const TestCaseAskQuery& testCase,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l, "runAskQueryTestCase");
  using enum ad_utility::MediaType;
  // TODO<joka921> match the exception
  EXPECT_ANY_THROW(runQueryStreamableResult(testCase.kg, testCase.query, tsv));
  EXPECT_ANY_THROW(runQueryStreamableResult(testCase.kg, testCase.query, csv));
  EXPECT_ANY_THROW(
      runQueryStreamableResult(testCase.kg, testCase.query, octetStream));
  EXPECT_ANY_THROW(
      runQueryStreamableResult(testCase.kg, testCase.query, turtle));
  auto resultJson = nlohmann::json::parse(
      runQueryStreamableResult(testCase.kg, testCase.query, qleverJson));
  ASSERT_EQ(resultJson["query"], testCase.query);
  ASSERT_EQ(resultJson["resultSizeExported"], 1u);
  EXPECT_EQ(resultJson["res"], testCase.resultQLeverJSON);

  EXPECT_EQ(nlohmann::json::parse(runQueryStreamableResult(
                testCase.kg, testCase.query, sparqlJson)),
            testCase.resultSparqlJSON);

  auto xmlAsString =
      runQueryStreamableResult(testCase.kg, testCase.query, sparqlXml);
  EXPECT_EQ(testCase.resultXml, xmlAsString);
}

// Create a `json` that can be used as the `resultQLeverJSON` of a
// `TestCaseSelectQuery`. This function can only be used when there is a single
// variable in the result. The `values` then become the bindings of that
// variable.
nlohmann::json makeExpectedQLeverJSON(
    const std::vector<std::optional<std::string>>& values) {
  nlohmann::json j;
  for (const auto& value : values) {
    if (value.has_value()) {
      j.push_back(std::vector{value.value()});
    } else {
      j.emplace_back();
      j.back().push_back(nullptr);
    }
  }
  return j;
}

// Create a single binding in the `SparqlJSON` format from the given `datatype`
// `type`, `value` and `langtag`. `datatype` and `langtag` are not always
// present, so those arguments are of type `std::optional`.
nlohmann::json makeJSONBinding(
    const std::optional<std::string>& datatype, const std::string& type,
    const std::string& value,
    const std::optional<std::string>& langtag = std::nullopt) {
  std::unordered_map<std::string, std::string> m;
  if (datatype.has_value()) {
    m["datatype"] = datatype.value();
  }
  m["type"] = type;
  m["value"] = value;
  if (langtag.has_value()) {
    m["xml:lang"] = langtag.value();
  }
  return m;
}

// Create a `json` that can be used as the `resultSparqlJSON` member of a
// `TestCaseSelectQuery`. This function can only be used when there is a single
// variable called `?o` in the result. The `bindings` then become the bindings
// of that variable. These bindings are typically created via the
// `makeJSONBinding` function.
nlohmann::json makeExpectedSparqlJSON(
    const std::vector<nlohmann::json>& bindings) {
  nlohmann::json j;
  j["head"]["vars"].push_back("o");
  auto& res = j["results"]["bindings"];
  res = std::vector<std::string>{};
  for (const auto& binding : bindings) {
    res.emplace_back();
    res.back()["o"] = binding;
  }
  return j;
}

// Return a header of a SPARQL XML export including the given variables until
// the opening `<results>` tag.
static std::string makeXMLHeader(
    std::vector<std::string> varsWithoutQuestionMark) {
  std::string result = R"(<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
<head>)";
  for (const auto& var : varsWithoutQuestionMark) {
    absl::StrAppend(&result, "\n  <variable name=\"", var, R"("/>)");
  }
  absl::StrAppend(&result, "\n</head>\n<results>");
  return result;
}

// The end of a SPARQL XML export.
static const std::string xmlTrailer = "\n</results>\n</sparql>";

// Helper function for easier testing of the `IdTable` generator.
std::vector<IdTable> convertToVector(
    ad_utility::InputRangeTypeErased<TableConstRefWithVocab> generator) {
  std::vector<IdTable> result;
  for (const TableConstRefWithVocab& pair : generator) {
    result.push_back(pair.idTable().clone());
  }
  return result;
}

// match the contents of a `vector<IdTable>` to the given `tables`.
template <typename... Tables>
auto matchesIdTables(const Tables&... tables) {
  return ElementsAre(matchesIdTable(tables)...);
}

std::vector<IdTable> convertToVector(
    ad_utility::InputRangeTypeErased<TableWithRange> generator) {
  std::vector<IdTable> result;
  for (const auto& [pair, range] : generator) {
    const auto& idTable = pair.idTable();
    result.emplace_back(idTable.numColumns(), idTable.getAllocator());
    result.back().insertAtEnd(idTable, *range.begin(), *(range.end() - 1) + 1);
  }
  return result;
}

std::chrono::milliseconds toChrono(std::string_view string) {
  EXPECT_THAT(string, EndsWith("ms"));
  return ad_utility::ParseableDuration<std::chrono::milliseconds>::fromString(
      string);
}
}  // namespace

namespace {
// The requirements of Boost.Asio on the properties and function objects that
// `CountingExecutor` forwards to its underlying executor.
template <typename Property>
concept QueryableProperty =
    boost::asio::can_query_v<const ql::any_io_executor&, Property>;
template <typename Property>
concept RequirableProperty =
    boost::asio::can_require_v<const ql::any_io_executor&, Property>;
template <typename Property>
concept PreferableProperty =
    boost::asio::can_prefer_v<const ql::any_io_executor&, Property>;
template <typename Function>
concept ExecutableFunction = std::invocable<std::decay_t<Function>&>;

// An executor that runs its function objects on an underlying executor and
// counts them, so that a test can check whether an export submitted tasks.
class CountingExecutor {
  ql::any_io_executor executor_;
  std::shared_ptr<std::atomic<size_t>> numberOfTasks_;

 public:
  CountingExecutor(ql::any_io_executor executor,
                   std::shared_ptr<std::atomic<size_t>> numberOfTasks)
      : executor_{std::move(executor)},
        numberOfTasks_{std::move(numberOfTasks)} {}

  bool operator==(const CountingExecutor&) const = default;

  // All properties are forwarded to the underlying executor.
  template <QueryableProperty Property>
  decltype(auto) query(const Property& property) const {
    return boost::asio::query(executor_, property);
  }

  template <RequirableProperty Property>
  CountingExecutor require(const Property& property) const {
    return {boost::asio::require(executor_, property), numberOfTasks_};
  }

  template <PreferableProperty Property>
  CountingExecutor prefer(const Property& property) const {
    return {boost::asio::prefer(executor_, property), numberOfTasks_};
  }

  template <ExecutableFunction Function>
  void execute(Function&& function) const {
    ++*numberOfTasks_;
    executor_.execute(AD_FWD(function));
  }
};

// A `QueryExecutionContext` on the index of `getQec(config)` whose operations
// and parallel CSV or TSV export run on `multiThreadExecutor()` (4 threads).
// The contexts of `getQec` run on a single thread, on which the chunks of a
// parallel export would be serialized one after the other. Not copyable or
// movable, because `qec_` points to the caches.
struct MultiThreadQec : ad_utility::NoCopyNoMove {
  QueryResultCache cache_{};
  NamedResultCache namedCache_{};
  // The number of tasks that have been run on the executor of `qec_`.
  std::shared_ptr<std::atomic<size_t>> numberOfTasks_ =
      std::make_shared<std::atomic<size_t>>(0);
  QueryExecutionContext qec_;
  explicit MultiThreadQec(ad_utility::testing::TestIndexConfig config)
      // The index of `getQec` is never destroyed, so the `shared_ptr` does not
      // have to own it.
      : qec_{std::shared_ptr<const Index>{
                 std::shared_ptr<const Index>{},
                 &ad_utility::testing::getQec(std::move(config))->getIndex()},
             &cache_,
             ad_utility::testing::makeAllocator(
                 ad_utility::MemorySize::megabytes(100)),
             &namedCache_,
             std::make_shared<MaterializedViewsManager>(),
             CountingExecutor{ad_utility::testing::multiThreadExecutor(),
                              numberOfTasks_}} {}
};

// Export the result of the SPARQL `query` on `qec` as the `mediaType`, with or
// without `parallel-csv-tsv-export`.
std::string exportQuery(QueryExecutionContext& qec, const std::string& query,
                        ad_utility::MediaType mediaType, bool inParallel,
                        std::optional<size_t> exportLimit = std::nullopt) {
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::parallelCsvTsvExport_>(
          inParallel);
  return runQueryStreamableResult(&qec, query, mediaType, exportLimit);
}

// A knowledge graph with `numberOfSubjects` subjects `<s{i}>`, each with one
// literal of more than 400 bytes that needs escaping in CSV and TSV, and every
// third with a blank node (so that the other rows of an OPTIONAL are unbound).
ad_utility::testing::TestIndexConfig makeConfigWithManyRows(
    size_t numberOfSubjects) {
  std::string kg;
  const std::string padding(400, 'x');
  for (size_t i : ql::views::iota(size_t{0}, numberOfSubjects)) {
    absl::StrAppend(&kg, "<s", i, "> <p> \"o", i, ", \\\"q\\\"\\t", padding,
                    "\" .\n");
    if (i % 3 == 0) {
      absl::StrAppend(&kg, "<s", i, "> <q> _:b", i, " .\n");
    }
  }
  ad_utility::testing::TestIndexConfig config{std::move(kg)};
  // With the default of 2 triples per batch of the test indexes, the index
  // builder would keep more than the usual limit of 1,024 files open.
  config.additionalSettings = {{"num-triples-per-batch", "10000"}};
  // The parser constructs a new tokenizer (and compiles its regexes) for each
  // buffer, so the default of 1 kB for the test indexes would make the index
  // build of these 11 MB take very long in a debug build.
  config.parserBufferSize = ad_utility::MemorySize::megabytes(16);
  // The consistency check of the test indexes is quadratic in the number of
  // blocks for this many subjects, so use larger blocks than the default of 2
  // rows. The lazy scans of the tests still yield many blocks of fewer rows
  // than a chunk.
  config.rowsPerBlock = 1000;
  return config;
}

// The maximal number of rows of a chunk, and the number of subjects of the
// knowledge graph of the parallel export tests (two full chunks and a partial
// one).
constexpr uint64_t maxNumberOfRowsPerChunk =
    ExportQueryExecutionTrees::MAX_NUMBER_OF_ROWS_PER_CSV_OR_TSV_CHUNK;
constexpr size_t numberOfSubjectsForParallelExport =
    2 * maxNumberOfRowsPerChunk + 3616;
}  // namespace

// The serialization of one chunk: separators, unbound values and columns that
// are not in the result, early stop, cancellation, and the memory limit.
// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, csvOrTsvRows) {
  using ad_utility::testing::IntId;
  using Columns =
      std::vector<std::optional<QueryExecutionTree::VariableAndColumnIndex>>;
  using Rows = ql::ranges::iota_view<uint64_t, uint64_t>;
  const Index& index = ad_utility::testing::getQec()->getIndex();
  IdTable idTable = makeIdTableFromVector({{1, 2}, {3, 4}, {5, 6}}, IntId);
  idTable(0, 1) = Id::makeUndefined();
  LocalVocab localVocab;
  TableConstRefWithVocab table{idTable.asStaticView<0>(), localVocab};
  Columns columns{QueryExecutionTree::VariableAndColumnIndex{"?a", 0},
                  std::nullopt,
                  QueryExecutionTree::VariableAndColumnIndex{"?b", 1}};
  ad_utility::AllocatorWithLimit<char> allocator{
      ad_utility::testing::makeAllocator()};
  ad_utility::CancellationHandle<> cancellationHandle;
  std::atomic<bool> stop = false;
  using enum ad_utility::MediaType;
  auto tsvRows = [&](Rows rows) {
    return std::string{ExportQueryExecutionTrees::csvOrTsvRows<tsv>(
        index, columns, table, rows, allocator, cancellationHandle,
        ad_utility::AsyncTransformStopFlag{stop})};
  };
  EXPECT_EQ(tsvRows(Rows{0, 3}), "1\t\t\n3\t\t4\n5\t\t6\n");
  EXPECT_EQ(tsvRows(Rows{1, 3}), "3\t\t4\n5\t\t6\n");
  EXPECT_EQ(tsvRows(Rows{2, 2}), "");
  EXPECT_EQ(std::string{ExportQueryExecutionTrees::csvOrTsvRows<csv>(
                index, columns, table, Rows{0, 2}, allocator,
                cancellationHandle, ad_utility::AsyncTransformStopFlag{stop})},
            "1,,\n3,,4\n");

  // A row or a column index that is not valid for the table violates the
  // contract.
  EXPECT_THROW(tsvRows(Rows{0, 4}), ad_utility::Exception);
  columns.emplace_back(QueryExecutionTree::VariableAndColumnIndex{"?c", 5});
  EXPECT_THROW(tsvRows(Rows{0, 3}), ad_utility::Exception);
  columns.pop_back();

  // Once `stop` is set, no more rows are serialized.
  stop = true;
  EXPECT_EQ(tsvRows(Rows{0, 3}), "");
  stop = false;

  // The lines are allocated with the given allocator, so they count towards
  // its memory limit (100 rows take more than the 100 bytes of the limit).
  IdTable largeIdTable = makeIdTableFromVector(
      std::vector<std::vector<IntOrId>>(100, {1, 2}), IntId);
  TableConstRefWithVocab largeTable{largeIdTable.asStaticView<0>(), localVocab};
  ad_utility::AllocatorWithLimit<char> smallAllocator{
      ad_utility::testing::makeAllocator(ad_utility::MemorySize::bytes(100))};
  EXPECT_THROW(
      ExportQueryExecutionTrees::csvOrTsvRows<tsv>(
          index, columns, largeTable, Rows{0, 100}, smallAllocator,
          cancellationHandle, ad_utility::AsyncTransformStopFlag{stop}),
      ad_utility::detail::AllocationExceedsLimitException);

  cancellationHandle.cancel(ad_utility::CancellationState::MANUAL);
  EXPECT_THROW(tsvRows(Rows{0, 3}), ad_utility::CancellationException);
}

// The parallel CSV and TSV export on four threads gives the same output as the
// sequential export, which converts and yields one cell after the other as
// before `parallel-csv-tsv-export` existed. `ORDER BY DESC(...)` forces an
// `OrderBy`, whose result is a single block, so each of these results is cut
// into balanced chunks of at most `maxNumberOfRowsPerChunk` rows: the whole
// result (three chunks), exactly one chunk (serialized sequentially), one chunk
// plus one row (two chunks), exactly two chunks, a block whose rows start at
// row 5000 (OFFSET), one row, and no row. Without ORDER BY, the result is a
// lazy scan of many small blocks, each of which is serialized sequentially.
// The export limit cuts the larger results to one chunk plus 808 rows (two
// chunks). With `max-num-threads-per-query` = 1, only two chunks are in
// flight. The number of tasks on the executor shows whether the parallel path
// is taken.
// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ParallelCsvTsvExport) {
  auto config = makeConfigWithManyRows(numberOfSubjectsForParallelExport);
  QueryExecutionContext& sequentialQec = *ad_utility::testing::getQec(config);
  MultiThreadQec parallel{config};
  using enum ad_utility::MediaType;
  const std::string orderedScan =
      "SELECT ?o ?s WHERE { ?s <p> ?o } ORDER BY DESC(?o)";
  for (const std::string& query : {
           "SELECT ?s ?o ?b WHERE { ?s <p> ?o OPTIONAL { ?s <q> ?b } } "
           "ORDER BY DESC(?s)"s,
           absl::StrCat(orderedScan, " LIMIT ", maxNumberOfRowsPerChunk),
           absl::StrCat(orderedScan, " LIMIT ", maxNumberOfRowsPerChunk + 1),
           absl::StrCat(orderedScan, " LIMIT ", 2 * maxNumberOfRowsPerChunk),
           absl::StrCat(orderedScan, " OFFSET 5000 LIMIT 10000"),
           absl::StrCat(orderedScan, " LIMIT 1"),
           absl::StrCat(orderedScan, " LIMIT 0"),
           "SELECT ?s WHERE { ?s <p> <nonexistent> }"s,
           "SELECT ?s ?o WHERE { ?s <p> ?o }"s,
       }) {
    for (auto mediaType : {csv, tsv}) {
      auto trace = generateLocationTrace(AD_CURRENT_SOURCE_LOC(), query);
      auto sequential = exportQuery(sequentialQec, query, mediaType, false);
      EXPECT_EQ(exportQuery(parallel.qec_, query, mediaType, true), sequential);
      const size_t exportLimit = maxNumberOfRowsPerChunk + 808;
      EXPECT_EQ(
          exportQuery(parallel.qec_, query, mediaType, true, exportLimit),
          exportQuery(sequentialQec, query, mediaType, false, exportLimit));
      auto oneThread = setRuntimeParameterForTest<
          &RuntimeParameters::maxNumThreadsPerQuery_>(1);
      EXPECT_EQ(exportQuery(parallel.qec_, query, mediaType, true), sequential);
    }
  }
  // The number of tasks that an export adds to those of the operations.
  auto numberOfExportTasks = [&parallel](const std::string& query) {
    auto numberOfTasks = [&parallel, &query](bool inParallel) {
      size_t before = *parallel.numberOfTasks_;
      exportQuery(parallel.qec_, query, csv, inParallel);
      return *parallel.numberOfTasks_ - before;
    };
    size_t sequential = numberOfTasks(false);
    return numberOfTasks(true) - sequential;
  };
  EXPECT_EQ(numberOfExportTasks(orderedScan), 3);
  EXPECT_EQ(numberOfExportTasks(
                absl::StrCat(orderedScan, " LIMIT ", maxNumberOfRowsPerChunk)),
            0);
  EXPECT_EQ(numberOfExportTasks(absl::StrCat(orderedScan, " LIMIT ",
                                             maxNumberOfRowsPerChunk + 1)),
            2);

  // The header line plus one line per subject.
  EXPECT_EQ(
      ql::ranges::count(
          exportQuery(parallel.qec_,
                      "SELECT ?s ?o WHERE { ?s <p> ?o } ORDER BY DESC(?s)", csv,
                      true),
          '\n'),
      numberOfSubjectsForParallelExport + 1);
}

// A lazy result with several blocks of more than one chunk: the view of the
// parallel export is rebuilt for each block, and OFFSET and LIMIT cut across
// block boundaries.
// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ParallelCsvTsvExportOfSeveralLargeBlocks) {
  using ad_utility::testing::IntId;
  MultiThreadQec parallel{ad_utility::testing::TestIndexConfig{}};
  // Blocks of 9000, 20000, 3 and 17000 rows with the row number in the first
  // column and an unbound value in every seventh row of the second column.
  auto makeBlocks = []() {
    std::vector<IdTable> blocks;
    int64_t rowNumber = 0;
    for (size_t numberOfRows : {9000, 20000, 3, 17000}) {
      IdTable block{2, ad_utility::testing::makeAllocator()};
      for ([[maybe_unused]] size_t i :
           ql::views::iota(size_t{0}, numberOfRows)) {
        block.push_back({IntId(rowNumber), rowNumber % 7 == 0
                                               ? Id::makeUndefined()
                                               : IntId(-rowNumber)});
        ++rowNumber;
      }
      blocks.push_back(std::move(block));
    }
    return blocks;
  };
  auto exportBlocks = [&](std::string query, ad_utility::MediaType mediaType,
                          bool inParallel) {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::parallelCsvTsvExport_>(
            inParallel);
    auto qet = ad_utility::makeExecutionTree<ValuesForTesting>(
        &parallel.qec_, makeBlocks(),
        std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}},
        true);
    parallel.qec_.clearCacheUnpinnedOnly();
    auto pq = parseQuery(std::move(query));
    ad_utility::Timer timer{ad_utility::Timer::Started};
    std::string result;
    for (const auto& batch : ExportQueryExecutionTrees::computeResult(
             pq, *qet, mediaType, timer,
             std::make_shared<ad_utility::CancellationHandle<>>())) {
      result += batch;
    }
    return result;
  };
  using enum ad_utility::MediaType;
  for (std::string query : {
           "SELECT ?a ?b WHERE { ?a ?b ?c }",
           "SELECT ?b ?a WHERE { ?a ?b ?c } OFFSET 8000 LIMIT 25000",
       }) {
    for (auto mediaType : {csv, tsv}) {
      auto trace = generateLocationTrace(AD_CURRENT_SOURCE_LOC(), query);
      auto sequential = exportBlocks(query, mediaType, false);
      EXPECT_EQ(exportBlocks(query, mediaType, true), sequential);
    }
  }
  EXPECT_EQ(
      ql::ranges::count(
          exportBlocks("SELECT ?a ?b WHERE { ?a ?b ?c }", tsv, true), '\n'),
      9000 + 20000 + 3 + 17000 + 1);
}

// A cancellation while the chunks of a parallel CSV export are serialized is
// thrown by the consumer or a worker. The consumer of `computeResult` sees what
// the server sends: the output so far, followed by an error message (see
// `convertStreamGeneratorForChunkedTransfer`). A consumer that stops early (as
// on a client disconnect) can destroy the export while chunks are in flight.
// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ParallelCsvExportCancellationAndEarlyStop) {
  auto config = makeConfigWithManyRows(numberOfSubjectsForParallelExport);
  MultiThreadQec parallel{config};
  auto inParallel =
      setRuntimeParameterForTest<&RuntimeParameters::parallelCsvTsvExport_>(
          true);
  // Two chunks in flight, so the third chunk (and with it the end of the
  // export) is only serialized after the first one has been consumed.
  auto oneThread =
      setRuntimeParameterForTest<&RuntimeParameters::maxNumThreadsPerQuery_>(1);
  std::string query = "SELECT ?s ?o WHERE { ?s <p> ?o } ORDER BY DESC(?s)";
  const std::string completeExport = runQueryStreamableResult(
      &parallel.qec_, query, ad_utility::MediaType::csv);
  auto pq = parseQuery(query);
  ad_utility::Timer timer{ad_utility::Timer::Started};
  auto startExport = [&parallel, &pq,
                      &timer](ad_utility::SharedCancellationHandle handle) {
    QueryPlanner qp{&parallel.qec_, handle};
    auto qet = qp.createExecutionTree(pq);
    return std::pair{qet, ExportQueryExecutionTrees::computeResult(
                              pq, *qet, ad_utility::MediaType::csv, timer,
                              std::move(handle))};
  };

  // Each chunk has more than 2 MB (6667 rows of more than 400 bytes), so the
  // first batch of 1 MB of the export ends inside the first chunk, while the
  // second chunk is in flight. After the cancellation, either the consumer
  // (before it yields the second chunk) or the task of the third chunk (which
  // is submitted only then) throws. Either way, the consumer receives the
  // output so far followed by the error message.
  {
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    auto [qet, generator] = startExport(handle);
    auto it = generator.begin();
    ASSERT_TRUE(it != generator.end());
    std::string output = *it;
    handle->cancel(ad_utility::CancellationState::MANUAL);
    for (++it; it != generator.end(); ++it) {
      output += *it;
    }
    EXPECT_THAT(output, ::testing::HasSubstr("!!!!>># An error has occurred"));
    EXPECT_THAT(output, ::testing::HasSubstr("cancelled"));
    EXPECT_LT(output.size(), completeExport.size());
  }

  // Stop after the first batch; destroying the generator stops the chunks in
  // flight and waits for them (see `AsyncTransformView`'s test
  // `stopIsRequestedOnDestruction`).
  {
    auto [qet, generator] =
        startExport(std::make_shared<ad_utility::CancellationHandle<>>());
    auto it = generator.begin();
    ASSERT_TRUE(it != generator.end());
    EXPECT_THAT(*it, ::testing::StartsWith("s,o\n"));
  }
}

// The parallel TSV export of more than one chunk of text records (which are
// read from the text index by the workers) gives the same output as the
// sequential export.
// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ParallelTsvExportOfTextRecords) {
  constexpr size_t numberOfTextRecords = maxNumberOfRowsPerChunk + 808;
  std::string wordsFile;
  std::string docsFile;
  for (size_t i : ql::views::iota(size_t{1}, numberOfTextRecords + 1)) {
    absl::StrAppend(&wordsFile,
                    createWordsFileLineAsString("alpha", false, i, 1),
                    createWordsFileLineAsString("<s>", true, i, 1));
    absl::StrAppend(&docsFile,
                    createDocsFileLineAsString(
                        i, absl::StrCat("record ", i, " said \"alpha\"")));
  }
  ad_utility::testing::TestIndexConfig config{"<s> <p> <o> ."};
  config.createTextIndex = true;
  config.contentsOfWordsFileAndDocsfile =
      std::pair{std::move(wordsFile), std::move(docsFile)};
  MultiThreadQec parallel{config};
  std::string query =
      "SELECT ?t WHERE { ?t ql:contains-entity <s> . ?t ql:contains-word "
      "\"alph*\" } ORDER BY DESC(?t)";
  auto sequential = exportQuery(*ad_utility::testing::getQec(config), query,
                                ad_utility::MediaType::tsv, false);
  EXPECT_EQ(ql::ranges::count(sequential, '\n'), numberOfTextRecords + 1);
  EXPECT_EQ(exportQuery(parallel.qec_, query, ad_utility::MediaType::tsv, true),
            sequential);
}

TEST(ExportQueryExecutionTrees, Integers) {
  std::string kg =
      "<s> <p> 42 . <s> <p> -42019234865781 . <s> <p> 4012934858173560";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#int">-42019234865781</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#int">42</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#int">4012934858173560</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 3,
      // TSV
      "?o\n"
      "-42019234865781\n"
      "42\n"
      "4012934858173560\n",
      // CSV
      "o\n"
      "-42019234865781\n"
      "42\n"
      "4012934858173560\n",
      makeExpectedQLeverJSON(
          {"\"-42019234865781\"^^<http://www.w3.org/2001/XMLSchema#int>"s,
           "\"42\"^^<http://www.w3.org/2001/XMLSchema#int>"s,
           "\"4012934858173560\"^^<http://www.w3.org/2001/XMLSchema#int>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal",
                           "-42019234865781"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal",
                           "42"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal",
                           "4012934858173560")}),
      expectedXml};
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg, "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 3, 3,
      // TSV
      "<s>\t<p>\t-42019234865781\n"
      "<s>\t<p>\t42\n"
      "<s>\t<p>\t4012934858173560\n",
      // CSV
      "<s>,<p>,-42019234865781\n"
      "<s>,<p>,42\n"
      "<s>,<p>,4012934858173560\n",
      // Turtle
      "<s> <p> -42019234865781 .\n"
      "<s> <p> 42 .\n"
      "<s> <p> 4012934858173560 .\n",
      // NTriples
      "<s> <p> \"-42019234865781\"^^<http://www.w3.org/2001/XMLSchema#int> "
      ".\n"
      "<s> <p> \"42\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
      "<s> <p> \"4012934858173560\"^^<http://www.w3.org/2001/XMLSchema#int> "
      ".\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "-42019234865781"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "42"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "4012934858173560"s});
        return j;
      }()};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, Bool) {
  std::string kg =
      "<s> <p> true . <s> <p> false ."
      " <s2> <p2> \"1\"^^<http://www.w3.org/2001/XMLSchema#boolean> ."
      " <s2> <p2> \"0\"^^<http://www.w3.org/2001/XMLSchema#boolean> .";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";

  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#boolean">false</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#boolean">0</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#boolean">true</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#boolean">1</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 4,
      // TSV
      "?o\n"
      "false\n"
      "0\n"
      "true\n"
      "1\n",
      // CSV
      "o\n"
      "false\n"
      "0\n"
      "true\n"
      "1\n",
      makeExpectedQLeverJSON(
          {"\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s,
           "\"0\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s,
           "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s,
           "\"1\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#boolean",
                           "literal", "false"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#boolean",
                           "literal", "0"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#boolean",
                           "literal", "true"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#boolean",
                           "literal", "1")}),
      expectedXml};
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg, "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 4, 4,
      // TSV
      "<s>\t<p>\tfalse\n"
      "<s2>\t<p2>\t\"0\"^^<http://www.w3.org/2001/XMLSchema#boolean>\n"
      "<s>\t<p>\ttrue\n"
      "<s2>\t<p2>\t\"1\"^^<http://www.w3.org/2001/XMLSchema#boolean>\n",
      // CSV
      "<s>,<p>,false\n"
      "<s2>,<p2>,\"\"\"0\"\"^^<http://www.w3.org/2001/XMLSchema#boolean>\"\n"
      "<s>,<p>,true\n"
      "<s2>,<p2>,\"\"\"1\"\"^^<http://www.w3.org/2001/XMLSchema#boolean>\"\n",
      // Turtle
      "<s> <p> false .\n"
      "<s2> <p2> \"0\"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n"
      "<s> <p> true .\n"
      "<s2> <p2> \"1\"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n",
      // Ntriples
      "<s> <p> \"false\"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n"
      "<s2> <p2> \"0\"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n"
      "<s> <p> \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n"
      "<s2> <p2> \"1\"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "false"s});
        j.push_back(
            std::vector{"<s2>"s, "<p2>"s,
                        "\"0\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "true"s});
        j.push_back(
            std::vector{"<s2>"s, "<p2>"s,
                        "\"1\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s});
        return j;
      }()};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, UnusedVariable) {
  std::string kg = "<s> <p> true . <s> <p> false.";
  std::string query = "SELECT ?o WHERE {?s ?p ?x} ORDER BY ?s";
  std::string expectedXml = makeXMLHeader({"o"}) + R"(
  <result>
  </result>
  <result>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 2,
      // TSV
      "?o\n"
      "\n"
      "\n",
      // CSV
      "o\n"
      "\n"
      "\n",
      makeExpectedQLeverJSON({std::nullopt, std::nullopt}),
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("o");
        j["results"]["bindings"].push_back({});
        j["results"]["bindings"].push_back({});
        return j;
      }(),
      expectedXml};
  runSelectQueryTestCase(testCase);

  // The `2` is the number of results including triples with UNDEF values. The
  // `0` is the number of results excluding such triples.
  TestCaseConstructQuery testCaseConstruct{
      kg, "CONSTRUCT {?x ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 2, 0,
      // TSV
      "",
      // CSV
      "",
      // Turtle
      "",
      // Ntriples
      "", []() { return nlohmann::json::parse("[]"); }()};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, Floats) {
  // The large double is `-42019234865.781e12` with the lowest 4 bits of the
  // mantissa cleared, so the legacy `Id` (which drops these bits) and the split
  // `Id` (which keeps them) export it identically.
  std::string kg =
      "<s> <p> 42.2 . <s> <p> -42019234865780982022144e0 ."
      " <s> <p> 100.0 . <s> <p> 960000.06 ."
      " <s> <p> 123456.00000001 . <s> <p> 1e-10 ."
      " <s> <p> 4.012934858173560e-12 ."
      " <s> <p> \"NaN\"^^<http://www.w3.org/2001/XMLSchema#double> ."
      " <s> <p> \"INF\"^^<http://www.w3.org/2001/XMLSchema#double> ."
      " <s> <p> \"-INF\"^^<http://www.w3.org/2001/XMLSchema#double> .";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";

  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#double">-INF</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">-42019234865780982022144.0</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">4.012934858174e-12</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">1e-10</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">42.2</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">100.0</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">123456.0</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">960000.06</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#double">INF</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#double">NaN</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseFloat{
      kg, query, 10,
      // TSV
      "?o\n"
      "-INF\n"
      "-42019234865780982022144.0\n"
      "4.012934858174e-12\n"
      "1e-10\n"
      "42.2\n"
      "100.0\n"
      "123456.0\n"
      "960000.06\n"
      "INF\n"
      "NaN\n",
      // CSV
      "o\n"
      "-INF\n"
      "-42019234865780982022144.0\n"
      "4.012934858174e-12\n"
      "1e-10\n"
      "42.2\n"
      "100.0\n"
      "123456.0\n"
      "960000.06\n"
      "INF\n"
      "NaN\n",
      makeExpectedQLeverJSON(
          {"\"-INF\"^^<http://www.w3.org/2001/XMLSchema#double>"s,
           "\"-42019234865780982022144.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"4.012934858174e-12\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"1e-10\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"42.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"100.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"123456.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"960000.06\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"INF\"^^<http://www.w3.org/2001/XMLSchema#double>"s,
           "\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#double", "literal",
                           "-INF"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "-42019234865780982022144.0"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "4.012934858174e-12"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "1e-10"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "42.2"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "100.0"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "123456.0"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "960000.06"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#double", "literal",
                           "INF"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#double", "literal",
                           "NaN")}),
      expectedXml};
  runSelectQueryTestCase(testCaseFloat);

  TestCaseConstructQuery testCaseConstruct{
      kg, "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 10, 10,
      // TSV
      "<s>\t<p>\t\"-INF\"^^<http://www.w3.org/2001/XMLSchema#double>\n"
      "<s>\t<p>\t-42019234865780982022144.0\n"
      "<s>\t<p>\t4.012934858174e-12\n"
      "<s>\t<p>\t1e-10\n"
      "<s>\t<p>\t42.2\n"
      "<s>\t<p>\t100.0\n"
      "<s>\t<p>\t123456.0\n"
      "<s>\t<p>\t960000.06\n"
      "<s>\t<p>\t\"INF\"^^<http://www.w3.org/2001/XMLSchema#double>\n"
      "<s>\t<p>\t\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double>\n",
      // CSV
      "<s>,<p>,\"\"\"-INF\"\"^^<http://www.w3.org/2001/XMLSchema#double>\"\n"
      "<s>,<p>,-42019234865780982022144.0\n"
      "<s>,<p>,4.012934858174e-12\n"
      "<s>,<p>,1e-10\n"
      "<s>,<p>,42.2\n"
      "<s>,<p>,100.0\n"
      "<s>,<p>,123456.0\n"
      "<s>,<p>,960000.06\n"
      "<s>,<p>,\"\"\"INF\"\"^^<http://www.w3.org/2001/XMLSchema#double>\"\n"
      "<s>,<p>,\"\"\"NaN\"\"^^<http://www.w3.org/2001/XMLSchema#double>\"\n",
      // Turtle
      "<s> <p> \"-INF\"^^<http://www.w3.org/2001/XMLSchema#double> .\n"
      "<s> <p> -42019234865780982022144.0 .\n"
      "<s> <p> 4.012934858174e-12 .\n"
      "<s> <p> 1e-10 .\n"
      "<s> <p> 42.2 .\n"
      "<s> <p> 100.0 .\n"
      "<s> <p> 123456.0 .\n"
      "<s> <p> 960000.06 .\n"
      "<s> <p> \"INF\"^^<http://www.w3.org/2001/XMLSchema#double> .\n"
      "<s> <p> \"NaN\"^^<http://www.w3.org/2001/XMLSchema#double> .\n",
      // N-Triples
      "<s> <p> \"-INF\"^^<http://www.w3.org/2001/XMLSchema#double> .\n"
      "<s> <p> "
      "\"-42019234865780982022144.0\"^^<http://www.w3.org/2001/"
      "XMLSchema#decimal> .\n"
      "<s> <p> "
      "\"4.012934858174e-12\"^^<http://www.w3.org/2001/XMLSchema#decimal> "
      ".\n"
      "<s> <p> \"1e-10\"^^<http://www.w3.org/2001/XMLSchema#decimal> .\n"
      "<s> <p> \"42.2\"^^<http://www.w3.org/2001/XMLSchema#decimal> .\n"
      "<s> <p> \"100.0\"^^<http://www.w3.org/2001/XMLSchema#decimal> .\n"
      "<s> <p> \"123456.0\"^^<http://www.w3.org/2001/XMLSchema#decimal> .\n"
      "<s> <p> \"960000.06\"^^<http://www.w3.org/2001/XMLSchema#decimal> "
      ".\n"
      "<s> <p> \"INF\"^^<http://www.w3.org/2001/XMLSchema#double> .\n"
      "<s> <p> \"NaN\"^^<http://www.w3.org/2001/XMLSchema#double> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{
            "<s>"s, "<p>"s,
            "\"-INF\"^^<http://www.w3.org/2001/XMLSchema#double>"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "-42019234865780982022144.0"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "4.012934858174e-12"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "1e-10"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "42.2"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "100.0"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "123456.0"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "960000.06"s});
        j.push_back(
            std::vector{"<s>"s, "<p>"s,
                        "\"INF\"^^<http://www.w3.org/2001/XMLSchema#double>"s});
        j.push_back(
            std::vector{"<s>"s, "<p>"s,
                        "\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double>"s});
        return j;
      }()};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, Dates) {
  std::string kg =
      "<s> <p> "
      "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>.";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#dateTime">1950-01-01T00:00:00</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 1,
      // TSV
      "?o\n"
      "1950-01-01T00:00:00\n",
      // should be
      // "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>\n",
      // but that is a bug in the TSV export for another PR. Note: the duplicate
      // quotes are due to the escaping for CSV.
      "o\n"
      "1950-01-01T00:00:00\n",
      makeExpectedQLeverJSON(
          {"\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#dateTime",
                           "literal", "1950-01-01T00:00:00")}),
      expectedXml};
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      1,
      // TSV
      "<s>\t<p>\t\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/"
      "XMLSchema#dateTime>\n",  // missing
                                // "^^<http://www.w3.org/2001/XMLSchema#dateTime>\n",
      // CSV
      // TODO<joka921> This format is wrong, but this is is due to the way that
      // CONSTRUCT queries are currently exported. This has to be fixed in a
      // different PR.
      "<s>,<p>,\"\"\"1950-01-01T00:00:00\"\"^^<http://www.w3.org/2001/"
      "XMLSchema#dateTime>\"\n",
      // Turtle
      "<s> <p> "
      "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> "
      ".\n",
      // N-Triples
      "<s> <p> "
      "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> "
      ".\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{
            "<s>"s, "<p>"s,
            "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, GeoPoints) {
  std::string kg =
      "<s> <p> "
      "\"POINT(50.0 "
      "50.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>.";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.opengis.net/ont/geosparql#wktLiteral">POINT(50.000000 50.000000)</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 1,
      // TSV
      "?o\n"
      "POINT(50.000000 50.000000)\n",
      // should be
      // "\"POINT(50.000000 50.000000)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>\n",
      // but that is a bug in the TSV export for another PR. Note: the duplicate
      // quotes are due to the escaping for CSV.
      "o\n"
      "POINT(50.000000 50.000000)\n",
      makeExpectedQLeverJSON(
          {"\"POINT(50.000000 50.000000)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.opengis.net/ont/geosparql#wktLiteral",
                           "literal", "POINT(50.000000 50.000000)")}),
      expectedXml};
  runSelectQueryTestCase(testCase);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, Entities) {
  std::string kg = "PREFIX qlever: <http://qlever.com/> \n <s> <p> qlever:o";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><uri>http://qlever.com/o</uri></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 1,
      // TSV
      "?o\n"
      "<http://qlever.com/o>\n",
      // CSV
      "o\n"
      "http://qlever.com/o\n",
      makeExpectedQLeverJSON({"<http://qlever.com/o>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding(std::nullopt, "uri", "http://qlever.com/o")}),
      expectedXml};
  runSelectQueryTestCase(testCase);
  testCase.kg = "<s> <x> <y>";
  testCase.query =
      "PREFIX qlever: <http://qlever.com/> \n SELECT ?o WHERE {VALUES ?o "
      "{qlever:o}} ORDER BY ?o";
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      1,
      // TSV
      "<s>\t<p>\t<http://qlever.com/o>\n",
      // CSV
      "<s>,<p>,<http://qlever.com/o>\n",
      // Turtle
      "<s> <p> <http://qlever.com/o> .\n",
      // N-triples
      "<s> <p> <http://qlever.com/o> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "<http://qlever.com/o>"s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, LiteralWithLanguageTag) {
  std::string kg = R"(<s> <p> "Some\"Where	Over,"@en-ca.)";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal xml:lang="en-ca">Some&quot;Where)" +
                            "\t" + R"(Over,</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 1,
      // TSV
      "?o\n"
      "\"Some\"Where Over,\"@en-ca\n",
      // CSV
      "o\n"
      "\"Some\"\"Where\tOver,\"\n",
      makeExpectedQLeverJSON({"\"Some\"Where\tOver,\"@en-ca"s}),
      makeExpectedSparqlJSON({makeJSONBinding(std::nullopt, "literal",
                                              "Some\"Where\tOver,", "en-ca")}),
      expectedXml};
  runSelectQueryTestCase(testCase);
  testCase.kg = "<s> <x> <y>";
  testCase.query =
      "SELECT ?o WHERE { VALUES ?o {\"\"\"Some\"Where\tOver,\"\"\"@en-ca}} "
      "ORDER BY ?o";
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      1,
      // TSV
      "<s>\t<p>\t\"Some\"Where Over,\"@en-ca\n",
      // CSV
      "<s>,<p>,\"\"\"Some\"\"Where\tOver,\"\"@en-ca\"\n",
      // Turtle
      "<s> <p> \"Some\\\"Where\tOver,\"@en-ca .\n",
      // N-Triples
      "<s> <p> \"Some\\\"Where\tOver,\"@en-ca .\n",
      []() {
        nlohmann::json j;
        j.push_back(
            std::vector{"<s>"s, "<p>"s, "\"Some\"Where\tOver,\"@en-ca"s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, LiteralWithDatatype) {
  std::string kg = "<s> <p> \"something\"^^<www.example.org/bim>";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="www.example.org/bim">something</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 1,
      // TSV
      "?o\n"
      "\"something\"^^<www.example.org/bim>\n",
      // CSV
      "o\n"
      "something\n",
      makeExpectedQLeverJSON({"\"something\"^^<www.example.org/bim>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("www.example.org/bim", "literal", "something")}),
      expectedXml};
  runSelectQueryTestCase(testCase);
  testCase.kg = "<s> <x> <y>";
  testCase.query =
      "SELECT ?o WHERE { VALUES ?o {\"something\"^^<www.example.org/bim>}} "
      "ORDER BY ?o";
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      1,
      // TSV
      "<s>\t<p>\t\"something\"^^<www.example.org/bim>\n",
      // CSV
      "<s>,<p>,\"\"\"something\"\"^^<www.example.org/bim>\"\n",
      // Turtle
      "<s> <p> \"something\"^^<www.example.org/bim> .\n",
      // N-Triples
      "<s> <p> \"something\"^^<www.example.org/bim> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s,
                                "\"something\"^^<www.example.org/bim>"s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, LiteralPlain) {
  std::string kg = "<s> <p> \"something\"";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal>something</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{kg, query, 1,
                               // TSV
                               "?o\n"
                               "\"something\"\n",
                               // CSV
                               "o\n"
                               "something\n",
                               makeExpectedQLeverJSON({"\"something\""s}),
                               makeExpectedSparqlJSON({makeJSONBinding(
                                   std::nullopt, "literal", "something")}),
                               expectedXml};
  runSelectQueryTestCase(testCase);
  testCase.kg = "<s> <x> <y>";
  testCase.query =
      "SELECT ?o WHERE { VALUES ?o {\"something\"}} "
      "ORDER BY ?o";
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      1,
      // TSV
      "<s>\t<p>\t\"something\"\n",
      // CSV
      "<s>,<p>,\"\"\"something\"\"\"\n",
      // Turtle
      "<s> <p> \"something\" .\n",
      // N-Triples
      "<s> <p> \"something\" .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "\"something\""s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, TestWithIriEscaped) {
  std::string kg = "<s> <p> <https://\\u0009:\\u0020)\\u000AtestIriKg>";
  std::string objectQuery = "SELECT ?o WHERE { ?s ?p ?o }";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><uri>https://)" +
                            "\x09" + R"(: )
testIriKg</uri></binding>
  </result>)" + xmlTrailer;

  TestCaseSelectQuery testCaseTextIndex{
      kg, objectQuery, 1,
      // TSV
      "?o\n"
      "<https:// : )\\ntestIriKg>\n",
      // CSV
      "o\n"
      "\"https://\t: )\ntestIriKg\"\n",
      makeExpectedQLeverJSON({"<https://\t: )\ntestIriKg>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding(std::nullopt, "uri", "https://\t: )\ntestIriKg")}),
      expectedXml};
  runSelectQueryTestCase(testCaseTextIndex);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      1,
      // TSV
      "<s>\t<p>\t<https:// : )\\ntestIriKg>\n",
      // CSV
      "<s>,<p>,\"<https://\t: )\ntestIriKg>\"\n",
      // Turtle
      "<s> <p> <https://\t: )\ntestIriKg> .\n",
      // N-Triples
      "<s> <p> <https://\t: )\ntestIriKg> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "<https://\t: )\ntestIriKg>"s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, TestWithIriExtendedEscaped) {
  std::string kg =
      "<s> <p>"
      "<iriescaped\\u0001o\\u0002e\\u0003i\\u0004o\\u0005u\\u0006e\\u00"
      "07g\\u0008c\\u0009u\\u000Ae\\u000Be\\u000Ca\\u000Dd\\u000En\\u000F?"
      "\\u0010u\\u0011u\\u0012u\\u0013###\\u0020d>";
  std::string objectQuery = "SELECT ?o WHERE { ?s ?p ?o }";
  std::string expectedXml =
      makeXMLHeader({"o"}) +
      R"(
  <result>
    <binding name="o"><uri>)" +
      "iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc\tu\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d" +
      R"(</uri></binding>
  </result>)" +
      xmlTrailer;

  TestCaseSelectQuery testCaseTextIndex{
      kg, objectQuery, 1,
      // TSV
      "?o\n"
      "<iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc u\\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d>\n",
      // CSV
      "o\n"
      "\"iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc\tu\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d\"\n",
      makeExpectedQLeverJSON(
          {"<iriescaped\u0001o\u0002e\u0003i\u0004o\u0005u\u0006e\u0007"
           "g\u0008c\u0009u\u000Ae\u000Be\u000Ca\u000Dd\u000En\u000F?"
           "\u0010u\u0011u\u0012u\u0013### d>"s}),
      makeExpectedSparqlJSON({makeJSONBinding(
          std::nullopt, "uri",
          "iriescaped\u0001o\u0002e\u0003i\u0004o\u0005u\u0006e"
          "\u0007"
          "g\u0008c\u0009u\u000Ae\u000Be\u000Ca\u000Dd\u000En\u000F?"
          "\u0010u\u0011u\u0012u\u0013### d")}),
      expectedXml};
  runSelectQueryTestCase(testCaseTextIndex);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      1,
      // TSV
      "<s>\t<p>\t<iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc u\\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d>\n",
      // CSV
      "<s>,<p>,\"<iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc\tu\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d>\"\n",
      // Turtle
      "<s> <p> <iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc\tu\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d> .\n",
      // N-Triples
      "<s> <p> <iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc\tu\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{
            "<s>"s, "<p>"s,
            "<iriescaped\x01o\x02"
            "e\x03i\x04o\x05u\x06"
            "e\ag\bc\tu\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d>"s});
        return j;
      }(),
  };
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, TestIriWithEscapedIriString) {
  std::string kg = "<s> <p> \" hallo\\n\\t welt\"";
  std::string objectQuery =
      "SELECT ?o WHERE { "
      "BIND(IRI(\" hallo\\n\\t welt\") AS ?o) }";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><uri> hallo
)" + "\t" + R"( welt</uri></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseTextIndex{
      kg, objectQuery, 1,
      // TSV
      "?o\n"
      "< hallo\\n  welt>\n",
      // CSV
      "o\n"
      "\" hallo\n\t welt\"\n",
      makeExpectedQLeverJSON({"< hallo\n\t welt>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding(std::nullopt, "uri", " hallo\n\t welt")}),
      expectedXml};
  runSelectQueryTestCase(testCaseTextIndex);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      1,
      // TSV
      "<s>\t<p>\t\" hallo\\n  welt\"\n",
      // CSV
      "<s>,<p>,\"\"\" hallo\n\t welt\"\"\"\n",
      // Turtle
      "<s> <p> \" hallo\\n\t welt\" .\n",
      // N-Triples
      "<s> <p> \" hallo\\n\t welt\" .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "\" hallo\n\t welt\""s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, UndefinedValues) {
  std::string kg = "<s> <p> <o>";
  std::string query =
      "SELECT ?o WHERE {?s <p> <o> OPTIONAL {?s <p2> ?o}} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg,
      query,
      1,
      "?o\n\n",
      "o\n\n",
      nlohmann::json{std::vector{std::vector{nullptr}}},
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("o");
        j["results"]["bindings"].push_back(nlohmann::json::object());
        return j;
      }(),
      expectedXml};
  runSelectQueryTestCase(testCase);

  // The `1` is the number of results including triples with UNDEF values. The
  // `0` is the number of results excluding such triples.
  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s <pred> ?o} WHERE {?s <p> <o> OPTIONAL {?s <p2> ?o}} ORDER "
      "BY ?o",
      1,
      0,
      "",
      "",
      "",
      "",
      std::vector<std::string>{}};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, EmptyLines) {
  std::string kg = "<s> <p> <o>";
  std::string query = "SELECT * WHERE { <s> <p> <o> }";
  std::string expectedXml = makeXMLHeader({}) +
                            R"(
  <result>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{kg,
                               query,
                               1,
                               "\n\n",
                               "\n\n",
                               nlohmann::json{std::vector{std::vector<int>{}}},
                               []() {
                                 nlohmann::json j;
                                 j["head"]["vars"] = nlohmann::json::array();
                                 j["results"]["bindings"].push_back({});
                                 return j;
                               }(),
                               expectedXml};
  runSelectQueryTestCase(testCase);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, BlankNode) {
  std::string kg = "<s> <p> _:blank";
  std::string objectQuery = "SELECT ?o WHERE { ?s ?p ?o } ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><bnode>bn0</bnode></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseBlankNode{
      kg, objectQuery, 1,
      // TSV
      "?o\n"
      "_:bn0\n",
      // CSV
      "o\n"
      "_:bn0\n",
      makeExpectedQLeverJSON({"_:bn0"s}),
      makeExpectedSparqlJSON({makeJSONBinding(std::nullopt, "bnode", "bn0")}),
      expectedXml};
  runSelectQueryTestCase(testCaseBlankNode);
  // Note: Blank nodes cannot be introduced in a `VALUES` clause, so they can
  // never be part of the local vocabulary. For this reason we don't need a
  // `VALUES` clause in the test query like in the test cases above.
  kg = "<s> <p> <o>";
  objectQuery =
      "SELECT (BNODE(\"1\") AS ?a) (BNODE(?x) AS ?b) WHERE { VALUES (?x) { (1) "
      "(2) } }";
  expectedXml = makeXMLHeader({"a", "b"}) +
                R"(
  <result>
    <binding name="a"><bnode>un1_0</bnode></binding>
    <binding name="b"><bnode>un1_0</bnode></binding>
  </result>
  <result>
    <binding name="a"><bnode>un1_1</bnode></binding>
    <binding name="b"><bnode>un2_1</bnode></binding>
  </result>)" + xmlTrailer;
  testCaseBlankNode = TestCaseSelectQuery{
      kg, objectQuery, 2,
      // TSV
      "?a\t?b\n"
      "_:un1_0\t_:un1_0\n"
      "_:un1_1\t_:un2_1\n",
      // CSV
      "a,b\n"
      "_:un1_0,_:un1_0\n"
      "_:un1_1,_:un2_1\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"_:un1_0"s, "_:un1_0"s});
        j.push_back(std::vector{"_:un1_1"s, "_:un2_1"s});
        return j;
      }(),
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("a");
        j["head"]["vars"].push_back("b");
        auto& bindings = j["results"]["bindings"];
        bindings.emplace_back();
        bindings.back()["a"] = makeJSONBinding(std::nullopt, "bnode", "un1_0");
        bindings.back()["b"] = makeJSONBinding(std::nullopt, "bnode", "un1_0");
        bindings.emplace_back();
        bindings.back()["a"] = makeJSONBinding(std::nullopt, "bnode", "un1_1");
        bindings.back()["b"] = makeJSONBinding(std::nullopt, "bnode", "un2_1");
        return j;
      }(),
      expectedXml};
  runSelectQueryTestCase(testCaseBlankNode);

  TestCaseConstructQuery testCaseConstruct{
      "<a> <b> <c> . <d> <e> <f> . <g> <h> <i> . <j> <k> <l>",
      "CONSTRUCT { [] <p> _:a . [] <p> _:a } WHERE { ?s ?p ?o }", 8, 8,
      // TSV
      "_:g0_0\t<p>\t_:u0_a\n"
      "_:g0_1\t<p>\t_:u0_a\n"
      "_:g1_0\t<p>\t_:u1_a\n"
      "_:g1_1\t<p>\t_:u1_a\n"
      "_:g2_0\t<p>\t_:u2_a\n"
      "_:g2_1\t<p>\t_:u2_a\n"
      "_:g3_0\t<p>\t_:u3_a\n"
      "_:g3_1\t<p>\t_:u3_a\n",
      // CSV
      "_:g0_0,<p>,_:u0_a\n"
      "_:g0_1,<p>,_:u0_a\n"
      "_:g1_0,<p>,_:u1_a\n"
      "_:g1_1,<p>,_:u1_a\n"
      "_:g2_0,<p>,_:u2_a\n"
      "_:g2_1,<p>,_:u2_a\n"
      "_:g3_0,<p>,_:u3_a\n"
      "_:g3_1,<p>,_:u3_a\n",
      // Turtle
      "_:g0_0 <p> _:u0_a .\n"
      "_:g0_1 <p> _:u0_a .\n"
      "_:g1_0 <p> _:u1_a .\n"
      "_:g1_1 <p> _:u1_a .\n"
      "_:g2_0 <p> _:u2_a .\n"
      "_:g2_1 <p> _:u2_a .\n"
      "_:g3_0 <p> _:u3_a .\n"
      "_:g3_1 <p> _:u3_a .\n",
      // N-Triples
      "_:g0_0 <p> _:u0_a .\n"
      "_:g0_1 <p> _:u0_a .\n"
      "_:g1_0 <p> _:u1_a .\n"
      "_:g1_1 <p> _:u1_a .\n"
      "_:g2_0 <p> _:u2_a .\n"
      "_:g2_1 <p> _:u2_a .\n"
      "_:g3_0 <p> _:u3_a .\n"
      "_:g3_1 <p> _:u3_a .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"_:g0_0"s, "<p>"s, "_:u0_a"s});
        j.push_back(std::vector{"_:g0_1"s, "<p>"s, "_:u0_a"s});
        j.push_back(std::vector{"_:g1_0"s, "<p>"s, "_:u1_a"s});
        j.push_back(std::vector{"_:g1_1"s, "<p>"s, "_:u1_a"s});
        j.push_back(std::vector{"_:g2_0"s, "<p>"s, "_:u2_a"s});
        j.push_back(std::vector{"_:g2_1"s, "<p>"s, "_:u2_a"s});
        j.push_back(std::vector{"_:g3_0"s, "<p>"s, "_:u3_a"s});
        j.push_back(std::vector{"_:g3_1"s, "<p>"s, "_:u3_a"s});
        return j;
      }(),
      2};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, TextIndex) {
  std::string kg = "<s> <p> \"alpha beta\". <s2> <p2> \"alphax betax\". ";
  std::string objectQuery =
      "SELECT ?o WHERE {<s> <p> ?t. ?text ql:contains-entity ?t .?text "
      "ql:contains-word \"alph*\" BIND (?ql_matchingword_text_alph AS ?o)}";

  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal>alpha</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseTextIndex{kg, objectQuery, 1,
                                        // TSV
                                        "?o\n"
                                        "alpha\n",
                                        // CSV
                                        "o\n"
                                        "alpha\n",
                                        makeExpectedQLeverJSON({"alpha"s}),
                                        makeExpectedSparqlJSON({makeJSONBinding(
                                            std::nullopt, "literal", "alpha")}),
                                        expectedXml};
  runSelectQueryTestCase(testCaseTextIndex, true);
}

// Test that a text record of the text index that contains quotes is exported
// in full. The text records are plain text (not the representation of a
// literal with quotes), so a quote inside must not be taken as the end of a
// literal in the SPARQL JSON and XML export.
TEST(ExportQueryExecutionTrees, TextRecordWithQuotes) {
  // A text index with a single text record that contains quotes and mentions
  // the entity `<s>`.
  std::string text = "She said \"alpha\" and left";
  ad_utility::testing::TestIndexConfig config{"<s> <p> <o> ."};
  config.createTextIndex = true;
  config.contentsOfWordsFileAndDocsfile =
      std::pair{absl::StrCat(createWordsFileLineAsString("alpha", false, 1, 1),
                             createWordsFileLineAsString("<s>", true, 1, 1)),
                createDocsFileLineAsString(1, text)};
  std::string query =
      "SELECT ?o WHERE { ?o ql:contains-entity <s> . ?o ql:contains-word "
      "\"alph*\" }";
  using enum ad_utility::MediaType;

  // The TSV export writes the text verbatim.
  EXPECT_EQ(runQueryStreamableResult(config, query, tsv),
            absl::StrCat("?o\n", text, "\n"));

  // The SPARQL JSON and XML exports contain the whole text as a plain literal.
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::sparqlResultsJsonWithTime_>(false);
  EXPECT_EQ(
      nlohmann::json::parse(
          runQueryStreamableResult(config, query, sparqlJson)),
      makeExpectedSparqlJSON({makeJSONBinding(std::nullopt, "literal", text)}));
  EXPECT_EQ(runQueryStreamableResult(config, query, sparqlXml),
            absl::StrCat(makeXMLHeader({"o"}), R"(
  <result>
    <binding name="o"><literal>)",
                         RdfEscaping::escapeForXml(text),
                         "</literal></binding>\n  </result>", xmlTrailer));
}

// Test that a SELECT query can be exported in QLever's binary format.
TEST(ExportQueryExecutionTrees, BinaryQLeverExport) {
  // The format itself is tested in `engine/BinaryExportTest.cpp`.
  auto result = runQueryStreamableResult(
      "<s> <p> <o> .", "SELECT ?o WHERE { <s> <p> ?o }",
      ad_utility::MediaType::binaryQleverExport);
  EXPECT_THAT(result, HasSubstr("QLEVER.EXPORT"));
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, MultipleVariables) {
  std::string kg = "<s> <p> <o>";
  std::string objectQuery = "SELECT ?p ?o WHERE {<s> ?p ?o } ORDER BY ?p ?o";
  std::string expectedXml = makeXMLHeader({"p", "o"}) +
                            R"(
  <result>
    <binding name="p"><uri>p</uri></binding>
    <binding name="o"><uri>o</uri></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseMultipleVariables{
      kg, objectQuery, 1,
      // TSV
      "?p\t?o\n"
      "<p>\t<o>\n",
      // CSV
      "p,o\n"
      "p,o\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<p>"s, "<o>"s});
        return j;
      }(),
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("p");
        j["head"]["vars"].push_back("o");
        auto& bindings = j["results"]["bindings"];
        bindings.emplace_back();
        bindings.back()["p"] = makeJSONBinding(std::nullopt, "uri", "p");
        bindings.back()["o"] = makeJSONBinding(std::nullopt, "uri", "o");
        return j;
      }(),
      expectedXml};
  runSelectQueryTestCase(testCaseMultipleVariables);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, LimitOffset) {
  std::string kg = "<a> <b> <c> . <d> <e> <f> . <g> <h> <i> . <j> <k> <l>";
  std::string expectedXml = makeXMLHeader({"s"}) +
                            R"(
  <result>
    <binding name="s"><uri>d</uri></binding>
  </result>
  <result>
    <binding name="s"><uri>g</uri></binding>
  </result>)" + xmlTrailer;
  // The `OrderBy` operation does not handle the limit.
  std::string_view objectQuery0 =
      "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 2 OFFSET 1";
  // The `IndexScan` operation handles the limit.
  std::string_view objectQuery1 =
      "SELECT ?s WHERE { ?s ?p ?o } INTERNAL SORT BY ?s LIMIT 2 OFFSET 1";
  for (auto objectQuery : {objectQuery0, objectQuery1}) {
    TestCaseSelectQuery testCaseLimitOffset{
        kg, std::string{objectQuery}, 2,
        // TSV
        "?s\n"
        "<d>\n"
        "<g>\n",
        // CSV
        "s\n"
        "d\n"
        "g\n",
        []() {
          nlohmann::json j;
          j.push_back(std::vector{
              "<d>"s,
          });
          j.push_back(std::vector{
              "<g>"s,
          });
          return j;
        }(),
        []() {
          nlohmann::json j;
          j["head"]["vars"].push_back("s");
          auto& bindings = j["results"]["bindings"];
          bindings.emplace_back();
          bindings.back()["s"] = makeJSONBinding(std::nullopt, "uri", "d");
          bindings.emplace_back();
          bindings.back()["s"] = makeJSONBinding(std::nullopt, "uri", "g");
          return j;
        }(),
        expectedXml};
    runSelectQueryTestCase(testCaseLimitOffset);
  }
}

// The trailing `VALUES` clause has to be joined before `DISTINCT` and
// `ORDER BY` are applied (SPARQL 1.1, sec. 18.2.4.3).
TEST(ExportQueryExecutionTrees, TrailingValuesBeforeDistinctAndOrderBy) {
  std::string kg = "<a> <p> <c> . <a> <p> <f> . <b> <p> <z> . <d> <p> <y> .";
  auto tsv = [&kg](const std::string& query) {
    return runQueryStreamableResult(kg, query, ad_utility::MediaType::tsv);
  };
  EXPECT_EQ(tsv("SELECT DISTINCT ?s { ?s <p> ?o } VALUES ?o { <f> }"),
            "?s\n<a>\n");
  EXPECT_EQ(tsv("SELECT DISTINCT ?s { ?s <p> ?o } VALUES ?s { <a> <a> }"),
            "?s\n<a>\n");
  EXPECT_EQ(
      tsv("SELECT ?s ?o { ?s <p> ?o } ORDER BY ?o VALUES ?s { <a> <b> <d> }"),
      "?s\t?o\n<a>\t<c>\n<a>\t<f>\n<d>\t<y>\n<b>\t<z>\n");
  EXPECT_EQ(tsv("SELECT ?s { ?s <p> ?o } ORDER BY DESC(?s) VALUES ?s { <a> <b> "
                "<d> }"),
            "?s\n<d>\n<b>\n<a>\n<a>\n");
  EXPECT_EQ(tsv("SELECT ?x { ?s <p> ?o } ORDER BY ?x VALUES ?x { 2 1 }"),
            "?x\n1\n1\n1\n1\n2\n2\n2\n2\n");
  // `ORDER BY` expressions and `SELECT` expressions also see the `VALUES`.
  EXPECT_EQ(tsv("SELECT ?x { <a> <p> ?o } ORDER BY STR(?x) VALUES ?x { 2 1 }"),
            "?x\n1\n1\n2\n2\n");
  EXPECT_EQ(tsv("SELECT ?x (STR(?x) AS ?y) { <b> <p> ?o } VALUES ?x { 1 }"),
            "?x\t?y\n1\t\"1\"\n");
  // The `FILTER`s of the `WHERE` clause don't see the `VALUES`.
  EXPECT_EQ(tsv("SELECT ?s { ?s <p> <z> FILTER(!BOUND(?x)) } VALUES ?x { 1 }"),
            "?s\n<b>\n");
  // With `GROUP BY`, the `VALUES` is joined after the grouping.
  EXPECT_EQ(tsv("SELECT ?s (COUNT(*) AS ?c) { ?s <p> ?o } GROUP BY ?s "
                "ORDER BY ?v ?s VALUES ?v { 2 1 }"),
            "?s\t?c\n<a>\t2\n<b>\t1\n<d>\t1\n"
            "<a>\t2\n<b>\t1\n<d>\t1\n");
  // With `GROUP BY`, `ORDER BY` expressions and `SELECT` expressions that use
  // the `VALUES` variables are computed after the join, the aggregates in these
  // expressions are still computed by the grouping.
  EXPECT_EQ(tsv("SELECT ?s (COUNT(*) AS ?c) { ?s <p> ?o } GROUP BY ?s "
                "ORDER BY LCASE(?n) "
                "VALUES (?s ?n) { (<a> \"b\") (<b> \"C\") (<d> \"a\") }"),
            "?s\t?c\n<d>\t1\n<a>\t2\n<b>\t1\n");
  EXPECT_EQ(tsv("SELECT ?s ?v (STR(?v) AS ?w) { ?s <p> ?o } GROUP BY ?s "
                "ORDER BY ?s VALUES ?v { 1 }"),
            "?s\t?v\t?w\n<a>\t1\t\"1\"\n<b>\t1\t\"1\"\n<d>\t1\t\"1\"\n");
  EXPECT_EQ(tsv("SELECT ?s ((COUNT(*) + ?v) AS ?x) (?x * 2 AS ?y) "
                "{ ?s <p> ?o } GROUP BY ?s ORDER BY DESC(COUNT(*) * ?v) ?s "
                "VALUES ?v { 10 }"),
            "?s\t?x\t?y\n<a>\t12\t24\n<b>\t11\t22\n<d>\t11\t22\n");
  EXPECT_EQ(tsv("SELECT (COUNT(*) + ?v AS ?x) { ?s <p> ?o } ORDER BY ?x "
                "VALUES ?v { 1 2 }"),
            "?x\n5\n6\n");
  // `HAVING` is applied before the join.
  EXPECT_EQ(tsv("SELECT ?s ?v { ?s <p> ?o } GROUP BY ?s "
                "HAVING (COUNT(*) > 1) VALUES ?v { 1 }"),
            "?s\t?v\n<a>\t1\n");
  EXPECT_EQ(tsv("SELECT ?s (?v AS ?w) { ?s <p> ?o } GROUP BY ?s "
                "HAVING (BOUND(?w)) VALUES ?v { 1 }"),
            "?s\t?w\n");
  EXPECT_EQ(tsv("SELECT ?s (?v AS ?w) { ?s <p> ?o } GROUP BY ?s "
                "HAVING (!BOUND(?w) && !BOUND(?v)) VALUES ?v { 1 }"),
            "?s\t?w\n<a>\t1\n<b>\t1\n<d>\t1\n");
  EXPECT_EQ(tsv("SELECT ?s { ?s <p> ?o } GROUP BY ?s "
                "HAVING (COUNT(*) > 1 || ?v = 1) VALUES ?v { 1 }"),
            "?s\n<a>\n");
  // The `CONSTRUCT` template can also use the `VALUES` variables, and the
  // `VALUES` can restrict a grouped variable.
  EXPECT_EQ(runQueryStreamableResult(
                kg,
                "CONSTRUCT { ?s <r> ?r } { ?s <p> ?o } GROUP BY ?s "
                "VALUES (?s ?r) { (<a> 1) (<b> 2) (<e> 4) }",
                ad_utility::MediaType::turtle),
            "<a> <r> 1 .\n<b> <r> 2 .\n");
}

// The trailing `VALUES` clause of a subquery belongs to the subquery, it is
// joined before the subquery's `DISTINCT`, `LIMIT` and projection.
TEST(ExportQueryExecutionTrees, TrailingValuesOfSubquery) {
  std::string kg = "<a> <p> <c> . <b> <p> <d> .";
  auto tsv = [&kg](const std::string& query) {
    return runQueryStreamableResult(kg, query, ad_utility::MediaType::tsv);
  };
  EXPECT_EQ(tsv("SELECT * { { SELECT ?s { ?s <p> ?o } ORDER BY ?s LIMIT 1 "
                "VALUES ?s { <b> } } }"),
            "?s\n<b>\n");
  EXPECT_EQ(tsv("SELECT * { { SELECT DISTINCT ?s { ?s <p> ?o } "
                "VALUES ?s { <a> <a> } } }"),
            "?s\n<a>\n");
  EXPECT_EQ(tsv("SELECT * { { SELECT ?s { ?s <p> ?o } VALUES ?x { 1 } } }"),
            "?s\n<a>\n<b>\n");
  EXPECT_EQ(tsv("SELECT * { { SELECT * { ?s <p> ?o } VALUES ?x { 1 } } }"),
            "?s\t?o\t?x\n<a>\t<c>\t1\n<b>\t<d>\t1\n");
  EXPECT_EQ(tsv("SELECT * { { SELECT ?s { ?s <p> ?o } VALUES ?o { <c> } } }"),
            "?s\n<a>\n");
  EXPECT_EQ(tsv("SELECT * { { SELECT ?s (COUNT(*) + ?v AS ?x) { ?s <p> ?o } "
                "GROUP BY ?s VALUES ?v { 10 } } } ORDER BY ?s"),
            "?s\t?x\n<a>\t11\n<b>\t11\n");
}

// Test that `HAVING` and `ORDER BY` expressions with `GROUP BY` also work for
// queries without a `SELECT` clause (`CONSTRUCT` and `ASK`), where the internal
// aliases that compute them are stored in the `ParsedQuery`.
TEST(ExportQueryExecutionTrees, HavingAndOrderByExpressionsWithoutSelect) {
  // Two triples with subject `<a>` and one with subject `<b>`.
  std::string kg = "<a> <p> <c> . <a> <p> <f> . <b> <p> <z> .";
  auto turtle = [&kg](const std::string& query) {
    return runQueryStreamableResult(kg, query, ad_utility::MediaType::turtle);
  };

  // `HAVING` and `ORDER BY` expressions in a `CONSTRUCT` query.
  EXPECT_EQ(turtle("CONSTRUCT { ?s <r> ?s } { ?s <p> ?o } GROUP BY ?s "
                   "HAVING (COUNT(*) > 1)"),
            "<a> <r> <a> .\n");
  EXPECT_EQ(turtle("CONSTRUCT { ?s <r> ?s } { ?s <p> ?o } GROUP BY ?s "
                   "ORDER BY COUNT(*)"),
            "<b> <r> <b> .\n<a> <r> <a> .\n");
  EXPECT_EQ(turtle("CONSTRUCT { ?s <r> ?s } { ?s <p> ?o } GROUP BY ?s "
                   "ORDER BY DESC(STR(?s))"),
            "<b> <r> <b> .\n<a> <r> <a> .\n");

  // An `ORDER BY` expression that uses a variable of the trailing `VALUES` is
  // computed after the join.
  EXPECT_EQ(turtle("CONSTRUCT { ?s <r> ?n } { ?s <p> ?o } GROUP BY ?s "
                   "ORDER BY LCASE(?n) VALUES (?s ?n) { (<a> \"B\") "
                   "(<b> \"a\") }"),
            "<b> <r> \"a\" .\n<a> <r> \"B\" .\n");

  // The pattern trick also reads the aliases (none, or the count of the
  // `ORDER BY` expression).
  EXPECT_EQ(turtle("CONSTRUCT { ?p <r> ?p } { ?s ?p ?o } GROUP BY ?p"),
            "<p> <r> <p> .\n");
  EXPECT_EQ(runQueryStreamableResult(
                kg + " <a> <q> <c> .",
                "CONSTRUCT { ?p <r> ?p } { ?s ?p ?o } GROUP BY ?p "
                "ORDER BY DESC(COUNT(DISTINCT ?s))",
                ad_utility::MediaType::turtle),
            "<p> <r> <p> .\n<q> <r> <q> .\n");

  // `HAVING` and `ORDER BY` expressions in an `ASK` query.
  auto ask = [&kg](const std::string& query) {
    return nlohmann::json::parse(
               runQueryStreamableResult(kg, query,
                                        ad_utility::MediaType::sparqlJson))
        .at("boolean")
        .get<bool>();
  };
  EXPECT_TRUE(ask("ASK { ?s <p> ?o } GROUP BY ?s HAVING (COUNT(*) > 1)"));
  EXPECT_FALSE(ask("ASK { ?s <p> ?o } GROUP BY ?s HAVING (COUNT(*) > 2)"));
  EXPECT_TRUE(ask("ASK { ?s <p> ?o } GROUP BY ?s ORDER BY STR(?s)"));
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, BinaryExport) {
  std::string kg = "<s> <p> 31 . <s> <o> 42";
  std::string query = "SELECT ?p ?o WHERE {<s> ?p ?o } ORDER BY ?p ?o";
  std::string result =
      runQueryStreamableResult(kg, query, ad_utility::MediaType::octetStream);
  ASSERT_EQ(4 * sizeof(Id), result.size());
  auto qec = ad_utility::testing::getQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto p = getId("<p>");
  auto o = getId("<o>");

  Id id0, id1, id2, id3;
  std::memcpy(&id0, result.data(), sizeof(Id));
  std::memcpy(&id1, result.data() + sizeof(Id), sizeof(Id));
  std::memcpy(&id2, result.data() + 2 * sizeof(Id), sizeof(Id));
  std::memcpy(&id3, result.data() + 3 * sizeof(Id), sizeof(Id));

  // The result is "p, 31" (first row) "o, 42" (second row)
  ASSERT_EQ(o, id0);
  ASSERT_EQ(ad_utility::testing::IntId(42), id1);
  ASSERT_EQ(p, id2);
  ASSERT_EQ(ad_utility::testing::IntId(31), id3);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, CornerCases) {
  std::string kg = "<s> <p> <o>";
  std::string query = "SELECT ?p ?o WHERE {<s> ?p ?o } ORDER BY ?p ?o";
  std::string constructQuery =
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o } ORDER BY ?p ?o";

  // Turtle is not supported for SELECT queries.
  ASSERT_THROW(
      runQueryStreamableResult(kg, query, ad_utility::MediaType::turtle),
      ad_utility::Exception);
  // SPARQL JSON is not supported for construct queries.
  ASSERT_THROW(
      runJSONQuery(kg, constructQuery, ad_utility::MediaType::sparqlJson),
      ad_utility::Exception);
  // XML is currently not supported for construct queries.
  AD_EXPECT_THROW_WITH_MESSAGE(
      runQueryStreamableResult(kg, constructQuery,
                               ad_utility::MediaType::sparqlXml),
      ::testing::HasSubstr(
          "application/sparql-results+xml is not supported for CONSTRUCT "
          "queries."));

  // Binary export is not supported for CONSTRUCT queries.
  ASSERT_THROW(runQueryStreamableResult(kg, constructQuery,
                                        ad_utility::MediaType::octetStream),
               ad_utility::Exception);

  // If none of the selected variables is defined in the query body, we have an
  // empty solution mapping per row, but there is no need to materialize any
  // IRIs or literals.
  std::string queryNoVariablesVisible = "SELECT ?not ?known WHERE {<s> ?p ?o}";
  auto resultNoColumns = runJSONQuery(kg, queryNoVariablesVisible,
                                      ad_utility::MediaType::sparqlJson);
  ASSERT_EQ(resultNoColumns["results"]["bindings"].size(), 1);
  EXPECT_TRUE(resultNoColumns["results"]["bindings"][0].is_object());
  EXPECT_TRUE(resultNoColumns["results"]["bindings"][0].empty());
  auto qec = ad_utility::testing::getQec(kg);
  AD_EXPECT_THROW_WITH_MESSAGE(
      ql::exportIds::idToStringAndType(qec->getIndex(), Id::max(),
                                       LocalVocab{}),
      ::testing::ContainsRegex("should be unreachable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ql::exportIds::getLiteralOrIriFromVocabIndex(qec->getIndex(), Id::max(),
                                                   LocalVocab{}),
      ::testing::ContainsRegex("should be unreachable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ql::exportIds::idToStringAndTypeForEncodedValue(
          ad_utility::testing::VocabId(12)),
      ::testing::ContainsRegex("should be unreachable"));
}

// Test the correct exporting of ASK queries.
// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, AskQuery) {
  auto askResultTrue = [](bool lazy) {
    TestCaseAskQuery testCase;
    if (lazy) {
      testCase.kg = "<x> <y> <z>";
      testCase.query = "ASK { <x> ?p ?o}";
    } else {
      testCase.query = "ASK { BIND (3 as ?x) FILTER (?x > 0)}";
    }
    testCase.resultQLeverJSON = nlohmann::json{std::vector<std::string>{
        "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>"}};
    testCase.resultSparqlJSON =
        nlohmann::json::parse(R"({"head":{ }, "boolean" : true})");
    testCase.resultXml =
        "<?xml version=\"1.0\"?>\n<sparql "
        "xmlns=\"http://www.w3.org/2005/sparql-results#\">\n  <head/>\n  "
        "<boolean>true</boolean>\n</sparql>";

    return testCase;
  };

  auto askResultFalse = [](bool lazy) {
    TestCaseAskQuery testCase;
    if (lazy) {
      testCase.kg = "<x> <y> <z>";
      testCase.query = "ASK { <y> ?p ?o}";
    } else {
      testCase.query = "ASK { BIND (3 as ?x) FILTER (?x < 0)}";
    }
    testCase.resultQLeverJSON = nlohmann::json{std::vector<std::string>{
        "\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>"}};
    testCase.resultSparqlJSON =
        nlohmann::json::parse(R"({"head":{ }, "boolean" : false})");
    testCase.resultXml =
        "<?xml version=\"1.0\"?>\n<sparql "
        "xmlns=\"http://www.w3.org/2005/sparql-results#\">\n  <head/>\n  "
        "<boolean>false</boolean>\n</sparql>";
    return testCase;
  };
  runAskQueryTestCase(askResultTrue(true));
  runAskQueryTestCase(askResultTrue(false));
  runAskQueryTestCase(askResultFalse(true));
  runAskQueryTestCase(askResultFalse(false));
}

using enum ad_utility::MediaType;

// ____________________________________________________________________________
class StreamableMediaTypesFixture
    : public ::testing::Test,
      public ::testing::WithParamInterface<ad_utility::MediaType> {};

TEST_P(StreamableMediaTypesFixture, CancellationCancelsStream) {
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  auto* qec = ad_utility::testing::getQec(
      "<s> <p> 42 . <s> <p> -42019234865781 . <s> <p> 4012934858173560");
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = parseQuery(GetParam() == turtle
                           ? "CONSTRUCT { ?x ?y ?z } WHERE { ?x ?y ?z }"
                           : "SELECT * WHERE { ?x ?y ?z }");
  auto qet = qp.createExecutionTree(pq);

  cancellationHandle->cancel(ad_utility::CancellationState::MANUAL);
  ad_utility::Timer timer(ad_utility::Timer::Started);
  EXPECT_ANY_THROW(([&]() {
    [[maybe_unused]] auto generator = ExportQueryExecutionTrees::computeResult(
        pq, *qet, GetParam(), timer, std::move(cancellationHandle));
  }()));
}

INSTANTIATE_TEST_SUITE_P(StreamableMediaTypes, StreamableMediaTypesFixture,
                         ::testing::Values(turtle, sparqlXml, tsv, csv,
                                           octetStream, sparqlJson,
                                           qleverJson));

// TODO<joka921> Unit tests for the more complex CONSTRUCT export (combination
// between constants and stuff from the knowledge graph).

// TODO<joka921> Unit tests that also test for the export of text records from
// the text index and thus systematically fill the coverage gaps.

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, getIdTablesReturnsSingletonIterator) {
  auto idTable = makeIdTableFromVector({{42}, {1337}});

  Result result{idTable.clone(), {}, LocalVocab{}};
  auto generator = ExportQueryExecutionTrees::getIdTables(result);

  EXPECT_THAT(convertToVector(std::move(generator)), matchesIdTables(idTable));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, getIdTablesMirrorsGenerator) {
  IdTable idTable1 = makeIdTableFromVector({{1}, {2}, {3}});
  IdTable idTable2 = makeIdTableFromVector({{42}, {1337}});
  auto tableGenerator = [](IdTable idTableA,
                           IdTable idTableB) -> Result::Generator {
    co_yield {std::move(idTableA), LocalVocab{}};

    co_yield {std::move(idTableB), LocalVocab{}};
  }(idTable1.clone(), idTable2.clone());

  Result result{std::move(tableGenerator), {}};
  auto generator = ExportQueryExecutionTrees::getIdTables(result);

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(idTable1, idTable2));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ensureCorrectSlicingOfSingleIdTable) {
  auto tableGenerator = []() -> Result::Generator {
    Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}, {2}, {3}}),
                                   LocalVocab{}};
    co_yield pair1;
  }();

  Result result{std::move(tableGenerator), {}};
  uint64_t resultSizeTotal = 0;
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 1, ._offset = 1}, result, resultSizeTotal);

  auto expectedResult = makeIdTableFromVector({{2}});
  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(expectedResult));
  EXPECT_EQ(resultSizeTotal, 1);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenFirstIsSkipped) {
  auto tableGenerator = []() -> Result::Generator {
    Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}, {2}, {3}}),
                                   LocalVocab{}};
    co_yield pair1;

    Result::IdTableVocabPair pair2{makeIdTableFromVector({{4}, {5}}),
                                   LocalVocab{}};
    co_yield pair2;
  }();

  Result result{std::move(tableGenerator), {}};
  uint64_t resultSizeTotal = 0;
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = std::nullopt, ._offset = 3}, result,
      resultSizeTotal);

  auto expectedResult = makeIdTableFromVector({{4}, {5}});

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(expectedResult));
  EXPECT_EQ(resultSizeTotal, 2);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenLastIsSkipped) {
  auto tableGenerator = []() -> Result::Generator {
    Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}, {2}, {3}}),
                                   LocalVocab{}};
    co_yield pair1;

    Result::IdTableVocabPair pair2{makeIdTableFromVector({{4}, {5}}),
                                   LocalVocab{}};
    co_yield pair2;
  }();

  Result result{std::move(tableGenerator), {}};
  uint64_t resultSizeTotal = 0;
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 3}, result, resultSizeTotal);

  auto expectedResult = makeIdTableFromVector({{1}, {2}, {3}});

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(expectedResult));
  EXPECT_EQ(resultSizeTotal, 3);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenFirstAndSecondArePartial) {
  auto tableGenerator = []() -> Result::Generator {
    Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}, {2}, {3}}),
                                   LocalVocab{}};
    co_yield pair1;

    Result::IdTableVocabPair pair2{makeIdTableFromVector({{4}, {5}}),
                                   LocalVocab{}};
    co_yield pair2;
  }();

  Result result{std::move(tableGenerator), {}};
  uint64_t resultSizeTotal = 0;
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 3, ._offset = 1}, result, resultSizeTotal);

  auto expectedResult1 = makeIdTableFromVector({{2}, {3}});
  auto expectedResult2 = makeIdTableFromVector({{4}});

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(expectedResult1, expectedResult2));
  EXPECT_EQ(resultSizeTotal, 3);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenFirstAndLastArePartial) {
  auto tableGenerator = []() -> Result::Generator {
    Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}, {2}, {3}}),
                                   LocalVocab{}};
    co_yield pair1;

    Result::IdTableVocabPair pair2{makeIdTableFromVector({{4}, {5}}),
                                   LocalVocab{}};
    co_yield pair2;

    Result::IdTableVocabPair pair3{makeIdTableFromVector({{6}, {7}, {8}, {9}}),
                                   LocalVocab{}};
    co_yield pair3;
  }();

  Result result{std::move(tableGenerator), {}};
  uint64_t resultSizeTotal = 0;
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 5, ._offset = 2}, result, resultSizeTotal);

  auto expectedTable1 = makeIdTableFromVector({{3}});
  auto expectedTable2 = makeIdTableFromVector({{4}, {5}});
  auto expectedTable3 = makeIdTableFromVector({{6}, {7}});

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(expectedTable1, expectedTable2, expectedTable3));
  EXPECT_EQ(resultSizeTotal, 5);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ensureGeneratorIsNotConsumedWhenNotRequired) {
  {
    auto throwingGenerator = []() -> Result::Generator {
      std::string message = "Generator was started, but should not have been";
      ADD_FAILURE() << message << std::endl;
      throw std::runtime_error(message);
      co_return;
    }();

    Result result{std::move(throwingGenerator), {}};
    uint64_t resultSizeTotal = 0;
    auto generator = ExportQueryExecutionTrees::getRowIndices(
        LimitOffsetClause{._limit = 0, ._offset = 0}, result, resultSizeTotal);
    EXPECT_NO_THROW(convertToVector(std::move(generator)));
  }

  {
    auto throwAfterYieldGenerator = []() -> Result::Generator {
      Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}}),
                                     LocalVocab{}};
      co_yield pair1;

      std::string message =
          "Generator was called a second time, but should not "
          "have been";
      ADD_FAILURE() << message << std::endl;
      throw std::runtime_error(message);
    }();

    Result result{std::move(throwAfterYieldGenerator), {}};
    uint64_t resultSizeTotal = 0;
    auto generator = ExportQueryExecutionTrees::getRowIndices(
        LimitOffsetClause{._limit = 1, ._offset = 0}, result, resultSizeTotal);
    IdTable expectedTable = makeIdTableFromVector({{1}});
    std::vector<IdTable> tables;
    EXPECT_NO_THROW({ tables = convertToVector(std::move(generator)); });
    EXPECT_THAT(tables, matchesIdTables(expectedTable));
    EXPECT_EQ(resultSizeTotal, 1);
  }
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, verifyQleverJsonContainsValidMetadata) {
  std::string_view query =
      "SELECT * WHERE { ?x ?y ?z . FILTER(?y != <p2>) } OFFSET 1 LIMIT 4";
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  auto* qec = ad_utility::testing::getQec(
      "<s> <p1> 40,41,42,43,44,45,46,47,48,49"
      " ; <p2> 50,51,52,53,54,55,56,57,58,59");
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = parseQuery(std::string{query});
  auto qet = qp.createExecutionTree(pq);

  ad_utility::Timer timer{ad_utility::Timer::Started};

  // Verify this is accounted for for time calculation.
  std::this_thread::sleep_for(1ms);

  auto jsonStream = ExportQueryExecutionTrees::computeResultAsQLeverJSON(
      pq, *qet, pq._limitOffset, timer, std::move(cancellationHandle));

  std::string aggregateString{};
  for (std::string_view chunk : jsonStream) {
    aggregateString += chunk;
  }
  nlohmann::json json = nlohmann::json::parse(aggregateString);
  auto originalRuntimeInfo = qet->getRootOperation()->runtimeInfo();

  EXPECT_EQ(json["query"], query);
  EXPECT_EQ(json["status"], "OK");
  EXPECT_THAT(json["warnings"], ElementsAre());
  EXPECT_THAT(json["selected"], ElementsAre(Eq("?x"), Eq("?y"), Eq("?z")));
  EXPECT_EQ(json["res"].size(), 4);
  auto& runtimeInformationWrapper = json["runtimeInformation"];
  EXPECT_TRUE(runtimeInformationWrapper.contains("meta"));
  ASSERT_TRUE(runtimeInformationWrapper.contains("query_execution_tree"));
  auto& runtimeInformation = runtimeInformationWrapper["query_execution_tree"];
  EXPECT_EQ(runtimeInformation["result_cols"], 3);
  EXPECT_EQ(runtimeInformation["result_rows"], 4);
  EXPECT_EQ(json["resultsize"], 4);
  auto& timingInformation = json["time"];
  EXPECT_GE(toChrono(timingInformation["total"].get<std::string_view>()), 1ms);
#ifndef _QLEVER_NO_TIMING_TESTS
  // Ensure that the result is not returned in microseconds and subsequently
  // interpreted in milliseconds.
  EXPECT_LT(
      toChrono(timingInformation["computeResult"].get<std::string_view>()),
      100ms);
#endif
  EXPECT_GE(
      toChrono(timingInformation["total"].get<std::string_view>()),
      toChrono(timingInformation["computeResult"].get<std::string_view>()));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, convertGeneratorForChunkedTransfer) {
  using S = ad_utility::streams::stream_generator;
  auto throwEarly = []() -> S {
    co_yield " Hallo... Ups\n";
    throw std::runtime_error{"failed"};
  };
  auto call = [](S stream) {
    [[maybe_unused]] auto res =
        ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
            std::move(stream));
  };
  AD_EXPECT_THROW_WITH_MESSAGE(call(throwEarly()), std::string_view("failed"));
  auto throwLate = [](bool throwProperException) -> S {
    size_t largerThanBufferSize = (size_t{1} << 20) + 4;
    std::string largerThanBuffer;
    largerThanBuffer.resize(largerThanBufferSize);
    co_yield largerThanBuffer;
    if (throwProperException) {
      throw std::runtime_error{"proper exception"};
    } else {
      throw 424231;
    }
  };

  auto consume = [](auto generator) {
    std::string res;
    for (const auto& el : generator) {
      res.append(el);
    }
    return res;
  };

  std::optional<ad_utility::InputRangeTypeErased<std::string>> res;
  using namespace ::testing;
  EXPECT_NO_THROW((
      res = ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
          throwLate(true))));
  EXPECT_THAT(consume(std::move(res.value())),
              AllOf(HasSubstr("!!!!>># An error has occurred"),
                    HasSubstr("proper exception")));

  EXPECT_NO_THROW((
      res = ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
          throwLate(false))));
  EXPECT_THAT(consume(std::move(res.value())),
              AllOf(HasSubstr("!!!!>># An error has occurred"),
                    HasSubstr("A very strange")));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, compensateForLimitOffsetClause) {
  auto* qec = ad_utility::testing::getQec();

  auto qet1 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}),
      std::vector<std::optional<Variable>>{std::nullopt}, false);
  auto qet2 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}),
      std::vector<std::optional<Variable>>{std::nullopt}, true);

  LimitOffsetClause limit{10, 5};
  ExportQueryExecutionTrees::compensateForLimitOffsetClause(limit, *qet1);
  EXPECT_EQ(limit._offset, 5);

  ExportQueryExecutionTrees::compensateForLimitOffsetClause(limit, *qet2);
  EXPECT_EQ(limit._offset, 0);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, EncodedIriManagerUsage) {
  // Test that encoded IRIs are properly handled during export

  // Create a knowledge graph with IRIs that should be encodable
  std::string kg =
      "<http://example.org/123> <http://example.org/predicate456> "
      "<http://example.org/789> ."
      "<http://test.com/id/111> <http://example.org/predicate456> \"literal "
      "value\" .";

  // Test XML export with encoded IRIs
  std::string query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o";

  // Create test configuration with EncodedIriManager
  auto encodedIriManager = std::make_shared<EncodedIriManager>(
      std::vector<std::string>{"http://example.org/", "http://test.com/id/"});

  ad_utility::testing::TestIndexConfig config{kg};
  config.encodedPrefixesWithoutAngleBrackets =
      std::vector<std::string>{"http://example.org/", "http://test.com/id/"};
  auto qec = ad_utility::testing::getQec(std::move(config));

  // Parse query with the same EncodedIriManager
  auto parsedQuery =
      SparqlParser::parseQuery(encodedIriManager.get(), query, {});

  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto qet = qp.createExecutionTree(parsedQuery);

  // Export as XML and verify encoded IRIs are properly converted back
  ad_utility::Timer timer{ad_utility::Timer::Started};
  auto cancellationHandle2 =
      std::make_shared<ad_utility::CancellationHandle<>>();
  std::string result;
  for (const auto& chunk : ExportQueryExecutionTrees::computeResult(
           parsedQuery, *qet, ad_utility::MediaType::sparqlXml, timer,
           std::move(cancellationHandle2))) {
    result += chunk;
  }

  // Verify that the original IRI strings appear in the output
  EXPECT_THAT(result, HasSubstr("http://example.org/123"));
  EXPECT_THAT(result, HasSubstr("http://example.org/predicate456"));
  EXPECT_THAT(result, HasSubstr("http://example.org/789"));
  EXPECT_THAT(result, HasSubstr("http://test.com/id/111"));
  EXPECT_THAT(result, HasSubstr("literal value"));

  // Test TSV export as well
  ad_utility::Timer tsvTimer{ad_utility::Timer::Started};
  auto cancellationHandle3 =
      std::make_shared<ad_utility::CancellationHandle<>>();
  std::string tsvResult;
  for (const auto& chunk : ExportQueryExecutionTrees::computeResult(
           parsedQuery, *qet, ad_utility::MediaType::tsv, tsvTimer,
           std::move(cancellationHandle3))) {
    tsvResult += chunk;
  }
  EXPECT_THAT(tsvResult, HasSubstr("http://example.org/123"));
  EXPECT_THAT(tsvResult, HasSubstr("http://example.org/predicate456"));
  EXPECT_THAT(tsvResult, HasSubstr("http://example.org/789"));
  EXPECT_THAT(tsvResult, HasSubstr("http://test.com/id/111"));
}

// Test that a `sparql-results+json` export includes a `meta` field if and
// only if the respective runtime parameter is enabled.
// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, SparqlJsonWithMetaField) {
  std::string kg = "<x> <y> <z>";
  std::string query = "SELECT ?s ?p ?o WHERE {?s ?p ?o}";

  // Case 1: Runtime parameter enabled (default).
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::sparqlResultsJsonWithTime_>(true);
    auto result = runJSONQuery(kg, query, ad_utility::MediaType::sparqlJson);
    ASSERT_TRUE(result.contains("head"));
    ASSERT_TRUE(result.contains("results"));
    ASSERT_TRUE(result["head"].contains("vars"));
    ASSERT_TRUE(result.contains("meta"));
    ASSERT_TRUE(result["meta"].contains("query-time-ms"));
    ASSERT_TRUE(result["meta"].contains("result-size-total"));
    ASSERT_TRUE(result["meta"]["query-time-ms"].is_number());
    ASSERT_TRUE(result["meta"]["result-size-total"].is_number());
    EXPECT_GE(result["meta"]["query-time-ms"].get<int64_t>(), 0);
    EXPECT_EQ(result["meta"]["result-size-total"].get<int64_t>(), 1);
  }

  // Case 2: Runtime parameter disabled.
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::sparqlResultsJsonWithTime_>(false);
    auto result = runJSONQuery(kg, query, ad_utility::MediaType::sparqlJson);
    ASSERT_TRUE(result.contains("head"));
    ASSERT_TRUE(result.contains("results"));
    ASSERT_TRUE(result["head"].contains("vars"));
    ASSERT_FALSE(result.contains("meta"));
  }
}

// Regression test for the `Full` deduplication of CONSTRUCT results: the same
// RDF term may reach the CONSTRUCT template through two different kinds of
// `Id`. A term that comes straight from the data is a `VocabIndex` `Id`, while
// a term computed by an expression such as `STR` is materialized in the query's
// `LocalVocab` and is therefore a `LocalVocabIndex` `Id`.
//
// Both branches of the `UNION` below instantiate the identical triple
// `<s> <p> "Alice"`, once via `?o` bound directly from the data and once via
// `BIND(STR(?l) AS ?o)`. Deduplication keys are compared bitwise, so unless
// `ConstructDeduplicator::canonicalize` maps the `LocalVocabIndex` `Id` back
// onto the index vocabulary's `VocabIndex` `Id`, the two keys differ and the
// duplicate survives.
// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ConstructFullDeduplicationAcrossLocalVocab) {
  const std::string kg =
      "<http://example.org/x> <http://example.org/name> \"Alice\" . "
      "<http://example.org/y> <http://example.org/label> \"Alice\" .";
  const std::string query =
      "PREFIX ex: <http://example.org/>\n"
      "CONSTRUCT { ex:s ex:p ?o }\n"
      "WHERE {\n"
      "  { ?x ex:name ?o }\n"
      "  UNION\n"
      "  { ?y ex:label ?l  BIND(STR(?l) AS ?o) }\n"
      "}";

  using ad_utility::DeduplicationMode;
  const std::string expected =
      "<http://example.org/s> <http://example.org/p> \"Alice\" .\n";

  // Without deduplication both branches emit the triple.
  {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::constructDeduplication_>(
            DeduplicationMode::none());
    EXPECT_EQ(
        runQueryStreamableResult(kg, query, ad_utility::MediaType::turtle),
        absl::StrCat(expected, expected));
  }

  // With `Full` deduplication the triple is emitted exactly once.
  {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::constructDeduplication_>(
            DeduplicationMode::full());
    EXPECT_EQ(
        runQueryStreamableResult(kg, query, ad_utility::MediaType::turtle),
        expected);
  }
}

// A minimal smoke test that the deduplication is wired end-to-end. VALUES
// clause that emits the identical triple 3 times.
TEST(ExportQueryExecutionTrees,
     ConstructDeduplicationValuesNoneKeepsDuplicates) {
  const std::string kg = "";
  const std::string query =
      "CONSTRUCT { ?s ?p ?o } WHERE {"
      " VALUES (?s ?p ?o) {"
      "  (<ex:s> <ex:p> <ex:o>) (<ex:s> <ex:p> <ex:o>) (<ex:s> <ex:p> <ex:o>)"
      " } }";
  const std::string expected = "<ex:s> <ex:p> <ex:o> .\n";

  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::constructDeduplication_>(
          ad_utility::DeduplicationMode::none());
  EXPECT_EQ(runQueryStreamableResult(kg, query, ad_utility::MediaType::turtle),
            absl::StrCat(expected, expected, expected));
}

TEST(ExportQueryExecutionTrees,
     ConstructDeduplicationValuesFullDropsDuplicates) {
  const std::string kg = "";
  const std::string query =
      "CONSTRUCT { ?s ?p ?o } WHERE {"
      " VALUES (?s ?p ?o) {"
      "  (<ex:s> <ex:p> <ex:o>) (<ex:s> <ex:p> <ex:o>) (<ex:s> <ex:p> <ex:o>)"
      " } }";
  const std::string expected = "<ex:s> <ex:p> <ex:o> .\n";

  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::constructDeduplication_>(
          ad_utility::DeduplicationMode::full());
  EXPECT_EQ(runQueryStreamableResult(kg, query, ad_utility::MediaType::turtle),
            expected);
}

// A-B-A pattern with a batch-wise window of 1: A inserted, B inserted
// (evicts A), A inserted again (evicts B, A is new again). All three insertions
// are distinct insertions from the deduplicator's point of view, so all three
// are emitted.
TEST(ExportQueryExecutionTrees,
     ConstructDeduplicationValuesLruAbaPatternEvictsAndReemits) {
  const std::string kg = "";
  const std::string query =
      "CONSTRUCT { ?s ?p ?o } WHERE {"
      " VALUES (?s ?p ?o) {"
      "  (<ex:a> <ex:a> <ex:a>) (<ex:b> <ex:b> <ex:b>) (<ex:a> <ex:a> <ex:a>)"
      " } }";
  const std::string a = "<ex:a> <ex:a> <ex:a> .\n";
  const std::string b = "<ex:b> <ex:b> <ex:b> .\n";

  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::constructDeduplication_>(
          ad_utility::DeduplicationMode::lru(1));
  EXPECT_EQ(runQueryStreamableResult(kg, query, ad_utility::MediaType::turtle),
            absl::StrCat(a, b, a));
}

namespace {
// ____________________________________________________________________________
// Formats the single triple `<ex:c> <ex:c> <ex:c> .` for one lowercase
// letter `c`, used by `ConstructDeduplicationLruWindowTest`.
std::string lruWindowTriple(char c) {
  return absl::StrCat("<ex:", std::string(1, c), "> <ex:", std::string(1, c),
                      "> <ex:", std::string(1, c), "> .\n");
}
}  // namespace

// Batchwise deduplication with a stream of 5 unique triples, each repeated
// twice. Thus we expect 10 triples total. The window size controls how many
// duplicates survive.
struct LruWindowParam {
  size_t windowSize;
  // The letters (each standing for one `<ex:c> <ex:c> <ex:c>` triple)
  // expected to survive deduplication, in order.
  std::string expectedLetters;
};

class ConstructDeduplicationLruWindowTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<LruWindowParam> {};

TEST_P(ConstructDeduplicationLruWindowTest, window) {
  const std::string kg = "";
  const std::string query =
      "CONSTRUCT { ?s ?p ?o } WHERE {"
      " VALUES (?s ?p ?o) {"
      "  (<ex:a> <ex:a> <ex:a>) (<ex:b> <ex:b> <ex:b>)"
      "  (<ex:c> <ex:c> <ex:c>) (<ex:d> <ex:d> <ex:d>)"
      "  (<ex:e> <ex:e> <ex:e>)"
      "  (<ex:a> <ex:a> <ex:a>) (<ex:b> <ex:b> <ex:b>)"
      "  (<ex:c> <ex:c> <ex:c>) (<ex:d> <ex:d> <ex:d>)"
      "  (<ex:e> <ex:e> <ex:e>)"
      " } }";

  std::string expected;
  for (char letter : GetParam().expectedLetters) {
    absl::StrAppend(&expected, lruWindowTriple(letter));
  }

  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::constructDeduplication_>(
          ad_utility::DeduplicationMode::lru(GetParam().windowSize));
  EXPECT_EQ(runQueryStreamableResult(kg, query, ad_utility::MediaType::turtle),
            expected);
}

INSTANTIATE_TEST_SUITE_P(
    WindowSizes, ConstructDeduplicationLruWindowTest,
    ::testing::Values(
        // window 4: A is 5 positions back when it reappears. Thus, it has
        // already been evicted from the deduplication window when it re-appears
        // in the triple stream and is thus emitted. All 10 triples survive
        // (window is too small to catch any repeating triples).
        LruWindowParam{4, "abcdeabcde"},
        // window 5: the second 'a' is only 5 positions after the first.
        // Thus, this triple is still in the deduplication cache when it
        // re-appears. Suppressed. 'b'-'e' repeats hit the same window
        // and are suppressed too. Result: 5 unique triples.
        LruWindowParam{5, "abcde"},
        // window 10: all duplicates are caught, 5 unique triples remain.
        LruWindowParam{10, "abcde"}));

// A trailing `VALUES` clause without variables is joined with the result of
// the query like any other: zero rows make the result empty, one row is the
// neutral element, and multiple rows duplicate each row of the result.
TEST(ExportQueryExecutionTrees, PostQueryValuesWithoutVariables) {
  const std::string kg = "<a> <b> <c> . <d> <e> <f> .";
  auto run = [&kg](std::string_view values) {
    return runQueryStreamableResult(
        kg,
        absl::StrCat("SELECT ?s { ?s ?p ?o } ORDER BY ?s VALUES () { ", values,
                     " }"),
        ad_utility::MediaType::tsv);
  };
  EXPECT_EQ(run(""), "?s\n");
  EXPECT_EQ(run("()"), "?s\n<a>\n<d>\n");
  EXPECT_EQ(run("() ()"), "?s\n<a>\n<a>\n<d>\n<d>\n");
}

// The trailing `VALUES` clause is joined before the projection, so `SELECT *`
// also selects its variables.
TEST(ExportQueryExecutionTrees, SelectStarWithTrailingValues) {
  const std::string kg = "<a> <p> <c> . <b> <p> <d> .";
  auto run = [&kg](std::string_view query) {
    return runQueryStreamableResult(kg, std::string{query},
                                    ad_utility::MediaType::tsv);
  };
  EXPECT_EQ(run("SELECT * { ?s <p> ?o } VALUES ?x { 1 }"),
            "?s\t?o\t?x\n<a>\t<c>\t1\n<b>\t<d>\t1\n");
  // A variable that also occurs in the query body is selected only once.
  EXPECT_EQ(run("SELECT * { ?s <p> ?o } VALUES (?s ?x) { (<a> 1) }"),
            "?s\t?o\t?x\n<a>\t<c>\t1\n");
}
