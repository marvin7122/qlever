// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryPlanner.h"
#include "engine/export_v2/CsvChunkSink.h"
#include "engine/export_v2/SelectCsvStreamer.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/ParsedQueryTestHelpers.h"

using ql::engine::export_v2::CsvChunkSink;
using ql::engine::export_v2::SelectCsvStreamer;

namespace {

constexpr std::string_view kg =
    "<s1> <p> \"a,b\" . <s2> <p> \"c\\\"d\" . <s3> <p> 42 . <s4> <q> <o> .";

// _____________________________________________________________________________
// Run `query` on `kg` and export the result as CSV, either with the V1 export
// (`rowsPerChunk` is `std::nullopt`) or with `SelectCsvStreamer`. Return the
// concatenated bytes.
std::string runCsvExport(const std::string& query,
                         std::optional<size_t> rowsPerChunk) {
  auto qec = ad_utility::testing::getQec(
      ad_utility::testing::TestIndexConfig{std::string{kg}});
  qec->clearCacheUnpinnedOnly();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = ad_utility::testing::parseQuery(query);
  auto qet = qp.createExecutionTree(pq);
  const ad_utility::Timer timer{ad_utility::Timer::Started};
  auto generator = rowsPerChunk.has_value()
                       ? SelectCsvStreamer::run(qet, pq, rowsPerChunk.value(),
                                                std::move(cancellationHandle))
                       : ExportQueryExecutionTrees::computeResult(
                             pq, qet, ad_utility::MediaType::csv, timer,
                             std::move(cancellationHandle));
  std::string result;
  for (const auto& chunk : generator) {
    result += chunk;
  }
  return result;
}

// _____________________________________________________________________________
TEST(SelectCsvStreamer, bytesMatchV1Export) {
  const std::vector<std::string> queries{
      "SELECT ?s ?o WHERE { ?s <p> ?o }",
      "SELECT * WHERE { ?s ?p ?o }",
      "SELECT ?o ?s WHERE { ?s ?p ?o } LIMIT 2",
      "SELECT ?s WHERE { ?s ?p ?o } LIMIT 2 OFFSET 1",
      "SELECT ?s WHERE { ?s ?p ?o } OFFSET 10",
      "SELECT * WHERE { ?s ?p ?o BIND(\"x,y\" AS ?x) }",
      "SELECT * WHERE { ?s <doesNotExist> ?o }"};
  for (const auto& query : queries) {
    const std::string expected = runCsvExport(query, std::nullopt);
    for (size_t rowsPerChunk : {size_t{1}, size_t{2}, size_t{3},
                                SelectCsvStreamer::defaultRowsPerChunk}) {
      EXPECT_EQ(runCsvExport(query, rowsPerChunk), expected)
          << query << " with rowsPerChunk=" << rowsPerChunk;
    }
  }
}

// _____________________________________________________________________________
TEST(SelectCsvStreamer, explicitCsvBytes) {
  EXPECT_EQ(
      runCsvExport("SELECT ?s ?o WHERE { ?s <p> ?o } ORDER BY ?s", size_t{2}),
      "s,o\ns1,\"a,b\"\ns2,\"c\"\"d\"\ns3,42\n");
}

// _____________________________________________________________________________
TEST(SelectCsvStreamer, contractChecks) {
  auto qec = ad_utility::testing::getQec(
      ad_utility::testing::TestIndexConfig{std::string{kg}});
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = ad_utility::testing::parseQuery("SELECT * WHERE { ?s ?p ?o }");
  auto qet = qp.createExecutionTree(pq);
  EXPECT_ANY_THROW(SelectCsvStreamer::run(qet, pq, 0, cancellationHandle));
  EXPECT_ANY_THROW(SelectCsvStreamer::run(qet, pq, 1, nullptr));
}

// _____________________________________________________________________________
TEST(CsvChunkSink, appendRowsWritesSelectedRowRange) {
  auto qec = ad_utility::testing::getQec(
      ad_utility::testing::TestIndexConfig{std::string{kg}});
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  const Id s1 = getId("<s1>");
  const Id s2 = getId("<s2>");
  const Id undef = Id::makeUndefined();
  IdTable table = makeIdTableFromVector({{s1, s2}, {s2, undef}, {s1, s1}});
  LocalVocab vocab;

  // Columns in reverse order, one variable that is not in the table.
  QueryExecutionTree::ColumnIndicesAndTypes columns{
      QueryExecutionTree::VariableAndColumnIndex{"?b", 1}, std::nullopt,
      QueryExecutionTree::VariableAndColumnIndex{"?a", 0}};
  CsvChunkSink sink{qec->getIndex(), std::move(columns)};

  std::string out = "prefix\n";
  sink.appendRows(table.asStaticView<0>(), vocab,
                  ql::ranges::iota_view<uint64_t, uint64_t>{1, 3}, out);
  EXPECT_EQ(out, "prefix\n,,s2\ns1,,s1\n");

  // An empty row range appends nothing.
  sink.appendRows(table.asStaticView<0>(), vocab,
                  ql::ranges::iota_view<uint64_t, uint64_t>{2, 2}, out);
  EXPECT_EQ(out, "prefix\n,,s2\ns1,,s1\n");
}

// _____________________________________________________________________________
TEST(CsvChunkSink, columnOutOfRangeIsContractViolation) {
  auto qec = ad_utility::testing::getQec(
      ad_utility::testing::TestIndexConfig{std::string{kg}});
  IdTable table = makeIdTableFromVector({{1}});
  QueryExecutionTree::ColumnIndicesAndTypes columns{
      QueryExecutionTree::VariableAndColumnIndex{"?a", 3}};
  CsvChunkSink sink{qec->getIndex(), std::move(columns)};
  std::string out;
  LocalVocab vocab;
  EXPECT_ANY_THROW(
      sink.appendRows(table.asStaticView<0>(), vocab,
                      ql::ranges::iota_view<uint64_t, uint64_t>{0, 1}, out));
}

}  // namespace
