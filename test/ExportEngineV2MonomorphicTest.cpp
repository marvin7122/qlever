// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Byte identity of the Export V2 SELECT CSV/TSV serializer with and without
// `MonomorphicRowSerializer` (runtime parameter `export-v2-monomorphic-rows`),
// and of both against the Legacy per-cell conversion.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <optional>
#include <string>
#include <vector>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryPlanner.h"
#include "engine/export_v2/ExportEngineV2.h"
#include "index/ExportIds.h"
#include "index/LocalVocabEntry.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/DateYearDuration.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/ParsedQueryTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

namespace {

using ql::engine::export_v2::ColumnLattice;
using ql::engine::export_v2::ExportEngineV2;
using ql::engine::export_v2::RowFormat;
using ql::engine::export_v2::ScatterGatherChunkBuilder;
using Selected = std::vector<std::optional<ColumnIndex>>;

constexpr std::string_view kg =
    "<http://ex.org/s1> <http://ex.org/p> <http://ex.org/o,1> .\n"
    "<http://ex.org/s1> <http://ex.org/p> \"plain\" .\n"
    "<http://ex.org/s1> <http://ex.org/p> \"hallo\"@de .\n"
    "<http://ex.org/s1> <http://ex.org/p> \"has \\\"quotes\\\", comma\" .\n"
    "<http://ex.org/s1> <http://ex.org/p> \"tab\\tand\\nnewline\" .\n"
    "<http://ex.org/s1> <http://ex.org/p> \"custom\"^^<http://ex.org/dt> .\n"
    "<http://ex.org/s1> <http://ex.org/p> 42 .\n"
    "<http://ex.org/s1> <http://ex.org/p> -7.25 .\n"
    "<http://ex.org/s1> <http://ex.org/p> "
    "\"2020-01-15\"^^<http://www.w3.org/2001/XMLSchema#date> .\n"
    "<http://ex.org/s1> <http://ex.org/p> true .\n"
    "<http://ex.org/s1> <http://ex.org/p> _:b0 .\n"
    "<http://ex.org/s2> <http://ex.org/num> 0 .\n"
    "<http://ex.org/s3> <http://ex.org/num> -12345 .\n"
    "<http://ex.org/s4> <http://ex.org/num> 999999 .\n"
    "<http://ex.org/s2> <http://ex.org/dbl> \"1.0\"^^"
    "<http://www.w3.org/2001/XMLSchema#double> .\n"
    "<http://ex.org/s3> <http://ex.org/dbl> \"1e300\"^^"
    "<http://www.w3.org/2001/XMLSchema#double> .\n"
    "<http://ex.org/s4> <http://ex.org/dbl> \"0.1\"^^"
    "<http://www.w3.org/2001/XMLSchema#double> .\n"
    "<http://ex.org/s2> <http://ex.org/bool> false .\n"
    "<http://ex.org/s3> <http://ex.org/bool> true .\n"
    "<http://ex.org/s4> <http://ex.org/bool> false .\n";

// The Legacy SELECT CSV/TSV bytes for `table` (see
// `ExportQueryExecutionTrees::selectQueryResultToStream`).
std::string legacyRows(const IdTable& table, const LocalVocab& localVocab,
                       RowFormat format, const Index& index,
                       const Selected& selected, size_t rowBegin,
                       size_t rowEnd) {
  std::string out;
  const char separator = format == RowFormat::Csv ? ',' : '\t';
  for (size_t row = rowBegin; row < rowEnd; ++row) {
    for (size_t j = 0; j < selected.size(); ++j) {
      if (selected[j].has_value()) {
        const Id id = table(row, selected[j].value());
        auto cell = format == RowFormat::Csv
                        ? ql::exportIds::idToStringAndType<true>(
                              index, id, localVocab, RdfEscaping::escapeForCsv)
                        : ql::exportIds::idToStringAndType<false>(
                              index, id, localVocab, RdfEscaping::escapeForTsv);
        if (cell.has_value()) {
          out += cell.value().first;
        }
      }
      if (j + 1 < selected.size()) {
        out.push_back(separator);
      }
    }
    out.push_back('\n');
  }
  return out;
}

std::string v2Rows(const IdTable& table, const LocalVocab& localVocab,
                   RowFormat format, const Index& index,
                   const Selected& selected, size_t rowBegin, size_t rowEnd,
                   bool monomorphic,
                   ql::span<const ColumnLattice> lattice = {}) {
  ScatterGatherChunkBuilder builder;
  ExportEngineV2::appendSerializedRows(table.asStaticView<0>(), localVocab,
                                       format, builder, index, selected,
                                       rowBegin, rowEnd, lattice, monomorphic);
  return std::move(builder).finalizeToString();
}

// Check every window of `table` (all rows, and each window of `windowSize`
// rows, so a column can be uniform in one window and mixed in another) in
// both formats: monomorphic == generic == Legacy.
void expectIdenticalBytes(
    const IdTable& table, const LocalVocab& localVocab, const Index& index,
    const Selected& selected, size_t windowSize,
    ql::span<const ColumnLattice> lattice = {},
    ad_utility::source_location l = ad_utility::source_location::current()) {
  auto trace = generateLocationTrace(l);
  const size_t numRows = table.numRows();
  std::vector<std::pair<size_t, size_t>> windows{{0, numRows}};
  for (size_t begin = 0; begin < numRows; begin += windowSize) {
    windows.emplace_back(begin, std::min(numRows, begin + windowSize));
  }
  for (RowFormat format : {RowFormat::Csv, RowFormat::Tsv}) {
    for (const auto& [begin, end] : windows) {
      const auto legacy =
          legacyRows(table, localVocab, format, index, selected, begin, end);
      const auto generic = v2Rows(table, localVocab, format, index, selected,
                                  begin, end, false, lattice);
      const auto monomorphic = v2Rows(table, localVocab, format, index,
                                      selected, begin, end, true, lattice);
      SCOPED_TRACE(absl::StrCat("format ",
                                format == RowFormat::Csv ? "csv" : "tsv",
                                " rows [", begin, ", ", end, ")"));
      EXPECT_EQ(generic, legacy);
      EXPECT_EQ(monomorphic, generic);
    }
  }
}

class ExportEngineV2Monomorphic : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ = ad_utility::testing::getQec(std::string{kg});
  const Index& index_ = qec_->getIndex();
  std::function<Id(const std::string&)> getId_ =
      ad_utility::testing::makeGetId(index_);
  LocalVocab localVocab_;

  Id localId(std::string representation) {
    return Id::makeFromLocalVocabIndex(localVocab_.getIndexAndAddIfNotContained(
        LocalVocabEntry::fromStringRepresentation(
            std::move(representation), qec_->getLocalVocabContext())));
  }

  // One id of every datatype the SELECT export can meet.
  std::vector<Id> mixedIds() {
    return {
        getId_("<http://ex.org/o,1>"),
        getId_("\"plain\""),
        getId_("\"hallo\"@de"),
        getId_("\"has \"quotes\", comma\""),
        getId_("\"tab\tand\nnewline\""),
        getId_("\"custom\"^^<http://ex.org/dt>"),
        Id::makeFromInt(42),
        Id::makeFromInt(-1),
        Id::makeFromDouble(-7.25),
        Id::makeFromDouble(1.0),
        Id::makeFromDouble(1e300),
        Id::makeFromDouble(std::numeric_limits<double>::quiet_NaN()),
        Id::makeFromDouble(-std::numeric_limits<double>::infinity()),
        Id::makeFromBool(true),
        Id::makeBoolFromZeroOrOne(false),
        Id::makeFromDate(DateYearOrDuration::parseXsdDate("2020-01-15")),
        Id::makeFromBlankNodeIndex(BlankNodeIndex::make(7)),
        Id::makeUndefined(),
        localId("<http://ex.org/local,iri>"),
        localId("\"local \"lit\", with comma\"@en"),
        localId("\"local\ttab\""),
    };
  }
};

// _____________________________________________________________________________
TEST_F(ExportEngineV2Monomorphic, MixedDatatypeColumns) {
  const auto ids = mixedIds();
  IdTable table{3, ad_utility::testing::makeAllocator()};
  // Three columns, each a rotation of all datatypes: every column is mixed
  // over the whole table, and each row combines different datatypes.
  for (size_t row = 0; row < ids.size(); ++row) {
    table.emplace_back();
    for (size_t col = 0; col < 3; ++col) {
      table.back()[col] = ids[(row + 5 * col) % ids.size()];
    }
  }
  for (size_t windowSize : {1, 2, 5}) {
    expectIdenticalBytes(table, localVocab_, index_, Selected{0, 1, 2},
                         windowSize);
    expectIdenticalBytes(table, localVocab_, index_,
                         Selected{2, std::nullopt, 0}, windowSize);
    expectIdenticalBytes(table, localVocab_, index_, Selected{1}, windowSize);
  }
}

// _____________________________________________________________________________
TEST_F(ExportEngineV2Monomorphic, UniformColumnsUseDirectCells) {
  // Column 0: Int, 1: Double, 2: Bool, 3: Undefined, 4: Date, 5: vocab.
  IdTable table{6, ad_utility::testing::makeAllocator()};
  const std::vector<int64_t> ints{0, -12345, 999999, -1, 7};
  const std::vector<double> doubles{1.0, 0.1, -0.0, 1e300, 153.07};
  for (size_t row = 0; row < ints.size(); ++row) {
    table.emplace_back();
    table.back()[0] = Id::makeFromInt(ints[row]);
    table.back()[1] = Id::makeFromDouble(doubles[row]);
    table.back()[2] = row % 2 == 0 ? Id::makeFromBool(row % 4 == 0)
                                   : Id::makeBoolFromZeroOrOne(true);
    table.back()[3] = Id::makeUndefined();
    table.back()[4] = Id::makeFromDate(DateYearOrDuration::parseXsdDate(
        absl::StrCat("20", 10 + row, "-02-0", 1 + row)));
    table.back()[5] = row % 2 == 0 ? getId_("\"has \"quotes\", comma\"")
                                   : getId_("<http://ex.org/o,1>");
  }
  for (size_t windowSize : {1, 3}) {
    for (const Selected& selected :
         {Selected{0}, Selected{1}, Selected{2}, Selected{3}, Selected{4},
          Selected{0, 1, 2}, Selected{3, 0, 5}, Selected{5, 1},
          Selected{4, std::nullopt, 2},
          // More columns than `MonomorphicRowSerializer` instantiates: the
          // generic loop serves them.
          Selected{0, 1, 2, 3, 4, 5}}) {
      expectIdenticalBytes(table, localVocab_, index_, selected, windowSize);
    }
  }
  // A proven-trivial plan-time lattice skips the uniformity scan.
  const std::vector<ColumnLattice> lattice{
      ColumnLattice::Int, ColumnLattice::Double, ColumnLattice::Bool};
  expectIdenticalBytes(table, localVocab_, index_, Selected{0, 1, 2}, 2,
                       lattice);
}

// _____________________________________________________________________________
// End to end through `ExportEngineV2::computeResult`: the runtime parameter
// switches the serializer, and both settings reproduce the Legacy bytes.
TEST_F(ExportEngineV2Monomorphic, RuntimeParameterKeepsLegacyBytes) {
  using enum ad_utility::MediaType;
  auto run = [this](const std::string& query, ad_utility::MediaType mediaType,
                    std::optional<bool> monomorphic) {
    qec_->clearCacheUnpinnedOnly();
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    QueryPlanner qp{qec_, handle};
    auto pq = ad_utility::testing::parseQuery(query);
    auto qet = qp.createExecutionTree(pq);
    std::string result;
    if (!monomorphic.has_value()) {
      ad_utility::Timer timer{ad_utility::Timer::Started};
      for (const auto& block : ExportQueryExecutionTrees::computeResult(
               pq, qet, mediaType, timer, std::move(handle))) {
        result += block;
      }
      return result;
    }
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::exportV2MonomorphicRows_>(monomorphic.value());
    EXPECT_TRUE(ExportEngineV2::canHandle(pq, qet, mediaType));
    for (const auto& block :
         ExportEngineV2::computeResult(pq, qet, mediaType, std::move(handle))) {
      result += block;
    }
    return result;
  };
  for (const std::string query :
       {"SELECT ?s ?o WHERE { ?s <http://ex.org/p> ?o }",
        "SELECT ?s ?n WHERE { ?s <http://ex.org/num> ?n }",
        "SELECT ?n ?d ?b WHERE { ?s <http://ex.org/num> ?n . "
        "?s <http://ex.org/dbl> ?d . ?s <http://ex.org/bool> ?b }",
        "SELECT ?o ?unbound WHERE { ?s <http://ex.org/p> ?o }",
        "SELECT ?s ?n ?d ?b WHERE { ?s <http://ex.org/num> ?n . "
        "?s <http://ex.org/dbl> ?d . ?s <http://ex.org/bool> ?b }"}) {
    for (auto mediaType : {csv, tsv}) {
      SCOPED_TRACE(query);
      const auto legacy = run(query, mediaType, std::nullopt);
      EXPECT_EQ(run(query, mediaType, false), legacy);
      EXPECT_EQ(run(query, mediaType, true), legacy);
    }
  }
}

}  // namespace
