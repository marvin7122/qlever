// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

// Byte identity of the Export V2 SELECT CSV/TSV serializer with and without
// the SIMD validity-bitmask fast path (runtime parameter
// `export-v2-simd-validity-bitmask`) for `Union`/mixed columns with many
// undefined cells, as produced by wide `OPTIONAL`/`UNION` patterns.
//
// NOTE (stack port): fork #226 tested this end to end through
// `ExportEngineV2::computeResult` with a wide-`OPTIONAL` query. On this stack
// tip `ExportEngineV2::canHandle` only accepts scans, joins, filters, BIND,
// inline VALUES, and internal sorts, so `OPTIONAL`/`UNION` plans still fall
// back to Legacy and never reach the fast path end to end. This port therefore
// exercises the same wiring point (`appendSerializedRows`, the only caller of
// `resolveColumnWithValidityFastPath`) directly with `Union`-lattice columns
// that hold long unbound runs, isolated bound rows, and a short tail.

#include <gmock/gmock.h>

#include <optional>
#include <string>
#include <vector>

#include "engine/SimdValidityBitmask.h"
#include "engine/export_v2/ExportEngineV2.h"
#include "index/ExportIds.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

namespace {

using ql::engine::export_v2::ColumnLattice;
using ql::engine::export_v2::ExportEngineV2;
using ql::engine::export_v2::RowFormat;
using ql::engine::export_v2::ScatterGatherChunkBuilder;
using Selected = std::vector<std::optional<ColumnIndex>>;

constexpr std::string_view kg =
    "<http://ex.org/s> <http://ex.org/p> <http://ex.org/o> .\n"
    "<http://ex.org/s> <http://ex.org/doi> \"doi/10.1234/ex\" .\n"
    "<http://ex.org/s> <http://ex.org/webpage> <http://ex.org/web> .\n";

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
                   ql::span<const ColumnLattice> lattice, bool monomorphic) {
  ScatterGatherChunkBuilder builder;
  ExportEngineV2::appendSerializedRows(table.asStaticView<0>(), localVocab,
                                       format, builder, index, selected,
                                       rowBegin, rowEnd, lattice, monomorphic);
  return std::move(builder).finalizeToString();
}

class ExportEngineV2SimdValidity : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ = ad_utility::testing::getQec(std::string{kg});
  const Index& index_ = qec_->getIndex();
  std::function<Id(const std::string&)> getId_ =
      ad_utility::testing::makeGetId(index_);
  LocalVocab localVocab_;

  // Two `Union` columns with long runs of undefined cells across 64-row SIMD
  // batches: column 0 is bound for every 17th row (isolated bound rows inside
  // mixed batches), column 1 only inside the first batch (later batches are
  // entirely unbound, so the fast path skips them). 300 rows also leave a
  // short (< 64) tail, which always goes through the general resolver.
  IdTable wideOptionalTable() {
    IdTable table{2, ad_utility::testing::makeAllocator()};
    const Id iri = getId_("<http://ex.org/o>");
    const Id literal = getId_("\"doi/10.1234/ex\"");
    for (size_t row = 0; row < 300; ++row) {
      table.emplace_back();
      table.back()[0] =
          row % 17 == 0 ? (row % 34 == 0 ? iri : literal) : Id::makeUndefined();
      table.back()[1] = row < 64 && row % 7 == 0 ? iri : Id::makeUndefined();
    }
    return table;
  }
};

// _____________________________________________________________________________
// The scanner pins the skip condition the serializer relies on: an
// all-unbound 64-batch is detected, a batch with a single bound cell is not.
TEST_F(ExportEngineV2SimdValidity, ScannerDetectsAllUnboundBatch) {
  using ad_utility::simd::SimdValidityScanner;
  std::vector<Id> unbound(64, Id::makeUndefined());
  EXPECT_TRUE(SimdValidityScanner::isAllUnbound64(unbound.data()));
  unbound[63] = getId_("<http://ex.org/o>");
  EXPECT_FALSE(SimdValidityScanner::isAllUnbound64(unbound.data()));
}

// _____________________________________________________________________________
// For `Union` columns with long unbound runs, the runtime parameter changes
// no byte: flag off, flag on, generic, and monomorphic all match Legacy.
TEST_F(ExportEngineV2SimdValidity, RuntimeParameterKeepsLegacyBytes) {
  const IdTable table = wideOptionalTable();
  // Sanity check: the table actually mixes bound and unbound cells, otherwise
  // this test would not exercise the fast path at all.
  size_t boundCol0 = 0, boundCol1 = 0;
  for (size_t row = 0; row < table.numRows(); ++row) {
    boundCol0 += table(row, 0).getDatatype() == Datatype::Undefined ? 0 : 1;
    boundCol1 += table(row, 1).getDatatype() == Datatype::Undefined ? 0 : 1;
  }
  EXPECT_GT(boundCol0, 0u);
  EXPECT_GT(boundCol1, 0u);
  EXPECT_LT(boundCol0, table.numRows());
  EXPECT_LT(boundCol1, table.numRows());

  const Selected selected{0, 1};
  const std::vector<ColumnLattice> lattice{ColumnLattice::Union,
                                           ColumnLattice::Union};
  for (RowFormat format : {RowFormat::Csv, RowFormat::Tsv}) {
    SCOPED_TRACE(format == RowFormat::Csv ? "csv" : "tsv");
    const auto legacy = legacyRows(table, localVocab_, format, index_, selected,
                                   0, table.numRows());
    // The wide-`OPTIONAL` shape really produces empty (unbound) cells.
    EXPECT_THAT(legacy, ::testing::HasSubstr(
                            format == RowFormat::Csv ? ",\n" : "\t\n"));
    for (bool monomorphic : {false, true}) {
      SCOPED_TRACE(monomorphic ? "monomorphic" : "generic");
      {
        auto flagOff = setRuntimeParameterForTest<
            &RuntimeParameters::exportV2SimdValidityBitmask_>(false);
        EXPECT_EQ(v2Rows(table, localVocab_, format, index_, selected, 0,
                         table.numRows(), lattice, monomorphic),
                  legacy);
      }
      {
        auto flagOn = setRuntimeParameterForTest<
            &RuntimeParameters::exportV2SimdValidityBitmask_>(true);
        EXPECT_EQ(v2Rows(table, localVocab_, format, index_, selected, 0,
                         table.numRows(), lattice, monomorphic),
                  legacy);
      }
    }
  }
}

}  // namespace
