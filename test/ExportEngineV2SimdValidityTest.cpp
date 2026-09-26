// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Byte identity of the Export V2 SELECT CSV/TSV serializer with and without
// the SIMD validity-bitmask fast path (runtime parameter
// `export-v2-simd-validity-bitmask`) for `Union`/mixed columns with many
// undefined cells, as produced by wide `OPTIONAL`/`UNION` patterns.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <optional>
#include <string>
#include <vector>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/QueryPlanner.h"
#include "engine/export_v2/ExportEngineV2.h"
#include "index/ExportIds.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/ParsedQueryTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

namespace {

using ql::engine::export_v2::ExportEngineV2;

// A knowledge graph where `?opt1`/`?opt2` are bound for only a minority of
// `?s` so that a wide-`OPTIONAL` SELECT produces long runs of undefined
// cells (all-unbound 64-row SIMD batches), plus isolated bound rows so runs
// are also exercised (mixed 64-row batches).
std::string makeKg() {
  std::string kg;
  for (int i = 0; i < 300; ++i) {
    kg += absl::StrCat("<http://ex.org/s", i, "> <http://ex.org/p> ",
                       "<http://ex.org/o", i, "> .\n");
    // Only every 17th subject has the optional predicates bound, so both
    // long unbound runs and isolated bound rows occur across 64-row
    // batches.
    if (i % 17 == 0) {
      kg += absl::StrCat("<http://ex.org/s", i, "> <http://ex.org/doi> \"doi/",
                         i, "\" .\n");
    }
    if (i % 53 == 0) {
      kg += absl::StrCat("<http://ex.org/s", i,
                         "> <http://ex.org/webpage> <http://ex.org/web", i,
                         "> .\n");
    }
  }
  return kg;
}

class ExportEngineV2SimdValidity : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ = ad_utility::testing::getQec(makeKg());
};

// _____________________________________________________________________________
// End to end through `ExportEngineV2::computeResult`: the runtime parameter
// switches the validity-bitmask fast path for `Union` columns on and off,
// and both settings reproduce the Legacy bytes byte-for-byte, including all
// the undefined (unbound) cells from the two `OPTIONAL` clauses.
TEST_F(ExportEngineV2SimdValidity, RuntimeParameterKeepsLegacyBytes) {
  using enum ad_utility::MediaType;
  const std::string query =
      "SELECT ?s ?doi ?webpage WHERE { "
      "?s <http://ex.org/p> ?o . "
      "OPTIONAL { ?s <http://ex.org/doi> ?doi } "
      "OPTIONAL { ?s <http://ex.org/webpage> ?webpage } }";
  auto run = [&](ad_utility::MediaType mediaType,
                 std::optional<bool> simdValidityBitmask) {
    qec_->clearCacheUnpinnedOnly();
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    QueryPlanner qp{qec_, handle};
    auto pq = ad_utility::testing::parseQuery(query);
    auto qet = qp.createExecutionTree(pq);
    std::string result;
    if (!simdValidityBitmask.has_value()) {
      ad_utility::Timer timer{ad_utility::Timer::Started};
      for (const auto& block : ExportQueryExecutionTrees::computeResult(
               pq, qet, mediaType, timer, std::move(handle))) {
        result += block;
      }
      return result;
    }
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::exportV2SimdValidityBitmask_>(
        simdValidityBitmask.value());
    EXPECT_TRUE(ExportEngineV2::canHandle(pq, qet, mediaType));
    for (const auto& block :
         ExportEngineV2::computeResult(pq, qet, mediaType, std::move(handle))) {
      result += block;
    }
    return result;
  };
  for (auto mediaType : {csv, tsv}) {
    SCOPED_TRACE(mediaType == csv ? "csv" : "tsv");
    const auto legacy = run(mediaType, std::nullopt);
    // Sanity check: the query actually produces undefined cells, otherwise
    // this test would not exercise the fast path at all.
    EXPECT_THAT(legacy,
                ::testing::HasSubstr(mediaType == csv ? ",\n" : "\t\n"));
    const auto flagOff = run(mediaType, false);
    const auto flagOn = run(mediaType, true);
    EXPECT_EQ(flagOff, legacy);
    EXPECT_EQ(flagOn, legacy);
    EXPECT_EQ(flagOn, flagOff);
  }
}

}  // namespace
