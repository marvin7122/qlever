// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// The Export V2 SELECT CSV/TSV path with the runtime parameter
// `export-v2-async-pipeline`: the same bytes in the same order as without it,
// producer exceptions reach the consumer, and abandoning the export stops the
// producer thread.

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <algorithm>
#include <string>
#include <vector>

#include "engine/QueryPlanner.h"
#include "engine/export_v2/ExportEngineV2.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/ParsedQueryTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

namespace {

using ad_utility::MediaType;
using ad_utility::export_v2::ElasticExportScheduler;
using ql::engine::export_v2::ExportEngineV2;

// More rows than one 8192-row morsel, so the export has several chunks.
constexpr size_t kNumSubjects = 20000;

std::string makeKnowledgeGraph() {
  std::string kg;
  for (size_t i = 0; i < kNumSubjects; ++i) {
    absl::StrAppend(&kg, "<http://ex.org/s", i, "> <http://ex.org/n> ", i,
                    " . <http://ex.org/s", i, "> <http://ex.org/l> \"label, ",
                    i, "\" . ");
  }
  return kg;
}

class ExportEngineV2AsyncPipeline : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ =
      ad_utility::testing::getQec(makeKnowledgeGraph());

  struct Planned {
    ad_utility::SharedCancellationHandle handle_;
    ParsedQuery parsedQuery_;
    QueryExecutionTree qet_;
  };

  Planned plan(const std::string& query) {
    qec_->clearCacheUnpinnedOnly();
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    QueryPlanner qp{qec_, handle};
    auto pq = parseQuery(query);
    auto qet = qp.createExecutionTree(pq);
    return {std::move(handle), std::move(pq), std::move(qet)};
  }

  // The chunks of `computeResult` (or of `computeResultChunks` with
  // `scatterGather`), with the async pipeline on or off.
  std::vector<std::string> chunks(const std::string& query, MediaType mediaType,
                                  bool asyncPipeline, bool scatterGather,
                                  ElasticExportScheduler* scheduler = nullptr) {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::exportV2AsyncPipeline_>(
            asyncPipeline);
    auto planned = plan(query);
    EXPECT_TRUE(ExportEngineV2::canHandle(planned.parsedQuery_, planned.qet_,
                                          mediaType));
    std::vector<std::string> result;
    if (scatterGather) {
      for (auto& chunk : ExportEngineV2::computeResultChunks(
               planned.parsedQuery_, planned.qet_, mediaType, planned.handle_,
               scheduler)) {
        result.push_back(chunk.toString());
      }
    } else {
      for (auto& chunk : ExportEngineV2::computeResult(
               planned.parsedQuery_, planned.qet_, mediaType, planned.handle_,
               scheduler)) {
        result.push_back(std::move(chunk));
      }
    }
    return result;
  }
};

const std::vector<std::string> kQueries{
    "SELECT ?s ?n WHERE { ?s <http://ex.org/n> ?n }",
    "SELECT ?s ?l ?n WHERE { ?s <http://ex.org/l> ?l . ?s <http://ex.org/n> "
    "?n }",
    "SELECT ?s ?n WHERE { ?s <http://ex.org/n> ?n } LIMIT 15000 OFFSET 7",
};

// _____________________________________________________________________________
TEST_F(ExportEngineV2AsyncPipeline, SameChunksInSameOrder) {
  for (const auto& query : kQueries) {
    for (auto mediaType : {MediaType::csv, MediaType::tsv}) {
      for (bool scatterGather : {false, true}) {
        SCOPED_TRACE(absl::StrCat(query, " ", ad_utility::toString(mediaType),
                                  scatterGather ? " iovec" : " string"));
        const auto expected = chunks(query, mediaType, false, scatterGather);
        ASSERT_GT(expected.size(), 2);
        EXPECT_EQ(chunks(query, mediaType, true, scatterGather), expected);
      }
    }
  }
}

// _____________________________________________________________________________
// With the scheduler, unbounded queries emit morsels in completion order, so
// only bounded (ordered) queries compare chunk by chunk; the others compare
// the concatenated rows as a multiset.
TEST_F(ExportEngineV2AsyncPipeline, SameRowsWithScheduler) {
  ElasticExportScheduler scheduler(2, 64);
  auto sortedLines = [](const std::vector<std::string>& parts) {
    std::vector<std::string> lines;
    for (const auto& part : parts) {
      for (auto line : absl::StrSplit(part, '\n', absl::SkipEmpty())) {
        lines.emplace_back(line);
      }
    }
    ql::ranges::sort(lines);
    return lines;
  };
  for (const auto& query : kQueries) {
    SCOPED_TRACE(query);
    const auto expected = chunks(query, MediaType::tsv, false, false);
    const auto async = chunks(query, MediaType::tsv, true, false, &scheduler);
    if (query.find("LIMIT") != std::string::npos) {
      EXPECT_EQ(absl::StrJoin(async, ""), absl::StrJoin(expected, ""));
    } else {
      EXPECT_EQ(sortedLines(async), sortedLines(expected));
    }
  }
}

// _____________________________________________________________________________
// A cancelled query surfaces as the producer's exception on the consumer,
// after the chunks produced before it (the header).
TEST_F(ExportEngineV2AsyncPipeline, CancellationReachesConsumer) {
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::exportV2AsyncPipeline_>(
          true);
  auto planned = plan(kQueries.at(0));
  planned.handle_->cancel(ad_utility::CancellationState::MANUAL);
  auto generator = ExportEngineV2::computeResult(
      planned.parsedQuery_, planned.qet_, MediaType::tsv, planned.handle_);
  auto it = generator.begin();
  ASSERT_NE(it, generator.end());
  EXPECT_EQ(*it, "?s\t?n\n");
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(++it, ::testing::_,
                                        ad_utility::CancellationException);
}

// _____________________________________________________________________________
// Destroying the generator mid-export (the client abandons the download)
// joins the producer thread instead of terminating the process.
TEST_F(ExportEngineV2AsyncPipeline, AbandonedExportStopsProducer) {
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::exportV2AsyncPipeline_>(
          true);
  for (bool scatterGather : {false, true}) {
    auto planned = plan(kQueries.at(1));
    if (scatterGather) {
      auto generator = ExportEngineV2::computeResultChunks(
          planned.parsedQuery_, planned.qet_, MediaType::csv, planned.handle_);
      EXPECT_NE(generator.begin(), generator.end());
    } else {
      auto generator = ExportEngineV2::computeResult(
          planned.parsedQuery_, planned.qet_, MediaType::csv, planned.handle_);
      EXPECT_NE(generator.begin(), generator.end());
    }
  }
}

}  // namespace
