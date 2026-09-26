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

// A small knowledge graph (the test index builder writes a few files per
// two-triple batch); the queries below take the cross product of three scans
// to get more rows than several 8192-row morsels (30^3 = 27000).
constexpr size_t kNumSubjects = 30;

std::string makeKnowledgeGraph() {
  std::string kg;
  for (size_t i = 0; i < kNumSubjects; ++i) {
    absl::StrAppend(&kg, "<http://ex.org/s", i, "> <http://ex.org/n> ", i,
                    " .\n<http://ex.org/s", i, "> <http://ex.org/l> \"label, ",
                    i, "\" .\n");
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
    auto pq = ad_utility::testing::parseQuery(query);
    auto qet = qp.createExecutionTree(pq);
    return {std::move(handle), std::move(pq), std::move(qet)};
  }

  // The chunks of `computeResult` (or of `computeResultChunks` with
  // `scatterGather`) for an already planned query, with the async pipeline on
  // or off. Both arms of a comparison use the same plan: the planner may
  // order the children of a cross product differently between two plannings,
  // which changes the row order independently of the pipeline. The cache is
  // cleared so that every run streams the same lazy blocks.
  std::vector<std::string> chunks(const Planned& planned, MediaType mediaType,
                                  bool asyncPipeline, bool scatterGather,
                                  ElasticExportScheduler* scheduler = nullptr) {
    qec_->clearCacheUnpinnedOnly();
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::exportV2AsyncPipeline_>(
            asyncPipeline);
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
    "SELECT ?a ?x WHERE { ?a <http://ex.org/n> ?x . ?b <http://ex.org/n> ?y . "
    "?c <http://ex.org/n> ?z }",
    "SELECT ?a ?l ?y WHERE { ?a <http://ex.org/l> ?l . ?b <http://ex.org/n> ?y "
    ". ?c <http://ex.org/n> ?z }",
    "SELECT ?a ?x WHERE { ?a <http://ex.org/n> ?x . ?b <http://ex.org/n> ?y . "
    "?c <http://ex.org/n> ?z } LIMIT 15000 OFFSET 7",
};

// _____________________________________________________________________________
TEST_F(ExportEngineV2AsyncPipeline, SameChunksInSameOrder) {
  for (const auto& query : kQueries) {
    for (auto mediaType : {MediaType::csv, MediaType::tsv}) {
      for (bool scatterGather : {false, true}) {
        SCOPED_TRACE(absl::StrCat(query, " ", ad_utility::toString(mediaType),
                                  scatterGather ? " iovec" : " string"));
        const auto planned = plan(query);
        const auto expected = chunks(planned, mediaType, false, scatterGather);
        ASSERT_GT(expected.size(), 2);
        EXPECT_EQ(chunks(planned, mediaType, true, scatterGather), expected);
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
    const auto planned = plan(query);
    const auto expected = chunks(planned, MediaType::tsv, false, false);
    const auto async = chunks(planned, MediaType::tsv, true, false, &scheduler);
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
  EXPECT_EQ(*it, "?a\t?x\n");
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
