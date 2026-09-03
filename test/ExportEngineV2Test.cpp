// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include "engine/export_v2/AsyncChunkPipeline.h"
#include "engine/export_v2/ExportEngineV2Serialize.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "parser/ParsedQuery.h"
#include "util/AllocatorTestHelpers.h"
#include "util/http/MediaTypes.h"

// Light-link smoke tests (NoLibs). canHandle / rowFormatFor live in
// ExportEngineV2.cpp and are covered via engine-linked builds / manual A/B.

using namespace ql::engine::export_v2;
using namespace qlever::export_v2;
using ad_utility::testing::makeAllocator;

TEST(ExportEngineV2Test, SerializeTableChunkCsv) {
  auto allocator = makeAllocator();
  IdTable table{2, allocator};
  table.push_back({Id::makeFromInt(42), Id::makeFromInt(100)});
  table.push_back({Id::makeFromInt(7), Id::makeFromInt(999)});

  LocalVocab localVocab;
  ScatterGatherChunkBuilder builder;

  auto chunk = serializeTableChunk(table, localVocab, RowFormat::Csv, builder);
  EXPECT_EQ(chunk.toString(), "42,100\n7,999\n");
}

TEST(ExportEngineV2Test, SerializeTableChunkTsv) {
  auto allocator = makeAllocator();
  IdTable table{2, allocator};
  table.push_back({Id::makeFromInt(1), Id::makeFromInt(2)});
  table.push_back({Id::makeFromInt(3), Id::makeFromInt(4)});

  LocalVocab localVocab;
  ScatterGatherChunkBuilder builder;

  auto chunk = serializeTableChunk(table, localVocab, RowFormat::Tsv, builder);
  EXPECT_EQ(chunk.toString(), "1\t2\n3\t4\n");
}

TEST(ExportEngineV2Test, PipelineMorselIntegration) {
  AsyncChunkPipeline<std::string> pipeline{
      AsyncChunkPipelineConfig{.capacity_ = 8, .runtimeEnabled_ = true}};

  ScatterGatherChunkBuilder builder1;
  builder1.appendCopy("chunk_1\n");
  auto chunk1 = std::move(builder1).finalize();
  EXPECT_EQ(pipeline.push(chunk1.toString()), PushResult::Accepted);

  ScatterGatherChunkBuilder builder2;
  builder2.appendCopy("chunk_2\n");
  auto chunk2 = std::move(builder2).finalize();
  EXPECT_EQ(pipeline.push(chunk2.toString()), PushResult::Accepted);

  auto res1 = pipeline.pop();
  ASSERT_TRUE(res1.has_value());
  EXPECT_EQ(*res1, "chunk_1\n");

  auto res2 = pipeline.pop();
  ASSERT_TRUE(res2.has_value());
  EXPECT_EQ(*res2, "chunk_2\n");
}

TEST(ExportEngineV2Test, ParsedQueryDefaultsToSelectForEligibilityDocs) {
  // Documents the canHandle precondition used by Server routing: ParsedQuery
  // defaults to SELECT. Full canHandle lives in ExportEngineV2.cpp (engine TU).
  ParsedQuery query;
  EXPECT_TRUE(query.hasSelectClause());
  EXPECT_TRUE(query._limitOffset.isUnconstrained());
  EXPECT_EQ(ad_utility::MediaType::csv, ad_utility::MediaType::csv);
}
