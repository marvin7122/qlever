// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include "engine/export_v2/ExportEngineV2.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/AllocatorTestHelpers.h"

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

  auto chunk = ExportEngineV2::serializeTableChunk(table, localVocab, RowFormat::Csv, builder);
  EXPECT_EQ(chunk.view(), "42,100\n7,999\n");
}

TEST(ExportEngineV2Test, SerializeTableChunkTsv) {
  auto allocator = makeAllocator();
  IdTable table{2, allocator};
  table.push_back({Id::makeFromInt(1), Id::makeFromInt(2)});
  table.push_back({Id::makeFromInt(3), Id::makeFromInt(4)});

  LocalVocab localVocab;
  ScatterGatherChunkBuilder builder;

  auto chunk = ExportEngineV2::serializeTableChunk(table, localVocab, RowFormat::Tsv, builder);
  EXPECT_EQ(chunk.view(), "1\t2\n3\t4\n");
}

TEST(ExportEngineV2Test, PipelineMorselIntegration) {
  AsyncChunkPipeline pipeline{8};

  ScatterGatherChunkBuilder builder1;
  builder1.appendCopy("chunk_1\n");
  auto chunk1 = builder1.build();
  pipeline.push(0, std::string(chunk1.view()));

  ScatterGatherChunkBuilder builder2;
  builder2.appendCopy("chunk_2\n");
  auto chunk2 = builder2.build();
  pipeline.push(1, std::string(chunk2.view()));

  EXPECT_EQ(pipeline.pop(), "chunk_1\n");
  EXPECT_EQ(pipeline.pop(), "chunk_2\n");
}
