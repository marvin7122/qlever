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
  ScatterGatherArenaStreamer streamer{64 * 1024};

  auto chunk = ExportEngineV2::serializeTableChunk(table, localVocab, RowFormat::Csv, streamer);
  EXPECT_EQ(chunk.view(), "42,100\n7,999\n");
}

TEST(ExportEngineV2Test, SerializeTableChunkTsv) {
  auto allocator = makeAllocator();
  IdTable table{2, allocator};
  table.push_back({Id::makeFromInt(1), Id::makeFromInt(2)});
  table.push_back({Id::makeFromInt(3), Id::makeFromInt(4)});

  LocalVocab localVocab;
  ScatterGatherArenaStreamer streamer{64 * 1024};

  auto chunk = ExportEngineV2::serializeTableChunk(table, localVocab, RowFormat::Tsv, streamer);
  EXPECT_EQ(chunk.view(), "1\t2\n3\t4\n");
}

TEST(ExportEngineV2Test, PipelineMorselIntegration) {
  AsyncChunkPipeline<std::string> pipeline{
      AsyncChunkPipelineConfig{.capacity_ = 8, .runtimeEnabled_ = true}};
  ScatterGatherArenaStreamer streamer{64 * 1024};

  auto chunk1 = streamer.allocateChunk(64);
  chunk1.append("chunk_1\n");
  pipeline.push(std::string(chunk1.view()));

  auto chunk2 = streamer.allocateChunk(64);
  chunk2.append("chunk_2\n");
  pipeline.push(std::string(chunk2.view()));

  EXPECT_EQ(pipeline.pop(), "chunk_1\n");
  EXPECT_EQ(pipeline.pop(), "chunk_2\n");
}
