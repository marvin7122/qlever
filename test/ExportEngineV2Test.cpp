// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>
#include <string>

#include "engine/export_v2/AsyncChunkPipeline.h"
#include "engine/export_v2/ExportEngineV2Serialize.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/LocalVocabContext.h"
#include "util/AllocatorTestHelpers.h"
#include "util/GTestHelpers.h"

// Light-link smoke tests (NoLibs). Do not include ParsedQuery /
// ExportEngineV2.h here — those pull GraphPatternOperation + range-v3 paths
// that fail on cluster GCC 11. canHandle / live Server wiring are verified via
// qlever-server builds.

using namespace ql::engine::export_v2;
using namespace qlever::export_v2;
using ad_utility::testing::makeAllocator;

namespace {
class MockLocalVocabContext : public LocalVocabContext {
 public:
  using VocabBounds = LocalVocabContext::VocabBounds;
  MOCK_METHOD(int, compareWords, (std::string_view, std::string_view),
              (const, override));
  MOCK_METHOD(VocabBounds, getPositionOfWord, (std::string_view),
              (const, override));
  MOCK_METHOD(bool, hasSecondaryVocabulary, (), (const, override));
  MOCK_METHOD(std::optional<SecondaryVocabIndex>, getSecondaryVocabIndex,
              (std::string_view), (const, override));
  MOCK_METHOD(std::optional<Id>, encodeAsId, (std::string_view),
              (const, override));
  MOCK_METHOD(ad_utility::BlankNodeManager*, getBlankNodeManager, (),
              (const, override));
};
}  // namespace

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

TEST(ExportEngineV2Test, SerializeTableChunkRendersIndexFreeDatatypes) {
  auto allocator = makeAllocator();
  IdTable table{3, allocator};
  table.push_back(
      {Id::makeFromBool(true), Id::makeFromDouble(1.5), Id::makeUndefined()});

  LocalVocab localVocab;
  ScatterGatherChunkBuilder builder;

  auto chunk = serializeTableChunk(table, localVocab, RowFormat::Csv, builder);
  EXPECT_EQ(chunk.toString(), "true,1.5,\n");
}

TEST(ExportEngineV2Test, SerializeTableChunkRendersLegacyDoubles) {
  auto allocator = makeAllocator();
  IdTable table{8, allocator};
  constexpr double infinity = std::numeric_limits<double>::infinity();
  table.push_back({Id::makeFromDouble(1.5), Id::makeFromDouble(1.0),
                   Id::makeFromDouble(-0.0), Id::makeFromDouble(0.25),
                   Id::makeFromDouble(std::numeric_limits<double>::quiet_NaN()),
                   Id::makeFromDouble(infinity), Id::makeFromDouble(-infinity),
                   Id::makeFromDouble(1e-20)});

  LocalVocab localVocab;
  for (RowFormat format : {RowFormat::Csv, RowFormat::Tsv}) {
    SCOPED_TRACE(static_cast<int>(format));
    ScatterGatherChunkBuilder builder;
    auto chunk = serializeTableChunk(table, localVocab, format, builder);
    EXPECT_EQ(chunk.toString(),
              format == RowFormat::Csv
                  ? "1.5,1.0,-0.0,0.25,NaN,INF,-INF,1e-20\n"
                  : "1.5\t1.0\t-0.0\t0.25\tNaN\tINF\t-INF\t1e-20\n");
  }
}

TEST(ExportEngineV2Test, SerializeTableChunkRendersLocalVocabForEachFormat) {
  // No context calls are expected: serialization only reads the stored word.
  ::testing::StrictMock<MockLocalVocabContext> context;
  LocalVocab localVocab;
  auto allocator = makeAllocator();
  IdTable table{2, allocator};
  int64_t row = 0;
  for (std::string_view representation :
       {"\"plain\"", "\"bonjour\"@fr", "\"typed\"^^<http://example.org/type>",
        "<http://example.org/iri>", "\"\"", "\"comma,\"quote\"\nline\ttab\""}) {
    auto index = localVocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::fromStringRepresentation(std::string{representation},
                                                  context));
    table.push_back({Id::makeFromInt(row), Id::makeFromLocalVocabIndex(index)});
    ++row;
  }

  ScatterGatherChunkBuilder csvBuilder;
  auto csv = serializeTableChunk(table, localVocab, RowFormat::Csv, csvBuilder);
  EXPECT_EQ(csv.toString(),
            "0,plain\n1,bonjour\n2,typed\n3,http://example.org/iri\n4,\n"
            "5,\"comma,\"\"quote\"\"\nline\ttab\"\n");

  ScatterGatherChunkBuilder tsvBuilder;
  auto tsv = serializeTableChunk(table, localVocab, RowFormat::Tsv, tsvBuilder);
  EXPECT_EQ(tsv.toString(),
            "0\t\"plain\"\n1\t\"bonjour\"@fr\n"
            "2\t\"typed\"^^<http://example.org/type>\n"
            "3\t<http://example.org/iri>\n4\t\"\"\n"
            "5\t\"comma,\"quote\"\\nline tab\"\n");
}

TEST(ExportEngineV2Test, SerializeTableChunkRejectsIndexBackedIds) {
  auto allocator = makeAllocator();
  IdTable table{1, allocator};
  table.push_back({Id::makeFromVocabIndex(VocabIndex::make(0))});

  LocalVocab localVocab;
  ScatterGatherChunkBuilder builder;

  // Index-backed IDs need the index vocabulary (`index/ExportIds.h`); this
  // lightweight serializer must fail loudly instead of emitting placeholder
  // text.
  AD_EXPECT_THROW_WITH_MESSAGE(
      serializeTableChunk(table, localVocab, RowFormat::Csv, builder),
      ::testing::HasSubstr("index-backed"));
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
