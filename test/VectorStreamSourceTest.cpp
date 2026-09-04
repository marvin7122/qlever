// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gmock/gmock.h>

#include <array>
#include <vector>

#include "engine/export_v2/VectorStreamSource.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

namespace {

using ql::engine::export_v2::EqualityFilter;
using ql::engine::export_v2::RowsPerChunk;
using ql::engine::export_v2::VectorStreamConfig;
using ql::engine::export_v2::VectorStreamSource;
using Pair = Result::IdTableVocabPair;

Pair makeBlock(const VectorTable& rows) {
  return {makeIdTableFromVector(rows, ad_utility::testing::IntId),
          LocalVocab{}};
}

Pair makeEmptyBlock(size_t numColumns) {
  return {IdTable{numColumns, ad_utility::testing::makeAllocator()},
          LocalVocab{}};
}

std::vector<Pair> collect(const VectorStreamSource& source,
                          std::vector<Pair>& blocks,
                          ql::span<const EqualityFilter> filters = {}) {
  std::vector<Pair> result;
  source.run(
      blocks,
      [&result](const Pair& chunk) {
        result.emplace_back(chunk.idTable_.clone(), chunk.localVocab_.clone());
      },
      filters);
  return result;
}

TEST(VectorStreamSource, RejectsZeroRowsPerChunk) {
  EXPECT_ANY_THROW(RowsPerChunk{0});
}

TEST(VectorStreamSource, EmptyInputProducesNoChunks) {
  VectorStreamSource source{VectorStreamConfig{RowsPerChunk{2}}};
  std::vector<Pair> blocks;
  EXPECT_TRUE(collect(source, blocks).empty());

  blocks.push_back(makeEmptyBlock(2));
  EXPECT_TRUE(collect(source, blocks).empty());
}

TEST(VectorStreamSource, RechunksAcrossBlocksAndKeepsFinalPartialChunk) {
  VectorStreamSource source{VectorStreamConfig{RowsPerChunk{3}}};
  std::vector<Pair> blocks;
  blocks.push_back(makeBlock({{1}, {2}}));
  blocks.push_back(makeBlock({{3}, {4}, {5}}));

  auto chunks = collect(source, blocks);
  ASSERT_EQ(chunks.size(), 2);
  EXPECT_EQ(chunks[0].idTable_,
            makeIdTableFromVector({{1}, {2}, {3}}, ad_utility::testing::IntId));
  EXPECT_EQ(chunks[1].idTable_,
            makeIdTableFromVector({{4}, {5}}, ad_utility::testing::IntId));
}

TEST(VectorStreamSource, ExactBoundaryDoesNotEmitEmptyFinalChunk) {
  VectorStreamSource source{VectorStreamConfig{RowsPerChunk{2}}};
  std::vector<Pair> blocks;
  blocks.push_back(makeBlock({{1}, {2}, {3}, {4}}));

  EXPECT_EQ(collect(source, blocks).size(), 2);
}


TEST(VectorStreamSource, AppliesConjoinedEqualityFilters) {
  VectorStreamSource source{VectorStreamConfig{RowsPerChunk{2}}};
  std::vector<Pair> blocks;
  blocks.push_back(makeBlock({{1, 7}, {1, 8}, {2, 7}, {1, 7}}));
  const std::array filters{
      EqualityFilter{0, Id::makeFromInt(1)},
      EqualityFilter{1, Id::makeFromInt(7)},
  };

  auto chunks = collect(source, blocks, filters);
  ASSERT_EQ(chunks.size(), 1);
  EXPECT_EQ(
      chunks[0].idTable_,
      makeIdTableFromVector({{1, 7}, {1, 7}}, ad_utility::testing::IntId));
}

TEST(VectorStreamSource, AllRowsFilteredOutProducesNoChunks) {
  VectorStreamSource source{VectorStreamConfig{RowsPerChunk{2}}};
  std::vector<Pair> blocks;
  blocks.push_back(makeBlock({{1}, {2}}));
  const std::array filters{EqualityFilter{0, Id::makeFromInt(3)}};

  EXPECT_TRUE(collect(source, blocks, filters).empty());
}

TEST(VectorStreamSource, RejectsChangingSchemasAndInvalidFilterColumns) {
  VectorStreamSource source{VectorStreamConfig{RowsPerChunk{2}}};
  std::vector<Pair> changingSchema;
  changingSchema.push_back(makeEmptyBlock(1));
  changingSchema.push_back(makeEmptyBlock(2));
  EXPECT_ANY_THROW(collect(source, changingSchema));

  std::vector<Pair> oneColumn;
  oneColumn.push_back(makeEmptyBlock(1));
  const std::array filters{EqualityFilter{1, Id::makeFromInt(1)}};
  EXPECT_ANY_THROW(collect(source, oneColumn, filters));
}

TEST(VectorStreamSource, OutputOwnsLocalVocabEntriesFromEveryInputBlock) {
  auto* qec = ad_utility::testing::getQec();
  VectorStreamSource source{VectorStreamConfig{RowsPerChunk{2}}};
  std::vector<Pair> chunks;
  Id firstId;
  Id secondId;
  {
    std::vector<Pair> blocks;
    LocalVocab firstVocab;
    firstId =
        Id::makeFromLocalVocabIndex(firstVocab.getIndexAndAddIfNotContained(
            LocalVocabEntry::literalWithoutQuotes(
                "first", qec->getLocalVocabContext())));
    IdTable firstTable{1, ad_utility::testing::makeAllocator()};
    firstTable.push_back({firstId});
    blocks.emplace_back(std::move(firstTable), std::move(firstVocab));

    LocalVocab secondVocab;
    secondId =
        Id::makeFromLocalVocabIndex(secondVocab.getIndexAndAddIfNotContained(
            LocalVocabEntry::literalWithoutQuotes(
                "second", qec->getLocalVocabContext())));
    IdTable secondTable{1, ad_utility::testing::makeAllocator()};
    secondTable.push_back({secondId});
    blocks.emplace_back(std::move(secondTable), std::move(secondVocab));

    chunks = collect(source, blocks);
  }

  ASSERT_EQ(chunks.size(), 1);
  EXPECT_TRUE(chunks[0].localVocab_.isLocalVocabIndexContained(
      firstId.getLocalVocabIndex()));
  EXPECT_TRUE(chunks[0].localVocab_.isLocalVocabIndexContained(
      secondId.getLocalVocabIndex()));
}

}  // namespace
