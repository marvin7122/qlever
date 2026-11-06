// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <vector>

#include "engine/export_v2/ExportMorselPlanner.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

namespace {

using ql::engine::export_v2::ExportMorsel;
using ql::engine::export_v2::planExportMorsels;
using Pair = Result::IdTableVocabPair;

Pair makeBlock(const VectorTable& rows) {
  return {makeIdTableFromVector(rows, ad_utility::testing::IntId),
          LocalVocab{}};
}

// A lazy result over moved-in blocks that counts how often the driver pulls
// the next block. All captures are by value: no reference capture across the
// coroutine suspension. `Pair` is move-only (`LocalVocab` has no copy
// constructor), so blocks are moved, never copied.
struct CountingBlocks {
  std::vector<Pair> blocks_;
  std::shared_ptr<size_t> pulls_ = std::make_shared<size_t>(0);

  void add(Pair block) { blocks_.push_back(std::move(block)); }

  Result::Generator generator() && {
    auto blocks = std::move(blocks_);
    auto pulls = pulls_;
    for (auto& block : blocks) {
      ++(*pulls);
      co_yield Pair{std::move(block.idTable_), std::move(block.localVocab_)};
    }
  }
};

CountingBlocks makeInput(std::vector<VectorTable> rowSets) {
  CountingBlocks input;
  for (const auto& rows : rowSets) {
    input.add(makeBlock(rows));
  }
  return input;
}

uint64_t plannedRows(Result::Generator generator,
                     const LimitOffsetClause& limitOffset,
                     uint64_t rowsPerMorsel,
                     std::vector<ExportMorsel>* morsels = nullptr) {
  Result result{std::move(generator), {}};
  uint64_t rows = 0;
  for (auto&& morsel :
       planExportMorsels(result.idTables(), limitOffset, rowsPerMorsel)) {
    rows += morsel.numRows_;
    if (morsels != nullptr) {
      morsels->push_back(std::move(morsel));
    }
  }
  return rows;
}

TEST(ExportMorselPlanner, SplitsLargeBlockIntoMorsels) {
  auto input = makeInput({{{1}, {2}, {3}, {4}, {5}, {6}, {7}}});
  auto pulls = input.pulls_;
  std::vector<ExportMorsel> morsels;
  const uint64_t rows = plannedRows(std::move(input).generator(),
                                    LimitOffsetClause{}, 3, &morsels);
  EXPECT_EQ(rows, 7u);
  ASSERT_EQ(morsels.size(), 3u);
  EXPECT_EQ(morsels[0].numRows_, 3u);
  EXPECT_EQ(morsels[1].numRows_, 3u);
  EXPECT_EQ(morsels[2].numRows_, 1u);
  // All segments window the same single block: no copy was planned.
  const void* block = morsels[0].segments_[0].block_.get();
  for (const auto& morsel : morsels) {
    for (const auto& segment : morsel.segments_) {
      EXPECT_EQ(segment.block_.get(), block);
    }
  }
  EXPECT_EQ(morsels[0].segments_[0].begin_, 0u);
  EXPECT_EQ(morsels[2].segments_[0].begin_, 6u);
  EXPECT_EQ(morsels[2].segments_[0].end_, 7u);
  EXPECT_EQ(*pulls, 1u);
}

TEST(ExportMorselPlanner, AccumulatesSmallBlocksIntoMorsels) {
  auto input = makeInput({{{1}, {2}}, {{3}, {4}}, {{5}, {6}}});
  std::vector<ExportMorsel> morsels;
  const uint64_t rows = plannedRows(std::move(input).generator(),
                                    LimitOffsetClause{}, 4, &morsels);
  EXPECT_EQ(rows, 6u);
  ASSERT_EQ(morsels.size(), 2u);
  EXPECT_EQ(morsels[0].numRows_, 4u);
  EXPECT_EQ(morsels[1].numRows_, 2u);
  // The first morsel spans two blocks.
  EXPECT_NE(morsels[0].segments_.front().block_.get(),
            morsels[0].segments_.back().block_.get());
}

TEST(ExportMorselPlanner, LimitStopsPulling) {
  auto input =
      makeInput({{{1}, {2}, {3}, {4}, {5}}, {{6}, {7}, {8}, {9}, {10}}});
  auto pulls = input.pulls_;
  const uint64_t rows = plannedRows(std::move(input).generator(),
                                    LimitOffsetClause{._limit = 3}, 8192);
  EXPECT_EQ(rows, 3u);
  EXPECT_EQ(*pulls, 1u);
}

TEST(ExportMorselPlanner, LimitExactlyAtBlockBoundary) {
  auto input =
      makeInput({{{1}, {2}, {3}, {4}, {5}}, {{6}, {7}, {8}, {9}, {10}}});
  auto pulls = input.pulls_;
  std::vector<ExportMorsel> morsels;
  const uint64_t rows =
      plannedRows(std::move(input).generator(), LimitOffsetClause{._limit = 5},
                  8192, &morsels);
  EXPECT_EQ(rows, 5u);
  ASSERT_EQ(morsels.size(), 1u);
  EXPECT_EQ(morsels[0].numRows_, 5u);
  ASSERT_EQ(morsels[0].segments_.size(), 1u);
  EXPECT_EQ(morsels[0].segments_[0].begin_, 0u);
  EXPECT_EQ(morsels[0].segments_[0].end_, 5u);
  // The limit lands exactly on the first block edge: the second block must
  // never be pulled.
  EXPECT_EQ(*pulls, 1u);
}

TEST(ExportMorselPlanner, OffsetSkipsRowsAcrossBlocks) {
  auto input =
      makeInput({{{1}, {2}, {3}, {4}, {5}}, {{6}, {7}, {8}, {9}, {10}}});
  std::vector<ExportMorsel> morsels;
  const uint64_t rows =
      plannedRows(std::move(input).generator(), LimitOffsetClause{._offset = 7},
                  8192, &morsels);
  EXPECT_EQ(rows, 3u);
  ASSERT_EQ(morsels.size(), 1u);
  ASSERT_EQ(morsels[0].segments_.size(), 1u);
  EXPECT_EQ(morsels[0].segments_[0].begin_, 2u);
  EXPECT_EQ(morsels[0].segments_[0].end_, 5u);
}

TEST(ExportMorselPlanner, ExportLimitCountsWithoutSerializing) {
  auto input =
      makeInput({{{1}, {2}, {3}, {4}, {5}}, {{6}, {7}, {8}, {9}, {10}}});
  auto pulls = input.pulls_;
  const uint64_t rows =
      plannedRows(std::move(input).generator(),
                  LimitOffsetClause{._limit = 10, .exportLimit_ = 2}, 8192);
  // Only two rows serialize, but the tail is still pulled for counting, like
  // Legacy `getRowIndices`.
  EXPECT_EQ(rows, 2u);
  EXPECT_EQ(*pulls, 2u);
}

TEST(ExportMorselPlanner, LimitZeroPullsNothing) {
  auto input = makeInput({{{1}, {2}, {3}}});
  auto pulls = input.pulls_;
  const uint64_t rows = plannedRows(std::move(input).generator(),
                                    LimitOffsetClause{._limit = 0}, 8192);
  EXPECT_EQ(rows, 0u);
  EXPECT_EQ(*pulls, 0u);
}

TEST(ExportMorselPlanner, EmptyBlocksAreSkipped) {
  CountingBlocks input;
  input.add(
      Pair{IdTable{1, ad_utility::testing::makeAllocator()}, LocalVocab{}});
  input.add(makeBlock({{1}, {2}}));
  const uint64_t rows =
      plannedRows(std::move(input).generator(), LimitOffsetClause{}, 8192);
  EXPECT_EQ(rows, 2u);
}

// The `std::function<uint64_t()>` overload (used to drive adaptive chunk
// sizing) is called once up front and again after every yielded morsel, so a
// growing (or shrinking) sequence of sizes changes the morsel boundaries
// exactly as requested, without the planner knowing anything about bytes.
TEST(ExportMorselPlanner, DynamicRowsPerMorselGrows) {
  auto input = makeInput({{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}}});
  Result result{std::move(input).generator(), {}};
  std::vector<uint64_t> sizes = {2, 3, 4};
  size_t index = 0;
  std::function<uint64_t()> rowsPerMorselFn = [&]() {
    const uint64_t size = sizes[std::min(index, sizes.size() - 1)];
    ++index;
    return size;
  };
  std::vector<ExportMorsel> morsels;
  for (auto&& morsel : planExportMorsels(result.idTables(), LimitOffsetClause{},
                                         rowsPerMorselFn)) {
    morsels.push_back(std::move(morsel));
  }
  // 2 + 3 + 4 = 9 rows total: growth (2 -> 3 -> 4) is reflected exactly in
  // the morsel boundaries.
  ASSERT_EQ(morsels.size(), 3u);
  EXPECT_EQ(morsels[0].numRows_, 2u);
  EXPECT_EQ(morsels[1].numRows_, 3u);
  EXPECT_EQ(morsels[2].numRows_, 4u);
}

TEST(ExportMorselPlanner, DynamicRowsPerMorselShrinks) {
  auto input = makeInput({{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}}});
  Result result{std::move(input).generator(), {}};
  std::vector<uint64_t> sizes = {5, 3, 1};
  size_t index = 0;
  std::function<uint64_t()> rowsPerMorselFn = [&]() {
    const uint64_t size = sizes[std::min(index, sizes.size() - 1)];
    ++index;
    return size;
  };
  std::vector<ExportMorsel> morsels;
  for (auto&& morsel : planExportMorsels(result.idTables(), LimitOffsetClause{},
                                         rowsPerMorselFn)) {
    morsels.push_back(std::move(morsel));
  }
  // 5 + 3 + 1 = 9 rows total: shrinking (5 -> 3 -> 1) is reflected exactly.
  ASSERT_EQ(morsels.size(), 3u);
  EXPECT_EQ(morsels[0].numRows_, 5u);
  EXPECT_EQ(morsels[1].numRows_, 3u);
  EXPECT_EQ(morsels[2].numRows_, 1u);
}

// The fixed-`rowsPerMorsel` overload (used when the adaptive-chunk-sizing
// runtime parameter is off) must plan byte-for-byte the same morsels as
// before this change: it is a thin wrapper delegating to the dynamic
// overload with a constant function.
TEST(ExportMorselPlanner, FixedOverloadUnchangedByDynamicOverload) {
  auto inputFixed = makeInput({{{1}, {2}, {3}, {4}, {5}, {6}, {7}}});
  auto inputDynamic = makeInput({{{1}, {2}, {3}, {4}, {5}, {6}, {7}}});
  std::vector<ExportMorsel> fixedMorsels;
  std::vector<ExportMorsel> dynamicMorsels;
  plannedRows(std::move(inputFixed).generator(), LimitOffsetClause{}, 3,
              &fixedMorsels);
  Result result{std::move(inputDynamic).generator(), {}};
  std::function<uint64_t()> constantThree = []() -> uint64_t { return 3; };
  for (auto&& morsel : planExportMorsels(result.idTables(), LimitOffsetClause{},
                                         constantThree)) {
    dynamicMorsels.push_back(std::move(morsel));
  }
  ASSERT_EQ(fixedMorsels.size(), dynamicMorsels.size());
  for (size_t i = 0; i < fixedMorsels.size(); ++i) {
    EXPECT_EQ(fixedMorsels[i].numRows_, dynamicMorsels[i].numRows_);
  }
}

}  // namespace
