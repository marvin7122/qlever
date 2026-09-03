// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/AdaptiveChunkSizer.h"
#include "util/Invariants.h"

namespace {

using qlever::AdaptiveChunkBuffer;
using qlever::AdaptiveChunkConfig;
using qlever::AdaptiveChunkSizer;
using qlever::AdaptiveChunkStats;

// =============================================================================
// Static Invariant Concept Verification (Law 7 / ARCHITECTURE.md Section 3)
// =============================================================================
static_assert(ad_utility::InvariantStatefulClass<AdaptiveChunkSizer>,
              "AdaptiveChunkSizer must satisfy InvariantStatefulClass concept");
static_assert(ad_utility::InvariantStatefulClass<AdaptiveChunkBuffer>,
              "AdaptiveChunkBuffer must satisfy InvariantStatefulClass concept");

// =============================================================================
// Unit Tests for AdaptiveChunkSizer
// =============================================================================

TEST(AdaptiveChunkSizerTest, DefaultConstructionAndInitialState) {
  AdaptiveChunkSizer sizer;

  EXPECT_EQ(sizer.currentChunkBytes(), 64 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 0);
  EXPECT_EQ(sizer.totalBytes(), 0);
  EXPECT_EQ(sizer.totalRows(), 0);
  EXPECT_DOUBLE_EQ(sizer.averageRowBytes(), 120.0);

    EXPECT_EQ(sizer.targetRowCount(), 547);

  const AdaptiveChunkStats stats = sizer.stats();
  EXPECT_EQ(stats.chunksFlushed_, 0);
  EXPECT_EQ(stats.totalBytes_, 0);
  EXPECT_EQ(stats.totalRows_, 0);
  EXPECT_EQ(stats.currentChunkBytes_, 64 * 1024);
  EXPECT_DOUBLE_EQ(stats.averageRowBytes_, 120.0);
  EXPECT_EQ(stats.targetRowsForNextChunk_, 547);
}

TEST(AdaptiveChunkSizerTest, ExponentialRampUpProgression) {
  AdaptiveChunkSizer sizer;

    // Start with a 64 KiB chunk.
  EXPECT_EQ(sizer.currentChunkBytes(), 64 * 1024);

  // Flush the first chunk and grow from 64 KiB to 128 KiB.
  sizer.recordChunk(64 * 1024, 500);
  EXPECT_EQ(sizer.currentChunkBytes(), 128 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 1);

  // Flush the second chunk and grow from 128 KiB to 256 KiB.
  sizer.recordChunk(128 * 1024, 1000);
  EXPECT_EQ(sizer.currentChunkBytes(), 256 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 2);

  // Flush the third chunk and grow from 256 KiB to 512 KiB.
  sizer.recordChunk(256 * 1024, 2000);
  EXPECT_EQ(sizer.currentChunkBytes(), 512 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 3);

  // Flush the fourth chunk and grow from 512 KiB to 1 MiB.
  sizer.recordChunk(512 * 1024, 4000);
  EXPECT_EQ(sizer.currentChunkBytes(), 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 4);

  // Flush the fifth chunk and grow from 1 MiB to 2 MiB.
  sizer.recordChunk(1024 * 1024, 8000);
  EXPECT_EQ(sizer.currentChunkBytes(), 2 * 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 5);

  // Flush the sixth chunk and grow from 2 MiB to the 4 MiB cap.
  sizer.recordChunk(2 * 1024 * 1024, 16000);
  EXPECT_EQ(sizer.currentChunkBytes(), 4 * 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 6);

  // Keep the chunk size at the 4 MiB cap after the seventh flush.
  sizer.recordChunk(4 * 1024 * 1024, 32000);
  EXPECT_EQ(sizer.currentChunkBytes(), 4 * 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 7);

  // Sustain bulk streaming at 4 MiB for the eighth flush.
  sizer.recordChunk(4 * 1024 * 1024, 32000);
  EXPECT_EQ(sizer.currentChunkBytes(), 4 * 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 8);
}

TEST(AdaptiveChunkSizerTest, DynamicRowEstimation) {
  AdaptiveChunkSizer sizer;

  // Suppose actual row format produces 250 bytes per row
  const size_t rows = 260;
  const size_t bytes = rows * 250;
  sizer.recordChunk(bytes, rows);

  // Estimated row bytes should adjust towards 250
  EXPECT_GT(sizer.averageRowBytes(), 150.0);
  EXPECT_LE(sizer.averageRowBytes(), 250.0);

  // Next chunk target is 128 KB (131072 bytes).
  // Target rows should reflect the updated row size estimate.
  const size_t expectedTargetRows = static_cast<size_t>(
      std::ceil(static_cast<double>(128 * 1024) / sizer.averageRowBytes()));
  EXPECT_EQ(sizer.targetRowCount(), expectedTargetRows);
}

TEST(AdaptiveChunkSizerTest, TargetRowCountWithRemainingRowsClamping) {
  AdaptiveChunkSizer sizer;

  const size_t target = sizer.targetRowCount();
  EXPECT_GT(target, 100);

    // Verify that `targetRowCount` clamps to 50 when 50 rows remain.
  EXPECT_EQ(sizer.targetRowCount(50), 50);

  // Verify that `targetRowCount` returns the unconstrained target for 100,000 remaining rows.
  EXPECT_EQ(sizer.targetRowCount(100'000), target);
}

TEST(AdaptiveChunkSizerTest, IsChunkFullPredicate) {
  AdaptiveChunkSizer sizer;
  const size_t targetBytes = sizer.currentChunkBytes();

  EXPECT_FALSE(sizer.isChunkFull(0));
  EXPECT_FALSE(sizer.isChunkFull(targetBytes - 1));
  EXPECT_TRUE(sizer.isChunkFull(targetBytes));
  EXPECT_TRUE(sizer.isChunkFull(targetBytes + 1024));

  const size_t targetRows = sizer.targetRowCount();
  EXPECT_FALSE(sizer.isChunkFull(0, 0));
  EXPECT_FALSE(sizer.isChunkFull(100, targetRows - 1));
  EXPECT_TRUE(sizer.isChunkFull(100, targetRows));
}

TEST(AdaptiveChunkSizerTest, ResetRestoresInitialState) {
  AdaptiveChunkSizer sizer;

  for (size_t i = 0; i < 10; ++i) {
    sizer.recordChunk(1024 * 1024, 8000);
  }
  EXPECT_EQ(sizer.currentChunkBytes(), 4 * 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 10);
  EXPECT_GT(sizer.totalBytes(), 0);
  EXPECT_GT(sizer.totalRows(), 0);

  sizer.reset();

  EXPECT_EQ(sizer.currentChunkBytes(), 64 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 0);
  EXPECT_EQ(sizer.totalBytes(), 0);
  EXPECT_EQ(sizer.totalRows(), 0);
  EXPECT_DOUBLE_EQ(sizer.averageRowBytes(), 120.0);
}

TEST(AdaptiveChunkSizerTest, ZeroRowAndZeroByteHandling) {
  AdaptiveChunkSizer sizer;

    // Verify that recording a 0-byte or 0-row chunk does not divide by zero or crash.
  sizer.recordChunk(0, 0);
  EXPECT_EQ(sizer.chunksFlushed(), 1);
  EXPECT_EQ(sizer.totalBytes(), 0);
  EXPECT_EQ(sizer.totalRows(), 0);
  EXPECT_DOUBLE_EQ(sizer.averageRowBytes(), 120.0);
  EXPECT_EQ(sizer.currentChunkBytes(), 128 * 1024);
}

TEST(AdaptiveChunkSizerTest, CustomConfiguration) {
  AdaptiveChunkConfig config{
      .initialChunkBytes_ = 32 * 1024,
      .maxChunkBytes_ = 512 * 1024,
      .growthFactor_ = 4.0,
      .initialEstimatedRowBytes_ = 64.0,
      .minChunkRows_ = 5,
      .maxChunkRows_ = 1000,
  };
  AdaptiveChunkSizer sizer(config);

  EXPECT_EQ(sizer.currentChunkBytes(), 32 * 1024);
  EXPECT_EQ(sizer.targetRowCount(), 512);

    // Verify that 32 KiB multiplied by 4 produces 128 KiB.
  sizer.recordChunk(32 * 1024, 500);
  EXPECT_EQ(sizer.currentChunkBytes(), 128 * 1024);

  // Verify that 128 KiB multiplied by 4 reaches the 512 KiB cap.
  sizer.recordChunk(128 * 1024, 2000);
  EXPECT_EQ(sizer.currentChunkBytes(), 512 * 1024);

  // Verify that the next flush remains at 512 KiB.
  sizer.recordChunk(512 * 1024, 8000);
  EXPECT_EQ(sizer.currentChunkBytes(), 512 * 1024);
}

// =============================================================================
// Unit Tests for AdaptiveChunkBuffer
// =============================================================================

TEST(AdaptiveChunkBufferTest, WriteAndFlushRampUp) {
  AdaptiveChunkBuffer buffer;

  EXPECT_EQ(buffer.bytesBuffered(), 0);
  EXPECT_EQ(buffer.rowsBuffered(), 0);
  EXPECT_FALSE(buffer.isReadyToFlush());

    buffer.write("Hello, World!\n");
  buffer.recordRow();
  EXPECT_EQ(buffer.bytesBuffered(), 14);
  EXPECT_EQ(buffer.rowsBuffered(), 1);
  EXPECT_EQ(buffer.currentView(), "Hello, World!\n");

    std::string flushed = buffer.flush();
  EXPECT_EQ(flushed, "Hello, World!\n");
  EXPECT_EQ(buffer.bytesBuffered(), 0);
  EXPECT_EQ(buffer.rowsBuffered(), 0);
  EXPECT_EQ(buffer.sizer().chunksFlushed(), 1);
  EXPECT_EQ(buffer.sizer().currentChunkBytes(), 128 * 1024);
}

}  // namespace
