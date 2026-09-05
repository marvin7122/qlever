// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "engine/export_v2/ExportRingDriver.h"
#include "engine/export_v2/ScatterGatherArenaStreamer.h"
#include "util/Generator.h"

namespace {

using ql::engine::export_v2::ExportRingDriver;
using qlever::export_v2::ScatterGatherChunk;
using qlever::export_v2::ScatterGatherChunkBuilder;

using ChunkGenerator = cppcoro::generator<ScatterGatherChunk>;

ScatterGatherChunk makeChunk(std::string text) {
  ScatterGatherChunkBuilder builder;
  builder.appendOwned(std::move(text));
  return std::move(builder).finalize();
}

ChunkGenerator chunksOf(std::vector<std::string> texts) {
  for (auto& text : texts) {
    co_yield makeChunk(std::move(text));
  }
}

std::string popString(ExportRingDriver& driver) {
  auto chunk = driver.pop();
  ASSERT_TRUE(chunk.has_value());
  return chunk->toString();
}

}  // namespace

// _____________________________________________________________________________
TEST(ExportRingDriverTest, YieldsEveryChunkInOrder) {
  ExportRingDriver driver{chunksOf({"a", "bb", "ccc"})};
  EXPECT_EQ(popString(driver), "a");
  EXPECT_EQ(popString(driver), "bb");
  EXPECT_EQ(popString(driver), "ccc");
  EXPECT_FALSE(driver.pop().has_value());
  auto stats = driver.stats();
  EXPECT_EQ(stats.chunksProduced_, 3u);
  EXPECT_EQ(stats.chunksConsumed_, 3u);
}

// _____________________________________________________________________________
TEST(ExportRingDriverTest, ProducerFailureSurfacesAfterDrain) {
  auto source = []() -> ChunkGenerator {
    co_yield makeChunk("a");
    throw std::runtime_error("Test Exception");
    co_return;
  }();
  ExportRingDriver driver{std::move(source)};
  EXPECT_EQ(popString(driver), "a");
  EXPECT_THROW(driver.pop(), std::runtime_error);
}

// An empty source finishes immediately: the first pop is empty and no
// chunk was ever produced.
// _____________________________________________________________________________
TEST(ExportRingDriverTest, EmptySourceFinishesImmediately) {
  ExportRingDriver driver{chunksOf({})};
  EXPECT_FALSE(driver.pop().has_value());
  auto stats = driver.stats();
  EXPECT_EQ(stats.chunksProduced_, 0u);
  EXPECT_EQ(stats.chunksConsumed_, 0u);
}

// The driver destructor must unblock a producer parked in a full ring;
// otherwise destruction hangs (and so would this test).
// _____________________________________________________________________________
TEST(ExportRingDriverTest, DestructionUnblocksParkedProducer) {
  auto source = []() -> ChunkGenerator {
    for (int i = 0; i < 100; ++i) {
      co_yield makeChunk("x");
    }
  }();
  {
    ExportRingDriver driver{std::move(source), 1};
    EXPECT_EQ(popString(driver), "x");
  }
}

// Capacity 1 with a delayed source forces both sides to wait at least once:
// the first pop arrives before the producer pushes, and the producer blocks
// on the second chunk while the consumer sleeps.
// _____________________________________________________________________________
TEST(ExportRingDriverTest, StatsShowBothWaits) {
  auto source = []() -> ChunkGenerator {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    co_yield makeChunk("c1");
    co_yield makeChunk("c2");
    co_yield makeChunk("c3");
  }();
  ExportRingDriver driver{std::move(source), 1};
  EXPECT_EQ(popString(driver), "c1");
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(popString(driver), "c2");
  EXPECT_EQ(popString(driver), "c3");
  EXPECT_FALSE(driver.pop().has_value());
  auto stats = driver.stats();
  EXPECT_GT(stats.producerWaits_, 0u);
  EXPECT_GT(stats.consumerWaits_, 0u);
}
