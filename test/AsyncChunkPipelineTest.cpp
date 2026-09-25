// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "engine/AsyncChunkPipeline.h"

namespace {

using qlever::export_pipeline::AsyncChunkPipeline;

// Block a producer on a full pipeline and wait until it is inside the
// backpressure wait. The stall counter is incremented under the same mutex
// hold that enters the wait, so once it is visible the producer is waiting.
void waitForBackpressureStall(const AsyncChunkPipeline<std::string>& pipeline) {
  while (pipeline.stats().backpressureStalls == 0) {
    std::this_thread::yield();
  }
}

// _____________________________________________________________________________
TEST(AsyncChunkPipeline, SetExceptionWakesBlockedProducer) {
  AsyncChunkPipeline<std::string> pipeline{1};
  ASSERT_TRUE(pipeline.push("a"));
  std::thread producer([&pipeline] { EXPECT_FALSE(pipeline.push("b")); });
  waitForBackpressureStall(pipeline);
  pipeline.setException(
      std::make_exception_ptr(std::runtime_error("producer failed")));
  producer.join();
  // The chunk that was buffered before the failure is still delivered.
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"a"});
  EXPECT_THROW(pipeline.pop(), std::runtime_error);
}

// _____________________________________________________________________________
TEST(AsyncChunkPipeline, FinishAndCancelWakeBlockedProducer) {
  {
    AsyncChunkPipeline<std::string> pipeline{1};
    ASSERT_TRUE(pipeline.push("a"));
    std::thread producer([&pipeline] { EXPECT_FALSE(pipeline.push("b")); });
    waitForBackpressureStall(pipeline);
    pipeline.finish();
    producer.join();
    EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"a"});
    EXPECT_EQ(pipeline.pop(), std::nullopt);
  }
  {
    AsyncChunkPipeline<std::string> pipeline{1};
    ASSERT_TRUE(pipeline.push("a"));
    std::thread producer([&pipeline] { EXPECT_FALSE(pipeline.push("b")); });
    waitForBackpressureStall(pipeline);
    pipeline.cancel();
    producer.join();
    EXPECT_TRUE(pipeline.isCancelled());
  }
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
// _____________________________________________________________________________
cppcoro::generator<std::string> finiteSource() {
  co_yield std::string{"a"};
  co_yield std::string{"b"};
  co_yield std::string{"c"};
}

// _____________________________________________________________________________
cppcoro::generator<std::string> infiniteSource() {
  while (true) {
    co_yield std::string{"x"};
  }
}

// _____________________________________________________________________________
cppcoro::generator<std::string> failingSource() {
  co_yield std::string{"a"};
  throw std::runtime_error("source failed");
}

// _____________________________________________________________________________
TEST(AsyncChunkPipeline, MakeDoubleBufferedYieldsAllChunks) {
  std::vector<std::string> chunks;
  for (auto& chunk :
       AsyncChunkPipeline<std::string>::makeDoubleBuffered(finiteSource(), 1)) {
    chunks.push_back(chunk);
  }
  EXPECT_THAT(chunks, ::testing::ElementsAre("a", "b", "c"));
}

// _____________________________________________________________________________
TEST(AsyncChunkPipeline, MakeDoubleBufferedEarlyStopJoinsWorker) {
  // The worker blocks on backpressure; destroying the generator must cancel
  // the pipeline and join the worker instead of hanging or terminating.
  auto generator =
      AsyncChunkPipeline<std::string>::makeDoubleBuffered(infiniteSource(), 1);
  auto it = generator.begin();
  ASSERT_NE(it, generator.end());
  EXPECT_EQ(*it, "x");
}

// _____________________________________________________________________________
TEST(AsyncChunkPipeline, MakeDoubleBufferedRethrowsProducerException) {
  auto generator =
      AsyncChunkPipeline<std::string>::makeDoubleBuffered(failingSource(), 1);
  auto it = generator.begin();
  ASSERT_NE(it, generator.end());
  EXPECT_EQ(*it, "a");
  EXPECT_THROW(++it, std::runtime_error);
}

// _____________________________________________________________________________
TEST(AsyncChunkPipeline, PipelineStreamRethrowsProducerException) {
  auto generator = AsyncChunkPipeline<std::string>::pipelineStream(
      [](qlever::export_pipeline::ChunkSink<std::string>& sink) {
        sink.push("a");
        throw std::runtime_error("producer failed");
      },
      1);
  auto it = generator.begin();
  ASSERT_NE(it, generator.end());
  EXPECT_EQ(*it, "a");
  EXPECT_THROW(++it, std::runtime_error);
}
#endif

}  // namespace
