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

}  // namespace
