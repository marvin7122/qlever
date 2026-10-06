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

using qlever::export_pipeline::AsyncChunkPipeline;

// _____________________________________________________________________________
TEST(AsyncChunkPipeline, PushPopPreservesOrderAndFinishEndsStream) {
  AsyncChunkPipeline<std::string> pipeline{2};
  std::thread producer{[&pipeline] {
    for (const char* chunk : {"a", "bb", "ccc", "dddd"}) {
      EXPECT_TRUE(pipeline.push(chunk));
    }
    pipeline.finish();
  }};
  std::vector<std::string> consumed;
  while (auto chunk = pipeline.pop()) {
    consumed.push_back(std::move(*chunk));
  }
  producer.join();
  EXPECT_THAT(consumed, ::testing::ElementsAre("a", "bb", "ccc", "dddd"));
  auto stats = pipeline.stats();
  EXPECT_EQ(stats.totalChunksProduced, 4u);
  EXPECT_EQ(stats.totalChunksConsumed, 4u);
  EXPECT_EQ(stats.totalBytesConsumed, 10u);
}

// _____________________________________________________________________________
TEST(AsyncChunkPipeline, ProducerExceptionIsRethrownAfterDrain) {
  AsyncChunkPipeline<std::string> pipeline{2};
  EXPECT_TRUE(pipeline.push("first"));
  pipeline.setException(
      std::make_exception_ptr(std::runtime_error{"producer failed"}));
  auto chunk = pipeline.pop();
  ASSERT_TRUE(chunk.has_value());
  EXPECT_EQ(*chunk, "first");
  EXPECT_THROW(pipeline.pop(), std::runtime_error);
}

// _____________________________________________________________________________
// `cancel()` must wake a consumer that is blocked on an empty pipeline. The
// loop repeats the race between the waiter entering `pop()` and `cancel()`
// so that a lost wakeup would hang the test.
TEST(AsyncChunkPipeline, CancelWakesBlockedConsumer) {
  for (size_t i = 0; i < 1000; ++i) {
    AsyncChunkPipeline<std::string> pipeline{1};
    std::thread consumer{[&pipeline] { EXPECT_FALSE(pipeline.pop()); }};
    pipeline.cancel();
    consumer.join();
    EXPECT_TRUE(pipeline.isCancelled());
  }
}

// _____________________________________________________________________________
// `cancel()` must wake a producer that is blocked on a full pipeline.
TEST(AsyncChunkPipeline, CancelWakesBlockedProducer) {
  for (size_t i = 0; i < 1000; ++i) {
    AsyncChunkPipeline<std::string> pipeline{1};
    ASSERT_TRUE(pipeline.push("fills the only slot"));
    std::thread producer{
        [&pipeline] { EXPECT_FALSE(pipeline.push("blocks on full")); }};
    pipeline.cancel();
    producer.join();
    EXPECT_FALSE(pipeline.push("rejected after cancel"));
  }
}
