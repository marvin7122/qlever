// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <string>

#include "./util/GTestHelpers.h"
#include "engine/AsyncChunkPipeline.h"

namespace {

using qlever::export_pipeline::AsyncChunkPipeline;

// Push/pop preserves order; finish drains to nullopt.
TEST(AsyncChunkPipelineTest, PushPopRoundtripInOrder) {
  AsyncChunkPipeline<std::string> pipeline(2);
  EXPECT_TRUE(pipeline.push("first"));
  EXPECT_TRUE(pipeline.push("second"));
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"first"});
  EXPECT_TRUE(pipeline.push("third"));
  pipeline.finish();
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"second"});
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"third"});
  EXPECT_EQ(pipeline.pop(), std::nullopt);
}

// Cancelling unblocks a consumer waiting in pop: the flag update and the
// wakeups hold the mutex, so no wakeup is missed between the predicate
// check and the wait.
TEST(AsyncChunkPipelineTest, CancelUnblocksWaitingPop) {
  AsyncChunkPipeline<std::string> pipeline(2);
  auto consumer =
      std::async(std::launch::async, [&pipeline]() { return pipeline.pop(); });
  EXPECT_EQ(consumer.wait_for(std::chrono::seconds(5)),
            std::future_status::timeout);
  pipeline.cancel();
  ASSERT_EQ(consumer.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_EQ(consumer.get(), std::nullopt);
  EXPECT_TRUE(pipeline.isCancelled());
}

// No production after cancellation.
TEST(AsyncChunkPipelineTest, PushAfterCancelReturnsFalse) {
  AsyncChunkPipeline<std::string> pipeline(2);
  pipeline.cancel();
  EXPECT_FALSE(pipeline.push("late"));
}

// Zero capacity violates the constructor precondition.
TEST(AsyncChunkPipelineTest, ZeroCapacityThrows) {
  AD_EXPECT_THROW_WITH_MESSAGE(AsyncChunkPipeline<std::string>(0),
                               ::testing::HasSubstr("capacity_ >= 1"));
}

}  // namespace
