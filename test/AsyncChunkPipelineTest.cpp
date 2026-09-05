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
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "engine/export_v2/AsyncChunkPipeline.h"
#include "util/GTestHelpers.h"

namespace {
using namespace qlever::export_v2;

static_assert(kExportV2CompiledIn);
static_assert(
    ad_utility::InvariantStatefulClass<AsyncChunkPipeline<std::string>>);

void waitUntilProducerBlocks(AsyncChunkPipeline<std::string>& pipeline) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{1};
  while (pipeline.stats().producerWaits_ == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_EQ(pipeline.stats().producerWaits_, 1);
}

TEST(AsyncChunkPipelineTest, RejectsZeroCapacity) {
  AD_EXPECT_THROW_WITH_MESSAGE(AsyncChunkPipeline<std::string>(
                                   {.capacity_ = 0, .runtimeEnabled_ = true}),
                               ::testing::HasSubstr("capacity_ > 0"));
}

TEST(AsyncChunkPipelineTest, RuntimeKillSwitchLeavesPipelineClosed) {
  AsyncChunkPipeline<std::string> pipeline{
      {.capacity_ = 2, .runtimeEnabled_ = false}};
  EXPECT_FALSE(pipeline.isEnabled());
  EXPECT_EQ(pipeline.push("ignored"), PushResult::Closed);
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
}

TEST(AsyncChunkPipelineTest, EmptyCompletedPipelineReturnsNoChunk) {
  AsyncChunkPipeline<std::string> pipeline{
      {.capacity_ = 2, .runtimeEnabled_ = true}};
  pipeline.finish();
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
}

TEST(AsyncChunkPipelineTest, MovesChunksWithoutCopyingTheirBuffer) {
  AsyncChunkPipeline<std::unique_ptr<std::string>> pipeline{
      {.capacity_ = 2, .runtimeEnabled_ = true}};
  auto chunk = std::make_unique<std::string>("payload");
  const auto* allocation = chunk.get();

  EXPECT_EQ(pipeline.push(std::move(chunk)), PushResult::Accepted);
  auto received = pipeline.pop();

  ASSERT_TRUE(received.has_value());
  EXPECT_EQ(received->get(), allocation);
  EXPECT_EQ(**received, "payload");
}

TEST(AsyncChunkPipelineTest, CompletionDrainsQueuedChunksInOrder) {
  AsyncChunkPipeline<std::string> pipeline{
      {.capacity_ = 2, .runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);
  pipeline.finish();

  EXPECT_EQ(static_cast<void>(pipeline.pop()),
            static_cast<void>(std::make_optional(std::string{"first"})));
  EXPECT_EQ(static_cast<void>(pipeline.pop()),
            static_cast<void>(std::make_optional(std::string{"second"})));
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
}

TEST(AsyncChunkPipelineTest, CancellationUnblocksProducer) {
  AsyncChunkPipeline<std::string> pipeline{
      {.capacity_ = 1, .runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  auto blockedPush =
      std::async(std::launch::async, [&] { return pipeline.push("second"); });

  waitUntilProducerBlocks(pipeline);
  pipeline.cancel();

  EXPECT_EQ(blockedPush.get(), PushResult::Closed);
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 1);
}

TEST(AsyncChunkPipelineTest, CancellationReleasesQueuedBuffer) {
  AsyncChunkPipeline<std::shared_ptr<std::string>> pipeline{
      {.capacity_ = 1, .runtimeEnabled_ = true}};
  auto chunk = std::make_shared<std::string>("payload");
  std::weak_ptr<std::string> lifetime = chunk;
  ASSERT_EQ(pipeline.push(std::move(chunk)), PushResult::Accepted);

  pipeline.cancel();

  EXPECT_TRUE(lifetime.expired());
}

TEST(AsyncChunkPipelineTest, PropagatesFailureAfterQueuedChunks) {
  AsyncChunkPipeline<std::string> pipeline{
      {.capacity_ = 2, .runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("before-error"), PushResult::Accepted);
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"producer failed"}));

  EXPECT_EQ(static_cast<void>(pipeline.pop()),
            static_cast<void>(std::make_optional(std::string{"before-error"})));
  EXPECT_THROW(static_cast<void>(pipeline.pop()), std::runtime_error);
}

TEST(AsyncChunkPipelineTest, BackpressureReusesFreedSlot) {
  AsyncChunkPipeline<std::string> pipeline{
      {.capacity_ = 1, .runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  auto blockedPush =
      std::async(std::launch::async, [&] { return pipeline.push("second"); });

  waitUntilProducerBlocks(pipeline);
  EXPECT_EQ(static_cast<void>(pipeline.pop()),
            static_cast<void>(std::make_optional(std::string{"first"})));
  EXPECT_EQ(blockedPush.get(), PushResult::Accepted);
  EXPECT_EQ(static_cast<void>(pipeline.pop()),
            static_cast<void>(std::make_optional(std::string{"second"})));
  EXPECT_EQ(pipeline.stats().producerWaits_, 1);
}

}  // namespace
