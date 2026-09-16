// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "engine/export_v2/AsyncChunkPipeline.h"
#include "util/GTestHelpers.h"

namespace {
using namespace qlever::export_v2;

static_assert(kExportV2CompiledIn);
static_assert(
    ad_utility::InvariantStatefulClass<AsyncChunkPipeline<std::string>>);
static_assert(kNumRingSlots == 2);

TEST(AsyncChunkPipelineTest, RuntimeKillSwitchLeavesPipelineClosed) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = false}};
  EXPECT_FALSE(pipeline.isEnabled());
  EXPECT_EQ(pipeline.push("ignored"), PushResult::Closed);
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
}

TEST(AsyncChunkPipelineTest, EmptyCompletedPipelineReturnsNoChunk) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  pipeline.finish();
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
}

TEST(AsyncChunkPipelineTest, MovesChunksWithoutCopyingTheirBuffer) {
  AsyncChunkPipeline<std::unique_ptr<std::string>> pipeline{
      {.runtimeEnabled_ = true}};
  auto chunk = std::make_unique<std::string>("payload");
  const auto* allocation = chunk.get();

  EXPECT_EQ(pipeline.push(std::move(chunk)), PushResult::Accepted);
  auto received = pipeline.pop();

  ASSERT_TRUE(received.has_value());
  EXPECT_EQ(received->get(), allocation);
  EXPECT_EQ(**received, "payload");
}

TEST(AsyncChunkPipelineTest, CompletionDrainsQueuedChunksInOrder) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);
  pipeline.finish();
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"first"});
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"second"});
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
}

TEST(AsyncChunkPipelineTest, FullRingSignalsBackpressureWithoutBlocking) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);
  // Both ring slots hold undrained chunks, so the producer must suspend
  // generation instead of blocking.
  EXPECT_EQ(pipeline.push("third"), PushResult::Full);
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"first"});
  // The drained slot is immediately reusable for the next chunk.
  EXPECT_EQ(pipeline.push("third"), PushResult::Accepted);
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"second"});
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"third"});
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
}

TEST(AsyncChunkPipelineTest, SlotsAlternateAcrossWraparound) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  // Rotate the ring several times so both slot indices wrap around and the
  // freed slot is reused on every iteration.
  for (int i = 0; i < 5; ++i) {
    const std::string chunk = "chunk-" + std::to_string(i);
    ASSERT_EQ(pipeline.push(chunk), PushResult::Accepted) << "iteration " << i;
    EXPECT_EQ(pipeline.pop(), std::optional<std::string>{chunk})
        << "iteration " << i;
  }
  EXPECT_EQ(pipeline.stats().chunksProduced_, 5);
  EXPECT_EQ(pipeline.stats().chunksConsumed_, 5);
}

TEST(AsyncChunkPipelineTest, CancellationDiscardsBothSlots) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);

  pipeline.cancel();

  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 2);
}

TEST(AsyncChunkPipelineTest, CancellationReleasesQueuedBuffer) {
  AsyncChunkPipeline<std::shared_ptr<std::string>> pipeline{
      {.runtimeEnabled_ = true}};
  auto chunk = std::make_shared<std::string>("payload");
  std::weak_ptr<std::string> lifetime = chunk;
  ASSERT_EQ(pipeline.push(std::move(chunk)), PushResult::Accepted);

  pipeline.cancel();

  EXPECT_TRUE(lifetime.expired());
}

TEST(AsyncChunkPipelineTest, PropagatesFailureAfterQueuedChunks) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("before-error"), PushResult::Accepted);
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"producer failed"}));
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"before-error"});
  // Use a lambda to discard the nodiscard return value while still checking
  // the exception
  EXPECT_THROW([&] { static_cast<void>(pipeline.pop()); }(),
               std::runtime_error);
}

}  // namespace
