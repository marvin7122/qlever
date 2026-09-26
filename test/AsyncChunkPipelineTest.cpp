// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "engine/export_v2/AsyncChunkPipeline.h"
#include "util/GTestHelpers.h"
#include "util/Generator.h"

namespace {
using namespace qlever::export_v2;

static_assert(kExportV2CompiledIn);

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
  EXPECT_FALSE(pipeline.pop().has_value());
}

TEST(AsyncChunkPipelineTest, EmptyCompletedPipelineReturnsNoChunk) {
  AsyncChunkPipeline<std::string> pipeline{
      {.capacity_ = 2, .runtimeEnabled_ = true}};
  pipeline.finish();
  EXPECT_FALSE(pipeline.pop().has_value());
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

  EXPECT_EQ(pipeline.pop(), "first");
  EXPECT_EQ(pipeline.pop(), "second");
  EXPECT_FALSE(pipeline.pop().has_value());
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
  EXPECT_FALSE(pipeline.pop().has_value());
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

  EXPECT_EQ(pipeline.pop(), "before-error");
  EXPECT_THROW(static_cast<void>(pipeline.pop()), std::runtime_error);
}

TEST(AsyncChunkPipelineTest, BackpressureReusesFreedSlot) {
  AsyncChunkPipeline<std::string> pipeline{
      {.capacity_ = 1, .runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  auto blockedPush =
      std::async(std::launch::async, [&] { return pipeline.push("second"); });

  waitUntilProducerBlocks(pipeline);
  EXPECT_EQ(pipeline.pop(), "first");
  EXPECT_EQ(blockedPush.get(), PushResult::Accepted);
  EXPECT_EQ(pipeline.pop(), "second");
  EXPECT_EQ(pipeline.stats().producerWaits_, 1);
}

// _____________________________________________________________________________
// `AsyncChunkProducer`: a generator on a producer thread, chunks by value.

// Wait (bounded) until `predicate` holds; the producer thread runs freely.
template <typename Predicate>
bool eventually(Predicate predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{5};
  while (!predicate()) {
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  return true;
}

// Record on which thread the generator frame is created and destroyed.
struct FrameThreads {
  std::atomic<std::thread::id> created_{};
  std::atomic<std::thread::id> destroyed_{};
  std::atomic<size_t> produced_{0};
};

struct OnDestroy {
  FrameThreads* threads_;
  ~OnDestroy() { threads_->destroyed_ = std::this_thread::get_id(); }
};

cppcoro::generator<std::string> numberedChunks(size_t numChunks,
                                               FrameThreads& threads,
                                               size_t throwAt = SIZE_MAX) {
  threads.created_ = std::this_thread::get_id();
  OnDestroy onDestroy{&threads};
  for (size_t i = 0; i < numChunks; ++i) {
    if (i == throwAt) {
      throw std::runtime_error{"producer failed"};
    }
    ++threads.produced_;
    co_yield std::to_string(i);
  }
}

TEST(AsyncChunkProducerTest, KeepsProductionOrder) {
  FrameThreads threads;
  AsyncChunkProducer<std::string> producer{
      [&threads] { return numberedChunks(1000, threads); }, 2};
  std::vector<std::string> received;
  while (auto chunk = producer.pop()) {
    received.push_back(std::move(chunk.value()));
  }
  ASSERT_EQ(received.size(), 1000);
  for (size_t i = 0; i < received.size(); ++i) {
    EXPECT_EQ(received[i], std::to_string(i));
  }
  EXPECT_EQ(producer.stats().chunksConsumed_, 1000);
  // The generator frame lives and dies on the producer thread.
  EXPECT_TRUE(eventually(
      [&] { return threads.destroyed_.load() != std::thread::id{}; }));
  EXPECT_EQ(threads.created_.load(), threads.destroyed_.load());
  EXPECT_NE(threads.created_.load(), std::this_thread::get_id());
}

TEST(AsyncChunkProducerTest, ProducerStaysAtMostCapacityAhead) {
  FrameThreads threads;
  AsyncChunkProducer<std::string> producer{
      [&threads] { return numberedChunks(100, threads); }, 2};
  // Two chunks queued plus one blocked in `push`.
  ASSERT_TRUE(eventually([&] { return producer.stats().producerWaits_ == 1; }));
  EXPECT_EQ(threads.produced_.load(), 3);
  EXPECT_EQ(producer.pop(), "0");
  ASSERT_TRUE(eventually([&] { return producer.stats().producerWaits_ == 2; }));
  EXPECT_EQ(threads.produced_.load(), 4);
  EXPECT_EQ(producer.stats().chunksProduced_, 3);
}

TEST(AsyncChunkProducerTest, RethrowsProducerExceptionAfterEarlierChunks) {
  FrameThreads threads;
  AsyncChunkProducer<std::string> producer{
      [&threads] { return numberedChunks(10, threads, 3); }, 2};
  EXPECT_EQ(producer.pop(), "0");
  EXPECT_EQ(producer.pop(), "1");
  EXPECT_EQ(producer.pop(), "2");
  AD_EXPECT_THROW_WITH_MESSAGE(static_cast<void>(producer.pop()),
                               ::testing::HasSubstr("producer failed"));
}

TEST(AsyncChunkProducerTest, RethrowsExceptionFromMakeRange) {
  AsyncChunkProducer<std::string> producer{
      []() -> cppcoro::generator<std::string> {
        throw std::runtime_error{"no range"};
      },
      2};
  AD_EXPECT_THROW_WITH_MESSAGE(static_cast<void>(producer.pop()),
                               ::testing::HasSubstr("no range"));
}

TEST(AsyncChunkProducerTest, DestroyingUnfinishedProducerStopsIt) {
  FrameThreads threads;
  {
    AsyncChunkProducer<std::string> producer{
        [&threads] { return numberedChunks(SIZE_MAX, threads); }, 2};
    EXPECT_EQ(producer.pop(), "0");
    // The consumer abandons the export here, the producer is blocked in
    // `push` or about to be.
  }
  // Joined in the destructor: the frame is already gone, on its own thread.
  EXPECT_NE(threads.destroyed_.load(), std::thread::id{});
  EXPECT_EQ(threads.created_.load(), threads.destroyed_.load());
  EXPECT_LE(threads.produced_.load(), 4);
}

TEST(AsyncChunkProducerTest, DestroyingBeforeFirstPopIsSafe) {
  FrameThreads threads;
  {
    AsyncChunkProducer<std::string> producer{
        [&threads] { return numberedChunks(SIZE_MAX, threads); }, 2};
  }
  EXPECT_LE(threads.produced_.load(), 3);
}

}  // namespace
