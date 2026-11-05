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

#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "engine/export_v2/AsyncChunkPipeline.h"
#include "util/GTestHelpers.h"

namespace {
using namespace qlever::export_v2;

// The tests below drive an enabled double-buffered ring, so they enforce
// the compile-time switch and the slot count they are written against.
// Tests require `export_v2` compiled in: a disabled build would return
// `Closed` from every `push`.
static_assert(exportV2CompiledIn);
// Tests assume a double-buffered ring: `Full` after two `push` calls and the
// wraparound rotation below only hold for exactly two slots.
static_assert(numRingSlots == 2);

TEST(AsyncChunkPipelineTest, RuntimeKillSwitchLeavesPipelineClosed) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = false}};
  EXPECT_FALSE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  EXPECT_EQ(pipeline.push("ignored"), PushResult::Closed);
  AD_EXPECT_NULLOPT(pipeline.pop());
  // A disabled pipeline leaves all six stats counters at zero.
  const auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 0);
  EXPECT_EQ(stats.chunksConsumed_, 0);
  EXPECT_EQ(stats.chunksDiscarded_, 0);
  EXPECT_EQ(stats.bytesProduced_, 0);
  EXPECT_EQ(stats.bytesConsumed_, 0);
  EXPECT_EQ(stats.bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, EmptyCompletedPipelineReturnsNoChunk) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  pipeline.finish();
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  AD_EXPECT_NULLOPT(pipeline.pop());
  // No `push` was `Accepted`, so no chunk entered the slots and all six
  // stats stay zero.
  const auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 0);
  EXPECT_EQ(stats.chunksConsumed_, 0);
  EXPECT_EQ(stats.chunksDiscarded_, 0);
  EXPECT_EQ(stats.bytesProduced_, 0);
  EXPECT_EQ(stats.bytesConsumed_, 0);
  EXPECT_EQ(stats.bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, MovesChunksWithoutCopyingTheirBuffer) {
  AsyncChunkPipeline<std::unique_ptr<std::string>> pipeline{
      {.runtimeEnabled_ = true}};
  auto chunk = std::make_unique<std::string>("payload");
  // Remember the heap allocation: after the round trip, pointer identity of
  // the received `unique_ptr` plus an equal payload proves ownership moved
  // via nothrow move, without deep-copying the buffer.
  const auto* allocation = chunk.get();

  EXPECT_EQ(pipeline.push(std::move(chunk)), PushResult::Accepted);
  // An `Accepted` push moves from the caller's chunk.
  EXPECT_EQ(chunk.get(), nullptr);
  const auto received = pipeline.pop();

  ASSERT_TRUE(received.has_value());
  EXPECT_EQ(received->get(), allocation);
  EXPECT_EQ(**received, "payload");
  // `std::unique_ptr<std::string>` has no `size()`, so `chunkSize()` returns
  // 0: bytesProduced_/bytesConsumed_/bytesDiscarded_ stay 0 while the chunk
  // counters reach 1.
  const auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 1);
  EXPECT_EQ(stats.chunksConsumed_, 1);
  EXPECT_EQ(stats.chunksDiscarded_, 0);
  EXPECT_EQ(stats.bytesProduced_, 0);
  EXPECT_EQ(stats.bytesConsumed_, 0);
  EXPECT_EQ(stats.bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, FullRejectionPreservesMoveOnlyChunk) {
  AsyncChunkPipeline<std::unique_ptr<std::string>> pipeline{
      {.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push(std::make_unique<std::string>("first")),
            PushResult::Accepted);
  ASSERT_EQ(pipeline.push(std::make_unique<std::string>("second")),
            PushResult::Accepted);
  auto third = std::make_unique<std::string>("third");
  const auto* allocation = third.get();
  EXPECT_EQ(pipeline.push(std::move(third)), PushResult::Full);
  // A `push` returning `Full` leaves the `Chunk&&` argument unmoved, so the
  // caller retains ownership and allocation for the retry after `pop`.
  EXPECT_EQ(third.get(), allocation);
  EXPECT_EQ(*third, "third");
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_TRUE(pipeline.isRunning());
  const auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 2);
  EXPECT_EQ(stats.chunksConsumed_, 0);
  EXPECT_EQ(stats.chunksDiscarded_, 0);
  EXPECT_EQ(stats.bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, CompletionDrainsQueuedChunksInOrder) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);
  pipeline.finish();
  // `Finished` + full ring: `Closed` is checked before `Full`, so the probe
  // is rejected as `Closed` (not `Full`) and keeps its chunk.
  std::string probe = "probe";
  EXPECT_EQ(pipeline.push(std::move(probe)), PushResult::Closed);
  EXPECT_EQ(probe, "probe");
  EXPECT_EQ(pipeline.stats().chunksProduced_, 2);
  EXPECT_EQ(pipeline.stats().bytesProduced_, 11);
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"first"});
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"second"});
  // After draining the two queued chunks in `Finished`, further `pop()`
  // returns `std::nullopt` on every call; the pipeline stays `Finished`
  // (`isEnabled` true, `isRunning` false) with discard counters at 0.
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  AD_EXPECT_NULLOPT(pipeline.pop());
  AD_EXPECT_NULLOPT(pipeline.pop());
  // A late `push` after `finish` returns `Closed`, leaves the `Chunk&&`
  // unmoved (the caller retains "late"), and leaves all six stats unchanged.
  std::string late = "late";
  EXPECT_EQ(pipeline.push(std::move(late)), PushResult::Closed);
  EXPECT_EQ(late, "late");
  const auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 2);
  EXPECT_EQ(stats.chunksConsumed_, 2);
  // "first"(5) + "second"(6) = 11 bytes produced and consumed.
  EXPECT_EQ(stats.bytesProduced_, 11);
  EXPECT_EQ(stats.bytesConsumed_, 11);
  EXPECT_EQ(stats.chunksDiscarded_, 0);
  EXPECT_EQ(stats.bytesDiscarded_, 0);
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
  AD_EXPECT_NULLOPT(pipeline.pop());
  // "first"(5) + "second"(6) + "third"(5) = 16: bytesProduced_ == 16 and
  // bytesConsumed_ == 16 (chunksProduced_ == 3, chunksConsumed_ == 3).
  EXPECT_EQ(pipeline.stats().bytesProduced_, 16);
  EXPECT_EQ(pipeline.stats().bytesConsumed_, 16);
  EXPECT_EQ(pipeline.stats().bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, SlotsAlternateAcrossWraparound) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  // Rotate the ring several times so both slot indices wrap around and the
  // freed slot is reused on every iteration. Five iterations wrap the two
  // slots twice with a remainder, covering both parities of `consumeIndex_`.
  for (int i = 0; i < 5; ++i) {
    std::string chunk = "chunk-" + std::to_string(i);
    // Copy before move: `chunk` is moved-from by `push`, so the `pop` result
    // is compared against the copy.
    const std::string expected = chunk;
    ASSERT_EQ(pipeline.push(std::move(chunk)), PushResult::Accepted)
        << "iteration " << i;
    EXPECT_EQ(pipeline.pop(), std::optional<std::string>{expected})
        << "iteration " << i;
  }
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_TRUE(pipeline.isRunning());
  AD_EXPECT_NULLOPT(pipeline.pop());
  EXPECT_EQ(pipeline.stats().chunksProduced_, 5);
  EXPECT_EQ(pipeline.stats().chunksConsumed_, 5);
  // Each "chunk-N" is 7 bytes: bytesProduced_ == 35 and bytesConsumed_ == 35
  // (chunksProduced_ == 5, chunksConsumed_ == 5).
  EXPECT_EQ(pipeline.stats().bytesProduced_, 35);
  EXPECT_EQ(pipeline.stats().bytesConsumed_, 35);
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 0);
  EXPECT_EQ(pipeline.stats().bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, CancellationDiscardsBothSlots) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);

  pipeline.cancel();

  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  AD_EXPECT_NULLOPT(pipeline.pop());
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 2);
  // "first"(5) + "second"(6) = 11 discarded bytes.
  EXPECT_EQ(pipeline.stats().bytesDiscarded_, 11);
}

TEST(AsyncChunkPipelineTest, CancelOfPartiallyFilledPipelineCountsOneDiscard) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("abc"), PushResult::Accepted);

  pipeline.cancel();

  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 1);
  // "abc" is 3 bytes.
  EXPECT_EQ(pipeline.stats().bytesDiscarded_, 3);
  AD_EXPECT_NULLOPT(pipeline.pop());
  // A late `push` after `cancel` returns `Closed` and keeps the chunk.
  std::string late = "late";
  EXPECT_EQ(pipeline.push(std::move(late)), PushResult::Closed);
  EXPECT_EQ(late, "late");
  EXPECT_EQ(pipeline.stats().chunksProduced_, 1);
  EXPECT_EQ(pipeline.stats().chunksConsumed_, 0);
  // A second `cancel` when already `Cancelled` is a no-op: it leaves the
  // state as `Cancelled` and does not increment the discard counters.
  pipeline.cancel();
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 1);
  // "abc" is 3 bytes.
  EXPECT_EQ(pipeline.stats().bytesDiscarded_, 3);
}

TEST(AsyncChunkPipelineTest, CancellationReleasesQueuedBuffer) {
  AsyncChunkPipeline<std::shared_ptr<std::string>> pipeline{
      {.runtimeEnabled_ = true}};
  auto chunk = std::make_shared<std::string>("payload");
  const std::weak_ptr<std::string> lifetime = chunk;
  ASSERT_EQ(pipeline.push(std::move(chunk)), PushResult::Accepted);

  pipeline.cancel();

  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  AD_EXPECT_NULLOPT(pipeline.pop());
  // `shared_ptr` has no `size()`: one chunk discarded, zero bytes.
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 1);
  EXPECT_EQ(pipeline.stats().bytesDiscarded_, 0);
  // The `weak_ptr` expires exactly when the pipeline drops its last
  // `shared_ptr` to the queued chunk.
  EXPECT_TRUE(lifetime.expired());
}

TEST(AsyncChunkPipelineTest, DestructorReleasesQueuedChunks) {
  std::weak_ptr<std::string> lifetime;
  {
    AsyncChunkPipeline<std::shared_ptr<std::string>> pipeline{
        {.runtimeEnabled_ = true}};
    auto chunk = std::make_shared<std::string>("payload");
    lifetime = chunk;
    ASSERT_EQ(pipeline.push(std::move(chunk)), PushResult::Accepted);
    EXPECT_TRUE(pipeline.isEnabled());
    EXPECT_TRUE(pipeline.isRunning());
    EXPECT_EQ(pipeline.stats().chunksProduced_, 1);
    // No explicit `cancel`: destruction when `Running` implicitly cancels,
    // discards and counts the queued chunk, and releases ownership.
  }
  EXPECT_TRUE(lifetime.expired());
}

TEST(AsyncChunkPipelineTest, PropagatesFailureAfterQueuedChunks) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("before-error"), PushResult::Accepted);
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"producer failed"}));
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"before-error"});
  // "before-error" is 12 bytes: bytesProduced_ == 12 and, by the `pop`
  // above, bytesConsumed_ == 12 (chunksProduced_ == 1, chunksConsumed_ == 1).
  EXPECT_EQ(pipeline.stats().bytesProduced_, 12);
  EXPECT_EQ(pipeline.stats().bytesConsumed_, 12);
  // `static_cast<void>` discards the `[[nodiscard]]` return value while the
  // helper checks the exception message.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(static_cast<void>(pipeline.pop()),
                                        ::testing::StrEq("producer failed"),
                                        std::runtime_error);
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  // A failed pipeline rethrows the first failure on every `pop`.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(static_cast<void>(pipeline.pop()),
                                        ::testing::StrEq("producer failed"),
                                        std::runtime_error);
  // A late `push` after `fail` returns `Closed` and keeps the chunk.
  std::string late = "late";
  EXPECT_EQ(pipeline.push(std::move(late)), PushResult::Closed);
  EXPECT_EQ(late, "late");
  const auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 1);
  EXPECT_EQ(stats.chunksConsumed_, 1);
  EXPECT_EQ(stats.chunksDiscarded_, 0);
  EXPECT_EQ(stats.bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, FailAfterFinishIsNoOp) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("only"), PushResult::Accepted);
  pipeline.finish();
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"too late"}));
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"only"});
  AD_EXPECT_NULLOPT(pipeline.pop());
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
  EXPECT_EQ(pipeline.stats().chunksProduced_, 1);
  EXPECT_EQ(pipeline.stats().chunksConsumed_, 1);
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, CancelAfterFinishIsNoOp) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("only"), PushResult::Accepted);
  pipeline.finish();
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  pipeline.cancel();
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"only"});
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 0);
  EXPECT_EQ(pipeline.stats().bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, SecondFailKeepsFirstException) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"first"}));
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"second"}));
  EXPECT_FALSE(pipeline.isRunning());
  // `isEnabled` reflects the construction-time switches, not the lifecycle.
  EXPECT_TRUE(pipeline.isEnabled());
  // `static_cast<void>` discards the `[[nodiscard]]` return value of `pop`,
  // as in `PropagatesFailureAfterQueuedChunks` above.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(static_cast<void>(pipeline.pop()),
                                        ::testing::StrEq("first"),
                                        std::runtime_error);
  // A failed pipeline rethrows the first failure on every `pop`.
  // `static_cast<void>` discards the `[[nodiscard]]` return value of `pop`
  // (as in `PropagatesFailureAfterQueuedChunks` above).
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(static_cast<void>(pipeline.pop()),
                                        ::testing::StrEq("first"),
                                        std::runtime_error);
  // No `push` was `Accepted`, so no chunk entered the slots and all six
  // stats stay zero.
  const auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 0);
  EXPECT_EQ(stats.chunksConsumed_, 0);
  EXPECT_EQ(stats.chunksDiscarded_, 0);
  EXPECT_EQ(stats.bytesProduced_, 0);
  EXPECT_EQ(stats.bytesConsumed_, 0);
  EXPECT_EQ(stats.bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, DefaultConstructedPipelineIsDisabled) {
  AsyncChunkPipeline<std::string> pipeline;
  EXPECT_FALSE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  // All lifecycle calls are no-ops and nothing is counted.
  pipeline.finish();
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"ignored"}));
  // A `fail(nullptr)` when not `Running` (here `Disabled`) early-returns
  // before the contract check: no throw, state stays `Disabled`.
  pipeline.fail(nullptr);
  pipeline.cancel();
  EXPECT_FALSE(pipeline.isRunning());
  std::string chunk = "kept";
  EXPECT_EQ(pipeline.push(std::move(chunk)), PushResult::Closed);
  // A `Closed` `push` leaves the caller's `chunk` untouched: the pipeline is
  // terminally `Disabled`, so there is nothing to retry.
  EXPECT_EQ(chunk, "kept");
  AD_EXPECT_NULLOPT(pipeline.pop());
  const auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 0);
  EXPECT_EQ(stats.chunksConsumed_, 0);
  EXPECT_EQ(stats.chunksDiscarded_, 0);
  EXPECT_EQ(stats.bytesProduced_, 0);
  EXPECT_EQ(stats.bytesConsumed_, 0);
  EXPECT_EQ(stats.bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, IsRunningTracksLifecycleWhileIsEnabledStays) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_TRUE(pipeline.isRunning());
  // An empty running pipeline has no chunk yet: `pop` returns `std::nullopt`
  // while `isRunning` stays true ("not yet", not "never again").
  AD_EXPECT_NULLOPT(pipeline.pop());
  EXPECT_TRUE(pipeline.isRunning());
  // No `push` was `Accepted`, so no chunk entered the slots and all six
  // stats stay zero.
  const auto emptyStats = pipeline.stats();
  EXPECT_EQ(emptyStats.chunksProduced_, 0);
  EXPECT_EQ(emptyStats.chunksConsumed_, 0);
  EXPECT_EQ(emptyStats.bytesProduced_, 0);
  EXPECT_EQ(emptyStats.bytesConsumed_, 0);
  pipeline.finish();
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  // A second `finish` is a no-op.
  pipeline.finish();
  EXPECT_FALSE(pipeline.isRunning());
}

TEST(AsyncChunkPipelineTest, RejectedPushLeavesChunkAndStatsUntouched) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);
  std::string third = "third";
  EXPECT_EQ(pipeline.push(std::move(third)), PushResult::Full);
  // The rejected `push` returns `Full` and leaves `third` unmoved ("third"
  // retained), so the caller can retry it after a `pop`.
  EXPECT_EQ(third, "third");
  EXPECT_EQ(pipeline.stats().chunksProduced_, 2);
  // "first"(5) + "second"(6) = 11: bytesProduced_ == 11, the rejected
  // "third" adds nothing.
  EXPECT_EQ(pipeline.stats().bytesProduced_, 11);
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"first"});
  EXPECT_EQ(pipeline.push(std::move(third)), PushResult::Accepted);
  EXPECT_EQ(pipeline.stats().chunksProduced_, 3);
  // After the retry bytesProduced_ == 16 (11 prior + "third"(5)) and
  // chunksProduced_ == 3, while bytesConsumed_ == 5 and chunksConsumed_ == 1
  // (only "first" popped); bytesDiscarded_ == 0.
  EXPECT_EQ(pipeline.stats().bytesProduced_, 16);
  EXPECT_EQ(pipeline.stats().bytesConsumed_, 5);
}

TEST(AsyncChunkPipelineTest, FailureDrainsBothSlotsThenRethrowsRepeatedly) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"producer failed"}));
  EXPECT_FALSE(pipeline.isRunning());
  // The pipeline was enabled at construction and stays so after `fail`.
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"first"});
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"second"});
  for (int i = 0; i < 2; ++i) {
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(static_cast<void>(pipeline.pop()),
                                          ::testing::StrEq("producer failed"),
                                          std::runtime_error);
  }
  // A failed export cannot be cancelled afterwards.
  pipeline.cancel();
  // `static_cast<void>` discards the `[[nodiscard]]` return value of `pop`
  // (as in `PropagatesFailureAfterQueuedChunks` above).
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(static_cast<void>(pipeline.pop()),
                                        ::testing::StrEq("producer failed"),
                                        std::runtime_error);
  // A late `finish` after `Failed` (state_ != `Running`) is a no-op: it
  // leaves state_ as `Failed` with the first failure retained, so `pop`
  // keeps rethrowing and `push` stays `Closed`.
  pipeline.finish();
  // `static_cast<void>` discards the `[[nodiscard]]` return value of `pop`
  // (as in `PropagatesFailureAfterQueuedChunks` above).
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(static_cast<void>(pipeline.pop()),
                                        ::testing::StrEq("producer failed"),
                                        std::runtime_error);
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 0);
  // "first"(5) + "second"(6) = 11: bytesProduced_ == 11 and
  // bytesConsumed_ == 11, chunksDiscarded_ == 0 and bytesDiscarded_ == 0
  // (both queued chunks were consumed before the rethrow).
  EXPECT_EQ(pipeline.stats().bytesProduced_, 11);
  EXPECT_EQ(pipeline.stats().bytesConsumed_, 11);
  EXPECT_EQ(pipeline.stats().bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, FailRequiresAnException) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  // `HasSubstr` tolerates the contract macro's file/line prefix around the
  // `AD_CONTRACT_CHECK(failure != nullptr)` expression text.
  AD_EXPECT_THROW_WITH_MESSAGE(pipeline.fail(nullptr),
                               ::testing::HasSubstr("failure != nullptr"));
  EXPECT_TRUE(pipeline.isRunning());
  // The rejected `fail(nullptr)` throws, leaves state_ as `Running`
  // (`isRunning` true, `isEnabled` true, no failure stored), and leaves all
  // six stats at 0.
  const auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 0);
  EXPECT_EQ(stats.bytesProduced_, 0);
}

TEST(AsyncChunkPipelineTest, CancelOfEmptyPipelineCloses) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  pipeline.cancel();
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  AD_EXPECT_NULLOPT(pipeline.pop());
  // A late `push` after `cancel` returns `Closed` and keeps the chunk.
  std::string late = "late";
  EXPECT_EQ(pipeline.push(std::move(late)), PushResult::Closed);
  EXPECT_EQ(late, "late");
  const auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 0);
  EXPECT_EQ(stats.chunksConsumed_, 0);
  EXPECT_EQ(stats.chunksDiscarded_, 0);
  EXPECT_EQ(stats.bytesProduced_, 0);
  EXPECT_EQ(stats.bytesConsumed_, 0);
  EXPECT_EQ(stats.bytesDiscarded_, 0);
  // Late `fail` and a second `cancel` are no-ops.
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"too late"}));
  pipeline.cancel();
  AD_EXPECT_NULLOPT(pipeline.pop());
  // A late `finish` after `Cancelled` (state_ != `Running`) is a no-op: it
  // leaves state_ as `Cancelled` (`isEnabled` true, `isRunning` false, no
  // failure stored), so `pop` still returns `std::nullopt`, `push` still
  // returns `Closed`, and the discard counters remain 0.
  pipeline.finish();
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  AD_EXPECT_NULLOPT(pipeline.pop());
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 0);
  EXPECT_EQ(pipeline.stats().bytesDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, CancellationCountsDiscardedBytes) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("abc"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("de"), PushResult::Accepted);
  pipeline.cancel();
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 2);
  // "abc"(3) + "de"(2) = 5 discarded bytes.
  EXPECT_EQ(pipeline.stats().bytesDiscarded_, 5);
}

}  // namespace
