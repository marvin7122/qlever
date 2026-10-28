// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <exception>
#include <stdexcept>
#include <string>

#include "engine/export_v2/AsyncChunkPipeline.h"
#include "util/GTestHelpers.h"

namespace {

TEST(AsyncChunkPipelineDisabledTest, CompileTimeSwitchOverridesRuntimeOptIn) {
  static_assert(!qlever::export_v2::exportV2CompiledIn);
  qlever::export_v2::AsyncChunkPipeline<std::string> pipeline{
      {.runtimeEnabled_ = true}};

  EXPECT_FALSE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  // A `Closed` push leaves the caller's chunk untouched for reuse elsewhere.
  std::string chunk = "kept";
  EXPECT_EQ(pipeline.push(std::move(chunk)),
            qlever::export_v2::PushResult::Closed);
  EXPECT_EQ(chunk, "kept");
  AD_EXPECT_NULLOPT(pipeline.pop());
  AD_EXPECT_NULLOPT(pipeline.pop());
  // All lifecycle calls are no-ops and nothing is counted, mirroring the
  // runtime-disabled contract in `AsyncChunkPipelineTest`.
  pipeline.finish();
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"ignored"}));
  // A null failure outside `Running` is a no-op like any other late `fail`.
  pipeline.fail(nullptr);
  pipeline.cancel();
  EXPECT_FALSE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  AD_EXPECT_NULLOPT(pipeline.pop());
  EXPECT_EQ(pipeline.push("late"), qlever::export_v2::PushResult::Closed);
  const auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 0);
  EXPECT_EQ(stats.chunksConsumed_, 0);
  EXPECT_EQ(stats.chunksDiscarded_, 0);
  EXPECT_EQ(stats.bytesProduced_, 0);
  EXPECT_EQ(stats.bytesConsumed_, 0);
  EXPECT_EQ(stats.bytesDiscarded_, 0);
}

}  // namespace
