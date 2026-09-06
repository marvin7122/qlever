// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <string>

// This test verifies the compiled-out configuration, but the top-level build
// defines `QLEVER_ENABLE_EXPORT_V2=1` globally (default ON). Undefine it for
// this translation unit so the header below genuinely sees the switch off.
#undef QLEVER_ENABLE_EXPORT_V2

#include "engine/export_v2/AsyncChunkPipeline.h"

namespace {

TEST(AsyncChunkPipelineDisabledTest, CompileTimeSwitchOverridesRuntimeOptIn) {
  static_assert(!qlever::export_v2::kExportV2CompiledIn);
  qlever::export_v2::AsyncChunkPipeline<std::string> pipeline{
      {.capacity_ = 2, .runtimeEnabled_ = true}};

  EXPECT_FALSE(pipeline.isEnabled());
  EXPECT_EQ(pipeline.push("ignored"), qlever::export_v2::PushResult::Closed);
  EXPECT_FALSE(pipeline.pop().has_value());
}

}  // namespace
