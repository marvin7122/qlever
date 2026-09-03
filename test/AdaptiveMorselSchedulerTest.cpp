// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include "engine/export_v2/AdaptiveMorselScheduler.h"

using namespace ql::engine::export_v2;

TEST(AdaptiveMorselSchedulerTest, InitialDefaultMorselSize) {
  AdaptiveMorselScheduler scheduler;
  EXPECT_EQ(scheduler.getCurrentMorselSize(), AdaptiveMorselScheduler::DEFAULT_MORSEL_ROWS);
}

TEST(AdaptiveMorselSchedulerTest, ShrinkOnHighLatency) {
  AdaptiveMorselScheduler scheduler;
  // Report high latency (10 ms >> 2 ms target)
  scheduler.recordMorselTelemetry(16384, std::chrono::microseconds{10000});

  EXPECT_LT(scheduler.getCurrentMorselSize(), AdaptiveMorselScheduler::DEFAULT_MORSEL_ROWS);
  EXPECT_GE(scheduler.getCurrentMorselSize(), AdaptiveMorselScheduler::MIN_MORSEL_ROWS);
}

TEST(AdaptiveMorselSchedulerTest, GrowOnLowLatency) {
  AdaptiveMorselScheduler scheduler;
  // Report ultra-low latency (200 µs << 2 ms target)
  scheduler.recordMorselTelemetry(16384, std::chrono::microseconds{200});

  EXPECT_GT(scheduler.getCurrentMorselSize(), AdaptiveMorselScheduler::DEFAULT_MORSEL_ROWS);
  EXPECT_LE(scheduler.getCurrentMorselSize(), AdaptiveMorselScheduler::MAX_MORSEL_ROWS);
}
