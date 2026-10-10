// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <future>
#include <limits>
#include <stdexcept>
#include <thread>
#include <vector>

#include "util/AsioHelpers.h"
#include "util/Exception.h"
#include "util/GTestHelpers.h"
#include "util/GlobalExecutor.h"

#if GTEST_HAS_DEATH_TEST
// _____________________________________________________________________________
TEST(GlobalExecutorDeathTest, failedConstructionAllowsReconfigurationAndRetry) {
  const auto oldStyle = ::testing::FLAGS_gtest_death_test_style;
  absl::Cleanup restoreStyle{
      [oldStyle] { ::testing::FLAGS_gtest_death_test_style = oldStyle; }};
  // Re-execute in a fresh process, independent of the singleton's state in the
  // parent and the order in which the tests run.
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  auto checkRetry = [] {
    try {
      ad_utility::setGlobalExecutorNumThreads(
          std::numeric_limits<size_t>::max());
      try {
        // Boost rejects this size before creating any worker threads.
        ad_utility::globalExecutor();
        std::_Exit(1);
      } catch (const std::out_of_range&) {
      }
      if (!ad_utility::trySetGlobalExecutorNumThreads(1)) {
        std::_Exit(2);
      }
      auto executor = ad_utility::globalExecutor();
      if (!static_cast<bool>(executor) ||
          ad_utility::globalExecutorNumThreads() != 1) {
        std::_Exit(3);
      }
      auto future = ad_utility::runFunctionOnExecutor(
          executor, [] { return 42; }, ad_utility::net::use_future);
      if (future.get() != 42) {
        std::_Exit(4);
      }
    } catch (...) {
      std::_Exit(5);
    }
    std::_Exit(0);
  };
  EXPECT_EXIT(checkRetry(), ::testing::ExitedWithCode(0), "");
}
#endif

// NOTE: The global executor is a process-wide singleton, so none of the
// following tests may assume that the pool doesn't exist yet. They are
// therefore deliberately written such that they pass in any order.

// _____________________________________________________________________________
TEST(GlobalExecutor, numThreadsIsPositive) {
  EXPECT_GE(ad_utility::globalExecutorNumThreads(), 1u);
}

// _____________________________________________________________________________
TEST(GlobalExecutor, numThreadsMustBePositive) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::setGlobalExecutorNumThreads(0),
      ::testing::HasSubstr("Assertion `numThreads > 0` failed"));
}

// _____________________________________________________________________________
TEST(GlobalExecutor, executorRunsPostedTasks) {
  auto executor = ad_utility::globalExecutor();
  ASSERT_TRUE(static_cast<bool>(executor));
  auto future = ad_utility::runFunctionOnExecutor(
      executor, []() { return 42; }, ad_utility::net::use_future);
  EXPECT_EQ(future.get(), 42);
}

// _____________________________________________________________________________
TEST(GlobalExecutor, executorRunsManyTasks) {
  auto executor = ad_utility::globalExecutor();
  std::atomic<size_t> counter = 0;
  std::vector<std::future<void>> futures;
  static constexpr size_t numTasks = 100;
  for (size_t i = 0; i < numTasks; ++i) {
    futures.push_back(ad_utility::runFunctionOnExecutor(
        executor, [&counter]() { ++counter; }, ad_utility::net::use_future));
  }
  for (auto& future : futures) {
    future.get();
  }
  EXPECT_EQ(counter.load(), numTasks);
}

// _____________________________________________________________________________
TEST(GlobalExecutor, settingTheNumThreadsTooLate) {
  // Make sure that the pool exists, no matter in which order the tests run.
  auto numThreadsBefore = ad_utility::globalExecutorNumThreads();
  ad_utility::globalExecutor();
  // The pool now exists and cannot be resized, so setting a different number
  // of threads fails and leaves the configuration unchanged.
  EXPECT_FALSE(
      ad_utility::trySetGlobalExecutorNumThreads(numThreadsBefore + 1));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::setGlobalExecutorNumThreads(numThreadsBefore + 1),
      ::testing::HasSubstr(
          "must not be set after the pool has already been accessed"));
  EXPECT_EQ(ad_utility::globalExecutorNumThreads(), numThreadsBefore);
  // Setting the number of threads that the pool already has is not a change
  // and therefore succeeds. This matters for a process that builds several
  // indices, because each build sets the number of threads.
  EXPECT_TRUE(ad_utility::trySetGlobalExecutorNumThreads(numThreadsBefore));
  EXPECT_NO_THROW(ad_utility::setGlobalExecutorNumThreads(numThreadsBefore));
  EXPECT_EQ(ad_utility::globalExecutorNumThreads(), numThreadsBefore);
  // In particular, the executor still works afterwards.
  auto future = ad_utility::runFunctionOnExecutor(
      ad_utility::globalExecutor(), []() { return 1; },
      ad_utility::net::use_future);
  EXPECT_EQ(future.get(), 1);
}

// _____________________________________________________________________________
TEST(GlobalExecutor, isThreadSafe) {
  // Concurrently obtain the executor and the number of threads from several
  // threads. This is mostly a smoke test for the thread sanitizer.
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 8; ++i) {
    threads.emplace_back([]() {
      EXPECT_GE(ad_utility::globalExecutorNumThreads(), 1u);
      auto executor = ad_utility::globalExecutor();
      EXPECT_TRUE(static_cast<bool>(executor));
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
