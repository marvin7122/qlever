// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <array>
#include <atomic>
#include <chrono>
#include <stdexcept>
#include <thread>

#include "util/OrderedTaskWindow.h"

using ad_utility::OrderedTaskWindow;
using Task = OrderedTaskWindow<size_t>::Task;

// _____________________________________________________________________________
TEST(OrderedTaskWindow, resolveNumThreads) {
  EXPECT_EQ(ad_utility::resolveNumThreads(1), 1u);
  EXPECT_EQ(ad_utility::resolveNumThreads(7), 7u);
  EXPECT_GE(ad_utility::resolveNumThreads(0), 1u);
}

// _____________________________________________________________________________
// Results come back in submission order, also when later tasks finish first,
// and every task gets a valid worker index.
TEST(OrderedTaskWindow, resultsInSubmissionOrder) {
  constexpr size_t numWorkers = 3;
  OrderedTaskWindow<size_t> window{numWorkers, 2 * numWorkers};
  EXPECT_EQ(window.numWorkers(), numWorkers);
  EXPECT_EQ(window.maxInFlight(), 2 * numWorkers);
  EXPECT_TRUE(window.empty());
  std::atomic<bool> badWorkerIndex = false;
  std::vector<size_t> results;
  constexpr size_t numTasks = 100;
  size_t submitted = 0;
  while (results.size() < numTasks) {
    while (submitted < numTasks && !window.full()) {
      window.submit(Task{[i = submitted, &badWorkerIndex](size_t worker) {
        if (worker >= numWorkers) {
          badWorkerIndex = true;
        }
        // Early tasks take longest, so they finish out of order.
        std::this_thread::sleep_for(
            std::chrono::microseconds((7 - i % 7) * 50));
        return i;
      }});
      ++submitted;
    }
    EXPECT_FALSE(window.empty());
    results.push_back(window.popFront());
  }
  EXPECT_TRUE(window.empty());
  EXPECT_FALSE(badWorkerIndex);
  for (size_t i = 0; i < numTasks; ++i) {
    EXPECT_EQ(results[i], i);
  }
}

// _____________________________________________________________________________
// Per-worker state needs no lock: no two tasks with the same worker index run
// at the same time.
TEST(OrderedTaskWindow, workerIndexIsExclusive) {
  constexpr size_t numWorkers = 4;
  std::array<std::atomic<int>, numWorkers> running{};
  std::atomic<bool> overlap = false;
  OrderedTaskWindow<size_t> window{numWorkers, 2 * numWorkers};
  for (size_t round = 0; round < 50; ++round) {
    while (!window.full()) {
      window.submit(Task{[&](size_t worker) {
        if (running[worker].fetch_add(1) != 0) {
          overlap = true;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(20));
        running[worker].fetch_sub(1);
        return worker;
      }});
    }
    window.popFront();
  }
  while (!window.empty()) {
    window.popFront();
  }
  EXPECT_FALSE(overlap);
}

// _____________________________________________________________________________
// An exception of a task is rethrown by `popFront`, later tasks still work.
TEST(OrderedTaskWindow, exceptionIsRethrown) {
  OrderedTaskWindow<size_t> window{2, 4};
  window.submit(Task{[](size_t) -> size_t { return 1; }});
  window.submit(
      Task{[](size_t) -> size_t { throw std::runtime_error{"task failed"}; }});
  window.submit(Task{[](size_t) -> size_t { return 3; }});
  EXPECT_EQ(window.popFront(), 1u);
  EXPECT_THROW(window.popFront(), std::runtime_error);
  EXPECT_EQ(window.popFront(), 3u);
}

// _____________________________________________________________________________
// Destroying a window with unconsumed tasks joins the running tasks and drops
// the queued ones.
TEST(OrderedTaskWindow, destructionDropsQueuedTasks) {
  std::atomic<size_t> numStarted = 0;
  {
    OrderedTaskWindow<size_t> window{1, 50};
    while (!window.full()) {
      window.submit(Task{[&numStarted](size_t) {
        ++numStarted;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        return size_t{0};
      }});
    }
    EXPECT_TRUE(window.full());
    EXPECT_ANY_THROW(window.submit(Task{[](size_t) { return size_t{0}; }}));
  }
  EXPECT_LT(numStarted.load(), 50u);
}

// _____________________________________________________________________________
TEST(OrderedTaskWindow, contractChecks) {
  EXPECT_ANY_THROW((OrderedTaskWindow<size_t>{0, 1}));
  EXPECT_ANY_THROW((OrderedTaskWindow<size_t>{1, 0}));
  OrderedTaskWindow<size_t> window{1, 1};
  EXPECT_ANY_THROW(window.popFront());
}
