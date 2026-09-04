// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Marvin Stoetzel <marvin.stoetzel@mailbox.org>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include "engine/export_v2/ElasticExportScheduler.h"
#include "util/http/websocket/QueryId.h"

using namespace ad_utility::export_v2;
using namespace std::chrono_literals;

// -----------------------------------------------------------------------------
// Test 1: Basic Execution & In-Order Consumption
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, BasicExecutionAndInOrderConsumption) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);

  // Set active foreground queries to `1` (only this export query is running).
  scheduler.onForegroundQueryStarted();
  EXPECT_EQ(scheduler.activeForegroundQueries(), 1u);

  auto session = scheduler.createSession<std::string>();
  EXPECT_EQ(session.state(), SessionState::HelpersEligible);

  constexpr size_t numMorsels = 10;
  for (size_t i = 0; i < numMorsels; ++i) {
    session.submitMorsel([i]() {
      std::this_thread::sleep_for(2ms);
      return "result_" + std::to_string(i);
    });
  }

  EXPECT_EQ(session.totalSlots(), numMorsels);
  EXPECT_TRUE(session.hasMoreResults());

  for (size_t i = 0; i < numMorsels; ++i) {
    std::string res = session.consumeNextResult();
    EXPECT_EQ(res, "result_" + std::to_string(i));
  }

  EXPECT_FALSE(session.hasMoreResults());
  EXPECT_EQ(session.consumedSlots(), numMorsels);

  auto profiles = session.inspectMorselProfiles();
  EXPECT_EQ(profiles.size(), numMorsels);
  for (size_t i = 0; i < numMorsels; ++i) {
    EXPECT_EQ(profiles[i].morselIndex_, i);
    EXPECT_EQ(profiles[i].finalStatus_, MorselStatus::Completed);
  }

  scheduler.onForegroundQueryEnded();
  EXPECT_EQ(scheduler.activeForegroundQueries(), 0u);
}

// -----------------------------------------------------------------------------
// Test 2: Single-Core Fallback Under High Foreground Load (> 1 Queries)
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, SingleCoreFallbackUnderHighForegroundLoad) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);

  // Simulate 2 active queries (e.g. export query + another concurrent query)
  scheduler.onForegroundQueryStarted();
  scheduler.onForegroundQueryStarted();
  EXPECT_EQ(scheduler.activeForegroundQueries(), 2u);

  auto session = scheduler.createSession<int>();
  // Because activeForegroundQueries > 1, state must be PrimaryOnly
  EXPECT_EQ(session.state(), SessionState::PrimaryOnly);

  constexpr size_t numMorsels = 5;
  for (size_t i = 0; i < numMorsels; ++i) {
    session.submitMorsel([i]() { return static_cast<int>(i * 10); });
  }

  for (size_t i = 0; i < numMorsels; ++i) {
    int val = session.consumeNextResult();
    EXPECT_EQ(val, static_cast<int>(i * 10));
  }

  auto profiles = session.inspectMorselProfiles();
  for (const auto& p : profiles) {
    EXPECT_FALSE(p.executedByHelper_);
    EXPECT_EQ(p.finalStatus_, MorselStatus::Completed);
  }

  scheduler.onForegroundQueryEnded();
  scheduler.onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 3: Dynamic Scale-Out When Foreground Query Completes
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, DynamicScaleOutWhenServerBecomesIdle) {
  ElasticExportScheduler scheduler(4, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);

  // Initially 2 queries active (helpers disabled)
  scheduler.onForegroundQueryStarted();
  scheduler.onForegroundQueryStarted();
  EXPECT_EQ(scheduler.activeForegroundQueries(), 2u);

  auto session = scheduler.createSession<std::string>();
  EXPECT_EQ(session.state(), SessionState::PrimaryOnly);

  for (size_t i = 0; i < 6; ++i) {
    session.submitMorsel([i]() {
      std::this_thread::sleep_for(5ms);
      return "dynamic_" + std::to_string(i);
    });
  }

  // End the concurrent query so that the active query count drops to `1`.
  scheduler.onForegroundQueryEnded();
  EXPECT_EQ(scheduler.activeForegroundQueries(), 1u);
  EXPECT_EQ(session.state(), SessionState::HelpersEligible);

    auto results = session.drainRemainingResults();
  EXPECT_EQ(results.size(), 6u);
  for (size_t i = 0; i < 6; ++i) {
    EXPECT_EQ(results[i], "dynamic_" + std::to_string(i));
  }

  scheduler.onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 4: Cooperative Revocation Under Foreground Pressure
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, CooperativeRevocationUnderForegroundPressure) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);

  scheduler.onForegroundQueryStarted();  // Query count = 1 (eligible)
  auto session = scheduler.createSession<int>();
  EXPECT_EQ(session.state(), SessionState::HelpersEligible);

  std::promise<void> morsel0StartedPromise;
  std::shared_future<void> morsel0Started =
      morsel0StartedPromise.get_future().share();
  std::promise<void> unblockMorsel0Promise;
  std::shared_future<void> unblockMorsel0 =
      unblockMorsel0Promise.get_future().share();

  // Submit morsel 0 which pauses while holding the helper lease
  session.submitMorsel(
      [morsel0StartedPromise = std::move(morsel0StartedPromise),
       unblockMorsel0]() mutable {
        morsel0StartedPromise.set_value();
        unblockMorsel0.wait();
        return 100;
      });

  // Submit morsel 1
  session.submitMorsel([]() { return 200; });

    morsel0Started.wait();

    scheduler.onForegroundQueryStarted();
  EXPECT_EQ(scheduler.activeForegroundQueries(), 2u);

  // Verify that the session transitions to `Revoking` while helper `0` is actively leased.
  EXPECT_EQ(session.state(), SessionState::Revoking);

    unblockMorsel0Promise.set_value();

  // Consume morsel 0 (was executed by helper)
  int r0 = session.consumeNextResult();
  EXPECT_EQ(r0, 100);

  // Once helper 0 released lease, session transitions to PrimaryOnly
  // Morsel 1 should be executed on coordinator thread
  int r1 = session.consumeNextResult();
  EXPECT_EQ(r1, 200);

  auto profiles = session.inspectMorselProfiles();
  EXPECT_TRUE(profiles[0].executedByHelper_);
  EXPECT_FALSE(profiles[1].executedByHelper_);

  scheduler.onForegroundQueryEnded();
  scheduler.onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 5: Deterministic Slot Ordering Under Out-Of-Order Helper Completion
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, DeterministicSlotOrderingWithVaryingDelays) {
  ElasticExportScheduler scheduler(4, 128);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);
  scheduler.onForegroundQueryStarted();

  auto session = scheduler.createSession<size_t>();

  constexpr size_t count = 20;
  for (size_t i = 0; i < count; ++i) {
    session.submitMorsel([i]() {
            // Invert delays: slot 0 sleeps longest (40ms), slot 19 sleeps shortest
      // (2ms)
      auto delay = std::chrono::milliseconds((count - i) * 2);
      std::this_thread::sleep_for(delay);
      return i;
    });
  }

  for (size_t i = 0; i < count; ++i) {
    size_t result = session.consumeNextResult();
    EXPECT_EQ(result, i);
  }

  scheduler.onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 6: Cancellation Cleanup
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, CancellationStopsAdmissionAndCleansUp) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);
  scheduler.onForegroundQueryStarted();

  auto session = scheduler.createSession<int>();

  std::promise<void> startedPromise;
  auto startedFuture = startedPromise.get_future();
  std::promise<void> unblockPromise;
  auto unblockFuture = unblockPromise.get_future().share();

  session.submitMorsel(
      [startedPromise = std::move(startedPromise), unblockFuture]() mutable {
        startedPromise.set_value();
        unblockFuture.wait();
        return 42;
      });

  for (size_t i = 1; i < 5; ++i) {
    session.submitMorsel([]() { return 99; });
  }

  startedFuture.wait();

    session.cancel();
  EXPECT_EQ(session.state(), SessionState::Closed);

    unblockPromise.set_value();

    EXPECT_THROW(session.consumeNextResult(), ad_utility::Exception);

  scheduler.onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 7: Move Semantics & RAII
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, MoveSemanticsAndRAII) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.onForegroundQueryStarted();

  auto session = scheduler.createSession<std::string>();
  session.submitMorsel([]() { return "moved"; });

  auto movedSession = std::move(session);
  EXPECT_EQ(movedSession.consumeNextResult(), "moved");

  ExportWorkLease lease1(&scheduler, 1, 10, 100);
  EXPECT_TRUE(lease1.isValid());
  EXPECT_EQ(lease1.epoch(), 1u);
  EXPECT_EQ(lease1.jobId(), 10u);

  ExportWorkLease lease2 = std::move(lease1);
  EXPECT_FALSE(lease1.isValid());
  EXPECT_TRUE(lease2.isValid());
  lease2.release();
  EXPECT_FALSE(lease2.isValid());

  scheduler.onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 8: QueryRegistry Lifecycle Hook Integration
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, QueryRegistryLifecycleHookIntegration) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);

  ad_utility::websocket::QueryRegistry registry;
  scheduler.attachToQueryRegistry(registry);

  EXPECT_EQ(scheduler.activeForegroundQueries(), 0u);

  {
    auto q1 = registry.uniqueId("SELECT ?x WHERE { ?x ?p ?o }");
    EXPECT_EQ(scheduler.activeForegroundQueries(), 1u);

    {
      auto q2 = registry.uniqueId("SELECT ?y WHERE { ?y ?p ?o }");
      EXPECT_EQ(scheduler.activeForegroundQueries(), 2u);
    }
    // q2 destroyed -> end callback fired
    EXPECT_EQ(scheduler.activeForegroundQueries(), 1u);
  }
  // q1 destroyed -> end callback fired
  EXPECT_EQ(scheduler.activeForegroundQueries(), 0u);
}

// -----------------------------------------------------------------------------
// Test 9: Concurrent Multi-Session Stress Test
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, ConcurrentMultiSessionStressTest) {
  ElasticExportScheduler scheduler(4, 256);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);

  std::atomic<bool> stopQueryChanger{false};
  // Start a background thread that fluctuates foreground demand.
  std::thread queryChanger([&]() {
    while (!stopQueryChanger.load()) {
      scheduler.onForegroundQueryStarted();
      std::this_thread::sleep_for(1ms);
      scheduler.onForegroundQueryEnded();
      std::this_thread::sleep_for(1ms);
    }
  });

  constexpr size_t numWorkerThreads = 4;
  constexpr size_t morselsPerSession = 15;
  std::vector<std::thread> sessionRunners;
  sessionRunners.reserve(numWorkerThreads);

  for (size_t t = 0; t < numWorkerThreads; ++t) {
    sessionRunners.emplace_back([&scheduler, t]() {
      auto session = scheduler.createSession<std::string>();
      for (size_t i = 0; i < morselsPerSession; ++i) {
        session.submitMorsel([t, i]() {
          return "t" + std::to_string(t) + "_m" + std::to_string(i);
        });
      }
      for (size_t i = 0; i < morselsPerSession; ++i) {
        std::string expected =
            "t" + std::to_string(t) + "_m" + std::to_string(i);
        std::string actual = session.consumeNextResult();
        EXPECT_EQ(actual, expected);
      }
    });
  }

  for (auto& t : sessionRunners) {
    t.join();
  }

  stopQueryChanger.store(true);
  queryChanger.join();
}

// -----------------------------------------------------------------------------
// Test 10: Worker Exception Propagates to Coordinator Without Leaks
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, WorkerExceptionPropagatesToCoordinator) {
  ElasticExportScheduler scheduler(2, 64);
  auto session = scheduler.createSession<int>();

  // Slot 0 succeeds
  session.submitMorsel([]() -> int { return 42; });
  // Slot 1 throws an exception
  session.submitMorsel([]() -> int {
    throw std::runtime_error("Simulated morsel processing failure");
  });
  // Slot 2 succeeds
  session.submitMorsel([]() -> int { return 100; });

  // Slot 0 should return 42
  EXPECT_EQ(session.consumeNextResult(), 42);

  // Slot 1 should throw std::runtime_error
  EXPECT_THROW(
      {
        try {
          [[maybe_unused]] int r = session.consumeNextResult();
        } catch (const std::runtime_error& e) {
          EXPECT_STREQ(e.what(), "Simulated morsel processing failure");
          throw;
        }
      },
      std::runtime_error);

  // Slot 2 should still return 100
  EXPECT_EQ(session.consumeNextResult(), 100);

  // Verify lease accounting did not leak
  EXPECT_EQ(scheduler.totalActiveHelpers(), 0u);
}

// -----------------------------------------------------------------------------
// Test 11: Clean Shutdown Under High Foreground Load With Pending Morsels
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, CleanShutdownUnderHighForegroundLoad) {
  ElasticExportScheduler scheduler(4, 64);

  // Simulate high foreground load (helpers ineligible)
  scheduler.onForegroundQueryStarted();
  scheduler.onForegroundQueryStarted();
  scheduler.onForegroundQueryStarted();

  // Create session and enqueue morsels
  auto session = scheduler.createSession<int>();
  for (int i = 0; i < 20; ++i) {
    session.submitMorsel([i]() -> int { return i * 2; });
  }

  // Shutdown scheduler while queue may contain pending items under high load
  // Must return promptly without deadlock or infinite spin loop
  scheduler.shutdown();
  EXPECT_EQ(scheduler.totalActiveHelpers(), 0u);
}

