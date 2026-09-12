// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Marvin Stoetzel <marvin.stoetzel@mailbox.org>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <optional>
#include <stdexcept>
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
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  // Set active foreground queries to 1 (only this export query running)
  scheduler->onForegroundQueryStarted();
  EXPECT_EQ(scheduler->activeForegroundQueries(), 1u);

  auto session = scheduler->createSession<std::string>();
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

  scheduler->onForegroundQueryEnded();
  EXPECT_EQ(scheduler->activeForegroundQueries(), 0u);
}

// -----------------------------------------------------------------------------
// Test 2: Single-Core Fallback Under High Foreground Load (> 1 Queries)
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, SingleCoreFallbackUnderHighForegroundLoad) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  // Simulate 2 active queries (e.g. export query + another concurrent query)
  scheduler->onForegroundQueryStarted();
  scheduler->onForegroundQueryStarted();
  EXPECT_EQ(scheduler->activeForegroundQueries(), 2u);

  auto session = scheduler->createSession<int>();
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

  scheduler->onForegroundQueryEnded();
  scheduler->onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 3: Dynamic Scale-Out When Foreground Query Completes
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, DynamicScaleOutWhenServerBecomesIdle) {
  auto scheduler = ElasticExportScheduler::create(4, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  // Initially 2 queries active (helpers disabled)
  scheduler->onForegroundQueryStarted();
  scheduler->onForegroundQueryStarted();
  EXPECT_EQ(scheduler->activeForegroundQueries(), 2u);

  auto session = scheduler->createSession<std::string>();
  EXPECT_EQ(session.state(), SessionState::PrimaryOnly);

  for (size_t i = 0; i < 6; ++i) {
    session.submitMorsel([i]() {
      std::this_thread::sleep_for(5ms);
      return "dynamic_" + std::to_string(i);
    });
  }

  // The concurrent query finishes; active queries drop to 1
  scheduler->onForegroundQueryEnded();
  EXPECT_EQ(scheduler->activeForegroundQueries(), 1u);
  EXPECT_EQ(session.state(), SessionState::HelpersEligible);

  // Consume all results
  auto results = session.drainRemainingResults();
  EXPECT_EQ(results.size(), 6u);
  for (size_t i = 0; i < 6; ++i) {
    EXPECT_EQ(results[i], "dynamic_" + std::to_string(i));
  }

  scheduler->onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 4: Cooperative Revocation Under Foreground Pressure
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, CooperativeRevocationUnderForegroundPressure) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  scheduler->onForegroundQueryStarted();  // Query count = 1 (eligible)
  auto session = scheduler->createSession<int>();
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

  // Wait until morsel 0 is running on a helper. Morsel 1 is submitted only
  // after revocation below: submitting it earlier would let the second
  // helper pick it up before the revocation arrives, which makes
  // `executedByHelper_` for morsel 1 nondeterministic (cooperative
  // revocation only stops not-yet-started helper work).
  morsel0Started.wait();

  // A new foreground query starts! (count = 2)
  scheduler->onForegroundQueryStarted();
  EXPECT_EQ(scheduler->activeForegroundQueries(), 2u);

  // Session must transition to Revoking because helper 0 is actively leased
  EXPECT_EQ(session.state(), SessionState::Revoking);

  // Submit morsel 1 while helpers are revoked, so it stays pending and runs
  // on the coordinator thread.
  session.submitMorsel([]() { return 200; });

  // Allow morsel 0 to complete cooperatively
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

  scheduler->onForegroundQueryEnded();
  scheduler->onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 5: Deterministic Slot Ordering Under Out-Of-Order Helper Completion
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, DeterministicSlotOrderingWithVaryingDelays) {
  auto scheduler = ElasticExportScheduler::create(4, 128);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);
  scheduler->onForegroundQueryStarted();

  auto session = scheduler->createSession<size_t>();

  constexpr size_t count = 20;
  for (size_t i = 0; i < count; ++i) {
    session.submitMorsel([i]() {
      // Invert delays: slot 0 sleeps longest (20ms), slot 19 sleeps shortest
      // (1ms)
      auto delay = std::chrono::milliseconds((count - i) * 2);
      std::this_thread::sleep_for(delay);
      return i;
    });
  }

  for (size_t i = 0; i < count; ++i) {
    size_t result = session.consumeNextResult();
    EXPECT_EQ(result, i);
  }

  scheduler->onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 6: Cancellation Cleanup
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, CancellationStopsAdmissionAndCleansUp) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);
  scheduler->onForegroundQueryStarted();

  auto session = scheduler->createSession<int>();

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

  // Cancel session
  session.cancel();
  EXPECT_EQ(session.state(), SessionState::Closed);

  // Unblock helper morsel
  unblockPromise.set_value();

  // Attempting to consume from cancelled session should throw
  EXPECT_THROW(session.consumeNextResult(), ad_utility::Exception);

  scheduler->onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 7: Move Semantics & RAII
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, MoveSemanticsAndRAII) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->onForegroundQueryStarted();

  auto session = scheduler->createSession<std::string>();
  session.submitMorsel([]() { return "moved"; });

  auto movedSession = std::move(session);
  EXPECT_EQ(movedSession.consumeNextResult(), "moved");

  ExportWorkLease lease1(scheduler.get(), 1, 10, 100);
  EXPECT_TRUE(lease1.isValid());
  EXPECT_EQ(lease1.epoch(), 1u);
  EXPECT_EQ(lease1.jobId(), 10u);

  ExportWorkLease lease2 = std::move(lease1);
  EXPECT_FALSE(lease1.isValid());
  EXPECT_TRUE(lease2.isValid());
  lease2.release();
  EXPECT_FALSE(lease2.isValid());

  scheduler->onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 8: QueryRegistry Lifecycle Hook Integration
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, QueryRegistryLifecycleHookIntegration) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  ad_utility::websocket::QueryRegistry registry;
  scheduler->attachToQueryRegistry(registry);

  EXPECT_EQ(scheduler->activeForegroundQueries(), 0u);

  {
    auto q1 = registry.uniqueId("SELECT ?x WHERE { ?x ?p ?o }");
    EXPECT_EQ(scheduler->activeForegroundQueries(), 1u);

    {
      auto q2 = registry.uniqueId("SELECT ?y WHERE { ?y ?p ?o }");
      EXPECT_EQ(scheduler->activeForegroundQueries(), 2u);
    }
    // q2 destroyed -> end callback fired
    EXPECT_EQ(scheduler->activeForegroundQueries(), 1u);
  }
  // q1 destroyed -> end callback fired
  EXPECT_EQ(scheduler->activeForegroundQueries(), 0u);
}

// -----------------------------------------------------------------------------
// Test 8b: Registry Callbacks Expire Safely With The Scheduler
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, RegistryCallbacksExpireSafelyWithScheduler) {
  ad_utility::websocket::QueryRegistry registry;
  std::optional<ad_utility::websocket::OwningQueryId> query;
  {
    auto scheduler = ElasticExportScheduler::create(2, 64);
    scheduler->attachToQueryRegistry(registry);
    query.emplace(registry.uniqueId("SELECT ?x WHERE { ?x ?p ?o }"));
    EXPECT_EQ(scheduler->activeForegroundQueries(), 1u);
  }
  // The scheduler is gone while the query is still registered. Destroying
  // the query fires the end callback into an expired `weak_ptr`, which must
  // be a no-op rather than a use-after-free.
  query.reset();
}

// -----------------------------------------------------------------------------
// Test 9: Concurrent Multi-Session Stress Test
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, ConcurrentMultiSessionStressTest) {
  auto scheduler = ElasticExportScheduler::create(4, 256);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  std::atomic<bool> stopQueryChanger{false};
  // Background thread fluctuating foreground demand
  std::thread queryChanger([&]() {
    while (!stopQueryChanger.load()) {
      scheduler->onForegroundQueryStarted();
      std::this_thread::sleep_for(1ms);
      scheduler->onForegroundQueryEnded();
      std::this_thread::sleep_for(1ms);
    }
  });

  constexpr size_t numWorkerThreads = 4;
  constexpr size_t morselsPerSession = 15;
  std::vector<std::thread> sessionRunners;
  sessionRunners.reserve(numWorkerThreads);

  for (size_t t = 0; t < numWorkerThreads; ++t) {
    sessionRunners.emplace_back([&scheduler, t]() {
      auto session = scheduler->createSession<std::string>();
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
  auto scheduler = ElasticExportScheduler::create(2, 64);
  auto session = scheduler->createSession<int>();

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

  // Verify lease accounting did not leak. The worker releases its lease
  // after signalling slot completion, so the counter reaches zero
  // asynchronously with respect to `consumeNextResult`. Poll with a
  // deadline: a genuine leak never reaches zero and still fails the test.
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (scheduler->activeHelperCount() != 0u &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(scheduler->activeHelperCount(), 0u);
}

// -----------------------------------------------------------------------------
// Test 11: Clean Shutdown Under High Foreground Load With Pending Morsels
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, CleanShutdownUnderHighForegroundLoad) {
  auto scheduler = ElasticExportScheduler::create(4, 64);

  // Simulate high foreground load (helpers ineligible)
  scheduler->onForegroundQueryStarted();
  scheduler->onForegroundQueryStarted();
  scheduler->onForegroundQueryStarted();

  // Create session and enqueue morsels
  auto session = scheduler->createSession<int>();
  for (int i = 0; i < 20; ++i) {
    session.submitMorsel([i]() -> int { return i * 2; });
  }

  // Shutdown scheduler while queue may contain pending items under high load
  // Must return promptly without deadlock or infinite spin loop
  scheduler->shutdown();
  EXPECT_EQ(scheduler->activeHelperCount(), 0u);
}

// -----------------------------------------------------------------------------
// Test 12: EnqueueMorsel admits or rejects synchronously with eligibility
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest,
     EnqueueMorselRejectsImmediatelyWhenIneligible) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  auto session = scheduler->createSession<int>();
  auto state = session.stateHandle();
  auto makeMorsel = [&]() {
    return OwnedMorsel(state, state->jobId(), scheduler->demandEpoch(), 0);
  };

  // Eligible while no foreground query is running: admitted synchronously.
  EXPECT_TRUE(scheduler->enqueueMorsel(makeMorsel()));

  // Two active queries exceed the admission threshold: rejected without
  // blocking, so the coordinator runs such morsels on the primary path.
  scheduler->onForegroundQueryStarted();
  scheduler->onForegroundQueryStarted();
  EXPECT_FALSE(scheduler->enqueueMorsel(makeMorsel()));

  scheduler->onForegroundQueryEnded();
  scheduler->onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 13: Blocked enqueuer wakes when the admission threshold flips
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, BlockedEnqueuerWakesWhenEligibilityFlips) {
  auto scheduler = ElasticExportScheduler::create(2, 1);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  auto session = scheduler->createSession<int>();
  std::promise<void> unblockPromise;
  auto unblockFuture = unblockPromise.get_future().share();
  auto blockingTask = [unblockFuture]() -> int {
    unblockFuture.wait();
    return 1;
  };
  session.submitMorsel(blockingTask);
  session.submitMorsel(blockingTask);

  // Wait until both workers picked up the blocking morsels (the queue is
  // empty again and both leases are held).
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (scheduler->activeHelperCount() != 2u &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(scheduler->activeHelperCount(), 2u);

  // Fill the single queue slot; the next enqueue must block.
  session.submitMorsel([]() -> int { return 2; });

  auto state = session.stateHandle();
  std::atomic<bool> enqueueReturned{false};
  std::atomic<bool> enqueueResult{true};
  std::thread blockedEnqueuer([&]() {
    bool admitted = scheduler->enqueueMorsel(
        OwnedMorsel(state, state->jobId(), scheduler->demandEpoch(), 99));
    enqueueResult.store(admitted);
    enqueueReturned.store(true);
  });

  // Flip to ineligible while the enqueuer is blocked: the threshold setter
  // must wake it so it re-checks eligibility instead of waiting on a stale
  // full queue.
  scheduler->onForegroundQueryStarted();
  scheduler->setMaxForegroundQueriesForHelperAdmission(0);
  blockedEnqueuer.join();
  ASSERT_TRUE(enqueueReturned.load());
  EXPECT_FALSE(enqueueResult.load());

  // Cleanup: release the workers and drain the three submitted morsels. The
  // rejected morsel was never queued, so exactly three results arrive.
  unblockPromise.set_value();
  auto results = session.drainRemainingResults();
  ASSERT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 1);
  EXPECT_EQ(results[2], 2);

  scheduler->onForegroundQueryEnded();
}
