// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
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
#include "util/GTestHelpers.h"
#include "util/http/websocket/QueryId.h"
#include "util/jthread.h"

using namespace ad_utility::export_v2;
using namespace std::chrono_literals;

// _____________________________________________________________________________
// Basic Execution & In-Order Consumption.

TEST(ElasticExportSchedulerTest, BasicExecutionAndInOrderConsumption) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  // The export query itself is the only foreground query.
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

// _____________________________________________________________________________
// Primary-Only Fallback Under More Than One Foreground Query.

TEST(ElasticExportSchedulerTest, PrimaryOnlyFallbackUnderHighForegroundLoad) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  // The export query plus one concurrent query.
  scheduler->onForegroundQueryStarted();
  scheduler->onForegroundQueryStarted();
  EXPECT_EQ(scheduler->activeForegroundQueries(), 2u);

  auto session = scheduler->createSession<int>();
  // Two foreground queries exceed the admission threshold of one.
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

// _____________________________________________________________________________
// Dynamic Scale-Out When Foreground Query Completes.

TEST(ElasticExportSchedulerTest, DynamicScaleOutWhenForegroundLoadDecreases) {
  auto scheduler = ElasticExportScheduler::create(4, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

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

  // The concurrent query finishes, so helpers become eligible again.
  scheduler->onForegroundQueryEnded();
  EXPECT_EQ(scheduler->activeForegroundQueries(), 1u);
  EXPECT_EQ(session.state(), SessionState::HelpersEligible);

  auto results = session.drainRemainingResults();
  EXPECT_EQ(results.size(), 6u);
  for (size_t i = 0; i < 6; ++i) {
    EXPECT_EQ(results[i], "dynamic_" + std::to_string(i));
  }

  scheduler->onForegroundQueryEnded();
}

// _____________________________________________________________________________
// Cooperative Revocation Under Foreground Pressure.

TEST(ElasticExportSchedulerTest, CooperativeRevocationUnderForegroundPressure) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  scheduler->onForegroundQueryStarted();  // Query count = 1 (eligible)
  auto session = scheduler->createSession<int>();
  EXPECT_EQ(session.state(), SessionState::HelpersEligible);

  std::promise<void> morsel0StartedPromise;
  std::future<void> morsel0Started = morsel0StartedPromise.get_future();
  std::promise<void> unblockMorsel0Promise;
  std::future<void> unblockMorsel0 = unblockMorsel0Promise.get_future();
  // Never leave the helper blocked if the test exits early, otherwise the
  // scheduler destructor would wait for it forever.
  bool morsel0Unblocked = false;
  absl::Cleanup unblockOnExit = [&] {
    if (!morsel0Unblocked) {
      unblockMorsel0Promise.set_value();
    }
  };

  // Morsel 0 pauses while holding the helper lease.
  session.submitMorsel(
      [morsel0StartedPromise = std::move(morsel0StartedPromise),
       unblockMorsel0 = std::move(unblockMorsel0)]() mutable {
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

  scheduler->onForegroundQueryStarted();
  EXPECT_EQ(scheduler->activeForegroundQueries(), 2u);

  // Helper 0 still holds its lease, so the session waits in `Revoking`.
  EXPECT_EQ(session.state(), SessionState::Revoking);

  // Submit morsel 1 while helpers are revoked, so it stays pending and runs
  // on the coordinator thread.
  session.submitMorsel([]() { return 200; });

  unblockMorsel0Promise.set_value();
  morsel0Unblocked = true;

  // Morsel 0 was executed by the helper.
  int r0 = session.consumeNextResult();
  EXPECT_EQ(r0, 100);

  // After helper 0 released its lease, morsel 1 runs on the coordinator.
  int r1 = session.consumeNextResult();
  EXPECT_EQ(r1, 200);

  auto profiles = session.inspectMorselProfiles();
  EXPECT_TRUE(profiles[0].executedByHelper_);
  EXPECT_FALSE(profiles[1].executedByHelper_);

  scheduler->onForegroundQueryEnded();
  scheduler->onForegroundQueryEnded();
}

// _____________________________________________________________________________
// Deterministic Slot Ordering Under Out-Of-Order Helper Completion.

TEST(ElasticExportSchedulerTest, DeterministicSlotOrderingWithVaryingDelays) {
  auto scheduler = ElasticExportScheduler::create(4, 128);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);
  scheduler->onForegroundQueryStarted();

  auto session = scheduler->createSession<size_t>();

  constexpr size_t count = 20;
  for (size_t i = 0; i < count; ++i) {
    session.submitMorsel([i]() {
      // Invert the delays: slot 0 sleeps longest (40 ms), slot 19 shortest
      // (2 ms).
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

// _____________________________________________________________________________
// Cancellation Cleanup.

TEST(ElasticExportSchedulerTest, CancellationStopsAdmissionAndCleansUp) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);
  scheduler->onForegroundQueryStarted();

  auto session = scheduler->createSession<int>();

  std::promise<void> startedPromise;
  auto startedFuture = startedPromise.get_future();
  std::promise<void> unblockPromise;
  auto unblockFuture = unblockPromise.get_future();
  // Never leave the helper blocked if the test exits early.
  bool unblocked = false;
  absl::Cleanup unblockOnExit = [&] {
    if (!unblocked) {
      unblockPromise.set_value();
    }
  };

  session.submitMorsel([startedPromise = std::move(startedPromise),
                        unblockFuture = std::move(unblockFuture)]() mutable {
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
  unblocked = true;

  EXPECT_THROW(session.consumeNextResult(), ad_utility::Exception);

  scheduler->onForegroundQueryEnded();
}

// _____________________________________________________________________________
// Move Semantics & RAII.

TEST(ElasticExportSchedulerTest, MoveSemanticsAndRAII) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->onForegroundQueryStarted();

  auto session = scheduler->createSession<std::string>();
  session.submitMorsel([]() { return "moved"; });

  auto movedSession = std::move(session);
  EXPECT_EQ(movedSession.consumeNextResult(), "moved");

  // Move assignment closes the session it replaces.
  auto replacement = scheduler->createSession<std::string>();
  auto replacedState = movedSession.stateHandle();
  movedSession = std::move(replacement);
  EXPECT_EQ(replacedState->state(), SessionState::Closed);
  EXPECT_NE(movedSession.state(), SessionState::Closed);

  scheduler->onForegroundQueryEnded();
}

// _____________________________________________________________________________
// QueryRegistry Lifecycle Hook Integration.

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

// _____________________________________________________________________________
// Registry Callbacks Expire Safely With The Scheduler.

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

// _____________________________________________________________________________
// Enqueue Refuses Work When Helpers Are Ineligible.

TEST(ElasticExportSchedulerTest, EnqueueRefusesWorkWhenHelpersIneligible) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);
  auto session = scheduler->createSession<std::string>();
  auto state = session.stateHandle();

  auto makeMorsel = [&](size_t index) {
    return OwnedMorsel(state, session.jobId(), scheduler->demandEpoch(), index);
  };
  // Eligible with room: enqueued. (A worker may pop it concurrently; an
  // index without a submitted slot is skipped safely.)
  EXPECT_TRUE(scheduler->enqueueMorsel(makeMorsel(0)));

  // Two foreground queries with max one: helpers ineligible, so enqueue
  // must refuse promptly instead of blocking forever on a queue that
  // workers refuse to drain.
  scheduler->onForegroundQueryStarted();
  scheduler->onForegroundQueryStarted();
  EXPECT_FALSE(scheduler->enqueueMorsel(makeMorsel(1)));

  // Eligibility restored: enqueue works again.
  scheduler->onForegroundQueryEnded();
  scheduler->onForegroundQueryEnded();
  EXPECT_TRUE(scheduler->enqueueMorsel(makeMorsel(2)));
}

// _____________________________________________________________________________
// A Session That Outlives Its Scheduler Falls Back To The Primary.

TEST(ElasticExportSchedulerTest, SessionOutlivesScheduler) {
  std::optional<ExportWorkSession<int>> session;
  {
    auto scheduler = ElasticExportScheduler::create(2, 64);
    scheduler->onForegroundQueryStarted();
    session.emplace(scheduler->createSession<int>());
    EXPECT_EQ(session->state(), SessionState::HelpersEligible);
  }
  // The scheduler is gone. Submitting must not touch it; the morsel stays
  // Pending and runs on the coordinator.
  session->submitMorsel([]() { return 7; });
  EXPECT_EQ(session->consumeNextResult(), 7);
  auto profiles = session->inspectMorselProfiles();
  ASSERT_EQ(profiles.size(), 1u);
  EXPECT_FALSE(profiles[0].executedByHelper_);
}

// _____________________________________________________________________________
// Concurrent Multi-Session Stress Test.

TEST(ElasticExportSchedulerTest, ConcurrentMultiSessionStressTest) {
  auto scheduler = ElasticExportScheduler::create(4, 256);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  std::atomic<bool> stopQueryChanger{false};
  // Fluctuate the foreground demand in the background.
  ad_utility::JThread queryChanger([&]() {
    while (!stopQueryChanger.load()) {
      scheduler->onForegroundQueryStarted();
      std::this_thread::sleep_for(1ms);
      scheduler->onForegroundQueryEnded();
      std::this_thread::sleep_for(1ms);
    }
  });
  // Declared after `queryChanger`, so it runs before that thread is joined,
  // also when the test exits early.
  absl::Cleanup stopOnExit = [&] { stopQueryChanger.store(true); };

  constexpr size_t numWorkerThreads = 4;
  constexpr size_t morselsPerSession = 15;
  std::vector<ad_utility::JThread> sessionRunners;
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

  // Destroying the `JThread`s joins the session runners first; then
  // `stopOnExit` stops the demand changer before it is joined.
}

// _____________________________________________________________________________
// Worker Exception Propagates to Coordinator Without Leaks.

TEST(ElasticExportSchedulerTest, WorkerExceptionPropagatesToCoordinator) {
  auto scheduler = ElasticExportScheduler::create(2, 64);
  auto session = scheduler->createSession<int>();

  session.submitMorsel([]() -> int { return 42; });
  session.submitMorsel([]() -> int {
    throw std::runtime_error("Simulated morsel processing failure");
  });
  session.submitMorsel([]() -> int { return 100; });

  EXPECT_EQ(session.consumeNextResult(), 42);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      session.consumeNextResult(),
      ::testing::StrEq("Simulated morsel processing failure"),
      std::runtime_error);
  // A failed morsel does not affect the later slots.
  EXPECT_EQ(session.consumeNextResult(), 100);

  // Verify lease accounting did not leak. Workers destroy their leases as
  // they finish loop iterations, which can lag behind the coordinator
  // consuming the final result, so wait briefly for quiescence instead of
  // asserting an instantaneous zero.
  const auto quiescenceDeadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (scheduler->activeHelperCount() != 0u &&
         std::chrono::steady_clock::now() < quiescenceDeadline) {
    std::this_thread::yield();
  }
  EXPECT_EQ(scheduler->activeHelperCount(), 0u);
}

// _____________________________________________________________________________
// Clean Shutdown Under High Foreground Load With Pending Morsels.

TEST(ElasticExportSchedulerTest, CleanShutdownUnderHighForegroundLoad) {
  auto scheduler = ElasticExportScheduler::create(4, 64);

  // High foreground load, so helpers are ineligible.
  scheduler->onForegroundQueryStarted();
  scheduler->onForegroundQueryStarted();
  scheduler->onForegroundQueryStarted();

  auto session = scheduler->createSession<int>();
  for (int i = 0; i < 20; ++i) {
    session.submitMorsel([i]() -> int { return i * 2; });
  }

  // Shutdown must return promptly without deadlock while morsels are
  // pending.
  scheduler->shutdown();
  EXPECT_EQ(scheduler->activeHelperCount(), 0u);

  // No helper ran anything, and every morsel is still delivered in order by
  // the coordinator after shutdown.
  auto results = session.drainRemainingResults();
  ASSERT_EQ(results.size(), 20u);
  for (int i = 0; i < 20; ++i) {
    EXPECT_EQ(results[i], i * 2);
  }
  for (const auto& profile : session.inspectMorselProfiles()) {
    EXPECT_FALSE(profile.executedByHelper_);
  }
}
// _____________________________________________________________________________
// Blocked enqueuer wakes when the admission threshold flips.

TEST(ElasticExportSchedulerTest, BlockedEnqueuerWakesWhenEligibilityFlips) {
  // Two helpers and a single queue slot: both helpers block on morsels while
  // one queued morsel fills the queue, so the next enqueue must block.
  auto scheduler = ElasticExportScheduler::create(2, 1);
  ASSERT_EQ(scheduler->queueCapacity(), 1u);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);

  auto session = scheduler->createSession<int>();
  std::promise<void> unblockPromise;
  // Share the future: both blocking morsels below wait on the same promise.
  auto unblockFuture = unblockPromise.get_future().share();
  auto blockingTask = [unblockFuture]() -> int {
    unblockFuture.wait();
    return 1;
  };
  // Never leave the helpers blocked if the test exits early, otherwise the
  // scheduler destructor would wait for them forever.
  bool helpersReleased = false;
  auto releaseHelpers = [&] {
    // Flag-first: `set_value` throws when the promise is already satisfied,
    // so setting the flag first keeps this idempotent and safe to call from
    // the cleanup guard below while unwinding.
    if (!helpersReleased) {
      helpersReleased = true;
      unblockPromise.set_value();
    }
  };
  absl::Cleanup releaseHelpersOnExit = [&] { releaseHelpers(); };
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
  // Orphan slot index on purpose: no morsel was submitted for it, so the
  // enqueue below must be rejected once helpers become ineligible.
  constexpr size_t kOrphanMorselIndex = 99;
  std::atomic<bool> enqueueReturned{false};
  std::atomic<bool> enqueueResult{true};
  ad_utility::JThread blockedEnqueuer([&]() {
    bool admitted = scheduler->enqueueMorsel(OwnedMorsel(
        state, state->jobId(), scheduler->demandEpoch(), kOrphanMorselIndex));
    enqueueResult.store(admitted);
    enqueueReturned.store(true);
  });
  // If the wakeup regressed and the enqueuer is still blocked, shut the
  // scheduler down so the enqueuer returns and the join below cannot hang.
  // This guard destroys before the `JThread`, so it also covers early
  // `ASSERT` exits; on success it runs after the explicit join as a no-op.
  absl::Cleanup unblockEnqueuerOnExit = [&] {
    if (!enqueueReturned.load()) {
      releaseHelpers();
      scheduler->shutdown();
    }
  };

  // Let the enqueuer thread reach the wait on the full queue before flipping
  // eligibility: otherwise a lost notification would go unnoticed, because
  // `enqueueMorsel` re-checks eligibility after acquiring the mutex anyway.
  std::this_thread::sleep_for(50ms);
  ASSERT_FALSE(enqueueReturned.load());
  // Flip to ineligible while the enqueuer is blocked: the threshold setter
  // must wake it so it re-checks eligibility instead of waiting on a stale
  // full queue.
  scheduler->onForegroundQueryStarted();
  // One active foreground query against a zero threshold: helpers are
  // ineligible from here on.
  ASSERT_EQ(scheduler->activeForegroundQueries(), 1u);
  absl::Cleanup rollbackForegroundQuery = [&] {
    scheduler->onForegroundQueryEnded();
  };
  scheduler->setMaxForegroundQueriesForHelperAdmission(0);
  // Fail fast on a wakeup regression instead of hanging in join() until the
  // global ctest timeout.
  auto enqueueDeadline = std::chrono::steady_clock::now() + 5s;
  while (!enqueueReturned.load() &&
         std::chrono::steady_clock::now() < enqueueDeadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(enqueueReturned.load());
  blockedEnqueuer.join();
  EXPECT_FALSE(enqueueResult.load());

  // Cleanup: release the workers and drain the three submitted morsels. The
  // rejected morsel was never queued, so exactly three results arrive.
  releaseHelpers();
  auto results = session.drainRemainingResults();
  ASSERT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 1);
  EXPECT_EQ(results[2], 2);
  // The `rollbackForegroundQuery` guard above balances the started query.
}

// _____________________________________________________________________________
// Idle workers wake when a threshold change makes helpers eligible again.
TEST(ElasticExportSchedulerTest, IdleWorkerWakesWhenThresholdRises) {
  // One helper isolates the busy-to-idle transition; eight slots hold the
  // second morsel without blocking, unlike the capacity-1 test above.
  auto scheduler = ElasticExportScheduler::create(1, 8);
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);
  scheduler->onForegroundQueryStarted();
  // Balance the started query on every exit path: an early `ASSERT` return
  // must not leak foreground-query state.
  absl::Cleanup rollbackForegroundQuery = [&] {
    scheduler->onForegroundQueryEnded();
  };
  auto session = scheduler->createSession<int>();
  ASSERT_EQ(session.state(), SessionState::HelpersEligible);

  std::promise<void> unblockPromise;
  auto unblockFuture = unblockPromise.get_future();
  // Never leave the helper blocked on an early exit: the scheduler
  // destructor would wait for it forever.
  bool unblocked = false;
  absl::Cleanup unblockOnExit = [&] {
    // Flag-first, as in the test above: `set_value` on a satisfied promise
    // throws, so the flag must be set before satisfying it.
    if (!unblocked) {
      unblocked = true;
      unblockPromise.set_value();
    }
  };
  session.submitMorsel([unblockFuture = std::move(unblockFuture)]() mutable {
    unblockFuture.wait();
    return 1;
  });
  // Poll until the condition holds or the timeout expires: there is no
  // notification primitive to wait on, 1ms avoids busy-spinning, and 5s is a
  // generous fail-fast bound instead of the global ctest timeout.
  auto waitFor = [](const auto& condition) {
    auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!condition() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(1ms);
    }
    return condition();
  };
  ASSERT_TRUE(waitFor([&] { return scheduler->activeHelperCount() == 1u; }));

  // The only worker is busy, so the second morsel stays in the queue.
  session.submitMorsel([]() { return 2; });

  // Make helpers ineligible, then let the worker finish its morsel: it goes
  // idle although the queue still holds the second morsel.
  uint64_t epochBeforeFlips = scheduler->demandEpoch();
  scheduler->setMaxForegroundQueriesForHelperAdmission(0);
  unblocked = true;
  unblockPromise.set_value();
  ASSERT_TRUE(waitFor([&] { return scheduler->activeHelperCount() == 0u; }));

  // The second morsel is still queued and untouched while the worker is
  // idle: this is the state the threshold raise below must wake. Index 1 is
  // the second submitted morsel (index 0 is the blocking one returning 1).
  ASSERT_GE(session.inspectMorselProfiles().size(), 2u);
  EXPECT_EQ(session.inspectMorselProfiles()[1].finalStatus_,
            MorselStatus::Pending);

  // Raising the threshold must wake the idle worker, which then runs the
  // queued morsel before the coordinator asks for it.
  scheduler->setMaxForegroundQueriesForHelperAdmission(1);
  // The setter only notifies: it must not advance the demand epoch, or the
  // queued morsel would go stale and never complete.
  ASSERT_EQ(scheduler->demandEpoch(), epochBeforeFlips);
  ASSERT_TRUE(waitFor([&] {
    return session.inspectMorselProfiles()[1].finalStatus_ ==
           MorselStatus::Completed;
  }));
  EXPECT_TRUE(session.inspectMorselProfiles()[1].executedByHelper_);
  EXPECT_EQ(session.consumeNextResult(), 1);
  EXPECT_EQ(session.consumeNextResult(), 2);
  // The `rollbackForegroundQuery` guard above balances the started query.
}
