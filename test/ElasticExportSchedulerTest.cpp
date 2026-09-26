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
