// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio/post.hpp>
#include <boost/asio/static_thread_pool.hpp>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "engine/export_v2/ElasticExportScheduler.h"
#include "engine/export_v2/ExportJobState.h"
#include "util/GTestHelpers.h"
#include "util/http/websocket/QueryId.h"

using namespace ad_utility::export_v2;
using namespace std::chrono_literals;

// -----------------------------------------------------------------------------
// Test 1: Basic Execution & In-Order Consumption
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, BasicExecutionAndInOrderConsumption) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);

  // Set active foreground queries to 1 (only this export query running)
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

  // The concurrent query finishes; active queries drop to 1
  scheduler.onForegroundQueryEnded();
  EXPECT_EQ(scheduler.activeForegroundQueries(), 1u);
  EXPECT_EQ(session.state(), SessionState::HelpersEligible);

  // Consume all results
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
  // Exactly one helper: it blocks inside morsel 0 while holding the lease,
  // so morsel 1 stays queued until revocation hands it to the coordinator.
  // With two helpers the idle worker would legitimately execute morsel 1
  // before the foreground query arrives, and `profiles[1].executedByHelper_`
  // would be `true`.
  ElasticExportScheduler scheduler(1, 64);
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

  // Wait until morsel 0 is running on a helper. Morsel 1 is submitted only
  // after revocation below: submitting it earlier would let the second
  // helper pick it up before the revocation arrives, which makes
  // `executedByHelper_` for morsel 1 nondeterministic (cooperative
  // revocation only stops not-yet-started helper work).
  morsel0Started.wait();

  // A new foreground query starts! (count = 2)
  scheduler.onForegroundQueryStarted();
  EXPECT_EQ(scheduler.activeForegroundQueries(), 2u);

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

  // Cancel session
  session.cancel();
  EXPECT_EQ(session.state(), SessionState::Closed);

  // Unblock helper morsel
  unblockPromise.set_value();

  // Attempting to consume from cancelled session should throw
  EXPECT_THROW(session.consumeNextResult(), ad_utility::Exception);

  scheduler.onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 6b: Throwing Morsel Surfaces Instead Of Hanging The Consumer
// -----------------------------------------------------------------------------
// A task exception must reach `consumeNextResult` as that same exception
// (via the terminal `Cancelled` slot state), whether the morsel ran on a
// helper worker or was stolen by the primary fallback path. The profile must
// also reach a terminal status instead of dangling in `Running`.
TEST(ElasticExportSchedulerTest, ThrowingMorselPropagatesToConsumer) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);

  auto session = scheduler.createSession<std::string>();
  session.submitMorsel(
      []() -> std::string { throw std::runtime_error{"morsel failed"}; });

  EXPECT_THROW(
      {
        try {
          session.consumeNextResult();
        } catch (const std::runtime_error& e) {
          EXPECT_STREQ(e.what(), "morsel failed");
          throw;
        }
      },
      std::runtime_error);

  auto profiles = session.inspectMorselProfiles();
  ASSERT_EQ(profiles.size(), 1u);
  EXPECT_EQ(profiles[0].finalStatus_, MorselStatus::Cancelled);
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
  // Background thread fluctuating foreground demand
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
// Test 10: Unordered Emission Consumes Every Morsel Exactly Once
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, UnorderedEmissionConsumesEveryMorselOnce) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);
  scheduler.onForegroundQueryStarted();

  auto session = scheduler.createSession<std::string>();
  session.setOrdered(false);

  constexpr size_t numMorsels = 20;
  for (size_t i = 0; i < numMorsels; ++i) {
    session.submitMorsel([i]() {
      // Later morsels finish first: invert completion order so slot order
      // and completion order disagree.
      std::this_thread::sleep_for(
          std::chrono::milliseconds(2 * (numMorsels - i)));
      return "result_" + std::to_string(i);
    });
  }

  std::set<std::string> seen;
  while (session.hasMoreResults()) {
    seen.insert(session.consumeNextResult());
  }
  EXPECT_EQ(seen.size(), numMorsels);
  for (size_t i = 0; i < numMorsels; ++i) {
    EXPECT_TRUE(seen.contains("result_" + std::to_string(i)));
  }
  EXPECT_EQ(session.consumedSlots(), numMorsels);
  EXPECT_FALSE(session.hasMoreResults());

  scheduler.onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 15: Unordered Emission Honors Completion Order
// -----------------------------------------------------------------------------
TEST(ElasticExportSchedulerTest, UnorderedEmissionHonorsCompletionOrder) {
  ElasticExportScheduler scheduler(2, 64);
  auto session = scheduler.createSession<int>();
  session.setOrdered(false);

  // Slot 0 finishes last (200ms); slot 1 finishes (near-)immediately. Both
  // run on the helpers, so after the wait below both are Completed and
  // slot 1's `completedAt_` timestamp is strictly earlier.
  session.submitMorsel([]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    return 0;
  });
  session.submitMorsel([]() { return 1; });
  std::this_thread::sleep_for(std::chrono::milliseconds(600));

  // The morsel that completed first must be emitted first. Slot-index
  // order would emit slot 0 here.
  EXPECT_EQ(session.consumeNextResult(), 1);
  EXPECT_EQ(session.consumeNextResult(), 0);
  EXPECT_FALSE(session.hasMoreResults());
}

// -----------------------------------------------------------------------------
// Test 11: TrySubmitMorsel Reports Instead Of Firing
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, TrySubmitMorselReportsInsteadOfFiring) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.onForegroundQueryStarted();

  auto session = scheduler.createSession<std::string>();
  EXPECT_TRUE(session.trySubmitMorsel([]() { return std::string{"a"}; }));
  EXPECT_EQ(session.totalSlots(), 1u);

  session.sharedState()->cancel();
  EXPECT_FALSE(session.trySubmitMorsel([]() { return std::string{"b"}; }));
  EXPECT_EQ(session.totalSlots(), 1u);

  scheduler.onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 12: Abandoned Remainder Runs Exactly Once
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, AbandonedRemainderRunsExactlyOnce) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);
  scheduler.onForegroundQueryStarted();

  auto session = scheduler.createSession<std::string>();
  session.setOrdered(false);
  auto state = session.sharedState();

  // Self-abandoning morsel: returns a partial result and resubmits its
  // tail via trySubmitMorsel, mirroring CheckpointMorselRunner on an
  // epoch change. May run on a helper or the coordinator thread.
  session.submitMorsel([state]() {
    EXPECT_TRUE(state->trySubmitMorsel([]() { return std::string{"tail"}; }));
    return std::string{"partial"};
  });
  session.submitMorsel([]() { return std::string{"other"}; });

  // A new foreground query revokes helper eligibility mid-flight.
  scheduler.onForegroundQueryStarted();

  std::set<std::string> seen;
  while (session.hasMoreResults()) {
    seen.insert(session.consumeNextResult());
  }
  EXPECT_EQ(seen.size(), 3u);
  EXPECT_TRUE(seen.contains("partial"));
  EXPECT_TRUE(seen.contains("tail"));
  EXPECT_TRUE(seen.contains("other"));
  EXPECT_EQ(session.consumedSlots(), 3u);
  EXPECT_FALSE(session.hasMoreResults());

  scheduler.onForegroundQueryEnded();
  scheduler.onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 13: Worker Exception Propagates to Coordinator Without Leaks
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, WorkerExceptionPropagatesToCoordinator) {
  ElasticExportScheduler scheduler(2, 64);
  // Helpers stay ineligible so every morsel runs on the coordinator:
  // a throw on a helper thread cannot cross threads (it would terminate),
  // so letting helpers race for the throwing slot would make this test
  // nondeterministic. Coordinator execution is deterministic.
  scheduler.onForegroundQueryStarted();
  scheduler.onForegroundQueryStarted();
  EXPECT_EQ(scheduler.activeForegroundQueries(), 2u);
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

  // Slot 1 should throw std::runtime_error with the task's message.
  AD_EXPECT_THROW_WITH_MESSAGE(
      session.consumeNextResult(),
      ::testing::HasSubstr("Simulated morsel processing failure"));

  // Slot 2 should still return 100
  EXPECT_EQ(session.consumeNextResult(), 100);

  // Verify lease accounting did not leak
  EXPECT_EQ(scheduler.activeHelperCount(), 0u);
}

// -----------------------------------------------------------------------------
// Test 14: Clean Shutdown Under High Foreground Load With Pending Morsels
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
  EXPECT_EQ(scheduler.activeHelperCount(), 0u);
}

// -----------------------------------------------------------------------------
// Even-split admission tests (poster transport with a deferred test poster)
// -----------------------------------------------------------------------------

namespace {
// Collects posted closures without running them; the test drives execution,
// which makes share and ordering assertions deterministic.
struct DeferredPoster {
  std::vector<absl::AnyInvocable<void()>> posted_;
  size_t totalPosted_{0};

  void post(absl::AnyInvocable<void()> work) {
    posted_.push_back(std::move(work));
    ++totalPosted_;
  }

  void runToIdle() {
    while (!posted_.empty()) {
      auto batch = std::move(posted_);
      posted_.clear();
      for (auto& work : batch) {
        std::move(work)();
      }
    }
  }
};
}  // namespace

TEST(ElasticExportSchedulerTest, EvenSplitAcrossSessions) {
  DeferredPoster deferred;
  size_t postedCount = 0;
  ElasticExportScheduler scheduler(
      [&deferred, &postedCount](absl::AnyInvocable<void()> work) {
        ++postedCount;
        deferred.post(std::move(work));
      },
      64);
  scheduler.setMaxConcurrentMorsels(4);
  scheduler.onForegroundQueryStarted();

  auto sessionA = scheduler.createSession<std::string>();
  auto sessionB = scheduler.createSession<std::string>();
  for (size_t i = 0; i < 4; ++i) {
    sessionA.submitMorsel([i]() { return "a_" + std::to_string(i); });
    sessionB.submitMorsel([i]() { return "b_" + std::to_string(i); });
  }
  // Two live sessions share four slots evenly: share = max/live = 4/2 = 2
  // per session, so two from A plus two from B post and four stay pending.
  EXPECT_EQ(postedCount, 4u);

  deferred.runToIdle();
  EXPECT_EQ(postedCount, 8u);
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(sessionA.consumeNextResult(), "a_" + std::to_string(i));
    EXPECT_EQ(sessionB.consumeNextResult(), "b_" + std::to_string(i));
  }
  EXPECT_FALSE(sessionA.hasMoreResults());
  EXPECT_FALSE(sessionB.hasMoreResults());
}

TEST(ElasticExportSchedulerTest, FloorGuaranteeUnderOversubscription) {
  DeferredPoster deferred;
  ElasticExportScheduler scheduler(
      [&deferred](absl::AnyInvocable<void()> work) {
        deferred.post(std::move(work));
      },
      64);
  scheduler.setMaxConcurrentMorsels(2);
  scheduler.onForegroundQueryStarted();

  auto sessionA = scheduler.createSession<std::string>();
  auto sessionB = scheduler.createSession<std::string>();
  auto sessionC = scheduler.createSession<std::string>();
  for (size_t i = 0; i < 2; ++i) {
    sessionA.submitMorsel([i]() { return "a_" + std::to_string(i); });
    sessionB.submitMorsel([i]() { return "b_" + std::to_string(i); });
    sessionC.submitMorsel([i]() { return "c_" + std::to_string(i); });
  }
  // Three sessions over two slots: the floor admits one morsel for the first
  // two sessions, the third waits even though its own count is zero.
  EXPECT_EQ(deferred.totalPosted_, 2u);

  deferred.runToIdle();
  for (size_t i = 0; i < 2; ++i) {
    EXPECT_EQ(sessionA.consumeNextResult(), "a_" + std::to_string(i));
    EXPECT_EQ(sessionB.consumeNextResult(), "b_" + std::to_string(i));
    EXPECT_EQ(sessionC.consumeNextResult(), "c_" + std::to_string(i));
  }
  EXPECT_EQ(deferred.totalPosted_, 6u);
}

TEST(ElasticExportSchedulerTest, CancelledSessionYieldsItsShare) {
  DeferredPoster deferred;
  ElasticExportScheduler scheduler(
      [&deferred](absl::AnyInvocable<void()> work) {
        deferred.post(std::move(work));
      },
      64);
  scheduler.setMaxConcurrentMorsels(2);
  scheduler.onForegroundQueryStarted();

  auto sessionA = scheduler.createSession<std::string>();
  auto sessionB = scheduler.createSession<std::string>();
  for (size_t i = 0; i < 3; ++i) {
    sessionA.submitMorsel([i]() { return "a_" + std::to_string(i); });
    sessionB.submitMorsel([i]() { return "b_" + std::to_string(i); });
  }
  // Share one each with interleaved submission: A and B post one morsel
  // each, the rest waits.
  EXPECT_EQ(deferred.totalPosted_, 2u);

  sessionA.cancel();
  deferred.runToIdle();
  // B drains fully; A's cancelled pending morsels are purged, never posted:
  // only B's two admitted morsels post on top of the initial two.
  EXPECT_EQ(deferred.totalPosted_, 4u);
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(sessionB.consumeNextResult(), "b_" + std::to_string(i));
  }
  EXPECT_FALSE(sessionB.hasMoreResults());
  // None of A's slots completed after the cancel, so nothing leaked to A.
  // (A's slots stay unconsumed by design, so `hasMoreResults` is not
  // asserted here.)
  EXPECT_EQ(sessionA.consumedSlots(), 0u);
}

TEST(ElasticExportSchedulerTest, SynchronousPosterDoesNotDeadlock) {
  // A poster that runs work inline must not deadlock: posting happens
  // without holding the queue mutex, so completion accounting can take the
  // non-recursive mutex again on the same thread.
  // Heap-owned: the worker holds a copy of the `shared_ptr`, so the
  // scheduler outlives a detached worker after a timeout instead of being
  // destroyed from under it.
  auto scheduler = std::make_shared<ElasticExportScheduler>(
      [](absl::AnyInvocable<void()> work) { std::move(work)(); }, 64);
  scheduler->setMaxConcurrentMorsels(2);

  // A posting-under-lock regression deadlocks instead of failing, so run
  // the scenario off-thread with a bounded wait. The worker only touches
  // the scheduler while this scope is alive: on success the join below
  // proves its session is destroyed before the scheduler is. On timeout
  // the worker is detached so a regression fails the test instead of
  // hanging the test binary.
  std::promise<std::vector<int>> done;
  auto finished = done.get_future();
  std::thread worker([scheduler, promise = std::move(done)]() mutable {
    try {
      auto session = scheduler->createSession<int>();
      for (int i = 0; i < 4; ++i) {
        session.submitMorsel([i]() { return i * 10; });
      }
      std::vector<int> results;
      for (int i = 0; i < 4; ++i) {
        results.push_back(session.consumeNextResult());
      }
      promise.set_value(std::move(results));
    } catch (...) {
      promise.set_exception(std::current_exception());
    }
  });
  if (finished.wait_for(10s) != std::future_status::ready) {
    // Detach so the regression fails instead of terminating (a joinable
    // thread must never be destroyed) or hanging the test binary. The
    // detached worker keeps the scheduler alive through its own copy.
    worker.detach();
    FAIL() << "Inline poster deadlocked: posting must not hold the queue mutex";
  }
  worker.join();
  EXPECT_EQ(finished.get(), (std::vector<int>{0, 10, 20, 30}));
  EXPECT_EQ(scheduler->activeHelperCount(), 0u);
}

TEST(ElasticExportSchedulerTest, RemainderSlotGoesToOldestSession) {
  DeferredPoster deferred;
  ElasticExportScheduler scheduler(
      [&deferred](absl::AnyInvocable<void()> work) {
        deferred.post(std::move(work));
      },
      64);
  // Three slots over two sessions: base share one each, remainder one.
  scheduler.setMaxConcurrentMorsels(3);
  scheduler.onForegroundQueryStarted();

  auto sessionA = scheduler.createSession<std::string>();
  auto sessionB = scheduler.createSession<std::string>();
  for (size_t i = 0; i < 3; ++i) {
    sessionA.submitMorsel([i]() { return "a_" + std::to_string(i); });
    sessionB.submitMorsel([i]() { return "b_" + std::to_string(i); });
  }
  // Base shares post A0 and B0; the truncated remainder slot goes to the
  // oldest session (A), so A1 posts promptly instead of waiting for the
  // next completion while capacity sits idle.
  EXPECT_EQ(deferred.totalPosted_, 3u);

  deferred.runToIdle();
  EXPECT_EQ(deferred.totalPosted_, 6u);
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(sessionA.consumeNextResult(), "a_" + std::to_string(i));
    EXPECT_EQ(sessionB.consumeNextResult(), "b_" + std::to_string(i));
  }
}

TEST(ElasticExportSchedulerTest, SetMaxConcurrentMorselsZeroThrows) {
  ElasticExportScheduler scheduler([](absl::AnyInvocable<void()>) {}, 64);
  EXPECT_THROW(scheduler.setMaxConcurrentMorsels(0), ad_utility::Exception);
}

// -----------------------------------------------------------------------------
// ExportJobState module: identity is derived, never passed
// -----------------------------------------------------------------------------

namespace {
// Minimal ExportJobStateBase with a fixed id for module-level tests.
struct FixedIdJobState : ExportJobStateBase {
  explicit FixedIdJobState(uint64_t jobId) : jobId_{jobId} {}
  [[nodiscard]] uint64_t jobId() const noexcept override { return jobId_; }
  void onDemandChanged(size_t, uint64_t) override {}
  void onHelperLeaseAcquired(uint64_t) override {}
  void onHelperLeaseReleased(uint64_t) override {}
  void executeHelperTask(size_t, uint64_t) override {}
  [[nodiscard]] bool isCancelled() const noexcept override { return false; }

 private:
  uint64_t jobId_;
};
}  // namespace

TEST(ElasticExportSchedulerTest, OwnedMorselDerivesJobIdFromState) {
  auto state = std::make_shared<FixedIdJobState>(42);
  OwnedMorsel morsel(state, 7, 3);
  EXPECT_EQ(morsel.jobId(), 42u);
  EXPECT_EQ(morsel.submissionEpoch_, 7u);
  EXPECT_EQ(morsel.morselIndex_, 3u);
  EXPECT_EQ(morsel.jobState_, state);
}

TEST(ElasticExportSchedulerTest, OwnedMorselNullStateThrows) {
  EXPECT_THROW(OwnedMorsel(nullptr, 0, 0), ad_utility::Exception);
}

// -----------------------------------------------------------------------------
// Server wiring validation.
// -----------------------------------------------------------------------------
// Mirror the wiring in `Server::Server` (a poster onto `queryThreadPool_`, a
// cap of the pool size on outstanding morsels, and the attachment to the query
// registry) against a real `static_thread_pool`. Check three invariants: the
// number of morsels in flight never exceeds the pool size, a second registered
// query stops helper admission, and once that query ends the pending morsels
// are posted again, still within the pool size.

namespace {
// Return true as soon as `condition` holds and false once `timeout` has
// passed; the caller asserts on the result. Yield instead of sleeping so the
// poll observes the blocked tasks as soon as they start.
bool pollUntil(const std::function<bool()>& condition,
               std::chrono::milliseconds timeout = 10s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (true) {
    if (condition()) {
      return true;
    }
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    std::this_thread::yield();
  }
}
}  // namespace

TEST(ElasticExportSchedulerTest, ProductionWiringBoundsPoolAndYieldsToLoad) {
  constexpr size_t poolSize = 4;
  boost::asio::static_thread_pool pool(poolSize);
  std::atomic<size_t> postedCount{0};
  ElasticExportScheduler scheduler(
      [&pool, &postedCount](absl::AnyInvocable<void()> work) {
        postedCount.fetch_add(1, std::memory_order_relaxed);
        // `boost::asio::post` requires a copyable handler, `AnyInvocable` is
        // move-only.
        const auto sharedWork =
            std::make_shared<absl::AnyInvocable<void()>>(std::move(work));
        boost::asio::post(pool, [sharedWork]() { (*sharedWork)(); });
      },
      64);
  scheduler.setMaxConcurrentMorsels(poolSize);
  ad_utility::websocket::QueryRegistry registry;
  scheduler.attachToQueryRegistry(registry);

  // The export query itself is the only registered query, which is within
  // the default limit of one foreground query for helper admission.
  const auto exportQuery = registry.uniqueId("SELECT ?x WHERE { ?x ?p ?o }");
  EXPECT_EQ(scheduler.activeForegroundQueries(), 1u);

  auto sessionA = scheduler.createSession<std::string>();
  auto sessionB = scheduler.createSession<std::string>();
  // Unordered consumption runs the oldest pending morsel inline, which the
  // consume loop at the end relies on.
  sessionA.setOrdered(false);
  sessionB.setOrdered(false);
  EXPECT_EQ(sessionA.state(), SessionState::HelpersEligible);

  // Block every morsel that reaches a pool thread until `gateOpen` is set, so
  // the pool saturates deterministically: `poolSize` morsels occupy the pool
  // threads, the rest stay pending in the scheduler.
  std::atomic<size_t> blockedTasksStarted{0};
  std::atomic<size_t> inFlight{0};
  std::atomic<size_t> maxInFlight{0};
  std::atomic<bool> gateOpen{false};
  // Open the gate and drain the pool on every exit, so a failed assertion
  // cannot leave pool threads spinning on the gate.
  absl::Cleanup openGateOnExit{[&gateOpen, &pool]() {
    gateOpen.store(true);
    pool.wait();
  }};
  const auto blockingTask = [&]() -> std::string {
    const size_t inFlightNow = inFlight.fetch_add(1) + 1;
    size_t observedMax = maxInFlight.load();
    while (inFlightNow > observedMax &&
           !maxInFlight.compare_exchange_weak(observedMax, inFlightNow)) {
    }
    blockedTasksStarted.fetch_add(1);
    // Spin rather than block: the gate is opened once, at the end of the
    // test, and the spinning tasks are what keeps the pool saturated.
    while (!gateOpen.load()) {
      std::this_thread::yield();
    }
    inFlight.fetch_sub(1);
    return "blocked";
  };
  for (size_t i = 0; i < poolSize; ++i) {
    sessionA.submitMorsel(blockingTask);
    sessionB.submitMorsel(blockingTask);
  }
  // Two sessions share four slots: four morsels are posted synchronously and
  // four stay pending.
  EXPECT_EQ(postedCount.load(), poolSize);
  ASSERT_TRUE(
      pollUntil([&] { return blockedTasksStarted.load() == poolSize; }));
  EXPECT_LE(maxInFlight.load(), poolSize);

  // A second registered query exceeds the limit of one foreground query, so
  // helper admission stops and new morsels run inline on the consuming thread
  // instead of taking pool threads. These morsels go to a fresh session: the
  // unordered consumption of `sessionA` would run one of its gate-blocked
  // pending morsels inline and deadlock, because the gate only opens after
  // this block.
  {
    const auto foregroundQuery =
        registry.uniqueId("SELECT ?y WHERE { ?y ?p ?o }");
    EXPECT_EQ(scheduler.activeForegroundQueries(), 2u);
    auto sessionC = scheduler.createSession<std::string>();
    EXPECT_EQ(sessionC.state(), SessionState::PrimaryOnly);
    sessionC.setOrdered(false);
    for (size_t i = 0; i < poolSize; ++i) {
      sessionC.submitMorsel([i]() { return absl::StrCat("inline_", i); });
    }
    EXPECT_EQ(postedCount.load(), poolSize);
    for (size_t i = 0; i < poolSize; ++i) {
      EXPECT_EQ(sessionC.consumeNextResult(), absl::StrCat("inline_", i));
    }
    EXPECT_FALSE(sessionC.hasMoreResults());
  }
  // The second query has ended, but all `poolSize` slots are still taken by
  // the blocked morsels, so the cap of outstanding morsels keeps the pending
  // ones from being posted.
  EXPECT_EQ(scheduler.activeForegroundQueries(), 1u);
  EXPECT_EQ(postedCount.load(), poolSize);

  gateOpen.store(true);
  // Wait until the pool has no work left rather than for thread exit: the
  // completion of each blocked morsel posts one pending morsel, and `wait()`
  // returns only after those have run as well.
  pool.wait();

  // The pending morsels were posted to the pool once slots became free,
  // without exceeding the pool size.
  EXPECT_EQ(postedCount.load(), 2 * poolSize);
  for (size_t i = 0; i < poolSize; ++i) {
    EXPECT_EQ(sessionA.consumeNextResult(), "blocked");
    EXPECT_EQ(sessionB.consumeNextResult(), "blocked");
  }
  EXPECT_FALSE(sessionA.hasMoreResults());
  EXPECT_FALSE(sessionB.hasMoreResults());
  EXPECT_LE(maxInFlight.load(), poolSize);
}
