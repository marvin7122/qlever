// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "engine/export_v2/ElasticExportScheduler.h"
#include "util/http/websocket/QueryId.h"
#include "util/jthread.h"

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

  // Submit morsel 1
  session.submitMorsel([]() { return 200; });

  // Wait until morsel 0 is running on a helper
  morsel0Started.wait();

  // A new foreground query starts! (count = 2)
  scheduler.onForegroundQueryStarted();
  EXPECT_EQ(scheduler.activeForegroundQueries(), 2u);

  // Session must transition to Revoking because helper 0 is actively leased
  EXPECT_EQ(session.state(), SessionState::Revoking);

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

// A morsel posted onto an external pool (`WorkPoster`, the live
// `queryThreadPool_` path) that throws must not let the exception escape the
// posted task, which would terminate the pool thread. The failure must still
// reach the consumer.
TEST(ElasticExportSchedulerTest, ThrowingPostedMorselDoesNotEscapePoolThread) {
  std::vector<absl::AnyInvocable<void()>> posted;
  ElasticExportScheduler scheduler(
      [&posted](absl::AnyInvocable<void()> task) {
        posted.push_back(std::move(task));
      },
      2, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);

  auto session = scheduler.createSession<std::string>();
  session.submitMorsel(
      []() -> std::string { throw std::runtime_error{"posted failed"}; });
  ASSERT_EQ(posted.size(), 1u);
  EXPECT_NO_THROW(posted[0]());
  EXPECT_EQ(scheduler.activeHelperCount(), 0u);

  EXPECT_THROW(
      {
        try {
          session.consumeNextResult();
        } catch (const std::runtime_error& e) {
          EXPECT_STREQ(e.what(), "posted failed");
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
  // `JThread` and the cleanup join all threads even if an assertion ends the
  // test early; destroying a joinable `std::thread` would terminate.
  ad_utility::JThread queryChanger([&]() {
    while (!stopQueryChanger.load()) {
      scheduler.onForegroundQueryStarted();
      std::this_thread::sleep_for(1ms);
      scheduler.onForegroundQueryEnded();
      std::this_thread::sleep_for(1ms);
    }
  });
  absl::Cleanup stopChanger = [&stopQueryChanger]() {
    stopQueryChanger.store(true);
  };

  constexpr size_t numWorkerThreads = 4;
  constexpr size_t morselsPerSession = 15;
  std::vector<ad_utility::JThread> sessionRunners;
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
// Test 10b: Unordered Consume Re-Selects When Another Running Morsel Completes
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest,
     UnorderedConsumeReselectsWhenAnotherRunningMorselCompletes) {
  ElasticExportScheduler scheduler(2, 64);
  scheduler.setMaxForegroundQueriesForHelperAdmission(1);
  scheduler.onForegroundQueryStarted();

  auto session = scheduler.createSession<std::string>();
  session.setOrdered(false);

  auto started = std::make_shared<std::atomic<int>>(0);
  auto release = std::make_shared<std::promise<void>>();
  std::shared_future<void> released = release->get_future().share();
  // Slot 0 keeps running until the consumer has emitted slot 1. The timeout
  // turns a head-of-line-blocking regression into a failure, not a hang.
  session.submitMorsel([started, released]() {
    ++*started;
    (void)released.wait_for(5s);
    return std::string{"slow"};
  });
  session.submitMorsel([started]() {
    ++*started;
    std::this_thread::sleep_for(20ms);
    return std::string{"fast"};
  });
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (started->load() < 2 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(started->load(), 2) << "both morsels must run on helpers";

  // Both slots are `Running`; the consumer must not wait on slot 0 while
  // slot 1 completes.
  EXPECT_EQ(session.consumeNextResult(), "fast");
  release->set_value();
  EXPECT_EQ(session.consumeNextResult(), "slow");
  EXPECT_FALSE(session.hasMoreResults());

  scheduler.onForegroundQueryEnded();
}

// -----------------------------------------------------------------------------
// Test 10c: SetOrdered Rejects a Change After an Unordered Consume
// -----------------------------------------------------------------------------

TEST(ElasticExportSchedulerTest, SetOrderedRejectsChangeAfterUnorderedConsume) {
  // Regression test: `nextSlotToConsume_ == 0` alone does not catch this,
  // because unordered consumption never advances `nextSlotToConsume_`.
  ElasticExportScheduler scheduler(2, 64);
  scheduler.onForegroundQueryStarted();

  auto session = scheduler.createSession<std::string>();
  session.setOrdered(false);
  session.submitMorsel([]() { return std::string{"a"}; });
  session.submitMorsel([]() { return std::string{"b"}; });

  EXPECT_TRUE(session.hasMoreResults());
  session.consumeNextResult();
  EXPECT_EQ(session.consumedSlots(), 1u);

  // The invariant "fixed before first consume" must still be enforced once
  // any slot has been consumed, ordered or not.
  EXPECT_THROW(session.setOrdered(true), ad_utility::Exception);

  session.consumeNextResult();
  scheduler.onForegroundQueryEnded();
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
// Fair helper policy
// -----------------------------------------------------------------------------

namespace {

// Wait until `predicate` holds, at most `timeout`.
template <typename Predicate>
bool eventually(Predicate predicate, std::chrono::milliseconds timeout = 5s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(1ms);
  }
  return predicate();
}

// Mirrors `CheckpointMorselRunner`: serializes rows [begin, end) and checks
// the session epoch after every `checkEvery` rows. On an epoch change it
// resubmits the unprocessed tail and returns the rows done so far. Every row
// increments its counter once, so a lost or duplicated row is visible.
struct RangeTask {
  std::shared_ptr<ExportJobState<std::string>> state_;
  std::shared_ptr<std::vector<std::atomic<int>>> counts_;
  size_t begin_;
  size_t end_;
  std::chrono::microseconds perRow_{0};
  size_t checkEvery_{1};
  bool checkpoints_{true};

  std::string operator()() const {
    const uint64_t epoch{state_->currentEpoch()};
    std::string done{};
    for (size_t pos{begin_}; pos < end_; ++pos) {
      (*counts_)[pos].fetch_add(1);
      done += std::to_string(pos) + ",";
      if (perRow_.count() > 0) {
        std::this_thread::sleep_for(perRow_);
      }
      const bool checkpoint = (pos + 1 - begin_) % checkEvery_ == 0;
      if (checkpoints_ && checkpoint && pos + 1 < end_ &&
          state_->currentEpoch() != epoch) {
        RangeTask tail{*this};
        tail.begin_ = pos + 1;
        // Check tail resubmit thread-safely; verify on main thread.
        // e.g. store success in atomic<bool> or AD_CONTRACT_CHECK
        auto ok = state_->trySubmitMorsel(tail);
        AD_CONTRACT_CHECK(ok);
        // or: tailSubmitOk->store(ok); and main thread ASSERT_TRUE(tailSubmitOk->load());
        return done;
      }
    }
    return done;
  }
};

auto makeCounts(size_t numRows) {
  return std::make_shared<std::vector<std::atomic<int>>>(numRows);
}

void expectEveryRowOnce(const std::vector<std::atomic<int>>& counts) {
  for (size_t i{0}; i < counts.size(); ++i) {
    ASSERT_EQ(counts[i].load(), 1) << "row " << i;
  }
}

// Submit `numMorsels` morsels of `rowsPerMorsel` rows each.
void submitRanges(ExportWorkSession<std::string>& session,
                  const std::shared_ptr<std::vector<std::atomic<int>>>& counts,
                  size_t numMorsels, size_t rowsPerMorsel,
                  std::chrono::microseconds perRow, bool checkpoints) {
  for (size_t i = 0; i < numMorsels; ++i) {
    session.submitMorsel(RangeTask{session.sharedState(), counts,
                                   i * rowsPerMorsel, (i + 1) * rowsPerMorsel,
                                   perRow, 1, checkpoints});
  }
}

}  // namespace

// The quota rule for every n = 1..m+1: floor(m/n) threads per query, the
// remainder to the earliest queries, one thread is the coordinator.
TEST(ElasticExportSchedulerTest, FairThreadQuotaFormula) {
  for (size_t m : {1u, 2u, 5u, 8u}) {
    for (size_t n = 1u; n <= m + size_t{1}; ++n) {
      size_t sum{0};
      for (size_t rank = 0; rank < n; ++rank) {
        const size_t total{ElasticExportScheduler::fairThreadQuota(m, n, rank)};
        EXPECT_EQ(total, m / n + (rank < m % n ? 1 : 0));
        // Earlier queries never get fewer threads than later ones.
        if (rank > 0) {
          EXPECT_GE(ElasticExportScheduler::fairThreadQuota(m, n, rank - 1),
                    total);
        }
        EXPECT_EQ(ElasticExportScheduler::fairHelperQuota(m, n, rank),
                  total > 0 ? total - 1 : 0);
        if (n >= m) {
          EXPECT_EQ(ElasticExportScheduler::fairHelperQuota(m, n, rank), 0u);
        }
        sum += total;
      }
      EXPECT_EQ(sum, m) << "m=" << m << " n=" << n;
    }
  }
  // m = 8: n = 1 -> 7 helpers; n = 3 -> 3,3,2 threads -> 2,2,1 helpers.
  const std::vector<size_t> actual = {
      ElasticExportScheduler::fairHelperQuota(8, 1, 0),
      ElasticExportScheduler::fairHelperQuota(8, 3, 0),
      ElasticExportScheduler::fairHelperQuota(8, 3, 1),
      ElasticExportScheduler::fairHelperQuota(8, 3, 2),
      ElasticExportScheduler::fairHelperQuota(8, 9, 0)};
  EXPECT_THAT(actual, ElementsAre(7u, 2u, 2u, 1u, 0u));
}

// Live sessions get their quota by start order; a finishing query hands its
// threads to the remaining sessions.
TEST(ElasticExportSchedulerTest, FairQuotasFollowSessionStartOrder) {
  ElasticExportScheduler scheduler{5, 64};
  scheduler.setHelperPolicy(HelperPolicy::Fair);
  for (int i = 0; i < 3; ++i) {
    scheduler.onForegroundQueryStarted();
  }
  auto s0{scheduler.createSession<std::string>()};
  auto s1 = scheduler.createSession<std::string>();
  auto s2 = scheduler.createSession<std::string>();
  EXPECT_EQ(s0.helperPolicy(), HelperPolicy::Fair);
  // m = 5, n = 3: 2, 2, 1 threads.
  EXPECT_EQ(s0.helperQuota(), 1u);
  EXPECT_EQ(s1.helperQuota(), 1u);
  EXPECT_EQ(s2.helperQuota(), 0u);
  EXPECT_EQ(s2.state(), SessionState::PrimaryOnly);

  // The first query finishes: m = 5, n = 2 -> 3, 2 threads.
  s0.drainRemainingResults();
  scheduler.onForegroundQueryEnded();
  EXPECT_EQ(s1.helperQuota(), 2u);
  EXPECT_EQ(s2.helperQuota(), 1u);
  EXPECT_EQ(s2.state(), SessionState::HelpersEligible);

  // A fourth query arrives: m = 5, n = 3 -> 2, 2 threads for s1, s2.
  scheduler.onForegroundQueryStarted();
  EXPECT_EQ(s1.helperQuota(), 1u);
  EXPECT_EQ(s2.helperQuota(), 1u);
  // n >= m: no helpers at all.
  scheduler.onForegroundQueryStarted();
  scheduler.onForegroundQueryStarted();
  EXPECT_EQ(s1.helperQuota(), 0u);
  EXPECT_EQ(s2.helperQuota(), 0u);
  for (int i = 0; i < 4; ++i) {
    scheduler.onForegroundQueryEnded();
  }
  EXPECT_EQ(s1.helperQuota(), 2u);
  EXPECT_EQ(s2.helperQuota(), 1u);
}

// Posted helper loops never exceed the quota and run every pending morsel.
TEST(ElasticExportSchedulerTest, FairPostedHelperLoopsRespectQuota) {
  std::vector<absl::AnyInvocable<void()>> posted;
  ElasticExportScheduler scheduler(
      [&posted](absl::AnyInvocable<void()> task) {
        posted.push_back(std::move(task));
      },
      3, 64);
  auto session = scheduler.createSession<std::string>(HelperPolicy::Fair);
  EXPECT_EQ(session.helperQuota(), 2u);
  for (int i = 0; i < 5; ++i) {
    session.submitMorsel([i]() { return std::to_string(i); });
  }
  ASSERT_EQ(posted.size(), 2u);
  EXPECT_EQ(session.postedHelpers(), 2u);
  posted[0]();
  posted[1]();
  EXPECT_EQ(session.postedHelpers(), 0u);
  EXPECT_EQ(session.activeHelpers(), 0u);
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(session.consumeNextResult(), std::to_string(i));
  }
  for (const auto& profile : session.inspectMorselProfiles()) {
    EXPECT_TRUE(profile.executedByHelper_);
  }
}

// Unordered session: a new query shrinks the running session at the next
// checkpoint, the end of that query grows it again; every row once.
TEST(ElasticExportSchedulerTest, FairShrinkOnArrivalGrowOnFinish) {
  ElasticExportScheduler scheduler(4, 256);
  scheduler.setHelperPolicy(HelperPolicy::Fair);
  scheduler.onForegroundQueryStarted();
  auto sessionA = scheduler.createSession<std::string>();
  sessionA.setOrdered(false);
  EXPECT_EQ(sessionA.helperQuota(), 3u);

  constexpr size_t numMorsels = 64;
  constexpr size_t rowsPerMorsel = 200;
  auto counts = makeCounts(numMorsels * rowsPerMorsel);
  submitRanges(sessionA, counts, numMorsels, rowsPerMorsel, 100us, true);
  EXPECT_TRUE(eventually([&] { return sessionA.activeHelpers() == 3; }));

  // Arrival: m = 4, n = 2 -> A keeps 2 threads = 1 helper, B gets 1 helper.
  scheduler.onForegroundQueryStarted();
  EXPECT_EQ(sessionA.helperQuota(), 1u);
  EXPECT_TRUE(eventually([&] { return sessionA.activeHelpers() <= 1; }));
  auto sessionB = scheduler.createSession<std::string>();
  EXPECT_EQ(sessionB.helperQuota(), 1u);
  EXPECT_LE(sessionA.activeHelpers(), 1u);

  // B finishes: A grows back to 3 helpers right away.
  sessionB.drainRemainingResults();
  scheduler.onForegroundQueryEnded();
  EXPECT_EQ(sessionA.helperQuota(), 3u);
  EXPECT_TRUE(eventually([&] { return sessionA.activeHelpers() == 3; }));

  sessionA.drainRemainingResults();
  expectEveryRowOnce(*counts);
  scheduler.onForegroundQueryEnded();
}

// Ordered sessions have no checkpoints: surplus helpers leave after their
// running morsel, and slot order is preserved.
TEST(ElasticExportSchedulerTest, FairOrderedSessionShrinksAfterMorsel) {
  ElasticExportScheduler scheduler(4, 256);
  scheduler.setHelperPolicy(HelperPolicy::Fair);
  scheduler.onForegroundQueryStarted();
  auto session = scheduler.createSession<std::string>();
  constexpr size_t numMorsels = 40;
  constexpr size_t rowsPerMorsel = 20;
  auto counts = makeCounts(numMorsels * rowsPerMorsel);
  submitRanges(session, counts, numMorsels, rowsPerMorsel, 1ms, false);
  EXPECT_TRUE(eventually([&] { return session.activeHelpers() == 3; }));

  scheduler.onForegroundQueryStarted();
  EXPECT_EQ(session.helperQuota(), 1u);
  EXPECT_TRUE(eventually([&] { return session.activeHelpers() <= 1; }));

  for (size_t i = 0; i < numMorsels; ++i) {
    std::string expected;
    for (size_t pos = i * rowsPerMorsel; pos < (i + size_t{1}) * rowsPerMorsel; ++pos) {
      expected += std::to_string(pos) + ",";
    }
    EXPECT_EQ(session.consumeNextResult(), expected);
  }
  EXPECT_EQ(session.totalSlots(), numMorsels);
  expectEveryRowOnce(*counts);
  scheduler.onForegroundQueryEnded();
  scheduler.onForegroundQueryEnded();
}

// Many arrivals and departures while the coordinator consumes: every
// revocation splits running morsels, no row is lost or duplicated.
TEST(ElasticExportSchedulerTest, FairRepeatedRevocationLosesNoRow) {
  ElasticExportScheduler scheduler(4, 1024);
  scheduler.setHelperPolicy(HelperPolicy::Fair);
  scheduler.onForegroundQueryStarted();
  auto session = scheduler.createSession<std::string>();
  session.setOrdered(false);
  constexpr size_t numMorsels = 200;
  constexpr size_t rowsPerMorsel = 64;
  auto counts = makeCounts(numMorsels * rowsPerMorsel);
  submitRanges(session, counts, numMorsels, rowsPerMorsel, 5us, true);

  std::atomic<bool> stop{false};
  std::thread churn{[&] {
    while (!stop.load()) {
      scheduler.onForegroundQueryStarted();
      std::this_thread::sleep_for(200us);
      scheduler.onForegroundQueryEnded();
      std::this_thread::sleep_for(200us);
    }
  });
  size_t rows{0};
  while (session.hasMoreResults()) {
    const std::string part = session.consumeNextResult();
    rows += static_cast<size_t>(std::count(part.begin(), part.end(), ','));
  }
  stop = true;
  churn.join();
  EXPECT_EQ(rows, numMorsels * rowsPerMorsel);
  expectEveryRowOnce(*counts);
  // The churn did split morsels.
  EXPECT_GE(session.totalSlots(), numMorsels);
  scheduler.onForegroundQueryEnded();
}

// Several concurrent fair sessions with query churn (TSan target).
TEST(ElasticExportSchedulerTest, FairConcurrentSessionsStress) {
  ElasticExportScheduler scheduler(6, 1024);
  scheduler.setHelperPolicy(HelperPolicy::Fair);
  constexpr size_t numSessions = 4;
  constexpr size_t numMorsels = 60;
  constexpr size_t rowsPerMorsel = 50;
  std::atomic<bool> stop{false};
  std::thread churn([&] {
    while (!stop.load()) {
      scheduler.onForegroundQueryStarted();
      std::this_thread::sleep_for(300us);
      scheduler.onForegroundQueryEnded();
    }
  });
  std::vector<std::thread> exports{};
  std::vector<std::shared_ptr<std::vector<std::atomic<int>>>> counts{};
  for (size_t s = 0; s < numSessions; ++s) {
    counts.push_back(makeCounts(numMorsels * rowsPerMorsel));
  }
  std::atomic<size_t> maxQuotaSeen{0};
  for (size_t s = 0; s < numSessions; ++s) {
    exports.emplace_back([&, s] {
      scheduler.onForegroundQueryStarted();
      auto session = scheduler.createSession<std::string>();
      session.setOrdered(s % 2u == 0u ? false : true);
      submitRanges(session, counts[s], numMorsels, rowsPerMorsel, 2us,
                   s % 2 == 0);
      while (session.hasMoreResults()) {
        session.consumeNextResult();
        size_t quota = session.helperQuota();
        size_t seen = maxQuotaSeen.load();
        while (quota > seen &&
               !maxQuotaSeen.compare_exchange_weak(seen, quota)) {
        }
      }
      session.drainRemainingResults();
      scheduler.onForegroundQueryEnded();
    });
  }
  for (auto& t : exports) {
    t.join();
  }
  stop = true;
  churn.join();
  for (const auto& c : counts) {
    expectEveryRowOnce(*c);
  }
  EXPECT_LE(maxQuotaSeen.load(), 5u);
  EXPECT_EQ(scheduler.activeForegroundQueries(), 0u);
  EXPECT_TRUE(eventually([&] { return scheduler.activeHelperCount() == 0; }));
}

// Under the exclusive policy a second query still revokes all helpers.
TEST(ElasticExportSchedulerTest, ExclusivePolicyIgnoresFairQuota) {
  ElasticExportScheduler scheduler(4, 64);
  scheduler.onForegroundQueryStarted();
  auto session = scheduler.createSession<std::string>(HelperPolicy::Exclusive);
  EXPECT_EQ(session.state(), SessionState::HelpersEligible);
  EXPECT_EQ(session.helperQuota(), 0u);
  scheduler.onForegroundQueryStarted();
  EXPECT_EQ(session.state(), SessionState::PrimaryOnly);
  scheduler.onForegroundQueryEnded();
  EXPECT_EQ(session.state(), SessionState::HelpersEligible);
  scheduler.onForegroundQueryEnded();
}
