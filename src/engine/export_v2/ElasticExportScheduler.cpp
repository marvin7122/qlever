// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/export_v2/ElasticExportScheduler.h"

#include <absl/cleanup/cleanup.h>

#include <algorithm>

#include "util/ExceptionHandling.h"

namespace ad_utility::export_v2 {

// -----------------------------------------------------------------------------
// ElasticExportScheduler Implementation
// -----------------------------------------------------------------------------

std::shared_ptr<ElasticExportScheduler> ElasticExportScheduler::create(
    size_t numThreads, size_t queueCapacity) {
  // `new`, not `make_shared`: the constructor is private, so only this
  // member function can invoke it.
  return std::shared_ptr<ElasticExportScheduler>(
      new ElasticExportScheduler(numThreads, queueCapacity));
}

ElasticExportScheduler::ElasticExportScheduler(size_t numThreads,
                                               size_t queueCapacity)
    : maxQueueCapacity_{queueCapacity > 0 ? queueCapacity
                                          : kDefaultQueueCapacity} {
  size_t threadCount = numThreads;
  if (threadCount == 0) {
    threadCount = std::max(1u, std::thread::hardware_concurrency());
  }

  workers_.reserve(threadCount);
  for (size_t i = 0; i < threadCount; ++i) {
    workers_.emplace_back(&ElasticExportScheduler::workerLoop, this);
  }
}

ElasticExportScheduler::~ElasticExportScheduler() { shutdown(); }

void ElasticExportScheduler::shutdown() {
  bool expected = false;
  // Seq-cst store; all readers use acquire loads, which synchronize with
  // this store once observed.
  if (stopping_.compare_exchange_strong(expected, true)) {
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      workAvailableCv_.notify_all();
      queueNotFullCv_.notify_all();
    }
    for (auto& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }
}

void ElasticExportScheduler::onForegroundQueryStarted() {
  size_t prev =
      activeForegroundQueries_.fetch_add(1, std::memory_order_relaxed);
  size_t current = prev + 1;
  size_t maxQueries =
      maxForegroundQueriesForHelperAdmission_.load(std::memory_order_relaxed);

  if (current > maxQueries) {
    // Release half pairs with the acquire load in `workerLoop`.
    uint64_t newEpoch =
        demandEpoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
    propagateDemandChange(current, newEpoch);
  }
}

void ElasticExportScheduler::onForegroundQueryEnded() {
  size_t prev =
      activeForegroundQueries_.fetch_sub(1, std::memory_order_relaxed);
  AD_CORRECTNESS_CHECK(prev > 0, "Underflow in activeForegroundQueries_");
  size_t current = prev - 1;
  size_t maxQueries =
      maxForegroundQueriesForHelperAdmission_.load(std::memory_order_relaxed);

  if (current <= maxQueries) {
    // Release half pairs with the acquire load in `workerLoop`.
    uint64_t newEpoch =
        demandEpoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
    propagateDemandChange(current, newEpoch);
  }
}

// Shared by both demand-change hooks: wake producers and workers (a
// threshold crossing can unblock a full queue or strand one, depending on
// direction), drop expired sessions, and notify the live ones without
// holding any scheduler lock while calling out.
void ElasticExportScheduler::propagateDemandChange(
    size_t activeForegroundQueries, uint64_t newEpoch) {
  {
    std::lock_guard<std::mutex> lock(queueMutex_);
    workAvailableCv_.notify_all();
    queueNotFullCv_.notify_all();
  }

  std::vector<std::shared_ptr<ExportJobStateBase>> aliveSessions;
  {
    std::lock_guard<std::mutex> lock(sessionsMutex_);
    sessions_.erase(
        std::remove_if(sessions_.begin(), sessions_.end(),
                       [&aliveSessions](const auto& weak) {
                         if (auto shared = weak.lock()) {
                           aliveSessions.push_back(std::move(shared));
                           return false;
                         }
                         return true;
                       }),
        sessions_.end());
  }

  for (auto& session : aliveSessions) {
    session->onDemandChanged(activeForegroundQueries, newEpoch);
  }
}

void ElasticExportScheduler::attachToQueryRegistry(
    ad_utility::websocket::QueryRegistry& registry) {
  // Never empty: instances only exist as `shared_ptr` (see `create`), so
  // `weak_from_this` always succeeds here.
  std::weak_ptr<ElasticExportScheduler> weak = weak_from_this();
  // Callbacks observe the scheduler instead of borrowing `this`: a start/end
  // event that fires after scheduler destruction — e.g. an `OwningQueryId`
  // unregister path running after teardown, or the registry's `shared_ptr`-
  // held end callbacks surviving registry destruction — is a no-op.
  registry.addOnStart(
      [weak](const ad_utility::websocket::QueryRegistry::StartInfo&) {
        if (auto self = weak.lock()) {
          self->onForegroundQueryStarted();
        }
      });
  registry.addOnEnd(
      [weak](const ad_utility::websocket::QueryRegistry::EndInfo&) {
        if (auto self = weak.lock()) {
          self->onForegroundQueryEnded();
        }
      });
}

bool ElasticExportScheduler::enqueueMorsel(OwnedMorsel morsel) {
  std::unique_lock<std::mutex> lock(queueMutex_);
  // Besides shutdown, also stop waiting when helpers become ineligible:
  // workers refuse to drain the queue while ineligible, so waiting for
  // space alone could block forever. Callers treat false as "leave the
  // morsel Pending for the primary", and every demand change wakes
  // waiters to re-check. A full queue with eligible helpers still blocks
  // for backpressure.
  queueNotFullCv_.wait(lock, [this] {
    return stopping_.load(std::memory_order_acquire) ||
           queue_.size() < maxQueueCapacity_ ||
           !isHelperAdmissionEligibleUnsafe();
  });
  if (stopping_.load(std::memory_order_acquire) ||
      !isHelperAdmissionEligibleUnsafe()) {
    return false;
  }
  queue_.push_back(std::move(morsel));
  workAvailableCv_.notify_one();
  return true;
}

void ElasticExportScheduler::registerSession(
    std::weak_ptr<ExportJobStateBase> sessionState) {
  std::lock_guard<std::mutex> lock(sessionsMutex_);
  sessions_.erase(
      std::remove_if(sessions_.begin(), sessions_.end(),
                     [](const auto& weak) { return weak.expired(); }),
      sessions_.end());
  sessions_.push_back(std::move(sessionState));
}

void ElasticExportScheduler::registerOutstandingLease(uint64_t leaseId) {
  std::lock_guard<std::mutex> lock(queueMutex_);
  AD_CORRECTNESS_CHECK(outstandingLeaseIds_.insert(leaseId).second,
                       "Duplicate export helper lease identity");
  // Accounting lives with identity: every outstanding lease holds exactly
  // one helper slot, so the count always equals the set size.
  totalActiveHelpers_.fetch_add(1, std::memory_order_relaxed);
}

void ElasticExportScheduler::onLeaseReleased(uint64_t leaseId) {
  std::lock_guard<std::mutex> lock(queueMutex_);
  // Only an outstanding lease identity may retire a helper slot; anything
  // else is a stale or duplicate release and an internal error.
  AD_CORRECTNESS_CHECK(outstandingLeaseIds_.erase(leaseId) == 1,
                       "Release of unknown export helper lease");
  totalActiveHelpers_.fetch_sub(1, std::memory_order_relaxed);
}

bool ElasticExportScheduler::isHelperAdmissionEligibleUnsafe() const noexcept {
  return activeForegroundQueries_.load(std::memory_order_relaxed) <=
         maxForegroundQueriesForHelperAdmission_.load(
             std::memory_order_relaxed);
}

void ElasticExportScheduler::workerLoop() {
  while (true) {
    std::shared_ptr<ExportJobStateBase> targetJobState;
    size_t targetMorselIndex = 0;
    uint64_t submissionEpoch = 0;
    uint64_t leaseEpoch = 0;
    uint64_t leaseId = 0;

    {
      std::unique_lock<std::mutex> lock(queueMutex_);
      workAvailableCv_.wait(lock, [this] {
        return stopping_.load(std::memory_order_acquire) ||
               (!queue_.empty() && isHelperAdmissionEligibleUnsafe());
      });

      if (stopping_.load(std::memory_order_acquire)) {
        break;
      }

      if (queue_.empty() || !isHelperAdmissionEligibleUnsafe()) {
        continue;
      }

      auto morsel = std::move(queue_.front());
      queue_.pop_front();
      queueNotFullCv_.notify_one();

      targetJobState = std::move(morsel.jobState_);
      targetMorselIndex = morsel.morselIndex_;
      submissionEpoch = morsel.submissionEpoch_;

      // Acquire pairs with the acq-rel increments of the epoch on demand
      // changes. Identity registration and slot accounting happen
      // below, outside `queueMutex_`.
      leaseEpoch = demandEpoch_.load(std::memory_order_acquire);
      leaseId = nextLeaseId_.fetch_add(1, std::memory_order_relaxed);
    }

    // Account the helper slot directly on `this`: `this` is valid here
    // because `shutdown` joins all workers before destruction completes, and
    // the worker never holds an owning reference to the scheduler (dropping
    // the last owner here would make `shutdown` join this worker from
    // itself).
    registerOutstandingLease(leaseId);
    // A failed release is an accounting bug that cannot propagate out of a
    // destructor, so it terminates with a diagnostic.
    absl::Cleanup releaseLease = [this, leaseId] {
      ad_utility::terminateIfThrows(
          [this, leaseId] { onLeaseReleased(leaseId); },
          "Releasing an export helper lease");
    };

    // A morsel skipped here (cancelled job or stale epoch) is only dropped
    // from the queue; its slot stays `Pending`, so the coordinator executes
    // it in `consumeNextResult`.
    if (targetJobState && !targetJobState->isCancelled() &&
        submissionEpoch == leaseEpoch) {
      targetJobState->onHelperLeaseAcquired(leaseEpoch);
      // Pair acquisition with release even if `executeHelperTask` throws, so
      // a session never stays `Revoking` on a leaked helper count.
      absl::Cleanup releaseJobLease = [&targetJobState, leaseEpoch] {
        ad_utility::terminateIfThrows(
            [&targetJobState, leaseEpoch] {
              targetJobState->onHelperLeaseReleased(leaseEpoch);
            },
            "Releasing an export job helper lease");
      };
      targetJobState->executeHelperTask(targetMorselIndex, leaseEpoch);
    }
  }
}

}  // namespace ad_utility::export_v2
