// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Marvin Stoetzel <marvin.stoetzel@mailbox.org>

#include "engine/export_v2/ElasticExportScheduler.h"

#include <algorithm>

namespace ad_utility::export_v2 {

// -----------------------------------------------------------------------------
// ExportWorkLease Implementation
// -----------------------------------------------------------------------------

ExportWorkLease::ExportWorkLease(
    std::weak_ptr<ElasticExportScheduler> scheduler, uint64_t epoch,
    uint64_t jobId, uint64_t leaseId) noexcept
    : scheduler_{std::move(scheduler)},
      epoch_{epoch},
      jobId_{jobId},
      leaseId_{leaseId},
      active_{true} {
  // Register before anyone can release: every live lease has exactly one
  // outstanding identity, which `onLeaseReleased` validates. An expired
  // scheduler needs no accounting, so there is nothing to register to.
  if (auto live = scheduler_.lock()) {
    live->registerOutstandingLease(leaseId);
  } else {
    active_ = false;
  }
}

ExportWorkLease::~ExportWorkLease() { release(); }

ExportWorkLease::ExportWorkLease(ExportWorkLease&& other) noexcept
    : scheduler_{std::move(other.scheduler_)},
      epoch_{other.epoch_},
      jobId_{other.jobId_},
      leaseId_{other.leaseId_},
      active_{other.active_} {
  // Only `active_` gates the destructor; the remaining members are
  // unreadable once inactive, so they are left untouched.
  other.active_ = false;
  other.scheduler_.reset();
}

ExportWorkLease& ExportWorkLease::operator=(ExportWorkLease&& other) noexcept {
  if (this != &other) {
    release();
    scheduler_ = std::move(other.scheduler_);
    epoch_ = other.epoch_;
    jobId_ = other.jobId_;
    leaseId_ = other.leaseId_;
    active_ = other.active_;
    other.active_ = false;
    other.scheduler_.reset();
  }
  return *this;
}

void ExportWorkLease::release() noexcept {
  if (active_) {
    active_ = false;
    // `lock()` is noexcept; an expired scheduler means teardown already
    // ran, so there is nothing to account to.
    if (auto scheduler = scheduler_.lock()) {
      scheduler->onLeaseReleased(leaseId_);
    }
  }
}

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
    uint64_t newEpoch =
        demandEpoch_.fetch_add(1, std::memory_order_relaxed) + 1;
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
    uint64_t newEpoch =
        demandEpoch_.fetch_add(1, std::memory_order_relaxed) + 1;
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
  // one helper slot, so the count always equals the set size, including
  // for directly constructed leases.
  totalActiveHelpers_.fetch_add(1, std::memory_order_relaxed);
}

void ElasticExportScheduler::onLeaseReleased(uint64_t leaseId) noexcept {
  std::lock_guard<std::mutex> lock(queueMutex_);
  // Only an outstanding lease identity may retire a helper slot; anything
  // else is a stale or duplicate release and an internal error. The
  // lease-side `active_` flag already prevents double release through one
  // handle, this guards the accounting against anything else.
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
    uint64_t jobId = 0;
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
      jobId = morsel.jobId_;

      // Acquire pairs with the release-sequence incrementing the epoch on
      // demand changes. Identity registration and slot accounting happen
      // in the lease constructor below, outside `queueMutex_`.
      leaseEpoch = demandEpoch_.load(std::memory_order_acquire);
      leaseId = nextLeaseId_.fetch_add(1, std::memory_order_relaxed);
    }

    // Never empty: the scheduler owns its workers and outlives them
    // (`shutdown` joins before destruction completes).
    ExportWorkLease lease(weak_from_this(), leaseEpoch, jobId, leaseId);

    if (targetJobState && !targetJobState->isCancelled()) {
      if (submissionEpoch == leaseEpoch) {
        targetJobState->onHelperLeaseAcquired(leaseEpoch);
        targetJobState->executeHelperTask(targetMorselIndex, leaseEpoch);
        targetJobState->onHelperLeaseReleased(leaseEpoch);
      }
    }
  }
}

}  // namespace ad_utility::export_v2
