// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Marvin Stoetzel <marvin.stoetzel@mailbox.org>

#include "engine/export_v2/ElasticExportScheduler.h"

#include <algorithm>

namespace ad_utility::export_v2 {

// -----------------------------------------------------------------------------
// Implement `ExportWorkLease`.
// -----------------------------------------------------------------------------

ExportWorkLease::ExportWorkLease(ElasticExportScheduler* scheduler,
                                 uint64_t epoch, uint64_t jobId,
                                 uint64_t leaseId) noexcept
    : scheduler_{scheduler},
      epoch_{epoch},
      jobId_{jobId},
      leaseId_{leaseId},
      active_{true} {}

ExportWorkLease::~ExportWorkLease() { release(); }

ExportWorkLease::ExportWorkLease(ExportWorkLease&& other) noexcept
    : scheduler_{other.scheduler_},
      epoch_{other.epoch_},
      jobId_{other.jobId_},
      leaseId_{other.leaseId_},
      active_{other.active_} {
  other.active_ = false;
  other.scheduler_ = nullptr;
  other.epoch_ = 0;
  other.jobId_ = 0;
  other.leaseId_ = 0;
}

ExportWorkLease& ExportWorkLease::operator=(ExportWorkLease&& other) noexcept {
  if (this != &other) {
    release();
    scheduler_ = other.scheduler_;
    epoch_ = other.epoch_;
    jobId_ = other.jobId_;
    leaseId_ = other.leaseId_;
    active_ = other.active_;
    other.active_ = false;
    other.scheduler_ = nullptr;
    other.epoch_ = 0;
    other.jobId_ = 0;
    other.leaseId_ = 0;
  }
  return *this;
}

void ExportWorkLease::release() noexcept {
  if (active_) {
    active_ = false;
    if (scheduler_ != nullptr) {
      scheduler_->onLeaseReleased(epoch_, jobId_);
    }
  }
}

// -----------------------------------------------------------------------------
// ElasticExportScheduler Implementation
// -----------------------------------------------------------------------------

ElasticExportScheduler::ElasticExportScheduler(size_t numThreads,
                                               size_t queueCapacity)
    : maxQueueCapacity_{queueCapacity > 0 ? queueCapacity : 1024} {
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
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      workAvailableCv_.notify_all();
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
      session->onDemandChanged(current, newEpoch);
    }
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
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      workAvailableCv_.notify_all();
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
      session->onDemandChanged(current, newEpoch);
    }
  }
}

void ElasticExportScheduler::attachToQueryRegistry(
    ad_utility::websocket::QueryRegistry& registry) {
  registry.addOnStart(
      [this](const ad_utility::websocket::QueryRegistry::StartInfo&) {
        onForegroundQueryStarted();
      });
  registry.addOnEnd(
      [this](const ad_utility::websocket::QueryRegistry::EndInfo&) {
        onForegroundQueryEnded();
      });
}

bool ElasticExportScheduler::enqueueMorsel(OwnedMorsel morsel) {
  std::unique_lock<std::mutex> lock(queueMutex_);
  while (queue_.size() >= maxQueueCapacity_ &&
         !stopping_.load(std::memory_order_relaxed)) {
    queueNotFullCv_.wait(lock);
  }
  if (stopping_.load(std::memory_order_relaxed)) {
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

void ElasticExportScheduler::onLeaseReleased(
    [[maybe_unused]] uint64_t epoch, [[maybe_unused]] uint64_t jobId) noexcept {
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
        return stopping_.load(std::memory_order_relaxed) ||
               (!queue_.empty() && isHelperAdmissionEligibleUnsafe());
      });

      if (stopping_.load(std::memory_order_relaxed)) {
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

      leaseEpoch = demandEpoch_.load(std::memory_order_relaxed);
      leaseId = nextLeaseId_.fetch_add(1, std::memory_order_relaxed);
      totalActiveHelpers_.fetch_add(1, std::memory_order_relaxed);
    }

    ExportWorkLease lease(this, leaseEpoch, jobId, leaseId);

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
