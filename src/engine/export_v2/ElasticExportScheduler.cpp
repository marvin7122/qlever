// Copyright 2026, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Marvin Stoetzel <marvin.stoetzel@mailbox.org>

#include "engine/export_v2/ElasticExportScheduler.h"

#include <algorithm>
#include <optional>
#include <vector>

namespace ad_utility::export_v2 {

// -----------------------------------------------------------------------------
// ExportWorkLease Implementation
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

std::shared_ptr<ElasticExportScheduler> ElasticExportScheduler::create(
    size_t numThreads, size_t queueCapacity) {
  // `new`, not `make_shared`: the constructor is private, so only this
  // member function can invoke it.
  return std::shared_ptr<ElasticExportScheduler>(
      new ElasticExportScheduler(numThreads, queueCapacity));
}

std::shared_ptr<ElasticExportScheduler> ElasticExportScheduler::create(
    WorkPoster poster, size_t queueCapacity) {
  return std::shared_ptr<ElasticExportScheduler>(
      new ElasticExportScheduler(std::move(poster), queueCapacity));
}

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

ElasticExportScheduler::ElasticExportScheduler(WorkPoster poster,
                                               size_t queueCapacity)
    : poster_{std::move(poster)},
      maxQueueCapacity_{queueCapacity > 0 ? queueCapacity : 1024} {
  AD_CONTRACT_CHECK(static_cast<bool>(poster_));
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
      // Wake blocked enqueuers too: this transition may have changed helper
      // eligibility, and enqueueMorsel re-checks it after every wakeup.
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

    liveSessionCount_.store(aliveSessions.size(), std::memory_order_relaxed);

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
      // Wake blocked enqueuers too: this transition may have restored helper
      // eligibility, and enqueueMorsel re-checks it after every wakeup.
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

    liveSessionCount_.store(aliveSessions.size(), std::memory_order_relaxed);

    for (auto& session : aliveSessions) {
      session->onDemandChanged(current, newEpoch);
    }
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
  if (poster_) {
    if (stopping_.load(std::memory_order_relaxed)) {
      return false;
    }
    // Decide admission under the lock, but post outside of it: `poster_`
    // may execute the work inline, and completion accounting takes
    // `queueMutex_` again (a non-recursive mutex), so posting while holding
    // the lock deadlocks a synchronous poster.
    std::optional<OwnedMorsel> toPost;
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      const size_t live = liveSessionCount_.load(std::memory_order_relaxed);
      const size_t max = maxConcurrentMorsels_.load(std::memory_order_relaxed);
      const size_t share = fairShareUnsafe(live);
      const size_t committed = committedOutstandingUnsafe(morsel.jobId_);
      // Even split with a progress floor of one: below-share sessions post
      // immediately while total capacity allows, the rest wait first-in
      // first-out in pendingAdmission_.
      if (committed < share && totalOutstanding_ < max) {
        toPost.emplace(std::move(morsel));
      } else {
        pendingAdmission_.push_back(std::move(morsel));
      }
    }
    if (toPost.has_value()) {
      postAccounted(std::move(*toPost));
    }
    return true;
  }
  std::unique_lock<std::mutex> lock(queueMutex_);
  // Rejected while helpers are ineligible: workers stop draining the queue
  // in that state, so blocking here could wait forever. The coordinator
  // executes rejected morsels on the primary path instead.
  if (!isHelperAdmissionEligibleUnsafe()) {
    return false;
  }
  while (queue_.size() >= maxQueueCapacity_ &&
         !stopping_.load(std::memory_order_relaxed) &&
         isHelperAdmissionEligibleUnsafe()) {
    queueNotFullCv_.wait(lock);
  }
  if (stopping_.load(std::memory_order_relaxed) ||
      !isHelperAdmissionEligibleUnsafe()) {
    return false;
  }
  queue_.push_back(std::move(morsel));
  workAvailableCv_.notify_one();
  return true;
}

void ElasticExportScheduler::postAccounted(OwnedMorsel morsel) {
  {
    std::lock_guard<std::mutex> lock(queueMutex_);
    ++outstandingPerSession_[morsel.jobId_];
    ++totalOutstanding_;
  }
  // Outside the lock (see `enqueueMorsel`): `poster_` may run the closure
  // inline, and its completion path takes `queueMutex_` again.
  poster_(makePostedWork(std::move(morsel)));
}

absl::AnyInvocable<void()> ElasticExportScheduler::makePostedWork(
    OwnedMorsel morsel) {
  const uint64_t jobId = morsel.jobId_;
  return [this, jobId, morsel = std::move(morsel)]() mutable {
    try {
      runPostedMorsel(std::move(morsel));
    } catch (...) {
      // Account completion before propagating: shares must not clog on
      // throwing tasks. Propagation semantics stay unchanged.
      onPostedMorselFinished(jobId);
      throw;
    }
    onPostedMorselFinished(jobId);
  };
}

void ElasticExportScheduler::onPostedMorselFinished(uint64_t jobId) {
  std::vector<OwnedMorsel> readyToPost;
  {
    std::lock_guard<std::mutex> lock(queueMutex_);
    decrementOutstandingUnsafe(jobId);
    readyToPost = drainPendingAdmissionUnsafe();
  }
  // Outside the lock: posting may run work inline (see `enqueueMorsel`).
  for (auto& ready : readyToPost) {
    postAccounted(std::move(ready));
  }
}

size_t ElasticExportScheduler::committedOutstandingUnsafe(
    uint64_t jobId) const {
  auto it = outstandingPerSession_.find(jobId);
  return it != outstandingPerSession_.end() ? it->second : 0;
}

void ElasticExportScheduler::decrementOutstandingUnsafe(uint64_t jobId) {
  auto it = outstandingPerSession_.find(jobId);
  AD_CORRECTNESS_CHECK(it != outstandingPerSession_.end(),
                       "Completion without outstanding morsel");
  AD_CORRECTNESS_CHECK(it->second > 0, "Outstanding count underflow");
  AD_CORRECTNESS_CHECK(totalOutstanding_ > 0, "Total outstanding underflow");
  if (--(it->second) == 0) {
    outstandingPerSession_.erase(it);
  }
  --totalOutstanding_;
}

std::vector<OwnedMorsel> ElasticExportScheduler::drainPendingAdmissionUnsafe() {
  const size_t max = maxConcurrentMorsels_.load(std::memory_order_relaxed);
  const size_t live = liveSessionCount_.load(std::memory_order_relaxed);
  const size_t share = fairShareUnsafe(live);
  std::vector<OwnedMorsel> readyToPost;
  // Purge cancelled sessions first so their morsels never occupy shares.
  pendingAdmission_.erase(
      std::remove_if(
          pendingAdmission_.begin(), pendingAdmission_.end(),
          [](const OwnedMorsel& m) { return m.jobState_->isCancelled(); }),
      pendingAdmission_.end());
  // Oldest session first: lowest jobId among servable entries wins, which
  // implements the remainder-oldest rule while preserving first-in
  // first-out order within each session. Each pass scans the pending queue
  // once; the queue stays short in practice (admission fills every free
  // share eagerly), so a per-session index is future work for proven load.
  while (totalOutstanding_ < max && !pendingAdmission_.empty()) {
    auto best = pendingAdmission_.end();
    for (auto it = pendingAdmission_.begin(); it != pendingAdmission_.end();
         ++it) {
      const size_t committed = committedOutstandingUnsafe(it->jobId_);
      if (committed >= share) {
        continue;
      }
      if (best == pendingAdmission_.end() || it->jobId_ < best->jobId_) {
        best = it;
      }
    }
    if (best == pendingAdmission_.end()) {
      break;
    }
    OwnedMorsel morsel = std::move(*best);
    pendingAdmission_.erase(best);
    ++outstandingPerSession_[morsel.jobId_];
    ++totalOutstanding_;
    readyToPost.push_back(std::move(morsel));
  }
  return readyToPost;
}

void ElasticExportScheduler::runPostedMorsel(OwnedMorsel morsel) {
  if (stopping_.load(std::memory_order_relaxed) ||
      !isHelperAdmissionEligibleUnsafe()) {
    return;
  }
  auto targetJobState = std::move(morsel.jobState_);
  const size_t targetMorselIndex = morsel.morselIndex_;
  const uint64_t submissionEpoch = morsel.submissionEpoch_;
  const uint64_t jobId = morsel.jobId_;
  const uint64_t leaseEpoch = demandEpoch_.load(std::memory_order_relaxed);
  const uint64_t leaseId = nextLeaseId_.fetch_add(1, std::memory_order_relaxed);
  totalActiveHelpers_.fetch_add(1, std::memory_order_relaxed);
  ExportWorkLease lease(this, leaseEpoch, jobId, leaseId);
  if (targetJobState && !targetJobState->isCancelled() &&
      submissionEpoch == leaseEpoch) {
    targetJobState->onHelperLeaseAcquired(leaseEpoch);
    targetJobState->executeHelperTask(targetMorselIndex, leaseEpoch);
    targetJobState->onHelperLeaseReleased(leaseEpoch);
  }
}

void ElasticExportScheduler::registerSession(
    std::weak_ptr<ExportJobStateBase> sessionState) {
  std::lock_guard<std::mutex> lock(sessionsMutex_);
  sessions_.erase(
      std::remove_if(sessions_.begin(), sessions_.end(),
                     [](const auto& weak) { return weak.expired(); }),
      sessions_.end());
  sessions_.push_back(std::move(sessionState));
  liveSessionCount_.store(sessions_.size(), std::memory_order_relaxed);
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

      if (stopping_.load(std::memory_order_relaxed) && queue_.empty()) {
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
