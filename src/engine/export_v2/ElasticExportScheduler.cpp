// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/export_v2/ElasticExportScheduler.h"

#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <exception>
#include <optional>
#include <vector>

#include "backports/algorithm.h"

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
      liveSessionCount_.store(aliveSessions.size(), std::memory_order_relaxed);
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
      liveSessionCount_.store(aliveSessions.size(), std::memory_order_relaxed);
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
  if (poster_) {
    if (stopping_.load(std::memory_order_relaxed)) {
      return false;
    }
    // Decide admission under the lock, but post outside of it: `poster_`
    // may execute the work inline, and completion accounting takes
    // `queueMutex_` again (a non-recursive mutex), so posting while holding
    // the lock deadlocks a synchronous poster.
    std::optional<OwnedMorsel> toPost;
    std::vector<OwnedMorsel> readyToPost;
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      const auto identity = std::pair{morsel.jobId_, morsel.morselIndex_};
      auto [it, inserted] =
          submissionEpochs_.try_emplace(identity, morsel.submissionEpoch_);
      if (!inserted) {
        it->second = std::max(it->second, morsel.submissionEpoch_);
        return true;
      }
      // An allocation failure while reserving or queuing a new identity must
      // not leave a phantom duplicate that suppresses a later retry.
      absl::Cleanup eraseIdentity{[&] { submissionEpochs_.erase(identity); }};
      const size_t live = liveSessionCount_.load(std::memory_order_relaxed);
      const size_t max = maxConcurrentMorsels_.load(std::memory_order_relaxed);
      const size_t share = fairShareUnsafe(live);
      const size_t committed = committedOutstandingUnsafe(morsel.jobId_);
      // Even split with a progress floor of one: below-share sessions post
      // immediately while total capacity allows, the rest wait first-in
      // first-out in pendingAdmission_.
      if (committed < share && totalOutstanding_ < max) {
        // Reserve the share atomically with the decision: a concurrent
        // enqueuer must see the reservation, and `postReady` below must
        // not count the morsel a second time.
        accountOutstandingUnsafe(morsel.jobId_);
        toPost.emplace(std::move(morsel));
        std::move(eraseIdentity).Cancel();
      } else {
        pendingAdmission_.push_back(std::move(morsel));
        std::move(eraseIdentity).Cancel();
        // The session may sit at its base share while total capacity is
        // still free (an indivisible max/live leaves a remainder): run the
        // admission loop now so remainder slots fill promptly instead of
        // waiting for the next completion.
        readyToPost = drainPendingAdmissionUnsafe();
      }
    }
    if (toPost.has_value()) {
      postReady(std::move(*toPost));
    }
    postReadyBatch(std::move(readyToPost));
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

void ElasticExportScheduler::accountOutstandingUnsafe(uint64_t jobId) {
  ++outstandingPerSession_[jobId];
  ++totalOutstanding_;
}

void ElasticExportScheduler::postReady(OwnedMorsel morsel,
                                       bool drainPendingOnFailure) {
  // Never holds `queueMutex_` here (see `enqueueMorsel`): `poster_` may run
  // the closure inline, and its completion path takes `queueMutex_` again.
  const uint64_t jobId = morsel.jobId_;
  auto jobState = morsel.jobState_;
  const size_t morselIndex = morsel.morselIndex_;
  std::shared_ptr<std::atomic<bool>> executionStarted;
  try {
    executionStarted = std::make_shared<std::atomic<bool>>(false);
    poster_(makePostedWork(std::move(morsel), executionStarted));
  } catch (...) {
    auto postingException = std::current_exception();
    // Once execution starts, the closure owns completion accounting, even
    // when the poster throws after invoking it inline.
    if (!executionStarted || !executionStarted->load()) {
      try {
        // Direct rollback may enter the iterative batch drain once. A batch
        // owns its drain, so only release this reservation on its failure.
        if (drainPendingOnFailure) {
          onPostedMorselFinished(jobId, morselIndex);
        } else {
          std::lock_guard<std::mutex> lock(queueMutex_);
          decrementOutstandingUnsafe(jobId, morselIndex);
        }
      } catch (...) {
        // Preserve the original posting error for both the caller and the
        // coordinator if rollback also fails.
        jobState->onMorselFailed(morselIndex, postingException);
      }
    }
    throw;
  }
}

// Post several already-accounted morsels without holding `queueMutex_`. A
// throwing poster releases its reservation via `postReady`, but this batch
// owns the iterative admission drain. Attempt the whole current batch before
// draining the next round, then rethrow the first failure across all rounds.
void ElasticExportScheduler::postReadyBatch(std::vector<OwnedMorsel> batch) {
  std::exception_ptr firstFailure;
  while (!batch.empty()) {
    for (auto& morsel : batch) {
      auto jobState = morsel.jobState_;
      const size_t morselIndex = morsel.morselIndex_;
      try {
        postReady(std::move(morsel), false);
      } catch (...) {
        jobState->onMorselFailed(morselIndex, std::current_exception());
        if (firstFailure == nullptr) {
          firstFailure = std::current_exception();
        }
      }
    }
    std::lock_guard<std::mutex> lock(queueMutex_);
    batch = drainPendingAdmissionUnsafe();
  }
  if (firstFailure != nullptr) {
    std::rethrow_exception(firstFailure);
  }
}

absl::AnyInvocable<void()> ElasticExportScheduler::makePostedWork(
    OwnedMorsel morsel, std::shared_ptr<std::atomic<bool>> executionStarted) {
  const uint64_t jobId = morsel.jobId_;
  auto jobState = morsel.jobState_;
  const size_t morselIndex = morsel.morselIndex_;
  return [this, jobId, jobState = std::move(jobState), morselIndex,
          executionStarted = std::move(executionStarted),
          morsel = std::move(morsel)]() mutable {
    executionStarted->store(true);
    try {
      runPostedMorsel(std::move(morsel));
    } catch (...) {
      // Pool handlers must not throw. The coordinator consumes the stored
      // task error after `runPostedMorsel` releases the helper lease.
      jobState->onMorselFailed(morselIndex, std::current_exception());
    }
    try {
      // Account completion exactly once on every path, so shares cannot clog
      // on throwing tasks.
      onPostedMorselFinished(jobId, morselIndex);
    } catch (...) {
      // Keep the original task error if completion or reposting also fails.
      jobState->onMorselFailed(morselIndex, std::current_exception());
    }
  };
}

void ElasticExportScheduler::onPostedMorselFinished(uint64_t jobId,
                                                    size_t morselIndex) {
  std::vector<OwnedMorsel> readyToPost;
  {
    std::lock_guard<std::mutex> lock(queueMutex_);
    decrementOutstandingUnsafe(jobId, morselIndex);
    readyToPost = drainPendingAdmissionUnsafe();
  }
  // Outside the lock: posting may run work inline (see `enqueueMorsel`).
  // The morsels are already accounted by the drain, so post without
  // counting them a second time.
  postReadyBatch(std::move(readyToPost));
}

size_t ElasticExportScheduler::committedOutstandingUnsafe(
    uint64_t jobId) const {
  auto it = outstandingPerSession_.find(jobId);
  return it != outstandingPerSession_.end() ? it->second : 0;
}

void ElasticExportScheduler::decrementOutstandingUnsafe(uint64_t jobId,
                                                        size_t morselIndex) {
  auto it = outstandingPerSession_.find(jobId);
  AD_CORRECTNESS_CHECK(it != outstandingPerSession_.end(),
                       "Completion without outstanding morsel");
  AD_CORRECTNESS_CHECK(it->second > 0, "Outstanding count underflow");
  AD_CORRECTNESS_CHECK(totalOutstanding_ > 0, "Total outstanding underflow");
  AD_CORRECTNESS_CHECK(submissionEpochs_.erase({jobId, morselIndex}) == 1,
                       "Completion without tracked morsel identity");
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
          [this](const OwnedMorsel& m) {
            if (!m.jobState_->isCancelled()) {
              return false;
            }
            AD_CORRECTNESS_CHECK(
                submissionEpochs_.erase({m.jobId_, m.morselIndex_}) == 1,
                "Cancellation without tracked morsel identity");
            return true;
          }),
      pendingAdmission_.end());
  // Admit one pending morsel and account it as outstanding.
  auto admitIt = [this, &readyToPost](auto it) {
    OwnedMorsel morsel = std::move(*it);
    pendingAdmission_.erase(it);
    accountOutstandingUnsafe(morsel.jobId_);
    readyToPost.push_back(std::move(morsel));
  };
  // Oldest session first means the lowest jobId, earliest in the queue on
  // ties, which preserves first-in first-out order within each session.
  auto oldestIt = [this]() {
    return ql::ranges::min_element(pendingAdmission_, {}, &OwnedMorsel::jobId_);
  };
  // Phase 1, even split: admit sessions still below the base share. Each
  // pass scans the pending queue once; the queue stays short in practice
  // (admission fills every free share eagerly), so a per-session index is
  // future work for proven load.
  while (totalOutstanding_ < max && !pendingAdmission_.empty()) {
    // Lowest jobId among the sessions still below the base share: sessions
    // at or above the share sort after every below-share session, so a
    // single `min_element` pass replaces the hand-written best-tracking loop
    // with identical selection.
    auto best = ql::ranges::min_element(
        pendingAdmission_, std::less<>{},
        [this, share](const OwnedMorsel& morsel) {
          return std::pair(committedOutstandingUnsafe(morsel.jobId_) >= share,
                           morsel.jobId_);
        });
    if (best == pendingAdmission_.end() ||
        committedOutstandingUnsafe(best->jobId_) >= share) {
      break;
    }
    admitIt(best);
  }
  // Phase 2, remainder-oldest: `max / live` truncates, so an indivisible
  // capacity leaves remainder slots that no below-share session can claim.
  // Hand them to the oldest waiting sessions instead of stranding them.
  while (totalOutstanding_ < max && !pendingAdmission_.empty()) {
    admitIt(oldestIt());
  }
  return readyToPost;
}

void ElasticExportScheduler::runPostedMorsel(OwnedMorsel morsel) {
  if (stopping_.load(std::memory_order_relaxed) ||
      !isHelperAdmissionEligibleUnsafe()) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(queueMutex_);
    auto it = submissionEpochs_.find({morsel.jobId_, morsel.morselIndex_});
    AD_CORRECTNESS_CHECK(it != submissionEpochs_.end(),
                         "Execution without tracked morsel identity");
    // Demand may have resubmitted this identity while its one closure was
    // deferred. Use the latest requested epoch, then unlock before leases or
    // job-state callbacks, which may themselves enqueue more work.
    morsel.submissionEpoch_ = it->second;
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
    absl::Cleanup releaseHelper{
        [&] { targetJobState->onHelperLeaseReleased(leaseEpoch); }};
    targetJobState->executeHelperTask(targetMorselIndex, leaseEpoch);
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
        if (stopping_.load(std::memory_order_relaxed)) {
          // Shutdown with work still queued but helpers ineligible: never
          // start new work here, otherwise the worker spins on the wait
          // predicate (which `stopping_` keeps true) forever and `shutdown()`
          // hangs while joining it.
          break;
        }
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
        try {
          targetJobState->executeHelperTask(targetMorselIndex, leaseEpoch);
        } catch (...) {
          // An exception must never escape the worker thread: that would call
          // `std::terminate`. `executeHelperTask` converts a task failure
          // into a terminal `Failed` slot state (storing the exception and
          // notifying waiters) before rethrowing, so the release below still
          // runs and `consumeNextResult` rethrows the original failure.
        }
        targetJobState->onHelperLeaseReleased(leaseEpoch);
      }
    }
  }
}

}  // namespace ad_utility::export_v2
