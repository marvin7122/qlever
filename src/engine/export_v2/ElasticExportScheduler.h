// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <absl/functional/any_invocable.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

#include "util/Exception.h"
#include "util/http/websocket/QueryId.h"

namespace ad_utility::export_v2 {

// -----------------------------------------------------------------------------
// Lifecycle & Morsel Status Enums
// -----------------------------------------------------------------------------

/// State machine states for an export session.
enum class SessionState {
  PrimaryOnly,  // Only primary coordinator executes morsels (helpers disabled
                // or foreground queries > 1)
  HelpersEligible,  // Helpers may be leased to execute morsels in parallel
  Revoking,  // Foreground load arrived while helpers were active; waiting for
             // active leases to drain
  Closed     // Session finished or cancelled; no new work accepted
};

/// Status of an individual work morsel slot.
enum class MorselStatus {
  Pending,    // Work submitted, awaiting execution
  Running,    // Actively executing on helper thread or primary thread
  Completed,  // Execution completed successfully; result is stored in slot
  Cancelled   // Job or morsel was cancelled
};

// Convert enums to human-readable strings for logging and assertion
// diagnostics.
inline std::string_view toString(SessionState state) noexcept {
  switch (state) {
    case SessionState::PrimaryOnly:
      return "PrimaryOnly";
    case SessionState::HelpersEligible:
      return "HelpersEligible";
    case SessionState::Revoking:
      return "Revoking";
    case SessionState::Closed:
      return "Closed";
  }
  return "Unknown";
}

inline std::string_view toString(MorselStatus status) noexcept {
  switch (status) {
    case MorselStatus::Pending:
      return "Pending";
    case MorselStatus::Running:
      return "Running";
    case MorselStatus::Completed:
      return "Completed";
    case MorselStatus::Cancelled:
      return "Cancelled";
  }
  return "Unknown";
}

// -----------------------------------------------------------------------------
// Instrumentation & Profiling
// -----------------------------------------------------------------------------

struct MorselProfile {
  size_t morselIndex_{0};
  std::chrono::steady_clock::time_point submittedAt_{};
  std::chrono::steady_clock::time_point startedAt_{};
  std::chrono::steady_clock::time_point completedAt_{};
  std::chrono::nanoseconds queueDelay_{0};
  std::chrono::nanoseconds wallDuration_{0};
  std::chrono::nanoseconds cpuDuration_{0};
  bool executedByHelper_{false};
  MorselStatus finalStatus_{MorselStatus::Pending};
};

class ElasticExportScheduler;

// -----------------------------------------------------------------------------
// Internal Base Job State & Owned Morsel for Type-Erased Thread Pool Dispatch
// -----------------------------------------------------------------------------

class ExportJobStateBase {
 public:
  virtual ~ExportJobStateBase() = default;
  [[nodiscard]] virtual uint64_t jobId() const noexcept = 0;
  virtual void onDemandChanged(size_t activeForegroundQueries,
                               uint64_t newEpoch) = 0;
  virtual void onHelperLeaseAcquired(uint64_t leaseEpoch) = 0;
  virtual void onHelperLeaseReleased(uint64_t leaseEpoch) = 0;
  virtual void executeHelperTask(size_t morselIndex, uint64_t leaseEpoch) = 0;
  [[nodiscard]] virtual bool isCancelled() const noexcept = 0;
};

struct OwnedMorsel {
  std::shared_ptr<ExportJobStateBase> jobState_;
  uint64_t jobId_{0};
  uint64_t submissionEpoch_{0};
  size_t morselIndex_{0};

  OwnedMorsel(std::shared_ptr<ExportJobStateBase> jobState, uint64_t jobId,
              uint64_t submissionEpoch, size_t morselIndex)
      : jobState_{std::move(jobState)},
        jobId_{jobId},
        submissionEpoch_{submissionEpoch},
        morselIndex_{morselIndex} {
    AD_CONTRACT_CHECK(jobState_ != nullptr);
  }
};

// -----------------------------------------------------------------------------
// Forward declarations for Session and Scheduler
// -----------------------------------------------------------------------------

template <typename ResultType>
class ExportWorkSession;

// -----------------------------------------------------------------------------
// ElasticExportScheduler: Isolated Thread Pool & Concurrency Coordinator
// -----------------------------------------------------------------------------

class ElasticExportScheduler
    : public std::enable_shared_from_this<ElasticExportScheduler> {
 public:
  // Sole way to obtain a scheduler. Shared ownership is a structural
  // guarantee, not a convention: `attachToQueryRegistry` hands the
  // `QueryRegistry` callbacks a `weak_ptr`, so a callback that outlives the
  // scheduler observes expiry instead of dereferencing a dangling `this`.
  // A non-shared scheduler cannot be constructed, hence that weak reference
  // is never empty.
  /// Default helper work queue capacity used by `create()` and as the
  /// fallback for non-positive capacities.
  static constexpr size_t kDefaultQueueCapacity = 1024;

  [[nodiscard]] static std::shared_ptr<ElasticExportScheduler> create(
      size_t numThreads = 0, size_t queueCapacity = kDefaultQueueCapacity);
  ~ElasticExportScheduler();

  ElasticExportScheduler(const ElasticExportScheduler&) = delete;
  ElasticExportScheduler& operator=(const ElasticExportScheduler&) = delete;
  ElasticExportScheduler(ElasticExportScheduler&&) = delete;
  ElasticExportScheduler& operator=(ElasticExportScheduler&&) = delete;

  /// Hook for observing start of a foreground SPARQL query.
  void onForegroundQueryStarted();

  /// Hook for observing completion/termination of a foreground SPARQL query.
  void onForegroundQueryEnded();

  /// Attach non-intrusively to QueryRegistry lifecycle callbacks. The
  /// registered callbacks observe the scheduler through a `weak_ptr` and
  /// no-op after its destruction, so no destruction order between scheduler
  /// and registry can dangle them.
  void attachToQueryRegistry(ad_utility::websocket::QueryRegistry& registry);

  /// Number of active registered foreground SPARQL queries.
  [[nodiscard]] size_t activeForegroundQueries() const noexcept {
    return activeForegroundQueries_.load(std::memory_order_relaxed);
  }

  /// Current monotonic demand epoch.
  [[nodiscard]] uint64_t demandEpoch() const noexcept {
    return demandEpoch_.load(std::memory_order_relaxed);
  }

  /// Current number of helper threads actively executing morsels.
  [[nodiscard]] size_t activeHelperCount() const noexcept {
    return totalActiveHelpers_.load(std::memory_order_relaxed);
  }

  /// Total number of dedicated helper worker threads in this pool.
  [[nodiscard]] size_t workerThreadCount() const noexcept {
    return workers_.size();
  }

  /// Bounded capacity of the helper work queue.
  [[nodiscard]] size_t queueCapacity() const noexcept {
    return maxQueueCapacity_;
  }

  /// Set the maximum number of active queries allowed for helper admission.
  /// Defaults to 1 (i.e. only the export query itself is running).
  void setMaxForegroundQueriesForHelperAdmission(size_t count) noexcept {
    maxForegroundQueriesForHelperAdmission_.store(count,
                                                  std::memory_order_relaxed);
  }

  [[nodiscard]] size_t maxForegroundQueriesForHelperAdmission() const noexcept {
    return maxForegroundQueriesForHelperAdmission_.load(
        std::memory_order_relaxed);
  }

  /// Shut down the thread pool and join all worker threads.
  void shutdown();

  /// Enqueue an owned morsel to the helper pool (called internally by
  /// sessions). Returns false without blocking when helpers are not
  /// currently eligible (the morsel stays Pending for primary fallback) or
  /// during shutdown; a full queue with eligible helpers still blocks for
  /// backpressure. Every demand change wakes waiters so they re-check.
  bool enqueueMorsel(OwnedMorsel morsel);

  /// Register an active session state for demand change notifications.
  void registerSession(std::weak_ptr<ExportJobStateBase> sessionState);

  /// Internal generator for monotonic job identifiers.
  uint64_t nextJobId() noexcept {
    return nextJobId_.fetch_add(1, std::memory_order_relaxed);
  }

  /// Create a typed ExportWorkSession.
  template <typename ResultType = std::string>
  ExportWorkSession<ResultType> createSession();

 private:
  explicit ElasticExportScheduler(size_t numThreads, size_t queueCapacity);
  void workerLoop();
  // Helper-slot accounting of a worker: register a newly issued lease
  // identity, and retire it again. Releasing an identity that is not
  // outstanding is an internal error.
  void registerOutstandingLease(uint64_t leaseId);
  void onLeaseReleased(uint64_t leaseId);
  [[nodiscard]] bool isHelperAdmissionEligibleUnsafe() const noexcept;
  // Shared demand-change propagation: wake both scheduler condition
  // variables, prune expired sessions, and notify the live ones outside
  // the locks.
  void propagateDemandChange(size_t activeForegroundQueries, uint64_t newEpoch);

  const size_t maxQueueCapacity_;
  std::atomic<size_t> maxForegroundQueriesForHelperAdmission_{1};
  std::atomic<uint64_t> demandEpoch_{1};
  std::atomic<size_t> activeForegroundQueries_{0};
  std::atomic<uint64_t> nextJobId_{1};
  std::atomic<uint64_t> nextLeaseId_{1};
  std::atomic<size_t> totalActiveHelpers_{0};
  std::atomic<bool> stopping_{false};

  mutable std::mutex queueMutex_;
  std::condition_variable workAvailableCv_;
  std::condition_variable queueNotFullCv_;
  std::deque<OwnedMorsel> queue_;
  // Lease identities handed out but not yet released, guarded by
  // `queueMutex_`. Validates releases against stale or duplicate leases.
  std::unordered_set<uint64_t> outstandingLeaseIds_;

  mutable std::mutex sessionsMutex_;
  std::vector<std::weak_ptr<ExportJobStateBase>> sessions_;

  std::vector<std::thread> workers_;
};

// -----------------------------------------------------------------------------
// Typed Job State
// -----------------------------------------------------------------------------

template <typename ResultType>
class ExportJobState final
    : public ExportJobStateBase,
      public std::enable_shared_from_this<ExportJobState<ResultType>> {
 public:
  struct Slot {
    MorselStatus status_{MorselStatus::Pending};
    absl::AnyInvocable<ResultType()> task_;
    std::optional<ResultType> result_;
    std::exception_ptr exception_{nullptr};
    MorselProfile profile_;
  };

  ExportJobState(uint64_t jobId, ElasticExportScheduler* scheduler,
                 uint64_t initialEpoch, SessionState initialState)
      : jobId_{jobId},
        scheduler_{scheduler},
        state_{initialState},
        currentEpoch_{initialEpoch} {
    AD_CONTRACT_CHECK(scheduler_ != nullptr);
  }

  [[nodiscard]] uint64_t jobId() const noexcept override { return jobId_; }

  [[nodiscard]] bool isCancelled() const noexcept override {
    return cancelled_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] SessionState state() const noexcept {
    return state_.load(std::memory_order_acquire);
  }

  [[nodiscard]] size_t activeHelpers() const noexcept {
    return activeHelpers_.load(std::memory_order_relaxed);
  }

  void onDemandChanged(size_t activeForegroundQueries,
                       uint64_t newEpoch) override {
    std::vector<size_t> pendingIndicesToEnqueue;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (closed_ || cancelled_) {
        return;
      }
      size_t maxQueries = scheduler_->maxForegroundQueriesForHelperAdmission();
      if (activeForegroundQueries <= maxQueries) {
        // Foreground load is low; helpers are eligible
        currentEpoch_.store(newEpoch, std::memory_order_release);
        state_.store(SessionState::HelpersEligible, std::memory_order_release);
        // Collect pending slots to submit to helper pool. The scan only
        // covers slots from `nextSlotToConsume_` onward, so it is bounded
        // by the in-flight morsels of this session and runs only on
        // threshold-crossing demand changes.
        for (size_t i = nextSlotToConsume_; i < slots_.size(); ++i) {
          if (slots_[i].status_ == MorselStatus::Pending) {
            pendingIndicesToEnqueue.push_back(i);
          }
        }
      } else {
        // Foreground load exceeded threshold; revoke helpers
        currentEpoch_.store(newEpoch, std::memory_order_release);
        if (activeHelpers_.load(std::memory_order_relaxed) > 0) {
          state_.store(SessionState::Revoking, std::memory_order_release);
        } else {
          state_.store(SessionState::PrimaryOnly, std::memory_order_release);
        }
      }
      cv_.notify_all();
    }

    // Enqueue pending morsels outside the lock
    if (!pendingIndicesToEnqueue.empty()) {
      auto self = this->shared_from_this();
      for (size_t index : pendingIndicesToEnqueue) {
        scheduler_->enqueueMorsel(OwnedMorsel(self, jobId_, newEpoch, index));
      }
    }
  }

  // `leaseEpoch` is part of the `ExportJobStateBase` lease protocol (it
  // identifies the demand epoch the lease was granted in); accounting here
  // needs no per-epoch distinction, so it is retained for interface
  // stability rather than removed.
  void onHelperLeaseAcquired([[maybe_unused]] uint64_t leaseEpoch) override {
    activeHelpers_.fetch_add(1, std::memory_order_relaxed);
  }

  void onHelperLeaseReleased([[maybe_unused]] uint64_t leaseEpoch) override {
    size_t prev = activeHelpers_.fetch_sub(1, std::memory_order_relaxed);
    AD_CORRECTNESS_CHECK(prev > 0, "Underflow in activeHelpers_");
    if (prev == 1) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_.load(std::memory_order_acquire) == SessionState::Revoking) {
        state_.store(SessionState::PrimaryOnly, std::memory_order_release);
      }
      cv_.notify_all();
    }
  }

  void executeHelperTask(size_t morselIndex, uint64_t leaseEpoch) override {
    // Fast-path filter; either outcome is safe (proceeding re-validates
    // under `mutex_`, skipping leaves the morsel Pending for the primary).
    // Acquire loads pair with the release stores in `onDemandChanged`.
    if (cancelled_.load(std::memory_order_relaxed) ||
        currentEpoch_.load(std::memory_order_acquire) != leaseEpoch ||
        state_.load(std::memory_order_acquire) == SessionState::Revoking ||
        state_.load(std::memory_order_acquire) == SessionState::Closed) {
      return;
    }

    absl::AnyInvocable<ResultType()> task;
    auto startWall = std::chrono::steady_clock::now();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (morselIndex >= slots_.size() ||
          slots_[morselIndex].status_ != MorselStatus::Pending) {
        return;
      }
      // Decisive re-check under `mutex_`: any state change between the
      // fast-path filter above and this lock is caught here before the
      // slot is marked Running, so no stale morsel can execute. Acquire
      // loads pair with the release stores in `onDemandChanged` (the
      // mutex already orders them; acquire states the protocol).
      if (cancelled_ ||
          currentEpoch_.load(std::memory_order_acquire) != leaseEpoch ||
          state_.load(std::memory_order_acquire) == SessionState::Revoking ||
          state_.load(std::memory_order_acquire) == SessionState::Closed) {
        return;
      }
      slots_[morselIndex].status_ = MorselStatus::Running;
      slots_[morselIndex].profile_.startedAt_ = startWall;
      slots_[morselIndex].profile_.queueDelay_ =
          startWall - slots_[morselIndex].profile_.submittedAt_;
      slots_[morselIndex].profile_.executedByHelper_ = true;
      task = std::move(slots_[morselIndex].task_);
    }

    auto startCpu = getCpuDuration();
    std::optional<ResultType> result;
    std::exception_ptr exceptionPtr = nullptr;
    try {
      result = task();
    } catch (...) {
      exceptionPtr = std::current_exception();
    }
    auto endCpu = getCpuDuration();
    auto endWall = std::chrono::steady_clock::now();

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (exceptionPtr) {
        slots_[morselIndex].exception_ = std::move(exceptionPtr);
      } else {
        slots_[morselIndex].result_ = std::move(result);
      }
      slots_[morselIndex].status_ = MorselStatus::Completed;
      slots_[morselIndex].profile_.completedAt_ = endWall;
      slots_[morselIndex].profile_.wallDuration_ = endWall - startWall;
      slots_[morselIndex].profile_.cpuDuration_ = endCpu - startCpu;
      slots_[morselIndex].profile_.finalStatus_ = MorselStatus::Completed;
      cv_.notify_all();
    }
  }

  size_t submitMorsel(absl::AnyInvocable<ResultType()> task) {
    AD_CONTRACT_CHECK(task != nullptr, "Cannot submit null morsel task");
    size_t index = 0;
    bool shouldEnqueue = false;
    uint64_t epochToSubmit = 0;

    {
      std::lock_guard<std::mutex> lock(mutex_);
      AD_CONTRACT_CHECK(!closed_, "Cannot submit morsel to closed session");
      AD_CONTRACT_CHECK(!cancelled_,
                        "Cannot submit morsel to cancelled session");

      index = slots_.size();
      Slot slot;
      slot.status_ = MorselStatus::Pending;
      slot.task_ = std::move(task);
      slot.profile_.morselIndex_ = index;
      slot.profile_.submittedAt_ = std::chrono::steady_clock::now();
      slots_.push_back(std::move(slot));

      epochToSubmit = currentEpoch_.load(std::memory_order_acquire);
      if (state_.load(std::memory_order_acquire) ==
          SessionState::HelpersEligible) {
        shouldEnqueue = true;
      }
    }

    if (shouldEnqueue) {
      // A false return means helpers became ineligible (or shutdown
      // started) after the check above; the slot stays Pending and the
      // primary consumes it, so ignoring the result is the fallback.
      scheduler_->enqueueMorsel(
          OwnedMorsel(this->shared_from_this(), jobId_, epochToSubmit, index));
    }
    return index;
  }

  [[nodiscard]] bool hasMoreResults() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return nextSlotToConsume_ < slots_.size();
  }

  [[nodiscard]] size_t totalSlots() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return slots_.size();
  }

  [[nodiscard]] size_t consumedSlots() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return nextSlotToConsume_;
  }

  ResultType consumeNextResult() {
    size_t index = 0;
    absl::AnyInvocable<ResultType()> primaryTask;

    std::unique_lock<std::mutex> lock(mutex_);
    AD_CONTRACT_CHECK(nextSlotToConsume_ < slots_.size(),
                      "No more submitted morsels to consume");
    index = nextSlotToConsume_++;

    while (true) {
      if (cancelled_) {
        AD_THROW("Export job cancelled while awaiting result slot " +
                 std::to_string(index));
      }

      if (slots_[index].status_ == MorselStatus::Completed) {
        if (slots_[index].exception_) {
          std::rethrow_exception(slots_[index].exception_);
        }
        AD_CORRECTNESS_CHECK(slots_[index].result_.has_value());
        return std::move(*slots_[index].result_);
      }

      if (slots_[index].status_ == MorselStatus::Pending) {
        // Single-core fallback: execute directly on coordinator thread
        slots_[index].status_ = MorselStatus::Running;
        auto startWall = std::chrono::steady_clock::now();
        slots_[index].profile_.startedAt_ = startWall;
        slots_[index].profile_.queueDelay_ =
            startWall - slots_[index].profile_.submittedAt_;
        slots_[index].profile_.executedByHelper_ = false;
        primaryTask = std::move(slots_[index].task_);

        lock.unlock();
        auto startCpu = getCpuDuration();
        std::optional<ResultType> result;
        std::exception_ptr exceptionPtr = nullptr;
        try {
          result = primaryTask();
        } catch (...) {
          exceptionPtr = std::current_exception();
        }
        auto endCpu = getCpuDuration();
        auto endWall = std::chrono::steady_clock::now();
        lock.lock();

        if (exceptionPtr) {
          slots_[index].exception_ = std::move(exceptionPtr);
        } else {
          slots_[index].result_ = std::move(result);
        }
        slots_[index].status_ = MorselStatus::Completed;
        slots_[index].profile_.completedAt_ = endWall;
        slots_[index].profile_.wallDuration_ = endWall - startWall;
        slots_[index].profile_.cpuDuration_ = endCpu - startCpu;
        slots_[index].profile_.finalStatus_ = MorselStatus::Completed;
        cv_.notify_all();
        if (slots_[index].exception_) {
          std::rethrow_exception(slots_[index].exception_);
        }
        return std::move(*slots_[index].result_);
      }

      if (slots_[index].status_ == MorselStatus::Running) {
        // Wait for running helper worker to finish CPU morsel
        cv_.wait(lock, [&] {
          return slots_[index].status_ == MorselStatus::Completed ||
                 cancelled_.load(std::memory_order_relaxed);
        });
      }
    }
  }

  std::vector<ResultType> drainRemainingResults() {
    std::vector<ResultType> results;
    while (hasMoreResults()) {
      results.push_back(consumeNextResult());
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      closed_ = true;
      state_.store(SessionState::Closed, std::memory_order_relaxed);
    }
    return results;
  }

  /// Abort the job: Pending slots are discarded, waiters and consumers
  /// observe `cancelled_` at the next consume boundary. A morsel already
  /// Running (marked under `mutex_` by exactly one executor) runs to
  /// completion; its result is stored consistently, never duplicated.
  /// Helpers check cancellation before and during execution and exit early.
  void cancel() {
    std::lock_guard<std::mutex> lock(mutex_);
    cancelled_ = true;
    closed_ = true;
    state_.store(SessionState::Closed, std::memory_order_relaxed);
    for (auto& slot : slots_) {
      if (slot.status_ == MorselStatus::Pending) {
        slot.status_ = MorselStatus::Cancelled;
        slot.profile_.finalStatus_ = MorselStatus::Cancelled;
      }
    }
    cv_.notify_all();
  }

  /// Graceful completion: no new submissions, but Pending slots remain
  /// consumable by the primary (unlike `cancel()`, nothing is discarded).
  /// Helpers observe `Closed` on their next check and exit early.
  void close() {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    state_.store(SessionState::Closed, std::memory_order_relaxed);
    cv_.notify_all();
  }

  [[nodiscard]] std::vector<MorselProfile> inspectMorselProfiles() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<MorselProfile> profiles;
    profiles.reserve(slots_.size());
    for (const auto& slot : slots_) {
      profiles.push_back(slot.profile_);
    }
    return profiles;
  }

 private:
  // Profiling-only per-thread CPU clock. Linux-only by design (other
  // platforms fall through to a zero duration, which degrades profiles
  // without affecting scheduling); revisit if non-Linux support is needed.
  static std::chrono::nanoseconds getCpuDuration() noexcept {
#if defined(__linux__)
    struct timespec ts;
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0) {
      return std::chrono::seconds(ts.tv_sec) +
             std::chrono::nanoseconds(ts.tv_nsec);
    }
#endif
    return std::chrono::nanoseconds(0);
  }

  const uint64_t jobId_;
  ElasticExportScheduler* const scheduler_;
  std::atomic<SessionState> state_{SessionState::PrimaryOnly};
  std::atomic<uint64_t> currentEpoch_{1};
  std::atomic<size_t> activeHelpers_{0};
  std::atomic<bool> cancelled_{false};
  std::atomic<bool> closed_{false};

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::vector<Slot> slots_;
  size_t nextSlotToConsume_{0};
};

// -----------------------------------------------------------------------------
// ExportWorkSession: Move-Only RAII Export Session Handle
// -----------------------------------------------------------------------------

template <typename ResultType>
class ExportWorkSession {
 public:
  explicit ExportWorkSession(
      std::shared_ptr<ExportJobState<ResultType>> state) noexcept
      : state_{std::move(state)} {}

  ~ExportWorkSession() {
    if (state_ && !state_->isCancelled()) {
      state_->close();
    }
  }

  ExportWorkSession(ExportWorkSession&&) noexcept = default;
  ExportWorkSession& operator=(ExportWorkSession&&) noexcept = default;

  ExportWorkSession(const ExportWorkSession&) = delete;
  ExportWorkSession& operator=(const ExportWorkSession&) = delete;

  [[nodiscard]] uint64_t jobId() const noexcept {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->jobId();
  }

  [[nodiscard]] SessionState state() const noexcept {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->state();
  }

  [[nodiscard]] size_t activeHelpers() const noexcept {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->activeHelpers();
  }

  size_t submitMorsel(absl::AnyInvocable<ResultType()> task) {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->submitMorsel(std::move(task));
  }

  [[nodiscard]] bool hasMoreResults() const {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->hasMoreResults();
  }

  [[nodiscard]] size_t totalSlots() const {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->totalSlots();
  }

  [[nodiscard]] size_t consumedSlots() const {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->consumedSlots();
  }

  ResultType consumeNextResult() {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->consumeNextResult();
  }

  std::vector<ResultType> drainRemainingResults() {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->drainRemainingResults();
  }

  void cancel() {
    if (state_) {
      state_->cancel();
    }
  }

  void close() {
    if (state_) {
      state_->close();
    }
  }

  [[nodiscard]] std::vector<MorselProfile> inspectMorselProfiles() const {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->inspectMorselProfiles();
  }

  [[nodiscard]] const std::shared_ptr<ExportJobState<ResultType>>& stateHandle()
      const noexcept {
    return state_;
  }

 private:
  std::shared_ptr<ExportJobState<ResultType>> state_;
};

// -----------------------------------------------------------------------------
// Template implementation of createSession
// -----------------------------------------------------------------------------

template <typename ResultType>
ExportWorkSession<ResultType> ElasticExportScheduler::createSession() {
  uint64_t jId = nextJobId();
  uint64_t epoch = demandEpoch();
  SessionState initialState =
      (activeForegroundQueries() <= maxForegroundQueriesForHelperAdmission())
          ? SessionState::HelpersEligible
          : SessionState::PrimaryOnly;

  auto state = std::make_shared<ExportJobState<ResultType>>(jId, this, epoch,
                                                            initialState);
  registerSession(state);
  return ExportWorkSession<ResultType>(std::move(state));
}

}  // namespace ad_utility::export_v2
