// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Marvin Stoetzel <marvin.stoetzel@mailbox.org>

#pragma once

#include <absl/functional/any_invocable.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "util/Exception.h"
#include "util/http/websocket/QueryId.h"

namespace ad_utility::export_v2 {

// -----------------------------------------------------------------------------
// Lifecycle & Morsel Status Enums
// -----------------------------------------------------------------------------

/// State machine states for an export session.
enum class SessionState {
  PrimaryOnly,  // Only the primary coordinator executes morsels (helpers disabled
                // or active foreground queries exceed the admission threshold)
  HelpersEligible,  // Helpers may be leased to execute morsels in parallel
  Revoking,  // Foreground load arrived while helpers were active; waiting for
             // active leases to drain
  Closed     // Session finished or cancelled; no new work accepted
};

/// Status of an individual work morsel slot.
enum class MorselStatus {
  Pending,    // Work submitted, awaiting execution
  Running,    // Actively executing on helper thread or primary thread
  Completed,  // Execution finished; a result or exception is stored in the slot
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
// ExportWorkLease: Opaque Move-Only RAII Lease Handle
// -----------------------------------------------------------------------------

/// Move-only RAII handle representing a leased helper execution slot.
class ExportWorkLease {
 public:
  ExportWorkLease() noexcept = default;
  ExportWorkLease(ElasticExportScheduler* scheduler, uint64_t epoch,
                  uint64_t jobId, uint64_t leaseId) noexcept;
  ~ExportWorkLease();

  ExportWorkLease(ExportWorkLease&& other) noexcept;
  ExportWorkLease& operator=(ExportWorkLease&& other) noexcept;

  ExportWorkLease(const ExportWorkLease&) = delete;
  ExportWorkLease& operator=(const ExportWorkLease&) = delete;

  [[nodiscard]] bool isValid() const noexcept { return active_; }
  [[nodiscard]] uint64_t epoch() const noexcept { return epoch_; }
  [[nodiscard]] uint64_t jobId() const noexcept { return jobId_; }
  [[nodiscard]] uint64_t leaseId() const noexcept { return leaseId_; }

  void release() noexcept;

 private:
  ElasticExportScheduler* scheduler_{nullptr};
  uint64_t epoch_{0};
  uint64_t jobId_{0};
  uint64_t leaseId_{0};
  bool active_{false};
};

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

class ElasticExportScheduler {
 public:
  explicit ElasticExportScheduler(size_t numThreads = 0,
                                  size_t queueCapacity = 1024);
  ~ElasticExportScheduler();

  ElasticExportScheduler(const ElasticExportScheduler&) = delete;
  ElasticExportScheduler& operator=(const ElasticExportScheduler&) = delete;
  ElasticExportScheduler(ElasticExportScheduler&&) = delete;
  ElasticExportScheduler& operator=(ElasticExportScheduler&&) = delete;

  /// Hook for observing start of a foreground SPARQL query.
  void onForegroundQueryStarted();

  /// Hook for observing completion/termination of a foreground SPARQL query.
  void onForegroundQueryEnded();

  /// Attach non-intrusively to QueryRegistry lifecycle callbacks.
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
  /// sessions).
  bool enqueueMorsel(OwnedMorsel morsel);

  /// Register an active session state for demand change notifications.
  void registerSession(std::weak_ptr<ExportJobStateBase> sessionState);

  /// Internal callback when an ExportWorkLease is released.
  void onLeaseReleased(uint64_t epoch, uint64_t jobId) noexcept;

  /// Internal generator for monotonic job identifiers.
  uint64_t nextJobId() noexcept {
    return nextJobId_.fetch_add(1, std::memory_order_relaxed);
  }

  

 private:
  void workerLoop();
  [[nodiscard]] bool isHelperAdmissionEligibleUnsafe() const noexcept;

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
    return state_.load(std::memory_order_relaxed);
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
        currentEpoch_.store(newEpoch, std::memory_order_relaxed);
        state_.store(SessionState::HelpersEligible, std::memory_order_relaxed);
        // Collect pending slots to submit to helper pool
        for (size_t i = nextSlotToConsume_; i < slots_.size(); ++i) {
          if (slots_[i].status_ == MorselStatus::Pending) {
            pendingIndicesToEnqueue.push_back(i);
          }
        }
      } else {
        // Foreground load exceeded threshold; revoke helpers
        currentEpoch_.store(newEpoch, std::memory_order_relaxed);
        if (activeHelpers_.load(std::memory_order_relaxed) > 0) {
          state_.store(SessionState::Revoking, std::memory_order_relaxed);
        } else {
          state_.store(SessionState::PrimaryOnly, std::memory_order_relaxed);
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

  void onHelperLeaseAcquired([[maybe_unused]] uint64_t leaseEpoch) override {
    activeHelpers_.fetch_add(1, std::memory_order_relaxed);
  }

  void onHelperLeaseReleased([[maybe_unused]] uint64_t leaseEpoch) override {
    size_t prev = activeHelpers_.fetch_sub(1, std::memory_order_relaxed);
    AD_CORRECTNESS_CHECK(prev > 0, "Underflow in activeHelpers_");
    if (prev == 1) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_.load(std::memory_order_relaxed) == SessionState::Revoking) {
        state_.store(SessionState::PrimaryOnly, std::memory_order_relaxed);
      }
      cv_.notify_all();
    }
  }

  void executeHelperTask(size_t morselIndex, uint64_t leaseEpoch) override {
    if (cancelled_.load(std::memory_order_relaxed) ||
        currentEpoch_.load(std::memory_order_relaxed) != leaseEpoch ||
        state_.load(std::memory_order_relaxed) == SessionState::Revoking ||
        state_.load(std::memory_order_relaxed) == SessionState::Closed) {
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
      if (cancelled_ ||
          currentEpoch_.load(std::memory_order_relaxed) != leaseEpoch ||
          state_.load(std::memory_order_relaxed) == SessionState::Revoking ||
          state_.load(std::memory_order_relaxed) == SessionState::Closed) {
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

      epochToSubmit = currentEpoch_.load(std::memory_order_relaxed);
      if (state_.load(std::memory_order_relaxed) ==
          SessionState::HelpersEligible) {
        shouldEnqueue = true;
      }
    }

    if (shouldEnqueue) {
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
