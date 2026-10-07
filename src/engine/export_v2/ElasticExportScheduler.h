// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <absl/functional/any_invocable.h>

#include <algorithm>
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
  PrimaryOnly,  // Only primary coordinator executes morsels (helpers disabled
                // or foreground queries > 1)
  HelpersEligible,  // Helpers may be leased to execute morsels in parallel
  Revoking,  // Foreground load arrived while helpers were active; waiting for
             // active leases to drain
  Closed     // Session finished or cancelled; no new work accepted
};

/// How the scheduler shares the `m` pool threads among export sessions.
///
/// `Exclusive` (the original policy): a session may use helpers only while at
/// most `maxForegroundQueriesForHelperAdmission()` queries (default 1, i.e.
/// the export itself) are registered. Any further query revokes all helpers
/// of every running session; they come back when the count drops again.
///
/// `Fair`: the `m` threads are split among the `n` running queries (see
/// `ElasticExportScheduler::fairThreadQuota`). Every session keeps at most
/// `quota - 1` helper threads besides its own coordinator thread. Each query
/// arrival or departure recomputes all quotas: sessions above their new quota
/// shrink at the next revocation checkpoint (unordered sessions, at most
/// `kRevocationCheckRows` rows) or after the running morsel (ordered sessions);
/// sessions below it post helpers for their pending morsels right away.
enum class HelperPolicy { Exclusive, Fair };

inline std::string_view toString(HelperPolicy policy) noexcept {
  return policy == HelperPolicy::Fair ? "fair" : "exclusive";
}

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

  // Fair policy only (see `HelperPolicy::Fair`).
  [[nodiscard]] virtual HelperPolicy helperPolicy() const noexcept = 0;
  [[nodiscard]] virtual bool isClosed() const noexcept = 0;
  [[nodiscard]] virtual size_t activeHelpers() const noexcept = 0;
  // Set the number of helper threads (excluding the coordinator) this session
  // may use; posts helper loops up to the new quota or triggers a checkpoint
  // shrink. Returns the previous quota.
  virtual size_t applyHelperQuota(size_t helpers) = 0;
  template <typename T, typename = std::enable_if_t<std::is_same_v<T, bool>>>
  size_t applyHelperQuota(T) = delete;
  // Body of one posted helper thread: runs pending morsels until none is left,
  // the session ends, or the session is above its quota.
  virtual void runHelperLoop() = 0;
};

struct OwnedMorsel {
  // `morselIndex_` of a fair-policy helper loop (not a single slot).
  static constexpr size_t kHelperLoop = static_cast<size_t>(-1);

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
  // Dedicated std::thread workers (unit tests).
  explicit ElasticExportScheduler(size_t numThreads = 0,
                                  size_t queueCapacity = 1024);
  // Live V2: post CPU morsels onto `Server::queryThreadPool_` so we do not
  // create a second pool. When another query is registered, admission
  // stops and in-flight tasks no-op; the coordinator serializes itself.
  // `poolSize` is the number of threads `poster` runs work on (`m` of the
  // fair policy).
  using WorkPoster = absl::AnyInvocable<void(absl::AnyInvocable<void()>)>;
  ElasticExportScheduler(WorkPoster poster, size_t poolSize,
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

  /// Dedicated std::thread workers. Zero when posting onto queryThreadPool_.
  [[nodiscard]] size_t workerThreadCount() const noexcept {
    return workers_.size();
  }

  [[nodiscard]] bool postsToQueryThreadPool() const noexcept {
    return static_cast<bool>(poster_);
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

  /// Policy for sessions created without an explicit one. Defaults to
  /// `Exclusive`; the server passes the runtime parameter
  /// `export-v2-helper-policy` per session instead.
  void setHelperPolicy(HelperPolicy policy) noexcept {
    helperPolicy_.store(policy, std::memory_order_seq_cst);
  }
  [[nodiscard]] HelperPolicy helperPolicy() const noexcept {
    return helperPolicy_.load(std::memory_order_seq_cst);
  }

  /// `m`: threads shared among sessions (pool size or dedicated workers).
  [[nodiscard]] size_t poolSize() const noexcept { return poolSize_; }

  /// Fair-policy quota rule. `m` pool threads, `n` running queries, `rank`
  /// is the 0-based start order among the running export sessions. A query
  /// gets `floor(m / n)` threads in total, the first `m mod n` queries one
  /// more; the coordinator thread of the session is one of them, so it may
  /// use `quota - 1` helpers. For `n >= m` every query gets at most one
  /// thread, i.e. no helpers. Returns the total (coordinator included).
  [[nodiscard]] static constexpr size_t fairThreadQuota(size_t m, size_t n,
                                                        size_t rank) noexcept {
    if (n == 0) {
      return m;
    }
    return m / n + (rank < m % n ? 1 : 0);
  }
  [[nodiscard]] static constexpr size_t fairHelperQuota(size_t m, size_t n,
                                                        size_t rank) noexcept {
    const size_t total = fairThreadQuota(m, n, rank);
    return total > 0 ? total - 1 : 0;
  }

  /// Recompute and apply the fair quotas of all live fair-policy sessions.
  /// Called on every query start/end and session creation.
  void rebalanceFairQuotas();

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

  /// Create a typed `ExportWorkSession`.
  template <typename ResultType = std::string>
  ExportWorkSession<ResultType> createSession(
      std::optional<HelperPolicy> policy = std::nullopt);

 private:
  void workerLoop();
  void runPostedMorsel(OwnedMorsel morsel);
  // Run one admitted morsel under an acquired lease. Shared by `workerLoop`
  // and `runPostedMorsel`; swallows the task's exception (already stored in
  // the slot) so that no helper thread terminates.
  static void runLeasedHelperTask(ExportJobStateBase* targetJobState,
                                  size_t targetMorselIndex,
                                  uint64_t submissionEpoch,
                                  uint64_t leaseEpoch);
  [[nodiscard]] bool isHelperAdmissionEligibleUnsafe() const noexcept;
  // Fair helper loops limit themselves by quota and are always admitted.
  [[nodiscard]] bool isAdmissibleUnsafe(
      const OwnedMorsel& morsel) const noexcept;

  WorkPoster poster_;
  size_t poolSize_{0};
  const size_t maxQueueCapacity_;
  std::atomic<HelperPolicy> helperPolicy_{HelperPolicy::Exclusive};
  // Serializes `rebalanceFairQuotas` so quotas are applied in event order.
  std::mutex rebalanceMutex_;
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
    // Set once the coordinator hands the result out. Unordered sessions emit
    // whichever morsel completes first, so consumption is tracked per slot
    // rather than by position.
    bool consumed_{false};
    absl::AnyInvocable<ResultType()> task_;
    std::optional<ResultType> result_;
    // Captured failure of `task_`: rethrown by `consumeNextResult` so a
    // throwing morsel can neither strand its slot in `Running` nor hang the
    // consumer forever (see `executeHelperTask`).
    std::exception_ptr error_;
    MorselProfile profile_;
  };

  ExportJobState(uint64_t jobId, ElasticExportScheduler* scheduler,
                 uint64_t initialEpoch, SessionState initialState,
                 HelperPolicy policy = HelperPolicy::Exclusive)
      : jobId_{jobId},
        scheduler_{scheduler},
        policy_{policy},
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

  [[nodiscard]] size_t activeHelpers() const noexcept override {
    return activeHelpers_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] HelperPolicy helperPolicy() const noexcept override {
    return policy_;
  }

  [[nodiscard]] bool isClosed() const noexcept override {
    return closed_.load(std::memory_order_relaxed);
  }

  // Fair policy: current helper quota and posted helper loops (started or
  // still queued in the pool).
  [[nodiscard]] size_t helperQuota() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return helperQuota_;
  }
  [[nodiscard]] size_t postedHelpers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return helpersPosted_;
  }

  size_t applyHelperQuota(size_t helpers) override {
    AD_CONTRACT_CHECK(policy_ == HelperPolicy::Fair);
    size_t previous = 0;
    size_t toPost = 0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      previous = helperQuota_;
      if (closed_ || cancelled_) {
        return previous;
      }
      helperQuota_ = helpers;
      if (helpersPosted_ > helperQuota_) {
        // Shrink: running unordered morsels compare the epoch at every
        // checkpoint, hand their unprocessed tail back as a new pending
        // slot and return; the surplus helper loops then exit (see
        // `runHelperLoop`). Ordered morsels finish first.
        currentEpoch_.fetch_add(1, std::memory_order_release);
        state_.store(SessionState::Revoking, std::memory_order_release);
      } else {
        state_.store(helperQuota_ > 0 ? SessionState::HelpersEligible
                                      : SessionState::PrimaryOnly,
                     std::memory_order_relaxed);
        toPost = reserveHelperLoopsUnsafe();
      }
      cv_.notify_all();
    }
    postHelperLoops(toPost);
    return previous;
  }

  void runHelperLoop() override {
    AD_CONTRACT_CHECK(policy_ == HelperPolicy::Fair);
    onHelperLeaseAcquired(currentEpoch());
    while (true) {
      size_t index = 0;
      absl::AnyInvocable<ResultType()> task;
      auto startWall = std::chrono::steady_clock::now();
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (cancelled_ || closed_ || helpersPosted_ > helperQuota_ ||
            !claimNextPendingUnsafe(&index)) {
          --helpersPosted_;
          if (helpersPosted_ <= helperQuota_ &&
              state_.load(std::memory_order_relaxed) ==
                  SessionState::Revoking) {
            state_.store(helperQuota_ > 0 ? SessionState::HelpersEligible
                                          : SessionState::PrimaryOnly,
                         std::memory_order_relaxed);
          }
          break;
        }
        startSlotUnsafe(index, startWall, true);
        task = std::move(slots_[index].task_);
      }
      // A failure is stored in the slot and rethrown to the consumer.
      runClaimedTask(index, std::move(task), startWall);
    }
    onHelperLeaseReleased(currentEpoch());
  }

  void onDemandChanged(size_t activeForegroundQueries,
                       uint64_t newEpoch) override {
    if (policy_ == HelperPolicy::Fair) {
      // Fair sessions follow `applyHelperQuota` instead.
      return;
    }
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
      // Fair sessions leave `Revoking` in `runHelperLoop` once at quota.
      if (policy_ == HelperPolicy::Exclusive &&
          state_.load(std::memory_order_relaxed) == SessionState::Revoking) {
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
      startSlotUnsafe(morselIndex, startWall, true);
      task = std::move(slots_[morselIndex].task_);
    }

    if (const auto error = runClaimedTask(morselIndex, std::move(task), startWall)) {
      // Rethrowing lets `runLeasedHelperTask` keep its never-escape guarantee
      // while `consumeNextResult` observes the stored failure instead of
      // waiting on a `Running` slot forever.
      std::rethrow_exception(error);
    }
  }

  size_t submitMorsel(absl::AnyInvocable<ResultType()> task) {
    AD_CONTRACT_CHECK(task != nullptr, "Cannot submit null morsel task");
    size_t index = 0;
    AD_CONTRACT_CHECK(appendAndEnqueue(std::move(task), &index),
                      "Cannot submit morsel to closed or cancelled session");
    return index;
  }

  // Soft version of `submitMorsel` for tasks that resubmit themselves (e.g.
  // an abandoned remainder after revocation): reports `false` instead of
  // firing when the session is closed or cancelled. The caller must drop the
  // remainder on `false`; on cancellation the whole job is torn down anyway.
  bool trySubmitMorsel(absl::AnyInvocable<ResultType()> task) {
    if (task == nullptr) {
      return false;
    }
    size_t index = 0;
    return appendAndEnqueue(std::move(task), &index);
  }

  // Submission epoch for a task created now. Morsel tasks compare it against
  // the live epoch at checkpoints to notice revocation without locking.
  [[nodiscard]] uint64_t currentEpoch() const {
    return currentEpoch_.load(std::memory_order_relaxed);
  }

  // Row order is significant only when the query bounds the result
  // (LIMIT/OFFSET/export limit select a deterministic prefix). Unordered
  // sessions emit whichever morsel completes first, which removes
  // head-of-line blocking behind a slow morsel. Must be fixed before the
  // first consume.
  void setOrdered(bool ordered) {
    std::lock_guard<std::mutex> lock(mutex_);
    // `nextSlotToConsume_ == 0` alone does not catch unordered sessions:
    // `consumeNextResult()` never advances `nextSlotToConsume_` while
    // `ordered_` is false, so it stays 0 across any number of unordered
    // consumes. Check `consumedCount_` instead, which is updated on every
    // consume regardless of mode.
    AD_CONTRACT_CHECK(consumedCount_ == 0,
                      "Emission order must be fixed before consuming");
    ordered_ = ordered;
  }

  [[nodiscard]] bool hasMoreResults() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return consumedCount_ < slots_.size();
  }

  [[nodiscard]] size_t totalSlots() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return slots_.size();
  }

  [[nodiscard]] size_t consumedSlots() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return consumedCount_;
  }

  ResultType consumeNextResult() {
    size_t index = 0;
    absl::AnyInvocable<ResultType()> primaryTask;

    std::unique_lock<std::mutex> lock(mutex_);
    if (ordered_) {
      AD_CONTRACT_CHECK(nextSlotToConsume_ < slots_.size(),
                        "No more submitted morsels to consume");
      index = nextSlotToConsume_++;
    } else {
      // Completion order: a finished morsel first, else a pending one for
      // inline execution, else a running one to wait on in the shared
      // machine below. Anything else means nothing is consumable.
      size_t completed = slots_.size();
      size_t pending = slots_.size();
      size_t running = slots_.size();
      for (size_t i = 0; i < slots_.size(); ++i) {
        if (slots_[i].consumed_) {
          continue;
        }
        if (slots_[i].status_ == MorselStatus::Completed ||
            slots_[i].status_ == MorselStatus::Cancelled) {
          // Prioritize any terminal state: a `Cancelled` slot carries the
          // stored task failure, which the loop below rethrows. Without
          // this, a lone `Cancelled` slot would fall through to the
          // contract check instead of surfacing the original exception.
          completed = i;
          break;
        }
        if (slots_[i].status_ == MorselStatus::Pending &&
            pending == slots_.size()) {
          pending = i;
        }
        if (slots_[i].status_ == MorselStatus::Running &&
            running == slots_.size()) {
          running = i;
        }
      }
      if (completed != slots_.size()) {
        index = completed;
      } else if (pending != slots_.size()) {
        index = pending;
      } else if (running != slots_.size()) {
        index = running;
      }
      AD_CONTRACT_CHECK(index < slots_.size() && !slots_[index].consumed_,
                        "No more submitted morsels to consume");
    }

    while (true) {
      if (cancelled_) {
        AD_THROW("Export job cancelled while awaiting result slot " +
                 std::to_string(index));
      }

      if (slots_[index].status_ == MorselStatus::Completed) {
        AD_CORRECTNESS_CHECK(slots_[index].result_.has_value());
        slots_[index].consumed_ = true;
        ++consumedCount_;
        return std::move(*slots_[index].result_);
      }

      if (slots_[index].status_ == MorselStatus::Cancelled) {
        // A helper worker converted a task exception into this terminal
        // state (see `executeHelperTask`): surface the original failure
        // instead of hanging on a slot that will never complete.
        std::exception_ptr error = slots_[index].error_;
        lock.unlock();
        if (error) {
          std::rethrow_exception(error);
        }
        AD_THROW("Export job cancelled while awaiting result slot " +
                 std::to_string(index));
      }

      if (slots_[index].status_ == MorselStatus::Pending) {
        // Single-core fallback: execute directly on coordinator thread
        auto startWall = std::chrono::steady_clock::now();
        startSlotUnsafe(index, startWall, false);
        primaryTask = std::move(slots_[index].task_);

        lock.unlock();
        auto startCpu = getCpuDuration();
        try {
          ResultType result = primaryTask();
          auto endCpu = getCpuDuration();
          auto endWall = std::chrono::steady_clock::now();
          lock.lock();

          slots_[index].result_ = std::move(result);
          slots_[index].status_ = MorselStatus::Completed;
          slots_[index].profile_.completedAt_ = endWall;
          slots_[index].profile_.wallDuration_ = endWall - startWall;
          slots_[index].profile_.cpuDuration_ = endCpu - startCpu;
          slots_[index].profile_.finalStatus_ = MorselStatus::Completed;
          slots_[index].consumed_ = true;
          ++consumedCount_;
          cv_.notify_all();
          return std::move(*slots_[index].result_);
        } catch (...) {
          // Same terminal-state protocol as the helper path, so the morsel
          // profile never dangles in `Running`; the original exception
          // propagates directly to this synchronous caller.
          lock.lock();
          slots_[index].error_ = std::current_exception();
          slots_[index].status_ = MorselStatus::Cancelled;
          slots_[index].profile_.completedAt_ =
              std::chrono::steady_clock::now();
          slots_[index].profile_.wallDuration_ =
              slots_[index].profile_.completedAt_ - startWall;
          slots_[index].profile_.cpuDuration_ = getCpuDuration() - startCpu;
          slots_[index].profile_.finalStatus_ = MorselStatus::Cancelled;
          cv_.notify_all();
          lock.unlock();
          throw;
        }
      }

      if (slots_[index].status_ == MorselStatus::Running) {
        // Wait for a running helper worker to finish a CPU morsel. In
        // unordered mode completions broadcast on this cv, so wake on any
        // finished unconsumed slot and re-select: emitting whichever morsel
        // is ready avoids head-of-line blocking behind the selected one.
        // Ordered sessions preserve slot order and keep waiting. A
        // `Cancelled` wakeup means the worker stored a task failure (handled
        // above on the next loop iteration).
        cv_.wait(lock, [&] {
          return slots_[index].status_ == MorselStatus::Completed ||
                 slots_[index].status_ == MorselStatus::Cancelled ||
                 cancelled_.load(std::memory_order_relaxed) ||
                 (!ordered_ &&
                  std::any_of(
                      slots_.begin(), slots_.end(), [](const Slot& slot) {
                        return !slot.consumed_ &&
                               (slot.status_ == MorselStatus::Completed ||
                                slot.status_ == MorselStatus::Cancelled);
                      }));
        });
        if (!ordered_ && slots_[index].status_ != MorselStatus::Completed &&
            slots_[index].status_ != MorselStatus::Cancelled) {
          for (size_t i = 0; i < slots_.size(); ++i) {
            if (!slots_[i].consumed_ &&
                (slots_[i].status_ == MorselStatus::Completed ||
                 slots_[i].status_ == MorselStatus::Cancelled)) {
              index = i;
              break;
            }
          }
        }
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
    pendingCount_ = 0;
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
  // Append a Pending slot and offer it to helpers when eligible; false when
  // closed or cancelled. Shared core of `submitMorsel` (which fires) and
  // `trySubmitMorsel` (which reports).
  bool appendAndEnqueue(absl::AnyInvocable<ResultType()> task, size_t* index) {
    bool shouldEnqueue = false;
    uint64_t epochToSubmit = 0;
    size_t helperLoopsToPost = 0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (closed_ || cancelled_) {
        return false;
      }
      *index = slots_.size();
      Slot slot;
      slot.status_ = MorselStatus::Pending;
      slot.task_ = std::move(task);
      slot.profile_.morselIndex_ = *index;
      slot.profile_.submittedAt_ = std::chrono::steady_clock::now();
      slots_.push_back(std::move(slot));
      ++pendingCount_;
      if (policy_ == HelperPolicy::Fair) {
        helperLoopsToPost = reserveHelperLoopsUnsafe();
      } else {
        epochToSubmit = currentEpoch_.load(std::memory_order_relaxed);
        if (state_.load(std::memory_order_relaxed) ==
            SessionState::HelpersEligible) {
          shouldEnqueue = true;
        }
      }
    }
    postHelperLoops(helperLoopsToPost);
    if (shouldEnqueue) {
      scheduler_->enqueueMorsel(
          OwnedMorsel(this->shared_from_this(), jobId_, epochToSubmit, *index));
    }
    return true;
  }

  // Pending -> Running transition shared by the coordinator, slot helpers and
  // fair helper loops. Requires `mutex_`.
  void startSlotUnsafe(size_t index,
                       std::chrono::steady_clock::time_point startWall,
                       bool byHelper) {
    AD_CORRECTNESS_CHECK(slots_[index].status_ == MorselStatus::Pending);
    AD_CORRECTNESS_CHECK(pendingCount_ > 0);
    --pendingCount_;
    slots_[index].status_ = MorselStatus::Running;
    slots_[index].profile_.startedAt_ = startWall;
    slots_[index].profile_.queueDelay_ =
        startWall - slots_[index].profile_.submittedAt_;
    slots_[index].profile_.executedByHelper_ = byHelper;
  }

  // Run a claimed (Running) slot's task on a helper thread and store its
  // result, or its failure as a terminal `Cancelled` state; wakes the
  // consumer either way. Returns the failure, if any.
  std::exception_ptr runClaimedTask(
      size_t index, absl::AnyInvocable<ResultType()> task,
      std::chrono::steady_clock::time_point startWall) {
    auto startCpu = getCpuDuration();
    try {
      ResultType result = task();
      auto endCpu = getCpuDuration();
      auto endWall = std::chrono::steady_clock::now();
      std::lock_guard<std::mutex> lock(mutex_);
      slots_[index].result_ = std::move(result);
      slots_[index].status_ = MorselStatus::Completed;
      slots_[index].profile_.completedAt_ = endWall;
      slots_[index].profile_.wallDuration_ = endWall - startWall;
      slots_[index].profile_.cpuDuration_ = endCpu - startCpu;
      slots_[index].profile_.finalStatus_ = MorselStatus::Completed;
      cv_.notify_all();
      return nullptr;
    } catch (...) {
      std::lock_guard<std::mutex> lock(mutex_);
      slots_[index].error_ = std::current_exception();
      slots_[index].status_ = MorselStatus::Cancelled;
      slots_[index].profile_.completedAt_ = std::chrono::steady_clock::now();
      slots_[index].profile_.wallDuration_ =
          slots_[index].profile_.completedAt_ - startWall;
      slots_[index].profile_.cpuDuration_ = getCpuDuration() - startCpu;
      slots_[index].profile_.finalStatus_ = MorselStatus::Cancelled;
      cv_.notify_all();
      return slots_[index].error_;
    }
  }

  // Fair policy: claim the lowest pending slot. Slots never return to
  // `Pending` and new slots are appended, so the scan cursor only advances.
  // Requires `mutex_`.
  bool claimNextPendingUnsafe(size_t* index) {
    if (pendingCount_ == 0) {
      return false;
    }
    while (pendingCursor_ < slots_.size() &&
           slots_[pendingCursor_].status_ != MorselStatus::Pending) {
      ++pendingCursor_;
    }
    AD_CORRECTNESS_CHECK(pendingCursor_ < slots_.size());
    *index = pendingCursor_;
    return true;
  }

  // Fair policy: number of helper loops to post so that the posted loops
  // reach the quota, but no more than there are pending morsels. Counts them
  // as posted. Requires `mutex_`.
  size_t reserveHelperLoopsUnsafe() {
    if (closed_ || cancelled_ || helpersPosted_ >= helperQuota_) {
      return 0;
    }
    const size_t toPost =
        std::min(helperQuota_ - helpersPosted_, pendingCount_);
    helpersPosted_ += toPost;
    return toPost;
  }

  // Post reserved helper loops (outside `mutex_`). A loop the scheduler
  // refuses (shutdown) is un-reserved; remaining pending morsels will be
  // executed by the coordinator fallback (see `consumeNextResult`).
  void postHelperLoops(size_t count) {
    if (count == 0) {
      return;
    }
    auto self = this->shared_from_this();
    for (size_t i = 0; i < count; ++i) {
      if (!scheduler_->enqueueMorsel(OwnedMorsel(self, jobId_, currentEpoch(),
                                                 OwnedMorsel::kHelperLoop))) {
        std::lock_guard<std::mutex> lock(mutex_);
        --helpersPosted_;
      }
    }
  }

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
  const HelperPolicy policy_;
  std::atomic<SessionState> state_{SessionState::PrimaryOnly};
  std::atomic<uint64_t> currentEpoch_{1};
  std::atomic<size_t> activeHelpers_{0};
  std::atomic<bool> cancelled_{false};
  std::atomic<bool> closed_{false};

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::vector<Slot> slots_;
  size_t nextSlotToConsume_{0};
  // Number of slots with `consumed_ == true`. Kept alongside `slots_`
  // (updated wherever `consumed_` is set) so `hasMoreResults()` and
  // `consumedSlots()` are O(1) instead of scanning all slots on every call
  // of the `while (hasMoreResults()) consumeNextResult()` driver loop.
  size_t consumedCount_{0};
  // False when row order is semantically irrelevant (no LIMIT/OFFSET/export
  // limit): morsels emit in completion order instead of slot order.
  bool ordered_{true};
  // Slots in `Pending` state; maintained by `appendAndEnqueue`,
  // `startSlotUnsafe` and `cancel`.
  size_t pendingCount_{0};
  // Fair policy (all guarded by `mutex_`): no pending slot below this index.
  size_t pendingCursor_{0};
  // Helper threads (coordinator excluded) this session may use.
  size_t helperQuota_{0};
  // Helper loops posted and not yet exited (running or queued in the pool).
  size_t helpersPosted_{0};
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

  // Shared ownership for morsel tasks that resubmit themselves (abandoned
  // remainders). Keeps the state alive while any task can still observe it.
  [[nodiscard]] std::shared_ptr<ExportJobState<ResultType>> sharedState()
      const noexcept {
    return state_;
  }

  [[nodiscard]] SessionState state() const noexcept {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->state();
  }

  [[nodiscard]] size_t activeHelpers() const noexcept {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->activeHelpers();
  }

  [[nodiscard]] HelperPolicy helperPolicy() const noexcept {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->helperPolicy();
  }

  [[nodiscard]] size_t helperQuota() const {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->helperQuota();
  }

  [[nodiscard]] size_t postedHelpers() const {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->postedHelpers();
  }

  size_t submitMorsel(absl::AnyInvocable<ResultType()> task) {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->submitMorsel(std::move(task));
  }

  void setOrdered(bool ordered) {
    AD_CONTRACT_CHECK(state_ != nullptr);
    state_->setOrdered(ordered);
  }

  bool trySubmitMorsel(absl::AnyInvocable<ResultType()> task) {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->trySubmitMorsel(std::move(task));
  }

  [[nodiscard]] uint64_t currentEpoch() const {
    AD_CONTRACT_CHECK(state_ != nullptr);
    return state_->currentEpoch();
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
ExportWorkSession<ResultType> ElasticExportScheduler::createSession(
    std::optional<HelperPolicy> policy) {
  const HelperPolicy sessionPolicy = policy.value_or(helperPolicy());
  const uint64_t jId = nextJobId();
  const uint64_t epoch = demandEpoch();
  // A fair session starts without helpers; the rebalance below assigns its
  // quota before the first morsel is submitted.
  SessionState initialState =
      (sessionPolicy == HelperPolicy::Exclusive &&
       activeForegroundQueries() <= maxForegroundQueriesForHelperAdmission())
          ? SessionState::HelpersEligible
          : SessionState::PrimaryOnly;

  auto state = std::make_shared<ExportJobState<ResultType>>(
      jId, this, epoch, initialState, sessionPolicy);
  registerSession(state);
  if (sessionPolicy == HelperPolicy::Fair) {
    rebalanceFairQuotas();
  }
  return ExportWorkSession<ResultType>(std::move(state));
}

}  // namespace ad_utility::export_v2
