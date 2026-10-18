// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BUFFERPOOLBUDGET_H
#define QLEVER_SRC_UTIL_BUFFERPOOLBUDGET_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "util/BufferPool.h"

namespace ad_utility {

// An elastic budget for `BufferPool`s.
//
// Purpose: a fixed pool budget is either too small (the pool loses against
// the OS page cache, which uses all free RAM) or too large (the pool's memory
// is anonymous memory of this process, which the kernel cannot reclaim
// without swap, so it starves everything else). The OS page cache can use all
// free RAM because the kernel shrinks it at once when memory is needed. This
// controller gives the pools the same elasticity, but keeps the decision with
// QLever: a background thread grows the budget while memory is free and
// shrinks it, with planned eviction in bounded steps, when free memory drops
// below a reserve or the kernel reports memory pressure (PSI).

// The tunables of the controller.
struct BufferPoolBudgetConfig {
  static constexpr size_t MiB = size_t{1} << 20;
  static constexpr size_t GiB = size_t{1} << 30;
  // The target never goes below this (unless the ceiling is lower), so the
  // pools do not thrash completely.
  size_t floorBytes = 256 * MiB;
  // Free memory to leave for everything else. 0 = the default
  // `max(256 MiB, 5 % of the RAM or of the cgroup's memory limit)`.
  size_t reserveBytes = 0;
  // How much the target grows per tick while memory is free.
  size_t stepBytes = GiB;
  // At most this much is evicted per tick (spread over all pools), so that
  // eviction happens in bounded chunks off the query path.
  size_t maxEvictBytesPerTick = 256 * MiB;
  // Memory pressure above this share of time (PSI `some avg10`, in percent)
  // shrinks the target.
  double pressureThreshold = 10.0;
  std::chrono::milliseconds period{100};
};

// Where the controller gets its information from. Every function may return
// `std::nullopt` if the information is not available; the policy then ignores
// that signal. Injectable, so that the policy can be tested without real
// memory pressure.
struct BufferPoolSignals {
  // Free memory for this process: `MemAvailable`, and inside a cgroup (v2)
  // also the headroom up to `memory.max`/`memory.high` (the smaller value
  // counts). The pools' own loaded pages are already counted as used.
  std::function<std::optional<size_t>()> availableBytes;
  // Memory pressure: the share of time some task stalled waiting for memory
  // (PSI `some avg10`, in percent), of the cgroup or else of the system.
  std::function<std::optional<double>()> pressurePercent;
  // The RAM this process may use at most (`MemTotal`, or a smaller cgroup
  // limit); used for the default reserve.
  std::function<std::optional<size_t>()> totalBytes;

  // The signals of this machine (`/proc/meminfo`, cgroup v2 files,
  // `/proc/pressure/memory`).
  static BufferPoolSignals system();
};

// The policy, as a pure function: the next target given the current one, the
// loaded bytes of the pools and the signals. Grows by `step` while
// `available > reserve + step` and there is no pressure; shrinks by the
// deficit (at least 10 %) when `available < reserve` or the pressure exceeds
// the threshold. Shrinking starts from `min(target, loaded)`: lowering a
// target that the pools do not use would free nothing. The result is always
// within `[min(floor, ceiling), ceiling]`.
size_t nextBufferPoolTarget(size_t target, size_t loaded, size_t ceiling,
                            size_t floor, size_t reserve, size_t step,
                            std::optional<size_t> available,
                            std::optional<double> pressurePercent,
                            double pressureThreshold);

// The background controller. One per process (`global()`); tests create their
// own instances with injected signals.
class BufferPoolBudgetController {
 public:
  BufferPoolBudgetController(BufferPoolSignals signals,
                             BufferPoolBudgetConfig config);
  // Stops the background thread (if it runs).
  ~BufferPoolBudgetController();

  BufferPoolBudgetController(const BufferPoolBudgetController&) = delete;
  BufferPoolBudgetController& operator=(const BufferPoolBudgetController&) =
      delete;

  // The controller of this process, with the system's signals. Never
  // destroyed (a pool may be unregistered during static destruction).
  static BufferPoolBudgetController& global();

  // While a `Registration` exists, its pools get their share of the target
  // (in proportion to the size of their files). The pools must outlive the
  // registration. The background thread runs while at least one registration
  // exists.
  class Registration {
   public:
    ~Registration();
    Registration(const Registration&) = delete;
    Registration& operator=(const Registration&) = delete;

   private:
    friend class BufferPoolBudgetController;
    Registration(BufferPoolBudgetController& controller, size_t id)
        : controller_{controller}, id_{id} {}
    BufferPoolBudgetController& controller_;
    size_t id_;
  };
  [[nodiscard]] std::unique_ptr<Registration> add(
      std::vector<BufferPool*> pools);

  // Optional explicit upper bound for the total target (0 = none) and the
  // reserve (0 = the default of the config). Cheap; may be called often.
  void setExplicitCeiling(size_t bytes) { explicitCeiling_.store(bytes); }
  void setReserve(size_t bytes) { reserveOverride_.store(bytes); }

  // The current total target (all pools together).
  size_t targetBytes() const { return target_.load(); }

  // Run one step of the background thread synchronously (for tests, with a
  // long `period` so that the thread itself rarely ticks in between).
  void tick();

 private:
  struct Group {
    size_t id_;
    std::vector<BufferPool*> pools_;
  };

  void remove(size_t id);
  void run(size_t generation);
  // `tick` with `mutex_` held.
  void tickLocked();
  size_t reserveBytes() const;

  BufferPoolSignals signals_;
  BufferPoolBudgetConfig config_;
  std::atomic<size_t> explicitCeiling_{0};
  std::atomic<size_t> reserveOverride_{0};
  std::atomic<size_t> target_;

  // Protects `groups_`, `nextId_`, `generation_` and the thread. Held during a
  // tick, so a registration cannot be removed (and its pools destroyed) while
  // the tick uses them.
  std::mutex mutex_;
  std::condition_variable wakeUp_;
  std::vector<Group> groups_;
  size_t nextId_ = 0;
  // Incremented when the last registration is removed: a thread whose
  // generation is outdated stops (a new registration may start a new thread
  // while the old one is still being joined).
  size_t generation_ = 0;
  std::thread thread_;

  // For rate-limited logging of target changes.
  size_t lastLoggedTarget_ = 0;
  std::chrono::steady_clock::time_point lastLog_{};
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BUFFERPOOLBUDGET_H
