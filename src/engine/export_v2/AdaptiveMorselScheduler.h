// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <vector>

namespace ql::engine::export_v2 {

// _____________________________________________________________________________
// Adaptive Morsel-Driven Scheduler:
// Dynamically adjusts chunk sizes between MIN_MORSEL (4K) and MAX_MORSEL (64K)
// based on thread latency telemetry and cache miss observations.
class AdaptiveMorselScheduler {
 public:
  static constexpr size_t MIN_MORSEL_ROWS = 4096;
  static constexpr size_t DEFAULT_MORSEL_ROWS = 16384;
  static constexpr size_t MAX_MORSEL_ROWS = 65536;

  static constexpr std::chrono::microseconds TARGET_MORSEL_LATENCY{2000};  // 2 ms

 private:
  std::atomic<size_t> currentMorselRows_{DEFAULT_MORSEL_ROWS};
  std::atomic<uint64_t> totalRowsProcessed_{0};

 public:
  [[nodiscard]] size_t getCurrentMorselSize() const noexcept {
    return currentMorselRows_.load(std::memory_order_relaxed);
  }

  // Telemetry feedback: adjust morsel size according to execution duration
  void recordMorselTelemetry(size_t rowsProcessed, std::chrono::microseconds duration) noexcept {
    totalRowsProcessed_.fetch_add(rowsProcessed, std::memory_order_relaxed);

    if (duration.count() == 0) {
      return;
    }

    size_t current = currentMorselRows_.load(std::memory_order_relaxed);
    if (duration > TARGET_MORSEL_LATENCY * 2) {
      // Morsel took too long -> shrink morsel to reduce preemption latency
      size_t newSize = std::max(MIN_MORSEL_ROWS, current / 2);
      currentMorselRows_.store(newSize, std::memory_order_relaxed);
    } else if (duration < TARGET_MORSEL_LATENCY / 2 && current < MAX_MORSEL_ROWS) {
      // Morsel finished very fast -> grow morsel to amortize scheduling overhead
      size_t newSize = std::min(MAX_MORSEL_ROWS, current * 2);
      currentMorselRows_.store(newSize, std::memory_order_relaxed);
    }
  }

  [[nodiscard]] uint64_t getTotalRowsProcessed() const noexcept {
    return totalRowsProcessed_.load(std::memory_order_relaxed);
  }
};

}  // namespace ql::engine::export_v2
