// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IOWAITSTATS_H
#define QLEVER_SRC_UTIL_IOWAITSTATS_H

#include <atomic>
#include <chrono>
#include <cstdint>

namespace ad_utility::io_wait_stats {

// Process-wide counters of the time that threads spend blocked on vocabulary
// I/O, for measurements (runtime parameter `export-io-wait-stats`, which sets
// `enabled`). They count all threads together, so they describe one request
// only while no other request runs. Off by default: then no clock is read.
inline std::atomic<bool> enabled{false};

// Wall time inside `io_uring_wait_cqes` (a thread waiting for completions of
// its own reads), and the number of such waits.
inline std::atomic<uint64_t> ioUringWaitNs{0};
inline std::atomic<uint64_t> ioUringWaits{0};
// Wall time a thread waits for a buffer-pool page that another thread is
// loading or evicting, and the number of such waits.
inline std::atomic<uint64_t> poolPageWaitNs{0};
inline std::atomic<uint64_t> poolPageWaits{0};

// Measures the lifetime of the object and adds it to `ns` and one to `count`,
// if the counters were enabled when it was created.
class ScopedWait {
 public:
  ScopedWait(std::atomic<uint64_t>& ns, std::atomic<uint64_t>& count)
      : ns_{ns}, count_{count}, on_{enabled.load(std::memory_order_relaxed)} {
    if (on_) {
      start_ = std::chrono::steady_clock::now();
    }
  }
  ~ScopedWait() {
    if (on_) {
      const auto elapsed = std::chrono::steady_clock::now() - start_;
      ns_.fetch_add(static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            elapsed)
                            .count()),
                    std::memory_order_relaxed);
      count_.fetch_add(1, std::memory_order_relaxed);
    }
  }
  ScopedWait(const ScopedWait&) = delete;
  ScopedWait& operator=(const ScopedWait&) = delete;

 private:
  std::atomic<uint64_t>& ns_;
  std::atomic<uint64_t>& count_;
  bool on_;
  std::chrono::steady_clock::time_point start_{};
};

// A copy of all counters, for differences between two points in time.
struct Snapshot {
  uint64_t ioUringWaitNs_ = 0;
  uint64_t ioUringWaits_ = 0;
  uint64_t poolPageWaitNs_ = 0;
  uint64_t poolPageWaits_ = 0;

  static Snapshot now() {
    return {ioUringWaitNs.load(), ioUringWaits.load(), poolPageWaitNs.load(),
            poolPageWaits.load()};
  }
  Snapshot operator-(const Snapshot& other) const {
    return {ioUringWaitNs_ - other.ioUringWaitNs_,
            ioUringWaits_ - other.ioUringWaits_,
            poolPageWaitNs_ - other.poolPageWaitNs_,
            poolPageWaits_ - other.poolPageWaits_};
  }
};

}  // namespace ad_utility::io_wait_stats

#endif  // QLEVER_SRC_UTIL_IOWAITSTATS_H
