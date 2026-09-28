// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_FIBERIOSTATS_H
#define QLEVER_SRC_UTIL_FIBERIOSTATS_H

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

// Process-wide counters of the `io_uring` waits and of the cooperative fiber
// scheduling (`FiberIoScheduler`). They exist to explain measurements (how
// often a fiber yielded, how often and how long the thread blocked in the
// kernel, how many batches were in flight when it did). They are compiled in
// only with the CMake option `FIBER_IO_STATS` (`QLEVER_FIBER_IO_STATS`);
// otherwise every function below is an empty inline function and
// `makeExportScope` returns `nullptr`, so the default build pays nothing.
namespace ad_utility::fiberIoStats {

enum class Counter : size_t {
  // `FiberIoScheduler::runAsFibers` calls with at least one body.
  FiberRuns,
  // Fibers created by `runAsFibers`.
  FiberBodies,
  // Cooperative waits (`waitForBatch`, `waitForFreeSlot`) inside a fiber.
  FiberWaits,
  // Sum over the cooperative waits of the number of fibers waiting at the
  // start of the wait (including the new one), i.e. the batches in flight on
  // this thread when a fiber starts to wait.
  WaitingFibersAtWaitSum,
  // `boost::this_fiber::yield` calls in the cooperative wait.
  Yields,
  // Last-resort parks (`drainOneCqe` from the cooperative wait).
  Parks,
  // Sum over the parks of the number of fibers waiting at the park.
  WaitingFibersAtParkSum,
  // Blocking waits (`IoUringPolicy::wait` on a plain thread).
  BlockingWaits,
  // Completions reaped without blocking (`io_uring_peek_cqe`).
  PeekReaps,
  // `drainOneCqe` calls that found a completion already posted.
  DrainsWithoutKernelWait,
  // `drainOneCqe` calls that had to wait in the kernel, and the wall time
  // (nanoseconds) spent in these waits.
  KernelWaits,
  KernelWaitNs,
  // Vocabulary IDs resolved by the CONSTRUCT batch evaluator (phase B).
  ResolvedIds,
  NumCounters
};

inline constexpr size_t kNumCounters =
    static_cast<size_t>(Counter::NumCounters);

using Snapshot = std::array<uint64_t, kNumCounters>;

#ifdef QLEVER_FIBER_IO_STATS
inline constexpr bool kEnabled = true;

namespace detail {
inline std::array<std::atomic<uint64_t>, kNumCounters>& counters() {
  static std::array<std::atomic<uint64_t>, kNumCounters> instance{};
  return instance;
}
}  // namespace detail

// Add `n` to `counter`.
inline void add(Counter counter, uint64_t n = 1) {
  detail::counters()[static_cast<size_t>(counter)].fetch_add(
      n, std::memory_order_relaxed);
}
#else
inline constexpr bool kEnabled = false;
inline void add(Counter, uint64_t = 1) {}
#endif

// The current values of all counters (all zero when compiled out).
Snapshot snapshot();

// `after - before`, element-wise.
Snapshot difference(const Snapshot& after, const Snapshot& before);

// One line `name=value ...` for all counters of `values`.
std::string format(const Snapshot& values);

// Logs the counters accumulated during its lifetime (one INFO line prefixed
// with `label`) when it is destroyed.
class ExportScope {
 public:
  explicit ExportScope(std::string_view label);
  ~ExportScope();
  ExportScope(const ExportScope&) = delete;
  ExportScope& operator=(const ExportScope&) = delete;

 private:
  std::string label_;
  Snapshot start_;
};

// A new `ExportScope` when the counters are compiled in, else `nullptr`.
std::shared_ptr<ExportScope> makeExportScope(std::string_view label);

}  // namespace ad_utility::fiberIoStats

#endif  // QLEVER_SRC_UTIL_FIBERIOSTATS_H
