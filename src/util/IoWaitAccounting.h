//  Copyright 2026, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_IOWAITACCOUNTING_H
#define QLEVER_SRC_UTIL_IOWAITACCOUNTING_H

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <mutex>
#include <vector>

namespace ad_utility::ioWait {

// Wall-clock accounting for the calls on which a query thread blocks waiting
// for storage: the positioned `pread` in `File::read` and the completion wait
// in `IoUringPolicy::drainAtLeast`.
//
// WHY THIS EXISTS. `cpu_s` (`utime + stime` from `/proc/<pid>/stat`) only
// ticks while a thread is scheduled on a CPU. A cold `pread` puts the thread
// in uninterruptible sleep, so the time it spends waiting is invisible to
// `cpu_s`, and `elapsed - cpu_s` only recovers it while `cpu_ratio <= 1`,
// which does not hold once several export threads run. `perf` off-CPU
// profiling would answer this directly, but needs `perf_event_paranoid <= 1`.
// `delayacct_blkio_ticks` needs `kernel.task_delayacct` to be enabled. This
// accounting depends on neither.
//
// COST. Two `clock_gettime(CLOCK_MONOTONIC)` calls per measured call, which
// are vDSO calls (~25 ns) as long as the system clocksource is `tsc`, plus a
// non-atomic add. Counters are `thread_local`, so no cache line is written by
// more than one core and the cost does not grow with thread count. Disabled by
// default; the enable flag is a relaxed atomic load of a value that does not
// change during a query, so the branch predicts perfectly.
//
// The same binary therefore measures both arms of an instrumentation-cost A/B:
// run it once with `measure-io-wait=false` and once with `=true`.

// Benchmark override. The benchmark harness has no way to append arguments to
// the server command line, so a driver cannot set the `measure-io-wait`
// runtime parameter for one arm of an A/B. Reading the environment instead
// keeps the two arms on one binary, which is the property that makes the
// instrumentation-cost measurement meaningful.
inline bool envOverride() {
  const char* value = std::getenv("QLEVER_MEASURE_IO_WAIT");
  return value != nullptr && value[0] == '1';
}

// Whether the instrumentation is active. Initialized from the environment and
// afterwards also settable from the `measure-io-wait` runtime parameter.
inline std::atomic<bool>& enabledFlag() {
  static std::atomic<bool> enabled{envOverride()};
  return enabled;
}
inline bool enabled() { return enabledFlag().load(std::memory_order_relaxed); }
// The environment override wins, so a per-query runtime-parameter update
// cannot switch the instrumentation off mid-benchmark.
inline void setEnabled(bool value) {
  enabledFlag().store(value || envOverride(), std::memory_order_relaxed);
}

// One call site's totals.
struct Counters {
  uint64_t nanos_ = 0;
  uint64_t calls_ = 0;
};

// Per-thread totals for the two blocking call sites.
struct ThreadCounters {
  Counters pread_;
  Counters ioUringWait_;
};

namespace detail {
// Registry of the live threads' counters, plus the totals of threads that have
// already exited. The mutex is taken once per thread, never per call.
struct Registry {
  std::mutex mutex_;
  std::vector<ThreadCounters*> live_;
  ThreadCounters finished_;
};
inline Registry& registry() {
  static Registry registry;
  return registry;
}

// Adds this thread's counters to the registry on first use and folds them into
// `finished_` when the thread exits, so no sample is lost.
struct ThreadRegistration {
  ThreadCounters counters_;
  ThreadRegistration() {
    auto& reg = registry();
    std::lock_guard lock{reg.mutex_};
    reg.live_.push_back(&counters_);
  }
  ~ThreadRegistration() {
    auto& reg = registry();
    std::lock_guard lock{reg.mutex_};
    reg.finished_.pread_.nanos_ += counters_.pread_.nanos_;
    reg.finished_.pread_.calls_ += counters_.pread_.calls_;
    reg.finished_.ioUringWait_.nanos_ += counters_.ioUringWait_.nanos_;
    reg.finished_.ioUringWait_.calls_ += counters_.ioUringWait_.calls_;
    std::erase(reg.live_, &counters_);
  }
};

// This thread's counters. `thread_local`, so the cache line is private to the
// owning core and the add needs no atomic.
inline ThreadCounters& threadCounters() {
  thread_local ThreadRegistration registration;
  return registration.counters_;
}

inline uint64_t nowNanos() {
  timespec time{};
  clock_gettime(CLOCK_MONOTONIC, &time);
  return static_cast<uint64_t>(time.tv_sec) * 1000000000ULL +
         static_cast<uint64_t>(time.tv_nsec);
}
}  // namespace detail

// Times the blocking call `callable` into `counters` when the instrumentation
// is enabled, and calls it directly otherwise. Returns whatever `callable`
// returns.
template <typename Selector, typename Callable>
decltype(auto) timed(Selector selector, Callable&& callable) {
  if (!enabled()) {
    return callable();
  }
  const uint64_t start = detail::nowNanos();
  // `Counters&` is resolved before the call so the reference is not held
  // across it any longer than necessary.
  Counters& counters = selector(detail::threadCounters());
  if constexpr (std::is_void_v<decltype(callable())>) {
    callable();
    counters.nanos_ += detail::nowNanos() - start;
    ++counters.calls_;
  } else {
    decltype(auto) result = callable();
    counters.nanos_ += detail::nowNanos() - start;
    ++counters.calls_;
    return result;
  }
}

// Selectors for the two instrumented call sites.
inline Counters& preadCounters(ThreadCounters& counters) {
  return counters.pread_;
}
inline Counters& ioUringWaitCounters(ThreadCounters& counters) {
  return counters.ioUringWait_;
}

// Totals over all threads, live and finished.
inline ThreadCounters total() {
  auto& reg = detail::registry();
  std::lock_guard lock{reg.mutex_};
  ThreadCounters total = reg.finished_;
  for (const ThreadCounters* counters : reg.live_) {
    total.pread_.nanos_ += counters->pread_.nanos_;
    total.pread_.calls_ += counters->pread_.calls_;
    total.ioUringWait_.nanos_ += counters->ioUringWait_.nanos_;
    total.ioUringWait_.calls_ += counters->ioUringWait_.calls_;
  }
  return total;
}

// Writes the totals to stderr when the process exits. The process starts fresh
// for each repetition and issues one query, so the process-lifetime totals are
// that query's totals plus the index load, which is identical across the arms
// of an A/B. Emitted unconditionally so that a run with the instrumentation
// disabled is visibly distinguishable from one where the line is missing.
struct ExitReporter {
  ~ExitReporter() {
    const ThreadCounters counters = total();
    std::fprintf(stderr,
                 "io-wait-accounting enabled=%d pread_calls=%llu "
                 "pread_wait_s=%.6f iouring_waits=%llu iouring_wait_s=%.6f\n",
                 static_cast<int>(enabled()),
                 static_cast<unsigned long long>(counters.pread_.calls_),
                 static_cast<double>(counters.pread_.nanos_) / 1e9,
                 static_cast<unsigned long long>(counters.ioUringWait_.calls_),
                 static_cast<double>(counters.ioUringWait_.nanos_) / 1e9);
  }
};
inline const ExitReporter& exitReporter() {
  static ExitReporter reporter;
  return reporter;
}

}  // namespace ad_utility::ioWait

#endif  // QLEVER_SRC_UTIL_IOWAITACCOUNTING_H
