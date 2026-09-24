// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IOWAITACCOUNTING_H
#define QLEVER_SRC_UTIL_IOWAITACCOUNTING_H

#include <dirent.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iterator>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace ad_utility::ioWait {

// Wall-clock accounting for the calls on which a query thread blocks waiting
// for storage: the positioned `pread` in `File::read`, the submission flush
// in `IoUringPolicy::addBatch`, and the completion wait in
// `IoUringPolicy::drainOneCqe`.
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
// relaxed load and store per counter (no read-modify-write). Counters are
// `thread_local`, so no cache line is written by more than one core and the
// cost does not grow with thread count. Disabled by default; the enable flag is
// a relaxed atomic load of a value that does not change during a query, so the
// branch predicts perfectly.
//
// Environment override for enabling the instrumentation independently of
// the runtime parameter.
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
// The environment override wins, so a runtime-parameter update cannot switch
// the instrumentation off while the process is running. Totals are shared
// across concurrent queries, which is fine as long as the flag is constant
// for the process lifetime, as it is in practice.
inline void setEnabled(bool value) {
  enabledFlag().store(value || envOverride(), std::memory_order_relaxed);
}

// A snapshot of one call site's totals.
struct Counters {
  uint64_t nanos_ = 0;
  uint64_t calls_ = 0;
};

// A snapshot of the totals for the three blocking call sites.
struct ThreadCounters {
  Counters pread_;
  Counters ioUringWait_;
  // `io_uring_submit` is a blocking call site too. With `flags = 0` on a
  // buffered file, a read that the kernel can service without punting is
  // performed inside `io_uring_enter`, so the thread blocks in submit and the
  // matching completion is already present when it is reaped. Without this
  // call site, that blocking time would be attributed to neither `pread` nor
  // the completion wait.
  Counters ioUringSubmit_;
};

// Add the totals in `part` to `sum`.
inline void addTo(ThreadCounters& sum, const ThreadCounters& part) {
  auto add = [](Counters& to, const Counters& from) {
    to.nanos_ += from.nanos_;
    to.calls_ += from.calls_;
  };
  add(sum.pread_, part.pread_);
  add(sum.ioUringWait_, part.ioUringWait_);
  add(sum.ioUringSubmit_, part.ioUringSubmit_);
}

// One call site's live counters. Only the owning thread writes them, so a
// relaxed load followed by a relaxed store suffices (no read-modify-write, and
// on x86-64 the same plain `mov` instructions as a non-atomic add). The atomics
// make the concurrent read in `total()` well-defined.
struct LiveCounters {
  std::atomic<uint64_t> nanos_{0};
  std::atomic<uint64_t> calls_{0};

  // Record one call that blocked for `nanos`. Must only be called by the
  // owning thread.
  void add(uint64_t nanos) {
    nanos_.store(nanos_.load(std::memory_order_relaxed) + nanos,
                 std::memory_order_relaxed);
    calls_.store(calls_.load(std::memory_order_relaxed) + 1,
                 std::memory_order_relaxed);
  }
  Counters snapshot() const {
    return {nanos_.load(std::memory_order_relaxed),
            calls_.load(std::memory_order_relaxed)};
  }
};

// One thread's live counters for the three blocking call sites.
struct LiveThreadCounters {
  LiveCounters pread_;
  LiveCounters ioUringWait_;
  LiveCounters ioUringSubmit_;

  ThreadCounters snapshot() const {
    return {pread_.snapshot(), ioUringWait_.snapshot(),
            ioUringSubmit_.snapshot()};
  }
};

namespace detail {
// Registry of the live threads' counters, plus the totals of threads that have
// already exited. The mutex is taken once per thread, never per call.
struct Registry {
  std::mutex mutex_;
  std::vector<const LiveThreadCounters*> live_;
  ThreadCounters finished_;
};
// Intentionally leaked: the detached reporter thread and the exit reporter may
// still read the registry during static destruction.
inline Registry& registry() {
  static Registry* registry = new Registry;
  return *registry;
}

// Add this thread's counters to the registry on first use and fold them into
// `finished_` when the thread exits, so no sample is lost.
struct ThreadRegistration {
  LiveThreadCounters counters_;

  ThreadRegistration() {
    auto& reg = registry();
    std::lock_guard lock{reg.mutex_};
    reg.live_.push_back(&counters_);
  }
  ~ThreadRegistration() {
    auto& reg = registry();
    std::lock_guard lock{reg.mutex_};
    addTo(reg.finished_, counters_.snapshot());
    reg.live_.erase(std::remove(reg.live_.begin(), reg.live_.end(), &counters_),
                    reg.live_.end());
  }
};

// Return this thread's counters. `thread_local`, so the cache line is written
// only by the owning core.
inline LiveThreadCounters& threadCounters() {
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

// Time the blocking call `callable` into the counters chosen by `selector` when
// the instrumentation is enabled, and call it directly otherwise. Return
// whatever `callable` returns.
template <typename Selector, typename Callable>
decltype(auto) timed(Selector selector, Callable&& callable) {
  if (!enabled()) {
    return callable();
  }
  // Resolved before the clock starts so one-time thread registration is not
  // counted as storage wait.
  LiveCounters& counters = selector(detail::threadCounters());
  const uint64_t start = detail::nowNanos();
  if constexpr (std::is_void_v<decltype(callable())>) {
    callable();
    counters.add(detail::nowNanos() - start);
  } else {
    auto result = callable();
    counters.add(detail::nowNanos() - start);
    return result;
  }
}

// Selectors for the three instrumented call sites.
inline LiveCounters& preadCounters(LiveThreadCounters& counters) {
  return counters.pread_;
}
inline LiveCounters& ioUringWaitCounters(LiveThreadCounters& counters) {
  return counters.ioUringWait_;
}
inline LiveCounters& ioUringSubmitCounters(LiveThreadCounters& counters) {
  return counters.ioUringSubmit_;
}

// Totals over all threads, live and finished.
inline ThreadCounters total() {
  auto& reg = detail::registry();
  std::lock_guard lock{reg.mutex_};
  ThreadCounters total = reg.finished_;
  for (const LiveThreadCounters* counters : reg.live_) {
    addTo(total, counters->snapshot());
  }
  return total;
}

// Kernel-side worker accounting.
//
// The vocabulary files are opened buffered and the ring is created with
// `flags = 0` (no SQPOLL, no IOPOLL). A buffered read that misses the page
// cache therefore cannot complete inline in the submitting task, so io_uring
// punts it to its `io-wq` worker pool. Those workers are created with
// `create_io_thread()` and belong to the submitting process's thread group,
// so they appear under `/proc/self/task/` with a `iou-wrk-` comm and their
// CPU time is charged to this process.
//
// That is where the concurrency of the batched path comes from: not from new
// application threads, but from several kernel workers performing blocking
// reads at once. Sampling them separates "the export thread waited less" from
// "more reads were in flight", which a single `cpu_s` figure cannot.
// Written by the reporter thread, read by `report()`. Relaxed atomics keep
// the sampler lock-free; a single writer makes load-modify-store safe.
struct WorkerSample {
  std::atomic<uint64_t> maxWorkers_{0};   // highest `iou-wrk-` count seen
  std::atomic<uint64_t> maxThreads_{0};   // highest total task count seen
  std::atomic<uint64_t> workerTicks_{0};  // highest worker utime+stime seen
};

namespace detail {
// Intentionally leaked, like `registry()`.
inline WorkerSample& workerSample() {
  static WorkerSample* sample = new WorkerSample;
  return *sample;
}

// Read `utime + stime` from a `/proc/.../stat` line. Both follow the comm
// field, which may itself contain spaces, so parse after the final ')'. Return
// 0 if the line is malformed.
inline uint64_t ticksFromStat(const std::string& stat) {
  const auto close = stat.rfind(')');
  if (close == std::string::npos) {
    return 0;
  }
  std::istringstream rest{stat.substr(close + 1)};
  const std::vector<std::string> fields(
      std::istream_iterator<std::string>{rest},
      std::istream_iterator<std::string>{});
  // The first field after comm is field 3 (state); utime is field 14 and
  // stime is field 15 (see `man 5 proc`).
  constexpr size_t firstFieldAfterComm = 3;
  constexpr size_t utimeOffset = 14 - firstFieldAfterComm;
  constexpr size_t stimeOffset = 15 - firstFieldAfterComm;
  if (fields.size() <= stimeOffset) {
    return 0;
  }
  return std::strtoull(fields[utimeOffset].c_str(), nullptr, 10) +
         std::strtoull(fields[stimeOffset].c_str(), nullptr, 10);
}

// Linux-only: reads `/proc/self/task`, returning early where it is absent.
// One pass over `/proc/self/task`, recording the io_uring worker population.
inline void sampleWorkers() {
  DIR* dir = opendir("/proc/self/task");
  if (dir == nullptr) {
    return;
  }
  uint64_t threads = 0;
  uint64_t workers = 0;
  uint64_t ticks = 0;
  while (dirent* entry = readdir(dir)) {
    if (entry->d_name[0] == '.') {
      continue;
    }
    ++threads;
    const std::string base = std::string{"/proc/self/task/"} + entry->d_name;
    std::ifstream commFile{base + "/comm"};
    std::string comm;
    if (!std::getline(commFile, comm) || comm.rfind("iou-wrk", 0) != 0) {
      continue;
    }
    ++workers;
    std::ifstream statFile{base + "/stat"};
    std::string stat;
    if (std::getline(statFile, stat)) {
      ticks += ticksFromStat(stat);
    }
  }
  closedir(dir);
  WorkerSample& sample = workerSample();
  const uint64_t prevThreads =
      sample.maxThreads_.load(std::memory_order_relaxed);
  sample.maxThreads_.store(std::max(prevThreads, threads),
                           std::memory_order_relaxed);
  const uint64_t prevWorkers =
      sample.maxWorkers_.load(std::memory_order_relaxed);
  sample.maxWorkers_.store(std::max(prevWorkers, workers),
                           std::memory_order_relaxed);
  const uint64_t prevTicks =
      sample.workerTicks_.load(std::memory_order_relaxed);
  sample.workerTicks_.store(std::max(prevTicks, ticks),
                            std::memory_order_relaxed);
}
}  // namespace detail

// Format the current totals as one line.
inline std::string report() {
  const ThreadCounters counters = total();
  const WorkerSample& sample = detail::workerSample();
  const double tick = 1.0 / static_cast<double>(sysconf(_SC_CLK_TCK));
  std::ostringstream out;
  out << "io-wait-accounting"
      << " enabled=" << static_cast<int>(enabled())
      << " pread_calls=" << counters.pread_.calls_
      << " pread_wait_s=" << static_cast<double>(counters.pread_.nanos_) / 1e9
      << " iouring_waits=" << counters.ioUringWait_.calls_ << " iouring_wait_s="
      << static_cast<double>(counters.ioUringWait_.nanos_) / 1e9
      << " iouring_submits=" << counters.ioUringSubmit_.calls_
      << " iouring_submit_s="
      << static_cast<double>(counters.ioUringSubmit_.nanos_) / 1e9
      << " iowq_workers_max="
      << sample.maxWorkers_.load(std::memory_order_relaxed)
      << " threads_max=" << sample.maxThreads_.load(std::memory_order_relaxed)
      << " iowq_cpu_s="
      << static_cast<double>(
             sample.workerTicks_.load(std::memory_order_relaxed)) *
             tick;
  return out.str();
}

// Periodically sample the io_uring worker pool and rewrite the report to the
// file named by `QLEVER_IO_WAIT_REPORT`. Does nothing if that variable is
// unset.
//
// WHY A FILE AND A POLLER, NOT AN EXIT HOOK. The server can stop with a
// signal, so static destructors do not necessarily run. Writing from a signal
// handler would mean formatting inside a handler, which is not
// async-signal-safe. A poller that keeps a complete report on disk is correct
// whatever kills the process, and also captures the worker population *while*
// the query runs, which is when the workers exist.
inline void startReporter() {
  static std::once_flag once;
  std::call_once(once, []() {
    const char* path = std::getenv("QLEVER_IO_WAIT_REPORT");
    if (path == nullptr) {
      return;
    }
    std::thread{[file = std::string{path}]() {
      while (true) {
        detail::sampleWorkers();
        // Write to a temporary and rename, so a reader never sees half a line.
        const std::string tmp = file + ".tmp";
        {
          std::ofstream out{tmp, std::ios::trunc};
          out << report() << '\n';
        }
        std::rename(tmp.c_str(), file.c_str());
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
      }
    }}.detach();
  });
}

// Print the report to stderr on a normal exit, for interactive use.
struct ExitReporter {
  ~ExitReporter() { std::fprintf(stderr, "%s\n", report().c_str()); }
};
// Register the exit reporter and start the periodic file reporter, both at most
// once per process.
inline const ExitReporter& exitReporter() {
  static ExitReporter reporter;
  startReporter();
  return reporter;
}

}  // namespace ad_utility::ioWait

#endif  // QLEVER_SRC_UTIL_IOWAITACCOUNTING_H
