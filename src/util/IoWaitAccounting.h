// Copyright 2026, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace ad_utility::ioWait {

// Wall-clock accounting for the calls on which a query thread blocks waiting
// for storage: the positioned `pread` in `File::read` and the completion wait
// in `IoUringPolicy::drainOneCqe`.
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
// the instrumentation off while the process is running.
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
  // `io_uring_submit` is a blocking call site too. With `flags = 0` on a
  // buffered file, a read that the kernel can service without punting is
  // performed inside `io_uring_enter`, so the thread blocks in submit and the
  // matching completion is already present when it is reaped. Run
  // io-wait-crossarm-3 measured 60.9 million completion waits totalling only
  // 1.87 s, which is what that looks like when submit is not instrumented.
  Counters ioUringSubmit_;
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

// Add this thread's counters to the registry on first use and fold them into
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
    reg.finished_.ioUringSubmit_.nanos_ += counters_.ioUringSubmit_.nanos_;
    reg.finished_.ioUringSubmit_.calls_ += counters_.ioUringSubmit_.calls_;
    reg.live_.erase(std::remove(reg.live_.begin(), reg.live_.end(), &counters_),
                    reg.live_.end());
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

// Time the blocking call `callable` into `counters` when the instrumentation
// is enabled, and call it directly otherwise. Return whatever `callable`
// returns.
template <typename Selector, typename Callable>
decltype(auto) timed(Selector selector, Callable&& callable) {
  if (!enabled()) {
    return callable();
  }
  const uint64_t start = detail::nowNanos();
  Counters& counters = selector(detail::threadCounters());
  if constexpr (std::is_void_v<decltype(callable())>) {
    callable();
    counters.nanos_ += detail::nowNanos() - start;
    ++counters.calls_;
  } else {
    auto result = callable();
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
inline Counters& ioUringSubmitCounters(ThreadCounters& counters) {
  return counters.ioUringSubmit_;
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
    total.ioUringSubmit_.nanos_ += counters->ioUringSubmit_.nanos_;
    total.ioUringSubmit_.calls_ += counters->ioUringSubmit_.calls_;
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
struct WorkerSample {
  uint64_t maxWorkers_ = 0;   // highest `iou-wrk-` count seen
  uint64_t maxThreads_ = 0;   // highest total task count seen
  uint64_t workerTicks_ = 0;  // utime + stime of `iou-wrk-` tasks, last sample
};

namespace detail {
inline WorkerSample& workerSample() {
  static WorkerSample sample;
  return sample;
}

// Read `utime + stime` from a `/proc/.../stat` line. Both follow the comm
// field, which may itself contain spaces, so parse after the final ')'.
inline uint64_t ticksFromStat(const std::string& stat) {
  const auto close = stat.rfind(')');
  if (close == std::string::npos) {
    return 0;
  }
  std::istringstream rest{stat.substr(close + 1)};
  std::string field;
  // Fields after comm: state(3) ppid(4) ... utime is 14, stime is 15.
  uint64_t utime = 0;
  uint64_t stime = 0;
  for (int index = 3; index <= 15; ++index) {
    if (!(rest >> field)) {
      return 0;
    }
    if (index == 14) {
      utime = std::strtoull(field.c_str(), nullptr, 10);
    } else if (index == 15) {
      stime = std::strtoull(field.c_str(), nullptr, 10);
    }
  }
  return utime + stime;
}

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
  sample.maxThreads_ = std::max(sample.maxThreads_, threads);
  sample.maxWorkers_ = std::max(sample.maxWorkers_, workers);
  sample.workerTicks_ = std::max(sample.workerTicks_, ticks);
}

// Shutdown flag for the reporter thread.
inline std::atomic<bool>& shutdownRequested() {
  static std::atomic<bool> flag{false};
  return flag;
}

// Storage for the reporter thread so it can be joined on shutdown.
inline std::thread& reporterThread() {
  static std::thread thread;
  return thread;
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
      << " iowq_workers_max=" << sample.maxWorkers_
      << " threads_max=" << sample.maxThreads_
      << " iowq_cpu_s=" << static_cast<double>(sample.workerTicks_) * tick;
  return out.str();
}

// Periodically sample the io_uring worker pool and rewrite the report to the
// file named by `QLEVER_IO_WAIT_REPORT`, plus stderr at process exit.
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

// Also report on a normal exit, for interactive use.
struct ExitReporter {
  ~ExitReporter() { std::fprintf(stderr, "%s\n", report().c_str()); }
};
inline const ExitReporter& exitReporter() {
  static ExitReporter reporter;
  startReporter();
  return reporter;
}

}  // namespace ad_utility::ioWait

#endif  // QLEVER_SRC_UTIL_IOWAITACCOUNTING_H
