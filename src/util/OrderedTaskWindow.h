// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ORDEREDTASKWINDOW_H
#define QLEVER_SRC_UTIL_ORDEREDTASKWINDOW_H

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include "util/Exception.h"
#include "util/jthread.h"

namespace ad_utility {

// Resolve a thread-count setting: 0 means "one thread per hardware thread"
// (at least one), any other value is returned unchanged.
inline size_t resolveNumThreads(size_t numThreads) {
  if (numThreads != 0) {
    return numThreads;
  }
  return std::max(size_t{1}, size_t{std::thread::hardware_concurrency()});
}

// Runs tasks on a fixed set of worker threads and hands their results back in
// submission order. The caller keeps at most `maxInFlight` tasks queued or
// running (`submit` requires `!full()`), which bounds the memory held by
// unconsumed results.
//
// Every task receives the index (in `[0, numWorkers)`) of the worker thread
// that runs it, so that it can use per-worker state without locking (e.g. one
// cache per worker). A task that throws stores the exception, and `popFront`
// rethrows it on the calling thread.
//
// The destructor drops the tasks that no worker has started yet and joins the
// workers, so it waits for at most `numWorkers` running tasks. Everything the
// tasks reference must therefore outlive the window.
template <typename Result>
class OrderedTaskWindow {
 public:
  using Task = std::packaged_task<Result(size_t)>;

  OrderedTaskWindow(size_t numWorkers, size_t maxInFlight)
      : maxInFlight_{maxInFlight} {
    AD_CONTRACT_CHECK(numWorkers > 0);
    AD_CONTRACT_CHECK(maxInFlight > 0);
    workers_.reserve(numWorkers);
    for (size_t i = 0; i < numWorkers; ++i) {
      workers_.emplace_back([this, i]() { runWorker(i); });
    }
  }

  OrderedTaskWindow(const OrderedTaskWindow&) = delete;
  OrderedTaskWindow& operator=(const OrderedTaskWindow&) = delete;

  ~OrderedTaskWindow() {
    {
      std::lock_guard lock{mutex_};
      stop_ = true;
      // The futures of the dropped tasks report `broken_promise`, but they
      // are destroyed below without being waited for.
      tasks_.clear();
    }
    taskAvailable_.notify_all();
    // `JThread` joins in its destructor.
    workers_.clear();
  }

  size_t numWorkers() const { return workers_.size(); }
  size_t maxInFlight() const { return maxInFlight_; }
  bool empty() const { return inFlight_.empty(); }
  bool full() const { return inFlight_.size() >= maxInFlight_; }

  // Queue `task` behind all previously submitted tasks.
  void submit(Task task) {
    AD_CONTRACT_CHECK(!full());
    inFlight_.push_back(task.get_future());
    {
      std::lock_guard lock{mutex_};
      tasks_.push_back(std::move(task));
    }
    taskAvailable_.notify_one();
  }

  // Wait for the oldest submitted task and return its result (or rethrow its
  // exception).
  Result popFront() {
    AD_CONTRACT_CHECK(!empty());
    auto future = std::move(inFlight_.front());
    inFlight_.pop_front();
    return future.get();
  }

 private:
  void runWorker(size_t workerIndex) {
    while (true) {
      Task task;
      {
        std::unique_lock lock{mutex_};
        taskAvailable_.wait(lock,
                            [this]() { return stop_ || !tasks_.empty(); });
        if (stop_) {
          return;
        }
        task = std::move(tasks_.front());
        tasks_.pop_front();
      }
      // A `packaged_task` stores a thrown exception in its future.
      task(workerIndex);
    }
  }

  size_t maxInFlight_;
  // Only accessed by the owning (submitting) thread.
  std::deque<std::future<Result>> inFlight_;
  std::mutex mutex_;
  std::condition_variable taskAvailable_;
  std::deque<Task> tasks_;
  bool stop_ = false;
  // Last member: the workers are joined before the other members are gone.
  std::vector<ad_utility::JThread> workers_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ORDEREDTASKWINDOW_H
