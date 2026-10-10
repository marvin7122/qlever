// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/GlobalExecutor.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <boost/asio/thread_pool.hpp>
#include <memory>
#include <mutex>
#include <thread>

#include "util/Exception.h"

namespace ad_utility {

namespace {

// The configuration of the global thread pool. All its members are protected by
// the `mutex_`.
//
// NOTE: Two atomics would not do instead of the mutex, because reading the
// number of threads, constructing the pool, and finalizing the configuration
// only after successful construction must happen under the same lock. Setters
// and readers are serialized through construction, so they cannot change or
// report a number that differs from the pool's size. If construction throws,
// the configuration remains changeable and static initialization can be
// retried.
struct GlobalExecutorConfig {
  std::mutex mutex_;
  // The number of threads that the pool has or will have.
  size_t numThreads_ = std::max<size_t>(1, std::thread::hardware_concurrency());
  // Set only after the pool has been successfully constructed, after which
  // `numThreads_` can no longer be changed.
  bool poolWasCreated_ = false;
};

// Return the single instance of the `GlobalExecutorConfig`. It is a
// function-local static, such that it is initialized on the first use and in
// particular before any other static initialization can access it.
GlobalExecutorConfig& config() {
  static GlobalExecutorConfig config;
  return config;
}

}  // namespace

// _____________________________________________________________________________
bool trySetGlobalExecutorNumThreads(size_t numThreads) {
  AD_CONTRACT_CHECK(numThreads > 0);
  auto& conf = config();
  std::lock_guard lock{conf.mutex_};
  if (conf.poolWasCreated_) {
    return conf.numThreads_ == numThreads;
  }
  conf.numThreads_ = numThreads;
  return true;
}

// _____________________________________________________________________________
void setGlobalExecutorNumThreads(size_t numThreads) {
  if (!trySetGlobalExecutorNumThreads(numThreads)) {
    AD_THROW(absl::StrCat(
        "The number of threads of the global thread pool must not be set after "
        "the pool has already been accessed: it was set to ",
        numThreads, ", but the pool had already been created with ",
        globalExecutorNumThreads(), " threads"));
  }
}

// _____________________________________________________________________________
size_t globalExecutorNumThreads() {
  auto& conf = config();
  std::lock_guard lock{conf.mutex_};
  return conf.numThreads_;
}

// _____________________________________________________________________________
ql::any_io_executor globalExecutor() {
  // NOTE: Function-local static initialization is thread-safe and is retried
  // after an exception. Keep the configuration lock through construction and
  // finalize it only after success, so a failed attempt allows reconfiguration.
  static auto pool = []() {
    auto& conf = config();
    std::lock_guard lock{conf.mutex_};
    auto ownedPool =
        std::make_unique<boost::asio::thread_pool>(conf.numThreads_);
    conf.poolWasCreated_ = true;
    return ownedPool;
  }();
  return pool->get_executor();
}

}  // namespace ad_utility
