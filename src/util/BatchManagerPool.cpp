// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/BatchManagerPool.h"

#include <algorithm>
#include <range/v3/range/conversion.hpp>
#include <range/v3/view/generate_n.hpp>
#include <thread>
#include <utility>

#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

// _____________________________________________________________________________
BatchManagerPool::Lease::Lease(BatchManagerPool* pool,
                               std::unique_ptr<BatchManagerBase> manager)
    : pool_{pool}, manager_{std::move(manager)} {}

// _____________________________________________________________________________
BatchManagerPool::Lease::Lease(Lease&& other) noexcept
    : pool_{std::exchange(other.pool_, nullptr)},
      manager_{std::move(other.manager_)} {}

// _____________________________________________________________________________
BatchManagerPool::Lease::~Lease() {
  if (pool_ != nullptr) {
    pool_->release(std::move(manager_));
  }
}

// _____________________________________________________________________________
BatchManagerPool::BatchManagerPool(size_t numManagers, bool preferIoUring)
    : numManagers_{numManagers} {
  AD_CONTRACT_CHECK(numManagers > 0);
  // `makeBatchManager` clears `preferIoUring` (a reference parameter) when the
  // io_uring setup fails or io_uring is not compiled in, so after the first
  // failure the remaining managers are synchronous ones, and the flag is still
  // set afterwards iff all managers use io_uring.
  auto makeManager = [&preferIoUring]() {
    return makeBatchManager(preferIoUring);
  };
  // All managers start idle, so `idle_` has room for all of them and `release`
  // never has to allocate.
  idle_ = ::ranges::to_vector(
      ::ranges::views::generate_n(makeManager, numManagers));
  usesIoUring_ = preferIoUring;
}

// _____________________________________________________________________________
BatchManagerPool::Lease BatchManagerPool::acquire() {
  std::unique_lock lock{mutex_};
  managerReturned_.wait(lock, [this]() { return !idle_.empty(); });
  auto manager = std::move(idle_.back());
  idle_.pop_back();
  return Lease{this, std::move(manager)};
}

// _____________________________________________________________________________
void BatchManagerPool::release(
    std::unique_ptr<BatchManagerBase> manager) noexcept {
  {
    std::lock_guard lock{mutex_};
    // The pool only ever hands out its own managers, so there is always room
    // and the `push_back` does not allocate.
    AD_CORRECTNESS_CHECK(idle_.size() < numManagers_);
    idle_.push_back(std::move(manager));
  }
  managerReturned_.notify_one();
}

namespace {

struct GlobalPoolConfig {
  std::mutex mutex_;
  // The configuration that the pool has or will have.
  size_t numManagers_ =
      std::max<size_t>(1, std::thread::hardware_concurrency());
  bool preferIoUring_ = true;
  // Set as soon as the pool has been created, after which the configuration
  // can no longer be changed.
  bool poolWasCreated_ = false;
};

GlobalPoolConfig& config() {
  static GlobalPoolConfig config;
  return config;
}

}  // namespace

// _____________________________________________________________________________
bool trySetGlobalBatchManagerPoolConfig(size_t numManagers,
                                        bool preferIoUring) {
  AD_CONTRACT_CHECK(numManagers > 0);
  auto& conf = config();
  std::lock_guard lock{conf.mutex_};
  if (conf.poolWasCreated_) {
    return conf.numManagers_ == numManagers &&
           conf.preferIoUring_ == preferIoUring;
  }
  conf.numManagers_ = numManagers;
  conf.preferIoUring_ = preferIoUring;
  return true;
}

// _____________________________________________________________________________
BatchManagerPool& globalBatchManagerPool() {
  // NOTE: The initialization of a function-local static is thread-safe, so the
  // lambda (and with it the marking of the configuration as final) runs exactly
  // once.
  static std::unique_ptr<BatchManagerPool> pool = []() {
    auto& conf = config();
    std::lock_guard lock{conf.mutex_};
    auto pool = std::make_unique<BatchManagerPool>(conf.numManagers_,
                                                   conf.preferIoUring_);
    // Only mark the configuration as final once the pool exists, so that a
    // constructor that throws leaves the configuration changeable.
    conf.poolWasCreated_ = true;
    return pool;
  }();
  return *pool;
}

// _____________________________________________________________________________
void setUpGlobalBatchManagerPool(size_t numManagers, bool preferIoUring) {
  trySetGlobalBatchManagerPoolConfig(numManagers, preferIoUring);
  const auto& pool = globalBatchManagerPool();
  // Now that the pool exists, the call reports whether the pool has this
  // configuration, also if another thread changed the configuration between
  // the first call and the creation.
  if (!trySetGlobalBatchManagerPoolConfig(numManagers, preferIoUring)) {
    AD_LOG_WARN << "The pool of batch I/O managers already exists with "
                << pool.size() << " managers (io_uring "
                << (pool.usesIoUring() ? "used" : "not used")
                << "), so the requested configuration of " << numManagers
                << " managers (io_uring "
                << (preferIoUring ? "preferred" : "not preferred")
                << ") is ignored" << std::endl;
  }
  AD_LOG_INFO << "Batched vocabulary reads use " << pool.size()
              << " I/O managers ("
              << (pool.usesIoUring() ? "io_uring" : "synchronous pread") << ")"
              << std::endl;
}

}  // namespace ad_utility
