// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BATCHMANAGERPOOL_H
#define QLEVER_SRC_UTIL_BATCHMANAGERPOOL_H

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "util/Exception.h"
#include "util/IoUringManager.h"

namespace ad_utility {

// A thread-safe pool of `BatchManagerBase`s, created on demand.
//
// A manager may be used by one thread at a time, so every concurrent user needs
// its own. `acquire` hands out an idle manager, or creates a new one if all
// existing managers are in use and fewer than `maxManagers` exist; only if
// `maxManagers` managers are in use at the same time does `acquire` wait. So
// the number of managers follows the number of threads that actually use them,
// and the bound `maxManagers` is only a limit on the memory they occupy (an
// `io_uring` ring takes a few tens of KiB of kernel memory), not a tuning
// parameter that silently serializes lookups.
class BatchManagerPool {
 public:
  // Creates a new manager. It is called with the pool locked, so it needs no
  // synchronization of its own. If it throws, `acquire` throws and the pool
  // is unchanged.
  using Factory = std::function<std::unique_ptr<BatchManagerBase>()>;

  // A manager on loan from the pool. The destructor gives it back, also if the
  // user threw an exception.
  class Lease {
   public:
    Lease(Lease&& other) noexcept
        : pool_{std::exchange(other.pool_, nullptr)},
          manager_{std::move(other.manager_)} {}
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease& operator=(Lease&&) = delete;
    ~Lease() {
      if (pool_ != nullptr) {
        pool_->release(std::move(manager_));
      }
    }

    BatchManagerBase& operator*() const { return *manager_; }
    BatchManagerBase* operator->() const { return manager_.get(); }

   private:
    friend class BatchManagerPool;
    Lease(BatchManagerPool* pool, std::unique_ptr<BatchManagerBase> manager)
        : pool_{pool}, manager_{std::move(manager)} {}

    BatchManagerPool* pool_;
    std::unique_ptr<BatchManagerBase> manager_;
  };

  // The pool creates at most `maxManagers` managers with `factory`.
  BatchManagerPool(size_t maxManagers, Factory factory)
      : maxManagers_{maxManagers}, factory_{std::move(factory)} {
    AD_CONTRACT_CHECK(maxManagers_ > 0);
    // `release` must never allocate, because it runs in a destructor.
    idle_.reserve(maxManagers_);
  }

  BatchManagerPool(const BatchManagerPool&) = delete;
  BatchManagerPool& operator=(const BatchManagerPool&) = delete;

  // Take a manager from the pool: the most recently returned idle one, a newly
  // created one, or, if `maxManagers` managers are in use, wait for one to be
  // given back.
  Lease acquire() {
    std::unique_lock lock{mutex_};
    available_.wait(lock, [this]() {
      return !idle_.empty() || numCreated_ < maxManagers_;
    });
    if (!idle_.empty()) {
      auto manager = std::move(idle_.back());
      idle_.pop_back();
      return Lease{this, std::move(manager)};
    }
    auto manager = factory_();
    ++numCreated_;
    return Lease{this, std::move(manager)};
  }

  // The number of managers created so far.
  size_t numCreated() const {
    std::lock_guard lock{mutex_};
    return numCreated_;
  }

 private:
  // Give a manager back. Does not throw: `idle_` has room for all managers.
  void release(std::unique_ptr<BatchManagerBase> manager) noexcept {
    {
      std::lock_guard lock{mutex_};
      idle_.push_back(std::move(manager));
    }
    available_.notify_one();
  }

  size_t maxManagers_;
  Factory factory_;
  mutable std::mutex mutex_;
  std::condition_variable available_;
  std::vector<std::unique_ptr<BatchManagerBase>> idle_;
  size_t numCreated_ = 0;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BATCHMANAGERPOOL_H
