// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/io_uring/BatchManagerPool.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <atomic>
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
  AD_CONTRACT_CHECK(numManagers > 0,
                    "A `BatchManagerPool` needs at least one manager.");
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
    AD_CORRECTNESS_CHECK(idle_.size() < numManagers_,
                         "More managers were given back to the pool than it "
                         "has, so `release` would have to allocate.");
    idle_.push_back(std::move(manager));
  }
  managerReturned_.notify_one();
}

namespace {

// The number of managers that the global pool gets when it is created.
std::atomic<size_t>& numberOfManagersOfGlobalPool() {
  static std::atomic<size_t> numberOfManagers =
      std::max<size_t>(1, std::thread::hardware_concurrency());
  return numberOfManagers;
}

}  // namespace

// _____________________________________________________________________________
BatchManagerPool& globalBatchManagerPool() {
  // NOTE: The initialization of a function-local static is thread-safe, so the
  // pool is created exactly once. The pool is deliberately never destroyed:
  // threads that are still inside a batched read when the process exits would
  // otherwise give their manager back to a destroyed pool.
  static BatchManagerPool* pool =
      new BatchManagerPool{numberOfManagersOfGlobalPool().load(),
                           /*preferIoUring=*/true};
  return *pool;
}

// _____________________________________________________________________________
void setUpGlobalBatchManagerPool(size_t numManagers) {
  AD_CONTRACT_CHECK(
      numManagers > 0,
      "The global `BatchManagerPool` needs at least one manager.");
  numberOfManagersOfGlobalPool() = numManagers;
  const auto& pool = globalBatchManagerPool();
  // The pool has another size if it was created before this call, or if a
  // concurrent call set another size first.
  if (pool.size() != numManagers) {
    AD_THROW(absl::StrCat(
        "The pool of batch I/O managers must be set up before its first use: ",
        numManagers,
        " managers were requested, but the pool already exists "
        "with ",
        pool.size(), " managers"));
  }
  AD_LOG_INFO << "Batched vocabulary reads use " << pool.size()
              << " I/O managers ("
              << (pool.usesIoUring() ? "io_uring" : "synchronous pread") << ")"
              << std::endl;
}

}  // namespace ad_utility
