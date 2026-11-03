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
#include <memory>
#include <mutex>
#include <vector>

#include "util/IoUringManager.h"

namespace ad_utility {

// A fixed set of `BatchManagerBase`s (an io_uring ring each, or the synchronous
// fallback) that threads borrow for batched reads. A manager serves one batch
// at a time, so a thread that performs a batched read takes a manager with
// `acquire`, uses it through the returned `Lease`, and gives it back when the
// lease goes out of scope. `acquire` waits while all managers are in use.
//
// All managers are created in the constructor. So the io_uring probe, and the
// fallback to synchronous reads if the `io_uring_setup` syscall fails, happen
// once when the pool is created and not during a lookup.
class BatchManagerPool {
 public:
  // A manager on loan from the pool. The destructor gives it back, also when
  // the user leaves the scope with an exception.
  class Lease {
   public:
    Lease(Lease&& other) noexcept;
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease& operator=(Lease&&) = delete;
    ~Lease();

    BatchManagerBase& operator*() const { return *manager_; }
    BatchManagerBase* operator->() const { return manager_.get(); }

   private:
    friend class BatchManagerPool;
    Lease(BatchManagerPool* pool, std::unique_ptr<BatchManagerBase> manager);

    BatchManagerPool* pool_;
    std::unique_ptr<BatchManagerBase> manager_;
  };

  // Create `numManagers` (> 0) managers. If `preferIoUring` is `true`, and
  // io_uring is compiled in and its setup succeeds, they are io_uring managers.
  // Otherwise they read synchronously (see `makeBatchManager`). If the setup
  // fails for one of the managers, that one and all later ones are synchronous
  // and `usesIoUring()` is `false`.
  BatchManagerPool(size_t numManagers, bool preferIoUring);

  BatchManagerPool(const BatchManagerPool&) = delete;
  BatchManagerPool& operator=(const BatchManagerPool&) = delete;

  // Borrow a manager. Wait while all managers are in use.
  Lease acquire();

  // The number of managers of the pool, idle or in use.
  size_t size() const { return numManagers_; }

  // `true` iff all managers use io_uring.
  bool usesIoUring() const { return usesIoUring_; }

 private:
  // Give a manager back. Never throws or allocates, because `idle_` has room
  // for all managers and this runs in the destructor of `Lease`.
  void release(std::unique_ptr<BatchManagerBase> manager) noexcept;

  size_t numManagers_;
  bool usesIoUring_ = false;
  std::mutex mutex_;
  std::condition_variable managerReturned_;
  std::vector<std::unique_ptr<BatchManagerBase>> idle_;
};

// The process-wide pool that all batched reads share (currently those of
// `VocabularyOnDisk::lookupBatch`). It is created on the first call of
// `globalBatchManagerPool()`, with the configuration that was set by
// `trySetGlobalBatchManagerPoolConfig` before, or by default with one manager
// per hardware thread and io_uring preferred. The server sets one manager per
// query thread and creates the pool at startup, so the rings exist and the
// io_uring probe has run before the first query. Other programs (the index
// builder, tests) get the default pool on their first batched read.
//
// TODO<joka921> Like `globalExecutor`, this is a mutable process-wide
// singleton whose configuration can only be set before its first use. Once
// the server passes its resources to the index explicitly, the pool should
// become an object that is owned by the server and handed to its users.

// Configure the pool: `numManagers` (> 0) managers that try io_uring iff
// `preferIoUring`. This only has an effect before the pool exists. Return
// `true` if the configuration was set, or if the pool already exists with
// exactly this configuration. Return `false` if the pool already exists with
// a different one. Thread-safe.
bool trySetGlobalBatchManagerPoolConfig(size_t numManagers, bool preferIoUring);

// Return the process-wide pool, creating it on the first call. Thread-safe.
BatchManagerPool& globalBatchManagerPool();

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BATCHMANAGERPOOL_H
