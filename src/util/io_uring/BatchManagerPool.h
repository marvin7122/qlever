// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IO_URING_BATCHMANAGERPOOL_H
#define QLEVER_SRC_UTIL_IO_URING_BATCHMANAGERPOOL_H

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "util/Exception.h"
#include "util/IoUringManager.h"
#include "util/NoCopyNoMove.h"

namespace ad_utility {

// A fixed number of `BatchManagerBase`s (an io_uring ring each, or the
// synchronous fallback) that threads borrow for batched reads. A manager serves
// one batch at a time, so a thread that performs a batched read takes a manager
// with `acquire`, uses it through the returned `Lease`, and gives it back when
// the lease goes out of scope. `acquire` waits, without a timeout, while all
// managers are in use, so the pool should have one manager per thread that
// performs batched reads.
//
// All managers are created in the constructor. So the io_uring probe, and the
// fallback to synchronous reads if the `io_uring_setup` syscall fails, happen
// once when the pool is created and not during a lookup.
//
// The pool is neither copied nor moved, because the leases refer to it by
// pointer and other threads wait on its condition variable.
class BatchManagerPool : public NoCopyNoMove {
 public:
  // Create one manager, with the protocol of `makeBatchManager`: if
  // `preferIoUring` is `true`, try to create an io_uring manager, and clear
  // `preferIoUring` if that fails; if `preferIoUring` is `false`, create a
  // synchronous manager. The parameter lets tests inject failures.
  using MakeManager =
      std::function<std::unique_ptr<BatchManagerBase>(bool& preferIoUring)>;

  // A manager on loan from the pool. The destructor gives it back. If the
  // destructor runs during stack unwinding (the batched read threw, e.g.
  // because a read failed), the manager may still have reads in flight, so the
  // pool destroys it, which waits for these reads, and puts a new manager in
  // its place. A lease can be moved (the moved-from lease then holds nothing),
  // but not assigned, because an assignment would have to give a manager back
  // in the middle of its use.
  class Lease : public NoCopy {
   public:
    Lease(Lease&& other) noexcept;
    Lease& operator=(Lease&&) = delete;
    ~Lease();

    // Return `true` iff this lease holds a manager, which only a moved-from
    // lease does not.
    bool isValid() const noexcept { return manager_ != nullptr; }

    // Return the manager. The lease has to be valid, see `isValid`.
    BatchManagerBase& operator*() const {
      AD_CONTRACT_CHECK(isValid(), "A moved-from `Lease` holds no manager.");
      return *manager_;
    }
    BatchManagerBase* operator->() const {
      AD_CONTRACT_CHECK(isValid(), "A moved-from `Lease` holds no manager.");
      return manager_.get();
    }

   private:
    friend class BatchManagerPool;
    Lease(BatchManagerPool* pool, std::unique_ptr<BatchManagerBase> manager);

    // The pool that the manager goes back to, and the manager itself. Both
    // are `nullptr` after a move.
    BatchManagerPool* pool_;
    std::unique_ptr<BatchManagerBase> manager_;
    // The value of `std::uncaught_exceptions()` when the lease was created. A
    // larger value in the destructor means that it runs during unwinding.
    int numberOfUncaughtExceptionsAtCreation_;
  };

  // Create `numManagers` (> 0) managers with `makeManager`, preferring
  // io_uring. If the io_uring setup fails for one of them, all managers are
  // synchronous ones (the io_uring managers that were created before the
  // failure are replaced), so that every lookup uses the same backend.
  explicit BatchManagerPool(size_t numManagers,
                            MakeManager makeManager = makeDefaultManager);

  // Borrow a manager and return the lease that gives it back. Wait while all
  // managers are in use.
  [[nodiscard]] Lease acquire();

  // Return the number of managers of the pool, idle or in use.
  size_t size() const { return numManagers_; }

  // Return `true` iff all managers use io_uring. This can change from `true`
  // to `false` if the io_uring setup fails for a manager that replaces a
  // failed one (see `Lease`).
  bool usesIoUring() const { return usesIoUring_; }

 private:
  static std::unique_ptr<BatchManagerBase> makeDefaultManager(
      bool& preferIoUring) {
    return makeBatchManager(preferIoUring);
  }

  // Whether a manager that is given back can serve the next batch.
  enum class ManagerState { Reusable, Failed };

  // Give a manager back. A `Failed` manager is replaced by a new one. Never
  // throws, and only allocates for such a new manager, because `idle_` has room
  // for all managers and this runs in the destructor of `Lease`.
  void release(std::unique_ptr<BatchManagerBase> manager,
               ManagerState state) noexcept;

  const size_t numManagers_;
  const MakeManager makeManager_;
  std::atomic<bool> usesIoUring_ = false;
  // The managers that are not on loan, guarded by `mutex_`. `release` notifies
  // `managerReturned_` when it puts a manager back.
  std::mutex mutex_;
  std::condition_variable managerReturned_;
  std::vector<std::unique_ptr<BatchManagerBase>> idle_;
};

// The process-wide pool that all batched reads share (currently those of
// `VocabularyOnDisk::lookupBatch`). It prefers io_uring and is created on the
// first call of `globalBatchManagerPool()` or `setUpGlobalBatchManagerPool`.
// The server calls the latter at startup with one manager per query thread, so
// the rings exist and the io_uring probe has run before the first query.
// Other programs (libqlever, tests) get one manager per hardware thread on
// their first batched read.
//
// NOTE: Like `globalExecutor` (see the TODO in `util/GlobalExecutor.h`), this
// is a mutable process-wide singleton whose size can only be set before its
// first use. It is global and not owned by the engine, because
// `lookupBatch(indices)` is the common interface of all vocabulary types, and
// passing a pool to the one type that performs I/O would mean to change
// either that interface or `open` in every vocabulary type and in all their
// tests. Once the engine passes its resources to the index explicitly, the pool
// should become an object that the engine owns and hands to its users.

// Create the process-wide pool with `numManagers` (> 0) managers and log their
// number and backend. Throw if the pool already exists with another number of
// managers, because then it was used before it was set up.
void setUpGlobalBatchManagerPool(size_t numManagers);

// Return the process-wide pool, creating it on the first call. Thread-safe.
BatchManagerPool& globalBatchManagerPool();

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IO_URING_BATCHMANAGERPOOL_H
