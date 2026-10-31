// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IOURINGMANAGER_H
#define QLEVER_SRC_UTIL_IOURINGMANAGER_H

#include <gtest/gtest_prod.h>
#include <sys/uio.h>

#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/Algorithm.h"
#include "util/BatchHandle.h"
#include "util/Exception.h"
#include "util/IoUringManagerConcepts.h"
#include "util/IoUringPolicy.h"
#include "util/Log.h"
#include "util/MonotonicCounter.h"
#include "util/PageCacheFirstPolicy.h"
#include "util/SyncIoPolicy.h"

namespace ad_utility {

// Abstract base of `BatchManager` so a pool can hold managers of different
// `ReadPolicy`s behind a unified interface and pick the backend at runtime.
class BatchManagerBase {
 public:
  using BatchHandle = ::ad_utility::BatchHandle;

  // Marked `virtual`, so that destroying a derived manager through this base
  // (the pool owns `std::unique_ptr<BatchManagerBase>`) runs the derived
  // destructor.
  virtual ~BatchManagerBase() = default;

  [[nodiscard]] virtual BatchHandle addBatch(int fd,
                                             ql::span<const size_t> numBytes,
                                             ql::span<const uint64_t> offsets,
                                             ql::span<char*> buffers) = 0;

  virtual void wait(BatchHandle handle) = 0;

  // Switch the page-cache fast path of this manager on or off (see
  // `PageCacheFirstPolicy`). Takes effect for the batches added afterwards.
  // Managers without a page-cache fast path ignore it.
  virtual void setPageCacheFastPath([[maybe_unused]] bool enabled) {}
};

namespace detail {
// True iff `Policy` has a page-cache fast path that can be switched.
template <typename Policy, typename = void>
inline constexpr bool hasPageCacheFastPathSwitch = false;

template <typename Policy>
inline constexpr bool hasPageCacheFastPathSwitch<
    Policy,
    std::void_t<decltype(std::declval<Policy&>().setPageCacheFastPath(true))>> =
    true;
}  // namespace detail

// `BatchManager` owns the batch bookkeeping (minting a `BatchHandle` per batch,
// validating the input spans) and delegates the reads from the underlying
// Vocabulary to the `Policy`, which must satisfy `detail::ReadPolicyConcept`
// from `IoUringManagerConcepts.h`.
template <typename ReadPolicy>
class BatchManager final : public BatchManagerBase {
  static_assert(
      detail::ReadPolicyConcept<ReadPolicy>,
      "BatchManager's ReadPolicy must satisfy the ReadPolicyConcept.");

 public:
  using BatchHandle = ::ad_utility::BatchHandle;

  // `explicit`, so that a bare ring size cannot silently convert to a
  // manager where a `BatchManager` is expected.
  explicit BatchManager(unsigned ringSize = 256) : policy_(ringSize) {}

  // Not copyable or movable: the manager owns its policy (for
  // `IoUringPolicy` the io_uring ring and the bookkeeping of its
  // outstanding reads), so a copy would double-own that state. A
  // correctly written move could transfer that state, batch handles
  // included, but no caller needs one: managers live in
  // `std::unique_ptr`s, which already provide the only transfer of
  // ownership that is needed. The deleted operations keep this a
  // deliberate decision instead of an accidentally available move.
  BatchManager(const BatchManager&) = delete;
  BatchManager& operator=(const BatchManager&) = delete;
  BatchManager(BatchManager&&) = delete;
  BatchManager& operator=(BatchManager&&) = delete;

  [[nodiscard]] BatchHandle addBatch(int fd, ql::span<const size_t> numBytes,
                                     ql::span<const uint64_t> offsets,
                                     ql::span<char*> buffers) override {
    validateSameLength(numBytes, offsets, buffers);

    BatchHandle handle{nextBatchHandle_.mintNext()};

    // Delegate the I/O work to the policy.
    policy_.addBatch(fd, numBytes, offsets, buffers, handle);

    return handle;
  }

  // Block until every read in `handle` has completed.
  void wait(BatchHandle handle) override { policy_.wait(handle); }

  // Forward the switch to the policy if it has one.
  void setPageCacheFastPath([[maybe_unused]] bool enabled) override {
    if constexpr (detail::hasPageCacheFastPathSwitch<ReadPolicy>) {
      policy_.setPageCacheFastPath(enabled);
    }
  }

 private:
  [[no_unique_address]] ReadPolicy policy_;
  // Mints the batch handles. Type is `MonotonicCounter`, so the handles are
  // increasing (and hence unique) by construction.
  MonotonicCounter nextBatchHandle_;
};

#ifdef QLEVER_HAS_IO_URING
using BatchIoManager = BatchManager<IoUringPolicy>;
#else
using BatchIoManager = BatchManager<SyncIoPolicy>;
#endif

namespace detail {
// The one `preadv2(fd, iov, iovcnt, offset, RWF_NOWAIT)` call per run that
// `readPageCacheHits` makes, with the same contract (the number of bytes read,
// or -1 with `errno` set). A replaceable function pointer so that unit tests
// can inject `EAGAIN`, short reads and `EOPNOTSUPP`; production code never
// changes it.
using PageCacheRead = int64_t (*)(int fd, const ::iovec* iov, int iovcnt,
                                  int64_t offset);
// The default: the system call. Where it is not available it fails with
// `EOPNOTSUPP` (it is never called there).
int64_t systemPageCacheRead(int fd, const ::iovec* iov, int iovcnt,
                            int64_t offset);
// The function `readPageCacheHits` calls (initially `systemPageCacheRead`).
PageCacheRead& pageCacheRead();
// Undo the effect of an `EOPNOTSUPP` on `pageCacheFastPathIsSupported()`, for
// tests that injected one.
void resetPageCacheFastPathSupport();
}  // namespace detail

namespace detail {
// Build a `BatchManager` for `Policy`, decorated with `PageCacheFirstPolicy` if
// the page-cache fast path is supported.
template <typename Policy>
std::unique_ptr<BatchManagerBase> makeBatchManagerFor(unsigned ringSize) {
  if (pageCacheFastPathIsSupported()) {
    return std::make_unique<BatchManager<PageCacheFirstPolicy<Policy>>>(
        ringSize);
  }
  return std::make_unique<BatchManager<Policy>>(ringSize);
}
}  // namespace detail

// Build a batch manager. When io_uring is compiled in and the runtime flag
// `preferIoUring` is set, try to build an `IoUringManager`. If its setup
// syscall fails at runtime clear `preferIoUring` and fall back to a
// `SyncIoManager`. Passing the flag by reference makes this probe-once: after
// the first failure, every subsequent call goes straight to the sync manager,
// so we don't repeat a failing syscall. Where `preadv2(RWF_NOWAIT)` is
// available, the manager serves page-cache hits first (`PageCacheFirstPolicy`,
// switched with `BatchManagerBase::setPageCacheFastPath`).
inline std::unique_ptr<BatchManagerBase> makeBatchManager(
    bool& preferIoUring, unsigned ringSize = 256) {
#ifdef QLEVER_HAS_IO_URING
  if (preferIoUring) {
    try {
      return detail::makeBatchManagerFor<IoUringPolicy>(ringSize);
    } catch (const std::exception& e) {
      preferIoUring = false;
      AD_LOG_WARN << "io_uring is compiled in but unavailable at runtime ("
                  << e.what()
                  << "); falling back to synchronous pread for vocabulary "
                     "lookups"
                  << std::endl;
    }
  }
#else
  preferIoUring = false;
#endif
  return detail::makeBatchManagerFor<SyncIoPolicy>(ringSize);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
