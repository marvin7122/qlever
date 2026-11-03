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

#include <cstdint>
#include <memory>

#include "backports/span.h"
#include "util/Algorithm.h"
#include "util/BatchHandle.h"
#include "util/IoUringManagerConcepts.h"
#include "util/IoUringPolicy.h"
#include "util/MonotonicCounter.h"
#include "util/NoCopyNoMove.h"
#include "util/PageCacheFirstPolicy.h"
#include "util/RingSize.h"
#include "util/SyncIoPolicy.h"

namespace ad_utility {

// Abstract base of `BatchManager` so a pool can hold managers of different
// `ReadPolicy`s behind a unified interface and pick the backend at runtime.
class BatchManagerBase {
 public:
  using BatchHandle = ::ad_utility::BatchHandle;

  // Virtual, because the pool owns its managers as
  // `std::unique_ptr<BatchManagerBase>`.
  virtual ~BatchManagerBase() = default;

  [[nodiscard]] virtual BatchHandle addBatch(int fd,
                                             ql::span<const size_t> numBytes,
                                             ql::span<const uint64_t> offsets,
                                             ql::span<char*> buffers) = 0;

  virtual void wait(BatchHandle handle) = 0;

  // Switch the page-cache fast path of this manager on or off (see
  // `PageCacheFirstPolicy`). Takes effect for the batches added afterwards.
  // Managers without a page-cache fast path ignore it.
  virtual void setPageCacheFastPath([[maybe_unused]] bool enabled) noexcept {}
};

// `BatchManager` owns the batch bookkeeping (minting a `BatchHandle` per batch,
// validating the input spans) and delegates the reads from the underlying
// Vocabulary to the `ReadPolicy`, which must satisfy
// `detail::ReadPolicyConcept` from `IoUringManagerConcepts.h`. Neither copyable
// nor movable: it owns its policy (for `IoUringPolicy` the io_uring ring), and
// the pool holds managers by `std::unique_ptr`.
template <typename ReadPolicy>
class BatchManager final : public BatchManagerBase, NoCopyNoMove {
  static_assert(
      detail::ReadPolicyConcept<ReadPolicy>,
      "BatchManager's ReadPolicy must satisfy the ReadPolicyConcept.");

 public:
  using BatchHandle = ::ad_utility::BatchHandle;

  // `explicit`, so that a bare ring size cannot silently convert to a
  // manager where a `BatchManager` is expected.
  explicit BatchManager(RingSize ringSize = RingSize{}) : policy_(ringSize) {}

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
  void setPageCacheFastPath([[maybe_unused]] bool enabled) noexcept override {
    if constexpr (CPP_requires_ref(detail::HasPageCacheFastPathSwitch,
                                   ReadPolicy)) {
      policy_.setPageCacheFastPath(enabled);
    }
  }

 private:
  [[no_unique_address]] ReadPolicy policy_;

  // Mints the handles that `addBatch` returns. They only identify batches, so
  // uniqueness, which `MonotonicCounter` guarantees, is all that is needed.
  MonotonicCounter nextBatchHandle_;
};

// Build a batch manager. Which backend is used is a runtime decision:
// when io_uring is compiled in and `preferIoUring` is set, try an
// `IoUringPolicy` manager; if its setup syscall fails at runtime, clear
// `preferIoUring` and fall back to a `SyncIoPolicy` manager.
//
// `preferIoUring` is an in/out parameter that makes the probe happen once:
// pass the same flag to every call that builds managers of one pool, so that
// after the first failure every later call goes straight to the sync manager
// instead of repeating the failing syscall. `VocabularyOnDisk::open`
// initializes it from the `vocabulary-prefer-io-uring` runtime parameter.
//
// Where `preadv2(RWF_NOWAIT)` is available, the manager serves page-cache
// hits first (`PageCacheFirstPolicy`, the default) and the rest through
// the chosen policy. The fast path can be switched off and on at runtime
// via `BatchManagerBase::setPageCacheFastPath`, as `VocabularyOnDisk`
// does per lookup with the `vocabulary-page-cache-fast-path` runtime
// parameter.
std::unique_ptr<BatchManagerBase> makeBatchManager(
    bool& preferIoUring, RingSize ringSize = RingSize{});

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGER_H
