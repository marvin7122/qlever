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

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/IoUringManagerConcepts.h"
#include "util/Log.h"
#include "util/PageCacheFirstPolicy.h"
#include "util/io_uring/BatchOfReads.h"

#ifdef QLEVER_HAS_IO_URING
#include "util/io_uring/IoUringPolicy.h"
#endif

namespace ad_utility {

// Abstract base of `BatchManager` so a pool can hold managers of different
// `ReadPolicy`s behind a unified interface and pick the backend at runtime.
class BatchManagerBase {
 public:
  virtual ~BatchManagerBase() = default;

  // Read a batch: read `i` reads `numBytes[i]` bytes from `fd`, starting at
  // byte `offsets[i]` of the file, into `buffers[i]`. Return (or throw) only
  // once no read of the batch can write into its buffer any more, so the
  // caller may free the buffers right away. Throw if the three spans differ in
  // length or a read fails or is short.
  virtual void readBatch(int fd, ql::span<const size_t> numBytes,
                         ql::span<const uint64_t> offsets,
                         ql::span<char*> buffers) = 0;

  // Switch the page-cache fast path of this manager on or off (see
  // `PageCacheFirstPolicy`). Takes effect for the batches read afterwards.
  // Managers without a page-cache fast path ignore it.
  virtual void setPageCacheFastPath([[maybe_unused]] bool enabled) noexcept {}
};

// `BatchManager` packs the input spans into a `BatchOfReads` (which checks
// their lengths) and delegates the reads from the underlying Vocabulary to the
// `Policy`, which must satisfy the `ReadPolicy` concept above.
template <typename ReadPolicy>
class BatchManager final : public BatchManagerBase {
  static_assert(
      ReadPolicyConcept<ReadPolicy>,
      "BatchManager's ReadPolicy must satisfy the ReadPolicyConcept concept.");

 public:
  explicit BatchManager(unsigned ringSize = 256) : policy_(ringSize) {}

  BatchManager(const BatchManager&) = delete;
  BatchManager& operator=(const BatchManager&) = delete;

  void readBatch(int fd, ql::span<const size_t> numBytes,
                 ql::span<const uint64_t> offsets,
                 ql::span<char*> buffers) override {
    // Delegate the I/O work to the policy.
    policy_.readBatch(fd, BatchOfReads{numBytes, offsets, buffers});
  }

  // Forward the switch to the policy if it has one.
  void setPageCacheFastPath([[maybe_unused]] bool enabled) noexcept override {
    if constexpr (CPP_requires_ref(detail::HasPageCacheFastPathSwitch,
                                   ReadPolicy)) {
      policy_.setPageCacheFastPath(enabled);
    }
  }

 private:
  [[no_unique_address]] ReadPolicy policy_;
};

// Fallback implementation for `IoUringPolicy` (in `io_uring/IoUringPolicy.h`).
// Schedules pread calls in a synchronous (blocking) manner. Single-threaded use
// only.
struct SyncIoPolicy {
  // The ring size is ignored; the parameter exists only so the policy is
  // constructible the same way as `IoUringPolicy`.
  explicit SyncIoPolicy(unsigned = 256) {}

  ~SyncIoPolicy() = default;
  SyncIoPolicy(const SyncIoPolicy&) = delete;
  SyncIoPolicy& operator=(const SyncIoPolicy&) = delete;

  // Immediately execute the `reads` from file descriptor `fd` synchronously.
  // This blocks the calling thread.
  void readBatch(int fd, const BatchOfReads& reads) const;

  // Read exactly `numBytes` bytes from file descriptor `fd` at `fileOffset`
  // (from the start of the file) into `targetBuffer`. Throws exception if the
  // read fails or returns fewer bytes than requested (a partial read or end of
  // file), since every read must be fully satisfied.
  static void readFullyOrThrow(int fd, char* targetBuffer, size_t numBytes,
                               uint64_t fileOffset);
};

#ifdef QLEVER_HAS_IO_URING
using BatchIoManager = BatchManager<IoUringPolicy>;
#else
using BatchIoManager = BatchManager<SyncIoPolicy>;
#endif

namespace detail {
// Build a `BatchManager` for `Policy`, decorated with `PageCacheFirstPolicy` if
// the page-cache fast path is supported.
template <typename Policy>
std::unique_ptr<BatchManagerBase> makeBatchManagerFor(unsigned ringSize) {
  static_assert(ReadPolicyConcept<Policy>,
                "makeBatchManagerFor's Policy must satisfy the "
                "ReadPolicyConcept.");
  if (pageCacheFastPathIsSupported()) {
    return std::make_unique<BatchManager<PageCacheFirstPolicy<Policy>>>(
        ringSize);
  }
  return std::make_unique<BatchManager<Policy>>(ringSize);
}
}  // namespace detail

// Build a batch manager. When io_uring is compiled in and the runtime flag
// `preferIoUring` is set, try to build an `IoUringPolicy` manager. If its ring
// cannot be set up at runtime, or the kernel does not confirm the operations
// that the ring needs (see `makeLiburingRing`), clear `preferIoUring` and fall
// back to a `SyncIoPolicy` manager. Passing the flag by reference makes this
// probe-once: after the first failure, every subsequent call goes straight to
// the sync manager, so we don't repeat a failing setup. Where
// `preadv2(RWF_NOWAIT)` is available, the manager serves page-cache hits first
// (`PageCacheFirstPolicy`, switched with
// `BatchManagerBase::setPageCacheFastPath`).
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
