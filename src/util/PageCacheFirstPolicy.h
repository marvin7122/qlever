// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PAGECACHEFIRSTPOLICY_H
#define QLEVER_SRC_UTIL_PAGECACHEFIRSTPOLICY_H

#include <sys/uio.h>

#include <cstdint>
#include <type_traits>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/BatchHandle.h"
#include "util/IoUringManagerConcepts.h"

namespace ad_utility {

// Serve the reads of a batch that are completely in the page cache with
// non-blocking `preadv2(RWF_NOWAIT)` calls, one call per run of adjacent file
// ranges (split into chunks of at most `IOV_MAX` reads), and return the
// positions of the other reads in ascending order. The caller issues those
// through its regular path, which also reports errors.
// Precondition: the three spans have the same length.
std::vector<size_t> readPageCacheHits(int fd, ql::span<const size_t> numBytes,
                                      ql::span<const uint64_t> offsets,
                                      ql::span<char*> buffers);

// False once `readPageCacheHits` found that `RWF_NOWAIT` is not supported, or
// if it is not available at compile time.
bool pageCacheFastPathIsSupported();

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
// The reads of a batch at `positions`, as the three arguments of
// `ReadPolicy::addBatch`.
struct SelectedReads {
  std::vector<size_t> numBytes_;
  std::vector<uint64_t> offsets_;
  std::vector<char*> buffers_;

  SelectedReads(ql::span<const size_t> numBytes,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                ql::span<const size_t> positions)
      : numBytes_{selectAt(numBytes, positions)},
        offsets_{selectAt(offsets, positions)},
        buffers_{selectAt(buffers, positions)} {}

 private:
  template <typename T>
  static std::vector<std::remove_const_t<T>> selectAt(
      ql::span<T> values, ql::span<const size_t> positions) {
    return ::ranges::to<std::vector<std::remove_const_t<T>>>(
        positions |
        ql::views::transform([values](size_t i) { return values[i]; }));
  }
};
}  // namespace detail

// A `ReadPolicy` that serves the reads of a batch that are in the page cache
// with `readPageCacheHits` and passes only the others on to `Policy`.
template <typename Policy>
class PageCacheFirstPolicy {
  static_assert(
      detail::ReadPolicyConcept<Policy>,
      "PageCacheFirstPolicy's Policy must satisfy the ReadPolicyConcept.");

 public:
  // `explicit`, so that a bare ring size cannot silently convert to a
  // policy where a policy is expected.
  explicit PageCacheFirstPolicy(unsigned ringSize) : policy_(ringSize) {}

  // Not copyable or movable: it owns the policy it decorates, so a copy
  // would double-own that policy, and a move is only possible once the
  // decorated policy supports one (it does not, see there). No caller
  // needs either: the policy is only ever held inside its `BatchManager`.
  PageCacheFirstPolicy(const PageCacheFirstPolicy&) = delete;
  PageCacheFirstPolicy& operator=(const PageCacheFirstPolicy&) = delete;
  PageCacheFirstPolicy(PageCacheFirstPolicy&&) = delete;
  PageCacheFirstPolicy& operator=(PageCacheFirstPolicy&&) = delete;

  // Switch the fast path on (the default) or off for the batches that are added
  // afterwards.
  void setPageCacheFastPath(bool enabled) { enabled_ = enabled; }

  // Serve the reads of the batch that are in the page cache and pass the others
  // on to the decorated policy. The contract of the arguments is the one of
  // `readPageCacheHits` and of `Policy::addBatch`.
  void addBatch(int fd, ql::span<const size_t> numBytes,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle) {
    if (!enabled_ || !pageCacheFastPathIsSupported()) {
      policy_.addBatch(fd, numBytes, offsets, buffers, handle);
      return;
    }
    const std::vector<size_t> notServed =
        readPageCacheHits(fd, numBytes, offsets, buffers);
    if (notServed.size() == numBytes.size()) {
      // Nothing was served, so the batch is passed on unchanged.
      policy_.addBatch(fd, numBytes, offsets, buffers, handle);
      return;
    }
    detail::SelectedReads missed{numBytes, offsets, buffers, notServed};
    policy_.addBatch(fd, missed.numBytes_, missed.offsets_, missed.buffers_,
                     handle);
  }

  // Block until every read of the batch `handle` has completed.
  void wait(BatchHandle handle) { policy_.wait(handle); }

 private:
  Policy policy_;
  bool enabled_ = true;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PAGECACHEFIRSTPOLICY_H
