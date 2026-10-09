// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H
#define QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "util/IoUringManager.h"

namespace pageCacheReadTestHelpers {

// Replace the `preadv2(RWF_NOWAIT)` call of `ad_utility::readPageCacheHits`
// with `function` for the lifetime of this object. On destruction the
// previous function is restored and an injected `EOPNOTSUPP` is undone, so
// later tests see the fast path as supported again.
class ScopedPageCacheRead {
 public:
  explicit ScopedPageCacheRead(ad_utility::detail::PageCacheRead function)
      : previous_{
            std::exchange(ad_utility::detail::pageCacheRead(), function)} {}
  ~ScopedPageCacheRead() {
    ad_utility::detail::pageCacheRead() = previous_;
    ad_utility::detail::resetPageCacheFastPathSupport();
  }
  ScopedPageCacheRead(const ScopedPageCacheRead&) = delete;
  ScopedPageCacheRead& operator=(const ScopedPageCacheRead&) = delete;

 private:
  ad_utility::detail::PageCacheRead previous_;
};

// A page-cache read that finds nothing cached (`EAGAIN`).
inline int64_t nothingCached(int, const ::iovec*, int, int64_t) {
  errno = EAGAIN;
  return -1;
}

// A page-cache read on a file system that rejects `RWF_NOWAIT`.
inline int64_t notSupported(int, const ::iovec*, int, int64_t) {
  errno = EOPNOTSUPP;
  return -1;
}

// Number of page-cache reads observed by `countingPageCacheRead` below.
// Reset it before the measured section; like the injected function itself it
// is process-wide, so only one test may use it at a time (GoogleTest runs
// tests sequentially by default).
inline std::atomic<size_t> numCountedPageCacheReads{0};

// A page-cache read that counts the call in `numCountedPageCacheReads` and
// then delegates to `Delegate` (the real `preadv2(RWF_NOWAIT)` by default).
// Inject it with `ScopedPageCacheRead` to assert how many fast-path reads a
// lookup issued, e.g. `countingPageCacheRead<&notSupported>` to count the
// probes on a file system that rejects `RWF_NOWAIT`.
template <ad_utility::detail::PageCacheRead Delegate =
              &ad_utility::detail::systemPageCacheRead>
int64_t countingPageCacheRead(int fd, const ::iovec* iov, int iovcnt,
                              int64_t offset) {
  numCountedPageCacheReads.fetch_add(1, std::memory_order_relaxed);
  return Delegate(fd, iov, iovcnt, offset);
}

}  // namespace pageCacheReadTestHelpers

#endif  // QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H
