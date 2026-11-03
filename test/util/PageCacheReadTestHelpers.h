// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H
#define QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H

#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "backports/span.h"
#include "util/NoCopyNoMove.h"
#include "util/io_uring/PageCacheFirstPolicy.h"
#include "util/io_uring/PageCacheReadHook.h"

namespace pageCacheReadTestHelpers {

//______________________________________________________________________________
// Switch the page-cache fast path on again after an injected (or a real)
// `EOPNOTSUPP` or `ENOSYS` switched it off for the process. Has no effect where
// the fast path is not compiled in (`pageCacheFastPathIsSupported()` stays
// false there).
inline void resetPageCacheFastPathSupport() noexcept {
  ad_utility::detail::pageCacheFastPathSupported().store(true);
}

// Replace the `preadv2(RWF_NOWAIT)` call of `ad_utility::readPageCacheHits`
// with `function` for the lifetime of this object, and restore the state
// that the test found on destruction. Two pieces of global state are
// involved, and both are restored:
//
// 1. The read function itself: the constructor swaps `function` into
//    `detail::pageCacheRead()` and remembers the previous one in
//    `previous_`; the destructor writes it back.
// 2. The process-wide fast-path latch (`pageCacheFastPathIsSupported()`):
//    an injected `EOPNOTSUPP` read switches the latch off for the rest of
//    the process (that is what `switchOffFastPathIfUnsupported` does with
//    a real failing call, too). `wasSupported_` remembers whether the
//    latch was on when this object was constructed. If it was, the
//    destructor calls `resetPageCacheFastPathSupport()` to switch it on
//    again, undoing an `EOPNOTSUPP` that the test itself injected. If it
//    was already off (a platform without `RWF_NOWAIT`), nothing is
//    reset: the test must not pretend the fast path works there.
class ScopedPageCacheRead : public ad_utility::NoCopyNoMove {
 public:
  explicit ScopedPageCacheRead(ad_utility::detail::PageCacheRead function)
      : previous_{
            std::exchange(ad_utility::detail::pageCacheRead(), function)} {}
  ~ScopedPageCacheRead() {
    // Restore 1: the read function that was in place before.
    ad_utility::detail::pageCacheRead() = previous_;
    // Restore 2: undo an injected `EOPNOTSUPP`, but only if the fast
    // path was supported before this test (see the class comment).
    if (wasSupported_) {
      resetPageCacheFastPathSupport();
    }
  }

 private:
  const ad_utility::detail::PageCacheRead previous_;
  bool wasSupported_ = ad_utility::pageCacheFastPathIsSupported();
};

//______________________________________________________________________________
// Read the file range of `fd` that starts at `offset` into the buffers of
// `iov` with blocking `pread` calls, like a `preadv2(RWF_NOWAIT)` for which the
// whole range is cached: return the number of bytes read (fewer than requested
// only at the end of the file), or -1 on an I/O error. Unlike the real call, it
// works on every file system (`RWF_NOWAIT` is rejected with `EOPNOTSUPP` on
// overlayfs, for example in the docker builds) and does not depend on the file
// still being cached.
inline int64_t fullyCached(int fd, const ::iovec* iov, int iovcnt,
                           int64_t offset) noexcept {
  int64_t numBytesReadTotal = 0;
  for (const ::iovec& buffer :
       ql::span<const ::iovec>{iov, static_cast<size_t>(iovcnt)}) {
    auto* target = static_cast<char*>(buffer.iov_base);
    size_t numBytesRemaining = buffer.iov_len;
    while (numBytesRemaining > 0) {
      const ssize_t numBytesRead =
          ::pread(fd, target, numBytesRemaining,
                  static_cast<off_t>(offset + numBytesReadTotal));
      if (numBytesRead < 0) {
        return -1;
      }
      if (numBytesRead == 0) {
        return numBytesReadTotal;
      }
      const auto numBytesReadUnsigned = static_cast<size_t>(numBytesRead);
      target += numBytesReadUnsigned;
      numBytesRemaining -= numBytesReadUnsigned;
      numBytesReadTotal += numBytesRead;
    }
  }
  return numBytesReadTotal;
}

//______________________________________________________________________________
// Report that nothing is cached (`EAGAIN`).
inline int64_t nothingCached(int, const ::iovec*, int, int64_t) noexcept {
  errno = EAGAIN;
  return -1;
}

//______________________________________________________________________________
// Report that `RWF_NOWAIT` is not supported (`EOPNOTSUPP`).
inline int64_t notSupported(int, const ::iovec*, int, int64_t) noexcept {
  errno = EOPNOTSUPP;
  return -1;
}

}  // namespace pageCacheReadTestHelpers

#endif  // QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H
