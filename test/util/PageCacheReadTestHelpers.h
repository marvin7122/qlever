// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H
#define QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H

#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/io_uring/PageCacheFirstPolicy.h"

// Fake `ad_utility::PageCacheRead`s for the tests of `PageCacheReader` and
// `PageCacheFirstPolicy`. Whether a read is served from the page cache depends
// on the kernel, the file system and the state of the page cache, so tests
// cannot produce `EAGAIN`, short reads or `EOPNOTSUPP` with real files. They
// pass one of these functions to the reader or policy under test instead.
namespace pageCacheReadTestHelpers {

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
  int64_t numberOfBytesReadTotal = 0;
  for (const ::iovec& buffer :
       ql::span<const ::iovec>{iov, static_cast<size_t>(iovcnt)}) {
    auto* target = static_cast<char*>(buffer.iov_base);
    size_t numberOfBytesRemaining = buffer.iov_len;
    while (numberOfBytesRemaining > 0) {
      const ssize_t numBytesRead =
          ::pread(fd, target, numberOfBytesRemaining,
                  static_cast<off_t>(offset + numberOfBytesReadTotal));
      if (numBytesRead < 0) {
        return -1;
      }
      if (numBytesRead == 0) {
        return numberOfBytesReadTotal;
      }
      const auto numberOfBytesReadUnsigned = static_cast<size_t>(numBytesRead);
      target += numberOfBytesReadUnsigned;
      numberOfBytesRemaining -= numberOfBytesReadUnsigned;
      numberOfBytesReadTotal += numBytesRead;
    }
  }
  return numberOfBytesReadTotal;
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

//______________________________________________________________________________
// A page-cache read that always fails with `errorCode`.
inline ad_utility::PageCacheRead failsWith(int errorCode) {
  return [errorCode](int, const ::iovec*, int, int64_t) -> int64_t {
    errno = errorCode;
    return -1;
  };
}

//______________________________________________________________________________
// `read`, but every call also increments `numberOfCalls`, which must outlive
// the returned function.
inline ad_utility::PageCacheRead counted(size_t& numberOfCalls,
                                         ad_utility::PageCacheRead read) {
  return [&numberOfCalls, read = std::move(read)](int fd, const ::iovec* iov,
                                                  int iovcnt, int64_t offset) {
    ++numberOfCalls;
    return read(fd, iov, iovcnt, offset);
  };
}

//______________________________________________________________________________
// A page-cache read for which exactly the runs that start at one of
// `cachedOffsets` are cached (as with `fullyCached`); every other run is not
// cached at all (`EAGAIN`).
inline ad_utility::PageCacheRead cachedAt(std::vector<int64_t> cachedOffsets) {
  return [cachedOffsets = std::move(cachedOffsets)](
             int fd, const ::iovec* iov, int iovcnt, int64_t offset) {
    if (::ranges::find(cachedOffsets, offset) == cachedOffsets.end()) {
      return nothingCached(fd, iov, iovcnt, offset);
    }
    return fullyCached(fd, iov, iovcnt, offset);
  };
}

}  // namespace pageCacheReadTestHelpers

// Skip the current test where `preadv2(RWF_NOWAIT)` is not compiled in: there,
// a `PageCacheReader` never calls its `PageCacheRead`.
#define SKIP_WITHOUT_PAGE_CACHE_FAST_PATH()                   \
  if (!ad_utility::pageCacheFastPathIsCompiledIn()) {         \
    GTEST_SKIP() << "preadv2(RWF_NOWAIT) is not compiled in"; \
  }

#endif  // QLEVER_TEST_UTIL_PAGECACHEREADTESTHELPERS_H
