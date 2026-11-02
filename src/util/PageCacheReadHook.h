// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PAGECACHEREADHOOK_H
#define QLEVER_SRC_UTIL_PAGECACHEREADHOOK_H

#include <sys/uio.h>

#include <cstdint>

namespace ad_utility {

// In `detail` because this hook exists only so that tests can inject
// failures of the page-cache read: production code must call
// `readPageCacheHits` (in `PageCacheFirstPolicy.h`) and must not call or
// replace this pointer. It has its own header so that the interface
// header of the page-cache fast path does not carry a test-only seam.
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
// Contract: the pointer may only be swapped while no `readPageCacheHits` call
// is in flight (tests set it before spawning workers and restore it after
// joining them); concurrent mutation is a bug, not a supported use.
PageCacheRead& pageCacheRead();
// Undo the effect of an `EOPNOTSUPP` on `pageCacheFastPathIsSupported()`, for
// tests that injected one.
void resetPageCacheFastPathSupport();
}  // namespace detail

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PAGECACHEREADHOOK_H
