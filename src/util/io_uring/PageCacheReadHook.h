// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IO_URING_PAGECACHEREADHOOK_H
#define QLEVER_SRC_UTIL_IO_URING_PAGECACHEREADHOOK_H

#include <sys/uio.h>

#include <atomic>
#include <cstdint>

// The two seams between `readPageCacheHits` (in `PageCacheFirstPolicy.cpp`)
// and the kernel. Whether a read is served from the page cache depends on the
// kernel, the file system and the state of the page cache, so tests cannot
// produce `EAGAIN`, short reads or `EOPNOTSUPP` with real files. Instead, they
// replace the system call through `pageCacheRead()`, and re-arm the
// process-wide support latch after injecting `EOPNOTSUPP` (see
// `test/util/PageCacheReadTestHelpers.h`). Production code only uses this
// header in `PageCacheFirstPolicy.cpp`, which calls the hook and clears the
// latch, but never replaces the hook or sets the latch.

namespace ad_utility::detail {
// The signature of the one page-cache read per run of adjacent reads that
// `readPageCacheHits` makes: the arguments and the result of `preadv2` (the
// number of bytes read, or -1 with `errno` set), without the flags.
using PageCacheRead = int64_t (*)(int fd, const ::iovec* iov, int iovcnt,
                                  int64_t offset) noexcept;

// Read the file range of `fd` that starts at byte `offset` into the `iovcnt`
// buffers of `iov` (filled one after the other), but only as far as the range
// is in the page cache: with `preadv2(RWF_NOWAIT)`, the kernel copies the bytes
// that it can copy without waiting for the disk. Return the number of bytes
// read (fewer than requested, possibly 0, if the rest is not cached or the file
// ends), or -1 with `errno` set (`EAGAIN` if the first byte is not cached,
// `EOPNOTSUPP` or `ENOSYS` if the kernel or file system does not support the
// call). Where `preadv2(RWF_NOWAIT)` is not compiled in, fail with
// `EOPNOTSUPP`.
int64_t systemPageCacheRead(int fd, const ::iovec* iov, int iovcnt,
                            int64_t offset) noexcept;

// The function `readPageCacheHits` calls (initially `systemPageCacheRead`).
// Tests replace it by assigning through the returned reference, before any
// read runs; it must never be `nullptr`, because it is called unconditionally.
PageCacheRead& pageCacheRead() noexcept;

// The process-wide latch behind `pageCacheFastPathIsSupported()`: initially
// true, and cleared by `readPageCacheHits` once a page-cache read fails with
// `EOPNOTSUPP` or `ENOSYS`. Where the fast path is not compiled in,
// `pageCacheFastPathIsSupported()` is false regardless of the latch.
std::atomic<bool>& pageCacheFastPathSupported() noexcept;
}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_UTIL_IO_URING_PAGECACHEREADHOOK_H
