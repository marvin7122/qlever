// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/io_uring/PageCacheFirstPolicy.h"

#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

// `preadv2` with `RWF_NOWAIT` exists on Linux only. Emscripten's headers
// define `RWF_NOWAIT`, but its libc has no `preadv2`; there, and wherever
// `RWF_NOWAIT` is missing, the fast path is compiled out.
#if defined(__linux__) && defined(RWF_NOWAIT) && !defined(__EMSCRIPTEN__)
#define QL_PAGE_CACHE_FAST_PATH
#endif

//______________________________________________________________________________
int64_t systemPageCacheRead(int fd, const ::iovec* iov, int iovcnt,
                            int64_t offset) noexcept {
#ifdef QL_PAGE_CACHE_FAST_PATH
  return preadv2(fd, iov, iovcnt, static_cast<off_t>(offset), RWF_NOWAIT);
#else
  // Fail like a kernel that does not support `RWF_NOWAIT`. A
  // `PageCacheReader` never calls this function then (it starts switched
  // off), but it is the default `PageCacheRead` on every platform.
  (void)fd;
  (void)iov;
  (void)iovcnt;
  (void)offset;
  errno = EOPNOTSUPP;
  return -1;
#endif
}

//______________________________________________________________________________
bool pageCacheFastPathIsCompiledIn() noexcept {
#ifdef QL_PAGE_CACHE_FAST_PATH
  return true;
#else
  return false;
#endif
}

// The fast-path helpers below only exist where `preadv2(RWF_NOWAIT)` is
// compiled in; without it, `readPageCacheHits` serves nothing.
#ifdef QL_PAGE_CACHE_FAST_PATH
namespace {
// The elements of the range of reads in `readPageCacheHits`: the position of a
// read in its batch, its number of bytes, its file offset and its buffer.
constexpr auto positionOf = [](const auto& read) { return std::get<0>(read); };
constexpr auto numBytesOf = [](const auto& read) { return std::get<1>(read); };
constexpr auto fileOffsetOf = [](const auto& read) {
  return std::get<2>(read);
};
constexpr auto toIovec = [](const auto& read) {
  return ::iovec{std::get<3>(read), numBytesOf(read)};
};
// True if the file range of `next` starts where that of `previous` ends.
constexpr auto areAdjacent = [](const auto& previous, const auto& next) {
  return fileOffsetOf(previous) + numBytesOf(previous) == fileOffsetOf(next);
};

//______________________________________________________________________________
// True if a page-cache read that failed with `errorCode` will fail for every
// later run as well: the kernel or file system rejects `RWF_NOWAIT`
// (`EOPNOTSUPP`), `preadv2` does not exist (`ENOSYS`, Linux < 4.6), or a
// seccomp filter forbids it (`ENOSYS` or `EPERM`).
bool failsForGood(int errorCode) {
  return errorCode == EOPNOTSUPP || errorCode == ENOSYS || errorCode == EPERM;
}

//______________________________________________________________________________
// Log that a `PageCacheReader` switched itself off. Every batch manager has its
// own reader, so the message is logged only for the first one in the process.
void warnOnceThatTheFastPathIsOff(int errorCode) {
  static std::atomic<bool> warned{false};
  if (!warned.exchange(true)) {
    AD_LOG_WARN << "preadv2 with RWF_NOWAIT failed ("
                << std::strerror(errorCode)
                << "); the vocabulary reads of this batch manager, and of "
                   "every other one that hits this error, are done without "
                   "the page-cache fast path from now on"
                << std::endl;
  }
}

//______________________________________________________________________________
// Read a `run` of adjacent reads with one call of `read` and return how many of
// its reads, from the front, were read completely. `EAGAIN`, a short read and
// any other error leave the remaining reads unserved; an error for which
// `failsForGood` is true also clears `supported`. `iovecs` is scratch space
// for the call that the caller keeps across the runs of a batch, so that a
// batch of many short runs does not allocate once per run.
template <typename Run>
size_t serveRun(int fd, Run run, const PageCacheRead& read, bool& supported,
                std::vector<::iovec>& iovecs) {
  iovecs.clear();
  ::ranges::push_back(iovecs, run | ::ranges::views::transform(toIovec));
  const int64_t numBytesRead =
      read(fd, iovecs.data(), static_cast<int>(iovecs.size()),
           static_cast<int64_t>(fileOffsetOf(*::ranges::begin(run))));
  if (numBytesRead < 0) {
    // Read `errno` before any other library call can change it.
    const int errorCode = errno;
    if (failsForGood(errorCode)) {
      supported = false;
      warnOnceThatTheFastPathIsOff(errorCode);
    }
    return 0;
  }
  // The end of each read within the run; a read is served if its end was read.
  auto readEnds = iovecs | ::ranges::views::transform(&::iovec::iov_len) |
                  ::ranges::views::partial_sum;
  const auto numBytesReadUnsigned = static_cast<size_t>(numBytesRead);
  return static_cast<size_t>(
      ::ranges::count_if(readEnds, [numBytesReadUnsigned](size_t end) {
        return end <= numBytesReadUnsigned;
      }));
}
}  // namespace
#endif

//______________________________________________________________________________
UnservedReads PageCacheReader::readPageCacheHits(
    int fd, ql::span<const size_t> numBytes, ql::span<const uint64_t> offsets,
    ql::span<char*> buffers) {
  AD_CONTRACT_CHECK(
      offsets.size() == numBytes.size() && buffers.size() == numBytes.size(),
      "The three spans describe the same reads, so they "
      "must have the same length.");
#ifdef QL_PAGE_CACHE_FAST_PATH
  if (!supported_) {
    return {UnservedReads::NothingRead{}};
  }
  const auto positions = ::ranges::views::iota(size_t{0}, numBytes.size());
  // View over the three spans and `positions`; it is only used inside
  // this call, while all four are alive.
  auto reads = ::ranges::views::zip(positions, numBytes, offsets, buffers);
  std::vector<size_t> notServed;
  notServed.reserve(numBytes.size());
  // The `iovec`s of the run that is currently served; at most `IOV_MAX`.
  std::vector<::iovec> iovecs;
  iovecs.reserve(std::min(numBytes.size(), static_cast<size_t>(IOV_MAX)));
  // One `preadv2` call reads one contiguous range, so reads are grouped
  // into maximal runs of adjacent file ranges and served run by run.
  for (auto adjacentReads : reads | ::ranges::views::chunk_by(areAdjacent)) {
    // One `preadv2` call takes at most `IOV_MAX` buffers.
    for (auto run :
         adjacentReads | ::ranges::views::chunk(static_cast<size_t>(IOV_MAX))) {
      const size_t numServed =
          supported_ ? serveRun(fd, run, read_, supported_, iovecs) : 0;
      ::ranges::push_back(notServed,
                          run | ::ranges::views::drop(numServed) |
                              ::ranges::views::transform(positionOf));
    }
  }
  if (notServed.size() == numBytes.size()) {
    return {UnservedReads::NothingRead{}};
  }
  return {UnservedReads::RemainingPositions{std::move(notServed)}};
#else
  (void)fd;
  return {UnservedReads::NothingRead{}};
#endif  // QL_PAGE_CACHE_FAST_PATH
}

}  // namespace ad_utility
