// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IO_URING_PAGECACHEFIRSTPOLICY_H
#define QLEVER_SRC_UTIL_IO_URING_PAGECACHEFIRSTPOLICY_H

#include <sys/uio.h>

#include <cstdint>
#include <functional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/HashSet.h"
#include "util/NoCopyNoMove.h"
#include "util/OverloadCallOperator.h"
#include "util/io_uring/IoUringManagerConcepts.h"

namespace ad_utility {

// The one read per run of adjacent reads that `PageCacheReader` makes: the
// arguments and the result of `preadv2` without its flags. Read the file range
// of `fd` that starts at byte `offset` into the `iovcnt` buffers of `iov`
// (filled one after the other), and return the number of bytes read, or -1
// with `errno` set.
using PageCacheRead = std::function<int64_t(int fd, const ::iovec* iov,
                                            int iovcnt, int64_t offset)>;

// The `PageCacheRead` of production code: `preadv2(RWF_NOWAIT)`, which copies
// only the bytes that the kernel can copy without waiting for the disk. It
// returns fewer bytes than requested (possibly 0) if the rest is not cached or
// the file ends, and fails with `EAGAIN` if the first byte is not cached, and
// with `EOPNOTSUPP`, `ENOSYS` or `EPERM` if the kernel, the file system or a
// seccomp filter does not allow the call. Where `preadv2(RWF_NOWAIT)` is not
// compiled in, it fails with `EOPNOTSUPP`.
int64_t systemPageCacheRead(int fd, const ::iovec* iov, int iovcnt,
                            int64_t offset) noexcept;

// Return whether `preadv2(RWF_NOWAIT)` is compiled in (Linux only). Without
// it, `PageCacheReader` never serves a read.
bool pageCacheFastPathIsCompiledIn() noexcept;

// The reads of a batch that `PageCacheReader::readPageCacheHits` did not
// serve, and that the caller therefore still has to issue through its regular
// path.
struct UnservedReads {
  // No read was served (none was in the page cache, the fast path is not
  // supported, or the batch is empty): the caller issues the batch unchanged.
  struct NothingRead {};
  // Every read of a non-empty batch was served: the caller issues nothing.
  struct AllRead {};
  // Some, but not all reads were served: `positions()` are the positions in
  // the batch of the reads that were not served, strictly ascending, neither
  // empty nor all positions of the batch. Only `fromNotServed` constructs one,
  // so no other `RemainingPositions` exists.
  class RemainingPositions {
   public:
    const std::vector<size_t>& positions() const { return positions_; }

   private:
    friend struct UnservedReads;
    explicit RemainingPositions(std::vector<size_t> positions)
        : positions_{std::move(positions)} {}
    std::vector<size_t> positions_;
  };
  std::variant<NothingRead, AllRead, RemainingPositions> reads_;

  // The `UnservedReads` of a batch of `numReads` reads of which the reads at
  // the strictly ascending positions `notServed` were not served: `NothingRead`
  // if that is every read (also for an empty batch), `AllRead` if it is none,
  // and `RemainingPositions` otherwise. Throw if `notServed` is not strictly
  // ascending or has a position that is not smaller than `numReads`.
  static UnservedReads fromNotServed(std::vector<size_t> notServed,
                                     size_t numReads);
};

// Serves the reads of a batch that are completely in the page cache with one
// `PageCacheRead` per run of adjacent file ranges (split into chunks of at most
// `IOV_MAX` reads). If a read fails with `EOPNOTSUPP`, `ENOSYS` or `EPERM`, the
// reader switches itself off for good: these errors do not go away, so
// retrying them would only add a failing system call per run. Each reader has
// its own switch (every batch manager has its own reader), so a vocabulary on a
// file system without `RWF_NOWAIT` does not switch the fast path off for the
// vocabularies on other file systems. Single-threaded use only.
class PageCacheReader {
 public:
  // `read` is `systemPageCacheRead` in production; tests inject a fake one.
  explicit PageCacheReader(PageCacheRead read = &systemPageCacheRead)
      : read_{std::move(read)} {}

  // Serve the reads of the batch that are in the page cache and return those
  // that were not served. The caller issues those through its regular path,
  // which also reports errors (`EAGAIN`, a short read and any other error leave
  // the reads of their run unserved). Read `i` reads `numBytes[i]` bytes at
  // file offset `offsets[i]` of `fd` into `buffers[i]`, so the three spans must
  // have the same length.
  [[nodiscard]] UnservedReads readPageCacheHits(
      int fd, ql::span<const size_t> numBytes, ql::span<const uint64_t> offsets,
      ql::span<char*> buffers);

  // False if the fast path is not compiled in, or once a read of this reader
  // has failed with `EOPNOTSUPP`, `ENOSYS` or `EPERM`.
  bool isSupported() const noexcept { return supported_; }

 private:
  PageCacheRead read_;
  bool supported_ = pageCacheFastPathIsCompiledIn();
};

namespace detail {
// The reads of a batch at the indices `positions` into the three equally long
// spans, in the order of `positions`, as the `numBytes`, `offsets` and
// `buffers` arguments of `Policy::addBatch`. The vectors are copies, but the
// pointers in `buffers_` still point to the caller's target buffers.
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
  // The elements of `values` at `positions`, copied into a vector.
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
// with a `PageCacheReader` and passes only the others on to `Policy`.
template <typename Policy>
class PageCacheFirstPolicy : public NoCopyNoMove {
  static_assert(
      ReadPolicyConcept<Policy>,
      "PageCacheFirstPolicy's Policy must satisfy the ReadPolicyConcept.");

 public:
  using BatchHandle = typename Policy::BatchHandle;

  // `ringSize` is forwarded to the constructor of the wrapped `Policy`, `read`
  // to the `PageCacheReader`.
  explicit PageCacheFirstPolicy(unsigned ringSize,
                                PageCacheRead read = &systemPageCacheRead)
      : policy_(ringSize), reader_{std::move(read)} {}

  // Switch the fast path on (the default) or off for the batches that are added
  // afterwards.
  void setPageCacheFastPath(bool enabled) noexcept { enabled_ = enabled; }

  // Serve the reads of the batch that are in the page cache and pass the others
  // on to the decorated policy. If all reads are served, the decorated policy
  // is not involved at all, neither here nor in `wait`. The arguments have the
  // contract of `PageCacheReader::readPageCacheHits` and `Policy::addBatch`.
  void addBatch(int fd, ql::span<const size_t> numBytes,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle) {
    auto forwardAll = [&](const UnservedReads::NothingRead&) {
      policy_.addBatch(fd, numBytes, offsets, buffers, handle);
    };
    auto forwardNothing = [&](const UnservedReads::AllRead&) {
      batchesServedCompletely_.insert(handle);
    };
    auto forwardRemaining =
        [&](const UnservedReads::RemainingPositions& remaining) {
          detail::SelectedReads missed{numBytes, offsets, buffers,
                                       remaining.positions()};
          policy_.addBatch(fd, missed.numBytes_, missed.offsets_,
                           missed.buffers_, handle);
        };
    const UnservedReads unserved =
        enabled_ ? reader_.readPageCacheHits(fd, numBytes, offsets, buffers)
                 : UnservedReads{UnservedReads::NothingRead{}};
    std::visit(
        OverloadCallOperator{forwardAll, forwardNothing, forwardRemaining},
        unserved.reads_);
  }

  // Block until every read of the batch `handle` has completed. The reads that
  // `addBatch` served from the page cache are complete already, so this only
  // waits for the reads that were passed on, and returns at once without
  // calling the decorated policy if there were none.
  void wait(BatchHandle handle) {
    if (batchesServedCompletely_.erase(handle) > 0) {
      return;
    }
    policy_.wait(handle);
  }

 private:
  Policy policy_;
  PageCacheReader reader_;
  bool enabled_ = true;
  // The batches whose reads `addBatch` all served from the page cache, so that
  // the decorated policy has nothing to do for them. `wait` removes a batch.
  ad_utility::HashSet<BatchHandle> batchesServedCompletely_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IO_URING_PAGECACHEFIRSTPOLICY_H
