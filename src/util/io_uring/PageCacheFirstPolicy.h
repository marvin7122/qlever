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

#include <cstdint>
#include <type_traits>
#include <variant>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/NoCopyNoMove.h"
#include "util/OverloadCallOperator.h"
#include "util/io_uring/IoUringManagerConcepts.h"

namespace ad_utility {

// The reads of a batch that `readPageCacheHits` did not serve, and that the
// caller therefore still has to issue through its regular path.
struct UnservedReads {
  // No read was served (none was in the page cache, the fast path is not
  // supported, or the batch is empty): the caller issues the batch unchanged.
  struct NothingRead {};
  // At least one read was served. `positions_` are the positions in the batch
  // of the reads that were not served, in ascending order; empty if every read
  // was served.
  struct RemainingPositions {
    std::vector<size_t> positions_;
  };
  std::variant<NothingRead, RemainingPositions> reads_;
};

// Serve the reads of a batch that are completely in the page cache with
// non-blocking `preadv2(RWF_NOWAIT)` calls, one call per run of adjacent file
// ranges (split into chunks of at most `IOV_MAX` reads), and return the reads
// that were not served. The caller issues those through its regular path,
// which also reports errors. A call that fails with `EOPNOTSUPP` or `ENOSYS`
// switches the fast path off for the whole process (see
// `pageCacheFastPathIsSupported`). The three spans describe the same reads, so
// they must have the same length.
[[nodiscard]] UnservedReads readPageCacheHits(int fd,
                                              ql::span<const size_t> numBytes,
                                              ql::span<const uint64_t> offsets,
                                              ql::span<char*> buffers);

// Return whether `readPageCacheHits` may use `preadv2(RWF_NOWAIT)`: false if it
// is not compiled in, or once a call has failed with `EOPNOTSUPP` or `ENOSYS`.
// That latch is process-wide.
bool pageCacheFastPathIsSupported() noexcept;

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
// with `readPageCacheHits` and passes only the others on to `Policy`.
template <typename Policy>
class PageCacheFirstPolicy : public NoCopyNoMove {
  static_assert(
      ReadPolicyConcept<Policy>,
      "PageCacheFirstPolicy's Policy must satisfy the ReadPolicyConcept.");

 public:
  using BatchHandle = typename Policy::BatchHandle;

  // `ringSize` is forwarded to the constructor of the wrapped `Policy`.
  explicit PageCacheFirstPolicy(unsigned ringSize) : policy_(ringSize) {}

  // Switch the fast path on (the default) or off for the batches that are added
  // afterwards.
  void setPageCacheFastPath(bool enabled) noexcept { enabled_ = enabled; }

  // Serve the reads of the batch that are in the page cache and pass the others
  // on to the decorated policy. If all reads are served, the decorated policy
  // gets an empty batch, so `Policy` must accept `addBatch` with no reads
  // (`ReadPolicyConcept` cannot express that; the test
  // `IoUringManagerTest.emptyBatchHasNothingToWaitFor` checks it for
  // `SyncIoPolicy` and `IoUringPolicy`). The arguments have the
  // contract of `readPageCacheHits` and `Policy::addBatch`.
  void addBatch(int fd, ql::span<const size_t> numBytes,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle) {
    auto forwardAll = [&](const UnservedReads::NothingRead&) {
      policy_.addBatch(fd, numBytes, offsets, buffers, handle);
    };
    auto forwardRemaining =
        [&](const UnservedReads::RemainingPositions& remaining) {
          detail::SelectedReads missed{numBytes, offsets, buffers,
                                       remaining.positions_};
          policy_.addBatch(fd, missed.numBytes_, missed.offsets_,
                           missed.buffers_, handle);
        };
    const UnservedReads unserved =
        enabled_ ? readPageCacheHits(fd, numBytes, offsets, buffers)
                 : UnservedReads{UnservedReads::NothingRead{}};
    std::visit(OverloadCallOperator{forwardAll, forwardRemaining},
               unserved.reads_);
  }

  // Block until every read of the batch `handle` has completed.
  void wait(BatchHandle handle) { policy_.wait(handle); }

 private:
  Policy policy_;
  bool enabled_ = true;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IO_URING_PAGECACHEFIRSTPOLICY_H
