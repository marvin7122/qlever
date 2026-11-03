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

#include <cstdint>
#include <type_traits>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/IoUringManagerConcepts.h"
#include "util/NoCopyNoMove.h"
#include "util/RingSize.h"

namespace ad_utility {

// Serve the reads of a batch that are completely in the page cache with
// non-blocking `preadv2(RWF_NOWAIT)` calls, one call per run of adjacent file
// ranges (split into chunks of at most `IOV_MAX` reads), and return the
// positions of the other reads in ascending order. The caller issues those
// through its regular path, which also reports errors. A call that fails with
// `EOPNOTSUPP` or `ENOSYS` switches the fast path off for the whole process
// (see `pageCacheFastPathIsSupported`). Throws if the three spans differ in
// length.
std::vector<size_t> readPageCacheHits(int fd, ql::span<const size_t> numBytes,
                                      ql::span<const uint64_t> offsets,
                                      ql::span<char*> buffers);

// Return whether `readPageCacheHits` may use `preadv2(RWF_NOWAIT)`: false if it
// is not compiled in, or once a call has failed with `EOPNOTSUPP` or `ENOSYS`.
// That latch is process-wide.
bool pageCacheFastPathIsSupported() noexcept;

// In `detail` because this only repackages the arguments of
// `PageCacheFirstPolicy::readBatch` (below) into the reads that were not
// served.
namespace detail {
// Owning copies of the reads of a batch at the indices `positions` into the
// three equally long spans, in the order of `positions`, as the `numBytes`,
// `offsets` and `buffers` arguments of `ReadPolicy::readBatch`. `positions`
// must be ascending and smaller than the length of the spans, as the result of
// `readPageCacheHits` is.
struct SelectedReads {
  std::vector<size_t> numBytes_;
  std::vector<uint64_t> offsets_;
  std::vector<char*> buffers_;

  SelectedReads(ql::span<const size_t> numBytes,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                ql::span<const size_t> positions)
      : numBytes_{selectAt(numBytes, positions)},
        offsets_{selectAt(offsets, positions)},
        buffers_{selectAt(buffers, positions)} {
    AD_CONTRACT_CHECK(ql::ranges::is_sorted(positions),
                      "The positions of the selected reads must be ascending.");
  }

 private:
  // The elements of `values` at `positions`, copied into a vector that is
  // allocated once. Checks that every position is in range before using it.
  template <typename T>
  static std::vector<std::remove_const_t<T>> selectAt(
      ql::span<T> values, ql::span<const size_t> positions) {
    AD_CONTRACT_CHECK(
        ql::ranges::all_of(
            positions, [size = values.size()](size_t i) { return i < size; }),
        "The positions of the selected reads must be in range.");
    std::vector<std::remove_const_t<T>> selected(positions.size());
    ql::ranges::transform(positions, selected.begin(),
                          [values](size_t i) { return values[i]; });
    return selected;
  }
};
}  // namespace detail

// A `ReadPolicy` that serves the reads of a batch that a non-blocking
// `preadv2(RWF_NOWAIT)` can serve right away (`readPageCacheHits`) and passes
// only the others on to `ReadPolicy`. Neither copyable nor movable, like the
// `BatchManager` that holds it.
template <typename ReadPolicy>
class PageCacheFirstPolicy : NoCopyNoMove {
  static_assert(detail::ReadPolicyConcept<ReadPolicy>,
                "PageCacheFirstPolicy's ReadPolicy must satisfy the "
                "ReadPolicyConcept.");

 public:
  // `ringSize` is forwarded to the constructor of the decorated `ReadPolicy`.
  explicit PageCacheFirstPolicy(RingSize ringSize) : policy_(ringSize) {}

  // Switch the fast path on (the default) or off for the batches that are read
  // afterwards.
  void setPageCacheFastPath(bool enabled) noexcept { enabled_ = enabled; }

  // Serve the reads of the batch that are in the page cache and pass the others
  // on to the decorated policy. The arguments have the contract of
  // `readPageCacheHits` and `ReadPolicy::readBatch`. The spans passed on to the
  // decorated policy may point into storage that lives only during this call,
  // which suffices because `ReadPolicy::readBatch` is synchronous.
  void readBatch(int fd, ql::span<const size_t> numBytes,
                 ql::span<const uint64_t> offsets, ql::span<char*> buffers) {
    validateSameLength(numBytes, offsets, buffers);
    if (!enabled_ || !pageCacheFastPathIsSupported()) {
      policy_.readBatch(fd, numBytes, offsets, buffers);
      return;
    }
    const std::vector<size_t> notServed =
        readPageCacheHits(fd, numBytes, offsets, buffers);
    if (notServed.size() == numBytes.size()) {
      // Nothing was served, so the batch is passed on unchanged.
      policy_.readBatch(fd, numBytes, offsets, buffers);
      return;
    }
    detail::SelectedReads missed{numBytes, offsets, buffers, notServed};
    policy_.readBatch(fd, missed.numBytes_, missed.offsets_, missed.buffers_);
  }

 private:
  ReadPolicy policy_;
  bool enabled_ = true;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PAGECACHEFIRSTPOLICY_H
