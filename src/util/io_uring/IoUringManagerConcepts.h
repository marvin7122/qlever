// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IO_URING_IOURINGMANAGERCONCEPTS_H
#define QLEVER_SRC_UTIL_IO_URING_IOURINGMANAGERCONCEPTS_H

#include <cstdint>

#include "backports/concepts.h"
#include "backports/span.h"

// The compile-time interface between `BatchManager` (in `IoUringManager.h`) and
// the policies that carry out its reads. A `BatchManager` owns the batch
// bookkeeping (it hands out one `BatchHandle` per batch) and delegates the
// reads themselves to a `ReadPolicy`. This header holds only the constraints,
// so that `PageCacheFirstPolicy.h`, a policy that wraps another policy, can
// state them without depending on `IoUringManager.h`. The models are
// `IoUringPolicy` (asynchronous reads via io_uring) and `SyncIoPolicy`
// (blocking `pread`) in `IoUringManager.h`, and `PageCacheFirstPolicy<Policy>`
// in `PageCacheFirstPolicy.h`.

namespace ad_utility {

// The member functions of a `ReadPolicy`: `addBatch` starts the reads of one
// batch (read `i` reads `numBytes[i]` bytes at file offset `offsets[i]` of `fd`
// into `buffers[i]`), tags them with `handle`, and returns whether any of them
// is still in flight; `wait(handle)` blocks until all reads of that batch have
// completed. `BatchManager` calls `wait` only for a batch for which `addBatch`
// returned `true`.
template <typename T>
CPP_requires(
    ReadPolicy_,
    requires(T& policy, int fd, ql::span<const size_t> numBytes,
             ql::span<const uint64_t> offsets, ql::span<char*> buffers,
             typename T::BatchHandle handle)(
        ::concepts::requires_<ql::concepts::same_as<
            decltype(policy.addBatch(fd, numBytes, offsets, buffers, handle)),
            bool>>,
        policy.wait(handle)));

// A `ReadPolicy`, used as the template argument of `BatchManager` and
// `PageCacheFirstPolicy`: it has the member functions of `ReadPolicy_` above,
// an unsigned integral `BatchHandle` type, and a constructor from the size of
// an io_uring ring (`unsigned`), which policies without a ring ignore.
template <typename T>
CPP_concept ReadPolicyConcept =
    ql::concepts::constructible_from<T, unsigned> &&
    ql::concepts::unsigned_integral<typename T::BatchHandle> &&
    CPP_requires_ref(ReadPolicy_, T);

namespace detail {
// Satisfied iff `Policy` has a member callable as `setPageCacheFastPath(true)`,
// i.e. a switchable page-cache fast path (`PageCacheFirstPolicy`).
// `BatchManager::setPageCacheFastPath` forwards its argument to such a member
// and checks at compile time that the member is `noexcept`.
template <typename Policy>
CPP_requires(HasPageCacheFastPathSwitch_,
             requires(Policy& policy)(policy.setPageCacheFastPath(true)));
}  // namespace detail

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IO_URING_IOURINGMANAGERCONCEPTS_H
