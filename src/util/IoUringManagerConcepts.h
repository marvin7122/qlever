// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IOURINGMANAGERCONCEPTS_H
#define QLEVER_SRC_UTIL_IOURINGMANAGERCONCEPTS_H

#include <cstdint>

#include "backports/concepts.h"
#include "backports/span.h"
#include "util/BatchHandle.h"
#include "util/RingSize.h"

namespace ad_utility::detail {

// In `detail` because the concept and the trait in this header only
// constrain and inspect the template arguments of `BatchManager` and
// `PageCacheFirstPolicy`: they are part of how those templates are
// implemented, not an interface that callers of a batch manager use
// (callers use `BatchManagerBase` and `makeBatchManager`).

// Definition of the `ReadPolicy Interface`
template <typename T>
CPP_requires(
    ReadPolicy_,
    requires(T& policy, int fd, ql::span<const size_t> numberOfBytesToRead,
             ql::span<const uint64_t> offsets, ql::span<char*> buffers,
             BatchHandle handle)(
        concepts::constructible_from<T, RingSize>,
        // Must provide a method `addBatch` with the following
        // parameters.
        policy.addBatch(fd, numberOfBytesToRead, offsets, buffers, handle),
        // Must provide a `wait` method with the following interface.
        policy.wait(handle)));

// The pluggable I/O backend of `BatchManager`: it specifies how the reads in a
// batch are carried out. See `IoUringPolicy` (asynchronous, via io_uring) and
// `SyncIoPolicy` (blocking `pread` fallback) in `IoUringManager.h`. Must
// additionally be constructible from a `RingSize`. The batch
// handle that `addBatch` and `wait` take is the wrapper type
// `ad_utility::BatchHandle` (see `BatchHandle.h`), not a raw integer.
template <typename T>
CPP_concept ReadPolicyConcept = concepts::constructible_from<T, RingSize> &&
                                CPP_requires_ref(ReadPolicy_, T);

// Satisfied iff `Policy` has a member callable as
// `setPageCacheFastPath(true)`, i.e. a switchable page-cache fast path
// (as `PageCacheFirstPolicy` has one). `BatchManager` uses this to
// forward its own switch only to policies that have one.
template <typename Policy>
CPP_requires(HasPageCacheFastPathSwitch,
             requires(Policy& policy)(policy.setPageCacheFastPath(true)));

}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGERCONCEPTS_H
