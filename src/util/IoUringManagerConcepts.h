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
#include <type_traits>
#include <utility>

#include "backports/concepts.h"
#include "backports/span.h"
#include "util/BatchHandle.h"

namespace ad_utility::detail {

// Definition of the `ReadPolicy Interface`
template <typename T>
CPP_requires(ReadPolicy_,
             requires(T& policy, int fd, ql::span<const size_t> numBytes,
                      ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                      BatchHandle handle)(
                 concepts::constructible_from<T, unsigned>,
                 // Must provide a method `addBatch` with the following
                 // parameters.
                 policy.addBatch(fd, numBytes, offsets, buffers, handle),
                 // Must provide a `wait` method with the following interface.
                 policy.wait(handle)));

// The pluggable I/O backend of `BatchManager`: it specifies how the reads in a
// batch are carried out. See `IoUringPolicy` (asynchronous, via io_uring) and
// `SyncIoPolicy` (blocking `pread` fallback) in `IoUringManager.h`. Must
// additionally be constructible from a ringsize (`unsigned`). The batch
// handle that `addBatch` and `wait` take is the wrapper type
// `ad_utility::BatchHandle` (see `BatchHandle.h`), not a raw integer.
template <typename T>
CPP_concept ReadPolicyConcept = concepts::constructible_from<T, unsigned> &&
                                CPP_requires_ref(ReadPolicy_, T);

// True iff `Policy` has a page-cache fast path that can be switched on
// and off via a `setPageCacheFastPath(bool)` member (as
// `PageCacheFirstPolicy` does). `BatchManager` uses this to forward its
// own switch only to policies that have one.
template <typename Policy, typename = void>
inline constexpr bool hasPageCacheFastPathSwitch = false;

template <typename Policy>
inline constexpr bool hasPageCacheFastPathSwitch<
    Policy,
    std::void_t<decltype(std::declval<Policy&>().setPageCacheFastPath(true))>> =
    true;

}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGERCONCEPTS_H
