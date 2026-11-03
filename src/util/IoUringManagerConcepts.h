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
#include "util/io_uring/BatchOfReads.h"

namespace ad_utility {

template <typename T>
CPP_requires(ReadPolicy_,
             requires(T& policy, int fd, const BatchOfReads& reads)(
                 concepts::constructible_from<T, unsigned>,
                 // Must provide `readBatch` with the following parameters.
                 policy.readBatch(fd, reads)));

// The pluggable I/O backend of `BatchManager`: it specifies how the reads in a
// batch are carried out. See `IoUringPolicy` (via io_uring, in
// `io_uring/IoUringPolicy.h`) and `SyncIoPolicy` (blocking `pread` fallback, in
// `IoUringManager.h`). Must additionally be constructible from a ring size
// (`unsigned`). `readBatch(fd, reads)` reads the `reads` from `fd` and returns,
// or throws, only once no read of the batch can write into its buffer any more;
// it reports a failed or short read by throwing.
template <typename T>
CPP_concept ReadPolicyConcept = concepts::constructible_from<T, unsigned> &&
                                CPP_requires_ref(ReadPolicy_, T);

namespace detail {
// Satisfied iff `Policy` has a member callable as `setPageCacheFastPath(true)`,
// i.e. a switchable page-cache fast path.
template <typename Policy>
CPP_requires(HasPageCacheFastPathSwitch,
             requires(Policy& policy)(policy.setPageCacheFastPath(true)));
}  // namespace detail

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGERCONCEPTS_H
