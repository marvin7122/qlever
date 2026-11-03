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

// The concepts that the templates of the batch-read module
// (`IoUringManager.h`) put on their template arguments:
// - `ReadPolicyConcept`: what a `ReadPolicy`, the pluggable I/O backend of
//   `BatchManager` and `PageCacheFirstPolicy`, must provide.
// - `HasPageCacheFastPathSwitch`: whether a policy has a page-cache fast path
//   that `BatchManagerBase::setPageCacheFastPath` can switch.
//
// They are in `detail` because they only constrain and inspect the template
// arguments of `BatchManager` and `PageCacheFirstPolicy`: callers of a batch
// manager use `BatchManagerBase` and `makeBatchManager` instead.
namespace ad_utility::detail {

template <typename T>
CPP_requires(ReadPolicy_,
             requires(T& policy, int fd, ql::span<const size_t> numBytes,
                      ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                      BatchHandle handle)(
                 // Must provide `addBatch` with the following parameters.
                 policy.addBatch(fd, numBytes, offsets, buffers, handle),
                 // Must provide a `wait` method with the following interface.
                 policy.wait(handle)));

// The pluggable I/O backend of `BatchManager`: it specifies how the reads in a
// batch are carried out. Implemented by `IoUringPolicy` (io_uring, in
// `IoUringPolicy.h`), `SyncIoPolicy` (blocking `pread`, in `SyncIoPolicy.h`)
// and `PageCacheFirstPolicy` (a decorator, in `PageCacheFirstPolicy.h`).
//
// A `ReadPolicy` must
// - be constructible from a `RingSize`, which `BatchManager` forwards (a
//   policy without a ring ignores it);
// - provide `addBatch(fd, numBytes, offsets, buffers, handle)`, where read `i`
//   reads `numBytes[i]` bytes from `fd`, starting at byte `offsets[i]` of the
//   file, into `buffers[i]`. The three spans have the same length (checked by
//   `BatchManager::addBatch`). The span contents may be used only during the
//   call, but the buffers they point to must stay valid until `wait(handle)`
//   has returned;
// - provide `wait(handle)`, which returns once every read of the batch
//   `handle` has completed.
// A failed read is reported by an exception from `addBatch` or from `wait`.
template <typename T>
CPP_concept ReadPolicyConcept = ::concepts::constructible_from<T, RingSize> &&
                                CPP_requires_ref(ReadPolicy_, T);

// Satisfied iff `Policy` has a member callable as `setPageCacheFastPath(true)`,
// i.e. a switchable page-cache fast path (as `PageCacheFirstPolicy` has).
// `BatchManager` forwards its own switch only to policies that have one.
template <typename Policy>
CPP_requires(HasPageCacheFastPathSwitch,
             requires(Policy& policy)(policy.setPageCacheFastPath(true)));

}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_UTIL_IOURINGMANAGERCONCEPTS_H
