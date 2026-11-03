// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IO_URING_BATCHMANAGER_H
#define QLEVER_SRC_UTIL_IO_URING_BATCHMANAGER_H

#include <cstdint>

#include "backports/concepts.h"
#include "backports/span.h"
#include "util/Algorithm.h"
#include "util/IoUringManager.h"
#include "util/IoUringManagerConcepts.h"
#include "util/NoCopyNoMove.h"
#include "util/io_uring/RingSize.h"

namespace ad_utility {

// `BatchManager` validates the input spans and delegates the reads from the
// underlying Vocabulary to the `ReadPolicy`, which must satisfy
// `detail::ReadPolicyConcept` from `IoUringManagerConcepts.h`. Neither copyable
// nor movable: it owns its policy (for `IoUringPolicy` the io_uring ring), and
// the pool holds managers by `std::unique_ptr`.
template <typename ReadPolicy>
class BatchManager final : public BatchManagerBase, public NoCopyNoMove {
  static_assert(
      detail::ReadPolicyConcept<ReadPolicy>,
      "BatchManager's ReadPolicy must satisfy the ReadPolicyConcept.");

 public:
  // `explicit`, so that a bare ring size cannot silently convert to a
  // manager where a `BatchManager` is expected.
  explicit BatchManager(RingSize ringSize = RingSize{}) : policy_(ringSize) {}

  void readBatch(int fd, ql::span<const size_t> numBytes,
                 ql::span<const uint64_t> offsets,
                 ql::span<char*> buffers) override {
    validateSameLength(numBytes, offsets, buffers);
    // Delegate the I/O work to the policy.
    policy_.readBatch(fd, numBytes, offsets, buffers);
  }

  // Forward the switch to the policy if it has one.
  void setPageCacheFastPath([[maybe_unused]] bool enabled) noexcept override {
    if constexpr (CPP_requires_ref(detail::HasPageCacheFastPathSwitch,
                                   ReadPolicy)) {
      policy_.setPageCacheFastPath(enabled);
    }
  }

 private:
  [[no_unique_address]] ReadPolicy policy_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IO_URING_BATCHMANAGER_H
