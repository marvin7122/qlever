// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SYNCIOPOLICY_H
#define QLEVER_SRC_UTIL_SYNCIOPOLICY_H

#include <cstdint>

#include "backports/span.h"
#include "util/BatchHandle.h"
#include "util/RingSize.h"

namespace ad_utility {

// Fallback implementation for the `IoUringPolicy` below. Schedules pread calls
// in a synchronous (blocking) manner. Single-threaded use only.
struct SyncIoPolicy {
  // `ringSize` is ignored; it exists only so the policy is constructible the
  // same way as `IoUringPolicy`.
  //
  // NOTE: GCC rejects `[[maybe_unused]]` on a defaulted parameter; cast to
  // void. `explicit`, so that a bare ring size cannot silently convert to
  // a policy where a policy is expected.
  explicit SyncIoPolicy(RingSize ringSize = RingSize{}) { (void)ringSize; }

  // Defaulted: this policy owns no resources (a batch already finishes
  // inside `addBatch`), so there is nothing to release.
  ~SyncIoPolicy() = default;
  // Not copyable or movable: declared for consistency with the other
  // policies, so that a `BatchManager` never has to support a policy
  // whose identity changes while batches are outstanding. Policies are
  // only ever held inside their manager.
  SyncIoPolicy(const SyncIoPolicy&) = delete;
  SyncIoPolicy& operator=(const SyncIoPolicy&) = delete;
  SyncIoPolicy(SyncIoPolicy&&) = delete;
  SyncIoPolicy& operator=(SyncIoPolicy&&) = delete;

  // Immediately execute a batch of reads synchronously. This blocks the calling
  // thread. Read `i` reads `numberOfBytesToReadPerRequest[i]` bytes from file
  // descriptor `fd`, starting at offset `fileOffsetPerRequest[i]` (from the
  // start of the file), into the buffer starting at
  // `targetBufferPerRequest[i]`. `handle` is unused (the batch completes before
  // `addBatch` returns).
  void addBatch(int fd, ql::span<const size_t> numberOfBytesToReadPerRequest,
                ql::span<const uint64_t> fileOffsetPerRequest,
                ql::span<char*> targetBufferPerRequest,
                BatchHandle handle) const;

  void wait(BatchHandle) const {
    // No-op: `addBatch` already completed all reads synchronously.
  }

  // Read exactly `numberOfBytesToRead` bytes from file descriptor `fd` at
  // `fileOffset` (from the start of the file) into `targetBuffer`. Throws
  // exception if the read fails or returns fewer bytes than requested (a
  // partial read or end of file), since every read must be fully satisfied.
  static void readFullyOrThrow(int fd, char* targetBuffer,
                               size_t numberOfBytesToRead, uint64_t fileOffset);
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_SYNCIOPOLICY_H
