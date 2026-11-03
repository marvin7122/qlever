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
#include "util/NoCopyNoMove.h"
#include "util/RingSize.h"

namespace ad_utility {

// Fallback for `IoUringPolicy` (in `IoUringPolicy.h`) where io_uring is not
// available. Schedules pread calls in a synchronous (blocking) manner.
// Single-threaded use only. Neither copyable nor movable, like the other
// policies: a policy lives inside its `BatchManager`.
struct SyncIoPolicy : NoCopyNoMove {
  // `ringSize` is ignored; it exists only so the policy is constructible the
  // same way as `IoUringPolicy`.
  //
  // NOTE: GCC rejects `[[maybe_unused]]` on a defaulted parameter; cast to
  // void.
  explicit SyncIoPolicy(RingSize ringSize = RingSize{}) { (void)ringSize; }

  // Immediately execute a batch of reads synchronously. This blocks the calling
  // thread. Read `i` reads `numBytesToReadPerRequest[i]` bytes from file
  // descriptor `fd`, starting at offset `fileOffsetPerRequest[i]` (from the
  // start of the file), into the buffer starting at
  // `targetBufferPerRequest[i]`.
  void readBatch(int fd, ql::span<const size_t> numBytesToReadPerRequest,
                 ql::span<const uint64_t> fileOffsetPerRequest,
                 ql::span<char*> targetBufferPerRequest) const;

  // Read exactly `numBytes` bytes from file descriptor `fd` at `fileOffset`
  // (from the start of the file) into `targetBuffer`. Throws exception if the
  // read fails or returns fewer bytes than requested (a partial read or end of
  // file), since every read must be fully satisfied.
  static void readFullyOrThrow(int fd, char* targetBuffer, size_t numBytes,
                               uint64_t fileOffset);
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_SYNCIOPOLICY_H
