// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_RINGSIZE_H
#define QLEVER_SRC_UTIL_RINGSIZE_H

#include "util/Exception.h"

namespace ad_utility {

// The size of an io_uring ring (the maximum number of reads that a batch
// manager keeps outstanding), as a bounded type: the realistic domain of
// an io_uring ring size is nowhere near the 2^32 values of an `unsigned`
// (a ring of that size could never be set up), so the raw type would
// admit almost only meaningless values. The universe of this type is
// the powers of two from 1 to 4096:
//
// - At least 1, because a ring without a single slot can never make
//   progress.
// - A power of two, because liburing rounds the requested size up to
//   the next power of two: accepting a non-power would silently make
//   the effective ring larger than the configured one, while the
//   bookkeeping still uses the configured value as its cap.
// - At most 4096 (sixteen times the default of 256), because each ring
//   costs on the order of 0.4 MB of kernel memory at that size and
//   there are several managers, while more outstanding reads than that
//   do not speed up the batched vocabulary lookups.
//
// The invariant is enforced at construction, so every consumer can
// rely on it instead of re-checking the value.
class RingSize {
 public:
  // The default ring size, used unless a caller asks for another one.
  static constexpr unsigned DEFAULT_VALUE = 256;
  // The largest ring size in the universe of this type.
  static constexpr unsigned MAX_VALUE = 4096;

  // The default size (256).
  RingSize() = default;

  // `explicit`, so that a bare integer cannot silently convert to a
  // ring size where one is expected. Throws if `value` is not a power
  // of two between 1 and `MAX_VALUE`.
  explicit RingSize(unsigned value) : value_{value} {
    AD_CONTRACT_CHECK(
        value >= 1 && value <= MAX_VALUE && (value & (value - 1)) == 0,
        "A ring size must be a power of two between 1 and 4096.");
  }

  // The ring size as the `unsigned` that liburing and the bookkeeping
  // comparisons work with. Guaranteed to satisfy the invariant above.
  [[nodiscard]] unsigned value() const { return value_; }

 private:
  unsigned value_ = DEFAULT_VALUE;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RINGSIZE_H
