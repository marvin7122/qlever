// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BATCHHANDLE_H
#define QLEVER_SRC_UTIL_BATCHHANDLE_H

#include <cstdint>
#include <utility>

namespace ad_utility {

// The handle of a batch of reads, minted by `BatchManager` and passed
// back to `wait` (see `IoUringManager.h`). A wrapper type, not an alias
// for `uint64_t`: an alias would be fully interchangeable with every
// other `uint64_t`, so a request id, a byte count, or any other number
// could silently be passed where a batch handle is expected. The wrapped
// value is only accessible via `value()`, for the places that genuinely
// need the number (hashing and diagnostics).
class BatchHandle {
 public:
  // The default handle: the value that the first minted handle wraps, so
  // that handles can be stored in default-constructed containers.
  BatchHandle() = default;

  // `explicit`, so that a bare integer cannot silently convert to a
  // `BatchHandle` where a handle is expected.
  explicit BatchHandle(uint64_t value) : value_{value} {}

  [[nodiscard]] uint64_t value() const { return value_; }

  // Two handles are equal iff they were minted for the same batch. The
  // comparison is needed so that handles can be compared at all (for
  // example in tests) and so that hash containers can resolve collisions
  // between different handles that hash to the same bucket.
  bool operator==(const BatchHandle&) const = default;

  // Hash by the wrapped value, so that a `BatchHandle` can be used as the
  // key of an absl hash container (the per-batch bookkeeping maps of
  // `IoUringPolicy` do exactly that). Without this hook, `HashMap`
  // would not accept the wrapper type as a key.
  template <typename H>
  friend H AbslHashValue(H h, const BatchHandle& handle) {
    return H::combine(std::move(h), handle.value_);
  }

 private:
  uint64_t value_ = 0;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BATCHHANDLE_H
