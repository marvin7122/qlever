// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_READONLYMMAP_H
#define QLEVER_SRC_UTIL_READONLYMMAP_H

#include <sys/mman.h>

#include <cstddef>
#include <utility>

#include "util/Invariants.h"
#include "util/Log.h"

namespace ad_utility {

// A read-only shared mapping of a byte range of a file, with RAII lifetime.
// A default-constructed instance is unmapped; `map` establishes the mapping
// and reports failure as `false` (never by throwing) so callers that own a
// fallback path can degrade gracefully. Move-only: a moved-from instance is
// unmapped.
class ReadOnlyMmap : public WithInvariants<ReadOnlyMmap> {
 private:
  void* base_ = nullptr;
  size_t numBytes_ = 0;

 public:
  ReadOnlyMmap() = default;

  ReadOnlyMmap(const ReadOnlyMmap&) = delete;
  ReadOnlyMmap& operator=(const ReadOnlyMmap&) = delete;

  ReadOnlyMmap(ReadOnlyMmap&& other) noexcept
      : base_{std::exchange(other.base_, nullptr)},
        numBytes_{std::exchange(other.numBytes_, 0)} {
    auto guard = makeInvariantGuard();
  }
  ReadOnlyMmap& operator=(ReadOnlyMmap&& other) noexcept {
    auto guard = makeInvariantGuard();
    if (this != &other) {
      unmap();
      base_ = std::exchange(other.base_, nullptr);
      numBytes_ = std::exchange(other.numBytes_, 0);
    }
    return *this;
  }

  ~ReadOnlyMmap() { unmap(); }

  void checkInvariants() const {
    AD_CORRECTNESS_CHECK((base_ == nullptr) == (numBytes_ == 0));
  }

  // Map `numBytes` starting at `fileOffset` of `fd` read-only. A no-op
  // returning `true` when this instance is already mapped. Returns `false`
  // (leaving this instance unmapped) when the mapping cannot be established.
  [[nodiscard]] bool map(int fd, size_t numBytes, off_t fileOffset = 0) {
    auto guard = makeInvariantGuard();
    if (isMapped()) {
      return true;
    }
    if (numBytes == 0) {
      return false;
    }
    void* base =
        ::mmap(nullptr, numBytes, PROT_READ, MAP_SHARED, fd, fileOffset);
    if (base == MAP_FAILED) {
      return false;
    }
    base_ = base;
    numBytes_ = numBytes;
    return true;
  }

  // Release the mapping; idempotent and non-throwing.
  void unmap() noexcept {
    // No guard: called from the destructor and the move assignment, where a
    // throwing exit check would be wrong. The body re-establishes the
    // invariant directly.
    if (base_ != nullptr) {
      if (::munmap(base_, numBytes_) != 0) {
        AD_LOG_WARN << "munmap failed in `ReadOnlyMmap::unmap`.\n";
      }
      base_ = nullptr;
      numBytes_ = 0;
    }
  }

  [[nodiscard]] bool isMapped() const noexcept { return base_ != nullptr; }
  // The mapped bytes; only valid when `isMapped()`.
  [[nodiscard]] const void* data() const {
    AD_CONTRACT_CHECK(isMapped());
    return base_;
  }
  [[nodiscard]] size_t size() const noexcept { return numBytes_; }
};

static_assert(ad_utility::InvariantStatefulClass<ReadOnlyMmap>);

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_READONLYMMAP_H
