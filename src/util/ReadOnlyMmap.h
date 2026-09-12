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
#include <unistd.h>

#include <cstddef>
#include <utility>

#include "util/Invariants.h"
#include "util/Log.h"

namespace ad_utility {

// A read-only shared mapping of a byte range of a file, with RAII lifetime.
// A default-constructed instance is unmapped; `map` establishes the mapping
// and reports failure as `false` (never by throwing) so callers that own a
// fallback path can degrade gracefully. The requested file offset needs no
// page alignment: it is rounded down internally and `data()` still points at
// exactly the requested first byte. Move-only: a moved-from instance is
// unmapped.
class ReadOnlyMmap : public WithInvariants<ReadOnlyMmap> {
 private:
  // Page-aligned base handed to `mmap`/`munmap`, or `nullptr` when unmapped.
  void* alignedBase_ = nullptr;
  // Bytes covered by the mapping starting at `alignedBase_`.
  size_t mappedBytes_ = 0;
  // Requested view into the mapping: `data()` plus `numBytes_`.
  const void* data_ = nullptr;
  size_t numBytes_ = 0;

 public:
  ReadOnlyMmap() = default;

  ReadOnlyMmap(const ReadOnlyMmap&) = delete;
  ReadOnlyMmap& operator=(const ReadOnlyMmap&) = delete;

  ReadOnlyMmap(ReadOnlyMmap&& other) noexcept
      : alignedBase_{std::exchange(other.alignedBase_, nullptr)},
        mappedBytes_{std::exchange(other.mappedBytes_, 0)},
        data_{std::exchange(other.data_, nullptr)},
        numBytes_{std::exchange(other.numBytes_, 0)} {
    auto guard = makeInvariantGuard();
  }
  ReadOnlyMmap& operator=(ReadOnlyMmap&& other) noexcept {
    auto guard = makeInvariantGuard();
    if (this != &other) {
      unmap();
      alignedBase_ = std::exchange(other.alignedBase_, nullptr);
      mappedBytes_ = std::exchange(other.mappedBytes_, 0);
      data_ = std::exchange(other.data_, nullptr);
      numBytes_ = std::exchange(other.numBytes_, 0);
    }
    return *this;
  }

  ~ReadOnlyMmap() { unmap(); }

  void checkInvariants() const {
    AD_CORRECTNESS_CHECK((alignedBase_ == nullptr) == (numBytes_ == 0));
    AD_CORRECTNESS_CHECK((data_ == nullptr) == (numBytes_ == 0));
    AD_CORRECTNESS_CHECK(mappedBytes_ >= numBytes_);
  }

  // Map `numBytes` starting at `fileOffset` of `fd` read-only. A no-op
  // returning `true` when this instance is already mapped. Returns `false`
  // (leaving this instance unmapped) when the mapping cannot be established.
  [[nodiscard]] bool map(int fd, size_t numBytes, off_t fileOffset = 0) {
    auto guard = makeInvariantGuard();
    if (isMapped()) {
      return true;
    }
    if (numBytes == 0 || fileOffset < 0) {
      return false;
    }
    const size_t pageSize = static_cast<size_t>(::sysconf(_SC_PAGE_SIZE));
    const auto offset = static_cast<uint64_t>(fileOffset);
    const uint64_t alignedOffset = offset - offset % pageSize;
    const size_t delta = static_cast<size_t>(offset - alignedOffset);
    void* base = ::mmap(nullptr, numBytes + delta, PROT_READ, MAP_SHARED, fd,
                        static_cast<off_t>(alignedOffset));
    if (base == MAP_FAILED) {
      return false;
    }
    alignedBase_ = base;
    mappedBytes_ = numBytes + delta;
    data_ = static_cast<const char*>(base) + delta;
    numBytes_ = numBytes;
    return true;
  }

  // Release the mapping; idempotent and non-throwing.
  void unmap() noexcept {
    // No guard: called from the destructor and the move assignment, where a
    // throwing exit check would be wrong. The body re-establishes the
    // invariant directly.
    if (alignedBase_ != nullptr) {
      if (::munmap(alignedBase_, mappedBytes_) != 0) {
        AD_LOG_WARN << "munmap failed in `ReadOnlyMmap::unmap`.\n";
      }
      alignedBase_ = nullptr;
      mappedBytes_ = 0;
      data_ = nullptr;
      numBytes_ = 0;
    }
  }

  [[nodiscard]] bool isMapped() const noexcept {
    return alignedBase_ != nullptr;
  }
  // The mapped bytes; only valid when `isMapped()`.
  [[nodiscard]] const void* data() const {
    AD_CONTRACT_CHECK(isMapped());
    return data_;
  }
  [[nodiscard]] size_t size() const noexcept { return numBytes_; }
};

static_assert(ad_utility::InvariantStatefulClass<ReadOnlyMmap>);

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_READONLYMMAP_H
