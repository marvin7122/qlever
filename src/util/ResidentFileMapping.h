// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_RESIDENTFILEMAPPING_H
#define QLEVER_SRC_UTIL_RESIDENTFILEMAPPING_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "backports/span.h"

namespace ad_utility {

// A read-only memory mapping of a file, together with one bit per page that
// records whether this process has read the page before ("known resident").
//
// Purpose: a small read that hits the page cache still costs a system call
// (`preadv2`), whose kernel work (page-cache lookup, permission and access
// time checks, copy) dominates the CPU time of lookups of many small,
// scattered words. `tryRead` serves a range whose pages are all known resident
// by copying it from the mapping, without a system call. All other ranges are
// read as before (page-cache fast path or `io_uring`); `markResident` then
// records their pages.
//
// A page that the kernel evicts after it was marked is not unmarked: reading
// it through the mapping then blocks until the page is read back (a major page
// fault), which is correct but synchronous. This only happens under memory
// pressure, for pages this process has read before.
//
// Thread safety: all member functions may be called concurrently.
class ResidentFileMapping {
 public:
  static constexpr size_t pageSize = 4096;

  // Map the first `fileSize` bytes of `fd` read-only. If the mapping fails
  // (or `fileSize` is zero, or memory mappings are not available), the object
  // is valid but `isMapped()` is false and `tryRead` never serves a range.
  ResidentFileMapping(int fd, size_t fileSize);
  ResidentFileMapping() = default;
  ~ResidentFileMapping();

  ResidentFileMapping(const ResidentFileMapping&) = delete;
  ResidentFileMapping& operator=(const ResidentFileMapping&) = delete;
  ResidentFileMapping(ResidentFileMapping&& other) noexcept;
  ResidentFileMapping& operator=(ResidentFileMapping&& other) noexcept;

  bool isMapped() const { return data_ != nullptr; }

  // If the range `[offset, offset + numBytes)` lies within the file and all
  // its pages are known resident, copy it to `target` and return true.
  // Otherwise return false and do not touch `target`. An empty range is
  // always served.
  bool tryRead(uint64_t offset, size_t numBytes, char* target) const;

  // Record that the range `[offset, offset + numBytes)` has just been read, so
  // that its pages are in the page cache. Ranges beyond the end of the file
  // are ignored.
  void markResident(uint64_t offset, size_t numBytes) const;

  // Serve every read `i` (`numBytes[i]` bytes at `offsets[i]` into
  // `buffers[i]`) whose pages are known resident via `tryRead`, and return the
  // indices (ascending) of the reads that were not served.
  std::vector<size_t> tryReadAll(ql::span<const size_t> numBytes,
                                 ql::span<const uint64_t> offsets,
                                 ql::span<char*> buffers) const;

  // Bound the pages kept marked resident to `capPages` (0 means unbounded,
  // the default and current behavior). When marking would exceed the cap, a
  // CLOCK hand clears marked pages down to the cap and demotes them from the
  // page cache (`MADV_PAGEOUT` where available, else `MADV_DONTNEED`),
  // batched over contiguous runs. This is the read-only subset of
  // VMCache-style explicit eviction (Leis et al., SIGMOD 2023): residency
  // stays in process state, but eviction no longer trusts the kernel, so a
  // marked page can never surprise the reader with an unbounded synchronous
  // fault storm under memory pressure.
  //
  // Eviction under a concurrent reader is transparent and needs no pinning:
  // the mapping is read-only, so a reader that loses its page to `DONTNEED`
  // faults it back and continues with correct data. The cost is one disk
  // read, exactly what the non-mapping path would have paid. The CLOCK hand
  // only makes that case rare by evicting the least recently marked pages.
  //
  // Takes effect on the next `markResident`; when the value changes, the
  // marked pages are recounted once and the new cap is enforced immediately.
  // May be called concurrently.
  void setResidentCapPages(size_t capPages) const;

  // The current cap in pages (0 means unbounded).
  size_t residentCapPages() const {
    return capPages_.load(std::memory_order_relaxed);
  }

  // `markResident` for the reads at `positions`.
  void markAllResident(ql::span<const size_t> numBytes,
                       ql::span<const uint64_t> offsets,
                       ql::span<const size_t> positions) const;

 private:
  const char* data_ = nullptr;
  size_t size_ = 0;
  size_t numBitWords_ = 0;
  std::unique_ptr<std::atomic<uint64_t>[]> residentBits_;
  // Mutable so the const member functions (`markResident`,
  // `setResidentCapPages`) can update the cap, the count, and the CLOCK hand;
  // all updates are atomic.
  mutable std::atomic<size_t> capPages_{0};
  mutable std::atomic<size_t> residentPageCount_{0};
  mutable std::atomic<size_t> clockHand_{0};

  // The pages `[firstPage, lastPage]` of a non-empty range within the file.
  std::pair<size_t, size_t> pagesOf(uint64_t offset, size_t numBytes) const {
    return {offset / pageSize, (offset + numBytes - 1) / pageSize};
  }
  void unmap();
  void evictDownTo(size_t target) const;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RESIDENTFILEMAPPING_H
