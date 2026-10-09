// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/ResidentFileMapping.h"

#include <cstring>
#include <utility>
#include <vector>

#include "util/Exception.h"

#if defined(__linux__) && !defined(__EMSCRIPTEN__)
#include <sys/mman.h>
#define QL_RESIDENT_FILE_MAPPING
#endif

namespace ad_utility {

// _____________________________________________________________________________
ResidentFileMapping::ResidentFileMapping([[maybe_unused]] int fd,
                                         size_t fileSize) {
#ifdef QL_RESIDENT_FILE_MAPPING
  if (fileSize == 0) {
    return;
  }
  void* data = ::mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
  if (data == MAP_FAILED) {
    return;
  }
  data_ = static_cast<const char*>(data);
  size_ = fileSize;
  const size_t numPages = (fileSize + pageSize - 1) / pageSize;
  numBitWords_ = (numPages + 63) / 64;
  residentBits_ = std::make_unique<std::atomic<uint64_t>[]>(numBitWords_);
  for (size_t i = 0; i < numBitWords_; ++i) {
    residentBits_[i].store(0, std::memory_order_relaxed);
  }
#else
  (void)fileSize;
#endif
}

// _____________________________________________________________________________
void ResidentFileMapping::unmap() {
#ifdef QL_RESIDENT_FILE_MAPPING
  if (data_ != nullptr) {
    ::munmap(const_cast<char*>(data_), size_);
  }
#endif
  data_ = nullptr;
  size_ = 0;
  numBitWords_ = 0;
  residentBits_.reset();
  residentPageCount_.store(0, std::memory_order_relaxed);
  clockHand_.store(0, std::memory_order_relaxed);
}

// _____________________________________________________________________________
ResidentFileMapping::~ResidentFileMapping() { unmap(); }

// _____________________________________________________________________________
ResidentFileMapping::ResidentFileMapping(ResidentFileMapping&& other) noexcept
    : data_{std::exchange(other.data_, nullptr)},
      size_{std::exchange(other.size_, 0)},
      numBitWords_{std::exchange(other.numBitWords_, 0)},
      residentBits_{std::move(other.residentBits_)} {
  capPages_.store(other.capPages_.load(std::memory_order_relaxed),
                  std::memory_order_relaxed);
  residentPageCount_.store(
      other.residentPageCount_.load(std::memory_order_relaxed),
      std::memory_order_relaxed);
  clockHand_.store(other.clockHand_.load(std::memory_order_relaxed),
                   std::memory_order_relaxed);
}

// _____________________________________________________________________________
ResidentFileMapping& ResidentFileMapping::operator=(
    ResidentFileMapping&& other) noexcept {
  if (this != &other) {
    unmap();
    data_ = std::exchange(other.data_, nullptr);
    size_ = std::exchange(other.size_, 0);
    numBitWords_ = std::exchange(other.numBitWords_, 0);
    residentBits_ = std::move(other.residentBits_);
    capPages_.store(other.capPages_.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
    residentPageCount_.store(
        other.residentPageCount_.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    clockHand_.store(other.clockHand_.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
  }
  return *this;
}

// _____________________________________________________________________________
bool ResidentFileMapping::tryRead(uint64_t offset, size_t numBytes,
                                  char* target) const {
  if (numBytes == 0) {
    return true;
  }
  if (data_ == nullptr || offset > size_ || numBytes > size_ - offset) {
    return false;
  }
  auto [firstPage, lastPage] = pagesOf(offset, numBytes);
  for (size_t page = firstPage; page <= lastPage; ++page) {
    const uint64_t bits =
        residentBits_[page / 64].load(std::memory_order_relaxed);
    if ((bits & (uint64_t{1} << (page % 64))) == 0) {
      return false;
    }
  }
  std::memcpy(target, data_ + offset, numBytes);
  return true;
}

// _____________________________________________________________________________
void ResidentFileMapping::markResident(uint64_t offset, size_t numBytes) const {
  if (data_ == nullptr || numBytes == 0 || offset > size_ ||
      numBytes > size_ - offset) {
    return;
  }
  auto [firstPage, lastPage] = pagesOf(offset, numBytes);
  size_t newlyMarked = 0;
  for (size_t page = firstPage; page <= lastPage; ++page) {
    const uint64_t mask = uint64_t{1} << (page % 64);
    auto& word = residentBits_[page / 64];
    // Skip the atomic read-modify-write if the bit is already set.
    if ((word.load(std::memory_order_relaxed) & mask) == 0) {
      if ((word.fetch_or(mask, std::memory_order_relaxed) & mask) == 0) {
        ++newlyMarked;
      }
    }
  }
  if (newlyMarked == 0) {
    return;
  }
  const size_t count =
      residentPageCount_.fetch_add(newlyMarked, std::memory_order_relaxed) +
      newlyMarked;
  const size_t cap = capPages_.load(std::memory_order_relaxed);
  if (cap > 0 && count > cap) {
    evictDownTo(cap);
  }
}

// _____________________________________________________________________________
void ResidentFileMapping::setResidentCapPages(size_t capPages) const {
  const size_t old = capPages_.exchange(capPages, std::memory_order_relaxed);
  if (old == capPages || data_ == nullptr) {
    return;
  }
  // The bits may predate the cap (marks without counting are impossible, but
  // marks before the first cap are); recount once so the cap enforces the
  // true state, then enforce immediately.
  size_t count = 0;
  for (size_t i = 0; i < numBitWords_; ++i) {
    count += static_cast<size_t>(
        __builtin_popcountll(residentBits_[i].load(std::memory_order_relaxed)));
  }
  residentPageCount_.store(count, std::memory_order_relaxed);
  if (capPages > 0 && count > capPages) {
    evictDownTo(capPages);
  }
}

// _____________________________________________________________________________
// Clear marked pages down to `target`, sweeping from the CLOCK hand in file
// order, and demote the cleared runs from the page cache. Stops after one
// revolution: pages marked concurrently may keep the count above target, which
// the next `markResident` re-evaluates. Only ever clears bits, so concurrent
// `tryRead` either sees the page (correct: it is still mapped) or misses it
// (correct: it takes the other path).
void ResidentFileMapping::evictDownTo(size_t target) const {
#ifdef QL_RESIDENT_FILE_MAPPING
  if (data_ == nullptr || numBitWords_ == 0) {
    return;
  }
  const size_t numPages = (size_ + pageSize - 1) / pageSize;
  size_t hand = clockHand_.load(std::memory_order_relaxed);
  size_t examined = 0;
  // Contiguous cleared runs share one `madvise` call. A run is the page
  // interval [runStart, runEnd); `runEnd == numPages + 1` marks "no open run".
  // Pages clear in ascending order within a word; words are visited in CLOCK
  // order, so a run flushes at every gap.
  size_t runStart = 0;
  size_t runEnd = numPages + 1;
  auto flushRun = [&]() {
    if (runEnd <= numPages && runStart < runEnd) {
      const size_t byteStart = runStart * pageSize;
      size_t byteEnd = runEnd * pageSize;
      if (byteEnd > size_) {
        byteEnd = size_;
      }
      if (byteEnd > byteStart) {
// `MADV_PAGEOUT` demotes clean pages and starts writeback on dirty ones;
// plain `MADV_DONTNEED` is a silent no-op on some machines in our fleet
// (verified on Ural: rc 0, pages stay resident), so prefer `PAGEOUT` where
// the headers know it.
#ifdef MADV_PAGEOUT
        ::madvise(const_cast<char*>(data_) + byteStart, byteEnd - byteStart,
                  MADV_PAGEOUT);
#else
        ::madvise(const_cast<char*>(data_) + byteStart, byteEnd - byteStart,
                  MADV_DONTNEED);
#endif
      }
    }
    runEnd = numPages + 1;
  };
  while (examined < numBitWords_ &&
         residentPageCount_.load(std::memory_order_relaxed) > target) {
    const size_t w = (hand + examined) % numBitWords_;
    ++examined;
    uint64_t bits = residentBits_[w].load(std::memory_order_relaxed);
    while (bits != 0 &&
           residentPageCount_.load(std::memory_order_relaxed) > target) {
      const unsigned bit = static_cast<unsigned>(__builtin_ctzll(bits));
      bits &= bits - 1;
      const size_t page = w * 64 + bit;
      if (page >= numPages) {
        continue;
      }
      const uint64_t mask = uint64_t{1} << bit;
      const uint64_t old =
          residentBits_[w].fetch_and(~mask, std::memory_order_relaxed);
      if ((old & mask) == 0) {
        continue;
      }
      residentPageCount_.fetch_sub(1, std::memory_order_relaxed);
      if (page != runEnd) {
        flushRun();
        runStart = page;
      }
      runEnd = page + 1;
    }
    flushRun();
  }
  flushRun();
  clockHand_.store((hand + examined) % numBitWords_, std::memory_order_relaxed);
#else
  (void)target;
#endif
}

// _____________________________________________________________________________
std::vector<size_t> ResidentFileMapping::tryReadAll(
    ql::span<const size_t> numBytes, ql::span<const uint64_t> offsets,
    ql::span<char*> buffers) const {
  AD_CONTRACT_CHECK(offsets.size() == numBytes.size() &&
                    buffers.size() == numBytes.size());
  std::vector<size_t> notServed;
  for (size_t i = 0; i < numBytes.size(); ++i) {
    if (!tryRead(offsets[i], numBytes[i], buffers[i])) {
      notServed.push_back(i);
    }
  }
  return notServed;
}

// _____________________________________________________________________________
void ResidentFileMapping::markAllResident(
    ql::span<const size_t> numBytes, ql::span<const uint64_t> offsets,
    ql::span<const size_t> positions) const {
  for (size_t i : positions) {
    markResident(offsets[i], numBytes[i]);
  }
}

}  // namespace ad_utility
