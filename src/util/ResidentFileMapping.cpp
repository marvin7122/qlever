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
  const size_t numPages = (fileSize + pageSize - 1) / pageSize;
  const size_t numBitWords = (numPages + 63) / 64;
  // Allocate the bitmap before mapping: if the allocation throws, no mapping
  // exists yet that could leak, and the object simply fails to construct.
  auto residentBits = std::make_unique<std::atomic<uint64_t>[]>(numBitWords);
  void* data = ::mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
  if (data == MAP_FAILED) {
    return;
  }
  data_ = static_cast<const char*>(data);
  size_ = fileSize;
  numBitWords_ = numBitWords;
  residentBits_ = std::move(residentBits);
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
}

// _____________________________________________________________________________
ResidentFileMapping::~ResidentFileMapping() { unmap(); }

// _____________________________________________________________________________
ResidentFileMapping::ResidentFileMapping(ResidentFileMapping&& other) noexcept
    : data_{std::exchange(other.data_, nullptr)},
      size_{std::exchange(other.size_, 0)},
      numBitWords_{std::exchange(other.numBitWords_, 0)},
      residentBits_{std::move(other.residentBits_)} {}

// _____________________________________________________________________________
ResidentFileMapping& ResidentFileMapping::operator=(
    ResidentFileMapping&& other) noexcept {
  if (this != &other) {
    unmap();
    data_ = std::exchange(other.data_, nullptr);
    size_ = std::exchange(other.size_, 0);
    numBitWords_ = std::exchange(other.numBitWords_, 0);
    residentBits_ = std::move(other.residentBits_);
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
  for (size_t page = firstPage; page <= lastPage; ++page) {
    const uint64_t mask = uint64_t{1} << (page % 64);
    auto& word = residentBits_[page / 64];
    // Skip the atomic read-modify-write if the bit is already set.
    if ((word.load(std::memory_order_relaxed) & mask) == 0) {
      word.fetch_or(mask, std::memory_order_relaxed);
    }
  }
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
  AD_CONTRACT_CHECK(offsets.size() == numBytes.size());
  for (size_t i : positions) {
    AD_CONTRACT_CHECK(i < numBytes.size());
    markResident(offsets[i], numBytes[i]);
  }
}

}  // namespace ad_utility
