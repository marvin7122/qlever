// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace qlever::export_pipeline {

// _____________________________________________________________________________
// 64-byte Cache-Line Aligned Batch Buffer.
// Enforces strict 64-byte alignment on ID vectors so sequential batch lookups
// cleanly trigger CPU hardware L2 stream prefetchers and avoid split-cache-line penalties.
template <typename T, size_t Alignment = 64>
class AlignedBatchBuffer {
 public:
  static constexpr size_t kAlignment = Alignment;

 private:
  struct AlignedDeleter {
    void operator()(T* ptr) const noexcept {
      if (ptr != nullptr) {
        ::operator delete[](ptr, std::align_val_t{Alignment});
      }
    }
  };

  std::unique_ptr<T[], AlignedDeleter> data_{nullptr};
  size_t capacity_{0};
  size_t size_{0};

 public:
  AlignedBatchBuffer() noexcept = default;

  explicit AlignedBatchBuffer(size_t capacity) {
    reserve(capacity);
  }

  void reserve(size_t newCapacity) {
    if (newCapacity <= capacity_) {
      return;
    }
    // Round capacity to multiple of alignment
    size_t alignedCapacity = (newCapacity + (Alignment / sizeof(T)) - 1) & ~((Alignment / sizeof(T)) - 1);
    T* raw = static_cast<T*>(::operator new[](alignedCapacity * sizeof(T), std::align_val_t{Alignment}));
    std::unique_ptr<T[], AlignedDeleter> newData(raw);

    if (data_ && size_ > 0) {
      std::memcpy(newData.get(), data_.get(), size_ * sizeof(T));
    }
    data_ = std::move(newData);
    capacity_ = alignedCapacity;
  }

  void clear() noexcept {
    size_ = 0;
  }

  void push_back(const T& val) noexcept {
    AD_CORRECTNESS_CHECK(size_ < capacity_);
    data_[size_++] = val;
  }

  [[nodiscard]] ql::span<const T> span() const noexcept {
    return ql::span<const T>(data_.get(), size_);
  }

  [[nodiscard]] ql::span<T> mutableSpan() noexcept {
    return ql::span<T>(data_.get(), size_);
  }

  [[nodiscard]] size_t size() const noexcept { return size_; }
  [[nodiscard]] size_t capacity() const noexcept { return capacity_; }
  [[nodiscard]] const T* data() const noexcept { return data_.get(); }
  [[nodiscard]] T* data() noexcept { return data_.get(); }

  [[nodiscard]] const T& operator[](size_t idx) const noexcept {
    return data_[idx];
  }

  [[nodiscard]] T& operator[](size_t idx) noexcept {
    return data_[idx];
  }
};

}  // namespace qlever::export_pipeline
