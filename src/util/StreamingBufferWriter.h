// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_STREAMINGBUFFERWRITER_H
#define QLEVER_SRC_UTIL_STREAMINGBUFFERWRITER_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__)
#include <emmintrin.h>
#include <immintrin.h>
#endif

#include "util/AlignedAllocator.h"
#include "util/Exception.h"
#include "util/Invariants.h"

namespace ad_utility {

// _____________________________________________________________________________
// High-throughput streaming buffer writer that utilizes non-temporal vector
// stores (`_mm_stream_si128` / `MOVNTDQ`) in 64-byte blocks using 16-byte-
// aligned stores.
//
// Uses CPU write-combining (WC) buffers to reduce cache pollution from
// multi-gigabyte export streaming chunks, helping preserve hot vocabulary
// tries, index metadata, and query caches.
//
// Conforms to the Software Architecture Standard (~/ARCHITECTURE.md):
// - Deep Module: Hides vector intrinsics, alignment math, and memory barriers.
// - Design by Contract: Enforces preconditions (`AD_CONTRACT_CHECK`) and
//   continuous structural state verification (`WithInvariants`).
class StreamingBufferWriter
    : public WithInvariants<StreamingBufferWriter> {
 public:
  static constexpr size_t Alignment = 64;
  static constexpr size_t VectorStoreSize = 16;
  static constexpr size_t BlockSize = 64;

  using AlignedBuffer =
      std::vector<char, AlignedAllocator<char, std::allocator<char>, Alignment>>;

 private:
  char* buffer_{nullptr};
  size_t capacity_{0};
  size_t bytesWritten_{0};
  std::optional<AlignedBuffer> ownedBuffer_{std::nullopt};

 public:
  // ___________________________________________________________________________
  // Static Helper: Drain CPU write-combining buffers and enforce store ordering.
  static void sfence() noexcept {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__)
    _mm_sfence();
#else
    std::atomic_thread_fence(std::memory_order_release);
#endif
  }

  // ___________________________________________________________________________
  // Static Helper: Non-temporal streaming memory copy without trailing fence.
  // Copies `count` bytes from `src` to `dest` using 64-byte non-temporal stores
  // on aligned blocks, with standard cache-friendly head and tail handling.
  static void streamCopyNoFence(void* dest, const void* src, size_t count) {
    AD_CONTRACT_CHECK(dest != nullptr || count == 0);
    AD_CONTRACT_CHECK(src != nullptr || count == 0);

    if (count == 0) {
      return;
    }

    auto* destPtr = static_cast<char*>(dest);
    const auto* srcPtr = static_cast<const char*>(src);

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__)
    // Phase 1: Align destination pointer to 16-byte vector boundary.
    const auto destAddr = reinterpret_cast<uintptr_t>(destPtr);
    const size_t unalignedHead = (VectorStoreSize - (destAddr & (VectorStoreSize - 1))) &
                                 (VectorStoreSize - 1);
    const size_t headBytes = std::min(unalignedHead, count);

    if (headBytes > 0) {
      std::memcpy(destPtr, srcPtr, headBytes);
      destPtr += headBytes;
      srcPtr += headBytes;
      count -= headBytes;
    }

    // Phase 2: Stream 64-byte cache line blocks (4 x 16-byte stores).
    while (count >= BlockSize) {
      const auto v0 = _mm_loadu_si128(
          reinterpret_cast<const __m128i*>(srcPtr + 0 * VectorStoreSize));
      const auto v1 = _mm_loadu_si128(
          reinterpret_cast<const __m128i*>(srcPtr + 1 * VectorStoreSize));
      const auto v2 = _mm_loadu_si128(
          reinterpret_cast<const __m128i*>(srcPtr + 2 * VectorStoreSize));
      const auto v3 = _mm_loadu_si128(
          reinterpret_cast<const __m128i*>(srcPtr + 3 * VectorStoreSize));

      _mm_stream_si128(
          reinterpret_cast<__m128i*>(destPtr + 0 * VectorStoreSize), v0);
      _mm_stream_si128(
          reinterpret_cast<__m128i*>(destPtr + 1 * VectorStoreSize), v1);
      _mm_stream_si128(
          reinterpret_cast<__m128i*>(destPtr + 2 * VectorStoreSize), v2);
      _mm_stream_si128(
          reinterpret_cast<__m128i*>(destPtr + 3 * VectorStoreSize), v3);

      destPtr += BlockSize;
      srcPtr += BlockSize;
      count -= BlockSize;
    }

    // Phase 3: Stream any remaining 16-byte aligned pieces.
    while (count >= VectorStoreSize) {
      const auto v = _mm_loadu_si128(
          reinterpret_cast<const __m128i*>(srcPtr));
      _mm_stream_si128(reinterpret_cast<__m128i*>(destPtr), v);
      destPtr += VectorStoreSize;
      srcPtr += VectorStoreSize;
      count -= VectorStoreSize;
    }

    // Phase 4: Handle unaligned tail bytes (< 16 bytes).
    if (count > 0) {
      std::memcpy(destPtr, srcPtr, count);
    }
#else
    // Fallback for non-x86 architectures.
    std::memcpy(destPtr, srcPtr, count);
#endif
  }

  // ___________________________________________________________________________
  // Static Helper: Non-temporal streaming memory copy with trailing memory fence.
  static void streamCopy(void* dest, const void* src, size_t count) {
    streamCopyNoFence(dest, src, count);
    sfence();
  }

  // ___________________________________________________________________________
  // Construct a writer wrapping a caller-provided destination buffer span.
  explicit StreamingBufferWriter(std::span<char> destinationBuffer)
      : buffer_{destinationBuffer.data()},
        capacity_{destinationBuffer.size()},
        bytesWritten_{0},
        ownedBuffer_{std::nullopt} {
    checkInvariants();
  }

  // ___________________________________________________________________________
  // Construct a writer wrapping a caller-provided memory pointer and capacity.
  StreamingBufferWriter(char* destination, size_t capacity)
      : buffer_{destination},
        capacity_{capacity},
        bytesWritten_{0},
        ownedBuffer_{std::nullopt} {
    AD_CONTRACT_CHECK(destination != nullptr || capacity == 0);
    checkInvariants();
  }

  // ___________________________________________________________________________
  // Construct an owning writer with a 64-byte aligned internal buffer.
  explicit StreamingBufferWriter(size_t initialCapacity)
      : capacity_{initialCapacity},
        bytesWritten_{0} {
    ownedBuffer_.emplace(initialCapacity);
    buffer_ = ownedBuffer_->data();
    checkInvariants();
  }

  // ___________________________________________________________________________
  StreamingBufferWriter(StreamingBufferWriter&& other) noexcept
      : buffer_{other.buffer_},
        capacity_{other.capacity_},
        bytesWritten_{other.bytesWritten_},
        ownedBuffer_{std::move(other.ownedBuffer_)} {
    other.buffer_ = nullptr;
    other.capacity_ = 0;
    other.bytesWritten_ = 0;
    checkInvariants();
  }

  StreamingBufferWriter& operator=(StreamingBufferWriter&& other) noexcept {
    if (this != &other) {
      buffer_ = other.buffer_;
      capacity_ = other.capacity_;
      bytesWritten_ = other.bytesWritten_;
      ownedBuffer_ = std::move(other.ownedBuffer_);

      other.buffer_ = nullptr;
      other.capacity_ = 0;
      other.bytesWritten_ = 0;
      checkInvariants();
    }
    return *this;
  }

  StreamingBufferWriter(const StreamingBufferWriter&) = delete;
  StreamingBufferWriter& operator=(const StreamingBufferWriter&) = delete;

  ~StreamingBufferWriter() = default;

  // ___________________________________________________________________________
  // Structural invariant check required by `ad_utility::InvariantStatefulClass`.
  void checkInvariants() const {
    AD_CORRECTNESS_CHECK(bytesWritten_ <= capacity_);
    if (capacity_ > 0) {
      AD_CORRECTNESS_CHECK(buffer_ != nullptr);
    }
    if (ownedBuffer_.has_value()) {
      AD_CORRECTNESS_CHECK(buffer_ == ownedBuffer_->data());
      AD_CORRECTNESS_CHECK(capacity_ == ownedBuffer_->size());
    }
  }

  // ___________________________________________________________________________
  // Write raw bytes using non-temporal streaming stores.
  void write(const void* src, size_t numBytes) {
    auto guard = makeInvariantGuard();
    AD_CONTRACT_CHECK(src != nullptr || numBytes == 0);
    AD_CONTRACT_CHECK(bytesWritten_ + numBytes <= capacity_);

    if (numBytes == 0) {
      return;
    }

    streamCopyNoFence(buffer_ + bytesWritten_, src, numBytes);
    bytesWritten_ += numBytes;
  }

  // ___________________________________________________________________________
  // Write string_view data using non-temporal streaming stores.
  void write(std::string_view data) {
    write(data.data(), data.size());
  }

  // ___________________________________________________________________________
  // Write span data using non-temporal streaming stores.
  void write(std::span<const char> data) {
    write(data.data(), data.size());
  }

  // ___________________________________________________________________________
  // Complete the current streaming chunk and drain CPU write-combining buffers.
  void flush() {
    auto guard = makeInvariantGuard();
    sfence();
  }

  // ___________________________________________________________________________
  // Reset write position to the beginning of the existing buffer.
  void reset() noexcept {
    auto guard = makeInvariantGuard();
    bytesWritten_ = 0;
  }

  // ___________________________________________________________________________
  // Retarget the writer to a new caller-provided buffer span.
  void reset(std::span<char> newBuffer) noexcept {
    auto guard = makeInvariantGuard();
    ownedBuffer_.reset();
    buffer_ = newBuffer.data();
    capacity_ = newBuffer.size();
    bytesWritten_ = 0;
  }

  // ___________________________________________________________________________
  // Accessors
  [[nodiscard]] size_t bytesWritten() const noexcept { return bytesWritten_; }
  [[nodiscard]] size_t capacity() const noexcept { return capacity_; }
  [[nodiscard]] size_t remainingCapacity() const noexcept {
    return capacity_ - bytesWritten_;
  }
  [[nodiscard]] bool empty() const noexcept { return bytesWritten_ == 0; }
  [[nodiscard]] bool full() const noexcept { return bytesWritten_ == capacity_; }
  [[nodiscard]] bool isOwner() const noexcept { return ownedBuffer_.has_value(); }

  [[nodiscard]] char* currentWritePointer() noexcept {
    return buffer_ + bytesWritten_;
  }
  [[nodiscard]] const char* currentWritePointer() const noexcept {
    return buffer_ + bytesWritten_;
  }
  [[nodiscard]] char* data() noexcept { return buffer_; }
  [[nodiscard]] const char* data() const noexcept { return buffer_; }

  [[nodiscard]] std::span<const char> writtenSpan() const noexcept {
    return {buffer_, bytesWritten_};
  }
  [[nodiscard]] std::span<char> remainingSpan() noexcept {
    return {buffer_ + bytesWritten_, capacity_ - bytesWritten_};
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_STREAMINGBUFFERWRITER_H
