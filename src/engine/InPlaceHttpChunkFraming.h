// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_INPLACEHTTPCHUNKFRAMING_H
#define QLEVER_SRC_ENGINE_INPLACEHTTPCHUNKFRAMING_H

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "util/AlignedAllocator.h"
#include "util/Exception.h"
#include "util/Invariants.h"

namespace ad_utility::http {

namespace detail {

// _____________________________________________________________________________
// Lowercase hex lookup table for branchless hex conversions.
inline constexpr char HEX_DIGITS[17] = "0123456789abcdef";

// _____________________________________________________________________________
// Branchless calculation of exact number of hex digits needed for uint64_t.
// Returns 1 for val == 0 ('0').
// For val > 0: ceil((64 - countl_zero(val)) / 4) = (67 - countl_zero(val)) >> 2.
// Note: (val | 1ULL) ensures val == 0 has countl_zero == 63 -> (67 - 63) >> 2 = 1.
[[nodiscard]] inline constexpr uint32_t numHexDigits(uint64_t val) noexcept {
  return (67 - std::countl_zero(val | 1ULL)) >> 2;
}

// _____________________________________________________________________________
// Branchless backward hex digit serialization ending at `endPtr`.
// Writes `digits` hex characters into `[endPtr - digits, endPtr)`.
// Returns pointer to first character written (`endPtr - digits`).
inline char* writeHexDigitsBackwards(char* endPtr, uint64_t val,
                                     uint32_t digits) noexcept {
  AD_CONTRACT_CHECK(endPtr != nullptr);
  AD_CONTRACT_CHECK(digits >= 1 && digits <= 16);
  char* p = endPtr;
  for (uint32_t i = 0; i < digits; ++i) {
    *(--p) = HEX_DIGITS[val & 0x0F];
    val >>= 4;
  }
  return p;
}

}  // namespace detail

// _____________________________________________________________________________
// Summary metrics returned upon finalizing an InPlaceHttpChunkStreamer.
struct HttpStreamSummary {
  uint64_t totalPayloadBytes_ = 0;
  uint64_t totalFramedBytes_ = 0;
  uint64_t chunksEmitted_ = 0;
};

// _____________________________________________________________________________
// Deep Module: InPlaceHttpChunk
//
// Zero-copy HTTP 1.1 Chunked Transfer Encoding buffer manager.
//
// HTTP 1.1 chunked encoding (RFC 7230 / RFC 9112) frames payload chunks as:
//   <hex-length>\r\n<payload>\r\n
//
// The chunk buffer reserves space for the framing bytes, so payload data can
// be framed in place without an additional copy when the completed chunk is
// emitted. The streaming wrapper copies input data into this buffer and
// pre-reserves 16 bytes at the head and 2 bytes at the tail:
//
//   [ Head: 16 bytes ] [ Payload: N bytes ] [ Tail: 2 bytes ]
//          ^                   ^                   ^
//      Hex + \r\n           Payload               \r\n
//
// During finalizeChunk(payloadBytes):
// 1. The trailing \r\n is written immediately at `payloadEnd`.
// 2. The branchless hex length string + \r\n is written backwards from
//    `payloadStart` into the pre-reserved header.
// 3. A single contiguous `ql::span<const char>` spanning exactly from the start
//    of the hex string to the trailing \r\n is returned ready for zero-copy
//    socket transmission (e.g. write(2), writev(2), send(2), Boost.Beast).
//
// Complies with ~/ARCHITECTURE.md (7 Universal Software Architecture Laws):
// - Law 1: Deep Module (hides buffer math, hex conversions, and framing offsets)
// - Law 2: Information Hiding & Zero Accounting Leakage
// - Law 3: Complexity Gravity (pulls framing down from callers)
// - Law 4: Define Errors Away & Fail-Fast Preconditions
// - Law 5: General-purpose buffer management (owning or non-owning)
// - Law 6: Intent-Revealing Naming
// - Law 7: Strategic Invariant Verification (WithInvariants<Derived>)
class InPlaceHttpChunk : public WithInvariants<InPlaceHttpChunk> {
 public:
  static constexpr size_t HEADER_RESERVE_BYTES = 16;
  static constexpr size_t TAIL_RESERVE_BYTES = 2;
  static constexpr size_t TOTAL_OVERHEAD_BYTES =
      HEADER_RESERVE_BYTES + TAIL_RESERVE_BYTES;  // 18 bytes
  static constexpr size_t DEFAULT_PAYLOAD_CAPACITY = 1024 * 1024;  // 1 MB

  // 64-byte aligned buffer type for CPU cache-line & vector streaming stores
  using AlignedBuffer =
      std::vector<char, AlignedAllocator<char, std::allocator<char>, 64>>;

 private:
  char* buffer_{nullptr};
  size_t totalCapacity_{0};
  size_t maxPayloadCapacity_{0};
  size_t lastPayloadBytes_{0};
  bool isFinalized_{false};
  const char* framedStart_{nullptr};
  size_t framedLength_{0};
  std::optional<AlignedBuffer> ownedBuffer_{std::nullopt};

 public:
  // ___________________________________________________________________________
  // Construct an owning chunk with 64-byte aligned internal memory.
  explicit InPlaceHttpChunk(size_t maxPayloadCapacity = DEFAULT_PAYLOAD_CAPACITY)
      : totalCapacity_{maxPayloadCapacity + TOTAL_OVERHEAD_BYTES},
        maxPayloadCapacity_{maxPayloadCapacity},
        lastPayloadBytes_{0},
        isFinalized_{false},
        framedStart_{nullptr},
        framedLength_{0} {
    ownedBuffer_.emplace(totalCapacity_);
    buffer_ = ownedBuffer_->data();
    checkInvariants();
  }

  // ___________________________________________________________________________
  // Construct a non-owning chunk wrapping a caller-provided destination span.
  // Precondition: destinationBuffer.size() >= TOTAL_OVERHEAD_BYTES (18 bytes).
  explicit InPlaceHttpChunk(ql::span<char> destinationBuffer)
      : buffer_{destinationBuffer.data()},
        totalCapacity_{destinationBuffer.size()},
        maxPayloadCapacity_{destinationBuffer.size() >= TOTAL_OVERHEAD_BYTES
                                ? destinationBuffer.size() - TOTAL_OVERHEAD_BYTES
                                : 0},
        lastPayloadBytes_{0},
        isFinalized_{false},
        framedStart_{nullptr},
        framedLength_{0},
        ownedBuffer_{std::nullopt} {
    AD_CONTRACT_CHECK(destinationBuffer.size() >= TOTAL_OVERHEAD_BYTES);
    checkInvariants();
  }

  // ___________________________________________________________________________
  
  InPlaceHttpChunk(InPlaceHttpChunk&& other) noexcept
      : buffer_{other.buffer_},
        totalCapacity_{other.totalCapacity_},
        maxPayloadCapacity_{other.maxPayloadCapacity_},
        lastPayloadBytes_{other.lastPayloadBytes_},
        isFinalized_{other.isFinalized_},
        framedStart_{other.framedStart_},
        framedLength_{other.framedLength_},
        ownedBuffer_{std::move(other.ownedBuffer_)} {
    other.buffer_ = nullptr;
    other.totalCapacity_ = 0;
    other.maxPayloadCapacity_ = 0;
    other.lastPayloadBytes_ = 0;
    other.isFinalized_ = false;
    other.framedStart_ = nullptr;
    other.framedLength_ = 0;
    checkInvariants();
  }

  InPlaceHttpChunk& operator=(InPlaceHttpChunk&& other) noexcept {
    if (this != &other) {
      buffer_ = other.buffer_;
      totalCapacity_ = other.totalCapacity_;
      maxPayloadCapacity_ = other.maxPayloadCapacity_;
      lastPayloadBytes_ = other.lastPayloadBytes_;
      isFinalized_ = other.isFinalized_;
      framedStart_ = other.framedStart_;
      framedLength_ = other.framedLength_;
      ownedBuffer_ = std::move(other.ownedBuffer_);

      other.buffer_ = nullptr;
      other.totalCapacity_ = 0;
      other.maxPayloadCapacity_ = 0;
      other.lastPayloadBytes_ = 0;
      other.isFinalized_ = false;
      other.framedStart_ = nullptr;
      other.framedLength_ = 0;
      checkInvariants();
    }
    return *this;
  }

  InPlaceHttpChunk(const InPlaceHttpChunk&) = delete;
  InPlaceHttpChunk& operator=(const InPlaceHttpChunk&) = delete;

  ~InPlaceHttpChunk() = default;

  // ___________________________________________________________________________
  // Structural Invariant Verification (Law 7 & Architecture Standard § 3).
  void checkInvariants() const {
    if (totalCapacity_ == 0) {
      AD_CORRECTNESS_CHECK(buffer_ == nullptr);
      AD_CORRECTNESS_CHECK(maxPayloadCapacity_ == 0);
    } else {
      AD_CORRECTNESS_CHECK(buffer_ != nullptr);
      AD_CORRECTNESS_CHECK(totalCapacity_ >= TOTAL_OVERHEAD_BYTES);
      AD_CORRECTNESS_CHECK(maxPayloadCapacity_ ==
                           totalCapacity_ - TOTAL_OVERHEAD_BYTES);
    }
    if (ownedBuffer_.has_value()) {
      AD_CORRECTNESS_CHECK(buffer_ == ownedBuffer_->data());
      AD_CORRECTNESS_CHECK(totalCapacity_ == ownedBuffer_->size());
    }
    if (isFinalized_) {
      AD_CORRECTNESS_CHECK(framedStart_ >= buffer_);
      AD_CORRECTNESS_CHECK(framedStart_ + framedLength_ <=
                           buffer_ + totalCapacity_);
      AD_CORRECTNESS_CHECK(lastPayloadBytes_ <= maxPayloadCapacity_);
    }
  }

  // ___________________________________________________________________________
  // Direct pointer to writable payload region (offset by HEADER_RESERVE_BYTES).
  [[nodiscard]] char* payloadData() noexcept {
    return buffer_ + HEADER_RESERVE_BYTES;
  }

  [[nodiscard]] const char* payloadData() const noexcept {
    return buffer_ + HEADER_RESERVE_BYTES;
  }

  // ___________________________________________________________________________
  // Writable span covering the maximum payload capacity.
  [[nodiscard]] ql::span<char> payloadSpan() noexcept {
    return {payloadData(), maxPayloadCapacity_};
  }

  [[nodiscard]] ql::span<const char> payloadSpan() const noexcept {
    return {payloadData(), maxPayloadCapacity_};
  }

  // ___________________________________________________________________________
  // Finalize the chunk in-place with branchless hex length formatting.
  //
  // Writes:
  // 1. Trailing CRLF ("\r\n") immediately after the `payloadBytes` payload.
  // 2. Preceding CRLF ("\r\n") immediately before the payload.
  // 3. Branchless hex representation of `payloadBytes` before CRLF.
  //
  // Returns single contiguous `ql::span<const char>` ready for direct socket
  // transmission with ZERO copies.
  [[nodiscard]] ql::span<const char> finalizeChunk(size_t payloadBytes) {
    auto guard = makeInvariantGuard();
    AD_CONTRACT_CHECK(payloadBytes <= maxPayloadCapacity_);

    char* headerEnd = buffer_ + HEADER_RESERVE_BYTES;

    
    *(headerEnd - 2) = '\r';
    *(headerEnd - 1) = '\n';

    // Branchless hex length formatting backwards
    const uint32_t digits = detail::numHexDigits(payloadBytes);
    AD_CORRECTNESS_CHECK(digits <= 16);  // At most 16 hex digits for uint64_t
    char* framedStart = detail::writeHexDigitsBackwards(headerEnd - 2,
                                                       payloadBytes, digits);

    
    char* tailPtr = headerEnd + payloadBytes;
    *(tailPtr) = '\r';
    *(tailPtr + 1) = '\n';

    framedStart_ = framedStart;
    framedLength_ = static_cast<size_t>((tailPtr + 2) - framedStart);
    lastPayloadBytes_ = payloadBytes;
    isFinalized_ = true;

    return {framedStart_, framedLength_};
  }

  // ___________________________________________________________________________
  // Convenience method: Generate the terminating HTTP/1.1 chunk ("0\r\n\r\n").
  [[nodiscard]] ql::span<const char> createFinalChunk() {
    return finalizeChunk(0);
  }

  // ___________________________________________________________________________
  // Reset the chunk state for reuse across multiple streaming iterations.
  void reset() noexcept {
    auto guard = makeInvariantGuard();
    isFinalized_ = false;
    lastPayloadBytes_ = 0;
    framedStart_ = nullptr;
    framedLength_ = 0;
  }

  // ___________________________________________________________________________
  // Retarget the chunk to a new caller-provided buffer span.
  void reset(ql::span<char> newBuffer) {
    auto guard = makeInvariantGuard();
    AD_CONTRACT_CHECK(newBuffer.size() >= TOTAL_OVERHEAD_BYTES);
    ownedBuffer_.reset();
    buffer_ = newBuffer.data();
    totalCapacity_ = newBuffer.size();
    maxPayloadCapacity_ = totalCapacity_ - TOTAL_OVERHEAD_BYTES;
    isFinalized_ = false;
    lastPayloadBytes_ = 0;
    framedStart_ = nullptr;
    framedLength_ = 0;
  }

  // ___________________________________________________________________________
  // Provide accessors.
  [[nodiscard]] size_t maxPayloadCapacity() const noexcept {
    return maxPayloadCapacity_;
  }
  [[nodiscard]] size_t totalCapacity() const noexcept { return totalCapacity_; }
  [[nodiscard]] bool isFinalized() const noexcept { return isFinalized_; }
  [[nodiscard]] size_t lastPayloadBytes() const noexcept {
    return lastPayloadBytes_;
  }
  [[nodiscard]] size_t framedLength() const noexcept { return framedLength_; }
  [[nodiscard]] bool isOwner() const noexcept {
    return ownedBuffer_.has_value();
  }

  [[nodiscard]] ql::span<const char> lastFinalizedSpan() const {
    AD_CONTRACT_CHECK(isFinalized_);
    return {framedStart_, framedLength_};
  }
};

// _____________________________________________________________________________
// Deep Module: InPlaceHttpChunkStreamer
//
// High-level streaming engine that buffers arbitrary writes, automatically
// frames full chunks in-place, and emits ready-to-transmit contiguous
// `ql::span<const char>` buffers to a sink callback.
class InPlaceHttpChunkStreamer
    : public WithInvariants<InPlaceHttpChunkStreamer> {
 public:
  using ChunkSink = std::function<void(ql::span<const char>)>;

 private:
  InPlaceHttpChunk chunkBuffer_;
  ChunkSink sink_;
  size_t currentPayloadBytes_ = 0;
  bool emitTerminatingChunkOnFinalize_ = true;

  // Aggregate stream metrics.
  uint64_t totalPayloadBytes_ = 0;
  uint64_t totalFramedBytes_ = 0;
  uint64_t chunksEmitted_ = 0;

 public:
  // ___________________________________________________________________________
  // Construct a streaming chunker with a designated consumer sink callback.
  explicit InPlaceHttpChunkStreamer(
      ChunkSink sink,
      size_t chunkPayloadCapacity = InPlaceHttpChunk::DEFAULT_PAYLOAD_CAPACITY,
      bool emitTerminatingChunkOnFinalize = true)
      : chunkBuffer_(chunkPayloadCapacity),
        sink_(std::move(sink)),
        currentPayloadBytes_(0),
        emitTerminatingChunkOnFinalize_(emitTerminatingChunkOnFinalize) {
    AD_CONTRACT_CHECK(sink_ != nullptr);
    checkInvariants();
  }

  // Use move-only semantics.
  InPlaceHttpChunkStreamer(InPlaceHttpChunkStreamer&&) noexcept = default;
  InPlaceHttpChunkStreamer& operator=(InPlaceHttpChunkStreamer&&) noexcept =
      default;
  InPlaceHttpChunkStreamer(const InPlaceHttpChunkStreamer&) = delete;
  InPlaceHttpChunkStreamer& operator=(const InPlaceHttpChunkStreamer&) = delete;

  ~InPlaceHttpChunkStreamer() = default;

  // ___________________________________________________________________________
  // Invariant verification.
  void checkInvariants() const {
    AD_CORRECTNESS_CHECK(currentPayloadBytes_ <=
                         chunkBuffer_.maxPayloadCapacity());
  }

  // ___________________________________________________________________________
  // Write arbitrary raw bytes into the streaming chunk buffer with auto-framing.
  void write(const void* src, size_t numBytes) {
    auto guard = makeInvariantGuard();
    AD_CONTRACT_CHECK(src != nullptr || numBytes == 0);

    if (numBytes == 0) {
      return;
    }

    const auto* srcPtr = static_cast<const char*>(src);
    const size_t maxCap = chunkBuffer_.maxPayloadCapacity();

    while (numBytes > 0) {
      const size_t available = maxCap - currentPayloadBytes_;
      if (available == 0) {
        flushCurrentChunk();
        continue;
      }

      const size_t toCopy = std::min(available, numBytes);
      std::memcpy(chunkBuffer_.payloadData() + currentPayloadBytes_, srcPtr,
                  toCopy);
      currentPayloadBytes_ += toCopy;
      srcPtr += toCopy;
      numBytes -= toCopy;

      if (currentPayloadBytes_ == maxCap) {
        flushCurrentChunk();
      }
    }
  }

  // ___________________________________________________________________________
  
  void write(std::string_view sv) { write(sv.data(), sv.size()); }

  // ___________________________________________________________________________
  
  void write(ql::span<const char> span) { write(span.data(), span.size()); }

  // ___________________________________________________________________________
  
  void writeChar(char c) { write(&c, 1); }

  // ___________________________________________________________________________
  // Explicitly flush accumulated payload data as an in-place framed chunk.
  void flush() {
    auto guard = makeInvariantGuard();
    if (currentPayloadBytes_ > 0) {
      flushCurrentChunk();
    }
  }

  // ___________________________________________________________________________
  // Finalizing typestate transition (Law 2 / Law 3 & Architecture Standard § 3).
  // Flushes any remaining bytes, optionally emits the "0\r\n\r\n" terminating
  // chunk, and returns the aggregate HTTP stream summary metrics.
  [[nodiscard]] HttpStreamSummary finalize() && {
    flush();
    if (emitTerminatingChunkOnFinalize_) {
      auto finalSpan = chunkBuffer_.createFinalChunk();
      totalFramedBytes_ += finalSpan.size();
      ++chunksEmitted_;
      sink_(finalSpan);
    }

    HttpStreamSummary summary{totalPayloadBytes_, totalFramedBytes_,
                              chunksEmitted_};
    currentPayloadBytes_ = 0;
    return summary;
  }

  // ___________________________________________________________________________
  // Metrics & State Inspection
  [[nodiscard]] size_t currentBufferedBytes() const noexcept {
    return currentPayloadBytes_;
  }
  [[nodiscard]] uint64_t totalPayloadBytes() const noexcept {
    return totalPayloadBytes_ + currentPayloadBytes_;
  }
  [[nodiscard]] uint64_t totalFramedBytes() const noexcept {
    return totalFramedBytes_;
  }
  [[nodiscard]] uint64_t chunksEmitted() const noexcept {
    return chunksEmitted_;
  }

 private:
  // ___________________________________________________________________________
  // Internal chunk finalization and emission helper.
  void flushCurrentChunk() {
    AD_CORRECTNESS_CHECK(currentPayloadBytes_ > 0);
    auto framedSpan = chunkBuffer_.finalizeChunk(currentPayloadBytes_);
    totalPayloadBytes_ += currentPayloadBytes_;
    totalFramedBytes_ += framedSpan.size();
    ++chunksEmitted_;

    sink_(framedSpan);

    currentPayloadBytes_ = 0;
    chunkBuffer_.reset();
  }
};

}  // namespace ad_utility::http

namespace ql::http {
using ad_utility::http::InPlaceHttpChunk;
using ad_utility::http::InPlaceHttpChunkStreamer;
using ad_utility::http::HttpStreamSummary;
}  // namespace ql::http

#endif  // QLEVER_SRC_ENGINE_INPLACEHTTPCHUNKFRAMING_H
