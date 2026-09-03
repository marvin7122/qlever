// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ZEROCOPYSOCKETSENDER_H
#define QLEVER_SRC_UTIL_ZEROCOPYSOCKETSENDER_H

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "backports/span.h"
#include "util/AlignedAllocator.h"
#include "util/Exception.h"
#include "util/Invariants.h"
#include "util/Log.h"

#if defined(__has_include)
#if __has_include(<liburing.h>)
#define QLEVER_HAS_LIBURING 1
#include <liburing.h>
#endif
#endif

#if defined(QLEVER_HAS_IO_URING) && !defined(QLEVER_HAS_LIBURING)
#define QLEVER_HAS_LIBURING 1
#include <liburing.h>
#endif

namespace ad_utility {

// 4KB memory page alignment for zero-copy buffers.
inline constexpr size_t kZeroCopyPageAlignment = 4096;

// _____________________________________________________________________________
// Configure a `ZeroCopySocketSender`.
struct ZeroCopySenderConfig {
  size_t ringEntries = 512;
  size_t numBuffers = 64;
  size_t bufferSizeBytes = 64 * 1024;  // 64 KB per slot
  bool useRegisteredBuffers = true;
  bool useZeroCopy = true;
  unsigned int additionalFlags = 0;
};

// _____________________________________________________________________________
// Preallocated, page-aligned fixed memory buffer pool.
// Provides buffers that can optionally be registered with the kernel via
// IORING_REGISTER_BUFFERS. Provides O(1) buffer acquisition and recycling with
// zero runtime dynamic allocations.
class ZeroCopyBufferPool : public WithInvariants<ZeroCopyBufferPool> {
 private:
  void* rawBuffer_ = nullptr;
  size_t numBuffers_ = 0;
  size_t bufferSizeBytes_ = 0;
  size_t totalBytes_ = 0;

  std::vector<iovec> iovecs_;
  std::vector<uint32_t> freeSlots_;
  std::vector<bool> slotInUse_;

 public:
  ZeroCopyBufferPool() = default;

  // Allocate a pool of `numBuffers` slots, each `bufferSizeBytes` bytes.
  // Both total size and buffer sizes are 4KB-aligned.
  ZeroCopyBufferPool(size_t numBuffers, size_t bufferSizeBytes) {
    AD_CONTRACT_CHECK(numBuffers > 0);
    AD_CONTRACT_CHECK(bufferSizeBytes > 0);
    AD_CONTRACT_CHECK((bufferSizeBytes % kZeroCopyPageAlignment) == 0);

    numBuffers_ = numBuffers;
    bufferSizeBytes_ = bufferSizeBytes;
    totalBytes_ = numBuffers_ * bufferSizeBytes_;

    int ret =
        posix_memalign(&rawBuffer_, kZeroCopyPageAlignment, totalBytes_);
    if (ret != 0 || rawBuffer_ == nullptr) {
      AD_THROW("posix_memalign failed to allocate zero-copy pinned buffer pool");
    }

    // Pre-fault memory pages before registration to avoid soft page faults
    // during async DMA transmission.
    std::memset(rawBuffer_, 0, totalBytes_);

    iovecs_.reserve(numBuffers_);
    freeSlots_.reserve(numBuffers_);
    slotInUse_.assign(numBuffers_, false);

    auto* basePtr = static_cast<char*>(rawBuffer_);
    for (size_t i = 0; i < numBuffers_; ++i) {
      iovecs_.push_back(iovec{.iov_base = basePtr + (i * bufferSizeBytes_),
                              .iov_len = bufferSizeBytes_});
      freeSlots_.push_back(static_cast<uint32_t>(numBuffers_ - 1 - i));
    }

    checkInvariants();
  }

  ~ZeroCopyBufferPool() {
    if (rawBuffer_ != nullptr) {
      std::free(rawBuffer_);
      rawBuffer_ = nullptr;
    }
  }

  ZeroCopyBufferPool(const ZeroCopyBufferPool&) = delete;
  ZeroCopyBufferPool& operator=(const ZeroCopyBufferPool&) = delete;

  ZeroCopyBufferPool(ZeroCopyBufferPool&& other) noexcept
      : rawBuffer_{std::exchange(other.rawBuffer_, nullptr)},
        numBuffers_{std::exchange(other.numBuffers_, 0)},
        bufferSizeBytes_{std::exchange(other.bufferSizeBytes_, 0)},
        totalBytes_{std::exchange(other.totalBytes_, 0)},
        iovecs_{std::move(other.iovecs_)},
        freeSlots_{std::move(other.freeSlots_)},
        slotInUse_{std::move(other.slotInUse_)} {}

  ZeroCopyBufferPool& operator=(ZeroCopyBufferPool&& other) noexcept {
    if (this != &other) {
      if (rawBuffer_ != nullptr) {
        std::free(rawBuffer_);
      }
      rawBuffer_ = std::exchange(other.rawBuffer_, nullptr);
      numBuffers_ = std::exchange(other.numBuffers_, 0);
      bufferSizeBytes_ = std::exchange(other.bufferSizeBytes_, 0);
      totalBytes_ = std::exchange(other.totalBytes_, 0);
      iovecs_ = std::move(other.iovecs_);
      freeSlots_ = std::move(other.freeSlots_);
      slotInUse_ = std::move(other.slotInUse_);
    }
    return *this;
  }

  // Structural invariant enforcement (Law 3 / InvariantStatefulClass).
  void checkInvariants() const {
    if (numBuffers_ == 0) {
      AD_CORRECTNESS_CHECK(rawBuffer_ == nullptr);
      AD_CORRECTNESS_CHECK(totalBytes_ == 0);
      AD_CORRECTNESS_CHECK(iovecs_.empty());
      AD_CORRECTNESS_CHECK(freeSlots_.empty());
      AD_CORRECTNESS_CHECK(slotInUse_.empty());
      return;
    }

    AD_CORRECTNESS_CHECK(rawBuffer_ != nullptr);
    AD_CORRECTNESS_CHECK(bufferSizeBytes_ > 0);
    AD_CORRECTNESS_CHECK((bufferSizeBytes_ % kZeroCopyPageAlignment) == 0);
    AD_CORRECTNESS_CHECK(totalBytes_ == numBuffers_ * bufferSizeBytes_);
    AD_CORRECTNESS_CHECK(iovecs_.size() == numBuffers_);
    AD_CORRECTNESS_CHECK(slotInUse_.size() == numBuffers_);

    size_t inUseCount = 0;
    for (bool inUse : slotInUse_) {
      if (inUse) {
        ++inUseCount;
      }
    }
    AD_CORRECTNESS_CHECK(freeSlots_.size() + inUseCount == numBuffers_);
  }

  // Acquire a free buffer slot with zero dynamic allocations.
  // Returns std::nullopt if all buffer slots are currently in flight.
  [[nodiscard]] std::optional<uint32_t> acquireSlot() {
    auto guard = makeInvariantGuard();
    if (freeSlots_.empty()) {
      return std::nullopt;
    }
    uint32_t slot = freeSlots_.back();
    freeSlots_.pop_back();
    AD_CORRECTNESS_CHECK(slot < numBuffers_);
    AD_CORRECTNESS_CHECK(!slotInUse_[slot]);
    slotInUse_[slot] = true;
    return slot;
  }

  // Release and recycle a buffer slot back into the pool.
  void releaseSlot(uint32_t slot) {
    auto guard = makeInvariantGuard();
    AD_CONTRACT_CHECK(slot < numBuffers_);
    AD_CONTRACT_CHECK(slotInUse_[slot]);
    slotInUse_[slot] = false;
    freeSlots_.push_back(slot);
  }

  // Obtain a writable span view over slot `slotIndex`.
  [[nodiscard]] ql::span<char> getSlotSpan(uint32_t slotIndex) {
    AD_CONTRACT_CHECK(slotIndex < numBuffers_);
    AD_CONTRACT_CHECK(slotInUse_[slotIndex]);
    auto* ptr = static_cast<char*>(rawBuffer_) + (slotIndex * bufferSizeBytes_);
    return {ptr, bufferSizeBytes_};
  }

  // Obtain a const span view over slot `slotIndex`.
  [[nodiscard]] ql::span<const char> getSlotSpan(uint32_t slotIndex) const {
    AD_CONTRACT_CHECK(slotIndex < numBuffers_);
    AD_CONTRACT_CHECK(slotInUse_[slotIndex]);
    const auto* ptr =
        static_cast<const char*>(rawBuffer_) + (slotIndex * bufferSizeBytes_);
    return {ptr, bufferSizeBytes_};
  }

  [[nodiscard]] ql::span<const iovec> iovecs() const noexcept {
    return {iovecs_.data(), iovecs_.size()};
  }

  [[nodiscard]] size_t numBuffers() const noexcept { return numBuffers_; }
  [[nodiscard]] size_t bufferSizeBytes() const noexcept {
    return bufferSizeBytes_;
  }
  [[nodiscard]] size_t availableSlots() const noexcept {
    return freeSlots_.size();
  }
  [[nodiscard]] bool isSlotInUse(uint32_t slot) const {
    AD_CONTRACT_CHECK(slot < numBuffers_);
    return slotInUse_[slot];
  }
};

// _____________________________________________________________________________
// High-performance asynchronous zero-copy network socket sender utilizing
// Linux io_uring IORING_OP_SEND_ZC and IORING_REGISTER_BUFFERS.
//
// Manages submission queue entries (SQEs), tracks dual completion queue
// notifications (transmission completion + buffer release notification), and
// achieves zero runtime memory allocation on the transmission fast path.
class ZeroCopySocketSender : public WithInvariants<ZeroCopySocketSender> {
 private:
  ZeroCopySenderConfig config_;
  ZeroCopyBufferPool bufferPool_;

#ifdef QLEVER_HAS_LIBURING
  io_uring ring_{};
  bool ringInitialized_ = false;
  bool buffersRegistered_ = false;
#endif

  // Internal fixed metadata for an in-flight socket send request.
  struct InFlightRequest {
    uint32_t bufferIndex = 0;
    size_t expectedBytes = 0;
    bool waitingForNotification = false;
    bool active = false;
  };

  // Preallocated tracking table mapped by (requestId % tableSize)
  std::vector<InFlightRequest> inFlightTable_;
    size_t numInFlightRequests_ = 0;  // Requests awaiting complete processing
  size_t numInFlightBuffers_ = 0;   // Buffers awaiting kernel release
  uint64_t nextRequestId_ = 0;

  size_t totalBytesSent_ = 0;
  size_t totalPacketsSent_ = 0;

 public:
  explicit ZeroCopySocketSender(ZeroCopySenderConfig config = {})
      : config_{config},
        bufferPool_{config.numBuffers, config.bufferSizeBytes} {
    AD_CONTRACT_CHECK(config_.ringEntries > 0);
    AD_CONTRACT_CHECK(config_.numBuffers > 0);
    AD_CONTRACT_CHECK(config_.bufferSizeBytes > 0);

    inFlightTable_.resize(config_.ringEntries * 2);

    initRing();
    checkInvariants();
  }

  ~ZeroCopySocketSender() { teardown(); }

  ZeroCopySocketSender(const ZeroCopySocketSender&) = delete;
  ZeroCopySocketSender& operator=(const ZeroCopySocketSender&) = delete;

  ZeroCopySocketSender(ZeroCopySocketSender&& other) noexcept
      : config_{other.config_},
        bufferPool_{std::move(other.bufferPool_)},
#ifdef QLEVER_HAS_LIBURING
        ring_{other.ring_},
        ringInitialized_{std::exchange(other.ringInitialized_, false)},
        buffersRegistered_{std::exchange(other.buffersRegistered_, false)},
#endif
        inFlightTable_{std::move(other.inFlightTable_)},
        numInFlightRequests_{std::exchange(other.numInFlightRequests_, 0)},
        numInFlightBuffers_{std::exchange(other.numInFlightBuffers_, 0)},
        nextRequestId_{std::exchange(other.nextRequestId_, 0)},
        totalBytesSent_{std::exchange(other.totalBytesSent_, 0)},
        totalPacketsSent_{std::exchange(other.totalPacketsSent_, 0)} {
  }

  ZeroCopySocketSender& operator=(ZeroCopySocketSender&& other) noexcept {
    if (this != &other) {
      teardown();
      config_ = other.config_;
      bufferPool_ = std::move(other.bufferPool_);
#ifdef QLEVER_HAS_LIBURING
      ring_ = other.ring_;
      ringInitialized_ = std::exchange(other.ringInitialized_, false);
      buffersRegistered_ = std::exchange(other.buffersRegistered_, false);
#endif
      inFlightTable_ = std::move(other.inFlightTable_);
      numInFlightRequests_ = std::exchange(other.numInFlightRequests_, 0);
      numInFlightBuffers_ = std::exchange(other.numInFlightBuffers_, 0);
      nextRequestId_ = std::exchange(other.nextRequestId_, 0);
      totalBytesSent_ = std::exchange(other.totalBytesSent_, 0);
      totalPacketsSent_ = std::exchange(other.totalPacketsSent_, 0);
    }
    return *this;
  }

  // Structural invariant enforcement (Law 3 / InvariantStatefulClass).
  void checkInvariants() const {
    AD_CORRECTNESS_CHECK(config_.ringEntries > 0);
    AD_CORRECTNESS_CHECK(config_.numBuffers > 0);
    AD_CORRECTNESS_CHECK(config_.bufferSizeBytes > 0);
    AD_CORRECTNESS_CHECK(inFlightTable_.size() >= config_.ringEntries);
    AD_CORRECTNESS_CHECK(numInFlightRequests_ <= inFlightTable_.size());
    AD_CORRECTNESS_CHECK(numInFlightBuffers_ <= config_.numBuffers);

    bufferPool_.checkInvariants();
  }

  // ___________________________________________________________________________
  // Acquire an available buffer slot from the pool.
  // If all buffer slots are currently occupied by in-flight transmissions,
  // flushes pending SQEs to the kernel and reaps CQEs until a slot is released.
  // Guaranteed zero heap allocation.
  [[nodiscard]] uint32_t acquireBuffer() {
    auto guard = makeInvariantGuard();

    while (true) {
      auto slotOpt = bufferPool_.acquireSlot();
      if (slotOpt.has_value()) {
        return slotOpt.value();
      }

#ifdef QLEVER_HAS_LIBURING
      if (ringInitialized_) {
        // Pool exhausted: submit pending queue and drain completions
        io_uring_submit(&ring_);
        drainOneCqe();
        continue;
      }
#endif
      AD_THROW("Buffer pool exhausted with no asynchronous engine initialized");
    }
  }

  // Obtain writable span for the allocated buffer slot.
  [[nodiscard]] ql::span<char> getSlotSpan(uint32_t slotIndex) {
    return bufferPool_.getSlotSpan(slotIndex);
  }

  // ___________________________________________________________________________
  // Prepare and enqueue an asynchronous zero-copy socket send request.
  // If the submission queue is saturated, automatically flushes SQEs to the
  // kernel and drains CQEs.
  void sendChunk(int sockfd, uint32_t bufferIndex, size_t numBytes,
                 int flags = 0, unsigned int zcFlags = 0) {
    auto guard = makeInvariantGuard();
    AD_CONTRACT_CHECK(sockfd >= 0);
    AD_CONTRACT_CHECK(bufferIndex < config_.numBuffers);
    AD_CONTRACT_CHECK(numBytes > 0);
    AD_CONTRACT_CHECK(numBytes <= config_.bufferSizeBytes);

#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      sendChunkSync(sockfd, bufferIndex, numBytes, flags);
      return;
    }

    // If submission ring is full, submit and reap CQEs to free ring entries
    if (numInFlightRequests_ >= config_.ringEntries) {
      io_uring_submit(&ring_);
      while (numInFlightRequests_ >= config_.ringEntries) {
        drainOneCqe();
      }
    }

    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);

    const auto slotSpan = bufferPool_.getSlotSpan(bufferIndex);

    if (config_.useZeroCopy) {
      if (buffersRegistered_ && config_.useRegisteredBuffers) {
        // Use zero-copy sending with a registered fixed buffer (`IORING_OP_SEND_ZC`).
        io_uring_prep_send_zc_fixed(sqe, sockfd, slotSpan.data(), numBytes,
                                    flags, zcFlags, bufferIndex);
      } else {
        // Use zero-copy sending with an unpinned buffer.
        io_uring_prep_send_zc(sqe, sockfd, slotSpan.data(), numBytes, flags,
                              zcFlags);
      }
    } else {
      // Use a standard asynchronous `io_uring` send.
      io_uring_prep_send(sqe, sockfd, slotSpan.data(), numBytes, flags);
    }

    const uint64_t reqId = nextRequestId_++;
    const size_t tableIdx = reqId % inFlightTable_.size();
    AD_CORRECTNESS_CHECK(!inFlightTable_[tableIdx].active);

    inFlightTable_[tableIdx] = InFlightRequest{
        .bufferIndex = bufferIndex,
        .expectedBytes = numBytes,
        .waitingForNotification = false,
        .active = true,
    };

    io_uring_sqe_set_data64(sqe, reqId);
    ++numInFlightRequests_;
    ++numInFlightBuffers_;
#else
    sendChunkSync(sockfd, bufferIndex, numBytes, flags);
#endif
  }

  // ___________________________________________________________________________
  // Flush all queued SQEs to the kernel.
  void submit() {
    auto guard = makeInvariantGuard();
#ifdef QLEVER_HAS_LIBURING
    if (ringInitialized_) {
      int ret = io_uring_submit(&ring_);
      if (ret < 0 && ret != -EAGAIN && ret != -EBUSY) {
        AD_THROW(absl::StrCat("io_uring_submit failed (errno: ", -ret, ")"));
      }
    }
#endif
  }

  // ___________________________________________________________________________
  // Wait for and drain all in-flight requests and kernel notifications,
  // ensuring all buffers are recycled back to the pool.
  void flushAndDrainAll() {
    auto guard = makeInvariantGuard();
#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      return;
    }

    submit();
    while (numInFlightRequests_ > 0 || numInFlightBuffers_ > 0) {
      drainOneCqe();
    }
#endif
  }

  // ___________________________________________________________________________
  // Drain at least `minCompletions` from the completion queue.
  void drainCompletions(size_t minCompletions = 1) {
    auto guard = makeInvariantGuard();
#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      return;
    }

    size_t reaped = 0;
    while (numInFlightRequests_ > 0 && reaped < minCompletions) {
      drainOneCqe();
      ++reaped;
    }
#else
    (void)minCompletions;
#endif
  }

  // ___________________________________________________________________________
  // Accessors
  [[nodiscard]] const ZeroCopyBufferPool& bufferPool() const noexcept {
    return bufferPool_;
  }
  [[nodiscard]] ZeroCopyBufferPool& bufferPool() noexcept { return bufferPool_; }

  [[nodiscard]] size_t inFlightRequests() const noexcept {
    return numInFlightRequests_;
  }
  [[nodiscard]] size_t inFlightBuffers() const noexcept {
    return numInFlightBuffers_;
  }
  [[nodiscard]] size_t totalBytesSent() const noexcept {
    return totalBytesSent_;
  }
  [[nodiscard]] size_t totalPacketsSent() const noexcept {
    return totalPacketsSent_;
  }
  [[nodiscard]] bool isBuffersRegistered() const noexcept {
#ifdef QLEVER_HAS_LIBURING
    return buffersRegistered_;
#else
    return false;
#endif
  }
  [[nodiscard]] bool isRingInitialized() const noexcept {
#ifdef QLEVER_HAS_LIBURING
    return ringInitialized_;
#else
    return false;
#endif
  }

 private:
  void initRing() {
#ifdef QLEVER_HAS_LIBURING
    int ret = io_uring_queue_init(
        static_cast<unsigned int>(config_.ringEntries), &ring_,
        config_.additionalFlags);
    if (ret < 0) {
      ringInitialized_ = false;
      AD_LOG_WARN << "io_uring_queue_init failed (errno: " << -ret
                  << "), falling back to synchronous send()\n";
      return;
    }
    ringInitialized_ = true;

    if (config_.useRegisteredBuffers) {
      const auto iov = bufferPool_.iovecs();
      int regRet = io_uring_register_buffers(
          &ring_, iov.data(), static_cast<unsigned int>(iov.size()));
      if (regRet < 0) {
        AD_LOG_WARN << "io_uring_register_buffers failed (errno: " << -regRet
                    << "), falling back to unregistered zero-copy send\n";
        buffersRegistered_ = false;
      } else {
        buffersRegistered_ = true;
      }
    }
#endif
  }

  void teardown() noexcept {
#ifdef QLEVER_HAS_LIBURING
    if (ringInitialized_) {
      while (numInFlightRequests_ > 0 || numInFlightBuffers_ > 0) {
        io_uring_cqe* cqe = nullptr;
        if (io_uring_wait_cqe(&ring_, &cqe) < 0) {
          break;
        }
        io_uring_cqe_seen(&ring_, cqe);
        if (numInFlightRequests_ > 0) {
          --numInFlightRequests_;
        }
      }

      if (buffersRegistered_) {
        io_uring_unregister_buffers(&ring_);
        buffersRegistered_ = false;
      }

      io_uring_queue_exit(&ring_);
      ringInitialized_ = false;
    }
#endif
  }

#ifdef QLEVER_HAS_LIBURING
  // ___________________________________________________________________________
  // Drain a single CQE and handle zero-copy dual notification lifecycle.
  void drainOneCqe() {
    io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(&ring_, &cqe);
    if (ret < 0) {
      AD_THROW(absl::StrCat("io_uring_wait_cqe failed (errno: ", -ret, ")"));
    }

    const int res = cqe->res;
    const unsigned int flags = cqe->flags;
    const uint64_t reqId = io_uring_cqe_get_data64(cqe);
    io_uring_cqe_seen(&ring_, cqe);

    const size_t tableIdx = reqId % inFlightTable_.size();
    auto& entry = inFlightTable_[tableIdx];
    AD_CORRECTNESS_CHECK(entry.active);

    if (entry.waitingForNotification) {
      // CQE 2: Kernel buffer release notification (IORING_CQE_F_NOTIF).
      // Buffer can now be safely recycled for subsequent writes.
      bufferPool_.releaseSlot(entry.bufferIndex);
      AD_CORRECTNESS_CHECK(numInFlightBuffers_ > 0);
      AD_CORRECTNESS_CHECK(numInFlightRequests_ > 0);
      --numInFlightBuffers_;
      --numInFlightRequests_;
      entry.active = false;
      entry.waitingForNotification = false;
      return;
    }

    // CQE 1: Transmission completion result.
    if (res < 0) {
      // Handle the send error.
      bufferPool_.releaseSlot(entry.bufferIndex);
      AD_CORRECTNESS_CHECK(numInFlightBuffers_ > 0);
      AD_CORRECTNESS_CHECK(numInFlightRequests_ > 0);
      --numInFlightBuffers_;
      --numInFlightRequests_;
      entry.active = false;
      AD_THROW(absl::StrCat("io_uring send error (res: ", res,
                            ", errno: ", -res, ")"));
    }

    totalBytesSent_ += static_cast<size_t>(res);
    ++totalPacketsSent_;

    if (flags & IORING_CQE_F_MORE) {
      // Kernel is holding the buffer for zero-copy DMA; wait for CQE 2 (NOTIF)
      entry.waitingForNotification = true;
    } else {
      // Standard completion or synchronous copy; release buffer immediately
      bufferPool_.releaseSlot(entry.bufferIndex);
      AD_CORRECTNESS_CHECK(numInFlightBuffers_ > 0);
      AD_CORRECTNESS_CHECK(numInFlightRequests_ > 0);
      --numInFlightBuffers_;
      --numInFlightRequests_;
      entry.active = false;
    }
  }
#endif

  // Synchronous send fallback.
  void sendChunkSync(int sockfd, uint32_t bufferIndex, size_t numBytes,
                     int flags) {
    const auto slotSpan = bufferPool_.getSlotSpan(bufferIndex);
    ssize_t bytesSent =
        ::send(sockfd, slotSpan.data(), numBytes, flags | MSG_NOSIGNAL);
    if (bytesSent < 0) {
      bufferPool_.releaseSlot(bufferIndex);
      AD_THROW(absl::StrCat("send failed (errno: ", strerror(errno), ")"));
    }

    totalBytesSent_ += static_cast<size_t>(bytesSent);
    ++totalPacketsSent_;
    bufferPool_.releaseSlot(bufferIndex);
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ZEROCOPYSOCKETSENDER_H
