
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_EXPORT_PROTOTYPES_REGISTEREDIOURINGREADER_H
#define QLEVER_SRC_UTIL_EXPORT_PROTOTYPES_REGISTEREDIOURINGREADER_H

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

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
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "util/AlignedAllocator.h"
#include "util/Exception.h"
#include "util/HashMap.h"
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

namespace ad_utility::export_prototypes {

// Direct I/O block alignment constants for NVMe and modern Linux kernels.
inline constexpr size_t kDirectIoBlockSize = 4096;
inline constexpr size_t kDirectIoAlignment = 4096;

// _____________________________________________________________________________
// Check whether a pointer or size is 4KB page/block aligned.
[[nodiscard]] constexpr bool isBlockAligned(uint64_t val) noexcept {
  return (val % kDirectIoBlockSize) == 0;
}



// _____________________________________________________________________________
// Wrap an open file descriptor with Direct I/O (O_DIRECT) support.
class DirectIoFile {
 private:
  int fd_ = -1;
  bool isDirect_ = false;
  uint64_t fileSize_ = 0;
  std::string path_;

 public:
  DirectIoFile() = default;

  DirectIoFile(std::string_view path, bool useDirectIo, bool readOnly = true) {
    open(path, useDirectIo, readOnly);
  }

  ~DirectIoFile() { close(); }

  DirectIoFile(const DirectIoFile&) = delete;
  DirectIoFile& operator=(const DirectIoFile&) = delete;

  DirectIoFile(DirectIoFile&& other) noexcept
      : fd_{std::exchange(other.fd_, -1)},
        isDirect_{other.isDirect_},
        fileSize_{other.fileSize_},
        path_{std::move(other.path_)} {}

  DirectIoFile& operator=(DirectIoFile&& other) noexcept {
    if (this != &other) {
      close();
      fd_ = std::exchange(other.fd_, -1);
      isDirect_ = other.isDirect_;
      fileSize_ = other.fileSize_;
      path_ = std::move(other.path_);
    }
    return *this;
  }

  void open(std::string_view path, bool useDirectIo, bool readOnly = true);

  void close() noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  [[nodiscard]] int fd() const noexcept { return fd_; }
  [[nodiscard]] bool isOpen() const noexcept { return fd_ >= 0; }
  [[nodiscard]] bool isDirect() const noexcept { return isDirect_; }
  [[nodiscard]] uint64_t size() const noexcept { return fileSize_; }
  [[nodiscard]] const std::string& path() const noexcept { return path_; }
};

// _____________________________________________________________________________
// Provide an aligned PMR memory arena for DMA and io_uring fixed buffer registration.
// Guarantees 4KB page alignment for Direct I/O and zero-copy DMA pinning.
class PinnedArena : public WithInvariants<PinnedArena> {
 private:
  using RawBuffer = std::vector<char, ad_utility::AlignedAllocator<char, kDirectIoAlignment>>;
  RawBuffer rawBuffer_;
  size_t totalBytes_ = 0;
  size_t slotSize_ = 0;
  size_t numSlots_ = 0;
  std::vector<iovec> iovecs_;

 public:
  PinnedArena() = default;

  // Allocate an arena of `numSlots` slots, each of size `slotSizeBytes`.
  // `slotSizeBytes` must be a multiple of 4KB for O_DIRECT alignment.
  PinnedArena(size_t numSlots, size_t slotSizeBytes = kDirectIoBlockSize) {
    AD_CONTRACT_CHECK(numSlots > 0);
    AD_CONTRACT_CHECK(slotSizeBytes > 0);
    AD_CONTRACT_CHECK(isBlockAligned(slotSizeBytes));

    slotSize_ = slotSizeBytes;
    numSlots_ = numSlots;
    totalBytes_ = numSlots * slotSizeBytes;

    rawBuffer_.resize(totalBytes_);
    std::memset(rawBuffer_.data(), 0, totalBytes_);

    iovecs_.reserve(numSlots_);
    auto* basePtr = rawBuffer_.data();
    for (size_t i = 0; i < numSlots_; ++i) {
      iovecs_.push_back(
          iovec{.iov_base = basePtr + (i * slotSize_), .iov_len = slotSize_});
    }

    checkInvariants();
  }

  ~PinnedArena() = default;

  PinnedArena(const PinnedArena&) = delete;
  PinnedArena& operator=(const PinnedArena&) = delete;

  PinnedArena(PinnedArena&& other) noexcept
      : totalBytes_{std::exchange(other.totalBytes_, 0)},
        slotSize_{std::exchange(other.slotSize_, 0)},
        numSlots_{std::exchange(other.numSlots_, 0)},
        iovecs_{std::move(other.iovecs_)},
        rawBuffer_{std::move(other.rawBuffer_)} {}

  PinnedArena& operator=(PinnedArena&& other) noexcept {
    if (this != &other) {
      totalBytes_ = std::exchange(other.totalBytes_, 0);
      slotSize_ = std::exchange(other.slotSize_, 0);
      numSlots_ = std::exchange(other.numSlots_, 0);
      iovecs_ = std::move(other.iovecs_);
      rawBuffer_ = std::move(other.rawBuffer_);
      checkInvariants();
    }
    return *this;
  }

  // Structural Invariant Verification (Law 3 & Architecture Standard)
  void checkInvariants() const {
    if (numSlots_ > 0) {
      AD_CORRECTNESS_CHECK(!rawBuffer_.empty());
      AD_CORRECTNESS_CHECK(totalBytes_ == numSlots_ * slotSize_);
      AD_CORRECTNESS_CHECK(isPointerAligned(rawBuffer_.data()));
      AD_CORRECTNESS_CHECK(isBlockAligned(slotSize_));
      AD_CORRECTNESS_CHECK(iovecs_.size() == numSlots_);
    } else {
      AD_CORRECTNESS_CHECK(rawBuffer_.empty());
      AD_CORRECTNESS_CHECK(totalBytes_ == 0);
    }
  }

  [[nodiscard]] size_t numSlots() const noexcept { return numSlots_; }
  [[nodiscard]] size_t slotSize() const noexcept { return slotSize_; }
  [[nodiscard]] size_t totalBytes() const noexcept { return totalBytes_; }
  [[nodiscard]] char* data() noexcept { return rawBuffer_.data(); }
  [[nodiscard]] const char* data() const noexcept { return rawBuffer_.data(); }


  // Access a specific block slot as a span.
  [[nodiscard]] ql::span<char> getSlotSpan(size_t slotIndex) {
    AD_CONTRACT_CHECK(slotIndex < numSlots_);
    auto* slotPtr = rawBuffer_.data() + (slotIndex * slotSize_);
    return {slotPtr, slotSize_};
  }

  [[nodiscard]] ql::span<const char> getSlotSpan(size_t slotIndex) const {
    AD_CONTRACT_CHECK(slotIndex < numSlots_);
    const auto* slotPtr =
        rawBuffer_.data() + (slotIndex * slotSize_);
    return {slotPtr, slotSize_};
  }

  [[nodiscard]] ql::span<const iovec> iovecs() const noexcept {
    return {iovecs_.data(), iovecs_.size()};
  }
};

// _____________________________________________________________________________
// Describe a block read request with invariant proofs.
// Uses a tagged union to enforce mutual exclusion between registered-buffer
// and direct-memory read paths (Heuristic 2: pair buffers and views by construction).
struct BlockReadRequest {
  uint32_t fileIndex = 0;       // Index into registered file table
  uint64_t fileOffset = 0;      // File byte offset (4KB aligned for O_DIRECT)
  uint32_t numBytes = 0;        // Number of bytes to read (multiple of 4KB)

  struct RegisteredBuffer {
    uint32_t bufferIndex = 0;   // Registered buffer index
    // bufferOffset removed: fixed buffers read into entire iovec; no per-read offset
  };

  struct DirectMemory {
    ql::span<char> destination;  // Target memory span (4KB aligned, borrows from caller)
  };

  std::variant<RegisteredBuffer, DirectMemory> target;

  BlockReadRequest() = default;

  // Registered-buffer read constructor
  BlockReadRequest(uint32_t fIndex, uint64_t fOffset, uint32_t bufIndex,
                   uint32_t bytes)
      : fileIndex{fIndex},
        fileOffset{fOffset},
        numBytes{bytes},
        target{RegisteredBuffer{bufIndex}} {
    AD_CONTRACT_CHECK(numBytes > 0);
    AD_CONTRACT_CHECK(isBlockAligned(fileOffset));
    AD_CONTRACT_CHECK(isBlockAligned(numBytes));
  }

  // Direct-memory read constructor
  BlockReadRequest(uint32_t fIndex, uint64_t fOffset, uint32_t bytes,
                   ql::span<char> dest)
      : fileIndex{fIndex},
        fileOffset{fOffset},
        numBytes{bytes},
        target{DirectMemory{dest}} {
    AD_CONTRACT_CHECK(numBytes > 0);
    AD_CONTRACT_CHECK(!dest.empty());
    AD_CONTRACT_CHECK(isBlockAligned(fileOffset));
    AD_CONTRACT_CHECK(isBlockAligned(numBytes));
    AD_CONTRACT_CHECK(isPointerAligned(dest.data()));
  }

 private:
  [[nodiscard]] char* destination() const {
    return std::get<DirectMemory>(target).destination.data();
  }
};

// _____________________________________________________________________________
// Represent the result of a completed I/O batch.
struct BatchResult {
  size_t requestsCompleted = 0;
  size_t totalBytesRead = 0;
  bool success = true;
};

// _____________________________________________________________________________
// Configure the RegisteredIoUringReader.
struct RegisteredReaderConfig {
  unsigned ringEntries = 512;
  bool useDirectIo = true;
  bool useRegisteredFiles = true;
  bool useRegisteredBuffers = true;
  unsigned additionalFlags = 0;
};

// _____________________________________________________________________________
// Deep Module: RegisteredIoUringReader
//
// Encapsulates kernel fixed-file table registration (`IORING_REGISTER_FILES`),
// fixed-buffer DMA page-pinning (`IORING_REGISTER_BUFFERS`), Direct I/O
// alignment enforcement (`O_DIRECT`), submission queue batching, and completion
// queue reaping behind a clean, minimal-bookkeeping interface.
class RegisteredIoUringReader
    : public WithInvariants<RegisteredIoUringReader> {
 public:
  using BatchId = uint64_t;
 public:

 private:
  RegisteredReaderConfig config_;
#ifdef QLEVER_HAS_LIBURING
  io_uring ring_{};
  bool ringInitialized_ = false;
#endif
  bool filesRegistered_ = false;
  bool buffersRegistered_ = false;

  std::vector<int> registeredFds_;
  std::vector<iovec> registeredIovecs_;

  size_t numInFlightRequests_ = 0;
  BatchId nextBatchId_ = 1;

  struct InFlightMeta {
    BatchId batchId;
    size_t expectedBytes;
  };
  ad_utility::HashMap<uint64_t, InFlightMeta> inFlightByReqId_;
  
    AD_CORRECTNESS_CHECK(!config_.useRegisteredBuffers || config_.useDirectIo);
    AD_CONTRACT_CHECK(config_.ringEntries > 0);
    initRing();
  }

  ~RegisteredIoUringReader() { teardown(); }

  RegisteredIoUringReader(const RegisteredIoUringReader&) = delete;
  RegisteredIoUringReader& operator=(const RegisteredIoUringReader&) = delete;

    RegisteredIoUringReader(RegisteredIoUringReader&&) = delete;
  RegisteredIoUringReader& operator=(RegisteredIoUringReader&&) = delete;
        filesRegistered_{other.filesRegistered_},
        buffersRegistered_{other.buffersRegistered_},
        registeredFds_{std::move(other.registeredFds_)},
        registeredIovecs_{std::move(other.registeredIovecs_)},
        numInFlightRequests_{other.numInFlightRequests_},
        nextBatchId_{other.nextBatchId_},
        inFlightByReqId_{std::move(other.inFlightByReqId_)},
        inFlightByBatchId_{std::move(other.inFlightByBatchId_)},
        nextReqId_{other.nextReqId_} {
#ifdef QLEVER_HAS_LIBURING
    other.ringInitialized_ = false;
#endif
    other.filesRegistered_ = false;
    other.buffersRegistered_ = false;
    other.numInFlightRequests_ = 0;
    other.inFlightByReqId_.clear();
    other.inFlightByBatchId_.clear();
  }

  RegisteredIoUringReader& operator=(RegisteredIoUringReader&& other) noexcept {
    if (this != &other) {
      teardown();
      config_ = other.config_;
#ifdef QLEVER_HAS_LIBURING
      ring_ = other.ring_;
      ringInitialized_ = other.ringInitialized_;
      other.ringInitialized_ = false;
#endif
      filesRegistered_ = other.filesRegistered_;
      buffersRegistered_ = other.buffersRegistered_;
      registeredFds_ = std::move(other.registeredFds_);
      registeredIovecs_ = std::move(other.registeredIovecs_);
      numInFlightRequests_ = other.numInFlightRequests_;
      nextBatchId_ = other.nextBatchId_;
      inFlightByReqId_ = std::move(other.inFlightByReqId_);
      inFlightByBatchId_ = std::move(other.inFlightByBatchId_);
      nextReqId_ = other.nextReqId_;

      other.filesRegistered_ = false;
      other.buffersRegistered_ = false;
      other.numInFlightRequests_ = 0;
      other.nextBatchId_ = 1;
      other.nextReqId_ = 0;
      other.inFlightByReqId_.clear();
      other.inFlightByBatchId_.clear();
    }
    return *this;
  }

  // Invariant verification (Law 3 & Architecture Standard)
  void checkInvariants() const {
#ifdef QLEVER_HAS_LIBURING
    if (ringInitialized_) {
      AD_CORRECTNESS_CHECK(config_.ringEntries > 0);
      if (filesRegistered_) {
        AD_CORRECTNESS_CHECK(!registeredFds_.empty());
      }
      if (buffersRegistered_) {
        AD_CORRECTNESS_CHECK(!registeredIovecs_.empty());
      }
    }
#endif
  }

  // ___________________________________________________________________________
  // IORING_REGISTER_FILES: Pre-register open file descriptors into the kernel
  // io_uring file table, eliminating fget()/fput() locking overhead per I/O.
  void registerFiles(ql::span<const int> fds) {
    auto guard = makeInvariantGuard();
    AD_CONTRACT_CHECK(!fds.empty());

#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      AD_THROW("io_uring is not initialized");
    }

    if (filesRegistered_) {
      unregisterFiles();
    }

    registeredFds_.assign(fds.begin(), fds.end());
    int ret = io_uring_register_files(
        &ring_, registeredFds_.data(),
        static_cast<unsigned int>(registeredFds_.size()));
    if (ret < 0) {
      registeredFds_.clear();
      AD_THROW(absl::StrCat("io_uring_register_files failed (errno: ", -ret,
                            ")"));
    }
    filesRegistered_ = true;
#else
    registeredFds_.assign(fds.begin(), fds.end());
    filesRegistered_ = true;
#endif
  }

  void unregisterFiles() noexcept {
#ifdef QLEVER_HAS_LIBURING
    if (ringInitialized_ && filesRegistered_) {
      io_uring_unregister_files(&ring_);
      filesRegistered_ = false;
      registeredFds_.clear();
    }
#else
    filesRegistered_ = false;
    registeredFds_.clear();
#endif
  }

  // ___________________________________________________________________________
  // IORING_REGISTER_BUFFERS: Pre-register and page-pin PMR arena buffers for
  // direct zero-copy DMA, eliminating get_user_pages() and TLB shootdowns.
  void registerBuffers(ql::span<const iovec> iovecs) {
    auto guard = makeInvariantGuard();
    AD_CONTRACT_CHECK(!iovecs.empty());
    for (const auto& iov : iovecs) {
      AD_CONTRACT_CHECK(isPointerAligned(iov.iov_base));
      AD_CONTRACT_CHECK(isBlockAligned(iov.iov_len));
    }

#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      AD_THROW("io_uring is not initialized");
    }

    if (buffersRegistered_) {
      unregisterBuffers();
    }

    int ret = io_uring_register_buffers(
        &ring_, iovecs.data(),
        static_cast<unsigned int>(iovecs.size()));
    if (ret < 0) {
      AD_THROW(absl::StrCat("io_uring_register_buffers failed (errno: ", -ret,
                            ")"));
    }
    registeredIovecs_.assign(iovecs.begin(), iovecs.end());
    buffersRegistered_ = true;
#else
    registeredIovecs_.assign(iovecs.begin(), iovecs.end());
    buffersRegistered_ = true;
#endif
  }

  void unregisterBuffers() noexcept {
#ifdef QLEVER_HAS_LIBURING
    if (ringInitialized_ && buffersRegistered_) {
      io_uring_unregister_buffers(&ring_);
      buffersRegistered_ = false;
      registeredIovecs_.clear();
    }
#else
    buffersRegistered_ = false;
    registeredIovecs_.clear();
#endif
  }

  // ___________________________________________________________________________
  // Submit a batch of block read requests to the kernel.
  // Support registered files, registered fixed buffers, and Direct I/O.
  [[nodiscard]] BatchId submitBatch(ql::span<const BlockReadRequest> requests) {
    auto guard = makeInvariantGuard();
    AD_CONTRACT_CHECK(!requests.empty());

    const BatchId batchId = nextBatchId_++;

#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      // Synchronous fallback if ring is not available
      submitBatchSync(requests);
      return batchId;
    }

    inFlightByBatchId_[batchId] = requests.size();

    for (const auto& req : requests) {
      // If submission queue is saturated, flush and drain completions to free slots
      if (numInFlightRequests_ >= config_.ringEntries) {
        io_uring_submit(&ring_);
        while (numInFlightRequests_ >= config_.ringEntries) {
          drainOneCqe();
        }
      }

      io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
      AD_CORRECTNESS_CHECK(sqe != nullptr);

      int targetFd = static_cast<int>(req.fileIndex);
      if (filesRegistered_ && config_.useRegisteredFiles) {
        AD_CONTRACT_CHECK(req.fileIndex < registeredFds_.size());
      } else if (filesRegistered_) {
        AD_CONTRACT_CHECK(req.fileIndex < registeredFds_.size());
        targetFd = registeredFds_[req.fileIndex];
      }

      if (buffersRegistered_ && config_.useRegisteredBuffers) {
        AD_CORRECTNESS_CHECK(req.bufferIndex < registeredIovecs_.size());
        // Fixed buffer read with kernel page-pinning
        io_uring_prep_read_fixed(sqe, targetFd, nullptr, req.numBytes,
                                 req.fileOffset, req.bufferIndex);
      } else {
        // Standard unpinned read
        io_uring_prep_read(sqe, targetFd, req.destination, req.numBytes,
                           req.fileOffset);
      }

      if (filesRegistered_ && config_.useRegisteredFiles) {
        sqe->flags |= IOSQE_FIXED_FILE;
      }

      const uint64_t reqId = nextReqId_++;
      inFlightByReqId_[reqId] = InFlightMeta{batchId, req.numBytes};
      io_uring_sqe_set_data64(sqe, reqId);
      ++numInFlightRequests_;
    }

    io_uring_submit(&ring_);
#else
    submitBatchSync(requests);
#endif

    return batchId;
  }

  // ___________________________________________________________________________
  // Block until all reads belonging to `batchId` have completed.
  BatchResult waitBatch(BatchId batchId) {
    auto guard = makeInvariantGuard();
    if (batchId == 0) {
      return BatchResult{0, 0, true};
    }

#ifdef QLEVER_HAS_LIBURING
    if (!ringInitialized_) {
      return BatchResult{0, 0, true};
    }

    size_t completed = 0;
    size_t totalBytes = 0;

    while (inFlightByBatchId_.find(batchId) != inFlightByBatchId_.end()) {
      auto [bytesRead, bId] = drainOneCqe();
      if (bId == batchId) {
        ++completed;
        totalBytes += bytesRead;
      }
    }

    return BatchResult{completed, totalBytes, true};
#else
    return BatchResult{0, 0, true};
#endif
  }

  // ___________________________________________________________________________
  // Provide a synchronous pread fallback supporting both Direct I/O and buffered I/O.
  static void readSync(int fd, uint64_t offset, ql::span<char> dest,
                       bool directIo = true) {
    AD_CONTRACT_CHECK(fd >= 0);
    AD_CONTRACT_CHECK(!dest.empty());
    if (directIo) {
      AD_CONTRACT_CHECK(isBlockAligned(offset));
      AD_CONTRACT_CHECK(isBlockAligned(dest.size()));
      AD_CONTRACT_CHECK(isPointerAligned(dest.data()));
    }

    ssize_t bytesRead =
        ::pread(fd, dest.data(), dest.size(), static_cast<off_t>(offset));
    if (bytesRead < 0) {
      AD_THROW(absl::StrCat("pread failed (errno: ", strerror(errno), ")"));
    }
    if (static_cast<size_t>(bytesRead) != dest.size()) {
      AD_THROW("pread read fewer bytes than requested");
    }
  }

  [[nodiscard]] bool isFilesRegistered() const noexcept {
    return filesRegistered_;
  }
  [[nodiscard]] bool isBuffersRegistered() const noexcept {
    return buffersRegistered_;
  }
  [[nodiscard]] size_t inFlightCount() const noexcept {
    return numInFlightRequests_;
  }

 private:
  void initRing() {
#ifdef QLEVER_HAS_LIBURING
    int ret = io_uring_queue_init(config_.ringEntries, &ring_,
                                  config_.additionalFlags);
    if (ret < 0) {
      ringInitialized_ = false;
      AD_LOG_WARN << "io_uring_queue_init failed: errno " << -ret
                  << ", falling back to synchronous I/O\n";
    } else {
      ringInitialized_ = true;
    }
#endif
  }

  void teardown() noexcept {
#ifdef QLEVER_HAS_LIBURING
    if (ringInitialized_) {
      while (numInFlightRequests_ > 0) {
        io_uring_cqe* cqe = nullptr;
        if (io_uring_wait_cqe(&ring_, &cqe) < 0) {
          break;
        }
        io_uring_cqe_seen(&ring_, cqe);
        --numInFlightRequests_;
      }
      unregisterBuffers();
      unregisterFiles();
      io_uring_queue_exit(&ring_);
      ringInitialized_ = false;
    }
#else
    unregisterBuffers();
    unregisterFiles();
#endif
  }

#ifdef QLEVER_HAS_LIBURING
  std::pair<size_t, BatchId> drainOneCqe() {
    io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(&ring_, &cqe);
    if (ret < 0) {
      AD_THROW(absl::StrCat("io_uring_wait_cqe failed (errno: ", -ret, ")"));
    }

    const int res = cqe->res;
    const uint64_t reqId = io_uring_cqe_get_data64(cqe);
    io_uring_cqe_seen(&ring_, cqe);
    --numInFlightRequests_;

    auto it = inFlightByReqId_.find(reqId);
    AD_CORRECTNESS_CHECK(it != inFlightByReqId_.end());
    const InFlightMeta meta = it->second;
    inFlightByReqId_.erase(it);

    if (res < 0) {
      AD_THROW(absl::StrCat("io_uring CQE error (res: ", res, ", errno: ", -res,
                            ")"));
    }
    if (static_cast<size_t>(res) != meta.expectedBytes) {
      AD_THROW(absl::StrCat("io_uring short read: expected ", meta.expectedBytes,
                            " got ", res));
    }

    auto batchIt = inFlightByBatchId_.find(meta.batchId);
    AD_CORRECTNESS_CHECK(batchIt != inFlightByBatchId_.end());
    if (--batchIt->second == 0) {
      inFlightByBatchId_.erase(batchIt);
    }

    return DrainResult{static_cast<size_t>(res), meta.batchId, true};
  }
#endif

  void submitBatchSync(ql::span<const BlockReadRequest> requests) {
    for (const auto& req : requests) {
      int targetFd = static_cast<int>(req.fileIndex);
      if (filesRegistered_ && req.fileIndex < registeredFds_.size()) {
        targetFd = registeredFds_[req.fileIndex];
      }
      readSync(targetFd, req.fileOffset, {req.destination, req.numBytes},
               config_.useDirectIo);
    }
  }
};

}  // namespace ad_utility::export_prototypes

#endif  // QLEVER_SRC_UTIL_EXPORT_PROTOTYPES_REGISTEREDIOURINGREADER_H
