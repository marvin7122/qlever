// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/IoUringManager.h"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <vector>


#include "util/Exception.h"
#include "util/Log.h"
#include <cstdlib>

namespace ad_utility {

//______________________________________________________________________________
void SyncIoPolicy::readFullyOrThrow(int fd, char* targetBuffer, size_t numBytes,
                                    uint64_t fileOffset) {
  // `pread` reads up to `numBytes` bytes from file descriptor `fd` at offset
  // `fileOffset` (from the start of the file) into `targetBuffer`. The file
  // offset is not changed. On success, it returns the number of bytes read (0
  // indicates end of file); on error it returns -1 and sets `errno`. See
  // https://man7.org/linux/man-pages/man2/pread.2.html for more details.
  const ssize_t numBytesRead =
      pread(fd, targetBuffer, numBytes, static_cast<off_t>(fileOffset));

  if (numBytesRead < 0) {
    AD_THROW("pread failed in readFullyOrThrow");
  }
  // A result smaller than requested (a partial read, or 0 at end of file) means
  // we read fewer bytes than expected, which we treat as an error.
  if (static_cast<size_t>(numBytesRead) != numBytes) {
    AD_THROW("read fewer bytes than requested in readFullyOrThrow");
  }
}

//______________________________________________________________________________
void SyncIoPolicy::addBatch(int fd,
                            ql::span<const size_t> numBytesToReadPerRequest,
                            ql::span<const uint64_t> fileOffsetPerRequest,
                            ql::span<char*> targetBufferPerRequest,
                            [[maybe_unused]] BatchHandle handle) const {
  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    SyncIoPolicy::readFullyOrThrow(fd, targetBuf, numBytesToRead, fileOffset);
  }
}

#ifdef QLEVER_HAS_IO_URING

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize, size_t registeredBufferSize)
    : ringSize_(ringSize), registeredBufferSize_(registeredBufferSize) {
  int ret = io_uring_queue_init(ringSize_, &ring_, /*flags=*/0);
  if (ret < 0) {
    AD_THROW("io_uring_queue_init failed in IoUringManager");
  }

  registeredBufferPoolSize_ = ringSize_ * registeredBufferSize_;
  if (posix_memalign(reinterpret_cast<void**>(&registeredBufferPool_), 4096,
                     registeredBufferPoolSize_) != 0) {
    registeredBufferPool_ = nullptr;
    registeredBufferPoolSize_ = 0;
  } else {
    registeredIovecs_.resize(ringSize_);
    freeBufferIndices_.reserve(ringSize_);
    for (size_t i = 0; i < ringSize_; ++i) {
      registeredIovecs_[i].iov_base =
          registeredBufferPool_ + i * registeredBufferSize_;
      registeredIovecs_[i].iov_len = registeredBufferSize_;
      freeBufferIndices_.push_back(ringSize_ - 1 - i);
    }
    ret = io_uring_register_buffers(&ring_, registeredIovecs_.data(),
                                    registeredIovecs_.size());
    if (ret < 0) {
      AD_LOG_WARN << "io_uring_register_buffers failed in IoUringPolicy: "
                     "failed to register "
                  << registeredIovecs_.size() * registeredBufferSize_
                  << " bytes of buffers (RLIMIT_MEMLOCK is likely too low), "
                     "falling back to unregistered reads.\n";
      std::free(registeredBufferPool_);
      registeredBufferPool_ = nullptr;
      registeredBufferPoolSize_ = 0;
      registeredIovecs_ = {};
      freeBufferIndices_ = {};
    }
  }
}

//______________________________________________________________________________
IoUringPolicy::~IoUringPolicy() {
  if (numInFlightReadRequests_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with " << numInFlightReadRequests_
                << " read request(s) still in flight; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  while (numInFlightReadRequests_ > 0) {
    io_uring_cqe* cqe = nullptr;
    if (io_uring_wait_cqe(&ring_, &cqe) < 0) {
      break;
    }
    const int res = cqe->res;
    const uint64_t requestId = io_uring_cqe_get_data64(cqe);
    io_uring_cqe_seen(&ring_, cqe);

    auto reqIt = inFlightReadsByRequestId_.find(requestId);
    if (reqIt != inFlightReadsByRequestId_.end()) {
      const InFlightRead inFlightRead = reqIt->second;
      inFlightReadsByRequestId_.erase(reqIt);

      auto batchIt = numInFlightReadRequestsPerBatch_.find(inFlightRead.batchHandle);
      if (batchIt != numInFlightReadRequestsPerBatch_.end()) {
        if (--batchIt->second == 0) {
          numInFlightReadRequestsPerBatch_.erase(batchIt);
        }
      }

      if (inFlightRead.poolBufferIndex != NO_POOL_BUFFER) {
        freePoolBuffer(inFlightRead.poolBufferIndex);
      }
    }

    AD_CORRECTNESS_CHECK(numInFlightReadRequests_ > 0);
    --numInFlightReadRequests_;
  }
  if (registeredBufferPool_ != nullptr) {
    io_uring_unregister_buffers(&ring_);
    std::free(registeredBufferPool_);
    registeredBufferPool_ = nullptr;
  }
  io_uring_queue_exit(&ring_);
}

//______________________________________________________________________________
// Claim a free buffer index from the registered pool. Precondition: pool not empty.
size_t IoUringPolicy::allocatePoolBuffer() {
  AD_CORRECTNESS_CHECK(!freeBufferIndices_.empty());
  const size_t index = freeBufferIndices_.back();
  freeBufferIndices_.pop_back();
  return index;
}

//______________________________________________________________________________
void IoUringPolicy::freePoolBuffer(size_t index) {
  AD_CORRECTNESS_CHECK(index < ringSize_);
  freeBufferIndices_.push_back(index);
}

//______________________________________________________________________________
void IoUringPolicy::addBatch(int fd,
                             ql::span<const size_t> numBytesToReadPerRequest,
                             ql::span<const uint64_t> fileOffsetPerRequest,
                             ql::span<char*> targetBufferPerRequest,
                             BatchHandle handle) {
  const size_t numReadRequestsToPerform = numBytesToReadPerRequest.size();

  if (numReadRequestsToPerform == 0) {
    return;
  }
  numInFlightReadRequestsPerBatch_[handle] = numReadRequestsToPerform;

  auto prepareOne = [&](size_t i) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);

    const size_t numBytes = numBytesToReadPerRequest[i];
    const bool usePool = (registeredBufferPool_ != nullptr) &&
                         (numBytes <= registeredBufferSize_) &&
                         !freeBufferIndices_.empty();

    size_t poolIdx = NO_POOL_BUFFER;
    if (usePool) {
      poolIdx = allocatePoolBuffer();
      io_uring_prep_read_fixed(
          sqe, fd, registeredBufferPool_ + poolIdx * registeredBufferSize_,
          static_cast<unsigned>(numBytes),
          static_cast<__u64>(fileOffsetPerRequest[i]),
          poolIdx);
    } else {
      io_uring_prep_read(sqe, fd, targetBufferPerRequest[i],
                         static_cast<unsigned>(numBytes),
                         static_cast<__u64>(fileOffsetPerRequest[i]));
    }

    const uint64_t requestId = nextRequestIdToAssign_++;
    inFlightReadsByRequestId_[requestId] =
        InFlightRead{handle, numBytes, poolIdx, targetBufferPerRequest[i]};
    io_uring_sqe_set_data64(sqe, requestId);
    ++numInFlightReadRequests_;
  };

  size_t next = 0;
  while (next < numReadRequestsToPerform) {
    const size_t freeSlots = ringSize_ - numInFlightReadRequests_;
    if (freeSlots == 0) {
      const unsigned want = static_cast<unsigned>(
          std::min<size_t>(kReapWave, numInFlightReadRequests_));
      drainAtLeast(want);
      continue;
    }
    const size_t wave = std::min({numReadRequestsToPerform - next, freeSlots,
                                  static_cast<size_t>(kSubmitWave)});
    for (size_t k = 0; k < wave; ++k) {
      prepareOne(next + k);
    }
    next += wave;
    if (io_uring_submit(&ring_) < 0) {
      AD_THROW("io_uring_submit failed in IoUringPolicy");
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  while (numInFlightReadRequestsPerBatch_.find(handle) !=
         numInFlightReadRequestsPerBatch_.end()) {
    const unsigned want = static_cast<unsigned>(
        std::min<size_t>(kReapWave, numInFlightReadRequests_));
    AD_CORRECTNESS_CHECK(want > 0);
    drainAtLeast(want);
  }
}

//______________________________________________________________________________
void IoUringPolicy::drainAtLeast(unsigned minComplete) {
  AD_CORRECTNESS_CHECK(minComplete > 0);
  AD_CORRECTNESS_CHECK(minComplete <= numInFlightReadRequests_);
  io_uring_cqe* cqe = nullptr;
  const int ret =
      io_uring_wait_cqes(&ring_, &cqe, minComplete, nullptr, nullptr);
  if (ret < 0) {
    AD_THROW("io_uring_wait_cqes failed in IoUringPolicy");
  }
  drainAllReadyCqes();
}

//______________________________________________________________________________
void IoUringPolicy::drainAllReadyCqes() {
  struct RawCqe {
    int res;
    uint64_t id;
  };
  std::vector<RawCqe> raw;
  raw.reserve(ringSize_);
  while (true) {
    std::array<io_uring_cqe*, 64> cqes{};
    const unsigned n =
        io_uring_peek_batch_cqe(&ring_, cqes.data(), cqes.size());
    if (n == 0) {
      break;
    }
    for (unsigned i = 0; i < n; ++i) {
      raw.push_back(RawCqe{cqes[i]->res, io_uring_cqe_get_data64(cqes[i])});
    }
    io_uring_cq_advance(&ring_, n);
  }
  if (raw.empty()) {
    return;
  }
  pendingErrorMessage_ = nullptr;
  for (const RawCqe& cqe : raw) {
    processCqe(cqe.res, cqe.id);
  }
  if (pendingErrorMessage_ != nullptr) {
    AD_THROW(pendingErrorMessage_);
  }
}

//______________________________________________________________________________
void IoUringPolicy::processCqe(int numBytesRead, uint64_t requestId) {
  --numInFlightReadRequests_;

  auto reqIt = inFlightReadsByRequestId_.find(requestId);
  AD_CORRECTNESS_CHECK(reqIt != inFlightReadsByRequestId_.end());
  const InFlightRead inFlightRead = reqIt->second;
  inFlightReadsByRequestId_.erase(reqIt);

  auto it = numInFlightReadRequestsPerBatch_.find(inFlightRead.batchHandle);
  AD_CORRECTNESS_CHECK(it != numInFlightReadRequestsPerBatch_.end());
  if (--it->second == 0) {
    numInFlightReadRequestsPerBatch_.erase(it);
  }

  const bool usedPool = (inFlightRead.poolBufferIndex != NO_POOL_BUFFER);

  if (pendingErrorMessage_ != nullptr) {
    if (usedPool) {
      freePoolBuffer(inFlightRead.poolBufferIndex);
    }
    return;
  }
  if (numBytesRead < 0) {
    if (usedPool) {
      freePoolBuffer(inFlightRead.poolBufferIndex);
    }
    pendingErrorMessage_ = "I/O error in IoUringPolicy read operation";
    return;
  }
  if (static_cast<size_t>(numBytesRead) != inFlightRead.expectedNumBytes) {
    if (usedPool) {
      freePoolBuffer(inFlightRead.poolBufferIndex);
    }
    pendingErrorMessage_ = "read fewer bytes than requested in IoUringPolicy";
    return;
  }

  if (usedPool) {
    std::memcpy(inFlightRead.targetBuffer,
                registeredBufferPool_ +
                    inFlightRead.poolBufferIndex * registeredBufferSize_,
                inFlightRead.expectedNumBytes);
    freePoolBuffer(inFlightRead.poolBufferIndex);
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
