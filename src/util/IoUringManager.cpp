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

#include <stdexcept>

#include "util/Exception.h"
#include "util/FiberIoScheduler.h"
#include "util/Log.h"

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
IoUringPolicy::IoUringPolicy(unsigned ringSize) : ringSize_(ringSize) {
  // Set up the submission and completion queues, shared between this process
  // and the kernel, with (at least) `ringSize_` submission slots in the
  // submission queue. liburing rounds the requested size up to a power of two,
  // so the actual ring may be larger than `ringSize`; `ringSize_` is therefore
  // a conservative (lower) bound for the "ring full" check below. See
  // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
  // details.
  int ret = io_uring_queue_init(ringSize_, &ring_, /*flags=*/0);
  if (ret < 0) {
    AD_THROW("io_uring_queue_init failed in IoUringManager");
  }
}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize,
                             const nvmePassthrough::Options& nvmeOptions)
    : ringSize_(ringSize) {
  if (!nvmeOptions.enabled) {
    int ret = io_uring_queue_init(ringSize_, &ring_, /*flags=*/0);
    if (ret < 0) {
      AD_THROW("io_uring_queue_init failed in IoUringManager");
    }
    return;
  }
  if (nvmeOptions.namespaceId == 0 ||
      nvmeOptions.logicalBlockSize == 0) {
    AD_THROW(
        "NVMe passthrough enabled with zero namespace id or block size in "
        "IoUringPolicy");
  }
  nvmeNamespaceId_ = nvmeOptions.namespaceId;
  nvmeLogicalBlockSize_ = nvmeOptions.logicalBlockSize;
#ifdef IORING_SETUP_SQE128
  if (nvmePassthrough::kUringCmdSupported) {
    // 128-byte SQEs carry the 80-byte NVMe command payload. Plain
    // `io_uring_prep_read` SQEs keep working on such a ring, so requests that
    // fall back still submit unchanged.
    struct io_uring_params params{};
    params.flags = IORING_SETUP_SQE128;
    int ret = io_uring_queue_init_params(ringSize_, &ring_, &params);
    if (ret < 0) {
      AD_THROW(
          "io_uring_queue_init_params with IORING_SETUP_SQE128 failed in "
          "IoUringPolicy");
    }
    sqe128_ = true;
    nvmePassthroughEnabled_ = true;
    return;
  }
  AD_LOG_WARN << "NVMe passthrough requested, but this build has no NVMe "
                 "`uring_cmd` support; continuing with a plain ring and the "
                 "passthrough path disabled.\n";
#else
  AD_LOG_WARN << "NVMe passthrough requested, but this liburing has no "
                 "`IORING_SETUP_SQE128`; continuing with a plain ring and the "
                 "passthrough path disabled.\n";
#endif
  int ret = io_uring_queue_init(ringSize_, &ring_, /*flags=*/0);
  if (ret < 0) {
    AD_THROW("io_uring_queue_init failed in IoUringManager");
  }
}

//______________________________________________________________________________
void IoUringPolicy::configureNvmePassthrough(uint32_t namespaceId,
                                             uint32_t logicalBlockSize) {
  if (namespaceId == 0 || logicalBlockSize == 0) {
    AD_THROW(
        "configureNvmePassthrough requires a nonzero namespace id and block "
        "size");
  }
  nvmeNamespaceId_ = namespaceId;
  nvmeLogicalBlockSize_ = logicalBlockSize;
}

//______________________________________________________________________________
void IoUringPolicy::setNvmePassthroughEnabled(bool enabled) {
  if (enabled && !sqe128_) {
    AD_LOG_WARN << "NVMe passthrough enabled on a 64-byte-SQE ring, which "
                   "has no SQE command area; every request keeps the plain "
                   "read path.\n";
  }
  nvmePassthroughEnabled_ = enabled;
}

//______________________________________________________________________________
bool IoUringPolicy::isNvmeCapable(int fd) const {
  auto it = nvmeCapableFds_.find(fd);
  if (it != nvmeCapableFds_.end()) {
    return it->second;
  }
  const bool capable = nvmePassthrough::isPassthroughCandidate(fd);
  nvmeCapableFds_[fd] = capable;
  return capable;
}

//______________________________________________________________________________
bool IoUringPolicy::tryPrepareNvmePassthrough(io_uring_sqe* sqe, int fd,
                                              uint64_t fileOffset,
                                              size_t numBytes,
                                              char* targetBuffer) {
  // Fail fast (and leave `sqe` untouched) unless every requirement holds:
  // explicitly enabled, compiled-in `uring_cmd` support, a 128-byte-SQE ring,
  // a capable (NVMe character) device, and a whole-block range.
  if (!nvmePassthroughEnabled_ || !nvmePassthrough::kUringCmdSupported ||
      !sqe128_ || !isNvmeCapable(fd)) {
    return false;
  }
  const auto params = nvmePassthrough::translateToReadParams(
      fileOffset, numBytes, nvmeNamespaceId_, nvmeLogicalBlockSize_);
  if (!params.has_value()) {
    return false;
  }
#ifdef QLEVER_HAS_NVME_URING_CMD
  nvmePassthrough::preparePassthroughRead(sqe, fd, *params, targetBuffer);
  return true;
#else
  return false;
#endif
}

//______________________________________________________________________________
IoUringPolicy::~IoUringPolicy() {
  if (numInFlightReadRequests_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with " << numInFlightReadRequests_
                << " read request(s) still in flight; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  // Reap the outstanding completions before tearing down the ring, so the
  // kernel is no longer writing into any target buffer once we return. We
  // deliberately do not call `drainOneCqe` here: it throws on I/O errors, and a
  // destructor must not throw. We also stop if `io_uring_wait_cqe` fails, to
  // avoid spinning forever (it would not decrement the in-flight count).
  while (numInFlightReadRequests_ > 0) {
    io_uring_cqe* cqe = nullptr;
    if (io_uring_wait_cqe(&ring_, &cqe) < 0) {
      break;
    }
    io_uring_cqe_seen(&ring_, cqe);
    --numInFlightReadRequests_;
  }
  io_uring_queue_exit(&ring_);
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

  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    // The ring has no free slot, so make room: submit what we have prepared so
    // far and block until enough completions have been drained.
    if (isRingFull()) {
      // Flush the SQEs prepared so far to the kernel so the kernel can start
      // servicing them. Their completions will free up submission slots.
      io_uring_submit(&ring_);
      drainUntilSlotFree();
    }

    // Claim the next free SQE. The check above guarantees a slot is available,
    // so `io_uring_get_sqe` must not return `nullptr` here.
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);

    // Record the read's parameters in the SQE (this only sets the SQE's fields;
    // the request is not handed to the kernel until a later `io_uring_submit`).
    // The passthrough path submits a native NVMe read through `uring_cmd`
    // when the device supports it; otherwise (or when disabled) the plain
    // block-layer read below runs, so the bytes read are identical. Both
    // share the request-id tagging that follows; `attributeCompletion`
    // interprets their completions differently (byte count vs. command
    // status), so the submission kind is recorded per request below.
    const bool isNvmePassthrough = tryPrepareNvmePassthrough(
        sqe, fd, fileOffset, numBytesToRead, targetBuf);
    if (!isNvmePassthrough) {
      io_uring_prep_read(sqe, fd, targetBuf,
                         static_cast<unsigned>(numBytesToRead),
                         static_cast<__u64>(fileOffset));
    }

    // Tag the SQE with a unique request id and record its metadata (the batch
    // it belongs to, how many bytes it should read, and whether it is a
    // passthrough read). io_uring copies the request id (the SQE's
    // `user_data`) verbatim into the matching completion, so `drainOneCqe`
    // can recover it.
    const uint64_t requestId = nextRequestIdToAssign_++;
    inFlightReadsByRequestId_[requestId] =
        InFlightRead{handle, numBytesToRead, isNvmePassthrough};
    io_uring_sqe_set_data64(sqe, requestId);
    numInFlightReadRequests_++;
  }
  // Flush the remaining prepared SQEs to the kernel (the loop above only
  // submits when the submission queue is full, so the last group of SQEs has
  // not yet been submitted).
  io_uring_submit(&ring_);
}

//______________________________________________________________________________
bool IoUringPolicy::isBatchComplete(BatchHandle handle) const {
  // `drainOneCqe`/`attributeCompletion` erases a batch as soon as its last
  // read completes, so a present entry always still has outstanding reads.
  return numInFlightReadRequestsPerBatch_.find(handle) ==
         numInFlightReadRequestsPerBatch_.end();
}

//______________________________________________________________________________
void IoUringPolicy::drainUntilSlotFree() {
#ifdef QLEVER_HAS_FIBER_IO
  if (FiberIoScheduler::isInsideFiber()) {
    FiberIoScheduler::local().waitForFreeSlot(*this);
    return;
  }
#endif
  while (isRingFull()) {
    drainOneCqe();
  }
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
#ifdef QLEVER_HAS_FIBER_IO
  // Inside a scheduler fiber, cooperate (reap and yield) instead of parking
  // the thread. Outside fibers, keep the blocking behavior, so existing
  // callers such as `VocabularyOnDisk::lookupBatch` are unaffected.
  if (FiberIoScheduler::isInsideFiber()) {
    FiberIoScheduler::local().waitForBatch(*this, handle);
    return;
  }
#endif
  // Drain completions until this batch is gone.
  while (!isBatchComplete(handle)) {
    drainOneCqe();
  }
}

//______________________________________________________________________________
bool IoUringPolicy::tryReapOneCqe() {
  // Peek at the completion queue without blocking. Returns 0 with `cqe` set
  // when a completion is available, `-EAGAIN` when the queue is empty.
  io_uring_cqe* cqe = nullptr;
  int ret = io_uring_peek_cqe(&ring_, &cqe);
  if (ret == -EAGAIN) {
    return false;
  }
  if (ret < 0) {
    AD_THROW("io_uring_peek_cqe failed in IoUringPolicy");
  }
  AD_CORRECTNESS_CHECK(cqe != nullptr);
  attributeCompletion(cqe);
  return true;
}

//______________________________________________________________________________
size_t IoUringPolicy::reapAvailableCompletions() {
  size_t numReaped = 0;
  while (tryReapOneCqe()) {
    ++numReaped;
  }
  return numReaped;
}

//______________________________________________________________________________
void ad_utility::IoUringPolicy::drainOneCqe() {
  // Block until at least one completion queue entry (CQE) is available.
  io_uring_cqe* cqe = nullptr;
  int ret = io_uring_wait_cqe(&ring_, &cqe);
  if (ret < 0) {
    AD_THROW("io_uring_wait_cqe failed in IoUringPolicy");
  }
  attributeCompletion(cqe);
}

//______________________________________________________________________________
void ad_utility::IoUringPolicy::attributeCompletion(io_uring_cqe* cqe) {
  AD_CORRECTNESS_CHECK(cqe != nullptr);
  // Recover the read's result (`cqe->res`) and the request id we stored in the
  // SQE, then consume the CQE so its slot is freed. Do this before any throw.
  const int numBytesRead = cqe->res;
  const uint64_t requestId = io_uring_cqe_get_data64(cqe);
  io_uring_cqe_seen(&ring_, cqe);
  numInFlightReadRequests_--;

  // Every reaped CQE corresponds to exactly one in-flight read whose id we
  // inserted in `addBatch`, so the entry must be present.
  auto reqIt = inFlightReadsByRequestId_.find(requestId);
  AD_CORRECTNESS_CHECK(reqIt != inFlightReadsByRequestId_.end());
  const InFlightRead inFlightRead = reqIt->second;
  inFlightReadsByRequestId_.erase(reqIt);

  // `cqe->res` < 0 is `-errno`.
  if (numBytesRead < 0) {
    AD_THROW("I/O error in IoUringPolicy read operation");
  }
  if (inFlightRead.isNvmePassthrough) {
    // An `IORING_OP_URING_CMD` completion carries the driver-defined command
    // result (0 on success), not a byte count, so a successful passthrough
    // read must not be compared against `expectedNumBytes`. Any nonzero
    // result means the NVMe command itself reported failure.
    if (numBytesRead != 0) {
      AD_THROW("NVMe passthrough read failed in IoUringPolicy");
    }
  } else if (static_cast<size_t>(numBytesRead) !=
             inFlightRead.expectedNumBytes) {
    // A result smaller than requested (a partial read, or 0 at end of file)
    // means we read fewer bytes than expected, which we treat as an error.
    AD_THROW("read fewer bytes than requested in IoUringPolicy");
  }

  // Attribute the completion to its batch and decrement that batch's in-flight
  // count, erasing the batch once its last read completes. The entry must still
  // be present here: the read we are processing belongs to this batch and was
  // outstanding, so the batch's count was at least one and it had not yet been
  // erased.
  auto it = numInFlightReadRequestsPerBatch_.find(inFlightRead.batchHandle);
  AD_CORRECTNESS_CHECK(it != numInFlightReadRequestsPerBatch_.end());
  if (--it->second == 0) {
    numInFlightReadRequestsPerBatch_.erase(it);
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
