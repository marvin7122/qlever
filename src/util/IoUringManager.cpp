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
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

//______________________________________________________________________________
FixedFileSlots::FixedFileSlots(InstallFunction install, DupFunction dupFd,
                               CloseFunction closeFd)
    : install_{std::move(install)},
      dup_{std::move(dupFd)},
      close_{std::move(closeFd)} {}

//______________________________________________________________________________
FixedFileSlots::~FixedFileSlots() { releaseAll(); }

//______________________________________________________________________________
int FixedFileSlots::defaultDup(int fd) { return dup(fd); }

//______________________________________________________________________________
void FixedFileSlots::defaultClose(int fd) { close(fd); }

//______________________________________________________________________________
unsigned FixedFileSlots::slotFor(int fd) {
  const auto slotIndex = [this](auto it) {
    return static_cast<unsigned>(ql::ranges::distance(slots_.begin(), it));
  };
  if (const auto known = ql::ranges::find(slots_, fd, &Slot::ownerFd);
      known != slots_.end()) {
    return slotIndex(known);
  }
  const auto freeSlot = ql::ranges::find(slots_, -1, &Slot::ownerFd);
  if (freeSlot == slots_.end()) {
    AD_THROW(
        "IoUringPolicy supports at most two vocabulary files as fixed files; "
        "rejecting descriptor " +
        std::to_string(fd) +
        " instead of reading it without fixed-file registration");
  }
  const int duped = dup_(fd);
  if (duped < 0) {
    AD_THROW(
        "dup failed in IoUringManager while registering fixed file for "
        "descriptor " +
        std::to_string(fd) + " (" + std::strerror(errno) + ")");
  }
  const unsigned slot = slotIndex(freeSlot);
  const int installRet = install_(slot, duped);
  if (installRet < 0) {
    close_(duped);
    AD_THROW(
        "io_uring_register_files_update failed in IoUringManager for "
        "slot " +
        std::to_string(slot) + ", descriptor " + std::to_string(fd) +
        " (error " + std::to_string(-installRet) + ")");
  }
  freeSlot->ownerFd = fd;
  freeSlot->registeredFd = duped;
  return slot;
}

//______________________________________________________________________________
void FixedFileSlots::releaseAll() noexcept {
  for (Slot& slot : slots_) {
    if (slot.registeredFd >= 0) {
      close_(slot.registeredFd);
    }
    slot = Slot{};
  }
}

//______________________________________________________________________________
size_t FixedFileSlots::numUsedSlots() const {
  return static_cast<size_t>(ql::ranges::count_if(
      slots_, [](const Slot& slot) { return slot.ownerFd >= 0; }));
}

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
  const int ret = io_uring_queue_init(ringSize_, &ring_, /*flags=*/0);
  if (ret < 0) {
    AD_THROW("io_uring_queue_init failed in IoUringManager for ring size " +
             std::to_string(ringSize_) + " (error " + std::to_string(-ret) +
             ")");
  }
  // Register an empty fixed-file table now, so kernels without
  // `IORING_REGISTER_FILES` fail fast here (and `makeBatchManager` falls back
  // to synchronous reads) instead of failing the first `addBatch`. The table
  // stays registered for the ring's lifetime; real descriptors fill its free
  // slots lazily via `IORING_REGISTER_FILES_UPDATE`.
  std::array<int, IoUringPolicy::NUM_FIXED_FILES> noFiles;
  noFiles.fill(-1);
  const int registerRet = io_uring_register_files(
      &ring_, noFiles.data(), static_cast<unsigned>(noFiles.size()));
  if (registerRet < 0) {
    // The destructor does not run when the constructor throws, so release the
    // queues here; otherwise the failed construction leaks them.
    io_uring_queue_exit(&ring_);
    AD_THROW("io_uring_register_files failed in IoUringManager for " +
             std::to_string(noFiles.size()) + " slots (error " +
             std::to_string(-registerRet) + "); fixed files are required");
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
  // Reap the outstanding completions before tearing down the ring, so the
  // kernel is no longer writing into any target buffer once we return. Do not
  // call `drainAtLeast` here: it throws on I/O errors, and a destructor must
  // not throw. Stop if `io_uring_wait_cqe` fails, to avoid spinning forever
  // (it would not decrement the in-flight count).
  while (numInFlightReadRequests_ > 0) {
    io_uring_cqe* cqe = nullptr;
    if (io_uring_wait_cqe(&ring_, &cqe) < 0) {
      break;
    }
    io_uring_cqe_seen(&ring_, cqe);
    --numInFlightReadRequests_;
  }
  // Drop the fixed-file table before tearing down the ring, then close the
  // `dup`ed descriptors. The caller's own descriptors were never closed here.
  io_uring_unregister_files(&ring_);
  fixedFileSlots_.releaseAll();
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
  // Resolve the fixed-file slot once per batch: every read in the batch
  // addresses the same file, so they all share the slot. Resolve before
  // inserting the batch bookkeeping below: `slotFor` throws when the
  // descriptor cannot be registered, and a premature entry would leave a batch
  // with no submitted reads behind that `wait()` could never drain.
  const unsigned fileIndex = fixedFileSlots_.slotFor(fd);
  numInFlightReadRequestsPerBatch_[handle] = numReadRequestsToPerform;

  auto prepareOne = [&](size_t i) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);
    io_uring_prep_read(sqe, static_cast<int>(fileIndex),
                       targetBufferPerRequest[i],
                       static_cast<unsigned>(numBytesToReadPerRequest[i]),
                       static_cast<__u64>(fileOffsetPerRequest[i]));
    sqe->flags |= IOSQE_FIXED_FILE;
    const uint64_t requestId = nextRequestIdToAssign_++;
    inFlightReadsByRequestId_[requestId] =
        InFlightRead{handle, numBytesToReadPerRequest[i]};
    // Store the id in the pointer-sized `user_data` field, which every
    // liburing version provides. The 64-bit `io_uring_sqe_set_data64` helper
    // requires a very recent liburing that older images (e.g. the gcc11 CI
    // image with its distro liburing) do not have yet.
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(requestId));
    ++numInFlightReadRequests_;
  };

  size_t next = 0;
  while (next < numReadRequestsToPerform) {
    const size_t freeSlots = ringSize_ - numInFlightReadRequests_;
    if (freeSlots == 0) {
      const unsigned want = static_cast<unsigned>(
          std::min<size_t>(REAP_WAVE, numInFlightReadRequests_));
      drainAtLeast(want);
      continue;
    }
    // Submit as many SQEs as the ring has room for. `io_uring_submit` is
    // non-blocking and costs one `io_uring_enter` regardless of how many SQEs
    // it flushes, so capping the wave only adds syscalls without bounding any
    // resource: the ring itself is the bound.
    const size_t wave = std::min(numReadRequestsToPerform - next, freeSlots);
    for (size_t k = 0; k < wave; ++k) {
      prepareOne(next + k);
    }
    next += wave;
    // `io_uring_submit` may return fewer SQEs than prepared. Those leftovers
    // stay in the SQ; submit them before preparing the next wave. A zero
    // return is treated as failure: draining would deadlock if nothing has
    // reached the kernel yet.
    size_t stillToSubmit = wave;
    while (stillToSubmit > 0) {
      const int submitted = io_uring_submit(&ring_);
      if (submitted <= 0) {
        // The trailing `stillToSubmit` SQEs of this wave were prepared (their
        // ids are registered and counted as in flight) but never reached the
        // kernel, so their completions will never arrive. Roll their
        // bookkeeping back before throwing, otherwise `wait()` and the
        // destructor would block forever on phantom completions. NOTE: the
        // unsubmitted SQEs stay queued in the submission queue, so the policy
        // must not be reused after this throw (a later submit would flush
        // them and their completions would hit unknown request ids).
        rollbackUnsubmittedRequests(handle, stillToSubmit);
        AD_THROW("io_uring_submit failed in IoUringPolicy");
      }
      AD_CORRECTNESS_CHECK(static_cast<size_t>(submitted) <= stillToSubmit);
      stillToSubmit -= static_cast<size_t>(submitted);
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::rollbackUnsubmittedRequests(BatchHandle handle,
                                                size_t numRequests) {
  // `prepareOne` mints request ids consecutively and `io_uring_submit`
  // submits SQEs in FIFO order, so the unsubmitted tail of the wave holds
  // exactly the trailing `numRequests` ids below `nextRequestIdToAssign_`.
  for (size_t k = 0; k < numRequests; ++k) {
    inFlightReadsByRequestId_.erase(nextRequestIdToAssign_ - 1 - k);
  }
  numInFlightReadRequests_ -= numRequests;
  auto it = numInFlightReadRequestsPerBatch_.find(handle);
  AD_CORRECTNESS_CHECK(it != numInFlightReadRequestsPerBatch_.end());
  AD_CORRECTNESS_CHECK(it->second >= numRequests);
  if (it->second == numRequests) {
    numInFlightReadRequestsPerBatch_.erase(it);
  } else {
    it->second -= numRequests;
  }
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  while (numInFlightReadRequestsPerBatch_.find(handle) !=
         numInFlightReadRequestsPerBatch_.end()) {
    // The batch still has outstanding reads, so the total in-flight count
    // (which includes this batch's reads) is nonzero as well.
    const unsigned want = static_cast<unsigned>(
        std::min<size_t>(REAP_WAVE, numInFlightReadRequests_));
    AD_CORRECTNESS_CHECK(want > 0);
    drainAtLeast(want);
  }
}

//______________________________________________________________________________
void IoUringPolicy::drainAtLeast(unsigned minComplete) {
  AD_CORRECTNESS_CHECK(minComplete > 0);
  AD_CORRECTNESS_CHECK(minComplete <= numInFlightReadRequests_);
  io_uring_cqe* cqe = nullptr;
  int ret = 0;
  do {
    ret = io_uring_wait_cqes(&ring_, &cqe, minComplete, nullptr, nullptr);
  } while (ret == -EINTR);
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
    // `io_uring_peek_batch_cqe` returns the number of ready CQEs, or a
    // negative `-errno` code on failure; a negative value must neither become
    // a huge unsigned loop bound nor be silently swallowed like "no CQEs
    // ready" (that would hide kernel/liburing failures and lose completions).
    const int n = io_uring_peek_batch_cqe(&ring_, cqes.data(), cqes.size());
    if (n < 0) {
      AD_THROW("io_uring_peek_batch_cqe failed in IoUringPolicy");
    }
    if (n == 0) {
      break;
    }
    for (int i = 0; i < n; ++i) {
      // Recover the id via the pointer-sized `user_data` field, see
      // `addBatch`.
      raw.push_back(RawCqe{cqes[i]->res, reinterpret_cast<uint64_t>(
                                             io_uring_cqe_get_data(cqes[i]))});
    }
    io_uring_cq_advance(&ring_, static_cast<unsigned>(n));
  }
  if (raw.empty()) {
    return;
  }
  // Process every CQE of the wave before throwing, so the in-flight
  // bookkeeping stays consistent. Report the first error of the wave.
  const char* firstErrorMessage = nullptr;
  for (const RawCqe& cqe : raw) {
    const char* errorMessage = processCqe(cqe.res, cqe.id);
    if (firstErrorMessage == nullptr) {
      firstErrorMessage = errorMessage;
    }
  }
  if (firstErrorMessage != nullptr) {
    AD_THROW(firstErrorMessage);
  }
}

//______________________________________________________________________________
const char* IoUringPolicy::processCqe(int numBytesRead, uint64_t requestId) {
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

  if (numBytesRead < 0) {
    return "I/O error in IoUringPolicy read operation";
  }
  if (static_cast<size_t>(numBytesRead) != inFlightRead.expectedNumBytes) {
    return "read fewer bytes than requested in IoUringPolicy";
  }
  return nullptr;
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
