// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/IoUringManager.h"

#include <absl/strings/str_cat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstring>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "util/Exception.h"
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

// `preadv2` with `RWF_NOWAIT` exists on Linux only. Emscripten's headers
// define `RWF_NOWAIT`, but its libc has no `preadv2`; there, and wherever
// `RWF_NOWAIT` is missing, the fast path is compiled out.
#if defined(__linux__) && defined(RWF_NOWAIT) && !defined(__EMSCRIPTEN__)
#define QL_PAGE_CACHE_FAST_PATH
#endif

namespace {
#ifdef QL_PAGE_CACHE_FAST_PATH
// Cleared once a `preadv2(RWF_NOWAIT)` fails with `EOPNOTSUPP`. The flag is a
// best-effort hint without associated data; the default sequential
// consistency (Sonar cpp:S8417) keeps the publication of the disabled state
// obvious.
std::atomic<bool> pageCacheFastPathSupported{true};
#endif
}  // namespace

namespace detail {
//______________________________________________________________________________
int64_t systemPageCacheRead(int fd, const ::iovec* iov, int iovcnt,
                            int64_t offset) {
#ifdef QL_PAGE_CACHE_FAST_PATH
  return preadv2(fd, iov, iovcnt, static_cast<off_t>(offset), RWF_NOWAIT);
#else
  (void)fd;
  (void)iov;
  (void)iovcnt;
  (void)offset;
  errno = EOPNOTSUPP;
  return -1;
#endif
}

//______________________________________________________________________________
PageCacheRead& pageCacheRead() {
  static PageCacheRead function = &systemPageCacheRead;
  return function;
}

//______________________________________________________________________________
void resetPageCacheFastPathSupport() {
#ifdef QL_PAGE_CACHE_FAST_PATH
  pageCacheFastPathSupported.store(true);
#endif
}
}  // namespace detail

//______________________________________________________________________________
bool pageCacheFastPathIsSupported() {
#ifdef QL_PAGE_CACHE_FAST_PATH
  return pageCacheFastPathSupported.load();
#else
  return false;
#endif
}

//______________________________________________________________________________
std::vector<size_t> readPageCacheHits(int fd, ql::span<const size_t> numBytes,
                                      ql::span<const uint64_t> offsets,
                                      ql::span<char*> buffers) {
  AD_CONTRACT_CHECK(offsets.size() == numBytes.size() &&
                    buffers.size() == numBytes.size());
  std::vector<size_t> notServed;
#ifdef QL_PAGE_CACHE_FAST_PATH
  const size_t numReads = numBytes.size();
  std::vector<iovec> iovecs;
  size_t runBegin = 0;
  while (runBegin < numReads) {
    // The run `[runBegin, runEnd)` holds exactly adjacent file ranges, at most
    // `IOV_MAX` of them (the limit of one `preadv2` call).
    size_t runEnd = runBegin + 1;
    while (runEnd < numReads &&
           runEnd - runBegin < static_cast<size_t>(IOV_MAX) &&
           offsets[runEnd - 1] + numBytes[runEnd - 1] == offsets[runEnd]) {
      ++runEnd;
    }
    if (!pageCacheFastPathSupported.load()) {
      for (size_t i = runBegin; i < numReads; ++i) {
        notServed.push_back(i);
      }
      return notServed;
    }
    iovecs.clear();
    for (size_t i = runBegin; i < runEnd; ++i) {
      iovecs.push_back(iovec{buffers[i], numBytes[i]});
    }
    // See https://man7.org/linux/man-pages/man2/preadv2.2.html: with
    // `RWF_NOWAIT`, the call fails with `EAGAIN` (or returns fewer bytes)
    // instead of waiting for the storage device when data is not cached.
    const int64_t numBytesRead = detail::pageCacheRead()(
        fd, iovecs.data(), static_cast<int>(iovecs.size()),
        static_cast<int64_t>(offsets[runBegin]));
    if (numBytesRead < 0 && errno == EOPNOTSUPP &&
        pageCacheFastPathSupported.exchange(false)) {
      AD_LOG_WARN << "preadv2 with RWF_NOWAIT is not supported for the "
                     "vocabulary files; reading them without the "
                     "page-cache fast path"
                  << std::endl;
    }
    // Reads that were read completely are served, the others (from the first
    // incomplete one on) are left to the caller.
    size_t remaining = numBytesRead < 0 ? 0 : static_cast<size_t>(numBytesRead);
    size_t i = runBegin;
    for (; i < runEnd && remaining >= numBytes[i]; ++i) {
      remaining -= numBytes[i];
    }
    for (; i < runEnd; ++i) {
      notServed.push_back(i);
    }
    runBegin = runEnd;
  }
#else
  (void)fd;
  for (size_t i = 0; i < numBytes.size(); ++i) {
    notServed.push_back(i);
  }
#endif
  return notServed;
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

namespace detail {
//______________________________________________________________________________
RingSubmit& ringSubmit() {
  static RingSubmit function = &io_uring_submit;
  return function;
}
}  // namespace detail

namespace {
// Log that `operation` failed with `error` and terminate the process. Used
// where the ring cannot be used any more while the kernel may still write into
// the target buffers of submitted reads: throwing would let the callers free
// those buffers.
[[noreturn]] void terminateBecauseRingIsUnusable(std::string_view operation,
                                                 int error) {
  const std::string message = absl::StrCat(
      operation, " failed in IoUringPolicy: ", std::strerror(error),
      ". The kernel may still write into the buffers of submitted reads, so "
      "the process cannot continue safely. Terminating");
  AD_LOG_ERROR << message << std::endl;
  std::cerr << message << std::endl;
  std::terminate();
}
}  // namespace

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
IoUringPolicy::~IoUringPolicy() {
  if (numOutstandingReadRequests_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with "
                << numOutstandingReadRequests_
                << " read request(s) still outstanding; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  // Reap the outstanding completions before tearing down the ring, so the
  // kernel is no longer writing into any target buffer once we return. We
  // deliberately do not call `trySubmit` or `reapAtLeast` here: their
  // bookkeeping (see `processCqe`) has correctness checks that throw, and a
  // destructor must not throw.
  //
  // SQEs that the kernel has not taken (after a partial `io_uring_submit`, or
  // the no-ops of `neutralizeQueuedReads`) produce no completion, so retry
  // submitting them once and wait only for the requests the kernel has
  // actually received; the rest are discarded by `io_uring_queue_exit`.
  if (io_uring_sq_ready(&ring_) > 0) {
    io_uring_submit(&ring_);
  }
  const size_t numNeverSubmitted = io_uring_sq_ready(&ring_);
  while (numOutstandingReadRequests_ > numNeverSubmitted) {
    io_uring_cqe* cqe = nullptr;
    int ret = 0;
    do {
      ret = io_uring_wait_cqe(&ring_, &cqe);
    } while (ret == -EINTR);
    if (ret < 0) {
      terminateBecauseRingIsUnusable("io_uring_wait_cqe", -ret);
    }
    io_uring_cqe_seen(&ring_, cqe);
    --numOutstandingReadRequests_;
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
  numOutstandingReadRequestsPerBatch_[handle] = numReadRequestsToPerform;

  size_t numPrepared = 0;
  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    // The ring has no free slot, so make room: submit what we have prepared so
    // far and block until enough completions have been drained. Wait for a
    // wave of completions instead of one, and reap every ready CQE, so a large
    // batch refills the ring in waves rather than one SQE per reaped CQE.
    while (numOutstandingReadRequests_ >= ringSize_) {
      trySubmit();
      const size_t numSubmitted = numSubmittedRequests();
      if (numSubmitted == 0) {
        // Every slot holds a no-op of `neutralizeQueuedReads` that the kernel
        // has not taken, so there is nothing to wait for.
        recordError(handle,
                    "io_uring_submit failed in IoUringPolicy: the ring is full "
                    "of requests that could not be submitted");
        break;
      }
      reapAtLeast(
          static_cast<unsigned>(std::min<size_t>(REAP_WAVE, numSubmitted)));
    }
    // The batch has failed (a read error was reaped above, or its reads could
    // not be submitted), so `wait()` throws anyway: don't issue its remaining
    // reads.
    if (firstErrorPerBatch_.contains(handle)) {
      break;
    }

    // Record the read's metadata (the batch it belongs to and how many bytes it
    // should read) under a unique request id before claiming an SQE, so an
    // allocation failure here leaves no claimed SQE without metadata behind.
    const uint64_t requestId = nextRequestIdToAssign_++;
    outstandingReadsByRequestId_[requestId] =
        OutstandingRead{handle, numBytesToRead};

    // Claim the next free SQE. The loop above guarantees a slot is available,
    // so `io_uring_get_sqe` must not return `nullptr` here.
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);

    // Record the read's parameters in the SQE (this only sets the SQE's fields;
    // the request is not handed to the kernel until a later `io_uring_submit`).
    // io_uring copies the request id (the SQE's `user_data`) verbatim into the
    // matching completion, so `processCqe` can recover it.
    io_uring_prep_read(sqe, fd, targetBuf,
                       static_cast<unsigned>(numBytesToRead),
                       static_cast<__u64>(fileOffset));
    io_uring_sqe_set_data64(sqe, requestId);
    numOutstandingReadRequests_++;
    ++numPrepared;
  }

  // Reads that were never prepared are done for their batch.
  if (const size_t numNotPrepared = numReadRequestsToPerform - numPrepared;
      numNotPrepared > 0) {
    auto it = numOutstandingReadRequestsPerBatch_.find(handle);
    AD_CORRECTNESS_CHECK(it != numOutstandingReadRequestsPerBatch_.end() &&
                         it->second >= numNotPrepared);
    it->second -= numNotPrepared;
    if (it->second == 0) {
      numOutstandingReadRequestsPerBatch_.erase(it);
    }
  }
  // Flush the remaining prepared SQEs to the kernel (the loop above only
  // submits when the submission queue is full, so the last group of SQEs has
  // not yet been submitted).
  trySubmit();
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  // Drain completions until this batch is gone. `processCqe` erases a batch as
  // soon as its last read completes, so a present entry always still has
  // outstanding reads. Waiting for up to `REAP_WAVE` CQEs never waits longer
  // than this batch needs: it cannot finish before its own remaining reads
  // complete, and any CQE (also of other batches) counts towards the wave.
  while (true) {
    // Submit SQEs that a partial `io_uring_submit` left in the submission
    // queue; the kernel would never complete them otherwise. If that fails,
    // `neutralizeQueuedReads` finishes the batch's queued reads.
    trySubmit();
    auto it = numOutstandingReadRequestsPerBatch_.find(handle);
    if (it == numOutstandingReadRequestsPerBatch_.end()) {
      break;
    }
    // The batch's remaining reads are not all queued (a successful submit
    // hands at least one request to the kernel, a failed one finishes the
    // queued reads), so the kernel has some request to complete.
    const size_t numSubmitted = numSubmittedRequests();
    AD_CORRECTNESS_CHECK(numSubmitted > 0);
    reapAtLeast(static_cast<unsigned>(
        std::min<size_t>({REAP_WAVE, it->second, numSubmitted})));
  }
  // An earlier wave (of this or another batch's `wait`, or of `addBatch`) may
  // have reaped the failed read, so check the recorded error last.
  if (auto it = firstErrorPerBatch_.find(handle);
      it != firstErrorPerBatch_.end()) {
    std::string errorMessage = std::move(it->second);
    firstErrorPerBatch_.erase(it);
    AD_THROW(errorMessage);
  }
}

//______________________________________________________________________________
size_t IoUringPolicy::numSubmittedRequests() const {
  return numOutstandingReadRequests_ - io_uring_sq_ready(&ring_);
}

//______________________________________________________________________________
void IoUringPolicy::trySubmit() {
  // `io_uring_submit` returns the number of submitted SQEs or `-errno`. On
  // failure, and for the SQEs a partial submit did not take, the SQEs stay in
  // the submission queue. Of the errors listed in io_uring_enter(2), only
  // `EINTR` and `EAGAIN` can occur for this ring (no setup flags, and never
  // more requests than CQ slots); the others mean the ring is broken. See
  // https://man7.org/linux/man-pages/man2/io_uring_enter.2.html
  unsigned numRetries = 0;
  while (io_uring_sq_ready(&ring_) > 0) {
    const int ret = detail::ringSubmit()(&ring_);
    if (ret >= 0) {
      return;
    }
    if (ret == -EINTR) {
      continue;
    }
    if (ret != -EAGAIN) {
      terminateBecauseRingIsUnusable("io_uring_submit", -ret);
    }
    if (numRetries == MAX_SUBMIT_RETRIES) {
      neutralizeQueuedReads(absl::StrCat(
          "io_uring_submit failed in IoUringPolicy: ", std::strerror(EAGAIN),
          " (after ", MAX_SUBMIT_RETRIES, " retries)"));
      return;
    }
    ++numRetries;
    // The kernel could not allocate memory for the requests. Completed
    // requests free some, so wait for one if there is any; otherwise give the
    // system a moment.
    if (numSubmittedRequests() > 0) {
      reapAtLeast(1);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
}

//______________________________________________________________________________
void IoUringPolicy::neutralizeQueuedReads(const std::string& errorMessage) {
  // Without `IORING_SETUP_SQPOLL`, the kernel reads SQEs only inside our own
  // `io_uring_enter` calls, so the SQEs it has not taken can safely be
  // rewritten between two calls.
  AD_CORRECTNESS_CHECK((ring_.flags & IORING_SETUP_SQPOLL) == 0);
  // The queued SQEs are the `io_uring_sq_ready` slots from the kernel's head
  // on. liburing maps the slots to SQEs one to one (`array[i] == i`); the
  // indirection is used where the array exists anyway.
  const unsigned head = io_uring_smp_load_acquire(ring_.sq.khead);
  const unsigned numQueued = io_uring_sq_ready(&ring_);
  for (unsigned i = 0; i < numQueued; ++i) {
    const unsigned slot = (head + i) & ring_.sq.ring_mask;
    io_uring_sqe* sqe =
        &ring_.sq.sqes[ring_.sq.array != nullptr ? ring_.sq.array[slot] : slot];
    if (const uint64_t requestId = sqe->user_data;
        requestId != NO_OP_REQUEST_ID) {
      recordError(finishRead(requestId).batchHandle, errorMessage);
    }
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data64(sqe, NO_OP_REQUEST_ID);
  }
}

//______________________________________________________________________________
void IoUringPolicy::reapAtLeast(unsigned minComplete) {
  AD_CORRECTNESS_CHECK(minComplete > 0);
  AD_CORRECTNESS_CHECK(minComplete <= numSubmittedRequests());

  // Block until at least `minComplete` completion queue entries (CQEs) are
  // ready. Unless a signal interrupts it, this costs one `io_uring_enter` for
  // the whole wave.
  io_uring_cqe* cqe = nullptr;
  int ret = 0;
  do {
    ret = io_uring_wait_cqes(&ring_, &cqe, minComplete, nullptr, nullptr);
  } while (ret == -EINTR);
  if (ret < 0) {
    terminateBecauseRingIsUnusable("io_uring_wait_cqes", -ret);
  }

  // Reap every ready CQE in chunks. `io_uring_peek_batch_cqe` does not block;
  // `io_uring_cq_advance` releases a whole chunk with one CQ-head update
  // instead of one `io_uring_cqe_seen` per CQE. The results are copied and the
  // chunk is released before they are processed, so a throwing `processCqe`
  // cannot leave a reaped CQE in the queue to be processed twice.
  std::array<io_uring_cqe*, 64> cqes{};
  std::array<std::pair<int, uint64_t>, 64> results{};
  while (true) {
    const unsigned n = io_uring_peek_batch_cqe(
        &ring_, cqes.data(), static_cast<unsigned>(cqes.size()));
    if (n == 0) {
      break;
    }
    // Recover the id via the 64-bit `user_data` field, see `addBatch`.
    ::ranges::transform(::ranges::views::take(cqes, n), results.begin(),
                        [](const io_uring_cqe* cqe) {
                          return std::pair{cqe->res,
                                           io_uring_cqe_get_data64(cqe)};
                        });
    io_uring_cq_advance(&ring_, n);
    for (const auto& [numBytesRead, requestId] :
         ::ranges::views::take(results, n)) {
      processCqe(numBytesRead, requestId);
    }
  }
}

//______________________________________________________________________________
IoUringPolicy::OutstandingRead IoUringPolicy::finishRead(uint64_t requestId) {
  // Every read id we inserted in `addBatch` is finished exactly once, so the
  // entry must be present.
  auto reqIt = outstandingReadsByRequestId_.find(requestId);
  AD_CORRECTNESS_CHECK(reqIt != outstandingReadsByRequestId_.end());
  const OutstandingRead outstandingRead = reqIt->second;
  outstandingReadsByRequestId_.erase(reqIt);

  // Decrement the batch's outstanding count, erasing the batch once its last
  // read is done. This happens also for a failed read, so a batch whose read
  // failed does not stay outstanding.
  auto it =
      numOutstandingReadRequestsPerBatch_.find(outstandingRead.batchHandle);
  AD_CORRECTNESS_CHECK(it != numOutstandingReadRequestsPerBatch_.end());
  if (--it->second == 0) {
    numOutstandingReadRequestsPerBatch_.erase(it);
  }
  return outstandingRead;
}

//______________________________________________________________________________
void IoUringPolicy::recordError(BatchHandle handle, std::string errorMessage) {
  // Only the first error of a batch is kept.
  firstErrorPerBatch_.try_emplace(handle, std::move(errorMessage));
}

//______________________________________________________________________________
void IoUringPolicy::processCqe(int numBytesRead, uint64_t requestId) {
  --numOutstandingReadRequests_;
  // The completion of a no-op of `neutralizeQueuedReads`: its read was
  // already finished there.
  if (requestId == NO_OP_REQUEST_ID) {
    return;
  }
  const OutstandingRead outstandingRead = finishRead(requestId);

  // `numBytesRead` (the completion's `res`) < 0 is `-errno`. A result smaller
  // than requested (a partial read, or 0 at end of file) means we read fewer
  // bytes than expected, which we treat as an error.
  if (numBytesRead < 0) {
    recordError(outstandingRead.batchHandle,
                absl::StrCat("I/O error in IoUringPolicy read operation: ",
                             std::strerror(-numBytesRead)));
  } else if (static_cast<size_t>(numBytesRead) !=
             outstandingRead.expectedNumBytes) {
    recordError(
        outstandingRead.batchHandle,
        absl::StrCat("read fewer bytes than requested in IoUringPolicy (",
                     numBytesRead, " of ", outstandingRead.expectedNumBytes,
                     ")"));
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
