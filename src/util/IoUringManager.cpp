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
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "util/Exception.h"
#include "util/Log.h"

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
// `liburing.h` pulls in the kernel header `<linux/fs.h>`, which defines a macro
// `BLOCK_SIZE`, a very common identifier. QLever does not use the macro, so we
// get rid of it again.
#undef BLOCK_SIZE
#endif

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

namespace {
// `IoUringInterface` via liburing.
class LiburingRing final : public IoUringInterface {
 public:
  explicit LiburingRing(unsigned ringSize) {
    // Set up the submission and completion queues, shared between this
    // process and the kernel, with (at least) `ringSize` submission slots in
    // the submission queue. liburing rounds the requested size up to a power
    // of two, so the actual ring may be larger than `ringSize`; `ringSize` is
    // therefore a conservative (lower) bound for the "ring full" check of
    // `IoUringPolicy`. See
    // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
    // details.
    if (io_uring_queue_init(ringSize, &ring_, /*flags=*/0) < 0) {
      AD_THROW("io_uring_queue_init failed in IoUringManager");
    }
  }

  ~LiburingRing() override { io_uring_queue_exit(&ring_); }

  LiburingRing(const LiburingRing&) = delete;
  LiburingRing& operator=(const LiburingRing&) = delete;

  bool prepareRead(int fd, char* buffer, size_t numberOfBytes, uint64_t offset,
                   uint64_t requestId) override {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      return false;
    }
    // Record the read's parameters in the SQE (this only sets the SQE's
    // fields; the request is not handed to the kernel until a later
    // `io_uring_submit`). io_uring copies the SQE's `user_data` verbatim into
    // the matching completion, so the request id can be recovered from it.
    io_uring_prep_read(sqe, fd, buffer, static_cast<unsigned>(numberOfBytes),
                       static_cast<__u64>(offset));
    io_uring_sqe_set_data64(sqe, requestId);
    return true;
  }

  int submit() override { return io_uring_submit(&ring_); }

  int waitForCompletions(unsigned minComplete) override {
    io_uring_cqe* cqe = nullptr;
    return io_uring_wait_cqes(&ring_, &cqe, minComplete, nullptr, nullptr);
  }

  size_t popCompletions(ql::span<Completion> completions) override {
    // `io_uring_peek_batch_cqe` does not block; `io_uring_cq_advance`
    // releases all of them with one CQ-head update instead of one
    // `io_uring_cqe_seen` per CQE. The fields are copied out before the
    // release, after which the kernel may reuse the slots.
    std::array<io_uring_cqe*, MAX_CHUNK> cqes{};
    const unsigned numberOfReady = io_uring_peek_batch_cqe(
        &ring_, cqes.data(),
        static_cast<unsigned>(std::min(completions.size(), cqes.size())));
    for (size_t i = 0; i < numberOfReady; ++i) {
      completions[i] =
          Completion{io_uring_cqe_get_data64(cqes[i]), cqes[i]->res};
    }
    io_uring_cq_advance(&ring_, numberOfReady);
    return numberOfReady;
  }

  void backOff() override {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // The largest chunk that `popCompletions` reaps per call.
  static constexpr size_t MAX_CHUNK = 64;

 private:
  io_uring ring_{};
};
}  // namespace

//______________________________________________________________________________
std::unique_ptr<IoUringInterface> makeLiburingRing(unsigned ringSize) {
  return std::make_unique<LiburingRing>(ringSize);
}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize)
    : IoUringPolicy(ringSize, makeLiburingRing(ringSize)) {}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize,
                             std::unique_ptr<IoUringInterface> ring)
    : ring_(std::move(ring)), ringSize_(ringSize) {
  AD_CONTRACT_CHECK(ring_ != nullptr,
                    "An IoUringPolicy needs a ring to run on.");
  AD_CONTRACT_CHECK(ringSize_ > 0, "An IoUringPolicy needs a ring size > 0.");
}

//______________________________________________________________________________
// Drain the outstanding reads, so that the kernel no longer writes into any
// target buffer, then tear down the ring.
IoUringPolicy::~IoUringPolicy() {
  if (!failedBatches_.empty()) {
    AD_LOG_WARN << "IoUringPolicy destroyed with " << failedBatches_.size()
                << " failed batch(es) that were never `wait()`ed; one of the "
                   "failures is: "
                << failedBatches_.begin()->second << "\n";
  }
  if (numOutstandingReadRequests_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with "
                << numOutstandingReadRequests_
                << " read request(s) still outstanding; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  // Reap the outstanding completions before tearing down the ring, so the
  // kernel is no longer writing into any target buffer once we return. We
  // deliberately do not call `reapWave` here: it throws if `io_uring_wait_cqes`
  // fails and on its correctness checks, and a destructor must not throw.
  // (Failed reads do not throw there; `processCqe` only records them.) We also
  // stop if `io_uring_wait_cqe` fails, to avoid spinning forever (it would not
  // decrement the outstanding count).
  while (numOutstandingReadRequests_ > 0) {
    if (ring_->waitForCompletions(1) < 0) {
      break;
    }
    std::array<IoUringInterface::Completion, 1> completion{};
    numOutstandingReadRequests_ -= ring_->popCompletions(completion);
  }
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

  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    // The ring has no free slot, so make room: submit what we have prepared so
    // far and block until enough completions have been drained.
    if (numOutstandingReadRequests_ >= ringSize_) {
      // Flush the SQEs prepared so far to the kernel so the kernel can start
      // servicing them. Their completions will free up submission slots.
      // Wait for a wave of completions instead of one, and reap every ready
      // CQE, so a large batch refills the ring in waves rather than one SQE
      // per reaped CQE.
      ring_->submit();
      while (numOutstandingReadRequests_ >= ringSize_) {
        reapWave(numOutstandingReadRequests_);
      }
    }

    // Claim the next free SQE and tag it with a unique request id, and record
    // the read's metadata (the batch it belongs to and how many bytes it
    // should read) under that id, so `processCqe` can recover it. The check
    // above guarantees a slot is available.
    const uint64_t requestId = nextRequestIdToAssign_++;
    AD_CORRECTNESS_CHECK(ring_->prepareRead(fd, targetBuf, numBytesToRead,
                                            fileOffset, requestId));
    outstandingReadsByRequestId_[requestId] =
        OutstandingRead{handle, numBytesToRead};
    numOutstandingReadRequests_++;
  }
  // Flush the remaining prepared SQEs to the kernel (the loop above only
  // submits when the submission queue is full, so the last group of SQEs has
  // not yet been submitted).
  ring_->submit();
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  // Drain completions until this batch is gone. `processCqe` erases a batch as
  // soon as its last read completes, so a present entry always still has
  // outstanding reads. Waiting for up to `REAP_WAVE` CQEs never waits longer
  // than this batch needs: it cannot finish before its own remaining reads
  // complete, and any CQE (also of other batches) counts towards the wave.
  // The iterator is looked up anew in every round, because `reapWave` may erase
  // the entry.
  while (true) {
    auto it = numOutstandingReadRequestsPerBatch_.find(handle);
    if (it == numOutstandingReadRequestsPerBatch_.end()) {
      break;
    }
    reapWave(it->second);
  }
  // Throw only now that every read of the batch is reaped, so the kernel no
  // longer writes into its buffers.
  if (auto failed = failedBatches_.find(handle);
      failed != failedBatches_.end()) {
    std::string errorMessage = std::move(failed->second);
    failedBatches_.erase(failed);
    AD_THROW(std::move(errorMessage));
  }
}

//______________________________________________________________________________
void IoUringPolicy::reapWave(size_t numReadsToComplete) {
  AD_CORRECTNESS_CHECK(numReadsToComplete > 0,
                       "a wave must wait for at least one completion");
  AD_CORRECTNESS_CHECK(
      numReadsToComplete <= numOutstandingReadRequests_,
      "a wave cannot wait for more reads than are outstanding");
  const auto waveSize =
      static_cast<unsigned>(std::min<size_t>(REAP_WAVE, numReadsToComplete));

  // Block until `waveSize` completion queue entries (CQEs) are ready. This
  // typically costs one `io_uring_enter` for the whole wave. A signal ends the
  // wait early: with an empty completion queue, `io_uring_wait_cqes` returns
  // `-EINTR` and we wait again; with at least one ready CQE, it returns 0
  // although fewer than `waveSize` CQEs may be ready (the kernel then reports
  // success, see `io_cqring_wait`), and we reap those.
  int ret = 0;
  do {
    ret = ring_->waitForCompletions(waveSize);
  } while (ret == -EINTR);
  if (ret < 0) {
    AD_THROW(absl::StrCat("io_uring_wait_cqes failed in IoUringPolicy: ",
                          std::strerror(-ret)));
  }

  // Reap every ready CQE in chunks of up to `REAP_CHUNK`. `popCompletions`
  // does not block and releases a whole chunk at once. Every CQE of the chunk
  // is applied even if `processCqe` throws for one of them (its correctness
  // checks, or the allocation of a failure message): a CQE that left the
  // queue without being applied would leave the outstanding counts too high,
  // so that the next `wait()` or the destructor would block forever. The first
  // exception is rethrown once every ready CQE is applied.
  std::array<IoUringInterface::Completion, REAP_CHUNK> completions{};
  size_t numReaped = 0;
  std::exception_ptr firstException;
  while (true) {
    const size_t n = ring_->popCompletions(completions);
    if (n == 0) {
      break;
    }
    for (const auto& [requestId, numBytesRead] :
         ql::span<const IoUringInterface::Completion>(completions.data(), n)) {
      try {
        processCqe(numBytesRead, requestId);
      } catch (...) {
        if (!firstException) {
          firstException = std::current_exception();
        }
      }
    }
    numReaped += n;
  }
  if (firstException) {
    std::rethrow_exception(std::move(firstException));
  }
  // `io_uring_wait_cqes` returned 0, so at least one CQE was ready, and the
  // loop above reaps every ready CQE.
  AD_CORRECTNESS_CHECK(numReaped > 0,
                       "`io_uring_wait_cqes` returned 0 without a ready CQE");
}

//______________________________________________________________________________
void IoUringPolicy::processCqe(int numBytesRead, uint64_t requestId) {
  // Every reaped CQE corresponds to exactly one outstanding read whose id we
  // inserted in `addBatch`, so the entry must be present, and so must the
  // entry of its batch. Both lookups and checks happen before any count is
  // changed, so that a failing check leaves the bookkeeping consistent.
  auto outstandingReadIt = outstandingReadsByRequestId_.find(requestId);
  AD_CORRECTNESS_CHECK(
      outstandingReadIt != outstandingReadsByRequestId_.end(),
      "a CQE carries the id of a read that is not outstanding");
  const OutstandingRead outstandingRead = outstandingReadIt->second;
  auto it =
      numOutstandingReadRequestsPerBatch_.find(outstandingRead.batchHandle);
  AD_CORRECTNESS_CHECK(it != numOutstandingReadRequestsPerBatch_.end(),
                       "an outstanding read belongs to a batch that has no "
                       "outstanding reads");

  // Attribute the completion to its batch and decrement that batch's
  // outstanding count, erasing the batch once its last read is reaped. This
  // happens also for a failed read, so a batch whose read failed does not stay
  // outstanding.
  --numOutstandingReadRequests_;
  outstandingReadsByRequestId_.erase(outstandingReadIt);
  if (--it->second == 0) {
    numOutstandingReadRequestsPerBatch_.erase(it);
  }

  // `cqe->res` < 0 is `-errno`. A result smaller than requested (a partial
  // read, or 0 at end of file) means we read fewer bytes than expected, which
  // we treat as an error. Keep the first failure of the batch; `wait()` of that
  // batch throws it.
  if (failedBatches_.contains(outstandingRead.batchHandle)) {
    return;
  }
  if (numBytesRead < 0) {
    failedBatches_.emplace(
        outstandingRead.batchHandle,
        absl::StrCat("I/O error in IoUringPolicy read operation for request ",
                     requestId, ": ", std::strerror(-numBytesRead)));
  } else if (static_cast<size_t>(numBytesRead) !=
             outstandingRead.expectedNumBytes) {
    failedBatches_.emplace(
        outstandingRead.batchHandle,
        absl::StrCat("read fewer bytes than requested in IoUringPolicy for "
                     "request ",
                     requestId, ": expected ", outstandingRead.expectedNumBytes,
                     " bytes, got ", numBytesRead));
  }
}

#endif  // QLEVER_HAS_IO_URING

}  // namespace ad_utility
