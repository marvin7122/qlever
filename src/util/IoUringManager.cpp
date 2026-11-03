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
#include <chrono>
#include <stdexcept>
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
// `IoUringInterface` via liburing. Neither copyable nor movable: it owns a
// kernel ring.
class LiburingRing final : public IoUringInterface, public NoCopyNoMove {
 public:
  // Set up a ring with (at least) `ringSize` SQEs. Throws if
  // `io_uring_queue_init` fails.
  explicit LiburingRing(unsigned ringSize) {
    // Set up the submission and completion queues, shared between this
    // process and the kernel, with `ringSize` submission slots (rounded up to
    // a power of two) and twice as many completion slots. See
    // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
    // details.
    const int result = io_uring_queue_init(ringSize, &ring_, /*flags=*/0);
    if (result < 0) {
      AD_THROW("io_uring_queue_init failed in IoUringManager");
    }
  }

  ~LiburingRing() override { io_uring_queue_exit(&ring_); }

  bool prepareRead(int fd, char* buffer, size_t numberOfBytes, uint64_t offset,
                   uint64_t requestId) override {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      return false;
    }
    // This only fills in the SQE; the kernel sees it at the next
    // `io_uring_submit`. io_uring copies the SQE's `user_data` verbatim into
    // the matching completion.
    io_uring_prep_read(sqe, fd, buffer, static_cast<unsigned>(numberOfBytes),
                       static_cast<__u64>(offset));
    io_uring_sqe_set_data64(sqe, requestId);
    return true;
  }

  size_t numberOfQueued() const override { return io_uring_sq_ready(&ring_); }

  // `*kring_entries` exists in every liburing version, `ring_entries` only
  // since 2.3.
  size_t sqCapacity() const override { return *ring_.sq.kring_entries; }
  size_t cqCapacity() const override { return *ring_.cq.kring_entries; }

  int submit() override { return io_uring_submit(&ring_); }

  int waitForCompletions(unsigned minComplete) override {
    io_uring_cqe* cqe = nullptr;
    return io_uring_wait_cqes(&ring_, &cqe, minComplete, nullptr, nullptr);
  }

  size_t popCompletions(ql::span<Completion> completions) override {
    // `io_uring_peek_batch_cqe` does not block; `io_uring_cq_advance` releases
    // all of them with one CQ-head update. They are copied first, so nothing
    // that happens with the results can leave a released CQE unprocessed or a
    // processed one in the queue.
    std::array<io_uring_cqe*, 64> cqes{};
    const unsigned numberOfReady = io_uring_peek_batch_cqe(
        &ring_, cqes.data(),
        static_cast<unsigned>(std::min(cqes.size(), completions.size())));
    for (unsigned i : ql::views::iota(0u, numberOfReady)) {
      completions[i] =
          Completion{io_uring_cqe_get_data64(cqes[i]), cqes[i]->res};
    }
    io_uring_cq_advance(&ring_, numberOfReady);
    return numberOfReady;
  }

  std::vector<uint64_t> cancelQueued(uint64_t noOpRequestId) override {
    // liburing consumes completions with this id itself.
    AD_CORRECTNESS_CHECK(noOpRequestId != LIBURING_UDATA_TIMEOUT,
                         "liburing consumes the completions of "
                         "LIBURING_UDATA_TIMEOUT itself, so a no-op with this "
                         "id would never be reaped.");
    // Without `IORING_SETUP_SQPOLL`, the kernel reads SQEs only inside our own
    // `io_uring_enter` calls, so the SQEs it has not taken can safely be
    // rewritten between two calls.
    AD_CORRECTNESS_CHECK((ring_.flags & IORING_SETUP_SQPOLL) == 0,
                         "Rewriting queued SQEs is only safe if the kernel "
                         "reads SQEs only inside io_uring_enter (no SQPOLL).");
    // The queued SQEs are the `io_uring_sq_ready` positions from the kernel's
    // head on, and liburing hands out the SQE of position `p` as
    // `sqes[p & mask]` (`io_uring_get_sqe`, in every version). The SQ array is
    // not used: liburing <= 2.2 fills it only when it flushes the SQEs to the
    // kernel. `*kring_mask` exists in every version, `ring_mask` only since
    // 2.3.
    const unsigned mask = *ring_.sq.kring_mask;
    const unsigned head = io_uring_smp_load_acquire(ring_.sq.khead);
    const unsigned numberOfReady = io_uring_sq_ready(&ring_);
    std::vector<uint64_t> cancelled;
    for (const unsigned i : ql::views::iota(0u, numberOfReady)) {
      io_uring_sqe* sqe = &ring_.sq.sqes[(head + i) & mask];
      if (sqe->user_data != noOpRequestId) {
        cancelled.push_back(sqe->user_data);
      }
      io_uring_prep_nop(sqe);
      io_uring_sqe_set_data64(sqe, noOpRequestId);
    }
    return cancelled;
  }

  void backOff() override {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

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
    : IoUringPolicy(makeLiburingRing(ringSize)) {}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(std::unique_ptr<IoUringInterface> ring)
    : ring_{std::move(ring)} {
  AD_CONTRACT_CHECK(ring_ != nullptr,
                    "An IoUringPolicy needs a ring to run on.");
  // `addBatch` keeps at most `ringSize_` reads in the ring, counting those that
  // are queued, in the kernel, or completed but not reaped. So each of them
  // has a free SQE, and at most `ringSize_` completions are ever pending.
  ringSize_ = ring_->sqCapacity();
  AD_CONTRACT_CHECK(ringSize_ > 0,
                    "The ring of an IoUringPolicy needs an SQE.");
  AD_CONTRACT_CHECK(ring_->cqCapacity() >= ringSize_,
                    "The ring of an IoUringPolicy needs at least as many CQEs "
                    "as SQEs, or completions could be dropped.");
}

//______________________________________________________________________________
IoUringPolicy::~IoUringPolicy() {
  if (numInFlightReadRequests_ > 0) {
    AD_LOG_WARN << "IoUringPolicy destroyed with " << numInFlightReadRequests_
                << " read request(s) still in flight; all batches should be "
                   "`wait()`ed before destroying the policy. Draining them now "
                   "so the kernel stops writing into the target buffers.\n";
  }
  // Reap the outstanding completions before tearing down the ring (in the
  // destructor of `ring_`), so the kernel is no longer writing into any target
  // buffer once we return. We deliberately do not call `drainOneCqe` here: it
  // throws on I/O errors, and a destructor must not throw. We also stop if
  // waiting fails, to avoid spinning forever (it would not decrement the
  // in-flight count).
  while (numInFlightReadRequests_ > 0) {
    if (ring_->waitForCompletions(1) < 0) {
      break;
    }
    IoUringInterface::Completion completion{};
    numInFlightReadRequests_ -= ring_->popCompletions({&completion, 1});
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
  numInFlightReadRequestsPerBatch_[handle] = numReadRequestsToPerform;

  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    // The ring has no free slot, so make room: submit what we have prepared so
    // far and block until enough completions have been drained.
    if (numInFlightReadRequests_ >= ringSize_) {
      // Flush the SQEs prepared so far to the kernel so the kernel can start
      // servicing them. Their completions will free up submission slots.
      ring_->submit();
      while (numInFlightReadRequests_ >= ringSize_) {
        drainOneCqe();
      }
    }

    // Tag the read with a unique request id and record its metadata (the batch
    // it belongs to and how many bytes it should read). io_uring copies the
    // request id verbatim into the matching completion, so `drainOneCqe` can
    // recover it.
    const uint64_t requestId = nextRequestIdToAssign_++;
    inFlightReadsByRequestId_[requestId] = InFlightRead{handle, numBytesToRead};

    // Prepare the read in the next free SQE (the request is not handed to the
    // kernel until a later submit). The check above guarantees a free SQE.
    AD_CORRECTNESS_CHECK(ring_->prepareRead(fd, targetBuf, numBytesToRead,
                                            fileOffset, requestId));
    numInFlightReadRequests_++;
  }
  // Flush the remaining prepared SQEs to the kernel (the loop above only
  // submits when the submission queue is full, so the last group of SQEs has
  // not yet been submitted).
  ring_->submit();
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  // Drain completions until this batch is gone. `drainOneCqe` erases a batch as
  // soon as its last read completes, so a present entry always still has
  // outstanding reads.
  while (numInFlightReadRequestsPerBatch_.find(handle) !=
         numInFlightReadRequestsPerBatch_.end()) {
    drainOneCqe();
  }
}

//______________________________________________________________________________
void ad_utility::IoUringPolicy::drainOneCqe() {
  // Block until at least one completion queue entry (CQE) is available.
  int ret = ring_->waitForCompletions(1);
  if (ret < 0) {
    AD_THROW("io_uring_wait_cqe failed in IoUringPolicy");
  }

  // Recover the read's result and the request id we stored in the SQE, and
  // consume the CQE so its slot is freed. Do this before any throw.
  IoUringInterface::Completion completion{};
  AD_CORRECTNESS_CHECK(ring_->popCompletions({&completion, 1}) == 1);
  const int numBytesRead = completion.result;
  const uint64_t requestId = completion.requestId;
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
  // A result smaller than requested (a partial read, or 0 at end of file) means
  // we read fewer bytes than expected, which we treat as an error.
  if (static_cast<size_t>(numBytesRead) != inFlightRead.expectedNumBytes) {
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
