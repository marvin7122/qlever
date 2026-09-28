// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/IoUringManager.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <fcntl.h>
#include <sched.h>
#include <sys/mman.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

namespace {
constexpr size_t numIoUringCounterSlots =
    static_cast<size_t>(IoUringCounter::NumCounters);

// The slots of the diagnostic counters (see `IoUringCounter`): a shared
// mapping of the file named by `QLEVER_IOURING_COUNTERS_FILE`, or process
// memory when the variable is unset or the file cannot be mapped.
uint64_t* ioUringCounterSlots() {
  static uint64_t* const slots = []() -> uint64_t* {
    const char* path = std::getenv("QLEVER_IOURING_COUNTERS_FILE");
    if (path != nullptr && path[0] != '\0') {
      const size_t numBytes = numIoUringCounterSlots * sizeof(uint64_t);
      const int fd = open(path, O_RDWR | O_CREAT, 0644);
      if (fd >= 0) {
        void* mapped = MAP_FAILED;
        if (ftruncate(fd, static_cast<off_t>(numBytes)) == 0) {
          mapped = mmap(nullptr, numBytes, PROT_READ | PROT_WRITE, MAP_SHARED,
                        fd, 0);
        }
        close(fd);
        if (mapped != MAP_FAILED) {
          auto* fileSlots = static_cast<uint64_t*>(mapped);
          __atomic_store_n(&fileSlots[0], IO_URING_COUNTERS_MAGIC,
                           __ATOMIC_RELAXED);
          return fileSlots;
        }
      }
      AD_LOG_WARN << "Cannot map the io_uring counters file " << path
                  << "; counting in process memory" << std::endl;
    }
    static std::array<uint64_t, numIoUringCounterSlots> processSlots{
        IO_URING_COUNTERS_MAGIC};
    return processSlots.data();
  }();
  return slots;
}

// The time since `start` in nanoseconds.
uint64_t nanosecondsSince(std::chrono::steady_clock::time_point start) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - start)
          .count());
}
}  // namespace

//______________________________________________________________________________
void detail::addToIoUringCounter(IoUringCounter counter, uint64_t value) {
  __atomic_fetch_add(&ioUringCounterSlots()[static_cast<size_t>(counter)],
                     value, __ATOMIC_RELAXED);
}

//______________________________________________________________________________
uint64_t getIoUringCounter(IoUringCounter counter) {
  if constexpr (!ioUringCountersEnabled) {
    return 0;
  }
  return __atomic_load_n(&ioUringCounterSlots()[static_cast<size_t>(counter)],
                         __ATOMIC_RELAXED);
}

#ifdef QLEVER_HAS_IO_URING
namespace {
// MEASUREMENT ONLY (bench branch, not for merge): environment overrides of the
// SQPoll setup for the research loop of marvin7122/qlever#164.
//   QLEVER_SQPOLL_IDLE_MS=<ms>        sq_thread_idle
//   QLEVER_SQPOLL_CPU=<cpu>|none      pin exactly to <cpu> (no remap), or no
//                                     IORING_SETUP_SQ_AFF at all
//   QLEVER_SQPOLL_SHARED=1            all SQPoll rings attach to one
//                                     process-wide poll thread (ATTACH_WQ)
//   QLEVER_SQPOLL_REAP=block|spin|timeout  how a reap waits when no CQE is
//                                     ready: io_uring_wait_cqe (default),
//                                     busy peek, or wait_cqe_timeout(50 us)
struct SqPollExperiment {
  std::optional<unsigned> idleMs;
  bool cpuOverride = false;
  std::optional<unsigned> cpu;  // nullopt with cpuOverride: no SQ_AFF
  bool shared = false;
  enum class Reap { Block, Spin, Timeout } reap = Reap::Block;
};

const SqPollExperiment& sqPollExperiment() {
  static const SqPollExperiment experiment = [] {
    SqPollExperiment e;
    if (const char* v = std::getenv("QLEVER_SQPOLL_IDLE_MS")) {
      e.idleMs = static_cast<unsigned>(std::strtoul(v, nullptr, 10));
    }
    if (const char* v = std::getenv("QLEVER_SQPOLL_CPU")) {
      e.cpuOverride = true;
      if (std::string_view{v} != "none") {
        e.cpu = static_cast<unsigned>(std::strtoul(v, nullptr, 10));
      }
    }
    if (const char* v = std::getenv("QLEVER_SQPOLL_SHARED")) {
      e.shared = std::string_view{v} == "1";
    }
    if (const char* v = std::getenv("QLEVER_SQPOLL_REAP")) {
      if (std::string_view{v} == "spin") {
        e.reap = SqPollExperiment::Reap::Spin;
      } else if (std::string_view{v} == "timeout") {
        e.reap = SqPollExperiment::Reap::Timeout;
      }
    }
    AD_LOG_INFO << "SQPoll experiment: idleMs="
                << (e.idleMs ? std::to_string(*e.idleMs) : "default") << " cpu="
                << (!e.cpuOverride ? "default"
                                   : (e.cpu ? std::to_string(*e.cpu) : "none"))
                << " shared=" << e.shared
                << " reap=" << static_cast<int>(e.reap) << std::endl;
    return e;
  }();
  return experiment;
}

// Return `preferredCpu` when it is in this process's affinity mask, otherwise
// the first CPU in the mask. Falls back to `preferredCpu` when the mask
// cannot be read; the kernel setup then reports the error as before.
// The descriptor of the process-wide ring whose SQPoll thread every SQPoll
// ring with `shareSqPollThread` attaches to, or a negative value if that ring
// cannot be set up. It is created on first use with the SQPoll parameters of
// the first such ring (CPU and idle time) and lives until the process ends,
// so no attached ring can outlive its poller. A tiny ring: only its poll
// thread is used, no reads are submitted to it.
int sharedSqPollRingFd(const io_uring_params& sqPollParams) {
  static const int fd = [&sqPollParams]() {
    static io_uring pollerRing{};
    io_uring_params params{};
    params.flags =
        sqPollParams.flags & (IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF);
    params.sq_thread_cpu = sqPollParams.sq_thread_cpu;
    params.sq_thread_idle = sqPollParams.sq_thread_idle;
    const int ret = io_uring_queue_init_params(8, &pollerRing, &params);
    if (ret < 0) {
      AD_LOG_WARN << "The shared SQPoll ring could not be set up ("
                  << std::strerror(-ret)
                  << "); every SQPoll ring gets its own poll thread"
                  << std::endl;
      return ret;
    }
    AD_LOG_INFO << "io_uring SQPoll: all SQPoll rings share one kernel poll "
                   "thread (idle "
                << params.sq_thread_idle << " ms, "
                << ((params.flags & IORING_SETUP_SQ_AFF)
                        ? absl::StrCat("pinned to CPU ", params.sq_thread_cpu)
                        : std::string{"not pinned"})
                << ")" << std::endl;
    return pollerRing.ring_fd;
  }();
  return fd;
}

unsigned firstCpuInAffinityOr(unsigned preferredCpu) {
  cpu_set_t affinity;
  CPU_ZERO(&affinity);
  if (sched_getaffinity(0, sizeof(affinity), &affinity) != 0) {
    return preferredCpu;
  }
  if (CPU_ISSET(preferredCpu, &affinity)) {
    return preferredCpu;
  }
  for (unsigned cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(cpu, &affinity)) {
      return cpu;
    }
  }
  return preferredCpu;
}
}  // namespace
#endif  // QLEVER_HAS_IO_URING

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
IoUringPolicy::IoUringPolicy(unsigned ringSize)
    : IoUringPolicy(ringSize, IoUringSetupOptions{}) {}

//______________________________________________________________________________
IoUringPolicy::IoUringPolicy(unsigned ringSize,
                             const IoUringSetupOptions& setupOptions)
    : ringSize_(ringSize) {
  AD_CORRECTNESS_CHECK(ringSize > 0);
  // Set up the submission and completion queues, shared between this process
  // and the kernel, with (at least) `ringSize_` submission slots in the
  // submission queue. liburing rounds the requested size up to a power of two,
  // so the actual ring may be larger than `ringSize`; `ringSize_` is therefore
  // a conservative (lower) bound for the "ring full" check below. See
  // https://man7.org/linux/man-pages/man3/io_uring_queue_init.3.html for
  // details.
  const bool wantsSpecialSetup = setupOptions.useSqPoll ||
                                 setupOptions.deferTaskrun ||
                                 setupOptions.singleIssuer;
  if (!wantsSpecialSetup) {
    int ret = io_uring_queue_init(ringSize_, &ring_, /*flags=*/0);
    if (ret < 0) {
      AD_THROW("io_uring_queue_init failed in IoUringManager");
    }
    return;
  }
  struct io_uring_params params {};
  if (setupOptions.useSqPoll) {
    params.flags |= IORING_SETUP_SQPOLL;
    if (setupOptions.sqThreadCpu.has_value()) {
      const unsigned configuredCpu = setupOptions.sqThreadCpu.value();
      params.flags |= IORING_SETUP_SQ_AFF;
      // Pin the poll thread to a CPU in this process's affinity mask: the
      // configured CPU may be offline or isolated, in which case the kernel
      // would deny the setup with `-EINVAL`.
      params.sq_thread_cpu = firstCpuInAffinityOr(configuredCpu);
      if (params.sq_thread_cpu != configuredCpu) {
        AD_LOG_WARN << "SQPoll CPU " << configuredCpu
                    << " is not in this process's affinity mask; pinning the "
                       "poll thread to CPU "
                    << params.sq_thread_cpu << " instead" << std::endl;
      }
    }
    params.sq_thread_idle = setupOptions.sqThreadIdleMs;
    // MEASUREMENT ONLY: environment overrides, see `SqPollExperiment`.
    const auto& experiment = sqPollExperiment();
    if (experiment.idleMs.has_value()) {
      params.sq_thread_idle = experiment.idleMs.value();
    }
    if (experiment.cpuOverride) {
      if (experiment.cpu.has_value()) {
        params.sq_thread_cpu = experiment.cpu.value();
      } else {
        params.flags &= ~IORING_SETUP_SQ_AFF;
        params.sq_thread_cpu = 0;
      }
    }
  }
  if (setupOptions.deferTaskrun) {
#ifdef IORING_SETUP_DEFER_TASKRUN
    params.flags |= IORING_SETUP_DEFER_TASKRUN;
#endif
  }
  if (setupOptions.singleIssuer) {
#ifdef IORING_SETUP_SINGLE_ISSUER
    params.flags |= IORING_SETUP_SINGLE_ISSUER;
#endif
  }
  // Under SQPoll, give the ring `ringSize_` headroom entries on top while the
  // in-flight cap in `addBatch` stays `ringSize_`. The poll thread publishes
  // the submission-queue head only after it has handed off a batch of entries,
  // so a slot freed by a just-reaped completion may not be visible to this
  // thread yet, and `io_uring_get_sqe` would return `nullptr` although fewer
  // than `ringSize_` reads are in flight. With the headroom the ring cannot
  // fill up before the cap does. (Draining and retrying instead was tried and
  // hung under cancellation.) The extra entries cost a few kilobytes.
  const unsigned entries = setupOptions.useSqPoll ? 2 * ringSize_ : ringSize_;
  int ret = -EINVAL;
  if (setupOptions.useSqPoll && setupOptions.shareSqPollThread) {
    if (const int pollerFd = sharedSqPollRingFd(params); pollerFd >= 0) {
      io_uring_params attached = params;
      attached.flags |= IORING_SETUP_ATTACH_WQ;
      attached.wq_fd = static_cast<__u32>(pollerFd);
      ret = io_uring_queue_init_params(entries, &ring_, &attached);
      if (ret < 0) {
        AD_LOG_WARN << "Attaching an io_uring to the shared SQPoll thread "
                       "failed ("
                    << std::strerror(-ret)
                    << "); this ring gets its own poll thread" << std::endl;
      }
    }
  }
  if (ret < 0) {
    ret = io_uring_queue_init_params(entries, &ring_, &params);
  }
  bool usedFallbackRing = false;
  if (ret == -EPERM || ret == -EINVAL) {
    // The kernel denied the requested setup (missing `CAP_SYS_NICE` for the
    // SQPoll thread, or a kernel without support for one of the flags).
    // Fall back to a plain ring so the lookup path keeps working; the outer
    // `makeBatchManager` still falls back to `SyncIoPolicy` when even the
    // plain setup fails.
    AD_LOG_WARN << "io_uring setup with special flags denied ("
                << std::strerror(-ret)
                << "); falling back to a plain ring without SQPoll"
                << std::endl;
    params = {};
    ret = io_uring_queue_init_params(ringSize_, &ring_, &params);
    usedFallbackRing = true;
  }
  if (ret < 0) {
    AD_THROW(absl::StrCat(
        "io_uring_queue_init_params failed in IoUringManager (",
        usedFallbackRing ? "plain fallback ring after the special setup was "
                           "denied"
                         : "setup with special flags",
        "): ", std::strerror(-ret)));
  }
  // Report SQPoll only when the kernel granted the requested setup. After the
  // fallback above no poll thread exists, even though SQPoll was requested.
  sqPollEnabled_ = setupOptions.useSqPoll && !usedFallbackRing;
  if (sqPollEnabled_) {
    countIoUring(IoUringCounter::SqPollRings);
  }
}

//______________________________________________________________________________
bool IoUringPolicy::sqPollAvailable() {
  // Probe each CPU in this process's affinity mask instead of hardcoding CPU
  // 0: on systems where CPU 0 is offline or isolated, the probe would fail
  // even though SQPoll works elsewhere.
  cpu_set_t affinity;
  CPU_ZERO(&affinity);
  if (sched_getaffinity(0, sizeof(affinity), &affinity) != 0) {
    return false;
  }
  for (unsigned cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (!CPU_ISSET(cpu, &affinity)) {
      continue;
    }
    struct io_uring probe {};
    struct io_uring_params params {};
    // Request the poll thread, pinned to `cpu`, with a short idle timeout so
    // a granted poller sleeps again almost immediately after the probe.
    params.flags = IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF;
    params.sq_thread_cpu = cpu;
    params.sq_thread_idle = 10;
    // A tiny ring keeps the probe cheap; 8 is below liburing's minimum and
    // gets rounded up.
    const int ret = io_uring_queue_init_params(8, &probe, &params);
    if (ret != 0) {
      // No ring was created, so there is nothing to release.
      if (ret == -EPERM) {
        // Missing `CAP_SYS_NICE`: no CPU will be granted a poller.
        return false;
      }
      continue;
    }
    // The guard releases the probe ring when this scope exits, so the
    // early return below cannot leak it even if more control flow is added
    // later.
    absl::Cleanup probeGuard{[&probe] { io_uring_queue_exit(&probe); }};
    return true;
  }
  return false;
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
  countIoUring(IoUringCounter::Batches);
  countIoUring(IoUringCounter::Reads, numReadRequestsToPerform);

  for (const auto& [numBytesToRead, fileOffset, targetBuf] :
       ::ranges::views::zip(numBytesToReadPerRequest, fileOffsetPerRequest,
                            targetBufferPerRequest)) {
    // The ring has no free slot, so make room: submit what we have prepared so
    // far and block until enough completions have been drained.
    if (numInFlightReadRequests_ >= ringSize_) {
      // Flush the SQEs prepared so far to the kernel so the kernel can start
      // servicing them. Their completions will free up submission slots.
      submit();
      while (numInFlightReadRequests_ >= ringSize_) {
        drainOneCqe();
      }
    }

    // Claim the next free SQE. The check above guarantees a slot is available
    // (under SQPoll thanks to the ring headroom, see the constructor), so
    // `io_uring_get_sqe` must not return `nullptr` here.
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);

    // Record the read's parameters in the SQE (this only sets the SQE's fields;
    // the request is not handed to the kernel until a later `io_uring_submit`).
    io_uring_prep_read(sqe, fd, targetBuf,
                       static_cast<unsigned>(numBytesToRead),
                       static_cast<__u64>(fileOffset));

    // Tag the SQE with a unique request id and record its metadata (the batch
    // it belongs to and how many bytes it should read). io_uring copies the
    // request id (the SQE's `user_data`) verbatim into the matching completion,
    // so `drainOneCqe` can recover it.
    const uint64_t requestId = nextRequestIdToAssign_++;
    inFlightReadsByRequestId_[requestId] = InFlightRead{handle, numBytesToRead};
    // Store the id in the pointer-sized `user_data` field, which every
    // liburing version provides. The 64-bit `io_uring_sqe_set_data64` helper
    // requires a very recent liburing that older images (e.g. the gcc11 CI
    // image with its distro liburing) do not have yet.
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(requestId));
    numInFlightReadRequests_++;
  }
  // Flush the remaining prepared SQEs to the kernel (the loop above only
  // submits when the submission queue is full, so the last group of SQEs has
  // not yet been submitted).
  submit();
}

//______________________________________________________________________________
void IoUringPolicy::submit() {
  if constexpr (ioUringCountersEnabled) {
    countIoUring(IoUringCounter::SubmitCalls);
    // Mirror liburing's decision whether `io_uring_submit` enters the
    // kernel: without SQPoll whenever entries are prepared, with SQPoll only
    // to wake a poll thread that went to sleep.
    const bool entersKernel = sqPollEnabled_
                                  ? (IO_URING_READ_ONCE(*ring_.sq.kflags) &
                                     IORING_SQ_NEED_WAKEUP) != 0
                                  : io_uring_sq_ready(&ring_) > 0;
    if (entersKernel) {
      countIoUring(IoUringCounter::SubmitSyscalls);
    }
  }
  io_uring_submit(&ring_);
}

//______________________________________________________________________________
void IoUringPolicy::wait(BatchHandle handle) {
  const auto start = ioUringCountersEnabled
                         ? std::chrono::steady_clock::now()
                         : std::chrono::steady_clock::time_point{};
  // Drain completions until this batch is gone. `drainOneCqe` erases a batch as
  // soon as its last read completes, so a present entry always still has
  // outstanding reads.
  while (numInFlightReadRequestsPerBatch_.find(handle) !=
         numInFlightReadRequestsPerBatch_.end()) {
    drainOneCqe();
  }
  if constexpr (ioUringCountersEnabled) {
    countIoUring(IoUringCounter::BatchWaits);
    countIoUring(IoUringCounter::BatchWaitNs, nanosecondsSince(start));
  }
}

//______________________________________________________________________________
void ad_utility::IoUringPolicy::drainOneCqe() {
  // Take a completion queue entry (CQE) if one is ready, else block until one
  // is available.
  io_uring_cqe* cqe = nullptr;
  int ret = io_uring_peek_cqe(&ring_, &cqe);
  if (ret == -EAGAIN) {
    const auto start = ioUringCountersEnabled
                           ? std::chrono::steady_clock::now()
                           : std::chrono::steady_clock::time_point{};
    // MEASUREMENT ONLY: the reap mode of `SqPollExperiment`.
    const auto reap = sqPollEnabled_ ? sqPollExperiment().reap
                                     : SqPollExperiment::Reap::Block;
    if (reap == SqPollExperiment::Reap::Spin) {
      // Busy-check the completion queue without entering the kernel; fall
      // back to a blocking wait after a bounded number of checks.
      for (size_t i = 0;; ++i) {
        ret = io_uring_peek_cqe(&ring_, &cqe);
        if (ret != -EAGAIN) {
          break;
        }
        if (i >= 10'000'000) {
          ret = io_uring_wait_cqe(&ring_, &cqe);
          break;
        }
        __builtin_ia32_pause();
      }
    } else if (reap == SqPollExperiment::Reap::Timeout) {
      __kernel_timespec timeout{};
      timeout.tv_nsec = 50'000;
      do {
        ret = io_uring_wait_cqe_timeout(&ring_, &cqe, &timeout);
      } while (ret == -ETIME);
    } else {
      ret = io_uring_wait_cqe(&ring_, &cqe);
    }
    if constexpr (ioUringCountersEnabled) {
      countIoUring(IoUringCounter::BlockingWaits);
      countIoUring(IoUringCounter::BlockingWaitNs, nanosecondsSince(start));
    }
  }
  if (ret < 0) {
    AD_THROW("io_uring_wait_cqe failed in IoUringPolicy");
  }
  countIoUring(IoUringCounter::CompletionsReaped);

  // Recover the read's result (`cqe->res`) and the request id we stored in the
  // SQE, then consume the CQE so its slot is freed. Do this before any throw.
  const int numBytesRead = cqe->res;
  // Recover the id via the pointer-sized `user_data` field, see `addBatch`.
  const uint64_t requestId =
      reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));
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
