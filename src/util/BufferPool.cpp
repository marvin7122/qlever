// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/BufferPool.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iterator>
#include <limits>
#include <thread>
#include <utility>

#include "util/Exception.h"
#include "util/IoWaitStats.h"

#if defined(__linux__) && !defined(__EMSCRIPTEN__)
#include <fcntl.h>
#include <linux/futex.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <climits>
#include <ctime>
#define QL_BUFFER_POOL
#endif

namespace ad_utility {

namespace {
// The state value of a page whose memory is being returned to the kernel
// (between the eviction and the end of `madvise`). Nobody may load the page
// then, because `madvise` would discard the new contents.
constexpr uint64_t evicting = 3;

// Read `numBytes` at `offset` of `fd` into `target` completely, or throw.
void preadFully(int fd, char* target, size_t numBytes, uint64_t offset) {
#ifdef QL_BUFFER_POOL
  size_t done = 0;
  while (done < numBytes) {
    ssize_t n = ::pread(fd, target + done, numBytes - done,
                        static_cast<off_t>(offset + done));
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n <= 0) {
      throw std::runtime_error{absl::StrCat(
          "BufferPool: reading ", numBytes, " bytes at offset ", offset,
          " failed: ",
          n < 0 ? std::strerror(errno) : "unexpected end of file")};
    }
    done += static_cast<size_t>(n);
  }
#else
  (void)fd;
  (void)target;
  (void)numBytes;
  (void)offset;
  throw std::runtime_error{"BufferPool is not supported on this platform"};
#endif
}

// The default `Loader`: synchronous `pread`s.
void loadSynchronously(int fd, ql::span<const size_t> numBytes,
                       ql::span<const uint64_t> offsets,
                       ql::span<char*> targets) {
  for (size_t i = 0; i < numBytes.size(); ++i) {
    preadFully(fd, targets[i], numBytes[i], offsets[i]);
  }
}

#ifdef QL_BUFFER_POOL
// The futex of a page: the low half of its state word, which holds the state
// bits, so it changes whenever the page stops loading or evicting.
static_assert(sizeof(std::atomic<uint64_t>) == sizeof(uint64_t));
static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
              "the futex is the low half of the state word");
uint32_t* futexOf(std::atomic<uint64_t>& word) {
  return reinterpret_cast<uint32_t*>(&word);
}

// Sleep while the futex of `word` holds `expected`, at most 10 ms (a safety
// net; every state change that ends a wait wakes the sleepers).
void futexWait(std::atomic<uint64_t>& word, uint32_t expected) {
  timespec timeout{0, 10'000'000};
  ::syscall(SYS_futex, futexOf(word), FUTEX_WAIT_PRIVATE, expected, &timeout,
            nullptr, 0);
}

void futexWakeAll(std::atomic<uint64_t>& word) {
  ::syscall(SYS_futex, futexOf(word), FUTEX_WAKE_PRIVATE, INT_MAX, nullptr,
            nullptr, 0);
}
#endif

// The pending split-phase reads of this thread (see
// `BufferPool::PendingRead`).
thread_local std::vector<BufferPool::PendingRead*> ownPendingReads;
}  // namespace

// _____________________________________________________________________________
template <typename F>
uint64_t BufferPool::transition(size_t page, F next) {
  uint64_t old = states_[page].load(std::memory_order_relaxed);
  uint64_t updated = next(old);
  while (!states_[page].compare_exchange_weak(old, updated,
                                              std::memory_order_acq_rel)) {
    updated = next(old);
  }
#ifdef QL_BUFFER_POOL
  if ((old & waiterBit) != 0 && (updated & waiterBit) == 0) {
    futexWakeAll(states_[page]);
  }
#endif
  return old;
}

// _____________________________________________________________________________
void BufferPool::waitWhileBusy(size_t page) {
  io_wait_stats::ScopedWait timeWait{io_wait_stats::poolPageWaitNs,
                                     io_wait_stats::poolPageWaits};
  size_t spins = 0;
  while (true) {
    uint64_t word = states_[page].load(std::memory_order_acquire);
    const uint64_t state = word & stateMask;
    if (state == resident || state == evicted) {
      return;
    }
    if (++spins < 64) {
#if defined(__x86_64__)
      __builtin_ia32_pause();
#endif
      continue;
    }
#ifdef QL_BUFFER_POOL
    if (bufferPoolFutexWait.load(std::memory_order_relaxed)) {
      // Announce the sleeper, so that the next state change wakes it, then
      // sleep unless the word has changed in the meantime.
      if ((word & waiterBit) == 0) {
        if (!states_[page].compare_exchange_weak(word, word | waiterBit,
                                                 std::memory_order_acq_rel)) {
          continue;
        }
        word |= waiterBit;
      }
      futexWait(states_[page], static_cast<uint32_t>(word));
      continue;
    }
#endif
    std::this_thread::yield();
  }
}

// _____________________________________________________________________________
BufferPool::BufferPool([[maybe_unused]] const std::string& filename,
                       size_t fileSize, size_t budgetBytes,
                       size_t pageSize)
    : pageSize_{pageSize}, budgetPages_{budgetBytes / pageSize} {
  AD_CONTRACT_CHECK(pageSize >= defaultPageSize &&
                    pageSize % defaultPageSize == 0 &&
                    (pageSize & (pageSize - 1)) == 0);
#ifdef QL_BUFFER_POOL
  if (fileSize == 0) {
    return;
  }
  numPages_ = (fileSize + pageSize_ - 1) / pageSize_;
  AD_CONTRACT_CHECK(numPages_ <= std::numeric_limits<uint32_t>::max());
  bufferedFd_ = ::open(filename.c_str(), O_RDONLY | O_CLOEXEC);
  if (bufferedFd_ < 0) {
    return;
  }
  fd_ = ::open(filename.c_str(), O_RDONLY | O_CLOEXEC | O_DIRECT);
  directIo_ = fd_ >= 0;
  if (!directIo_) {
    // E.g. tmpfs: read through the page cache instead.
    fd_ = ::dup(bufferedFd_);
  }
  // Allocate the states before reserving the region: if the allocation
  // throws, no mapping exists yet that could leak.
  states_ = std::make_unique<std::atomic<uint64_t>[]>(numPages_);
  for (size_t i = 0; i < numPages_; ++i) {
    states_[i].store(evicted, std::memory_order_relaxed);
  }
  regionSize_ = numPages_ * pageSize_;
  void* region = ::mmap(nullptr, regionSize_, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (region == MAP_FAILED || fd_ < 0) {
    if (region != MAP_FAILED) {
      ::munmap(region, regionSize_);
    }
    return;
  }
  region_ = static_cast<char*>(region);
  fileSize_ = fileSize;
#else
  (void)fileSize;
#endif
}

// _____________________________________________________________________________
BufferPool::~BufferPool() {
#ifdef QL_BUFFER_POOL
  if (region_ != nullptr) {
    ::munmap(region_, regionSize_);
  }
  if (fd_ >= 0) {
    ::close(fd_);
  }
  if (bufferedFd_ >= 0) {
    ::close(bufferedFd_);
  }
#endif
}

// _____________________________________________________________________________
void BufferPool::setBudget(size_t budgetBytes) {
  budgetPages_.store(budgetBytes / pageSize_);
}

// _____________________________________________________________________________
bool BufferPool::tryCopy(uint64_t offset, size_t numBytes, char* target) {
  if (numBytes == 0) {
    return true;
  }
  const size_t first = offset / pageSize_;
  const size_t last = (offset + numBytes - 1) / pageSize_;
  // The state words seen before the copy, without the reference bit (most
  // reads touch one or two pages).
  constexpr size_t numInline = 8;
  uint64_t inlineSeen[numInline];
  std::vector<uint64_t> heapSeen;
  uint64_t* seen = inlineSeen;
  if (last - first + 1 > numInline) {
    heapSeen.resize(last - first + 1);
    seen = heapSeen.data();
  }
  for (size_t page = first; page <= last; ++page) {
    const uint64_t word = states_[page].load(std::memory_order_acquire);
    if ((word & stateMask) != resident) {
      return false;
    }
    seen[page - first] = word & ~referenceBit;
  }
  std::memcpy(target, region_ + offset, numBytes);
  // The copy is valid if no page was evicted in the meantime: an eviction
  // changes the state and the version (seqlock-style validation).
  std::atomic_thread_fence(std::memory_order_acquire);
  for (size_t page = first; page <= last; ++page) {
    const uint64_t word = states_[page].load(std::memory_order_relaxed);
    if ((word & ~referenceBit) != seen[page - first]) {
      return false;
    }
    if ((word & referenceBit) == 0) {
      states_[page].fetch_or(referenceBit, std::memory_order_relaxed);
    }
  }
  return true;
}

// _____________________________________________________________________________
std::vector<size_t> BufferPool::copyHits(ql::span<const size_t> numBytes,
                                         ql::span<const uint64_t> offsets,
                                         ql::span<char*> buffers,
                                         size_t prefetchDistance) {
  AD_CONTRACT_CHECK(offsets.size() == numBytes.size() &&
                    buffers.size() == numBytes.size());
  AD_CONTRACT_CHECK(isValid());
  const size_t n = numBytes.size();
  for (size_t i = 0; i < n; ++i) {
    AD_CONTRACT_CHECK(offsets[i] <= fileSize_ &&
                      numBytes[i] <= fileSize_ - offsets[i]);
  }

  // Hits: copy from the region, prefetching ahead (see `io/08`).
  std::vector<size_t> pending;
  for (size_t i = 0; i < std::min(prefetchDistance, n); ++i) {
    prefetch(offsets[i], numBytes[i]);
  }
  for (size_t i = 0; i < n; ++i) {
    if (prefetchDistance > 0 && i + prefetchDistance < n) {
      prefetch(offsets[i + prefetchDistance], numBytes[i + prefetchDistance]);
    }
    if (!tryCopy(offsets[i], numBytes[i], buffers[i])) {
      pending.push_back(i);
    }
  }
  hits_.fetch_add(n - pending.size(), std::memory_order_relaxed);
  return pending;
}

// _____________________________________________________________________________
void BufferPool::readAll(ql::span<const size_t> numBytes,
                         ql::span<const uint64_t> offsets,
                         ql::span<char*> buffers, const Loader* loader,
                         size_t prefetchDistance) {
  servePending(copyHits(numBytes, offsets, buffers, prefetchDistance),
               numBytes, offsets, buffers, loader);
  // Keep the budget also when this call loaded more than it allowed.
  evictFor(0);
}

// _____________________________________________________________________________
void BufferPool::servePending(std::vector<size_t> pending,
                              ql::span<const size_t> numBytes,
                              ql::span<const uint64_t> offsets,
                              ql::span<char*> buffers, const Loader* loader) {
  // Misses: load the missing pages of all pending reads, then copy. In the
  // first round only the missing pages are claimed. If a read is still not
  // served after that (because another thread evicted one of its pages in the
  // meantime, e.g. under a tiny budget), later rounds also pin its loaded
  // pages (resident -> loading, without reloading them), so that all pages of
  // the read belong to this thread while it is copied.
  for (size_t round = 0; !pending.empty(); ++round) {
    std::vector<size_t> neededPages;
    for (size_t i : pending) {
      if (numBytes[i] == 0) {
        continue;
      }
      const size_t first = offsets[i] / pageSize_;
      const size_t last = (offsets[i] + numBytes[i] - 1) / pageSize_;
      for (size_t page = first; page <= last; ++page) {
        neededPages.push_back(page);
      }
    }
    std::sort(neededPages.begin(), neededPages.end());
    neededPages.erase(std::unique(neededPages.begin(), neededPages.end()),
                      neededPages.end());

    // Claim the evicted pages (and in later rounds pin the loaded ones);
    // another thread is loading, pinning or evicting the others.
    std::vector<size_t> claimed;
    std::vector<size_t> pinned;
    std::vector<size_t> busy;
    for (size_t page : neededPages) {
      uint64_t word = states_[page].load(std::memory_order_acquire);
      while (true) {
        const uint64_t state = word & stateMask;
        if (state == evicted || (state == resident && round > 0)) {
          const uint64_t ownedWord =
              (word & ~(stateMask | referenceBit | waiterBit)) | loading;
          if (states_[page].compare_exchange_weak(word, ownedWord,
                                                  std::memory_order_acq_rel)) {
            (state == evicted ? claimed : pinned).push_back(page);
            break;
          }
          continue;
        }
        if (state != resident) {
          busy.push_back(page);
        }
        break;
      }
    }

    if (!claimed.empty()) {
      evictFor(claimed.size());
      try {
        loadClaimed(claimed, loader);
      } catch (...) {
        unclaim(claimed);
        unpin(pinned);
        throw;
      }
    }
    // Copy the reads whose pages all belong to this thread: they cannot be
    // evicted, so these copies need no validation.
    std::vector<size_t> owned;
    std::merge(claimed.begin(), claimed.end(), pinned.begin(), pinned.end(),
               std::back_inserter(owned));
    std::vector<size_t> stillPending;
    for (size_t i : pending) {
      bool allOwned = !owned.empty() && numBytes[i] > 0;
      if (allOwned) {
        const size_t first = offsets[i] / pageSize_;
        const size_t last = (offsets[i] + numBytes[i] - 1) / pageSize_;
        for (size_t page = first; page <= last && allOwned; ++page) {
          allOwned = std::binary_search(owned.begin(), owned.end(), page);
        }
      }
      if (allOwned) {
        std::memcpy(buffers[i], region_ + offsets[i], numBytes[i]);
      } else {
        stillPending.push_back(i);
      }
    }
    publish(claimed);
    unpin(pinned);

    // Wait for the pages that other threads are loading, pinning or evicting
    // (after releasing our own, so no two threads wait for each other; this
    // includes the claims of this thread's pending split-phase reads).
    if (!busy.empty()) {
      completeOwnPendingReads();
    }
    for (size_t page : busy) {
      waitWhileBusy(page);
    }

    pending.clear();
    for (size_t i : stillPending) {
      if (!tryCopy(offsets[i], numBytes[i], buffers[i])) {
        pending.push_back(i);
      }
    }
  }
}

// _____________________________________________________________________________
BufferPool::PageRuns BufferPool::runsOf(ql::span<const size_t> pages) const {
  // Adjacent pages are read as one range. An incomplete last page of the file
  // is read separately through the page cache, because an `O_DIRECT` read of
  // a whole page there returns fewer bytes.
  const bool lastPageIsPartial = fileSize_ % pageSize_ != 0;
  PageRuns runs;
  for (size_t k = 0; k < pages.size();) {
    size_t end = k + 1;
    while (end < pages.size() && pages[end] == pages[end - 1] + 1) {
      ++end;
    }
    size_t firstPage = pages[k];
    size_t numRunPages = end - k;
    if (lastPageIsPartial && pages[end - 1] == numPages_ - 1) {
      runs.partialLastPage_ = true;
      --numRunPages;
    }
    if (numRunPages > 0) {
      runs.numBytes_.push_back(numRunPages * pageSize_);
      runs.offsets_.push_back(firstPage * pageSize_);
      runs.targets_.push_back(region_ + firstPage * pageSize_);
    }
    k = end;
  }
  return runs;
}

// _____________________________________________________________________________
void BufferPool::readPartialLastPage() {
  const uint64_t offset = (numPages_ - 1) * pageSize_;
  preadFully(bufferedFd_, region_ + offset, fileSize_ - offset, offset);
}

// _____________________________________________________________________________
void BufferPool::loadClaimed(ql::span<const size_t> pages,
                             const Loader* loader) {
  auto runs = runsOf(pages);
  if (!runs.numBytes_.empty()) {
    if (loader != nullptr) {
      (*loader)(fd_, runs.numBytes_, runs.offsets_, runs.targets_);
    } else {
      loadSynchronously(fd_, runs.numBytes_, runs.offsets_, runs.targets_);
    }
  }
  if (runs.partialLastPage_) {
    readPartialLastPage();
  }
  pageLoads_.fetch_add(pages.size(), std::memory_order_relaxed);
}

// _____________________________________________________________________________
void BufferPool::startReadAll(PendingRead& read,
                              ql::span<const size_t> numBytes,
                              ql::span<const uint64_t> offsets,
                              ql::span<char*> buffers,
                              const AsyncLoader& loader,
                              size_t prefetchDistance) {
  AD_CONTRACT_CHECK(!read.active());
  AD_CONTRACT_CHECK(loader.submit && loader.wait);
  auto pending = copyHits(numBytes, offsets, buffers, prefetchDistance);

  // Claim the evicted pages of the pending reads. Pages that other threads
  // (or other pending reads of this thread) are loading are left to
  // `finishReadAll`.
  std::vector<size_t> neededPages;
  for (size_t i : pending) {
    if (numBytes[i] == 0) {
      continue;
    }
    const size_t first = offsets[i] / pageSize_;
    const size_t last = (offsets[i] + numBytes[i] - 1) / pageSize_;
    for (size_t page = first; page <= last; ++page) {
      neededPages.push_back(page);
    }
  }
  std::sort(neededPages.begin(), neededPages.end());
  neededPages.erase(std::unique(neededPages.begin(), neededPages.end()),
                    neededPages.end());
  std::vector<size_t> claimed;
  for (size_t page : neededPages) {
    uint64_t word = states_[page].load(std::memory_order_acquire);
    while ((word & stateMask) == evicted) {
      const uint64_t ownedWord =
          (word & ~(stateMask | referenceBit | waiterBit)) | loading;
      if (states_[page].compare_exchange_weak(word, ownedWord,
                                              std::memory_order_acq_rel)) {
        claimed.push_back(page);
        break;
      }
    }
  }

  std::optional<uint64_t> token;
  if (!claimed.empty()) {
    evictFor(claimed.size());
    try {
      auto runs = runsOf(claimed);
      if (runs.partialLastPage_) {
        readPartialLastPage();
      }
      if (!runs.numBytes_.empty()) {
        token =
            loader.submit(fd_, runs.numBytes_, runs.offsets_, runs.targets_);
      }
    } catch (...) {
      unclaim(claimed);
      throw;
    }
    pageLoads_.fetch_add(claimed.size(), std::memory_order_relaxed);
  }
  read.pool_ = this;
  read.pending_ = std::move(pending);
  read.claimed_ = std::move(claimed);
  read.token_ = token;
  read.wait_ = loader.wait;
  read.completed_ = false;
  read.error_ = nullptr;
  ownPendingReads.push_back(&read);
}

// _____________________________________________________________________________
void BufferPool::finishReadAll(PendingRead& read,
                               ql::span<const size_t> numBytes,
                               ql::span<const uint64_t> offsets,
                               ql::span<char*> buffers, const Loader* loader) {
  AD_CONTRACT_CHECK(read.pool_ == this);
  read.complete();
  auto error = read.error_;
  auto pending = std::move(read.pending_);
  read.release();
  if (error) {
    std::rethrow_exception(error);
  }
  // The claimed pages are published now: copy the reads that they complete
  // (unless they were evicted again in the meantime) and serve the rest.
  std::vector<size_t> stillPending;
  for (size_t i : pending) {
    if (!tryCopy(offsets[i], numBytes[i], buffers[i])) {
      stillPending.push_back(i);
    }
  }
  servePending(std::move(stillPending), numBytes, offsets, buffers, loader);
  evictFor(0);
}

// _____________________________________________________________________________
void BufferPool::completeOwnPendingReads() noexcept {
  for (auto* read : ownPendingReads) {
    read->complete();
  }
}

// _____________________________________________________________________________
void BufferPool::PendingRead::complete() noexcept {
  if (completed_ || pool_ == nullptr) {
    return;
  }
  completed_ = true;
  try {
    if (token_.has_value()) {
      wait_(token_.value());
    }
    pool_->publish(claimed_);
  } catch (...) {
    error_ = std::current_exception();
    pool_->unclaim(claimed_);
  }
  claimed_.clear();
}

// _____________________________________________________________________________
void BufferPool::PendingRead::release() {
  auto it = std::find(ownPendingReads.begin(), ownPendingReads.end(), this);
  if (it != ownPendingReads.end()) {
    ownPendingReads.erase(it);
  }
  pool_ = nullptr;
  pending_.clear();
  claimed_.clear();
  token_.reset();
  wait_ = nullptr;
  completed_ = false;
  error_ = nullptr;
}

// _____________________________________________________________________________
BufferPool::PendingRead::~PendingRead() {
  if (pool_ != nullptr) {
    complete();
    release();
  }
}

// _____________________________________________________________________________
void BufferPool::publish(ql::span<const size_t> pages) {
  if (pages.empty()) {
    return;
  }
  for (size_t page : pages) {
    const uint64_t old = transition(page, [](uint64_t word) {
      return (word & ~(stateMask | waiterBit)) | resident | referenceBit;
    });
    AD_CORRECTNESS_CHECK((old & stateMask) == loading);
  }
  numLoaded_.fetch_add(pages.size());
  std::lock_guard lock{clockMutex_};
  for (size_t page : pages) {
    clockPages_.push_back(static_cast<uint32_t>(page));
  }
}

// _____________________________________________________________________________
void BufferPool::unpin(ql::span<const size_t> pages) {
  // The pages stayed loaded (and in the CLOCK): only the state goes back.
  for (size_t page : pages) {
    const uint64_t old = transition(page, [](uint64_t word) {
      return (word & ~(stateMask | waiterBit)) | resident | referenceBit;
    });
    AD_CORRECTNESS_CHECK((old & stateMask) == loading);
  }
}

// _____________________________________________________________________________
void BufferPool::unclaim(ql::span<const size_t> pages) {
  // The pages may hold part of a read: discard them like evicted pages.
  // Sleepers keep waiting (the waiter bit stays) until `releasePages`.
  for (size_t page : pages) {
    transition(page, [](uint64_t word) {
      return ((word & ~stateMask) + versionOne) | evicting;
    });
  }
  releasePages({pages.begin(), pages.end()});
}

// _____________________________________________________________________________
void BufferPool::evictFor(size_t incoming) {
  const size_t budget = budgetPages_.load();
  const size_t target = budget > incoming ? budget - incoming : 0;
  evictDownTo(target, std::numeric_limits<size_t>::max());
}

// _____________________________________________________________________________
size_t BufferPool::evictTowardsBudget(size_t maxPages) {
  if (!isValid()) {
    return 0;
  }
  return evictDownTo(budgetPages_.load(), maxPages);
}

// _____________________________________________________________________________
size_t BufferPool::evictDownTo(size_t target, size_t maxVictims) {
  if (numLoaded_.load() <= target || maxVictims == 0) {
    return 0;
  }
  std::vector<size_t> victims;
  {
    std::lock_guard lock{clockMutex_};
    size_t loaded = numLoaded_.load();
    // Two rounds of the hand clear every reference bit once; if nothing can
    // be evicted after that (all pages are being read right now), give up
    // and let the next call try again.
    size_t steps = 2 * clockPages_.size() + 1;
    while (loaded > target && victims.size() < maxVictims &&
           !clockPages_.empty() && steps-- > 0) {
      if (clockHand_ >= clockPages_.size()) {
        clockHand_ = 0;
      }
      const size_t page = clockPages_[clockHand_];
      uint64_t word = states_[page].load(std::memory_order_acquire);
      // A page in the CLOCK is loaded, but a reader may have pinned it right
      // now (see `readAll`): skip it.
      if ((word & stateMask) != resident) {
        ++clockHand_;
        continue;
      }
      if (word & referenceBit) {
        states_[page].compare_exchange_strong(word, word & ~referenceBit,
                                              std::memory_order_relaxed);
        ++clockHand_;
        continue;
      }
      const uint64_t evictedWord =
          ((word & ~(stateMask | referenceBit)) + versionOne) | evicting;
      if (!states_[page].compare_exchange_strong(word, evictedWord,
                                                 std::memory_order_acq_rel)) {
        // A reader set the reference bit: look at this page again.
        continue;
      }
      victims.push_back(page);
      clockPages_[clockHand_] = clockPages_.back();
      clockPages_.pop_back();
      --loaded;
    }
    numLoaded_.fetch_sub(victims.size());
  }
  const size_t numVictims = victims.size();
  evictions_.fetch_add(numVictims, std::memory_order_relaxed);
  if (!victims.empty()) {
    releasePages(std::move(victims));
  }
  return numVictims;
}

// _____________________________________________________________________________
void BufferPool::releasePages(std::vector<size_t> pages) {
  std::sort(pages.begin(), pages.end());
#ifdef QL_BUFFER_POOL
  for (size_t k = 0; k < pages.size();) {
    size_t end = k + 1;
    while (end < pages.size() && pages[end] == pages[end - 1] + 1) {
      ++end;
    }
    ::madvise(region_ + pages[k] * pageSize_, (end - k) * pageSize_,
              MADV_DONTNEED);
    k = end;
  }
#endif
  // Only now may another thread load these pages again.
  for (size_t page : pages) {
    const uint64_t old = transition(page, [](uint64_t word) {
      return (word & ~(stateMask | waiterBit)) | evicted;
    });
    AD_CORRECTNESS_CHECK((old & stateMask) == evicting);
  }
}

}  // namespace ad_utility
