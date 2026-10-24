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

#if defined(__linux__) && !defined(__EMSCRIPTEN__)
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
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

// The OS page size, if the architecture fixes it at compile time (x86 uses
// 4096-byte base pages with every kernel). Zero means: the kernel chooses at
// boot time (e.g. ARM with 4/16/64 KiB pages), so it must be queried.
#if defined(__x86_64__) || defined(__i386__)
constexpr size_t knownHostPageSize = 4096;
#else
constexpr size_t knownHostPageSize = 0;
#endif
static_assert(knownHostPageSize == 0 ||
                  (knownHostPageSize & (knownHostPageSize - 1)) == 0,
              "the known host page size must be a power of two");

// The binary logarithm of a power of two (0 for 0).
constexpr size_t log2OfPowerOfTwo(size_t value) {
  size_t result = 0;
  while ((size_t{1} << result) < value) {
    ++result;
  }
  return result;
}

// Wait until another thread has finished loading or evicting a page.
void pause(size_t& spins) {
  if (++spins < 64) {
#if defined(__x86_64__)
    __builtin_ia32_pause();
#endif
  } else {
    std::this_thread::yield();
  }
}
}  // namespace

// _____________________________________________________________________________
size_t BufferPool::hostPageSize() {
  if constexpr (knownHostPageSize != 0) {
    return knownHostPageSize;
  }
#ifdef QL_BUFFER_POOL
  // Query once: the kernel's page size cannot change while we run. A failed
  // or implausible answer yields 0, which makes every pool invalid instead
  // of guessing (guessing is the bug this function exists to prevent).
  static const size_t size = [] {
    const long queried = ::sysconf(_SC_PAGESIZE);
    if (queried <= 0) {
      return size_t{0};
    }
    const size_t result = static_cast<size_t>(queried);
    return (result & (result - 1)) == 0 ? result : size_t{0};
  }();
  return size;
#else
  return 0;
#endif
}

// _____________________________________________________________________________
BufferPool::BufferPool([[maybe_unused]] const std::string& filename,
                       size_t fileSize, size_t budgetBytes)
    : pageSize_{hostPageSize()},
      pageShift_{log2OfPowerOfTwo(pageSize_)},
      budgetPages_{pageSize_ == 0 ? 0 : budgetBytes / pageSize_} {
  // Existence means usable: construction either establishes every invariant
  // (file open, region reserved) or throws. There is no half-valid pool
  // whose reads fail later; callers that want to continue without a pool
  // catch the exception where they create it.
  AD_CONTRACT_CHECK(fileSize > 0);
#ifdef QL_BUFFER_POOL
  if (pageSize_ == 0) {
    throw std::runtime_error{
        "BufferPool: could not determine the OS page size"};
  }
  numPages_ = (fileSize + pageSize_ - 1) >> pageShift_;
  AD_CONTRACT_CHECK(numPages_ <= std::numeric_limits<uint32_t>::max());
  // Allocate the states before opening files or reserving the region: if the
  // allocation throws, nothing exists yet that could leak (the destructor
  // does not run when a constructor throws).
  states_ = std::make_unique<std::atomic<uint64_t>[]>(numPages_);
  for (size_t i = 0; i < numPages_; ++i) {
    states_[i].store(evicted, std::memory_order_relaxed);
  }
  bufferedFd_ = ::open(filename.c_str(), O_RDONLY | O_CLOEXEC);
  if (bufferedFd_ < 0) {
    throw std::runtime_error{absl::StrCat(
        "BufferPool: could not open ", filename, ": ", std::strerror(errno))};
  }
  fd_ = ::open(filename.c_str(), O_RDONLY | O_CLOEXEC | O_DIRECT);
  directIo_ = fd_ >= 0;
  if (!directIo_) {
    // E.g. tmpfs: read through the page cache instead.
    fd_ = ::dup(bufferedFd_);
  }
  regionSize_ = numPages_ << pageShift_;
  void* region = ::mmap(nullptr, regionSize_, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (region == MAP_FAILED || fd_ < 0) {
    // Members with plain descriptors do not clean up when a constructor
    // throws, so release what was acquired before reporting the failure.
    const int error = errno;
    if (region != MAP_FAILED) {
      ::munmap(region, regionSize_);
    }
    if (fd_ >= 0) ::close(fd_);
    ::close(bufferedFd_);
    fd_ = -1;
    bufferedFd_ = -1;
    throw std::runtime_error{absl::StrCat("BufferPool: could not reserve ",
                                          regionSize_, " bytes for ", filename,
                                          ": ", std::strerror(error))};
  }
  region_ = static_cast<char*>(region);
  fileSize_ = fileSize;
#else
  throw std::runtime_error{"BufferPool is not supported on this platform"};
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
  budgetPages_.store(pageSize_ == 0 ? 0 : budgetBytes >> pageShift_);
}

// _____________________________________________________________________________
bool BufferPool::tryCopy(uint64_t offset, size_t numBytes, char* target) {
  if (numBytes == 0) {
    return true;
  }
  const size_t first = firstPageOf(offset);
  const size_t last = lastPageOf(offset, numBytes);
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
void BufferPool::readAll(ql::span<const size_t> numBytes,
                         ql::span<const uint64_t> offsets,
                         ql::span<char*> buffers, const Loader* loader,
                         size_t prefetchDistance) {
  AD_CONTRACT_CHECK(offsets.size() == numBytes.size() &&
                    buffers.size() == numBytes.size());
  AD_CORRECTNESS_CHECK(region_ != nullptr);
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
      const size_t first = firstPageOf(offsets[i]);
      const size_t last = lastPageOf(offsets[i], numBytes[i]);
      for (size_t page = first; page <= last; ++page) {
        neededPages.push_back(page);
      }
    }
    std::sort(neededPages.begin(), neededPages.end());
    neededPages.erase(std::unique(neededPages.begin(), neededPages.end()),
                      neededPages.end());

    // Claim the evicted pages (and in later rounds pin the loaded ones);
    // another thread is loading, pinning or evicting the others. Reserve
    // all ownership vectors up front: once a compare-and-swap below has
    // claimed a page, recording it must not allocate (and thus not throw),
    // or the page would stay in state `loading` forever, owned by nobody.
    std::vector<size_t> claimed;
    std::vector<size_t> pinned;
    std::vector<size_t> busy;
    claimed.reserve(neededPages.size());
    pinned.reserve(neededPages.size());
    busy.reserve(neededPages.size());
    for (size_t page : neededPages) {
      uint64_t word = states_[page].load(std::memory_order_acquire);
      while (true) {
        const uint64_t state = word & stateMask;
        if (state == evicted || (state == resident && round > 0)) {
          const uint64_t ownedWord =
              (word & ~(stateMask | referenceBit)) | loading;
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

    // Everything from evicting for the claimed pages to publishing them can
    // throw (allocation, I/O). Until `publish` has run, this thread owns the
    // claimed and pinned pages, so any failure must release exactly them:
    // a page left in state `loading` would block every later reader.
    std::vector<size_t> stillPending;
    bool published = false;
    try {
      if (!claimed.empty()) {
        evictFor(claimed.size());
        loadClaimed(claimed, loader);
      }
      // Copy the reads whose pages all belong to this thread: they cannot be
      // evicted, so these copies need no validation.
      std::vector<size_t> owned;
      std::merge(claimed.begin(), claimed.end(), pinned.begin(), pinned.end(),
                 std::back_inserter(owned));
      for (size_t i : pending) {
        bool allOwned = !owned.empty() && numBytes[i] > 0;
        if (allOwned) {
          const size_t first = firstPageOf(offsets[i]);
          const size_t last = lastPageOf(offsets[i], numBytes[i]);
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
      published = true;
      unpin(pinned);
    } catch (...) {
      if (!published) {
        // Release both sets even if one release reports an invariant failure
        // (it completes its state transitions first): a secondary failure
        // must not strand the other set. The original exception wins.
        try {
          unclaim(claimed);
        } catch (...) {
        }
        try {
          unpin(pinned);
        } catch (...) {
        }
      }
      throw;
    }

    // Wait for the pages that other threads are loading, pinning or evicting
    // (after releasing our own, so no two threads wait for each other).
    for (size_t page : busy) {
      size_t spins = 0;
      while (true) {
        const uint64_t state =
            states_[page].load(std::memory_order_acquire) & stateMask;
        if (state == resident || state == evicted) {
          break;
        }
        pause(spins);
      }
    }

    pending.clear();
    for (size_t i : stillPending) {
      if (!tryCopy(offsets[i], numBytes[i], buffers[i])) {
        pending.push_back(i);
      }
    }
  }
  // Keep the budget also when this call loaded more than it allowed.
  evictFor(0);
}

// _____________________________________________________________________________
void BufferPool::loadClaimed(ql::span<const size_t> pages,
                             const Loader* loader) {
  // Adjacent pages are read as one range. An incomplete last page of the file
  // is read separately through the page cache, because an `O_DIRECT` read of
  // a whole page there returns fewer bytes.
  const bool lastPageIsPartial = fileSize_ % pageSize_ != 0;
  std::vector<size_t> runBytes;
  std::vector<uint64_t> runOffsets;
  std::vector<char*> runTargets;
  bool readPartialLastPage = false;
  for (size_t k = 0; k < pages.size();) {
    size_t end = k + 1;
    while (end < pages.size() && pages[end] == pages[end - 1] + 1) {
      ++end;
    }
    size_t firstPage = pages[k];
    size_t numRunPages = end - k;
    if (lastPageIsPartial && pages[end - 1] == numPages_ - 1) {
      readPartialLastPage = true;
      --numRunPages;
    }
    if (numRunPages > 0) {
      runBytes.push_back(numRunPages << pageShift_);
      runOffsets.push_back(byteOffsetOfPage(firstPage));
      runTargets.push_back(region_ + byteOffsetOfPage(firstPage));
    }
    k = end;
  }
  if (!runBytes.empty()) {
    if (loader != nullptr) {
      (*loader)(fd_, runBytes, runOffsets, runTargets);
    } else {
      loadSynchronously(fd_, runBytes, runOffsets, runTargets);
    }
  }
  if (readPartialLastPage) {
    const uint64_t offset = byteOffsetOfPage(numPages_ - 1);
    preadFully(bufferedFd_, region_ + offset, fileSize_ - offset, offset);
  }
  pageLoads_.fetch_add(pages.size(), std::memory_order_relaxed);
}

// _____________________________________________________________________________
void BufferPool::publish(ql::span<const size_t> pages) {
  if (pages.empty()) {
    return;
  }
  // Grow the CLOCK first, while the pages are still in state `loading`: if
  // the allocation throws, the caller releases the pages as if the load had
  // failed. Hold the lock from the reserve to the last append, so no other
  // publisher can consume the reserved capacity in between; after the
  // reserve nothing here allocates. A page thus never becomes resident
  // without a CLOCK entry (it would never be evictable again). The hit path
  // does not take this lock: it may see a page resident before its CLOCK
  // entry, which is fine because eviction is blocked on the lock meanwhile.
  std::lock_guard lock{clockMutex_};
  clockPages_.reserve(clockPages_.size() + pages.size());
  // Check all pages before publishing any of them, so a failed check leaves
  // every page in `loading` for the caller's rollback instead of a mix.
  for (size_t page : pages) {
    const uint64_t word = states_[page].load(std::memory_order_relaxed);
    AD_CORRECTNESS_CHECK((word & stateMask) == loading);
  }
  for (size_t page : pages) {
    const uint64_t word = states_[page].load(std::memory_order_relaxed);
    states_[page].store((word & ~stateMask) | resident | referenceBit,
                        std::memory_order_release);
  }
  numLoaded_.fetch_add(pages.size());
  for (size_t page : pages) {
    clockPages_.push_back(static_cast<uint32_t>(page));
  }
}

// _____________________________________________________________________________
void BufferPool::unpin(ql::span<const size_t> pages) {
  // The pages stayed loaded (and in the CLOCK): only the state goes back.
  // Like `releasePages`, complete every transition before checking, so an
  // invariant failure cannot strand the remaining pages in `loading`.
  bool allLoading = true;
  for (size_t page : pages) {
    const uint64_t word = states_[page].load(std::memory_order_relaxed);
    allLoading &= (word & stateMask) == loading;
    states_[page].store((word & ~stateMask) | resident | referenceBit,
                        std::memory_order_release);
  }
  AD_CORRECTNESS_CHECK(allLoading,
                       "BufferPool: unpinning a page that is not pinned");
}

// _____________________________________________________________________________
void BufferPool::unclaim(ql::span<const size_t> pages) {
  // The pages may hold part of a read: discard them like evicted pages.
  for (size_t page : pages) {
    const uint64_t word = states_[page].load(std::memory_order_relaxed);
    states_[page].store(((word & ~stateMask) + versionOne) | evicting,
                        std::memory_order_release);
  }
  // `pages` comes from `readAll`, where the claimed pages are a sorted,
  // unique subsequence of the sorted needed pages.
  releasePages(pages);
}

// _____________________________________________________________________________
void BufferPool::evictFor(size_t incoming) {
  const size_t budget = budgetPages_.load();
  const size_t target = budget > incoming ? budget - incoming : 0;
  evictDownTo(target, std::numeric_limits<size_t>::max());
}

// _____________________________________________________________________________
size_t BufferPool::evictTowardsBudget(size_t maxPages) {
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
    // Reserve before flipping any page to `evicting`: if recording a victim
    // could still allocate (and throw), that victim would be removed from
    // the CLOCK, stuck in `evicting`, and never released. Each victim is
    // popped from `clockPages_`, so its size bounds the victims.
    victims.reserve(clockPages_.size());
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
    std::sort(victims.begin(), victims.end());
    releasePages(victims);
  }
  return numVictims;
}

// _____________________________________________________________________________
void BufferPool::releasePages(ql::span<const size_t> pages) {
  // This runs on exception-unwind paths (`unclaim`), where the pages are
  // already in state `evicting`: every state transition below must complete
  // before anything may throw, or the remaining pages would stay `evicting`
  // forever, owned by nobody. So: try every `madvise`, remember failures,
  // transition every page, and only then report. (Sorted input only affects
  // how well adjacent pages merge into one `madvise`; correctness of the
  // transitions does not depend on it.)
  [[maybe_unused]] int madviseErrno = 0;
#ifdef QL_BUFFER_POOL
  for (size_t k = 0; k < pages.size();) {
    size_t end = k + 1;
    while (end < pages.size() && pages[end] == pages[end - 1] + 1) {
      ++end;
    }
    // `pages` are OS pages (`pageSize_` is the OS page size, and `region_` is
    // OS-page aligned), so every range is aligned and the call discards
    // exactly these pages. A failure would mean that this invariant does not
    // hold on this system: fail loudly (below, after the transitions)
    // instead of silently keeping (or, on a mismatched page size,
    // corrupting) cached bytes.
    if (::madvise(region_ + byteOffsetOfPage(pages[k]), (end - k) << pageShift_,
                  MADV_DONTNEED) != 0 &&
        madviseErrno == 0) {
      madviseErrno = errno;
    }
    k = end;
  }
#endif
  // Only now may another thread load these pages again.
  bool allEvicting = true;
  for (size_t page : pages) {
    const uint64_t word = states_[page].load(std::memory_order_relaxed);
    allEvicting &= (word & stateMask) == evicting;
    states_[page].store((word & ~stateMask) | evicted,
                        std::memory_order_release);
  }
  AD_CORRECTNESS_CHECK(allEvicting,
                       "BufferPool: releasing a page that is not evicting");
  AD_CORRECTNESS_CHECK(madviseErrno == 0,
                       "BufferPool: madvise(MADV_DONTNEED) failed: ",
                       std::strerror(madviseErrno));
}

}  // namespace ad_utility
