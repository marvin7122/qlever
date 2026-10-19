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
#include <chrono>
#include <cstring>
#include <iterator>
#include <optional>
#include <limits>
#include <thread>
#include <utility>

#include "util/Exception.h"

#if defined(__linux__) && !defined(__EMSCRIPTEN__)
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#define QL_BUFFER_POOL
// `MADV_POPULATE_WRITE` (Linux 5.14) is missing in older headers.
#ifndef MADV_POPULATE_WRITE
#define MADV_POPULATE_WRITE 23
#endif
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
BufferPool::BufferPool([[maybe_unused]] const std::string& filename,
                       size_t fileSize, size_t budgetBytes, size_t pageSize,
                       size_t numFramePartitions)
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
  if (fd_ < 0) {
    return;
  }
  if (numFramePartitions > 0) {
    // Fixed frames: the budget in whole frames per partition (a frame number
    // must fit into the 32 bits of the state word).
    numFramePartitions_ = numFramePartitions;
    framesPerPartition_ = std::max(
        minFramesPerPartition,
        std::min(budgetPages_.load(), numPages_) / numFramePartitions);
    numFrames_ = framesPerPartition_ * numFramePartitions;
    AD_CONTRACT_CHECK(numFrames_ <= std::numeric_limits<uint32_t>::max());
    framesPerChunk_ = std::max<size_t>(1, maxFixedBufferBytes / pageSize_);
    void* frames = ::mmap(nullptr, numFrames_ * pageSize_,
                          PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS,
                          -1, 0);
    if (frames == MAP_FAILED) {
      return;
    }
    frames_ = static_cast<char*>(frames);
    prefaultFrames();
    // Hand out the low frames of each partition first, so that consecutive
    // pages get consecutive frames.
    freeFrames_ = std::make_unique<FreeFrames[]>(numFramePartitions_);
    for (size_t p = 0; p < numFramePartitions_; ++p) {
      auto& free = freeFrames_[p].frames_;
      free.resize(framesPerPartition_);
      for (size_t i = 0; i < framesPerPartition_; ++i) {
        free[i] = static_cast<uint32_t>((p + 1) * framesPerPartition_ - 1 - i);
      }
    }
    versionMask_ = fixedVersionMask;
    budgetPages_ = std::min(budgetPages_.load(), numFrames_);
  } else {
    regionSize_ = numPages_ * pageSize_;
    void* region = ::mmap(nullptr, regionSize_, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (region == MAP_FAILED) {
      return;
    }
    region_ = static_cast<char*>(region);
  }
  fileSize_ = fileSize;
  valid_ = true;
#else
  (void)fileSize;
  (void)numFramePartitions;
#endif
}

// _____________________________________________________________________________
void BufferPool::prefaultFrames() {
#ifdef QL_BUFFER_POOL
  // Fault in every frame now (allocate, zero, map), in parallel slices of
  // whole pages. `MADV_POPULATE_WRITE` does this in one call per slice; where
  // the kernel lacks it, touch one byte per page instead.
  const auto start = std::chrono::steady_clock::now();
  const size_t bytes = numFrames_ * pageSize_;
  constexpr size_t osPage = 4096;
  constexpr size_t minBytesPerThread = size_t{64} << 20;
  const size_t numThreads = std::clamp<size_t>(
      bytes / minBytesPerThread, 1,
      std::clamp<size_t>(std::thread::hardware_concurrency(), 1, 16));
  const size_t numOsPages = bytes / osPage;
  auto populate = [this, numOsPages, numThreads](size_t t) {
    const size_t begin = numOsPages * t / numThreads * osPage;
    const size_t end = numOsPages * (t + 1) / numThreads * osPage;
    if (end == begin) {
      return;
    }
    if (::madvise(frames_ + begin, end - begin, MADV_POPULATE_WRITE) == 0) {
      return;
    }
    for (size_t offset = begin; offset < end; offset += osPage) {
      *static_cast<volatile char*>(frames_ + offset) = 0;
    }
  };
  std::vector<std::thread> threads;
  for (size_t t = 1; t < numThreads; ++t) {
    threads.emplace_back(populate, t);
  }
  populate(0);
  for (auto& thread : threads) {
    thread.join();
  }
  prefaultThreads_ = numThreads;
  prefaultSeconds_ = std::chrono::duration<double>(
                         std::chrono::steady_clock::now() - start)
                         .count();
#endif
}

// _____________________________________________________________________________
std::vector<std::pair<char*, size_t>> BufferPool::fixedBufferIovecs(
    size_t partition) const {
  std::vector<std::pair<char*, size_t>> result;
  if (!usesFixedFrames()) {
    return result;
  }
  AD_CONTRACT_CHECK(partition < numFramePartitions_);
  const size_t firstFrame = partition * framesPerPartition_;
  for (size_t frame = 0; frame < framesPerPartition_;
       frame += framesPerChunk_) {
    const size_t n = std::min(framesPerChunk_, framesPerPartition_ - frame);
    result.emplace_back(frames_ + (firstFrame + frame) * pageSize_,
                        n * pageSize_);
  }
  return result;
}

// _____________________________________________________________________________
BufferPool::~BufferPool() {
#ifdef QL_BUFFER_POOL
  if (region_ != nullptr) {
    ::munmap(region_, regionSize_);
  }
  if (frames_ != nullptr) {
    ::munmap(frames_, numFrames_ * pageSize_);
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
  size_t pages = budgetBytes / pageSize_;
  if (usesFixedFrames()) {
    pages = std::min(pages, numFrames_);
  }
  budgetPages_.store(pages);
}

// _____________________________________________________________________________
void BufferPool::copyFromPages(const uint64_t* words, uint64_t offset,
                               size_t numBytes, char* target) const {
  if (!usesFixedFrames()) {
    std::memcpy(target, region_ + offset, numBytes);
    return;
  }
  // Page by page, each from its frame.
  size_t done = 0;
  for (size_t k = 0; done < numBytes; ++k) {
    const uint64_t position = offset + done;
    const size_t inPage = position % pageSize_;
    const size_t n = std::min(numBytes - done, pageSize_ - inPage);
    std::memcpy(target + done, frameAddress(words[k]) + inPage, n);
    done += n;
  }
}

// _____________________________________________________________________________
void BufferPool::copyOwned(uint64_t offset, size_t numBytes,
                           char* target) const {
  if (!usesFixedFrames()) {
    std::memcpy(target, region_ + offset, numBytes);
    return;
  }
  size_t done = 0;
  while (done < numBytes) {
    const uint64_t position = offset + done;
    const size_t inPage = position % pageSize_;
    const size_t n = std::min(numBytes - done, pageSize_ - inPage);
    const uint64_t word =
        states_[position / pageSize_].load(std::memory_order_relaxed);
    std::memcpy(target + done, frameAddress(word) + inPage, n);
    done += n;
  }
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
  copyFromPages(seen, offset, numBytes, target);
  // The copy is valid if no page was evicted in the meantime: an eviction
  // changes the state and the version (seqlock-style validation). With fixed
  // frames, a frame is reused only after its page was evicted, so a copy
  // from a reused frame also fails this check.
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
                         size_t prefetchDistance, size_t partition) {
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
  const bool fixed = usesFixedFrames();
  if (fixed) {
    partition %= numFramePartitions_;
  }
  // With fixed frames: the state words `prefetchDistance` reads ahead, the
  // bytes half as far ahead.
  const size_t frameDistance = prefetchDistance / 2;
  for (size_t i = 0; i < std::min(prefetchDistance, n); ++i) {
    if (fixed) {
      prefetchState(offsets[i], numBytes[i]);
    } else {
      prefetch(offsets[i], numBytes[i]);
    }
  }
  for (size_t i = 0; fixed && i < std::min(frameDistance, n); ++i) {
    prefetchFrame(offsets[i], numBytes[i]);
  }
  for (size_t i = 0; i < n; ++i) {
    if (prefetchDistance > 0 && i + prefetchDistance < n) {
      if (fixed) {
        prefetchState(offsets[i + prefetchDistance],
                      numBytes[i + prefetchDistance]);
      } else {
        prefetch(offsets[i + prefetchDistance],
                 numBytes[i + prefetchDistance]);
      }
    }
    if (fixed && frameDistance > 0 && i + frameDistance < n) {
      prefetchFrame(offsets[i + frameDistance], numBytes[i + frameDistance]);
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

    // With fixed frames: whether a claimed page found no frame in this
    // round. The reads that are not served then are read directly.
    bool framesExhausted = false;
    if (!claimed.empty()) {
      evictFor(claimed.size());
      std::vector<size_t> withoutFrame;
      try {
        if (fixed) {
          withoutFrame = loadClaimedIntoFrames(claimed, loader, partition);
        } else {
          loadClaimed(claimed, loader);
        }
      } catch (...) {
        unclaim(claimed);
        unpin(pinned);
        throw;
      }
      if (!withoutFrame.empty()) {
        framesExhausted = true;
        unclaim(withoutFrame);
        claimed.resize(claimed.size() - withoutFrame.size());
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
        copyOwned(offsets[i], numBytes[i], buffers[i]);
      } else {
        stillPending.push_back(i);
      }
    }
    publish(claimed);
    unpin(pinned);

    // No free frame: read the remaining reads from the file, past the pool
    // (only with fixed frames, when all frames are being loaded or read).
    if (framesExhausted) {
      for (size_t i : stillPending) {
        preadFully(bufferedFd_, buffers[i], numBytes[i], offsets[i]);
      }
      bypassedReads_.fetch_add(stillPending.size(), std::memory_order_relaxed);
      break;
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
      runBytes.push_back(numRunPages * pageSize_);
      runOffsets.push_back(firstPage * pageSize_);
      runTargets.push_back(region_ + firstPage * pageSize_);
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
    const uint64_t offset = (numPages_ - 1) * pageSize_;
    preadFully(bufferedFd_, region_ + offset, fileSize_ - offset, offset);
  }
  pageLoads_.fetch_add(pages.size(), std::memory_order_relaxed);
}

// _____________________________________________________________________________
void BufferPool::takeFreeFrames(size_t n, size_t partition,
                                std::vector<uint32_t>& out) {
  for (size_t k = 0; k < numFramePartitions_ && out.size() < n; ++k) {
    auto& free = freeFrames_[(partition + k) % numFramePartitions_];
    std::lock_guard lock{free.mutex_};
    while (!free.frames_.empty() && out.size() < n) {
      out.push_back(free.frames_.back());
      free.frames_.pop_back();
    }
  }
}

// _____________________________________________________________________________
std::vector<uint32_t> BufferPool::takeFrames(size_t n, size_t partition) {
  std::vector<uint32_t> frames;
  frames.reserve(n);
  takeFreeFrames(n, partition, frames);
  if (frames.size() < n) {
    // Evict as many loaded pages as frames are missing (their frames go back
    // to their partitions), then try again.
    const size_t missing = n - frames.size();
    const size_t loaded = numLoaded_.load();
    evictDownTo(loaded > missing ? loaded - missing : 0, missing);
    takeFreeFrames(n, partition, frames);
  }
  return frames;
}

// _____________________________________________________________________________
std::vector<size_t> BufferPool::loadClaimedIntoFrames(
    ql::span<const size_t> pages, const Loader* loader, size_t partition) {
  auto frames = takeFrames(pages.size(), partition);
  const size_t numLoading = frames.size();
  std::vector<size_t> withoutFrame(pages.begin() + numLoading, pages.end());
  // Record the frame of each page in its state word (the page is claimed by
  // this thread, so only a reader's reference bit can change concurrently).
  constexpr uint64_t frameMask = ~uint64_t{0} << frameShift;
  for (size_t k = 0; k < numLoading; ++k) {
    uint64_t word = states_[pages[k]].load(std::memory_order_relaxed);
    while (!states_[pages[k]].compare_exchange_weak(
        word, (word & ~frameMask) | (uint64_t{frames[k]} << frameShift),
        std::memory_order_relaxed)) {
    }
  }
  // Adjacent pages in consecutive frames of the same registered buffer are
  // read as one range; the incomplete last page of the file is read through
  // the page cache (see `loadClaimed`).
  auto chunkOf = [this](uint32_t frame) {
    return frame / framesPerPartition_ * framesPerPartition_ +
           frame % framesPerPartition_ / framesPerChunk_;
  };
  const bool lastPageIsPartial = fileSize_ % pageSize_ != 0;
  auto isPartialLastPage = [&](size_t page) {
    return lastPageIsPartial && page == numPages_ - 1;
  };
  std::vector<size_t> runBytes;
  std::vector<uint64_t> runOffsets;
  std::vector<char*> runTargets;
  std::optional<size_t> partialLastPage;
  for (size_t k = 0; k < numLoading;) {
    if (isPartialLastPage(pages[k])) {
      partialLastPage = k;
      ++k;
      continue;
    }
    size_t end = k + 1;
    while (end < numLoading && pages[end] == pages[end - 1] + 1 &&
           frames[end] == frames[end - 1] + 1 &&
           chunkOf(frames[end]) == chunkOf(frames[k]) &&
           !isPartialLastPage(pages[end])) {
      ++end;
    }
    runBytes.push_back((end - k) * pageSize_);
    runOffsets.push_back(pages[k] * pageSize_);
    runTargets.push_back(frames_ + size_t{frames[k]} * pageSize_);
    k = end;
  }
  // If a load fails, the frames are not reused (a read of the batch may still
  // be in flight into them); the caller unclaims the pages.
  if (!runBytes.empty()) {
    if (loader != nullptr) {
      (*loader)(fd_, runBytes, runOffsets, runTargets);
    } else {
      loadSynchronously(fd_, runBytes, runOffsets, runTargets);
    }
  }
  if (partialLastPage.has_value()) {
    const uint64_t offset = (numPages_ - 1) * pageSize_;
    preadFully(bufferedFd_,
               frames_ + size_t{frames[partialLastPage.value()]} * pageSize_,
               fileSize_ - offset, offset);
  }
  pageLoads_.fetch_add(numLoading, std::memory_order_relaxed);
  return withoutFrame;
}

// _____________________________________________________________________________
void BufferPool::publish(ql::span<const size_t> pages) {
  if (pages.empty()) {
    return;
  }
  for (size_t page : pages) {
    const uint64_t word = states_[page].load(std::memory_order_relaxed);
    AD_CORRECTNESS_CHECK((word & stateMask) == loading);
    states_[page].store((word & ~stateMask) | resident | referenceBit,
                        std::memory_order_release);
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
    const uint64_t word = states_[page].load(std::memory_order_relaxed);
    AD_CORRECTNESS_CHECK((word & stateMask) == loading);
    states_[page].store((word & ~stateMask) | resident | referenceBit,
                        std::memory_order_release);
  }
}

// _____________________________________________________________________________
void BufferPool::unclaim(ql::span<const size_t> pages) {
  // The pages may hold part of a read: discard them like evicted pages.
  for (size_t page : pages) {
    const uint64_t word = states_[page].load(std::memory_order_relaxed);
    states_[page].store((nextVersion(word) & ~stateMask) | evicting,
                        std::memory_order_release);
  }
  if (usesFixedFrames()) {
    // The frames are not given back: after a failed load, a read may still be
    // in flight into them (and a page without a frame has none).
    for (size_t page : pages) {
      const uint64_t word = states_[page].load(std::memory_order_relaxed);
      states_[page].store((word & ~stateMask) | evicted,
                          std::memory_order_release);
    }
    return;
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
          (nextVersion(word) & ~(stateMask | referenceBit)) | evicting;
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
  if (usesFixedFrames()) {
    // The memory stays: the frames go back to their partitions for reuse
    // (pushed in descending order, so that runs of consecutive frames are
    // handed out in ascending order again).
    std::vector<std::vector<uint32_t>> freed(numFramePartitions_);
    for (size_t page : pages) {
      const auto frame = static_cast<uint32_t>(
          states_[page].load(std::memory_order_relaxed) >> frameShift);
      freed[frame / framesPerPartition_].push_back(frame);
    }
    for (size_t p = 0; p < numFramePartitions_; ++p) {
      if (freed[p].empty()) {
        continue;
      }
      std::sort(freed[p].begin(), freed[p].end(), std::greater<>{});
      std::lock_guard lock{freeFrames_[p].mutex_};
      auto& free = freeFrames_[p].frames_;
      free.insert(free.end(), freed[p].begin(), freed[p].end());
    }
  }
#ifdef QL_BUFFER_POOL
  for (size_t k = 0; !usesFixedFrames() && k < pages.size();) {
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
    const uint64_t word = states_[page].load(std::memory_order_relaxed);
    AD_CORRECTNESS_CHECK((word & stateMask) == evicting);
    states_[page].store((word & ~stateMask) | evicted,
                        std::memory_order_release);
  }
}

}  // namespace ad_utility
