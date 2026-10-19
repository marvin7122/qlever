// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BUFFERPOOL_H
#define QLEVER_SRC_UTIL_BUFFERPOOL_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "backports/span.h"

namespace ad_utility {

// A buffer pool for one immutable, read-only file, in the style of vmcache
// (Leis et al., "Virtual-Memory Assisted Buffer Management", SIGMOD 2023).
//
// Purpose: reading many small, scattered ranges of a large file. A read whose
// pages are in the pool is a plain memory copy (no system call, no lock), as
// with a memory mapping of the file. Unlike a file mapping, the memory is
// anonymous memory owned by this process and bounded by an explicit budget:
// the kernel cannot evict a cached page behind our back (no hidden major page
// faults under memory pressure), and the pool decides what to evict (CLOCK).
// Missing pages are read in batches, by default with `O_DIRECT` (no second
// copy in the OS page cache).
//
// Layout: one anonymous mapping of the size of the file (rounded up to whole
// pages) is reserved with `MAP_NORESERVE`; page `p` of the file lives at
// `region + p * pageSize`, so no translation table is needed. Reserving the
// addresses costs no RAM; RAM is used only for pages that are loaded, and
// `madvise(MADV_DONTNEED)` returns it when a page is evicted.
//
// Fixed frames (optional, see the constructor): instead of the region above,
// the pool owns an arena of frames of `pageSize` bytes that is faulted in once
// when the pool is created and never returned to the kernel. A loaded page
// lives in some frame (its number is part of the page's state word), an
// evicted page's frame is reused for the next page. The frames are split into
// `numFramePartitions` partitions, one per io_uring ring, so that each ring
// can register its own partition as fixed buffers (`fixedBufferIovecs`,
// `IORING_REGISTER_BUFFERS`): a read into a registered frame then needs
// neither a page fault nor pinning of the target pages. A read that names a
// partition takes its frames from that partition first.
//
// Every page has one atomic state word: its state (evicted, loading,
// resident), a CLOCK reference bit, and a version that changes whenever the
// page is evicted (with fixed frames: the version in bits 3-31 and the frame
// in bits 32-63). A reader copies a resident page optimistically and then
// checks that the state and version have not changed (the file is immutable,
// so only a concurrent eviction can invalidate the copy; then the read is
// retried).
//
// Thread safety: `readAll`, `setBudget` and the statistics may be called
// concurrently from any number of threads.
class BufferPool {
 public:
  // The default page size. Every page size is a power of two and a multiple
  // of it, so that the pages are aligned as `O_DIRECT` requires.
  static constexpr size_t defaultPageSize = 4096;

  // The size of the pages of this pool (the unit of loading, caching and
  // eviction).
  size_t pageSize() const { return pageSize_; }

  // Reads the page-aligned ranges `[offsets[i], offsets[i] + numBytes[i])` of
  // the file into `targets[i]` (page-aligned, `numBytes[i]` a multiple of
  // `pageSize()`, all within the file) from the descriptor `fd`, and returns
  // only when all of them are complete. Must throw on an error.
  using Loader = std::function<void(int fd, ql::span<const size_t> numBytes,
                                    ql::span<const uint64_t> offsets,
                                    ql::span<char*> targets)>;

  // A pool for the file `filename`, of which the first `fileSize` bytes are
  // read. `budgetBytes` is the maximal size of the loaded pages (see
  // `setBudget`). `pageSize` must be a power of two and a multiple of
  // `defaultPageSize`; larger pages mean fewer, larger reads (like readahead)
  // but more bytes read for scattered ranges. The file is opened with
  // `O_DIRECT` if the file system supports it, otherwise with the page cache.
  //
  // If `numFramePartitions` is positive, the pool uses fixed frames (see
  // above): `budgetBytes` (rounded down to whole frames per partition, but at
  // least `minFramesPerPartition` frames per partition) is allocated and
  // faulted in right here, and the budget can later only be lowered.
  BufferPool(const std::string& filename, size_t fileSize, size_t budgetBytes,
             size_t pageSize = defaultPageSize, size_t numFramePartitions = 0);

  // The minimal number of frames per partition (so that a tiny budget still
  // leaves frames to load into).
  static constexpr size_t minFramesPerPartition = 16;

  // The largest registered buffer that the kernel accepts
  // (`io_validate_user_buf_range` in `io_uring/rsrc.c`: at most 1 GiB).
  static constexpr size_t maxFixedBufferBytes = size_t{1} << 30;
  ~BufferPool();

  BufferPool(const BufferPool&) = delete;
  BufferPool& operator=(const BufferPool&) = delete;
  BufferPool(BufferPool&&) = delete;
  BufferPool& operator=(BufferPool&&) = delete;

  // Whether the region could be reserved and the file opened. If not, every
  // `readAll` throws.
  bool isValid() const { return valid_; }

  // Whether the pool uses fixed frames (see above), and their number.
  bool usesFixedFrames() const { return frames_ != nullptr; }
  size_t numFrames() const { return numFrames_; }
  size_t numFramePartitions() const { return numFramePartitions_; }

  // The frames of `partition` as buffers for `IORING_REGISTER_BUFFERS`
  // (contiguous chunks of at most `maxFixedBufferBytes`). Empty without fixed
  // frames. Loads never merge pages across two of these buffers.
  std::vector<std::pair<char*, size_t>> fixedBufferIovecs(
      size_t partition) const;

  // How long faulting in the frames took in the constructor (seconds), and
  // with how many threads.
  double prefaultSeconds() const { return prefaultSeconds_; }
  size_t prefaultThreads() const { return prefaultThreads_; }

  // Whether the file is read with `O_DIRECT`.
  bool usesDirectIo() const { return directIo_; }

  // The descriptor that missing pages are read from (opened with `O_DIRECT`
  // if `usesDirectIo()`).
  int fd() const { return fd_; }

  // Change the budget. The loaded pages may exceed it by the pages of the
  // `readAll` calls that are running; the next `readAll` evicts the excess.
  // A budget of zero keeps no page beyond the end of the `readAll` that
  // loaded it.
  // With fixed frames, the budget is at most the size of the frames.
  void setBudget(size_t budgetBytes);
  size_t budgetBytes() const { return budgetPages_.load() * pageSize_; }

  // Evict loaded pages (CLOCK) until at most the budget remains, but at most
  // `maxPages` of them, and return how many were evicted. For a background
  // thread that lowers the budget and then evicts in bounded steps, so that
  // queries rarely have to evict themselves (see `BufferPoolBudget`).
  size_t evictTowardsBudget(size_t maxPages);

  // The bytes of the pages that are currently loaded.
  size_t loadedBytes() const { return numLoaded_.load() * pageSize_; }

  // Read `numBytes[i]` bytes at `offsets[i]` of the file into `buffers[i]`
  // for every `i` (any alignment; the ranges must lie within the file).
  // Ranges whose pages are loaded are copied from the region; the missing
  // pages of all ranges are loaded with one call of `loader` (adjacent pages
  // as one read; `nullptr` = synchronous `pread`), then copied. While read `i`
  // is copied, the region addresses of read `i + prefetchDistance` are
  // prefetched (0 = no prefetch). With fixed frames, missing pages are loaded
  // into frames of `partition` (modulo the number of partitions) if it has
  // free ones; if no frame can be freed for a page (all frames are being
  // loaded or read by other calls), the reads that need it are read from the
  // file directly into their buffers.
  void readAll(ql::span<const size_t> numBytes,
               ql::span<const uint64_t> offsets, ql::span<char*> buffers,
               const Loader* loader = nullptr, size_t prefetchDistance = 0,
               size_t partition = 0);

  // Statistics, for logging and tests.
  size_t numLoadedPages() const { return numLoaded_.load(); }
  uint64_t numHits() const { return hits_.load(); }
  uint64_t numPageLoads() const { return pageLoads_.load(); }
  uint64_t numEvictions() const { return evictions_.load(); }
  size_t numPages() const { return numPages_; }
  // Reads that were read from the file directly, because no frame was free.
  uint64_t numBypassedReads() const { return bypassedReads_.load(); }

 private:
  // The layout of a page's state word.
  static constexpr uint64_t stateMask = 3;
  static constexpr uint64_t evicted = 0;
  static constexpr uint64_t loading = 1;
  static constexpr uint64_t resident = 2;
  static constexpr uint64_t referenceBit = 4;
  static constexpr uint64_t versionOne = 8;

  // Bits of the version in the state word, and with fixed frames the position
  // of the frame number.
  static constexpr uint64_t fullVersionMask = ~uint64_t{7};
  static constexpr uint64_t fixedVersionMask = 0xFFFF'FFF8;
  static constexpr unsigned frameShift = 32;

  size_t pageSize_ = defaultPageSize;
  bool valid_ = false;
  uint64_t versionMask_ = fullVersionMask;
  char* region_ = nullptr;
  // Fixed frames (see above): the arena, the number of frames (a multiple of
  // the number of partitions), and the free frames of each partition (used
  // as stacks, so that consecutive frames are handed out together).
  char* frames_ = nullptr;
  size_t numFrames_ = 0;
  size_t numFramePartitions_ = 0;
  size_t framesPerPartition_ = 0;
  size_t framesPerChunk_ = 0;
  double prefaultSeconds_ = 0;
  size_t prefaultThreads_ = 0;
  struct FreeFrames {
    std::mutex mutex_;
    std::vector<uint32_t> frames_;
  };
  std::unique_ptr<FreeFrames[]> freeFrames_;
  std::atomic<uint64_t> bypassedReads_{0};
  size_t regionSize_ = 0;
  size_t fileSize_ = 0;
  size_t numPages_ = 0;
  int fd_ = -1;
  bool directIo_ = false;
  // Only used to read the last page if it is incomplete (`O_DIRECT` reads of
  // whole pages would return fewer bytes there).
  int bufferedFd_ = -1;
  std::unique_ptr<std::atomic<uint64_t>[]> states_;

  std::atomic<size_t> budgetPages_{0};
  std::atomic<size_t> numLoaded_{0};
  std::atomic<uint64_t> hits_{0};
  std::atomic<uint64_t> pageLoads_{0};
  std::atomic<uint64_t> evictions_{0};

  // The CLOCK: the loaded pages (in no particular order) and the hand. Only
  // touched when pages are published or evicted, never on the hit path.
  std::mutex clockMutex_;
  std::vector<uint32_t> clockPages_;
  size_t clockHand_ = 0;

  // Try to copy `numBytes` at `offset` from the region into `target`. Return
  // false (with an unspecified `target`) if a page is not loaded or was
  // evicted during the copy.
  bool tryCopy(uint64_t offset, size_t numBytes, char* target);

  // Load the pages `pages` (sorted, unique) that this thread has claimed
  // (state `loading`).
  void loadClaimed(ql::span<const size_t> pages, const Loader* loader);

  // The same with fixed frames: first assign a frame to each page (from
  // `partition` first, evicting if needed). The pages for which no frame
  // could be found are not loaded and returned (the caller unclaims them).
  std::vector<size_t> loadClaimedIntoFrames(ql::span<const size_t> pages,
                                            const Loader* loader,
                                            size_t partition);

  // Take up to `n` free frames, from `partition` first, then from the other
  // partitions; evict loaded pages once if that is not enough.
  std::vector<uint32_t> takeFrames(size_t n, size_t partition);

  // Take up to `n` free frames without evicting.
  void takeFreeFrames(size_t n, size_t partition, std::vector<uint32_t>& out);

  // The address of the page whose state word is `word` (fixed frames: in its
  // frame; otherwise at its place in the region).
  char* frameAddress(uint64_t word) const {
    return frames_ + (word >> frameShift) * pageSize_;
  }

  // Copy `numBytes` at `offset` of the file into `target` from the loaded
  // pages, whose state words (from `first` on) are `words`.
  void copyFromPages(const uint64_t* words, uint64_t offset, size_t numBytes,
                     char* target) const;

  // Copy a read whose pages this thread owns (claimed or pinned).
  void copyOwned(uint64_t offset, size_t numBytes, char* target) const;

  // The state word after an eviction: the next version (within the version
  // bits; the frame bits stay).
  uint64_t nextVersion(uint64_t word) const {
    return (word & ~versionMask_) | ((word + versionOne) & versionMask_);
  }

  // Fault in the frames (in parallel), see the constructor.
  void prefaultFrames();

  // Publish the claimed pages as loaded (and add them to the CLOCK).
  void publish(ql::span<const size_t> pages);

  // Release pages that this thread pinned (they stay loaded).
  void unpin(ql::span<const size_t> pages);

  // Undo a claim after a failed load.
  void unclaim(ql::span<const size_t> pages);

  // Evict loaded pages until at most `budgetPages_ - incoming` remain (or no
  // loaded page can be evicted).
  void evictFor(size_t incoming);

  // Evict loaded pages until at most `target` remain, but at most `maxVictims`
  // of them (or until no loaded page can be evicted). Return the number of
  // evicted pages.
  size_t evictDownTo(size_t target, size_t maxVictims);

  // Return the memory of the evicted pages to the kernel.
  void releasePages(std::vector<size_t> pages);

  void prefetch(uint64_t offset, size_t numBytes) const {
    __builtin_prefetch(region_ + offset, 0, 3);
    __builtin_prefetch(region_ + offset + numBytes - 1, 0, 3);
  }

  // With fixed frames the address of a page is only known from its state
  // word: prefetch the state words of a read far ahead, and the bytes of a
  // read closer ahead (its state word is then likely in the cache).
  void prefetchState(uint64_t offset, size_t numBytes) const {
    __builtin_prefetch(&states_[offset / pageSize_], 0, 3);
    __builtin_prefetch(&states_[(offset + numBytes) / pageSize_], 0, 3);
  }
  void prefetchFrame(uint64_t offset, size_t numBytes) const {
    // An empty read may start at the end of the file (past the last page).
    if (numBytes == 0) {
      return;
    }
    const uint64_t word =
        states_[offset / pageSize_].load(std::memory_order_relaxed);
    if ((word & stateMask) == resident) {
      __builtin_prefetch(frameAddress(word) + offset % pageSize_, 0, 3);
    }
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BUFFERPOOL_H
