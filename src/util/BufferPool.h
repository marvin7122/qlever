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
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "backports/span.h"

namespace ad_utility {

// Process-wide switch, set by the runtime parameter
// `vocabulary-buffer-pool-futex-wait`: a thread that waits for a page which
// another thread is loading or evicting first spins briefly and then sleeps on
// a futex until that thread publishes or releases the page. If false (the
// default), it calls `sched_yield` in a loop instead.
inline std::atomic<bool> bufferPoolFutexWait{false};

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
// Every page has one atomic state word: its state (evicted, loading,
// resident), a CLOCK reference bit, and a version that changes whenever the
// page is evicted. A reader copies a resident page optimistically and then
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
  BufferPool(const std::string& filename, size_t fileSize, size_t budgetBytes,
             size_t pageSize = defaultPageSize);
  ~BufferPool();

  BufferPool(const BufferPool&) = delete;
  BufferPool& operator=(const BufferPool&) = delete;
  BufferPool(BufferPool&&) = delete;
  BufferPool& operator=(BufferPool&&) = delete;

  // Whether the region could be reserved and the file opened. If not, every
  // `readAll` throws.
  bool isValid() const { return region_ != nullptr; }

  // Whether the file is read with `O_DIRECT`.
  bool usesDirectIo() const { return directIo_; }

  // The descriptor that missing pages are read from (opened with `O_DIRECT`
  // if `usesDirectIo()`).
  int fd() const { return fd_; }

  // Change the budget. The loaded pages may exceed it by the pages of the
  // `readAll` calls that are running; the next `readAll` evicts the excess.
  // A budget of zero keeps no page beyond the end of the `readAll` that
  // loaded it.
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
  // prefetched (0 = no prefetch).
  void readAll(ql::span<const size_t> numBytes,
               ql::span<const uint64_t> offsets, ql::span<char*> buffers,
               const Loader* loader = nullptr, size_t prefetchDistance = 0);

  // Split-phase reading (see `startReadAll`): `submit` starts reading the
  // ranges like a `Loader` (same requirements) and returns a token without
  // waiting; `wait(token)` returns when they are complete and must throw on
  // an error.
  struct AsyncLoader {
    std::function<uint64_t(int fd, ql::span<const size_t> numBytes,
                           ql::span<const uint64_t> offsets,
                           ql::span<char*> targets)>
        submit;
    std::function<void(uint64_t token)> wait;
  };

  // The state of a read between `startReadAll` and `finishReadAll`: the reads
  // that are not served yet, and the pages that this thread claimed and whose
  // loads are in flight. Other threads that need these pages wait until they
  // are published, so a thread must never wait for another thread's page
  // while it holds such claims: every waiting thread first completes its own
  // pending reads (`completeOwnPendingReads`). Belongs to the thread that
  // started it. Not movable, because the thread keeps a list of its pending
  // reads. The destructor completes an unfinished read (its pages are then
  // published, but the buffers of its unserved reads stay unwritten).
  class PendingRead {
   public:
    PendingRead() = default;
    ~PendingRead();
    PendingRead(const PendingRead&) = delete;
    PendingRead& operator=(const PendingRead&) = delete;

    // True between `startReadAll` and `finishReadAll`.
    bool active() const { return pool_ != nullptr; }

   private:
    friend class BufferPool;
    // Wait for the loads and publish the claimed pages; on an error discard
    // them and keep the error for `finishReadAll`. Idempotent, never throws.
    void complete() noexcept;
    // Remove this read from its thread's list and reset it.
    void release();

    BufferPool* pool_ = nullptr;
    std::vector<size_t> pending_;
    std::vector<size_t> claimed_;
    std::optional<uint64_t> token_;
    std::function<void(uint64_t)> wait_;
    bool completed_ = false;
    std::exception_ptr error_;
  };

  // The first half of `readAll`: copy the reads whose pages are loaded, claim
  // the evicted pages of the other reads and start loading them with
  // `loader.submit` (one call for all, adjacent pages as one read), without
  // waiting. The spans must stay valid and unchanged until `finishReadAll`.
  // `read` must not be active.
  void startReadAll(PendingRead& read, ql::span<const size_t> numBytes,
                    ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                    const AsyncLoader& loader, size_t prefetchDistance = 0);

  // The second half: wait for the loads of `read`, publish its pages and serve
  // all remaining reads (as `readAll` does, missing pages through `loader`).
  // The spans must be the ones passed to `startReadAll`.
  void finishReadAll(PendingRead& read, ql::span<const size_t> numBytes,
                     ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                     const Loader* loader = nullptr);

  // Complete all pending reads of the calling thread (see `PendingRead`).
  static void completeOwnPendingReads() noexcept;

  // Statistics, for logging and tests.
  size_t numLoadedPages() const { return numLoaded_.load(); }
  uint64_t numHits() const { return hits_.load(); }
  uint64_t numPageLoads() const { return pageLoads_.load(); }
  uint64_t numEvictions() const { return evictions_.load(); }
  size_t numPages() const { return numPages_; }

 private:
  // The layout of a page's state word.
  static constexpr uint64_t stateMask = 3;
  static constexpr uint64_t evicted = 0;
  static constexpr uint64_t loading = 1;
  static constexpr uint64_t resident = 2;
  static constexpr uint64_t referenceBit = 4;
  // Set by a thread that sleeps on the futex of a page that is loading or
  // evicting (see `bufferPoolFutexWait`); the thread that changes the state
  // clears it and wakes the sleepers.
  static constexpr uint64_t waiterBit = 8;
  static constexpr uint64_t versionOne = 16;

  size_t pageSize_ = defaultPageSize;
  char* region_ = nullptr;
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

  // The reads that load the claimed `pages` (sorted, unique): adjacent pages
  // as one read, without an incomplete last page of the file, which is read
  // separately (`partialLastPage`).
  struct PageRuns {
    std::vector<size_t> numBytes_;
    std::vector<uint64_t> offsets_;
    std::vector<char*> targets_;
    bool partialLastPage_ = false;
  };
  PageRuns runsOf(ql::span<const size_t> pages) const;

  // Read the incomplete last page of the file through the page cache.
  void readPartialLastPage();

  // Copy the hits of all reads into their buffers (prefetching ahead) and
  // return the indices of the reads that are not served.
  std::vector<size_t> copyHits(ql::span<const size_t> numBytes,
                               ql::span<const uint64_t> offsets,
                               ql::span<char*> buffers,
                               size_t prefetchDistance);

  // Serve the reads `pending` (the rounds of `readAll`).
  void servePending(std::vector<size_t> pending,
                    ql::span<const size_t> numBytes,
                    ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                    const Loader* loader);

  // Wait until `page` is neither loading nor evicting.
  void waitWhileBusy(size_t page);

  // Atomically replace the state word of `page` by `next(old)`; return `old`.
  // Wakes the threads sleeping on the page if `old` had the waiter bit and the
  // new word does not.
  template <typename F>
  uint64_t transition(size_t page, F next);

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
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BUFFERPOOL_H
