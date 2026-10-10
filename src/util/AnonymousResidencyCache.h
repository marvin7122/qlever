// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ANONYMOUSRESIDENCYCACHE_H
#define QLEVER_SRC_UTIL_ANONYMOUSRESIDENCYCACHE_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "util/Exception.h"
#include "util/File.h"

namespace ad_utility {

// Anonymous VMCache subset (Leis et al., SIGMOD 2023) for immutable files:
// an explicitly managed residency cache over `mmap`ed anonymous memory.
//
// * Reservation: `numFrames` pages of `MAP_PRIVATE | MAP_ANONYMOUS` memory.
//   Anonymous pages are process-owned, so demotion via `MADV_DONTNEED` is
//   effective (physical pages are freed; the next access zero-fills). There
//   is no file-backed mapping anywhere, so the "kernel ignores partial
//   demotion" failure mode of file mappings cannot occur.
// * Promotion: explicit `pread` fills through the caller's `File`.
// * Demotion: explicit `MADV_DONTNEED` on the victim frame range.
// * Replacement: CLOCK second-chance over unpinned frames only.
//
// Residency state is owned by the vocabulary layer (`VocabularyOnDisk` holds
// one cache per file); nothing leaks to callers. All member functions may be
// called concurrently. On any cache miss that cannot be served (all frames
// pinned, frame loading on another thread, fill failure), the API returns
// `std::nullopt`/`false` and the caller falls back to direct `pread`:
// correctness never depends on the cache.
class AnonymousResidencyCache {
 public:
  static constexpr size_t kPageSize = 4096;

  enum class FrameState { Free, Loading, Resident };

  // Move-only RAII pin guard: the frame stays pinned (unevictable) exactly
  // while the guard is alive; the destructor unpins. Pin lifetime is thus
  // encoded in the type; there is no manual pin/unpin API to unbalance.
  class PinnedFrame {
   public:
    PinnedFrame() = default;
    PinnedFrame(const PinnedFrame&) = delete;
    PinnedFrame& operator=(const PinnedFrame&) = delete;
    PinnedFrame(PinnedFrame&& other) noexcept;
    PinnedFrame& operator=(PinnedFrame&& other) noexcept;
    ~PinnedFrame();

    explicit operator bool() const { return cache_ != nullptr; }

    // Page bytes, valid while the guard is alive. Only frames in `Resident`
    // state are ever handed out; `Loading` frames are invisible to lookup.
    const char* data() const;

   private:
    friend class AnonymousResidencyCache;
    PinnedFrame(AnonymousResidencyCache* cache, size_t frameIndex);
    void reset();
    AnonymousResidencyCache* cache_ = nullptr;
    size_t frameIndex_ = 0;
  };

  struct Stats {
    size_t hits_ = 0;
    size_t misses_ = 0;
    size_t evictions_ = 0;
    size_t fallbacks_ = 0;
    // Hits on frames that were already pinned (pin count > 0) when the
    // fetch arrived. With a persistent hot set held by the owner (see
    // `VocabularyOnDisk`), hot pages hit here; without one, only transient
    // concurrent pins can produce such hits.
    size_t pinned_ = 0;
  };

  // Reserve `numFrames` anonymous frames for a file of `fileSize` bytes.
  // A zero `numFrames` (or a failed reservation) yields a valid but inert
  // object: `fetch` returns `std::nullopt` and `readThrough` falls back to
  // direct reads.
  AnonymousResidencyCache(size_t numFrames, uint64_t fileSize);
  ~AnonymousResidencyCache();

  AnonymousResidencyCache(const AnonymousResidencyCache&) = delete;
  AnonymousResidencyCache& operator=(const AnonymousResidencyCache&) = delete;
  AnonymousResidencyCache(AnonymousResidencyCache&&) = delete;
  AnonymousResidencyCache& operator=(AnonymousResidencyCache&&) = delete;

  bool isActive() const { return base_ != nullptr; }
  size_t capacity() const { return frames_.size(); }

  // Fetch page `page` (0-based) of the file: hit returns a pinned guard over
  // the resident frame; miss evicts one unpinned frame via CLOCK, fills it
  // via `file` (`pread`, outside the lock), marks it resident, and returns a
  // pinned guard. Returns `std::nullopt` when the page is out of range, the
  // cache is inert, every frame is pinned, the page is `Loading` on another
  // thread, or the fill fails (the frame is returned to `Free`).
  std::optional<PinnedFrame> fetch(const File& file, uint64_t page);

  // Serve `[offset, offset + numBytes)` into `target` page by page via
  // `fetch()`; each unserved page falls back to a direct `pread`. Returns
  // false only when a `pread` itself fails (callers then propagate the same
  // failure the direct path would have produced). Empty ranges succeed
  // trivially. Ranges past end-of-file fail.
  bool readThrough(const File& file, uint64_t offset, size_t numBytes,
                   char* target);

  size_t residentCount() const;
  Stats stats() const;

 private:
  struct Frame {
    FrameState state_ = FrameState::Free;
    size_t pinCount_ = 0;
    bool referenceBit_ = false;
    uint64_t page_ = 0;
  };

  // CLOCK victim among unpinned frames, or `frames_.size()` when every frame
  // is pinned. Caller must hold `mutex_`.
  size_t clockVictimLocked();
  // Unpin `frameIndex` (must be pinned). Caller must hold `mutex_`.
  void unpinLocked(size_t frameIndex);

  char* base_ = nullptr;
  uint64_t fileSize_ = 0;
  std::vector<Frame> frames_;
  std::unordered_map<uint64_t, size_t> index_;
  mutable std::mutex mutex_;
  size_t hand_ = 0;
  mutable std::atomic<size_t> hits_{0};
  mutable std::atomic<size_t> misses_{0};
  mutable std::atomic<size_t> evictions_{0};
  mutable std::atomic<size_t> fallbacks_{0};
  mutable std::atomic<size_t> pinned_{0};
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ANONYMOUSRESIDENCYCACHE_H
