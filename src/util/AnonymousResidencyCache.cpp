// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/AnonymousResidencyCache.h"

#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>

namespace ad_utility {

// _____________________________________________________________________________
AnonymousResidencyCache::PinnedFrame::PinnedFrame(
    AnonymousResidencyCache* cache, size_t frameIndex)
    : cache_{cache}, frameIndex_{frameIndex} {}

// _____________________________________________________________________________
AnonymousResidencyCache::PinnedFrame::PinnedFrame(PinnedFrame&& other) noexcept
    : cache_{std::exchange(other.cache_, nullptr)},
      frameIndex_{std::exchange(other.frameIndex_, 0)} {}

// _____________________________________________________________________________
AnonymousResidencyCache::PinnedFrame&
AnonymousResidencyCache::PinnedFrame::operator=(PinnedFrame&& other) noexcept {
  if (this != &other) {
    reset();
    cache_ = std::exchange(other.cache_, nullptr);
    frameIndex_ = std::exchange(other.frameIndex_, 0);
  }
  return *this;
}

// _____________________________________________________________________________
AnonymousResidencyCache::PinnedFrame::~PinnedFrame() { reset(); }

// _____________________________________________________________________________
void AnonymousResidencyCache::PinnedFrame::reset() {
  if (cache_ != nullptr) {
    std::lock_guard guard{cache_->mutex_};
    cache_->unpinLocked(frameIndex_);
    cache_ = nullptr;
  }
}

// _____________________________________________________________________________
const char* AnonymousResidencyCache::PinnedFrame::data() const {
  AD_CORRECTNESS_CHECK(cache_ != nullptr);
  AD_CORRECTNESS_CHECK(cache_->base_ != nullptr);
  return cache_->base_ + frameIndex_ * kPageSize;
}

// _____________________________________________________________________________
AnonymousResidencyCache::AnonymousResidencyCache(size_t numFrames,
                                                 uint64_t fileSize)
    : fileSize_{fileSize}, frames_(numFrames) {
  if (numFrames == 0) {
    frames_.clear();
    return;
  }
  void* base = ::mmap(nullptr, numFrames * kPageSize, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (base == MAP_FAILED) {
    // Inert object: all reads fall back to direct `pread`.
    frames_.clear();
    return;
  }
  base_ = static_cast<char*>(base);
}

// _____________________________________________________________________________
AnonymousResidencyCache::~AnonymousResidencyCache() {
  if (base_ == nullptr) {
    return;
  }
  // No live pins may outlive the cache; a live guard at destruction is a
  // pin-lifecycle bug at the call site.
  for (const Frame& frame : frames_) {
    AD_CORRECTNESS_CHECK(frame.pinCount_ == 0);
  }
  AD_CORRECTNESS_CHECK(::munmap(base_, frames_.size() * kPageSize) == 0);
}

// _____________________________________________________________________________
size_t AnonymousResidencyCache::clockVictimLocked() {
  // Second chance: clear reference bits on the first sweep, evict frames
  // without a reference bit on the second. Pinned frames are never victims.
  const size_t n = frames_.size();
  for (size_t sweep = 0; sweep < 2; ++sweep) {
    for (size_t i = 0; i < n; ++i) {
      size_t idx = (hand_ + i) % n;
      Frame& frame = frames_[idx];
      if (frame.pinCount_ > 0) {
        continue;
      }
      if (frame.referenceBit_) {
        frame.referenceBit_ = false;
        continue;
      }
      hand_ = (idx + 1) % n;
      return idx;
    }
  }
  return n;
}

// _____________________________________________________________________________
void AnonymousResidencyCache::unpinLocked(size_t frameIndex) {
  AD_CORRECTNESS_CHECK(frameIndex < frames_.size());
  Frame& frame = frames_[frameIndex];
  AD_CORRECTNESS_CHECK(frame.pinCount_ > 0);
  AD_CORRECTNESS_CHECK(frame.state_ != FrameState::Free);
  --frame.pinCount_;
}

// _____________________________________________________________________________
std::optional<AnonymousResidencyCache::PinnedFrame>
AnonymousResidencyCache::fetch(const File& file, uint64_t page) {
  if (base_ == nullptr) {
    fallbacks_.fetch_add(1, std::memory_order_relaxed);
    return std::nullopt;
  }
  if (fileSize_ == 0 || page > (fileSize_ - 1) / kPageSize) {
    fallbacks_.fetch_add(1, std::memory_order_relaxed);
    return std::nullopt;
  }
  const uint64_t firstByte = page * kPageSize;
  const size_t bytesToFill =
      static_cast<size_t>(std::min<uint64_t>(kPageSize, fileSize_ - firstByte));

  size_t frameIndex;
  char* frameBase;
  {
    std::lock_guard guard{mutex_};
    if (auto it = index_.find(page); it != index_.end()) {
      Frame& frame = frames_[it->second];
      if (frame.state_ != FrameState::Resident) {
        // `Loading` on another thread: no blocking, caller reads directly.
        fallbacks_.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
      }
      if (frame.pinCount_ > 0) {
        // A guard (persistent hot-set pin or transient concurrent fetch)
        // already holds this frame: a pinned hit.
        pinned_.fetch_add(1, std::memory_order_relaxed);
      }
      frame.pinCount_ += 1;
      frame.referenceBit_ = true;
      hits_.fetch_add(1, std::memory_order_relaxed);
      return PinnedFrame{this, it->second};
    }
    misses_.fetch_add(1, std::memory_order_relaxed);
    frameIndex = clockVictimLocked();
    if (frameIndex == frames_.size()) {
      // Every frame is pinned: caller reads directly, no evict-while-pinned.
      fallbacks_.fetch_add(1, std::memory_order_relaxed);
      return std::nullopt;
    }
    Frame& victim = frames_[frameIndex];
    if (victim.state_ == FrameState::Resident) {
      evictions_.fetch_add(1, std::memory_order_relaxed);
      index_.erase(victim.page_);
      // Effective on anonymous memory: the physical pages are freed; the
      // frame is refilled below before it becomes visible again.
      AD_CORRECTNESS_CHECK(::madvise(base_ + frameIndex * kPageSize, kPageSize,
                                     MADV_DONTNEED) == 0);
    } else {
      AD_CORRECTNESS_CHECK(victim.state_ == FrameState::Free);
    }
    victim.state_ = FrameState::Loading;
    victim.pinCount_ = 1;
    victim.referenceBit_ = true;
    victim.page_ = page;
    index_[page] = frameIndex;
    frameBase = base_ + frameIndex * kPageSize;
  }

  // Fill outside the lock. The frame is `Loading` and pinned, so it is
  // invisible to lookup and unevictable while we read.
  const bool fillOk =
      file.read(frameBase, bytesToFill, static_cast<off_t>(firstByte)) ==
      static_cast<ssize_t>(bytesToFill);
  // Deterministic tail past end-of-file (only the last page can be short).
  if (fillOk && bytesToFill < kPageSize) {
    std::memset(frameBase + bytesToFill, 0, kPageSize - bytesToFill);
  }

  {
    std::lock_guard guard{mutex_};
    auto it = index_.find(page);
    if (!fillOk || it == index_.end() || it->second != frameIndex ||
        frames_[frameIndex].state_ != FrameState::Loading) {
      // Fill failed (or, defensively, the table changed under us, which
      // cannot happen while pinned): return the frame to `Free`.
      if (auto it2 = index_.find(page);
          it2 != index_.end() && it2->second == frameIndex) {
        index_.erase(it2);
      }
      Frame& frame = frames_[frameIndex];
      frame.state_ = FrameState::Free;
      frame.pinCount_ = 0;
      frame.referenceBit_ = false;
      fallbacks_.fetch_add(1, std::memory_order_relaxed);
      return std::nullopt;
    }
    frames_[frameIndex].state_ = FrameState::Resident;
    return PinnedFrame{this, frameIndex};
  }
}

// _____________________________________________________________________________
bool AnonymousResidencyCache::readThrough(const File& file, uint64_t offset,
                                          size_t numBytes, char* target) {
  if (numBytes == 0) {
    return true;
  }
  if (offset + numBytes < offset || offset + numBytes > fileSize_) {
    return false;
  }
  const uint64_t end = offset + numBytes;
  size_t written = 0;
  for (uint64_t cur = offset; cur < end;) {
    const uint64_t page = cur / kPageSize;
    const size_t offsetInPage = static_cast<size_t>(cur % kPageSize);
    const size_t bytesInPage = static_cast<size_t>(
        std::min<uint64_t>(kPageSize - offsetInPage, end - cur));
    if (auto pin = fetch(file, page)) {
      std::memcpy(target + written, pin->data() + offsetInPage, bytesInPage);
    } else {
      // Fallback: direct `pread`, same bytes the uncached path would read.
      if (file.read(target + written, bytesInPage, static_cast<off_t>(cur)) !=
          static_cast<ssize_t>(bytesInPage)) {
        return false;
      }
    }
    cur += bytesInPage;
    written += bytesInPage;
  }
  return true;
}

// _____________________________________________________________________________
size_t AnonymousResidencyCache::residentCount() const {
  std::lock_guard guard{mutex_};
  size_t count = 0;
  for (const Frame& frame : frames_) {
    if (frame.state_ == FrameState::Resident) {
      ++count;
    }
  }
  return count;
}

// _____________________________________________________________________________
AnonymousResidencyCache::Stats AnonymousResidencyCache::stats() const {
  return Stats{hits_.load(std::memory_order_relaxed),
               misses_.load(std::memory_order_relaxed),
               evictions_.load(std::memory_order_relaxed),
               fallbacks_.load(std::memory_order_relaxed),
               pinned_.load(std::memory_order_relaxed)};
}

}  // namespace ad_utility
