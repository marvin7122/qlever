// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/ResidentFileMapping.h"

#include <algorithm>
#include <cstring>
#include <utility>
#include <vector>

#include "util/Exception.h"

#if defined(__linux__) && !defined(__EMSCRIPTEN__)
#include <sys/mman.h>
#define QL_RESIDENT_FILE_MAPPING
#endif

namespace ad_utility {

// _____________________________________________________________________________
VocabFramePool::VocabFramePool(size_t numFrames) {
  frames_.resize(std::max<size_t>(numFrames, 1));
}

// _____________________________________________________________________________
VocabFramePool::Frame* VocabFramePool::findLocked(const FrameKey& key) {
  auto it = index_.find(key);
  return it == index_.end() ? nullptr : &frames_[it->second];
}

// _____________________________________________________________________________
VocabFramePool::Frame* VocabFramePool::tryPin(const FrameKey& key) {
  std::lock_guard<std::mutex> lock{mutex_};
  Frame* frame = findLocked(key);
  if (frame == nullptr || frame->state != FrameState::Resident) {
    return nullptr;
  }
  frame->pinCount += 1;
  frame->referenceBit = true;
  return frame;
}

// _____________________________________________________________________________
size_t VocabFramePool::clockVictimLocked() {
  const size_t n = frames_.size();
  // Prefer a free slot without touching the hand.
  for (size_t i = 0; i < n; ++i) {
    if (frames_[i].state == FrameState::Free) {
      return i;
    }
  }
  // CLOCK over unpinned frames only: clear set reference bits for a second
  // chance, evict the first unpinned frame whose bit is already clear.
  // Loading frames are always pinned, so they are never victims.
  for (size_t revolution = 0; revolution < 2; ++revolution) {
    for (size_t examined = 0; examined < n; ++examined) {
      size_t i = hand_;
      hand_ = (hand_ + 1) % n;
      Frame& frame = frames_[i];
      if (frame.pinCount > 0) {
        continue;
      }
      if (frame.referenceBit) {
        frame.referenceBit = false;
        continue;
      }
      return i;
    }
  }
  return n;
}

// _____________________________________________________________________________
std::pair<VocabFramePool::Frame*, bool> VocabFramePool::allocate(
    const FrameKey& key) {
  std::lock_guard<std::mutex> lock{mutex_};
  if (Frame* hit = findLocked(key)) {
    // Pin existing frames, but leave filling to the installing caller.
    hit->pinCount += 1;
    hit->referenceBit = true;
    return {hit, false};
  }
  const size_t victim = clockVictimLocked();
  if (victim == frames_.size()) {
    // Every frame is pinned: never evict-while-pinned.
    return {nullptr, false};
  }
  Frame& frame = frames_[victim];
  if (frame.state != FrameState::Free) {
    index_.erase(frame.key);
  }
  frame.key = key;
  frame.state = FrameState::Loading;
  frame.pinCount = 1;
  frame.referenceBit = true;
  index_[key] = victim;
  return {&frame, true};
}

// _____________________________________________________________________________
void VocabFramePool::markLoaded(const FrameKey& key) {
  std::lock_guard<std::mutex> lock{mutex_};
  if (Frame* frame = findLocked(key)) {
    if (frame->state == FrameState::Loading) {
      frame->state = FrameState::Resident;
    }
  }
}

// _____________________________________________________________________________
void VocabFramePool::abandon(const FrameKey& key) {
  std::lock_guard<std::mutex> lock{mutex_};
  auto it = index_.find(key);
  if (it == index_.end()) {
    return;
  }
  Frame& frame = frames_[it->second];
  if (frame.state != FrameState::Loading) {
    return;
  }
  AD_CONTRACT_CHECK(frame.pinCount > 0);
  frame.pinCount -= 1;
  if (frame.pinCount == 0) {
    index_.erase(it);
    frame.state = FrameState::Free;
    frame.referenceBit = false;
  }
}

// _____________________________________________________________________________
void VocabFramePool::unpin(const FrameKey& key) {
  std::lock_guard<std::mutex> lock{mutex_};
  Frame* frame = findLocked(key);
  AD_CONTRACT_CHECK(frame != nullptr);
  AD_CONTRACT_CHECK(frame->pinCount > 0);
  frame->pinCount -= 1;
  // A failed filler may abandon while another allocator still holds a pin.
  // Release that loading frame when the final observer unpins it.
  if (frame->pinCount == 0 && frame->state == FrameState::Loading) {
    index_.erase(key);
    frame->state = FrameState::Free;
    frame->referenceBit = false;
  }
}

// _____________________________________________________________________________
bool VocabFramePool::copyPinned(const FrameKey& key, size_t offsetInPage,
                                size_t numBytes, char* target) {
  std::lock_guard<std::mutex> lock{mutex_};
  Frame* frame = findLocked(key);
  if (frame == nullptr || frame->state != FrameState::Resident) {
    return false;
  }
  if (offsetInPage > kPageSize || numBytes > kPageSize - offsetInPage) {
    return false;
  }
  if (numBytes > 0) {
    std::memcpy(target, frame->data.data() + offsetInPage, numBytes);
  }
  return true;
}

// _____________________________________________________________________________
size_t VocabFramePool::residentCount() const {
  std::lock_guard<std::mutex> lock{mutex_};
  size_t count = 0;
  for (const auto& frame : frames_) {
    count += frame.state == FrameState::Resident ? 1 : 0;
  }
  return count;
}

// _____________________________________________________________________________
size_t VocabFramePool::pinnedCount() const {
  std::lock_guard<std::mutex> lock{mutex_};
  size_t count = 0;
  for (const auto& frame : frames_) {
    count += frame.pinCount > 0 ? 1 : 0;
  }
  return count;
}

// _____________________________________________________________________________
ScanRingBuffer::ScanRingBuffer() : slots_(kNumSlots) {
  for (auto& slot : slots_) {
    slot.resize(kSlotSize, 0);
  }
}

// _____________________________________________________________________________
char* ScanRingBuffer::next() {
  std::lock_guard<std::mutex> lock{mutex_};
  char* slot = slots_[next_].data();
  next_ = (next_ + 1) % kNumSlots;
  return slot;
}

// _____________________________________________________________________________
HotStringCache::HotStringCache(size_t capacity) : capacity_{capacity} {}

// _____________________________________________________________________________
std::optional<std::string> HotStringCache::lookup(uint64_t vocabIndex) const {
  std::lock_guard<std::mutex> lock{mutex_};
  auto it = index_.find(vocabIndex);
  if (it == index_.end()) {
    return std::nullopt;
  }
  lru_.splice(lru_.begin(), lru_, it->second);
  return it->second->value;
}

// _____________________________________________________________________________
void HotStringCache::insert(uint64_t vocabIndex, std::string value) {
  std::lock_guard<std::mutex> lock{mutex_};
  if (capacity_ == 0) {
    return;
  }
  if (auto it = index_.find(vocabIndex); it != index_.end()) {
    it->second->value = std::move(value);
    lru_.splice(lru_.begin(), lru_, it->second);
    return;
  }
  lru_.push_front(Entry{vocabIndex, std::move(value)});
  index_[vocabIndex] = lru_.begin();
  while (index_.size() > capacity_) {
    index_.erase(lru_.back().index);
    lru_.pop_back();
  }
}

// _____________________________________________________________________________
size_t HotStringCache::size() const {
  std::lock_guard<std::mutex> lock{mutex_};
  return index_.size();
}

// _____________________________________________________________________________
ResidentFileMapping::ResidentFileMapping([[maybe_unused]] int fd,
                                         size_t fileSize) {
#ifdef QL_RESIDENT_FILE_MAPPING
  if (fileSize == 0) {
    return;
  }
  void* data = ::mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
  if (data == MAP_FAILED) {
    return;
  }
  data_ = static_cast<const char*>(data);
  size_ = fileSize;
  const size_t numPages = (fileSize + pageSize - 1) / pageSize;
  numBitWords_ = (numPages + 63) / 64;
  residentBits_ = std::make_unique<std::atomic<uint64_t>[]>(numBitWords_);
  referenceBits_ = std::make_unique<std::atomic<uint64_t>[]>(numBitWords_);
  for (size_t i = 0; i < numBitWords_; ++i) {
    residentBits_[i].store(0, std::memory_order_relaxed);
    referenceBits_[i].store(0, std::memory_order_relaxed);
  }
#else
  (void)fileSize;
#endif
}

// _____________________________________________________________________________
void ResidentFileMapping::unmap() {
#ifdef QL_RESIDENT_FILE_MAPPING
  if (data_ != nullptr) {
    ::munmap(const_cast<char*>(data_), size_);
  }
#endif
  data_ = nullptr;
  size_ = 0;
  numBitWords_ = 0;
  residentBits_.reset();
  referenceBits_.reset();
  clockHand_.store(0, std::memory_order_relaxed);
  std::lock_guard<std::mutex> lock{framePoolMutex_};
  framePool_.reset();
}

// _____________________________________________________________________________
ResidentFileMapping::~ResidentFileMapping() { unmap(); }

// _____________________________________________________________________________
ResidentFileMapping::ResidentFileMapping(ResidentFileMapping&& other) noexcept
    : data_{std::exchange(other.data_, nullptr)},
      size_{std::exchange(other.size_, 0)},
      numBitWords_{std::exchange(other.numBitWords_, 0)},
      residentBits_{std::move(other.residentBits_)},
      referenceBits_{std::move(other.referenceBits_)} {
  capPages_.store(other.capPages_.load(std::memory_order_relaxed),
                  std::memory_order_relaxed);
  clockHand_.store(other.clockHand_.load(std::memory_order_relaxed),
                   std::memory_order_relaxed);
  std::lock_guard<std::mutex> lock{other.framePoolMutex_};
  framePool_ = std::move(other.framePool_);
  framePoolFileId_ = other.framePoolFileId_;
}

// _____________________________________________________________________________
ResidentFileMapping& ResidentFileMapping::operator=(
    ResidentFileMapping&& other) noexcept {
  if (this != &other) {
    unmap();
    data_ = std::exchange(other.data_, nullptr);
    size_ = std::exchange(other.size_, 0);
    numBitWords_ = std::exchange(other.numBitWords_, 0);
    residentBits_ = std::move(other.residentBits_);
    referenceBits_ = std::move(other.referenceBits_);
    capPages_.store(other.capPages_.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
    clockHand_.store(other.clockHand_.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock{other.framePoolMutex_};
    std::lock_guard<std::mutex> selfLock{framePoolMutex_};
    framePool_ = std::move(other.framePool_);
    framePoolFileId_ = other.framePoolFileId_;
  }
  return *this;
}

// _____________________________________________________________________________
bool ResidentFileMapping::tryRead(uint64_t offset, size_t numBytes,
                                  char* target) const {
  if (numBytes == 0) {
    return true;
  }
  if (data_ == nullptr || offset > size_ || numBytes > size_ - offset) {
    return false;
  }
  auto [firstPage, lastPage] = pagesOf(offset, numBytes);
  for (size_t page = firstPage; page <= lastPage; ++page) {
    const uint64_t bits =
        residentBits_[page / 64].load(std::memory_order_relaxed);
    if ((bits & (uint64_t{1} << (page % 64))) == 0) {
      return false;
    }
  }
  for (size_t page = firstPage; page <= lastPage; ++page) {
    referenceBits_[page / 64].fetch_or(uint64_t{1} << (page % 64),
                                       std::memory_order_relaxed);
  }
  std::memcpy(target, data_ + offset, numBytes);
  return true;
}

// _____________________________________________________________________________
void ResidentFileMapping::markResident(uint64_t offset, size_t numBytes) const {
  if (data_ == nullptr || numBytes == 0 || offset > size_ ||
      numBytes > size_ - offset) {
    return;
  }
  auto [firstPage, lastPage] = pagesOf(offset, numBytes);
  bool addedResidentPage = false;
  for (size_t page = firstPage; page <= lastPage; ++page) {
    const uint64_t mask = uint64_t{1} << (page % 64);
    referenceBits_[page / 64].fetch_or(mask, std::memory_order_relaxed);
    auto& word = residentBits_[page / 64];
    // Skip the atomic read-modify-write if the bit is already set.
    if ((word.load(std::memory_order_relaxed) & mask) == 0) {
      const uint64_t old = word.fetch_or(mask, std::memory_order_relaxed);
      addedResidentPage |= (old & mask) == 0;
    }
  }
  const size_t cap = capPages_.load(std::memory_order_relaxed);
  if (cap > 0 && addedResidentPage) {
    evictDownTo(cap);
  }
}

// _____________________________________________________________________________
void ResidentFileMapping::setResidentCapPages(size_t capPages) const {
  capPages_.store(capPages, std::memory_order_relaxed);
  if (capPages > 0) {
    evictDownTo(capPages);
  }
}

// _____________________________________________________________________________
// Clear marked pages down to `target`, sweeping from the CLOCK hand in file
// order, giving referenced pages a second chance and demoting cleared runs
// from the page cache. Stops after two revolutions: concurrent accesses may
// keep the bitmap above target, which a `markResident` adding pages
// re-evaluates. Only ever clears bits, so concurrent `tryRead` either sees the
// page (correct: it is still mapped) or misses it (correct: it takes the other
// path).
void ResidentFileMapping::evictDownTo(size_t target) const {
#ifdef QL_RESIDENT_FILE_MAPPING
  if (data_ == nullptr || numBitWords_ == 0) {
    return;
  }
  const size_t numPages = (size_ + pageSize - 1) / pageSize;
  size_t hand = clockHand_.load(std::memory_order_relaxed);
  auto countResidentPages = [&]() {
    size_t count = 0;
    for (size_t i = 0; i < numBitWords_; ++i) {
      count += static_cast<size_t>(__builtin_popcountll(
          residentBits_[i].load(std::memory_order_relaxed)));
    }
    return count;
  };
  size_t count = countResidentPages();
  // Contiguous cleared runs share one `madvise` call. A run is the page
  // interval [runStart, runEnd); `runEnd == numPages + 1` marks "no open run".
  // Pages are visited in CLOCK order, so a run flushes at every gap or wrap.
  size_t runStart = 0;
  size_t runEnd = numPages + 1;
  auto flushRun = [&]() {
    if (runEnd <= numPages && runStart < runEnd) {
      const size_t byteStart = runStart * pageSize;
      size_t byteEnd = runEnd * pageSize;
      if (byteEnd > size_) {
        byteEnd = size_;
      }
      if (byteEnd > byteStart) {
// `MADV_PAGEOUT` demotes clean pages and starts writeback on dirty ones;
// plain `MADV_DONTNEED` is a silent no-op on some machines in our fleet
// (verified on Ural: rc 0, pages stay resident), so prefer `PAGEOUT` where
// the headers know it.
#ifdef MADV_PAGEOUT
        ::madvise(const_cast<char*>(data_) + byteStart, byteEnd - byteStart,
                  MADV_PAGEOUT);
#else
        ::madvise(const_cast<char*>(data_) + byteStart, byteEnd - byteStart,
                  MADV_DONTNEED);
#endif
      }
    }
    runEnd = numPages + 1;
  };
  for (size_t revolution = 0; revolution < 2 && count > target; ++revolution) {
    for (size_t examined = 0; examined < numPages && count > target;
         ++examined) {
      const size_t page = hand;
      hand = (hand + 1) % numPages;
      const size_t w = page / 64;
      const uint64_t mask = uint64_t{1} << (page % 64);
      if ((residentBits_[w].load(std::memory_order_relaxed) & mask) == 0) {
        continue;
      }
      // Clear the reference bit, but leave recently used pages resident.
      if ((referenceBits_[w].fetch_and(~mask, std::memory_order_relaxed) &
           mask) != 0) {
        continue;
      }
      const uint64_t old =
          residentBits_[w].fetch_and(~mask, std::memory_order_relaxed);
      if ((old & mask) == 0) {
        continue;
      }
      --count;
      if (page != runEnd) {
        flushRun();
        runStart = page;
      }
      runEnd = page + 1;
      // Concurrent marks can change the bitmap during the sweep. Recount
      // before deciding that the target has been reached.
      if (count <= target) {
        count = countResidentPages();
      }
    }
    flushRun();
    count = countResidentPages();
  }
  clockHand_.store(hand, std::memory_order_relaxed);
#else
  (void)target;
#endif
}

// _____________________________________________________________________________
std::vector<size_t> ResidentFileMapping::tryReadAll(
    ql::span<const size_t> numBytes, ql::span<const uint64_t> offsets,
    ql::span<char*> buffers) const {
  AD_CONTRACT_CHECK(offsets.size() == numBytes.size() &&
                    buffers.size() == numBytes.size());
  std::vector<size_t> notServed;
  for (size_t i = 0; i < numBytes.size(); ++i) {
    if (!tryRead(offsets[i], numBytes[i], buffers[i])) {
      notServed.push_back(i);
    }
  }
  return notServed;
}

// _____________________________________________________________________________
void ResidentFileMapping::markAllResident(
    ql::span<const size_t> numBytes, ql::span<const uint64_t> offsets,
    ql::span<const size_t> positions) const {
  for (size_t i : positions) {
    markResident(offsets[i], numBytes[i]);
  }
}

// _____________________________________________________________________________
void ResidentFileMapping::enableFramePool(size_t numFrames,
                                          uint32_t fileId) const {
  std::lock_guard<std::mutex> lock{framePoolMutex_};
  framePool_ = std::make_shared<VocabFramePool>(numFrames);
  framePoolFileId_ = fileId;
}

// _____________________________________________________________________________
void ResidentFileMapping::disableFramePool() const {
  std::lock_guard<std::mutex> lock{framePoolMutex_};
  framePool_.reset();
}

// _____________________________________________________________________________
bool ResidentFileMapping::framePoolEnabled() const {
  std::lock_guard<std::mutex> lock{framePoolMutex_};
  return framePool_ != nullptr;
}

// _____________________________________________________________________________
bool ResidentFileMapping::fillFrameFromFile(
    uint64_t page, const FrameFillReader& reader) const {
  if (size_ == 0 || reader == nullptr) {
    return false;
  }
  const size_t numPages = (size_ + pageSize - 1) / pageSize;
  if (page >= numPages) {
    return false;
  }
  std::shared_ptr<VocabFramePool> pool;
  uint32_t fileId = 0;
  {
    std::lock_guard<std::mutex> lock{framePoolMutex_};
    pool = framePool_;
    fileId = framePoolFileId_;
  }
  if (pool == nullptr) {
    return false;
  }
  const FrameKey key{fileId, page};
  auto [frame, installed] = pool->allocate(key);
  if (frame == nullptr) {
    return false;
  }
  if (!installed) {
    // Query residency under the pool lock without copying any bytes. A
    // concurrent fill may still be loading; only its installer may write.
    const bool resident = pool->copyPinned(key, 0, 0, nullptr);
    pool->unpin(key);
    return resident;
  }
  const uint64_t fileOffset = page * pageSize;
  const size_t numBytes = std::min(pageSize, size_ - fileOffset);
  const bool ok = reader(frame->data.data(), fileOffset, numBytes);
  if (!ok) {
    pool->abandon(key);
    return false;
  }
  if (numBytes < VocabFramePool::kPageSize) {
    std::fill(frame->data.begin() + numBytes, frame->data.end(), 0);
  }
  pool->markLoaded(key);
  pool->unpin(key);
  return true;
}

// _____________________________________________________________________________
bool ResidentFileMapping::tryReadPooled(uint64_t offset, size_t numBytes,
                                        char* target) const {
  if (numBytes == 0) {
    return true;
  }
  if (size_ == 0 || offset > size_ || numBytes > size_ - offset) {
    return false;
  }
  std::shared_ptr<VocabFramePool> pool;
  uint32_t fileId = 0;
  {
    std::lock_guard<std::mutex> lock{framePoolMutex_};
    pool = framePool_;
    fileId = framePoolFileId_;
  }
  if (pool == nullptr) {
    return false;
  }
  auto [firstPage, lastPage] = pagesOf(offset, numBytes);
  // Pin every page first so concurrent eviction cannot drop a page between
  // the hit check and the copy (no use-after-unpin).
  std::vector<FrameKey> pinned;
  pinned.reserve(lastPage - firstPage + 1);
  for (size_t page = firstPage; page <= lastPage; ++page) {
    const FrameKey key{fileId, page};
    if (pool->tryPin(key) == nullptr) {
      for (const auto& done : pinned) {
        pool->unpin(done);
      }
      return false;
    }
    pinned.push_back(key);
  }
  size_t copied = 0;
  bool ok = true;
  for (size_t page = firstPage; page <= lastPage && ok; ++page) {
    const FrameKey key{fileId, page};
    const size_t pageBase = page * pageSize;
    const size_t begin = std::max<size_t>(offset, pageBase);
    const size_t end = std::min<size_t>(offset + numBytes, pageBase + pageSize);
    ok = pool->copyPinned(key, begin - pageBase, end - begin, target + copied);
    copied += end - begin;
  }
  for (const auto& key : pinned) {
    pool->unpin(key);
  }
  return ok;
}

}  // namespace ad_utility
