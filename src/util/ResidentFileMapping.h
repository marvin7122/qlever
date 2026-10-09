// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_RESIDENTFILEMAPPING_H
#define QLEVER_SRC_UTIL_RESIDENTFILEMAPPING_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "backports/span.h"

namespace ad_utility {

// Key of a frame: the vocabulary file plus the 4 KiB page index within it.
// `fileId` distinguishes the words file (0) from the offsets file (1); both
// files share the same frame-table key shape so a future shared pool can index
// them together. Per-mapping pools (see `ResidentFileMapping::enableFramePool`)
// fix `fileId` and map page index to frames.
struct FrameKey {
  uint32_t fileId = 0;
  uint64_t page = 0;
  bool operator==(const FrameKey& other) const {
    return fileId == other.fileId && page == other.page;
  }
};

struct FrameKeyHash {
  size_t operator()(FrameKey const& key) const noexcept {
    // Splitmix64 over the combined 96 bits.
    uint64_t x = key.page + uint64_t{0x9e3779b97f4a7c15ull} +
                 (uint64_t{key.fileId} << 1);
    x = (x ^ (x >> 30)) * uint64_t{0xbf58476d1ce4e5b9ull};
    x = (x ^ (x >> 27)) * uint64_t{0x94d049bb133111ebull};
    return static_cast<size_t>(x ^ (x >> 31));
  }
};

// Explicit frame pool over immutable vocabulary files (design sketch in
// PR 283: "explicit frame pool"). Each frame holds one 4 KiB buffer plus
// state (free, loading, or resident), a pin count, and a reference bit. The
// CLOCK hand sweeps unpinned frames only, so concurrent exports can never
// evict each other's in-flight pages. Immutable files remove dirty-page
// handling entirely: fills are byte copies, eviction just drops the buffer.
//
// Fills go through the caller: `allocate` identifies who installed a pinned
// loading frame. Only that caller reads the page (in production via the
// existing io_uring `BatchManagerBase::addBatch`/`wait` path, see
// `VocabularyOnDisk`), then
// calls `markLoaded`. Thread safety: all member functions may be called
// concurrently.
class VocabFramePool {
 public:
  static constexpr size_t kPageSize = 4096;

  enum class FrameState { Free, Loading, Resident };

  struct Frame {
    FrameState state = FrameState::Free;
    size_t pinCount = 0;
    bool referenceBit = false;
    FrameKey key{};
    std::vector<char> data;
    Frame() : data(kPageSize, 0) {}
  };

  explicit VocabFramePool(size_t numFrames = 256);
  VocabFramePool(const VocabFramePool&) = delete;
  VocabFramePool& operator=(const VocabFramePool&) = delete;
  VocabFramePool(VocabFramePool&&) = delete;
  VocabFramePool& operator=(VocabFramePool&&) = delete;

  // Admission filter (Postgres discipline): single-pass scan traffic must
  // never enter the pool; it streams through confined ring buffers instead
  // (see `ScanRingBuffer`). Scattered reads are admitted.
  static bool admit(bool isSequentialScan) { return !isSequentialScan; }

  size_t capacity() const { return frames_.size(); }

  // Pin the resident frame for `key` on a hit: increments the pin count,
  // sets the reference bit, and returns the frame. Returns nullptr on a
  // miss. The caller must call `unpin` once the bytes are copied.
  Frame* tryPin(const FrameKey& key);

  // Return the frame for `key`, allocating it on a miss: on a hit this pins
  // like `tryPin`; on a miss it evicts one unpinned frame via CLOCK (second
  // chance via the reference bit), installs `key` as loading, and returns it
  // pinned exactly once. The bool is true only for the caller that installed
  // the loading frame; only that caller may fill it. Existing loading frames
  // are also pinned, but must not be filled by another caller. Returns
  // {nullptr, false} when every frame is pinned (no evict-while-pinned).
  std::pair<Frame*, bool> allocate(const FrameKey& key);

  // Mark a loading frame resident after its buffer was filled. No-op when
  // the frame is not loading (e.g. after `abandon`).
  void markLoaded(const FrameKey& key);

  // Drop a loading frame after a failed fill so its slot can be reused.
  void abandon(const FrameKey& key);

  // Unpin a pinned frame. Asserts (contract-checks) that it is pinned.
  void unpin(const FrameKey& key);

  // Copy `numBytes` at `offsetInPage` out of a pinned frame. Returns false
  // when the frame is not resident (caller must unpin regardless).
  bool copyPinned(const FrameKey& key, size_t offsetInPage, size_t numBytes,
                  char* target);

  size_t residentCount() const;
  size_t pinnedCount() const;

 private:
  // CLOCK victim index among unpinned frames, or `frames_.size()` when every
  // frame is pinned. Caller must hold `mutex_`.
  size_t clockVictimLocked();
  Frame* findLocked(const FrameKey& key);

  std::vector<Frame> frames_;
  std::unordered_map<FrameKey, size_t, FrameKeyHash> index_;
  mutable std::mutex mutex_;
  size_t hand_ = 0;
};

// Confined ring buffers for single-pass scan traffic (Postgres discipline):
// bulk/export scans stream through these private slots and never enter the
// `VocabFramePool`, so a scan cannot evict the hot set. Fixed capacity,
// round-robin, thread-safe.
class ScanRingBuffer {
 public:
  static constexpr size_t kNumSlots = 8;
  static constexpr size_t kSlotSize = VocabFramePool::kPageSize;

  ScanRingBuffer();
  ScanRingBuffer(const ScanRingBuffer&) = delete;
  ScanRingBuffer& operator=(const ScanRingBuffer&) = delete;

  // Hand out the next slot (round-robin). The returned pointer stays valid
  // until the ring wraps around (at least `kNumSlots` further calls).
  char* next();

 private:
  std::vector<std::vector<char>> slots_;
  size_t next_ = 0;
  std::mutex mutex_;
};

// Small LRU of decompressed hot strings in front of FSST decode (which costs
// ~15% of export CPU). Keyed by the vocabulary index; stores the decompressed
// string. Thread-safe, bounded, evicts the least-recently-used entry.
class HotStringCache {
 public:
  explicit HotStringCache(size_t capacity = 512);
  HotStringCache(const HotStringCache&) = delete;
  HotStringCache& operator=(const HotStringCache&) = delete;

  std::optional<std::string> lookup(uint64_t vocabIndex) const;
  void insert(uint64_t vocabIndex, std::string value);
  size_t size() const;

 private:
  struct Entry {
    uint64_t index;
    std::string value;
  };
  size_t capacity_;
  mutable std::mutex mutex_;
  mutable std::list<Entry> lru_;
  mutable std::unordered_map<uint64_t, std::list<Entry>::iterator> index_;
};

// A read-only memory mapping of a file, together with one bit per page that
// records whether this process has read the page before ("known resident"),
// plus a reference bit per page for CLOCK eviction.
//
// Purpose: a small read that hits the page cache still costs a system call
// (`preadv2`), whose kernel work (page-cache lookup, permission and access
// time checks, copy) dominates the CPU time of lookups of many small,
// scattered words. `tryRead` serves a range whose pages are all known resident
// by copying it from the mapping, without a system call. All other ranges are
// read as before (page-cache fast path or `io_uring`); `markResident` then
// records their pages.
//
// A page that the kernel evicts after it was marked is not unmarked: reading
// it through the mapping then blocks until the page is read back (a major page
// fault), which is correct but synchronous. This only happens under memory
// pressure, for pages this process has read before.
//
// The explicit frame pool (see `VocabFramePool`) is an opt-in scheduled-fill
// path alongside the mapping: `enableFramePool` creates per-mapping frames,
// `fillFrameFromFile` fills them through the caller's reader (production:
// the existing io_uring ring), and `tryReadPooled` serves pinned copies with
// byte identity. The resident-bit path stays the default; nothing is retired.
//
// Thread safety: all member functions may be called concurrently.
class ResidentFileMapping {
 public:
  static constexpr size_t pageSize = 4096;

  // Fill callback for one page: copy `numBytes` at `fileOffset` into `dst`,
  // return true on success. Production wires this to
  // `BatchManagerBase::addBatch` + `wait` (single-page batch); tests use
  // `pread`.
  using FrameFillReader =
      std::function<bool(char* dst, uint64_t fileOffset, size_t numBytes)>;

  // Map the first `fileSize` bytes of `fd` read-only. If the mapping fails
  // (or `fileSize` is zero, or memory mappings are not available), the object
  // is valid but `isMapped()` is false and `tryRead` never serves a range.
  ResidentFileMapping(int fd, size_t fileSize);
  ResidentFileMapping() = default;
  ~ResidentFileMapping();

  ResidentFileMapping(const ResidentFileMapping&) = delete;
  ResidentFileMapping& operator=(const ResidentFileMapping&) = delete;
  ResidentFileMapping(ResidentFileMapping&& other) noexcept;
  ResidentFileMapping& operator=(ResidentFileMapping&& other) noexcept;

  bool isMapped() const { return data_ != nullptr; }

  // If the range `[offset, offset + numBytes)` lies within the file and all
  // its pages are known resident, copy it to `target` and return true.
  // Otherwise return false and do not touch `target`. An empty range is
  // always served.
  bool tryRead(uint64_t offset, size_t numBytes, char* target) const;

  // Record that the range `[offset, offset + numBytes)` has just been read, so
  // that its pages are in the page cache. Ranges beyond the end of the file
  // are ignored.
  void markResident(uint64_t offset, size_t numBytes) const;

  // Serve every read `i` (`numBytes[i]` bytes at `offsets[i]` into
  // `buffers[i]`) whose pages are known resident via `tryRead`, and return the
  // indices (ascending) of the reads that were not served.
  std::vector<size_t> tryReadAll(ql::span<const size_t> numBytes,
                                 ql::span<const uint64_t> offsets,
                                 ql::span<char*> buffers) const;

  // Bound the pages kept marked resident to `capPages` (0 means unbounded,
  // the default and current behavior). When marking would exceed the cap, a
  // CLOCK hand clears marked pages down to the cap and demotes them from the
  // page cache (`MADV_PAGEOUT` where available, else `MADV_DONTNEED`),
  // batched over contiguous runs. Demotion applies to clean pages; freshly
  // written dirty pages are left for writeback (vocabulary files are synced
  // at index build). This is the read-only subset of
  // VMCache-style explicit eviction (Leis et al., SIGMOD 2023): residency
  // stays in process state, but eviction no longer trusts the kernel, so a
  // marked page can never surprise the reader with an unbounded synchronous
  // fault storm under memory pressure.
  //
  // Eviction under a concurrent reader is transparent and needs no pinning:
  // the mapping is read-only, so a reader that loses its page to `DONTNEED`
  // faults it back and continues with correct data. The cost is one disk
  // read, exactly what the non-mapping path would have paid. The CLOCK hand
  // sweeps pages in file order from its saved page position. Pages read via
  // `tryRead` or marked via `markResident` get a second chance: the sweep
  // clears their reference bit and skips them, evicting unreferenced pages.
  //
  // Enforced when `markResident` adds pages and immediately when set, using
  // the resident bitmap to count marked pages. Concurrent accesses can keep
  // pages marked above the cap after the bounded sweep; adding pages or
  // setting the cap re-evaluates it.
  // May be called concurrently.
  void setResidentCapPages(size_t capPages) const;

  // The current cap in pages (0 means unbounded).
  size_t residentCapPages() const {
    return capPages_.load(std::memory_order_relaxed);
  }

  // Testing hook: the mapping base, for `mincore` probes. Do not read or
  // write through it outside tests; use `tryRead`. A second live mapping of
  // the same file pins its pages against cross-mapping pageout, so probes
  // must use this address (the single-mapping pattern).
  const char* mappingForTesting() const { return data_; }

  // `markResident` for the reads at `positions`.
  void markAllResident(ql::span<const size_t> numBytes,
                       ql::span<const uint64_t> offsets,
                       ql::span<const size_t> positions) const;

  // Create the opt-in explicit frame pool with `numFrames` 4 KiB frames for
  // this mapping (`fileId` tags the keys, default 0 = words file). Replaces
  // any existing pool. The resident-bit path keeps working unchanged.
  void enableFramePool(size_t numFrames, uint32_t fileId = 0) const;
  void disableFramePool() const;
  bool framePoolEnabled() const;
  uint32_t framePoolFileId() const {
    std::lock_guard<std::mutex> lock{framePoolMutex_};
    return framePoolFileId_;
  }

  // Fill the frame for `page` through `reader` (allocate pinned loading
  // frame, read the page bytes, mark resident). Returns false when `page` is
  // beyond the file, the pool is disabled, every frame is pinned, the frame
  // is already loading, or the reader fails. Byte-identity: the frame holds
  // an exact copy of the file page (immutable files, no dirty handling).
  bool fillFrameFromFile(uint64_t page, const FrameFillReader& reader) const;

  // Serve `[offset, offset + numBytes)` from resident pooled frames (pin,
  // copy, unpin each page). Returns false and touches nothing when the pool
  // is disabled, any page is not resident, or the range is out of bounds.
  // Empty ranges are served.
  bool tryReadPooled(uint64_t offset, size_t numBytes, char* target) const;

  // Direct pool access for tests and for the future shared-pool wiring.
  std::shared_ptr<VocabFramePool> framePool() const {
    std::lock_guard<std::mutex> lock{framePoolMutex_};
    return framePool_;
  }

 private:
  const char* data_ = nullptr;
  size_t size_ = 0;
  size_t numBitWords_ = 0;
  std::unique_ptr<std::atomic<uint64_t>[]> residentBits_;
  std::unique_ptr<std::atomic<uint64_t>[]> referenceBits_;
  // Mutable so const member functions can update the cap and CLOCK hand;
  // all updates are atomic. The hand is the next page to examine.
  mutable std::atomic<size_t> capPages_{0};
  mutable std::atomic<size_t> clockHand_{0};
  // Opt-in explicit frame pool (null when disabled); guarded so const reads
  // can lazily use it. The pool itself is internally synchronized.
  mutable std::mutex framePoolMutex_;
  mutable std::shared_ptr<VocabFramePool> framePool_;
  mutable uint32_t framePoolFileId_ = 0;

  // The pages `[firstPage, lastPage]` of a non-empty range within the file.
  std::pair<size_t, size_t> pagesOf(uint64_t offset, size_t numBytes) const {
    return {offset / pageSize, (offset + numBytes - 1) / pageSize};
  }
  void unmap();
  void evictDownTo(size_t target) const;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RESIDENTFILEMAPPING_H
