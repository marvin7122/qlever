# RFC: Pillar 8 — Deterministic Direct I/O (`O_DIRECT`) & Static Ring Arenas

**Status:** Proposed  
**Author:** Marvin Stoetzel (`stoetzem@email.uni-freiburg.de`)  
**Target:** QLever I/O Backend / Storage Engine V2  
**Applicability:** NVMe Asynchronous Reads, `io_uring` Pipelines, Zero-Garbage-Collection Serving

---

## 1. Executive Summary & Problem Statement

Standard buffered POSIX file reads (`pread`) rely on the Linux Page Cache, introducing non-deterministic OS background writebacks, memory fragmentation, and double-buffering (kernel pages duplicated in user-space buffers).

---

## 2. Technical Specification: TigerBeetle-Style Direct DMA & Static Allocations

Adopted from **TigerBeetle**:
1. **Zero Dynamic Allocation:** All memory pools and `io_uring` completion rings are statically allocated at startup.
2. **`O_DIRECT` 4096-Byte Sector Alignment:** Direct DMA transfers from NVMe SSDs into aligned user-space arenas.

```cpp
namespace ql::io {

class DirectIoEngine {
 public:
  static constexpr size_t SECTOR_SIZE = 4096;

 private:
  int directFd_ = -1;
  alignas(SECTOR_SIZE) std::vector<uint8_t> staticBufferRing_;

 public:
  void openDirect(const std::string& path) {
    directFd_ = ::open(path.c_str(), O_RDONLY | O_DIRECT | O_NOATIME);
    if (directFd_ < 0) {
      throw std::system_error(errno, std::generic_category(), "open O_DIRECT failed");
    }
  }

  // Issue zero-copy asynchronous DMA read directly into pre-aligned chunk
  void submitDirectRead(struct io_uring* ring, off_t fileOffset, size_t numBytes, uint8_t* alignedDest) {
    AD_CORRECTNESS_CHECK(reinterpret_cast<uintptr_t>(alignedDest) % SECTOR_SIZE == 0);
    AD_CORRECTNESS_CHECK(fileOffset % SECTOR_SIZE == 0);

    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_read(sqe, directFd_, alignedDest, numBytes, fileOffset);
  }
};

}  // namespace ql::io
```

---

## 3. Benefits & Performance Invariants
1. **Elimination of Page Cache Thrashing:** Zero page evictions or kernel locks when scanning 500GB+ indexes.
2. **Deterministic Tail Latency:** Sub-millisecond $p99.9$ query latency on NVMe flash storage.
