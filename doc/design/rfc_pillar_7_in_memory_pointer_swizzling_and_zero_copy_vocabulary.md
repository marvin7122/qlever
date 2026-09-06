# RFC: Pillar 7 — In-Memory Pointer Swizzling & Zero-Copy Vocabulary Buffer Pool

**Status:** Proposed  
**Author:** Marvin Stoetzel (`stoetzem@email.uni-freiburg.de`)  
**Target:** QLever Vocabulary V2 / Buffer Pool Manager  
**Applicability:** Disk-Backed Vocabulary Lookups, FSST Decompression, High-Concurrency Query Serving

---

## 1. Executive Summary & Problem Statement

Translating on-disk vocabulary IDs to string representations requires traversing page tables, computing binary search indices, or managing page eviction locks.

### Inefficiencies in Current Engine:
1. **Double Lookup Overhead:** Every vocab word lookup queries `VocabularyOnDisk` or internal codebook tables.
2. **Buffer Manager Lock Contention:** Shared locks on buffer frames cause cache ping-pong across multi-threaded workers.

---

## 2. Technical Specification: Pointer Swizzling Architecture

Adopted from **LeanStore** and TUM's **Umbra**, **Pointer Swizzling** dynamically transforms on-disk block references into direct memory pointers when pages are cached in RAM:

```
Unswizzled (On Disk):
+-------------------+----------------------------+-----------------------+
| Bit 63: 0 (Disk)  |   Block Index (31 bits)    |  Byte Offset (32 bits)|
+-------------------+----------------------------+-----------------------+

Swizzled (In Memory):
+-------------------+----------------------------------------------------+
| Bit 63: 1 (RAM)   |       Direct 64-Bit Virtual Memory Address (63b)   |
+-------------------+----------------------------------------------------+
```

```cpp
namespace ql::vocab {

class SwizzledVocabRef {
 private:
  uint64_t rawBits_;

  static constexpr uint64_t SWIZZLE_MASK = 1ULL << 63;
  static constexpr uint64_t ADDRESS_MASK = ~SWIZZLE_MASK;

 public:
  [[nodiscard]] constexpr bool isSwizzled() const noexcept {
    return (rawBits_ & SWIZZLE_MASK) != 0;
  }

  [[nodiscard]] const char* getDirectPointer() const noexcept {
    return reinterpret_cast<const char*>(rawBits_ & ADDRESS_MASK);
  }

  // Resolves the string in 1 single CPU cycle if swizzled
  [[nodiscard]] const char* resolve(BufferPool& pool) const {
    if (isSwizzled()) {
      return getDirectPointer();
    }
    return pool.resolveAndSwizzle(rawBits_);
  }
};

}  // namespace ql::vocab
```

---

## 3. Benefits & Guarantees
1. **Single-Cycle Dereference:** For memory-resident working sets, vocabulary resolution costs 1 CPU instruction (`mov rax, [ptr]`), eliminating hash tables and binary searches.
2. **Zero-Overhead Memory Scaling:** Evicted blocks seamlessly revert to unswizzled state upon buffer pool reclamation.
