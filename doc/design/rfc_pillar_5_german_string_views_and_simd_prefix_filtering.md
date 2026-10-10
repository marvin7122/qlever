# RFC: Pillar 5 — 12-Byte Inlined "German Strings" & SIMD Prefix Filtering

**Status:** Proposed  
**Author:** Marvin Stoetzel (`stoetzem@email.uni-freiburg.de`)  
**Target:** QLever Engine V2 / Vocabulary V2  
**Applicability:** In-Memory String Views, Vocabulary Iterators, Expression Filters, Result Serializers

---

## 1. Executive Summary & Problem Statement

In the current QLever architecture, string representation during query execution, expression evaluation (e.g. `FILTER(STRSTARTS(?x, "http"))`), and vocabulary lookup relies on `std::string_view` (16 bytes: `const char*` pointer + `size_t` length) pointing to external memory buffers.

### Core Inefficiencies:
1. **Pointer Dereference Overhead:** Checking equality (`?x = "value"`), ordering (`?x < "value"`), or prefix matching (`STRSTARTS`) requires traversing memory pointers into vocabulary arenas or decompression buffers, causing L1/L2 data cache misses on every cell.
2. **String Sorting Overhead:** Sorting string columns requires following pointers for every comparison in `std::sort` / `parallel_sort`.

---

## 2. Technical Specification: The 16-Byte German String Layout

Adopted from **DuckDB** and TUM's **Umbra**, the `GermanStringView` is a 16-byte value-type structure that inlines short strings and prefixes directly inside CPU registers:

```
Short String (<= 12 bytes):
+-------------------+----------------------------------------------------+
|  Length (4 bytes) |      Inlined ASCII / UTF-8 Payload (12 bytes)       |
+-------------------+----------------------------------------------------+

Long String (> 12 bytes):
+-------------------+-------------------+--------------------------------+
|  Length (4 bytes) |  Prefix (4 bytes) |   External Data Pointer (8B)   |
+-------------------+-------------------+--------------------------------+
```

```cpp
namespace ql::string {

class alignas(16) GermanStringView {
 public:
  static constexpr size_t INLINE_CAPACITY = 12;
  static constexpr size_t PREFIX_CAPACITY = 4;

 private:
  uint32_t length_;
  union {
    // Inlined string payload for length <= 12
    char inlined_[INLINE_CAPACITY];
    struct {
      // First 4 bytes cached directly in register space
      char prefix_[PREFIX_CAPACITY];
      // Pointer to external zero-copy arena or vocabulary buffer
      const char* data_;
    } external_;
  };

 public:
  [[nodiscard]] constexpr uint32_t length() const noexcept { return length_; }
  [[nodiscard]] constexpr bool isInlined() const noexcept {
    return length_ <= INLINE_CAPACITY;
  }

  [[nodiscard]] constexpr uint32_t prefixAsInt() const noexcept {
    uint32_t val = 0;
    std::memcpy(&val, isInlined() ? inlined_ : external_.prefix_, sizeof(uint32_t));
    return val;
  }

  [[nodiscard]] const char* data() const noexcept {
    return isInlined() ? inlined_ : external_.data_;
  }

  [[nodiscard]] std::string_view stringView() const noexcept {
    return {data(), length_};
  }

  // O(1) in-register string equality check
  [[nodiscard]] bool operator==(const GermanStringView& other) const noexcept {
    if (length_ != other.length_) {
      return false;
    }
    // Fast path: compare first 4 bytes in a single 32-bit register check
    if (prefixAsInt() != other.prefixAsInt()) {
      return false;
    }
    if (isInlined()) {
      return std::memcmp(inlined_ + 4, other.inlined_ + 4, length_ - 4) == 0;
    }
    return std::memcmp(external_.data_ + 4, other.external_.data_ + 4, length_ - 4) == 0;
  }
};

}  // namespace ql::string
```

---

## 3. Vectorized AVX2 Prefix & Equality Filtering

For batch filtering over a `ColumnarIdTable` or vocabulary string buffer:
- Pack 8 string prefixes into a 256-bit AVX2 register (`__m256i`).
- Compare against target prefix `_mm256_cmpeq_epi32`.
- Generates an 8-lane bitmask in a single clock cycle, completely skipping non-matching long strings without reading RAM.
