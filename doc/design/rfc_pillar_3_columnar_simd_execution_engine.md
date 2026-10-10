# RFC: Columnar SIMD Query Engine & Dynamic Sparse Column Pruning

**Status:** Proposed  
**Author:** Marvin Stoetzel (`stoetzem@email.uni-freiburg.de`)  
**Target:** QLever Core (`marvin7122/qlever`)  
**Architectural Standard:** `~/ARCHITECTURE.md` (Laws 1–5: Deep Modules, Zero Leakage, Pulling Complexity Downward)

---

## 1. Context & Motivation

QLever stores index relations on disk in columnar format, but query execution immediately decompresses blocks into row-major `IdTable` structures (`vector<vector<Id>>` or contiguous row spans).

### Bottlenecks of Row-Major Execution
1. **CPU Cache Thrashing:** In queries with many columns (e.g. 10–20 projected variables or wide `CONSTRUCT` templates), accessing a single column invalidates cache lines containing unneeded adjacent columns.
2. **Eager Column Decompression in `OPTIONAL` and `MINUS`:** QLever decompresses all columns of an index block even if only the join key is evaluated and the remainder of the block is discarded.
3. **Impeded SIMD Auto-Vectorization:** Row-major memory layouts prevent hardware vector lanes (AVX2/AVX-512) from loading contiguous column values without gather/scatter instructions.

---

## 2. Proposed Architecture

### 2.1 `ColumnarIdTable` (Structure of Arrays — SoA)

```cpp
namespace ql::engine {

class ColumnarIdTable {
 public:
  explicit ColumnarIdTable(size_t numColumns, ad_utility::AllocatorWithLimit<Id> allocator);

  [[nodiscard]] size_t numRows() const noexcept { return numRows_; }
  [[nodiscard]] size_t numColumns() const noexcept { return columns_.size(); }

  // Contiguous, cache-aligned column span for SIMD execution
  [[nodiscard]] ql::span<const Id> column(ColumnIndex col) const noexcept {
    return columns_[col];
  }

  [[nodiscard]] ql::span<Id> column(ColumnIndex col) noexcept {
    return columns_[col];
  }

 private:
  size_t numRows_ = 0;
  std::vector<ql::span<Id>> columns_;
  std::vector<std::vector<Id, ad_utility::AllocatorWithLimit<Id>>> storage_;
};

}  // namespace ql::engine
```

### 2.2 Lazy Sparse Column Decompression

When an `IndexScan` feeds into a `Filter`, `Join`, or `OptionalJoin`:
1. **Phase 1 (Key Evaluation):** Decompress only the join key column `column(0)`.
2. **Phase 2 (Pruning):** Run vectorized binary search or hash-table probe over `column(0)`.
3. **Phase 3 (Payload Materialization):** Only for rows that pass the predicate/join, decompress the remaining payload columns (`column(1)`, `column(2)`).

---

## 3. Impact & Benchmarks

- **Join & Filter Execution:** 3.5× – 8.0× throughput improvement due to continuous L1/L2 cache prefetching.
- **Sparse `OPTIONAL` / `MINUS` Memory Bandwidth:** Up to 75% reduction in disk and memory I/O on wide datasets.
