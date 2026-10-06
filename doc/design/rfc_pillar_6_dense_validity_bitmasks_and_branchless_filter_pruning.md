# RFC: Pillar 6 — 64-Bit Dense Validity Bitmasks & Branchless Filter Pruning

**Status:** Proposed  
**Author:** Marvin Stoetzel (`stoetzem@email.uni-freiburg.de`)  
**Target:** QLever Columnar Execution Engine V2 / `IdTable` V2  
**Applicability:** Optional Joins, Null/Undef Handling, SIMD Vector Filters

---

## 1. Executive Summary & Problem Statement

SPARQL queries with `OPTIONAL` clauses, `BOUND(?x)` tests, or `COALESCE` expressions generate unbound (`ValueId::makeUndefined()`) entries in tabular results.

### Inefficiencies in Current Engine:
1. **Branch Mispredictions:** Every filter condition or relational comparison checks `id == ValueId::makeUndefined()` on a per-row basis.
2. **SIMD Vectorization Obstacle:** Per-row conditionals prevent tight auto-vectorization across numeric and date columns.

---

## 2. Technical Specification: Dense Validity Bitmask Representation

Adopted from **DuckDB** and **Apache Arrow**, every column in `ColumnarIdTable` is paired with an optional 64-bit dense validity bitmask (`ValidityBitmask`):

```
Row Index:        0  1  2  3  4  5 ... 63
Bit Representation: 1  1  0  1  1  1 ...  1  (uint64_t word)
Meaning:          Valid (1) | Undef (0)
```

```cpp
namespace ql::columnar {

class ValidityBitmask {
 public:
  static constexpr uint64_t ALL_VALID = 0xFFFFFFFFFFFFFFFFULL;
  static constexpr uint64_t ALL_NULL = 0x0ULL;

 private:
  std::vector<uint64_t> maskWords_;
  size_t numRows_ = 0;

 public:
  explicit ValidityBitmask(size_t numRows, bool allValid = true)
      : maskWords_((numRows + 63) / 64, allValid ? ALL_VALID : ALL_NULL),
        numRows_(numRows) {}

  [[nodiscard]] bool isValid(size_t rowIndex) const noexcept {
    return (maskWords_[rowIndex / 64] & (1ULL << (rowIndex % 64))) != 0;
  }

  void setInvalid(size_t rowIndex) noexcept {
    maskWords_[rowIndex / 64] &= ~(1ULL << (rowIndex % 64));
  }

  // Branchless 64-row evaluation
  template <typename SimdPredicateOp>
  void applyFilter(size_t wordIndex, SimdPredicateOp&& op, uint64_t& resultMask) const noexcept {
    const uint64_t valid = maskWords_[wordIndex];
    if (valid == ALL_NULL) {
      resultMask = ALL_NULL;
      return;
    }
    resultMask = op(wordIndex) & valid;
  }

  // Fast morsel check: returns true if all rows in the 64-row block are valid
  [[nodiscard]] bool isAllValid(size_t wordIndex) const noexcept {
    return maskWords_[wordIndex] == ALL_VALID;
  }
};

}  // namespace ql::columnar
```

---

## 3. Impact on SIMD Filter Pipelines
- **64 Rows Processed in 1 Instruction:** Evaluating `IS NOT NULL` / `BOUND(?x)` requires 1 bitwise `AND` per 64 rows.
- **Morsel Skipping:** When `maskWords_[wordIndex] == 0`, the SIMD comparison kernel skips decompression and arithmetic entirely.
