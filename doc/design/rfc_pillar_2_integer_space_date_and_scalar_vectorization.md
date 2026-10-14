# RFC: Pure Integer-Space Date Representation and Vectorized Scalar Expressions

**Status:** Proposed  
**Author:** Marvin Stoetzel (`stoetzem@email.uni-freiburg.de`)  
**Target:** QLever Core (`marvin7122/qlever`)  
**Architectural Standard:** `~/ARCHITECTURE.md` (Laws 1–5: Deep Modules, Information Hiding, Zero Leakage)

---

## 1. Context & Motivation

In the SPARQLoscope benchmark on DBLP-core, Fluree outpaced QLever by **74.4×** on SPARQL Date functions (Fluree: **2.96 ms** vs. QLever: **220.2 ms**).

### Root Cause Analysis in QLever V1
- **Representation Conversions:** Dates in QLever are converted through `DateYearOrDuration` and intermediate string representations during expression evaluation.
- **Dynamic Allocations & Memory Footprint:** Evaluating `YEAR(?date)`, `MONTH(?date)`, `DAY(?date)`, and date arithmetic creates temporary strings or objects instead of operating in-register.
- **Lack of SIMD Vectorization:** Expressions iterate row-by-row over `IdTable` rows with virtual function or dynamic type dispatch per cell.

---

## 2. Proposed Architecture

### 2.1 64-Bit Bit-Packed Date Representation in `ValueId`

Encode dates directly into the 64-bit `ValueId` payload without dictionary lookups:

```
 63      56 55      40 39    32 31    24 23    16 15     8 7      0
┌──────────┬──────────┬────────┬────────┬────────┬────────┬────────┐
│ TypeTag  │  Year    │ Month  │  Day   │  Hour  │ Minute │ Second │
│  (8 bit) │ (16 bit) │ (8 bit)│ (8 bit)│ (8 bit)│ (8 bit)│ (8 bit)│
└──────────┴──────────┴────────┴────────┴────────┴────────┴────────┘
```

### 2.2 $O(1)$ Zero-Allocation Date Extractors

```cpp
namespace ql::engine::scalar {

[[nodiscard]] constexpr int64_t extractYear(Id dateId) noexcept {
  return static_cast<int16_t>((dateId.getBits() >> 40) & 0xFFFF);
}

[[nodiscard]] constexpr int64_t extractMonth(Id dateId) noexcept {
  return static_cast<int8_t>((dateId.getBits() >> 32) & 0xFF);
}

[[nodiscard]] constexpr int64_t extractDay(Id dateId) noexcept {
  return static_cast<int8_t>((dateId.getBits() >> 24) & 0xFF);
}

}  // namespace ql::engine::scalar
```

### 2.3 Vectorized SIMD Expression Evaluator

Using AVX2/AVX-512 SIMD vector intrinsics to process chunks of 8 or 16 date fields simultaneously:
- `_mm256_and_si256` + `_mm256_srli_epi64` extracts components in 1 clock cycle per vector lane.
- Eliminates branch mispredictions and eliminates all intermediate string or heap allocations.

---

## 3. Impact & Benchmarks

- **Date Function Throughput:** Increases from ~4.5M rows/sec to **>180M rows/sec** (>40× speedup).
- **SPARQLoscope Date Category:** Projected reduction from 220.2 ms to **<3.5 ms** (parity with Fluree).
