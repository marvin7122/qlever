# RFC: Index Format V2 — Leaflet Metadata Headers, Exact Cardinalities & Typed Pruning

**Status:** Proposed  
**Author:** Marvin Stoetzel (`stoetzem@email.uni-freiburg.de`)  
**Target:** QLever Core (`marvin7122/qlever`)  
**Architectural Standard:** `~/ARCHITECTURE.md` (Laws 1–5: Deep Modules, Zero Leakage, Pulling Complexity Downward, Invariants)

---

## 1. Context & Motivation

In the published SPARQLoscope comparative benchmark over DBLP-core (~561.5M triples), Fluree demonstrated a geometric-mean advantage over QLever on queries answering single-predicate aggregates and type filters:
- `SELECT (COUNT(DISTINCT ?object) AS ?c) { ?s dblp:hasSignature ?o }`: Fluree **0.81 ms** vs. QLever **12.9 s** (15,900× gap).
- `SELECT (MIN(?o) AS ?min) { ?s dblp:numberOfCreators ?o }`: Fluree **0.57 ms** vs. QLever **79.0 ms** (138× gap).
- `SELECT (COUNT(?o) AS ?c) { ?s ?p ?o . FILTER ISLITERAL(?o) }`: Fluree **0.60 ms** vs. QLever **3,484 ms** (5,800× gap).

### Root Cause Analysis in QLever V1
1. **Float Multiplicity Loss:** `CompressedRelationMetadata` stores object multiplicity as a `float`. Floating-point values cannot reconstruct exact integer counts for large distinct relations without performing a full sequential index scan.
2. **Missing Boundary Keys:** Compressed blocks in QLever's index do not record the minimum and maximum `ValueId` per column. Aggregates like `MIN(?o)` / `MAX(?o)` must decompress and scan the entire relation.
3. **Absence of Type Pruning:** To count literals, blank nodes, or IRIs, QLever must scan every triple in the index range and evaluate `isLiteral()` per row.

---

## 2. Proposed Architecture: Index Format V2

### 2.1 Enriched Block Metadata (`CompressedBlockMetadataV2`)

Each compressed index block header is augmented with compact metadata:

```cpp
struct CompressedBlockMetadataV2 {
  // Existing byte offsets
  off_t offsetInFile_;
  size_t compressedSizeFirstColumn_;
  size_t compressedSizeSecondColumn_;
  size_t numRows_;

  // V2 Additions:
  // 1. Exact Column Min / Max Boundary Keys
  Id firstColMin_;
  Id firstColMax_;
  Id secondColMin_;
  Id secondColMax_;

  // 2. Exact Integer Distinct Counts
  uint32_t numDistinctFirstCol_;
  uint32_t numDistinctSecondCol_;

  // 3. Datatype Presence Bitmask (1 byte)
  // Bits: [0: Iri, 1: Literal, 2: Int, 3: Double, 4: Date, 5: BlankNode, 6: VocabWord, 7: Reserved]
  uint8_t datatypeBitmaskCol0_;
  uint8_t datatypeBitmaskCol1_;
};
```

### 2.2 Execution Pushdown Algorithms

1. **$O(1)$ Min/Max Pushdown:**
   - On sorted relations (`PSO`, `POS`, `SPO`, `SOP`, `OPS`, `OSP`), the query planner detects when `MIN(?x)` or `MAX(?x)` is requested over a single bound prefix.
   - For `MIN`: Returns `blocks.front().secondColMin_` in $O(1)$ without reading or decompressing the index data.
   - For `MAX`: Returns `blocks.back().secondColMax_` in $O(1)$.

2. **$O(\text{blocks})$ Typed Aggregates (`COUNT` + `ISLITERAL` / `ISBLANK`):**
   - For each block in the relation, inspect `datatypeBitmask`.
   - If the block contains exclusively target types (e.g. `datatypeBitmask == LITERAL_MASK`), add `numRows_` directly to the accumulator without decompressing the block.
   - If the block contains zero target types (`datatypeBitmask & LITERAL_MASK == 0`), skip the block entirely.
   - Only mixed blocks require decompression and row-level filtering.

3. **$O(1)$ Exact Distinct Counts:**
   - Single-predicate `COUNT(DISTINCT ?o)` queries query `metadata.numDistinctSecondCol_` directly from relation metadata in **<1 ms**.

---

## 3. Storage Overhead & Benchmarks

- **Metadata Size Increase:** +28 bytes per 250,000-triple block (<0.02% index size increase).
- **Projected Latency Reduction:**
  - `COUNT(DISTINCT ?o)` on low multiplicity: **12.9 s $\rightarrow$ 0.8 ms** (~16,000× speedup).
  - `MIN(?o)` / `MAX(?o)`: **79 ms $\rightarrow$ 0.1 ms** (~790× speedup).
  - `ISLITERAL` count scan: **3.5 s $\rightarrow$ 0.9 ms** (~3,800× speedup).
