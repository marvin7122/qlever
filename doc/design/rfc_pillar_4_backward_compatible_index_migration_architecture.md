# RFC: Backward-Compatible Index Migration & In-Place Conversion Architecture

**Status:** Proposed  
**Author:** Marvin Stoetzel (`stoetzem@email.uni-freiburg.de`)  
**Target:** QLever Core (`marvin7122/qlever`)  
**Architectural Standard:** `~/ARCHITECTURE.md` (Laws 1–5: Deep Modules, Information Hiding, Zero Leakage)

---

## 1. Context & Motivation

Upgrading QLever's index format (Pillar 1) must be non-disruptive. Real-world deployments host multi-terabyte indexes (`wikidata`, `dblp`, `freebase`) where re-indexing from raw RDF text dumps can take hours or days.

### Design Requirements
1. **Multi-Version Index Compatibility:** A single QLever engine binary must transparently read both Index V1 (legacy) and Index V2 (leaflet headers).
2. **Fast In-Place Metadata Migration:** Upgrading an existing V1 index to V2 must only scan and rewrite index metadata files, not re-sort triple data.
3. **Zero-Downtime Hot Swapping:** Support live index swaps via `IndexSwap` without restarting `qlever-server`.

---

## 2. Proposed Architecture

### 2.1 Format Versioning in Index Metadata

The `.meta-data.json` file records the index schema version:

```json
{
  "index_format_version": 2,
  "leaflet_headers_enabled": true,
  "num_triples": 561500000,
  "permutations": ["PSO", "POS", "SPO", "SOP", "OPS", "OSP"]
}
```

### 2.2 Deep Interface: `IndexReader` Abstraction

Following **Law 1 & 2 of `~/ARCHITECTURE.md` (Deep Modules & Zero Accounting Leakage)**:

```cpp
namespace ql::index {

class IndexReader {
 public:
  virtual ~IndexReader() = default;

  // Polymorphic block scanner hiding V1 vs V2 internal representations
  [[nodiscard]] virtual ScanResult scanRelation(const ScanSpecification& spec) const = 0;

  // Metadata queries transparently use V2 leaflet acceleration or fallback to V1 scans
  [[nodiscard]] virtual std::optional<uint64_t> tryGetExactDistinctCount(
      const ScanSpecification& spec) const noexcept = 0;
  
  [[nodiscard]] virtual std::optional<IdPair> tryGetMinMaxBoundary(
      const ScanSpecification& spec) const noexcept = 0;
};

}  // namespace ql::index
```

### 2.3 `IndexFormatConverter` Fast Path

The `IndexFormatConverter` utility upgrades a V1 index to V2 in minutes:
1. Open the existing `.index.*` data files read-only.
2. Read each 250,000-triple block sequentially to extract `min`, `max`, `datatypeBitmask`, and `distinctCount`.
3. Write the augmented `.index.*.meta` file and update `.meta-data.json`.
4. The underlying multi-gigabyte `.index.*` triple data files are modified zero times (100% data reuse).

---

## 3. Operational & Downtime Analysis

- **Full Re-index from RDF dump (DBLP):** ~15 minutes.
- **`IndexFormatConverter` Metadata Upgrade (DBLP):** **<12 seconds** (pure sequential disk read of block boundaries).
- **Online Transition:** Can be triggered live using `POST /api/index/swap` without connection drops.
