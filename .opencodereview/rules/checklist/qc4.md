---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, qc]
last_verified: 2026-09-23
---
# QC4: Read-only sequence parameters are `ql::span<const T>`, never `const std::vector<T>&`

## Statement
Read-only sequence parameters are `ql::span<const T>`, never `const std::vector<T>&`. Beware `size_t` vs `ColumnIndex` element-type mismatch (differ on wasm even at 64 bits); prefer domain typedef `ColumnIndex` for column indices.

## Sources
- review: `.opencodereview/rule.json#182` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `span`, `std::vector`, `vector`.
