---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, qc]
last_verified: 2026-09-23
---
# QC5: Use `ql::` backports (`ql::span`, `ql::ranges::sort`, `ql::views::*` from …

## Statement
Use `ql::` backports (`ql::span`, `ql::ranges::sort`, `ql::views::*` from `src/backports/`) instead of `std::` equivalents where C++17 compat matters.

## Sources
- review: `.opencodereview/rule.json#193` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `backports`, `std::ranges`, `std::span`, `std::views`.
