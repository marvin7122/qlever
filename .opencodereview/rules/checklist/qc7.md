---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, qc]
last_verified: 2026-09-23
---
# QC7: `[[maybe_unused]]` on functions whose returned object should always be used

## Statement
`[[maybe_unused]]` on functions whose returned object should always be used; no dead/unused parameters left after refactors (remove and update call sites, or `[[maybe_unused]]` if signature stability matters).

## Sources
- review: `.opencodereview/rule.json#208` QLever OCR fan-out project rule, file dated 2026-09-23.
