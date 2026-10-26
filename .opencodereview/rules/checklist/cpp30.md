---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP30: Weaker memory orders (`memory_order_relaxed`/`acquire`/`release`) on atomics must be …

## Statement
Weaker memory orders (`memory_order_relaxed`/`acquire`/`release`) on atomics must be provably correct; default to `seq_cst` unless you can justify that a weaker order guards no other shared data.

## Sources
- review: `.opencodereview/rule.json#24` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `memory_order`.
