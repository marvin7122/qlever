---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP25: Shared mutable state across threads requires a mutex or atomic

## Statement
Shared mutable state across threads requires a mutex or atomic; a plain variable written by one thread and read by another without synchronization is a data race (UB).

## Sources
- review: `.opencodereview/rule.json#18` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `atomic`, `jthread`, `mutex`, `thread`.
