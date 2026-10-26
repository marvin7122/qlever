---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP69: Assignment operators return `*this` by reference and handle self-assignment

## Statement
Assignment operators return `*this` by reference and handle self-assignment; keep copy / move / assign consistent.

## Sources
- review: `.opencodereview/rule.json#67` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `operator=`.
