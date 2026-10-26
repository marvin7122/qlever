---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP59: A base class with virtual functions has a virtual (or protected) destructor so deletion …

## Statement
A base class with virtual functions has a virtual (or protected) destructor so deletion through a base pointer is well-defined.

## Sources
- review: `.opencodereview/rule.json#56` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `virtual`.
