---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP21: A function claiming the strong guarantee (commit-or-rollback) must not mutate observable …

## Statement
A function claiming the strong guarantee (commit-or-rollback) must not mutate observable state before the throwing step; a basic-guarantee function must document which members are left valid-but-unspecified after a throw.

## Sources
- review: `.opencodereview/rule.json#14` QLever OCR fan-out project rule, file dated 2026-09-23.
