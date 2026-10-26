---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP65: No uninitialized variables

## Statement
No uninitialized variables — always initialize (default-member-init or in the constructor); reading uninitialized memory is UB.

## Sources
- review: `.opencodereview/rule.json#63` QLever OCR fan-out project rule, file dated 2026-09-23.
