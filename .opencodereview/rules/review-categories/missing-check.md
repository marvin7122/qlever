---
kind: guideline
tags: [qlever, review-category]
last_verified: 2026-09-23
---
# Review category: missing-check

## Statement
missing AD_CHECK/assert or unhandled edge case; new preconditions in non-test code must use the assertion macros from PR #71 (AD_CONTRACT_CHECK for caller obligations, AD_CORRECTNESS_CHECK for internal invariants), never hand-rolled if-throw/AD_THROW guards

## Sources
- review: `.opencodereview/rule.json#212` Finding category of the QLever OCR fan-out runner, file dated 2026-09-23.
