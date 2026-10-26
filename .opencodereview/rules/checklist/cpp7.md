---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP7: No UB patterns

## Statement
No UB patterns: dangling string_view/span, iterator invalidation, signed overflow, uninitialized reads.

## Sources
- review: `.opencodereview/rule.json#68` QLever OCR fan-out project rule, file dated 2026-09-23.
- doc: `Core Guidelines ES.65; Abseil ToTW #1` Cited for CPP7 by REVIEW_CHECKLIST.md.
