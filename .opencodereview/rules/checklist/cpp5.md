---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP5: Special members

## Statement
Special members: rule of zero preferred; if any of dtor/copy/move is user-defined, all are considered.

## Sources
- review: `.opencodereview/rule.json#45` QLever OCR fan-out project rule, file dated 2026-09-23.
- doc: `Core Guidelines C.20, C.21` Cited for CPP5 by REVIEW_CHECKLIST.md.

## Notes
Fan-out review trigger words: `= default`, `= delete`.
