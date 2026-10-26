---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP2: No owning raw pointers / manual new-delete

## Statement
No owning raw pointers / manual new-delete; ownership via unique_ptr/values.

## Sources
- review: `.opencodereview/rule.json#12` QLever OCR fan-out project rule, file dated 2026-09-23.
- doc: `Core Guidelines R.11, R.20-21` Cited for CPP2 by REVIEW_CHECKLIST.md.

## Notes
Fan-out review trigger words: `\bnew\b`, `delete`, `make_shared`, `make_unique`, `shared_ptr`, `unique_ptr`.
