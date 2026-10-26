---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP67: Do not overload `&&`, `||`, or `,`

## Statement
Do not overload `&&`, `||`, or `,` — they destroy short-circuit evaluation / sequencing; keep operators semantically unsurprising.

## Sources
- review: `.opencodereview/rule.json#65` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `operator`.
