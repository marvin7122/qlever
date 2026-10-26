---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP34: Prefer concepts (`requires`) over `std::enable_if`/SFINAE for constraints

## Statement
Prefer concepts (`requires`) over `std::enable_if`/SFINAE for constraints — clearer and better diagnostics; when SFINAE is needed, keep the failure in the immediate context (no hard error).

## Sources
- review: `.opencodereview/rule.json#28` QLever OCR fan-out project rule, file dated 2026-09-23.
