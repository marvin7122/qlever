---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP3: Avoid raw pointers for non-owning access

## Statement
Avoid raw pointers for non-owning access: use references for non-null single-object params/returns, `ql::span`/`gsl::span` for contiguous ranges, and smart pointers for optional ownership. A raw `T*` is not the default way to pass data — if it can be null, prefer `unique_ptr`/`shared_ptr` or an explicit `std::optional<std::reference_wrapper<T>>`.

## Sources
- review: `.opencodereview/rule.json#23` QLever OCR fan-out project rule, file dated 2026-09-23.
- doc: `Core Guidelines R.3, F.9; Abseil ToTW #1, #93` Cited for CPP3 by REVIEW_CHECKLIST.md.
