---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, qc]
last_verified: 2026-09-23
---
# QC9: Don't reinvent existing helpers

## Statement
Don't reinvent existing helpers: `ad_utility::HashMap`, `Cache.h`/`ConcurrentCache`, `AllocatorWithLimit`, `CancellationHandle`, `Synchronized`, `TaskQueue`, `MemorySize` (`_GB`/`_MB` literals), `stream_generator`, `Algorithm.h`, `BitUtils.h`, `ConstexprUtils.h`, `CtreHelpers.h`, `Exception.h`/`ExceptionHandling.h`. absl (`absl::StrCat`, absl containers) is in use — prefer it over manual string concatenation.

## Sources
- review: `.opencodereview/rule.json#210` QLever OCR fan-out project rule, file dated 2026-09-23.
