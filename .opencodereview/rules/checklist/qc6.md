---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, qc]
last_verified: 2026-09-23
---
# QC6: Template constraints, type traits, and concepts use `src/backports/concepts.h` …

## Statement
Template constraints, type traits, and concepts use `src/backports/concepts.h` (`CPP_requires`, `CPP_concept`, `CPP_requires_ref`, `CPP_template`, `CPP_member`) rather than raw C++20 `requires` expressions or raw `std::void_t` SFINAE structs, ensuring clean semantic names and dual C++17/C++20 compatibility.

## Sources
- review: `.opencodereview/rule.json#204` QLever OCR fan-out project rule, file dated 2026-09-23.
