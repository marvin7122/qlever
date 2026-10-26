---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, style, headers]
last_verified: 2026-10-26
---
# Headers use path-derived `QLEVER_..._H` include guards, and includes are repository-relative with the own header first

## Statement
A header is guarded by `#ifndef QLEVER_<PATH>_H` / `#define QLEVER_<PATH>_H`, where
`<PATH>` is the file's path from the repository root, upper-cased, with separators and dots
replaced by underscores (`src/util/Timer.h` becomes `QLEVER_SRC_UTIL_TIMER_H`), and closed
by `#endif  // QLEVER_<PATH>_H`. `#pragma once` is not used. Project headers are included
with quotes and a path relative to `src/` (`#include "engine/Operation.h"`), never
relative to the including file; standard and third-party headers use angle brackets. In a
`.cpp`, the own header is the first include, followed by a blank line, then the remaining
blocks; clang-format sorts within each block.

## Sources
- git-history: `src/util/*.h` 119 of 120 headers use the `QLEVER_...` guard; the form is `#ifndef QLEVER_SRC_UTIL_TIMER_H` ... `#endif  // QLEVER_SRC_UTIL_TIMER_H`.
- git-history: `src/engine/Join.cpp` Own header `"engine/Join.h"` first, blank line, then `"engine/JoinImpl.h"`.
- review: `review-categories/includes.md` Missing, unused, or non-repository-relative includes are a review finding.
- review: `checklist/qc25.md` What belongs in a header at all.
