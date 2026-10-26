---
kind: rule
severity: MUST
language: cpp
tags: [qlever, style, formatting]
last_verified: 2026-10-26
---
# Formatting is clang-format 18 with the repository's `.clang-format`, Google style at 80 columns

## Statement
All C++ under `src/`, `test/`, and `benchmark/` is formatted by clang-format 18 with the
`.clang-format` at the repository root, and CI rejects any file the formatter would change.
The configuration is Google style with these deliberate changes: pointers and references
bind to the type (`T* p`, `const T& r`), line endings are LF with a newline at end of file,
and a `requires` clause stays on a single line without extra indentation. Everything else
follows the Google base: 80-column hard wrap, two-space indentation, no tabs, attached
braces, and includes sorted within their blocks. Do not hand-format against the tool, do
not disable it with `// clang-format off` for taste, and do not reformat lines the change
does not otherwise touch.

## Sources
- tool-config: `.clang-format` Repository root; `BasedOnStyle: Google`, `PointerAlignment: Left`, `LineEnding: LF`, `InsertNewlineAtEOF: true`, `RequiresClausePosition: SingleLine`, `IndentRequiresClause: false`.
- tool-config: `misc/format-check.sh` The CI check: runs `clang-format-18 -output-replacements-xml` over every source file and fails on any replacement.
- doc: [Google C++ Style Guide: Formatting](https://google.github.io/styleguide/cppguide.html#Formatting) The base style, including the 80-column limit and two-space indent.
- review: `pitfalls/pp3.md` No reformatting of lines whose logic is unchanged.

## Notes
Run `misc/format-check.sh` or `clang-format-18 -i <files>` before committing. The `.clang-format` carries a maintainer TODO to update to clang-format 17 macro support for the `CPP_...` template macros; until then those are listed as `AttributeMacros`.
