---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, pitfall]
last_verified: 2026-09-22
---
# PP2: Missing/wrong copyright header

## Statement
Every new file has the modern QLever copyright header ("Copyright ... The QLever authors, in particular <Name> (UFR)", Apache 2.0), and the author added himself.

## Sources
- review: `.ai-review/personal_pitfalls.md#PP2` Mined from maintainer review comments, file dated 2026-09-22.
- review: `QLever PR #102` "The new file's copyright line uses `Copyright 2026, The` with a comma; use the current standard QLever header spelling."
- review: `QLever PR #162` "This updated source file still starts with the legacy three-line copyright header, unlike the repository's current header format."
- review: `QLever PR #2652` "When adding new files, also add the copyright message. Use the new syntax"
- review: `QLever PR #2765` "Check the format, the `Authors:` is redundant, and everyone of the people should have the `, UFR` after them."
- review: `QLever PR #2938` "Copyrtight header?"
- review: `QLever PR #121` "The license header here says \"compliance with the License\" while most other files in this PR spell out \"Apache 2.0 License\". For consistency and clarity (and to match other headers), please use the same Apache 2.0 wording."
- review: `QLever PR #95` "The file header was changed from the project-wide Apache 2.0 license header to an older short author comment, which is inconsistent with the repository's licensing headers and removes the explicit Apache 2.0 notice."
- review: `QLever PR #80` "The updated file header deviates from the project’s prevalent copyright header format in `src/engine` and introduces an inconsistent convention within the same module."
- review: `.opencodereview/rule.json#135` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Frequency in the mined comments: ~22 (of 639 review comments by joka921/hannahbast).
Fan-out rule wording: Missing/wrong copyright header: Every new file has the modern QLever copyright header ("Copyright ... The QLever authors, in particular <Name> (UFR)", Apache 2.0), and the author added himself.
