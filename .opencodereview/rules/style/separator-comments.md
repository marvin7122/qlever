---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, style, comments]
last_verified: 2026-10-26
---
# Every out-of-line function definition and every test case sits under an 80-column `// ____` separator

## Statement
In `.cpp` files, each out-of-line function definition (free functions, member functions,
constructors, destructors, operators, template instantiations) is preceded by one separator
line: two slashes, a space, and 77 underscores, 80 columns in total. A group of
consecutive one-line defaulted members may share one separator. Test cases in `test/`
get the same separator above each `TEST`, `TEST_F`, or `TEST_P`. The separator is bare:
the documenting comment lives on the declaration in the header, so a definition in the
`.cpp` normally has nothing between the separator and its first line. The separator is a
deliberate project convention; a review does not flag a correctly placed one as redundant.

## Sources
- git-history: `src/engine/*.cpp, src/util/*.cpp` 1,161 of 1,172 separators are followed directly by the definition; 842 of 1,009 are exactly 80 columns, the rest are older 78- or 79-column variants.
- git-history: `test/*.cpp` 1,436 of 2,561 test cases carry the separator; new tests add it.
- review: `pitfalls/pp1.md` "every function/test has at least a dummy `// ____...` separator comment", the most frequent review comment in the mined corpus.
- review: `.opencodereview/rule.json#0` The fan-out preamble: QLever uses section-divider comments intentionally; do not flag a correctly placed divider as redundant.

## Notes
The exact line is `// ` followed by 77 underscores. Copy it from any `.cpp` rather than counting.
