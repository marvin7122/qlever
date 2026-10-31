#!/usr/bin/env python3
"""Deterministically checkable style rules, enforced at commit time.

These rules used to be checked only by the (non-deterministic) LLM
review fan-out. Everything a line-based check can decide reliably lives
here instead; the LLM rules keep the judgement calls (see CPP73-CPP75
in the review corpus).

Checks (on added lines only in `--staged` mode):
  CPP73  No type alias that merely renames a primitive type
         (`using BatchHandle = uint64_t;`) or `std::string` - use a
         wrapper type.
         Escape: `// alias-allowed: <reason>` on the same line.
  CPP74  A `virtual` destructor that is `= default`ed, `= delete`d, or
         pure virtual must have an intent comment (the contiguous
         comment block directly above it, or a trailing comment).
  CPP75  An `explicit` constructor must have an intent comment, in the
         same sense as CPP74.
  CPP51  No plain `enum`; use `enum class`.
         Escape: `// rule-allowed: <reason>` on the same line.
  QC64   A `namespace detail {` opening must state why its contents are
         in `detail` (comment block directly above it, or directly
         inside it). Presence only; whether the stated reason is true
         stays an LLM/human judgement (QC63/QC64 in the corpus).

Usage:
  check-deterministic-rules.py --staged     # lines added in the index
  check-deterministic-rules.py FILE ...     # whole files (for testing)

Exit code 1 (and one line per finding) iff a violation is found.
Python 3 standard library only; deterministic.
"""

import re
import subprocess
import sys
from pathlib import Path

PRIMITIVE = (
    r"(?:bool|char|char8_t|char16_t|char32_t|wchar_t|"
    r"(?:signed|unsigned)\s+(?:char|short(?:\s+int)?|int|long(?:\s+long)?(?:\s+int)?)|"
    r"short(?:\s+int)?|int|long(?:\s+long)?(?:\s+int)?|float|double|long\s+double|"
    r"(?:std::)?(?:u?int(?:8|16|32|64)_t|size_t|ptrdiff_t|intptr_t|uintptr_t)|"
    r"std::string|std::basic_string<char>)"
)
USING_RE = re.compile(
    rf"^\s*using\s+(?P<name>[A-Za-z_]\w*)\s*=\s*(?:const\s+)?{PRIMITIVE}\s*;")
TYPEDEF_RE = re.compile(
    rf"^\s*typedef\s+(?:const\s+)?{PRIMITIVE}\s+(?P<name>[A-Za-z_]\w*)\s*;")
VIRTUAL_DTOR_RE = re.compile(
    r"\bvirtual\s+~\w+\s*\(\s*\)\s*(?:=\s*(?:default|delete)|=\s*0)")
EXPLICIT_CTOR_RE = re.compile(r"^\s*explicit\s+[A-Z]\w*\s*\(")
PLAIN_ENUM_RE = re.compile(r"^\s*enum\s+(?!class\b|struct\b)[A-Z]\w*\s*(?:\{|;)")
DETAIL_NS_RE = re.compile(r"^\s*namespace\s+(?:[\w:]+::)?detail\s*\{")
CXX_EXTENSIONS = (".h", ".hpp", ".cpp", ".cc", ".cxx")


def has_intent_comment(lines, idx):
    """True iff line `idx` has a trailing comment or a comment block
    directly above it (blank lines and attributes may intervene)."""
    if "//" in lines[idx]:
        return True
    for line in reversed(lines[max(0, idx - 8):idx]):
        stripped = line.strip()
        if stripped.startswith("//") or stripped.startswith("/*"):
            return True
        if stripped == "" or stripped.startswith("[["):
            continue
        return False
    return False


def check_line(lines, idx):
    """Return a list of (rule, message) violations for line `idx`."""
    line = lines[idx]
    if "rule-allowed:" in line:
        return []
    code = line.split("//", 1)[0]
    findings = []
    match = USING_RE.match(code) or TYPEDEF_RE.match(code)
    if match and "alias-allowed:" not in line:
        findings.append((
            "CPP73",
            f"alias `{match.group('name')}` renames a primitive type or "
            "`std::string`; use "
            "a wrapper type instead, or mark an intentional interop alias "
            "with `// alias-allowed: <reason>`"))
    if VIRTUAL_DTOR_RE.search(code) and not has_intent_comment(lines, idx):
        findings.append((
            "CPP74",
            "virtual destructor without an intent comment: state why it "
            "is virtual and why it is defaulted/deleted/pure (CPP74)"))
    if EXPLICIT_CTOR_RE.match(code) and not has_intent_comment(lines, idx):
        findings.append((
            "CPP75",
            "`explicit` constructor without an intent comment: state "
            "which silent conversion it forbids and why (CPP75)"))
    if DETAIL_NS_RE.match(code) and not (
        has_intent_comment(lines, idx)
        or any(l.strip().startswith("//") for l in lines[idx + 1:idx + 4])
    ):
        findings.append((
            "QC64",
            "`detail` namespace without a reason comment: state why the "
            "part is not public interface and who its only users are "
            "(QC64)"))
    if PLAIN_ENUM_RE.match(code):
        findings.append((
            "CPP51",
            "plain `enum`; use `enum class`, or mark the exception with "
            "`// rule-allowed: <reason>`"))
    return findings


def scan(lines, label, only=None):
    findings = []
    for idx, _line in enumerate(lines):
        if only is not None and (idx + 1) not in only:
            continue
        for rule, message in check_line(lines, idx):
            findings.append(f"{label}:{idx + 1}: [{rule}] {message}")
    return findings


def staged_added_lines():
    """Map each staged C++ file to the set of its added line numbers,
    together with the staged file contents."""
    diff = subprocess.run(
        ["git", "diff", "--cached", "--unified=0", "--diff-filter=ACM",
         "--", "*.h", "*.hpp", "*.cpp", "*.cc", "*.cxx"],
        capture_output=True, text=True, check=True).stdout.splitlines()
    added = {}
    path, lineno = None, 0
    for line in diff:
        if line.startswith("+++ b/"):
            path = line[6:]
            added.setdefault(path, set())
        elif line.startswith("@@ ") and path:
            lineno = int(re.search(r"\+(\d+)", line).group(1))
        elif line.startswith("+") and not line.startswith("+++") and path:
            added[path].add(lineno)
            lineno += 1
        elif not line.startswith("-") and path:
            lineno += 1
    result = {}
    for path, numbers in added.items():
        if not numbers:
            continue
        content = subprocess.run(
            ["git", "show", f":{path}"],
            capture_output=True, text=True, check=True).stdout
        result[path] = (content.splitlines(), numbers)
    return result


def main(argv):
    findings = []
    if argv[:1] == ["--staged"]:
        for path, (lines, numbers) in sorted(staged_added_lines().items()):
            findings += scan(lines, path, only=numbers)
    else:
        for name in argv:
            if Path(name).suffix in CXX_EXTENSIONS:
                findings += scan(Path(name).read_text().splitlines(), name)
    for finding in findings:
        print(finding)
    if findings:
        print(f"{len(findings)} deterministic rule violation(s); "
              "see misc/check-deterministic-rules.py", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
