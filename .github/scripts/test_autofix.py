#!/usr/bin/env python3
"""Focused tests for the autofix patch-extraction logic.

The v1 skip storm came from model replies wrapped in markdown fences
with trailing prose. These cases pin that behavior down.
Run: python3 .github/scripts/test_autofix.py
"""

import importlib.util
import os
import sys

# autofix.py reads required env at import; stub them.
os.environ.setdefault("GITHUB_REPOSITORY", "owner/repo")
os.environ.setdefault("PR_NUMBER", "1")
os.environ.setdefault("GITHUB_TOKEN", "test")
os.environ.setdefault("OPENROUTER_API_KEY", "test")

_spec = importlib.util.spec_from_file_location(
    "autofix", os.path.join(os.path.dirname(__file__), "autofix.py"))
fx = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fx)

CLEAN = """\
--- a/src/foo.cpp
+++ b/src/foo.cpp
@@ -1,3 +1,4 @@
 int a;
+int b;
 int c;
"""

FENCED = "Here is the fix:\n```diff\n" + CLEAN + "```\n\nThis adds b.\n"

PROSE_TAIL = CLEAN + "Let me know if this helps!\n"

NO_DIFF = "I cannot fix this safely, sorry.\n"

EMPTY_CTX = """\
--- a/src/foo.cpp
+++ b/src/foo.cpp
@@ -1,3 +1,3 @@
 line1

-line3
+line4
"""


def test_clean_diff_passthrough():
    assert fx.extract_diff(CLEAN) == CLEAN


def test_fenced_diff_unwrapped():
    assert fx.extract_diff(FENCED) == CLEAN


def test_trailing_prose_stripped():
    assert fx.extract_diff(PROSE_TAIL) == CLEAN


def test_no_diff_is_none():
    assert fx.extract_diff(NO_DIFF) is None
    assert fx.extract_diff(None) is None
    assert fx.extract_diff("") is None


def test_empty_context_line_kept():
    assert fx.extract_diff(EMPTY_CTX) == EMPTY_CTX.replace("\n\n", "\n \n")


def test_touched_files_parsed():
    assert fx.touched_files(CLEAN) == {"src/foo.cpp"}
    assert fx.touched_files("no headers here\n") == set()


WRONG_COUNTS = CLEAN.replace("@@ -1,3 +1,4 @@", "@@ -1,99 +1,7 @@", 1)

FIXED_COUNTS = CLEAN.replace("@@ -1,3 +1,4 @@", "@@ -1,2 +1,3 @@", 1)


def test_normalize_fixes_wrong_counts():
    assert fx.normalize_diff(WRONG_COUNTS) == FIXED_COUNTS


def test_normalize_keeps_good_diff_stable():
    assert fx.normalize_diff(FIXED_COUNTS) == FIXED_COUNTS


def test_normalize_keeps_section_heading():
    hunk = "@@ -1,9 +1,9 @@ def foo():"
    diff = CLEAN.replace("@@ -1,3 +1,4 @@", hunk, 1)
    fixed = fx.normalize_diff(diff)
    assert fixed.splitlines()[2] == "@@ -1,2 +1,3 @@ def foo():"
    assert fixed.splitlines()[3:] == CLEAN.splitlines()[3:]


def test_normalize_none_without_hunk():
    assert fx.normalize_diff("no headers here\n") is None
    assert fx.normalize_diff(None) is None
    assert fx.normalize_diff("") is None


def _finding(path, line, bot="bot", created="2026-10-15T00:00:00Z"):
    return {"path": path, "line": line, "body": "b", "bot": bot,
            "created": created}


def test_group_by_file_orders_newest_first():
    fs = [_finding("b.py", 1, created="2026-10-15T01:00:00Z"),
          _finding("a.py", 2, created="2026-10-15T02:00:00Z"),
          _finding("b.py", 3, created="2026-10-15T03:00:00Z")]
    # input order (already newest-first) decides file order; same-file
    # findings stay together so one patch covers them all.
    grouped = fx.group_by_file(fs)
    assert [p for p, _ in grouped] == ["b.py", "a.py"]
    assert [f["line"] for _, g in grouped for f in g] == [1, 3, 2]


def test_group_by_file_empty():
    assert fx.group_by_file([]) == []


def test_render_findings_numbers_newest_first():
    fs = [_finding("a.py", 10, bot="cb"), _finding("a.py", 20, bot="ca")]
    out = fx.render_findings(fs)
    assert "[1] (line 10, cb):" in out
    assert "[2] (line 20, ca):" in out
    assert "Findings for" not in out  # numbering only; header lives in prompt


def test_file_prompt_covers_every_finding():
    fs = [_finding("a.py", 10), _finding("a.py", 20)]
    prompt = fx.FILE_PROMPT.format(
        path="a.py", findings=fx.render_findings(fs), content="x")
    assert "[1]" in prompt and "[2]" in prompt
    assert "a.py" in prompt


def test_group_failure_splits_to_singletons():
    from unittest import mock
    fd = os.path.join(os.path.dirname(__file__), "test_autofix.py")
    fs = [_finding(fd, 1), _finding(fd, 2)]
    calls = []

    def fake_make(path, findings, content):
        calls.append([f["line"] for f in findings])
        return None, 1, "nope", ""

    with mock.patch.object(fx, "make_patch_for_file", fake_make), \
            mock.patch.object(fx, "git"):
        recs = fx.process_file(fd, fs, {fd}, False)
    # one group attempt, then one attempt per finding; singletons stop.
    assert calls == [[1, 2], [1], [2]]
    assert [r["status"] for r in recs] == ["skipped", "skipped"]
    assert all("no valid patch" in r["note"] for r in recs)


def test_model_call_has_timeout():
    import urllib.request
    from unittest import mock
    seen = {}

    def fake(req, timeout=None):
        seen["timeout"] = timeout
        raise ValueError("boom")

    with mock.patch.object(urllib.request, "urlopen", fake):
        assert fx.openrouter([{"role": "user", "content": "hi"}]) is None
    assert seen["timeout"] and seen["timeout"] > 0


def test_gh_call_has_timeout():
    import io
    import urllib.request
    from unittest import mock
    seen = {}

    def fake(req, timeout=None):
        seen["timeout"] = timeout
        return io.BytesIO(b"{}")

    with mock.patch.object(urllib.request, "urlopen", fake):
        assert fx.gh("GET", "/x") == {}
    assert seen["timeout"] and seen["timeout"] > 0


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"ok {name}")
            except AssertionError:
                failed += 1
                print(f"FAIL {name}")
    sys.exit(1 if failed else 0)
