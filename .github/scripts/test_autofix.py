#!/usr/bin/env python3
"""Focused tests for autofix patch extraction, containment, and auditing.

The v1 skip storm came from model replies wrapped in markdown fences
with trailing prose. These cases pin that behavior down.
Run: python3 .github/scripts/test_autofix.py
"""

import contextlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from unittest import mock

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
    assert fx.touched_files(fx.normalize_diff(CLEAN)) == {"src/foo.cpp"}
    assert fx.touched_files("no headers here\n") == set()


def _patch(path, new="new"):
    return (f"--- old/{path}\n+++ new/{path}\n"
            f"@@ -1 +1 @@\n-old\n+{new}\n")


def test_touched_files_nonstandard_headers():
    assert fx.touched_files(_patch("target.txt") + _patch("other.txt")) == {
        "target.txt", "other.txt"}


def test_touched_files_rename_and_copy_sources():
    for operation in ("rename", "copy"):
        patch = ("diff --git a/source.txt b/destination.txt\n"
                 "similarity index 100%\n"
                 f"{operation} from source.txt\n"
                 f"{operation} to destination.txt\n")
        assert fx.touched_files(patch) == {"source.txt", "destination.txt"}


def test_touched_files_unusual_names_and_new_files():
    with tempfile.TemporaryDirectory() as tmp:
        for name in ("space name", "tab\tname", "line\nname", "return\rname",
                     'quote"name', "café"):
            path = os.path.join(tmp, name)
            with open(path, "w") as f:
                f.write("new\n")
            r = subprocess.run(
                ["git", "diff", "--no-index", "--", "/dev/null", path],
                capture_output=True)
            assert r.returncode == 1
            # Absolute diff paths lose their leading slash with Git's -p1.
            expected = path.lstrip("/")
            patch = os.fsdecode(r.stdout)
            assert fx.touched_files(patch) == {expected}


def test_touched_files_deletion():
    patch = "--- a/deleted.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-old\n"
    assert fx.touched_files(patch) == {"deleted.txt"}


@contextlib.contextmanager
def _checkout():
    previous = os.getcwd()
    with tempfile.TemporaryDirectory() as tmp:
        try:
            os.chdir(tmp)
            fx.git("init", "-q")
            for path in ("target.txt", "other.txt"):
                with open(path, "w") as f:
                    f.write("old\n")
            fx.git("add", ".")
            fx.git("-c", "user.name=test", "-c", "user.email=test@example.com",
                   "commit", "-qm", "fixture")
            yield tmp
        finally:
            os.chdir(previous)


def test_process_file_rejects_extra_paths_before_apply():
    for keep in (False, True):
        for allowed, reason in (({"target.txt"}, "outside-pr"),
                                ({"target.txt", "other.txt"}, "outside-file")):
            patch = _patch("target.txt") + _patch("other.txt")
            with _checkout(), mock.patch.object(
                    fx, "make_patch_for_file", return_value=(patch, 1, "", "")):
                assert fx.check_apply(patch).returncode == 0
                recs = fx.process_file("target.txt", [_finding("target.txt", 1)],
                                       allowed, keep)
                assert recs[0]["status"] == "skipped"
                assert recs[0]["reason"] == reason
                assert recs[0]["patch_sha"] == ""
                assert fx.git("status", "--porcelain").stdout == ""
                for path in allowed | {"other.txt"}:
                    with open(path) as f:
                        assert f.read() == "old\n"


def test_process_file_preserves_keep_and_verification_rollback():
    for keep in (False, True):
        for new in ("new", "new "):
            patch = _patch("target.txt", new)
            if new.endswith(" "):
                # Keep bad whitespace inside the hunk, away from the
                # normalizer's existing removal of trailing whitespace.
                patch += "+tail\n"
            with _checkout(), mock.patch.object(
                    fx, "call_model", return_value=patch):
                recs = fx.process_file("target.txt", [_finding("target.txt", 1)],
                                       {"target.txt"}, keep)
                valid = new == "new"
                expected = "applied" if keep else "verified"
                assert recs[0]["status"] == (expected if valid else "skipped")
                if not valid:
                    assert recs[0]["reason"] == "verify-failed"
                with open("target.txt") as f:
                    assert f.read() == ("new\n" if valid and keep else "old\n")
                with open("other.txt") as f:
                    assert f.read() == "old\n"


def test_run_writes_external_audit_in_both_modes():
    original_git = fx.git

    def local_git(*args, **kwargs):
        if args[0] == "push":
            return subprocess.CompletedProcess(args, 0)
        return original_git(*args, **kwargs)

    for mode in ("suggest", "apply"):
        with _checkout(), tempfile.TemporaryDirectory() as audit_dir, \
                mock.patch.dict(os.environ, {"RUNNER_TEMP": audit_dir}), \
                mock.patch.object(fx, "MODE", mode), \
                mock.patch.object(fx, "gh", return_value={"head": {"ref": "test"}}), \
                mock.patch.object(fx, "gh_paged", return_value=[{"filename": "target.txt"}]), \
                mock.patch.object(fx, "collect_findings", return_value=[_finding("target.txt", 1)]), \
                mock.patch.object(fx, "call_model", return_value=_patch("target.txt")), \
                mock.patch.object(fx, "post_audit") as post, \
                mock.patch.object(fx, "git", side_effect=local_git) as git:
            records = fx.run()
            with open(os.path.join(audit_dir, "autofix-audit.json")) as f:
                assert json.load(f) == {"pr": fx.PR, "mode": mode,
                                        "model": fx.MODEL, "records": records}
            assert records[0]["status"] == (
                "applied" if mode == "apply" else "verified")
            post.assert_called_once_with(records, mode)
            assert not os.path.exists("autofix-audit.json")
            assert original_git("status", "--porcelain").stdout == ""
            assert original_git("ls-files").stdout.splitlines() == [
                "other.txt", "target.txt"]
            assert any(c.args[0] == "push" for c in git.call_args_list) == (
                mode == "apply")


def test_workflow_uploads_external_audit():
    workflow = os.path.join(os.path.dirname(__file__), "..", "workflows", "autofix.yml")
    with open(workflow) as f:
        paths = [line.strip() for line in f if line.strip().startswith("path:")]
    assert paths == ["path: ${{ runner.temp }}/autofix-audit.json"] * 2


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


def test_failure_reason_codes():
    assert fx.failure_reason("no diff in response") == "model-declined"
    assert fx.failure_reason("no diff in retry response") == "model-declined"
    assert fx.failure_reason(
        "error: corrupt patch at /tmp/autofix.patch:4") == "unparseable"
    assert fx.failure_reason(
        "error: patch failed: a.py:12") == "stale-context"
    assert fx.failure_reason("unreachable") == "unknown"
    assert fx.failure_reason("") == "unknown"
    assert fx.failure_reason(None) == "unknown"


def test_group_failure_splits_to_singletons():
    from unittest import mock
    fd = os.path.join(os.path.dirname(__file__), "test_autofix.py")
    fs = [_finding(fd, 1), _finding(fd, 2)]
    calls = []

    def fake_make(path, findings, content):
        calls.append([f["line"] for f in findings])
        return None, 1, "no diff in response", ""

    with mock.patch.object(fx, "make_patch_for_file", fake_make), \
            mock.patch.object(fx, "git"):
        recs = fx.process_file(fd, fs, {fd}, False)
    # one group attempt, then one attempt per finding; singletons stop.
    assert calls == [[1, 2], [1], [2]]
    assert [r["status"] for r in recs] == ["skipped", "skipped"]
    assert all("no valid patch" in r["note"] for r in recs)
    assert all(r["reason"] == "model-declined" for r in recs)


def test_missing_file_reason_recorded():
    from unittest import mock
    fs = [_finding("no/such/file.py", 1)]
    with mock.patch.object(fx, "git"):
        recs = fx.process_file("no/such/file.py", fs, {"no/such/file.py"},
                              False)
    assert recs[0]["status"] == "skipped"
    assert recs[0]["reason"] == "missing-file"


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
