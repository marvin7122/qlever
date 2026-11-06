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
from unittest import TestCase, mock

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


def test_run_reuses_suggest_audit_without_generating_patches():
    original_git = fx.git

    def local_git(*args, **kwargs):
        if args[0] == "push":
            return subprocess.CompletedProcess(args, 0)
        return original_git(*args, **kwargs)

    with _checkout(), tempfile.TemporaryDirectory() as audit_dir:
        head_sha = original_git("rev-parse", "HEAD").stdout.strip()
        suggested = None
        for mode in ("suggest", "apply"):
            source = os.path.join(audit_dir, "suggest.json")
            with mock.patch.dict(os.environ, {"RUNNER_TEMP": audit_dir,
                                             "VERIFIED_AUDIT_PATH": source}), \
                    mock.patch.object(fx, "MODE", mode), \
                    mock.patch.object(fx, "SELECTION", "/autofix apply all"), \
                    mock.patch.object(fx, "gh", return_value={
                        "head": {"ref": "test", "sha": head_sha}}), \
                    mock.patch.object(fx, "gh_paged", return_value=[{"filename": "target.txt"}]), \
                    mock.patch.object(fx, "collect_findings", return_value=[_finding("target.txt", 1)]) as collect, \
                    mock.patch.object(fx, "call_model", return_value=_patch("target.txt")) as model, \
                    mock.patch.object(fx, "process_file", wraps=fx.process_file) as process, \
                    mock.patch.object(fx, "post_audit") as post, \
                    mock.patch.object(fx, "git", side_effect=local_git) as git:
                records = fx.run()
                if mode == "apply":
                    collect.assert_not_called()
                    process.assert_not_called()
                    model.assert_not_called()
                    assert records[0]["patch"] == suggested[0]["patch"]
                    assert records[0]["patch_sha"] == suggested[0]["patch_sha"]
                    assert original_git("show", "HEAD:target.txt").stdout == "new\n"
                else:
                    suggested = records
                with open(os.path.join(audit_dir, "autofix-audit.json")) as f:
                    audit = json.load(f)
                    assert audit == {
                        "repo": fx.REPO, "pr": fx.PR, "head_sha": head_sha,
                        "mode": mode, "model": fx.MODEL, "records": records}
                if mode == "suggest":
                    with open(source, "w") as f:
                        json.dump(audit, f)
                assert records[0]["status"] == (
                    "applied" if mode == "apply" else "verified")
                post.assert_called_once_with(records, mode)
                assert not os.path.exists("autofix-audit.json")
                assert original_git("status", "--porcelain").stdout == ""
                assert original_git("ls-files").stdout.splitlines() == [
                    "other.txt", "target.txt"]
                assert any(c.args[0] == "push" for c in git.call_args_list) == (
                    mode == "apply")


@contextlib.contextmanager
def _suggested_audit():
    with _checkout(), tempfile.TemporaryDirectory() as audit_dir, \
            mock.patch.object(fx, "call_model", return_value=_patch("target.txt")):
        records = fx.process_file("target.txt", [_finding("target.txt", 1)],
                                  {"target.txt"}, False)
        audit = {"repo": fx.REPO, "pr": fx.PR, "mode": "suggest",
                 "head_sha": fx.git("rev-parse", "HEAD").stdout.strip(),
                 "records": records}
        source = os.path.join(audit_dir, "suggest.json")
        with open(source, "w") as f:
            json.dump(audit, f)
        with mock.patch.dict(os.environ, {"VERIFIED_AUDIT_PATH": source}):
            yield audit, source


def test_apply_rejects_missing_stale_or_wrong_audit():
    cases = [({"head_sha": "0" * 40}, "head changed"),
             ({"pr": fx.PR + 1}, "not a suggestion for this PR"),
             ({"repo": "another/repo"}, "not a suggestion for this PR"),
             ({"mode": "apply"}, "not a suggestion for this PR")]
    for changes, message in cases:
        with _suggested_audit() as (audit, source):
            head_sha = audit["head_sha"]
            audit.update(changes)
            with open(source, "w") as f:
                json.dump(audit, f)
            with TestCase().assertRaisesRegex(SystemExit, message):
                fx.apply_verified(head_sha, {"target.txt"})
            assert fx.git("status", "--porcelain").stdout == ""
    with _checkout(), mock.patch.dict(os.environ, {"VERIFIED_AUDIT_PATH": ""}):
        with TestCase().assertRaisesRegex(SystemExit, "requires a stored"):
            fx.apply_verified("head", {"target.txt"})


def test_apply_rejects_checkout_that_does_not_match_pr_head():
    with _checkout(), mock.patch.object(fx, "MODE", "apply"), \
            mock.patch.object(fx, "gh", return_value={"head": {"sha": "0" * 40}}), \
            mock.patch.object(fx, "apply_verified") as apply:
        with TestCase().assertRaisesRegex(SystemExit, "checkout does not match"):
            fx.run()
        apply.assert_not_called()


def test_apply_selects_and_deduplicates_stored_patches():
    for selection in ("all", "selected", "unselected"):
        with _suggested_audit() as (audit, source):
            record = audit["records"][0]
            # Two findings may share a single verified patch.
            audit["records"].append({**record, "line": 2})
            with mock.patch.object(fx, "call_model", return_value=_patch("other.txt")):
                audit["records"].extend(fx.process_file(
                    "other.txt", [_finding("other.txt", 1)], {"other.txt"}, False))
            with open(source, "w") as f:
                json.dump(audit, f)
            patch_id = {"all": "all", "selected": record["patch_sha"][:6],
                        "unselected": "deadbeef"}[selection]
            with mock.patch.object(fx, "SELECTION", f"/autofix apply {patch_id}"):
                records = fx.apply_verified(audit["head_sha"], {"target.txt", "other.txt"})
            selected = selection != "unselected"
            assert [r["status"] for r in records[:2]] == [
                "applied" if selected else "skipped"] * 2
            assert records[2]["status"] == ("applied" if selection == "all" else "skipped")
            with open("target.txt") as f:
                assert f.read() == ("new\n" if selected else "old\n")
            with open("other.txt") as f:
                assert f.read() == ("new\n" if selection == "all" else "old\n")


def test_apply_rejects_modified_or_out_of_scope_patch():
    for change in ("diff", "path", "allowed"):
        with _suggested_audit() as (audit, source):
            if change == "diff":
                audit["records"][0]["patch"] = _patch("target.txt", "tampered")
            if change == "path":
                audit["records"][0]["path"] = "other.txt"
            with open(source, "w") as f:
                json.dump(audit, f)
            allowed = {"other.txt"} if change == "allowed" else {"target.txt", "other.txt"}
            with TestCase().assertRaisesRegex(SystemExit, "integrity or path"):
                fx.apply_verified(audit["head_sha"], allowed)
            assert fx.git("status", "--porcelain").stdout == ""


def test_apply_rolls_back_failed_verification_without_regenerating():
    with _suggested_audit() as (audit, source), \
            mock.patch.object(fx, "verify", return_value=(False, "test failure")), \
            mock.patch.object(fx, "call_model") as model:
        with TestCase().assertRaisesRegex(SystemExit, "verification failed"):
            fx.apply_verified(audit["head_sha"], {"target.txt"})
        assert fx.git("status", "--porcelain").stdout == ""
        model.assert_not_called()


def test_apply_rejects_conflicting_stored_patches_before_changing_files():
    with _suggested_audit() as (audit, source):
        with mock.patch.object(fx, "call_model", return_value=_patch("target.txt", "another")):
            audit["records"].extend(fx.process_file(
                "target.txt", [_finding("target.txt", 2)], {"target.txt"}, False))
        # Both patches passed verification independently on the PR head.
        assert all(r["status"] == "verified" for r in audit["records"])
        with open(source, "w") as f:
            json.dump(audit, f)
        with TestCase().assertRaisesRegex(SystemExit, "do not apply"):
            fx.apply_verified(audit["head_sha"], {"target.txt"})
        assert fx.git("status", "--porcelain").stdout == ""


def test_apply_does_not_retry_skipped_suggestions():
    with _suggested_audit() as (audit, source):
        audit["records"][0].update(status="skipped", reason="verify-failed")
        with open(source, "w") as f:
            json.dump(audit, f)
        records = fx.apply_verified(audit["head_sha"], {"target.txt"})
        assert records[0]["reason"] == "verify-failed"
        assert fx.git("status", "--porcelain").stdout == ""


def test_workflow_uploads_external_audit():
    workflow = os.path.join(os.path.dirname(__file__), "..", "workflows", "autofix.yml")
    with open(workflow) as f:
        uploads = f.read().split("uses: actions/upload-artifact@v4")[1:]
    paths = [next(line.strip() for line in upload.splitlines()
                  if line.strip().startswith("path:")) for upload in uploads]
    assert paths == ["path: ${{ runner.temp }}/autofix-audit.json"] * 2


def test_workflow_passes_suggestions_between_runs_on_pr_head():
    workflow = os.path.join(os.path.dirname(__file__), "..", "workflows", "autofix.yml")
    with open(workflow) as f:
        suggest, apply = f.read().split("\n  apply:\n")
    assert "ref: ${{ github.event.pull_request.head.sha }}" in suggest
    assert "name: autofix-suggestions-${{ github.event.pull_request.number }}" in suggest
    assert "actions: read" in suggest
    assert "ref: ${{ steps.pr.outputs.sha }}" in apply
    assert "actions/download-artifact@v4" in apply
    assert "name: autofix-suggestions-${{ github.event.issue.number }}" in apply
    assert "run-id: ${{ steps.suggestions.outputs.run_id }}" in apply
    assert "VERIFIED_AUDIT_PATH: ${{ runner.temp }}/autofix-suggestions/autofix-audit.json" in apply


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


def test_parse_selection_variants():
    assert fx.parse_selection("") == "all"
    assert fx.parse_selection("/autofix apply") == "all"
    assert fx.parse_selection("/autofix apply all") == "all"
    assert fx.parse_selection("/autofix apply abc123") == ["abc123"]
    assert fx.parse_selection(
        "please /autofix apply ab12, cd34") == ["ab12", "cd34"]
    assert fx.parse_selection("/autofix apply hello") == []


def test_verified_record_stores_patch_and_options_markdown():
    with _checkout(), mock.patch.object(
            fx, "call_model", return_value=_patch("target.txt")):
        recs = fx.process_file("target.txt", [_finding("target.txt", 1)],
                               {"target.txt"}, False)
    assert recs[0]["status"] == "verified"
    assert recs[0]["reason"] == "verified"
    assert "target.txt" in recs[0]["patch"]
    body = fx.audit_markdown(recs, "suggest")
    assert "| Reason |" in body
    assert recs[0]["reason"] in body
    assert "### Suggestions (ready to apply)" in body
    assert "Problem:" in body and "Tradeoff:" in body
    assert "```diff" in body
    assert f"/autofix apply {recs[0]['patch_sha']}" in body
    assert "Option B" in body


def test_verify_rejects_whitespace_only_change():
    with _checkout():
        with open("target.txt", "w") as f:
            f.write("  old\n")
        ok, note = fx.verify("target.txt")
        assert not ok
        assert note == "no functional change"


def test_apply_selection_reverts_unselected():
    with _checkout(), mock.patch.object(
            fx, "call_model", return_value=_patch("target.txt")):
        recs = fx.process_file("target.txt", [_finding("target.txt", 1)],
                               {"target.txt"}, True)
        assert recs[0]["status"] == "applied"
        with open("target.txt") as f:
            assert f.read() == "new\n"
        with mock.patch.object(fx, "SELECTION", "/autofix apply deadbeef"):
            fx.deselect_unwanted(recs)
        assert recs[0]["status"] == "skipped"
        assert recs[0]["reason"] == "not-selected"
        with open("target.txt") as f:
            assert f.read() == "old\n"


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
