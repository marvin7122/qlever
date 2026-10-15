#!/usr/bin/env python3
"""Bot-finding autofix for Qlever PRs (v2).

Options-first, command-driven apply:
  suggest: collect bot inline findings, generate one minimal verified
           patch per file (Option A), and post a sticky audit comment
           that states the problem, shows the suggestion ready to apply,
           and names its tradeoff. Option B is always "leave as-is".
           Nothing is pushed.
  apply:   regenerate on the current head and push verified patches as
           one commit. A human selects with `/autofix apply all` or
           `/autofix apply <patch-id> [more ids]`; anything not selected
           is left out. Triggered only by a human approval comment.

Environment:
  GITHUB_REPOSITORY   owner/repo
  PR_NUMBER           pull request number
  GITHUB_TOKEN        token with contents:write + pull-requests:write
  OPENROUTER_API_KEY  model key (spend-capped on the OpenRouter side)
  FIX_MODEL           model id (default qwen/qwen3-coder-30b-a3b-instruct)
  MAX_FINDINGS        cap per run (default 25)
  MODE                suggest (default) | apply
  SELECTION           raw `/autofix apply ...` comment body (apply only)
"""

import hashlib
import json
import os
import re
import subprocess
import urllib.request

REPO = os.environ["GITHUB_REPOSITORY"]
PR = int(os.environ["PR_NUMBER"])
GH_TOKEN = os.environ["GITHUB_TOKEN"]
OR_KEY = os.environ["OPENROUTER_API_KEY"]
MODEL = os.environ.get("FIX_MODEL", "qwen/qwen3-coder-30b-a3b-instruct")
MAX_FINDINGS = int(os.environ.get("MAX_FINDINGS", "25"))
MODE = os.environ.get("MODE", "suggest")
SELECTION = os.environ.get("SELECTION", "")
API = "https://api.github.com"
MARKER = "<!-- autofix:audit -->"

BOT_AUTHORS = {
    "coderabbitai[bot]",
    "qodo-code-review[bot]",
    "codeant-ai[bot]",
    "sourcery-ai[bot]",
    "deepsource-io[bot]",
    "superagent-security[bot]",
    "greptile-apps[bot]",
    "cubic-dev-ai[bot]",
}


def gh(method, path, data=None):
    req = urllib.request.Request(
        API + path,
        data=json.dumps(data).encode() if data is not None else None,
        headers={
            "Authorization": "Bearer " + GH_TOKEN,
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
        },
        method=method,
    )
    return json.load(urllib.request.urlopen(req, timeout=30))


def gh_paged(path):
    out, page = [], 1
    while True:
        batch = gh("GET", f"{path}?per_page=100&page={page}")
        out.extend(batch)
        if len(batch) < 100:
            return out
        page += 1


def git(*args, check=True):
    r = subprocess.run(["git", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        print(r.stdout[-2000:], r.stderr[-2000:])
        raise SystemExit(f"git command failed: {args}")
    return r


_DIFF_OK = ("--- ", "+++ ", "@@", " ", "+", "-", "\\",
            "diff --git ", "index ", "new file mode")


def extract_diff(text):
    """Pull a unified diff out of a model reply, fences and prose stripped."""
    if not text:
        return None
    m = re.search(r"```(?:diff|patch)?\n(.*?)```", text, re.S)
    lines = (m.group(1) if m else text).splitlines()
    start = next((i for i, ln in enumerate(lines)
                  if ln.startswith(("--- ", "diff --git "))), None)
    if start is None:
        return None
    out = []
    for ln in lines[start:]:
        if ln.strip().startswith("```"):
            break
        if ln.startswith(_DIFF_OK):
            out.append(ln)
        elif ln == "":
            out.append(" ")
        else:
            break
    return ("\n".join(out).rstrip() + "\n") if out else None


def touched_files(diff):
    """Use Git's patch parser to collect both sides of every change."""
    with open("/tmp/autofix.patch", "w") as f:
        f.write(diff)
    touched = set()
    # Forward numstat reports destinations; reverse also exposes the
    # source paths of renames and copies. Keep NUL-delimited names raw.
    for reverse in ((), ("--reverse",)):
        r = subprocess.run(
            ["git", "apply", "--numstat", "-z", *reverse,
             "/tmp/autofix.patch"], capture_output=True)
        if r.returncode != 0:
            return set()
        for entry in r.stdout.split(b"\0"):
            if entry:
                touched.add(os.fsdecode(entry.split(b"\t", 2)[2]))
    return touched


_HUNK = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(.*)$")


def normalize_diff(diff):
    """Rewrite @@ counts from the actual hunk bodies.

    Models routinely emit plausible-but-wrong hunk lengths, which makes
    `git apply` reject the patch as corrupt before it even tries the
    context. Recounting is semantics-preserving: no line is touched.
    Returns None when there is no parseable hunk.
    """
    if not diff:
        return None
    lines = diff.splitlines()
    first = next((i for i, ln in enumerate(lines)
                  if _HUNK.match(ln)), None)
    if first is None:
        return None
    head, out, body, hdr = lines[:first], [], [], None
    for ln in lines[first:] + [None]:
        if ln is None or _HUNK.match(ln):
            if hdr is not None:
                old, new = _count(body)
                out.append(f"@@ -{start},{old} +{new_start},{new} @@{suffix}")
                out.extend(body)
                body = []
            if ln is None:
                break
            m = _HUNK.match(ln)
            start, new_start, suffix = int(m.group(1)), int(m.group(3)), m.group(5)
            hdr = ln
        else:
            body.append(ln)
    return ("\n".join(head + out).rstrip() + "\n") if out else None


def _count(body):
    old = new = 0
    for ln in body:
        if ln.startswith("+"):
            new += 1
        elif ln.startswith("-"):
            old += 1
        elif ln.startswith("\\"):
            pass
        else:
            old += 1
            new += 1
    return old, new


def openrouter(messages, max_tokens=1800):
    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps(
            {"model": MODEL, "messages": messages, "max_tokens": max_tokens}
        ).encode(),
        headers={
            "Authorization": "Bearer " + OR_KEY,
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/" + REPO,
            "X-Title": "Qlever bot autofix",
        },
    )
    try:
        return json.load(urllib.request.urlopen(req, timeout=180))
    except Exception as e:
        try:
            print("model call failed:", e.read().decode()[:500])
        except Exception:
            print("model call failed:", e)
        return None


def collect_findings(allowed=None):
    """Newest bot inline comments, deduped by (path, line), richest body wins.

    When allowed (the PR's filenames) is given, comments on other paths
    are dropped up front instead of refetching the file list here.
    """
    comments = gh_paged(f"/repos/{REPO}/pulls/{PR}/comments")
    by_key = {}
    for c in comments:
        author = (c.get("user") or {}).get("login") or ""
        if author not in BOT_AUTHORS or not c.get("path") or not c.get("body"):
            continue
        if allowed is not None and c["path"] not in allowed:
            continue
        # Skip resolved comments (comments with an empty body or that have
        # been deleted)
        if not c["body"].strip():
            continue
        key = (c["path"], c.get("line") or c.get("original_line"))
        old = by_key.get(key)
        if old and len(old["body"]) >= len(c["body"]):
            continue
        by_key[key] = {
            "path": c["path"],
            "line": c.get("line") or c.get("original_line"),
            "body": c["body"][:4000],
            "bot": author.replace("[bot]", ""),
            "created": c.get("created_at") or "",
        }
    findings = sorted(by_key.values(), key=lambda f: f["created"],
                      reverse=True)
    return findings[:MAX_FINDINGS]


def group_by_file(findings):
    """Group findings by path, newest file first. -> [(path, [findings])]."""
    groups, order = {}, []
    for f in findings:
        if f["path"] not in groups:
            groups[f["path"]] = []
            order.append(f["path"])
        groups[f["path"]].append(f)
    return [(p, groups[p]) for p in order]


FILE_PROMPT = """\
You fix all code-review findings below for one file with the smallest
correct changes.
Rules: reply with ONLY one unified diff (--- a/<path> / +++ b/<path>
headers, @@ hunks), no prose, no code fences. Address every finding;
keep changes minimal; do not refactor surroundings. Count every hunk's
@@ -start,len +start,len @@ numbers exactly from its body lines.
The diff must apply with `git apply` to the file content below.
If two findings conflict, satisfy the newer one (lower number).

Findings for {path} (newest first):
{findings}

File content:
```
{content}
```
"""

RETRY_PROMPT = """\
Your previous diff failed `git apply --check` with:
{error}
Previous diff:
{diff}
File content it must apply to:
```
{content}
```
Return a corrected unified diff only, no prose, no code fences.
Count every hunk's @@ numbers exactly from its body lines.
It must apply cleanly to this file content.
"""


def call_model(prompt_text):
    resp = openrouter([{"role": "user", "content": prompt_text}])
    try:
        return resp["choices"][0]["message"]["content"]
    except (TypeError, KeyError, IndexError):
        return None


def check_apply(diff):
    with open("/tmp/autofix.patch", "w") as f:
        f.write(diff)
    return git("apply", "--check", "/tmp/autofix.patch", check=False)


def render_findings(findings):
    return "\n\n".join(
        f"[{i}] (line {f['line']}, {f['bot']}):\n{f['body']}"
        for i, f in enumerate(findings, 1))


def make_patch_for_file(path, findings, content):
    """-> (diff, attempts, short_err, full_err). One patch per file."""
    text = call_model(FILE_PROMPT.format(
        path=path, findings=render_findings(findings), content=content))
    diff = normalize_diff(extract_diff(text))
    if not diff:
        return None, 1, "no diff in response", ""
    tag = f"{path} ({len(findings)} findings)"
    for attempt in (1, 2):
        r = check_apply(diff)
        if r.returncode == 0:
            return diff, attempt, "", ""
        full = (r.stderr or r.stdout).strip()
        short = full.splitlines()[0] if full else "apply check failed"
        if attempt == 2:
            return None, 2, short, full[:500]
        print(f"retry {tag} after: {full.splitlines()[0] if full else ''}")
        text = call_model(RETRY_PROMPT.format(
            error=full[:800], diff=diff, content=content))
        diff = normalize_diff(extract_diff(text))
        if not diff:
            return None, 2, "no diff in retry response", ""
    return None, 2, "unreachable", ""


def verify(path):
    """Targeted post-apply checks. -> (ok, note)"""
    r = git("diff", "--check", "--", path, check=False)
    if r.returncode != 0:
        return False, "whitespace errors"
    if path.endswith(".py"):
        r = subprocess.run(["python3", "-m", "py_compile", path],
                           capture_output=True, text=True)
        if r.returncode != 0:
            return False, "python syntax error"
    return True, ""


def failure_reason(short_err):
    """Stable machine-readable skip code for a patch-generation failure."""
    if not short_err:
        return "unknown"
    if "no diff" in short_err:
        return "model-declined"
    if short_err.startswith("error: corrupt"):
        return "unparseable"
    if short_err.startswith("error: patch failed"):
        return "stale-context"
    return "unknown"


def parse_selection(body):
    """Parse `/autofix apply ...` comment bodies. -> "all" | [ids] | [].

    "all" means every verified patch; a list holds requested patch-id
    prefixes (case-insensitive); [] means an explicit selection matched
    nothing (apply nothing, say so in the audit).
    """
    if not body:
        return "all"
    m = re.search(r"/autofix\s+apply\s*([^\n]*)?", body, re.I)
    if not m:
        return "all"
    arg = (m.group(1) or "").strip().lower()
    if arg in ("", "all"):
        return "all"
    ids = [t.strip("`,. ").lower() for t in re.split(r"[\s,]+", arg) if t]
    ids = [t for t in ids if re.fullmatch(r"[0-9a-f]{2,40}", t)]
    return ids if ids else []


def _matches(sha, wanted):
    return any(sha.startswith(w) or w.startswith(sha) for w in wanted)


def deselect_unwanted(records):
    """Revert applied patches the human did not select. -> records."""
    wanted = parse_selection(SELECTION)
    if wanted == "all":
        return records
    wanted = wanted if isinstance(wanted, list) else []
    seen = set()
    for r in records:
        if r["status"] != "applied" or not r["patch_sha"]:
            continue
        if r["patch_sha"] in seen:
            r.update(status="skipped", reason="not-selected",
                     note="not selected; comment `/autofix apply "
                     f"{r['patch_sha']}` to apply it")
            continue
        if _matches(r["patch_sha"], wanted):
            seen.add(r["patch_sha"])
            continue
        if r.get("patch"):
            with open("/tmp/autofix.patch", "w") as f:
                f.write(r["patch"])
            git("apply", "-R", "/tmp/autofix.patch", check=False)
        seen.add("reverted:" + r["patch_sha"])
        for other in records:
            if other.get("patch_sha") == r["patch_sha"]:
                other.update(status="skipped", reason="not-selected",
                             note="not selected; comment `/autofix apply "
                             f"{r['patch_sha']}` to apply it")
    return records


def tradeoff_line(diff, path):
    """One-line tradeoff for a verified patch."""
    plus = sum(1 for ln in diff.splitlines() if ln.startswith("+")
               and not ln.startswith("+++"))
    minus = sum(1 for ln in diff.splitlines() if ln.startswith("-")
                and not ln.startswith("---"))
    return (f"applies a minimal {plus + minus}-line change in {path} only; "
            "alternative is leave as-is (no churn, finding stays open)")


def process_file(path, findings, allowed, keep):
    """Generate, verify, optionally keep one patch per file. -> records."""
    recs = [{**{k: f[k] for k in ("path", "line", "bot")},
             "status": "", "patch_sha": "", "attempts": 0,
             "note": "", "error": "", "reason": "",
             "patch": "", "finding": f.get("body", "")[:500]}
            for f in findings]

    def skip_all(note, reason, error=""):
        for r in recs:
            r.update(status="skipped", note=note, error=error,
                     reason=reason)
        return recs

    def fail(note, reason, error=""):
        # One stale hunk sinks a whole-file patch, losing the fresh
        # findings with it. Fall back to single-finding patches so each
        # finding succeeds or fails on its own context. Singletons never
        # split further, so this recurses at most once.
        if len(findings) > 1:
            print(f"split {path} ({len(findings)}): {note}")
            return [rec for f in findings
                    for rec in process_file(path, [f], allowed, keep)]
        return skip_all(note, reason, error)

    try:
        content = open(path, encoding="utf-8", errors="replace").read()
    except FileNotFoundError:
        return skip_all("file not in checkout", "missing-file")
    if len(content) > 25000:
        return skip_all("file too large for one call", "too-large")
    if not keep:
        # Suggest mode verifies each patch against a pristine file, then
        # reverts. Apply mode must NOT reset: it would wipe fixes applied
        # for earlier files (including ones that end up skipped).
        git("checkout", "--", path, check=False)
    diff, attempts, err, full_err = make_patch_for_file(
        path, findings, content)
    for r in recs:
        r["attempts"] = attempts
    if not diff:
        return fail(f"no valid patch: {err}", failure_reason(err),
                    full_err)
    touched = touched_files(diff)
    if not touched:
        return fail("patch without file headers", "unparseable")
    if touched - allowed:
        return fail("patch touches files outside the PR", "outside-pr")
    if touched - {path}:
        return fail("patch touches files beyond the finding's path",
                    "outside-file")
    sha = hashlib.sha256(diff.encode()).hexdigest()[:12]
    for r in recs:
        r["patch_sha"] = sha
        r["patch"] = diff
    git("apply", "/tmp/autofix.patch")
    ok, note = verify(path)
    if not ok:
        # Reverse only this patch: a full checkout would also wipe fixes
        # already applied for earlier files.
        git("apply", "-R", "/tmp/autofix.patch", check=False)
        if not verify(path)[0]:
            git("checkout", "--", path)
        return fail(f"verification failed: {note}", "verify-failed")
    if not keep:
        git("checkout", "--", path)
        for r in recs:
            r.update(status="verified", reason="verified",
                     note=f"verified in {attempts} attempt(s)")
        return recs
    for r in recs:
        r.update(status="applied", reason="fixed",
                 note=f"applied in {attempts} attempt(s)")
    return recs


def audit_markdown(records, mode):
    counts = {}
    for r in records:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    head = ", ".join(f"{v} {k}" for k, v in sorted(counts.items()))
    rows = ["| File | Bot | Status | Patch | Reason | Note |",
            "| ------ | ------ | ------ | ------ | ------ | ------ |"]
    for r in records:
        rows.append(f"| `{r['path']}:{r['line']}` | {r['bot']} | "
                    f"{r['status']} | `{r['patch_sha'] or '-'}` | "
                    f"{r.get('reason') or '-'} | {r['note']} |")
    # One options block per verified patch (deduplicated: same-file
    # findings share one patch). States the problem, shows the ready
    # suggestion, and names the tradeoff; Option B is always leave-as-is.
    seen, options = set(), []
    for r in records:
        sha = r.get("patch_sha") or ""
        if r["status"] not in ("verified", "applied") or not sha:
            continue
        if sha in seen or not r.get("patch"):
            continue
        seen.add(sha)
        problem = (r.get("finding") or "").strip().splitlines()
        problem = " ".join(problem)[:500] or "bot finding (see review)"
        options.append(
            f"#### `{r['path']}` — Option A (recommended, `{sha}`)\n"
            f"Problem: {problem}\n\n"
            f"Tradeoff: {tradeoff_line(r['patch'], r['path'])}\n\n"
            f"```diff\n{r['patch'].rstrip()}\n```\n"
            f"Apply: comment `/autofix apply {sha}` (write access). "
            "Option B is leave as-is (no action).")
    opts = ("\n\n### Suggestions (ready to apply)\n\n" + "\n\n".join(options)
            if options else "")
    if mode == "suggest":
        nxt = ("Verified suggestions only — nothing was pushed. Comment "
               "`/autofix apply all` or `/autofix apply <patch-id>` with "
               "write access and the agent commits the selection. "
               "One-click suggestion Apply buttons need PR review comments; "
               "this audit comment uses the command path.")
    else:
        nxt = "Applied fixes were pushed; re-review will confirm."
    return (f"{MARKER}\n## Autofix audit ({mode})\n\n{head}\n\n"
            + "\n".join(rows) + opts + f"\n\n{nxt}\n")


def post_audit(records, mode):
    body = audit_markdown(records, mode)
    comments = gh_paged(f"/repos/{REPO}/issues/{PR}/comments")
    for c in comments:
        if MARKER in (c.get("body") or ""):
            gh("PATCH", f"/repos/{REPO}/issues/comments/{c['id']}",
               {"body": body})
            return
    gh("POST", f"/repos/{REPO}/issues/{PR}/comments", {"body": body})


def run():
    pr = gh("GET", f"/repos/{REPO}/pulls/{PR}")
    files = gh_paged(f"/repos/{REPO}/pulls/{PR}/files")
    allowed = {f["filename"] for f in files}
    findings = collect_findings(allowed)
    print(f"{len(findings)} bot findings (deduped), mode={MODE}")
    keep = MODE == "apply"
    records = [rec for path, group in group_by_file(findings)
               for rec in process_file(path, group, allowed, keep)]
    if keep:
        records = deselect_unwanted(records)
    audit_path = os.path.join(os.environ.get("RUNNER_TEMP", "/tmp"),
                              "autofix-audit.json")
    with open(audit_path, "w") as fh:
        json.dump({"pr": PR, "mode": MODE, "model": MODEL,
                   "records": records}, fh, indent=1)
    tracked = [ln for ln in
               git("status", "--short").stdout.splitlines()
               if not ln.startswith("??")]
    if keep and tracked:
        # -u stages only tracked modifications: -A would sweep untracked
        # junk (such as __pycache__) into the fix commit.
        git("add", "-u")
        git("-c", "user.name=autofix-bot",
            "-c", "user.email=autofix-bot@users.noreply.github.com",
            "commit", "-m", "autofix: apply verified bot review findings")
        git("push", "origin", "HEAD:refs/heads/" + pr["head"]["ref"])
    post_audit(records, MODE)
    return records


def main():
    try:
        label = f"/repos/{REPO}/issues/{PR}/labels/autofix"
        gh("DELETE", label)
    except Exception as e:
        print("label removal failed (non-fatal):", e)
    try:
        run()
    finally:
        try:
            gh("DELETE", f"/repos/{REPO}/issues/{PR}/labels/autofix")
        except Exception as e:
            print("label removal failed (non-fatal):", e)


if __name__ == "__main__":
    main()
