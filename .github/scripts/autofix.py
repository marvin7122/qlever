#!/usr/bin/env python3
"""Bot-finding autofix for Qlever PRs (v2).

Two phases, CodeRabbit-style:
  suggest: collect bot inline findings, generate minimal patches with an
           LLM, verify each (apply-check + targeted checks), and post a
           sticky audit table plus a JSON record. Nothing is pushed.
  apply:   regenerate on the current head and push the verified patches
           as one commit (triggered only by a human approval comment).

Environment:
  GITHUB_REPOSITORY   owner/repo
  PR_NUMBER           pull request number
  GITHUB_TOKEN        token with contents:write + pull-requests:write
  OPENROUTER_API_KEY  model key (spend-capped on the OpenRouter side)
  FIX_MODEL           model id (default qwen/qwen3-coder-30b-a3b-instruct)
  MAX_FINDINGS        cap per run (default 25)
  MODE                suggest (default) | apply
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
    return {
        m.group(1)
        for m in re.finditer(r"^[+-]{3} [ab]/(.+)$", diff, re.M)
    }


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


def collect_findings():
    """Newest bot inline comments, deduped by (path, line), richest body wins."""
    comments = gh_paged(f"/repos/{REPO}/pulls/{PR}/comments")
    by_key = {}
    for c in comments:
        author = (c.get("user") or {}).get("login") or ""
        if author not in BOT_AUTHORS or not c.get("path") or not c.get("body"):
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


FIX_PROMPT = """\
You fix one code-review finding with the smallest correct change.
Rules: reply with ONLY a unified diff (--- a/<path> / +++ b/<path> headers,
@@ hunks), no prose, no code fences. Keep the change minimal; do not
refactor surroundings. Count every hunk's @@ -start,len +start,len @@
numbers exactly from its body lines. The diff must apply with `git apply`
to the file content below.

Finding (file {path}, line {line}):
{finding}

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


def make_patch(finding, content):
    """-> (diff, attempts, error). Retries once with the git error fed back."""
    text = call_model(FIX_PROMPT.format(
        path=finding["path"], line=finding["line"],
        finding=finding["body"], content=content))
    diff = normalize_diff(extract_diff(text))
    if not diff:
        return None, 1, "no diff in response"
    for attempt in (1, 2):
        r = check_apply(diff)
        if r.returncode == 0:
            return diff, attempt, ""
        if attempt == 2:
            return None, 2, (r.stderr or r.stdout).strip().splitlines()[0]
        print(f"retry {finding['path']}:{finding['line']} after: "
              f"{r.stderr.strip().splitlines()[0] if r.stderr else ''}")
        text = call_model(RETRY_PROMPT.format(
            error=(r.stderr or r.stdout)[:800], diff=diff, content=content))
        diff = normalize_diff(extract_diff(text))
        if not diff:
            return None, 2, "no diff in retry response"
    return None, 2, "unreachable"


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


def process(finding, allowed, keep):
    """Generate, verify, optionally keep one patch. -> audit record."""
    path, line = finding["path"], finding["line"]
    rec = {**{k: finding[k] for k in ("path", "line", "bot")},
           "status": "", "patch_sha": "", "attempts": 0, "note": ""}
    try:
        content = open(path, encoding="utf-8", errors="replace").read()
    except FileNotFoundError:
        rec.update(status="skipped", note="file not in checkout")
        return rec
    if len(content) > 25000:
        rec.update(status="skipped", note="file too large for one call")
        return rec
    git("checkout", "--", path, check=False)
    diff, attempts, err = make_patch(finding, content)
    rec["attempts"] = attempts
    if not diff:
        rec.update(status="skipped", note=f"no valid patch: {err}")
        return rec
    touched = touched_files(diff)
    if not touched:
        rec.update(status="skipped", note="patch without file headers")
        return rec
    if touched - allowed:
        rec.update(status="skipped",
                   note="patch touches files outside the PR")
        return rec
    rec["patch_sha"] = hashlib.sha256(diff.encode()).hexdigest()[:12]
    git("apply", "/tmp/autofix.patch")
    ok, note = verify(path)
    if not ok:
        git("checkout", "--", path)
        rec.update(status="skipped", note=f"verification failed: {note}")
        return rec
    if not keep:
        git("checkout", "--", path)
        rec.update(status="verified",
                   note=f"verified in {attempts} attempt(s)")
        return rec
    rec.update(status="applied", note=f"applied in {attempts} attempt(s)")
    return rec


def audit_markdown(records, mode):
    counts = {}
    for r in records:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    head = ", ".join(f"{v} {k}" for k, v in sorted(counts.items()))
    rows = ["| File | Bot | Status | Patch | Note |",
            "| ------ | ------ | ------ | ------ | ------ |"]
    for r in records:
        rows.append(f"| `{r['path']}:{r['line']}` | {r['bot']} | "
                    f"{r['status']} | `{r['patch_sha'] or '-'}` | "
                    f"{r['note']} |")
    nxt = ("Verified fixes only — comment `/autofix apply` with write "
           "access to apply them." if mode == "suggest" else
           "Applied fixes were pushed; re-review will confirm.")
    return (f"{MARKER}\n## Autofix audit ({mode})\n\n{head}\n\n"
            + "\n".join(rows) + f"\n\n{nxt}\n")


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
    findings = collect_findings()
    print(f"{len(findings)} bot findings (deduped), mode={MODE}")
    keep = MODE == "apply"
    records = [process(f, allowed, keep) for f in findings]
    with open("autofix-audit.json", "w") as fh:
        json.dump({"pr": PR, "mode": MODE, "model": MODEL,
                   "records": records}, fh, indent=1)
    if keep and git("status", "--short").stdout.strip():
        git("add", "-A")
        git("-c", "user.name=autofix-bot",
            "-c", "user.email=autofix-bot@users.noreply.github.com",
            "commit", "-m", "autofix: apply verified bot review findings")
        git("push", "origin", "HEAD:refs/heads/" + pr["head"]["ref"])
    post_audit(records, MODE)
    return records


def main():
    try:
        run()
    finally:
        try:
            gh("DELETE", f"/repos/{REPO}/issues/{PR}/labels/autofix")
        except Exception as e:
            print("label removal failed (non-fatal):", e)


if __name__ == "__main__":
    main()
