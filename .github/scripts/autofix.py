#!/usr/bin/env python3
"""Bot-finding autofix for Qlever PRs (v1).

Collects inline review comments posted by known bot reviewers on a pull
request, asks an LLM for a minimal unified diff per finding, applies the
diffs that pass `git apply --check`, commits once, and pushes to the PR
branch. Only files already touched by the PR are eligible for edits.

Environment:
  GITHUB_REPOSITORY   owner/repo
  PR_NUMBER           pull request number
  GITHUB_TOKEN        token with contents:write + pull-requests:write
  OPENROUTER_API_KEY  model key (spend-capped on the OpenRouter side)
  FIX_MODEL           model id (default qwen/qwen3-coder-30b-a3b-instruct)
  MAX_FINDINGS        cap per run (default 10)
"""

import json
import os
import subprocess
import sys
import urllib.request

REPO = os.environ["GITHUB_REPOSITORY"]
PR = int(os.environ["PR_NUMBER"])
GH_TOKEN = os.environ["GITHUB_TOKEN"]
OR_KEY = os.environ["OPENROUTER_API_KEY"]
MODEL = os.environ.get("FIX_MODEL", "qwen/qwen3-coder-30b-a3b-instruct")
MAX_FINDINGS = int(os.environ.get("MAX_FINDINGS", "10"))
API = "https://api.github.com"

BOT_AUTHORS = {
    "coderabbitai[bot]",
    "qodo-code-review[bot]",
    "codeant-ai[bot]",
    "sourcery-ai[bot]",
    "deepsource-io[bot]",
    "superagent-security[bot]",
    "greptile-apps[bot]",
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
    return json.load(urllib.request.urlopen(req))


def gh_paged(path):
    out, page = [], 1
    while True:
        batch = gh("GET", f"{path}?per_page=100&page={page}")
        out.extend(batch)
        if len(batch) < 100:
            return out
        page += 1


def sh(*args, check=True):
    r = subprocess.run(args, capture_output=True, text=True)
    if check and r.returncode != 0:
        print(r.stdout[-2000:], r.stderr[-2000:])
        raise SystemExit(f"command failed: {args}")
    return r


def openrouter(messages, max_tokens=1500):
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
        return json.load(urllib.request.urlopen(req))
    except Exception as e:
        print("model call failed:", getattr(e, "read", lambda: e)())
        return None


def collect_findings():
    """Bot inline comments on the PR diff. One entry per (path, line)."""
    comments = gh_paged(f"/repos/{REPO}/pulls/{PR}/comments")
    seen, findings = set(), []
    for c in comments:
        if (c.get("user") or {}).get("login") not in BOT_AUTHORS:
            continue
        if not c.get("path") or not c.get("body"):
            continue
        key = (c["path"], c.get("line") or c.get("original_line"), c["body"][:200])
        if key in seen:
            continue
        seen.add(key)
        findings.append(
            {
                "path": c["path"],
                "line": c.get("line") or c.get("original_line"),
                "body": c["body"][:3000],
            }
        )
    return findings[:MAX_FINDINGS]


FIX_PROMPT = """\
You fix one code-review finding with the smallest correct change.
Rules: reply with ONLY a unified diff (--- a/<path> / +++ b/<path> headers,
@@ hunks), no prose. Keep the change minimal; do not refactor surroundings.
The diff must apply with `git apply` to the file content below.

Finding (file {path}, line {line}):
{finding}

File content:
```
{content}
```
"""


def fix_one(path, line, body, allowed):
    try:
        content = open(path, encoding="utf-8", errors="replace").read()
    except FileNotFoundError:
        print(f"skip {path}: not in checkout")
        return False
    if len(content) > 20000:
        print(f"skip {path}: file too large for one call")
        return False
    resp = openrouter(
        [{"role": "user", "content": FIX_PROMPT.format(
            path=path, line=line, finding=body, content=content)}]
    )
    if resp is None:
        return False
    try:
        text = resp["choices"][0]["message"]["content"]
    except (KeyError, IndexError):
        print(f"skip {path}: bad model response")
        return False
    start = text.find("--- ")
    if start < 0:
        print(f"skip {path}: no diff in response")
        return False
    diff = text[start:].strip() + "\n"
    touched = [
        ln[6:] for ln in diff.splitlines()
        if ln.startswith("+++ b/") or ln.startswith("--- a/")
    ]
    if any(t not in allowed for t in set(touched)):
        print(f"skip {path}: patch touches files outside the PR")
        return False
    with open("/tmp/autofix.patch", "w") as f:
        f.write(diff)
    r = sh("git", "apply", "--check", "/tmp/autofix.patch", check=False)
    if r.returncode != 0:
        print(f"skip {path}: patch does not apply cleanly")
        return False
    sh("git", "apply", "/tmp/autofix.patch")
    print(f"applied fix for {path}:{line}")
    return True


def main():
    pr = gh("GET", f"/repos/{REPO}/pulls/{PR}")
    files = gh_paged(f"/repos/{REPO}/pulls/{PR}/files")
    allowed = {f["filename"] for f in files}
    findings = collect_findings()
    print(f"{len(findings)} bot findings collected")
    if not findings:
        gh("POST", f"/repos/{REPO}/issues/{PR}/comments",
           {"body": "Autofix: no bot findings to fix."})
        return
    applied, skipped = [], []
    for f in findings:
        ok = fix_one(f["path"], f["line"], f["body"], allowed)
        (applied if ok else skipped).append(f"{f['path']}:{f['line']}")
    status = sh("git", "status", "--short").stdout.strip()
    if not status:
        summary = "Autofix: nothing applied, everything skipped."
    else:
        r = sh("git", "diff", "--check", check=False)
        if r.returncode != 0:
            print("whitespace errors, aborting push")
            sh("git", "checkout", "--", ".")
            gh("POST", f"/repos/{REPO}/issues/{PR}/comments",
               {"body": "Autofix: patches failed the whitespace check, "
                        "nothing was pushed."})
            return
        sh("git", "add", "-A")
        sh("git", "-c", "user.name=autofix-bot",
           "-c", "user.email=autofix-bot@users.noreply.github.com",
           "commit", "-m", "autofix: apply bot review findings")
        sh("git", "push", "origin", f"HEAD:{pr['head']['ref']}")
        summary = (
            f"Autofix applied {len(applied)} finding(s), "
            f"skipped {len(skipped)}.\n"
            + "".join(f"\n- fixed {a}" for a in applied)
            + "".join(f"\n- skipped {s}" for s in skipped)
            + "\nCI and bot re-reviews will validate; re-add the "
              "`autofix` label to run again."
        )
    gh("POST", f"/repos/{REPO}/issues/{PR}/comments", {"body": summary})
    try:
        gh("DELETE", f"/repos/{REPO}/issues/{PR}/labels/autofix")
    except Exception as e:
        print("label removal failed (non-fatal):", e)


if __name__ == "__main__":
    main()
