#!/usr/bin/env python3
"""Stage 0 of the review pipeline: the PR's existing CI must have passed.

Usage: stage0_ci_gate.py REPORT_MD

Environment: GH_TOKEN, REPO (owner/name), PR_NUMBER, GITHUB_OUTPUT, and
optionally CI_WORKFLOWS (newline-separated workflow names), CI_WAIT_MINUTES
(default 120) and CI_POLL_SECONDS (default 60).

Reads the workflow runs (and SonarCloud check runs) of the PR's head commit.
Only the listed workflows count, and only those that ran for that commit.
While one of them is still running, waits (bounded); no model is called before
CI is done. Writes `ok=true|false` and `head_sha` to GITHUB_OUTPUT, and on
failure a report for the PR comment to REPORT_MD.
"""

import json
import os
import subprocess
import sys
import time

DEFAULT_WORKFLOWS = [
    "Build and test",
    "Format check",
    "Codespell",
    "pr81 gcc11 Werror check",
    "CPP17 libQLever",
    "sonarcloud-analysis",
]
# Conclusions that pass the gate. `skipped`: a workflow whose jobs do not
# apply to this PR (e.g. path or base-branch filters).
PASSING = {"success", "skipped", "neutral"}


def gh_api(path):
    out = subprocess.run(["gh", "api", path], check=True, capture_output=True, text=True).stdout
    return json.loads(out)


def latest_by_name(items, name_key, time_key):
    latest = {}
    for item in items:
        name = item[name_key]
        if name not in latest or item[time_key] > latest[name][time_key]:
            latest[name] = item
    return latest


def ci_state(repo, sha, workflows):
    """Return [(name, status, conclusion, url)] of the relevant CI of sha."""
    runs = gh_api(f"repos/{repo}/actions/runs?head_sha={sha}&per_page=100")["workflow_runs"]
    relevant = [r for r in runs if r["name"] in workflows]
    states = [
        (name, run["status"], run["conclusion"], run["html_url"])
        for name, run in sorted(latest_by_name(relevant, "name", "created_at").items())
    ]
    checks = gh_api(f"repos/{repo}/commits/{sha}/check-runs?per_page=100")["check_runs"]
    sonar = [c for c in checks if "sonarcloud" in (c.get("app") or {}).get("slug", "").lower()]
    for name, check in sorted(latest_by_name(sonar, "name", "started_at").items()):
        states.append((name, check["status"], check["conclusion"], check["html_url"]))
    return states


def main():
    report_path = sys.argv[1]
    repo, number = os.environ["REPO"], os.environ["PR_NUMBER"]
    workflows = [w.strip() for w in os.environ.get("CI_WORKFLOWS", "").splitlines() if w.strip()]
    workflows = workflows or DEFAULT_WORKFLOWS
    wait_seconds = int(os.environ.get("CI_WAIT_MINUTES", "120")) * 60
    poll_seconds = int(os.environ.get("CI_POLL_SECONDS", "60"))

    sha = gh_api(f"repos/{repo}/pulls/{number}")["head"]["sha"]
    deadline = time.monotonic() + wait_seconds
    while True:
        states = ci_state(repo, sha, workflows)
        pending = [s for s in states if s[1] != "completed"]
        failed = [s for s in states if s[1] == "completed" and s[2] not in PASSING]
        for name, status, conclusion, _ in states:
            print(f"[stage0] {name}: {status}/{conclusion}", file=sys.stderr)
        if failed or not pending or time.monotonic() >= deadline:
            break
        print(f"[stage0] waiting for {len(pending)} CI run(s)", file=sys.stderr)
        time.sleep(poll_seconds)

    ok = not failed and not pending
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write(f"ok={'true' if ok else 'false'}\n")
        out.write(f"head_sha={sha}\n")
    if ok:
        print(f"[stage0] passed ({len(states)} CI run(s) for {sha[:9]})", file=sys.stderr)
        return
    lines = [f"**Review stopped at stage 0 (CI)** for `{sha[:9]}`; no model was called.", ""]
    for name, status, conclusion, url in failed:
        lines.append(f"- [{name}]({url}): {conclusion}")
    for name, status, conclusion, url in pending:
        lines.append(f"- [{name}]({url}): still {status} after {wait_seconds // 60} min")
    lines += ["", "Fix CI, then trigger the review again."]
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("[stage0] stopped:\n" + "\n".join(lines), file=sys.stderr)


if __name__ == "__main__":
    main()
