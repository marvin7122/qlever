#!/usr/bin/env python3
"""Stage 1 of the review pipeline: deterministic checks on the changed lines.

Usage: stage1_deterministic.py CLANG_FORMAT_STYLE_FILE REPORT_MD

Environment: GH_TOKEN, REPO (owner/name), PR_NUMBER, GITHUB_OUTPUT, and
optionally CLANG_FORMAT (default clang-format-18, as in format-check.yml).

Checks, all without a model call:
- clang-format on the added lines of every changed C++ file (error level).
- The mechanical project rules of .opencodereview/rule.json, listed in
  SCRIPTED_RULE_IDS: each is a pattern on the added lines (warning level).
  select_rules.py leaves these rules out of the model stages.

Writes `ok=false` to GITHUB_OUTPUT if any error-level finding exists (the
pipeline stops before the model stages), and the findings to REPORT_MD.
"""

import json
import os
import re
import subprocess
import sys
import tempfile

CPP_FILE = re.compile(r"\.(h|hpp|cpp|cc|cxx)$")
MAX_REPORTED = 60

# The rules of rule.json that this script implements completely. Their ids are
# the first word of the rule text after the shared preamble.
SCRIPTED_RULE_IDS = {
    "CPP15", "CPP23", "CPP28", "CPP51", "CPP72", "CQ60", "PP2", "QC2", "QC42",
    "QC44", "QC49", "QC5", "rule:cpp-template-macro", "IDM026", "IDM075",
}


def code_part(line):
    """The line without a trailing `//` comment (string literals ignored)."""
    return re.sub(r'"(?:\\.|[^"\\])*"', '""', line).split("//", 1)[0]


def comment_part(line):
    stripped = re.sub(r'"(?:\\.|[^"\\])*"', '""', line)
    return stripped.split("//", 1)[1] if "//" in stripped else ""


# (rule id, message, applies to path, test on (line, previous lines)).
def code_rule(rule_id, pattern, message, path_filter=lambda p: True):
    regex = re.compile(pattern)
    return (rule_id, message, path_filter, lambda line, _: bool(regex.search(code_part(line))))


in_src = lambda p: p.startswith("src/")  # noqa: E731

LINE_RULES = [
    code_rule("CPP15", r"\breturn\s+std::move\(\s*[a-z]\w*[a-z0-9]\s*\)\s*;",
              "`return std::move(local)` suppresses copy elision; return the local directly"),
    code_rule("CPP23", r"\.\s*(un)?lock\s*\(\s*\)",
              "manual `lock()`/`unlock()`; use `std::scoped_lock` or `std::lock_guard`"),
    code_rule("CPP28", r"\bvolatile\b", "`volatile` gives no inter-thread ordering; use `std::atomic`"),
    code_rule("CPP51", r"\benum\s+(?!class\b|struct\b)[A-Za-z_]\w*\s*(:|\{|$)",
              "plain `enum`; use `enum class`"),
    code_rule("CPP72", r"\bsystem_clock::now\b",
              "`system_clock` for time measurement; use `steady_clock` (or `ad_utility::Timer`)"),
    ("CQ60", "TODO/FIXME without an owner; write `TODO<name>`", lambda p: True,
     lambda line, _: bool(re.search(r"\b(TODO|FIXME)\b(?!\s*<)", comment_part(line)))),
    code_rule("QC2", r"\bthrow\s+std::\w+\s*[({]",
              "bare `throw std::...`; use `AD_THROW(...)` (or `AD_FAIL()` for unreachable code)"),
    code_rule("QC42", r"\bstd::unordered_(multi)?(map|set)\b",
              "`std::unordered_map/set`; use `ad_utility::HashMap`/`HashSet`"),
    code_rule("QC44", r"#\s*include\s*[<\"]nlohmann/json",
              "include `util/json.h` instead of nlohmann directly"),
    code_rule("QC49", r"\bstd::(cout|cerr)\b|\b(f?printf)\s*\(",
              "console output; use the `AD_LOG_*` macros from `util/Log.h`", in_src),
    code_rule("QC5", r"\bstd::(span|ranges::|views::)",
              "use the `ql::` backports (`ql::span`, `ql::ranges`, `ql::views`)"),
    # `requires` inside the CPP_ macros (`CPP_template(...)(requires ...)`,
    # `CPP_requires(name, requires(...)(...))`) is the sanctioned form.
    ("rule:cpp-template-macro",
     "raw `requires`; use `CPP_template` / `CPP_requires` from `backports/concepts.h`", lambda p: True,
     lambda line, _: "CPP_" not in line and bool(re.search(r"(?<![A-Za-z0-9_])requires\b", code_part(line)))),
    code_rule("IDM026", r"\bfor\s*\([^;:()]*;[^;]*;",
              "index loop `for (init; cond; step)`; iterate over a range"),
    ("IDM075", "test without the `// ___` separator comment before it", lambda p: "test" in p.lower(),
     lambda line, previous: bool(re.match(r"\s*TEST(_F|_P)?\s*\(", line))
     and not re.match(r"\s*//\s*_{5,}", next((l for l in reversed(previous) if l.strip()), ""))),
]


def gh_api(path, raw=False):
    command = ["gh", "api", path]
    if raw:
        command += ["-H", "Accept: application/vnd.github.raw"]
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def added_lines(patch):
    """Line numbers (in the new file) of the added lines of a unified diff."""
    numbers, new_line = [], 0
    for line in (patch or "").splitlines():
        hunk = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
        if hunk:
            new_line = int(hunk.group(1))
        elif line.startswith("+"):
            numbers.append(new_line)
            new_line += 1
        elif not line.startswith("-"):
            new_line += 1
    return numbers


def clang_format_findings(path, content, lines, style_file):
    if not lines:
        return []
    clang_format = os.environ.get("CLANG_FORMAT", "clang-format-18")
    suffix = os.path.splitext(path)[1]
    with tempfile.NamedTemporaryFile("w", suffix=suffix, delete=False) as f:
        f.write(content)
        temp = f.name
    ranges = [f"--lines={n}:{n}" for n in lines]
    result = subprocess.run(
        [clang_format, f"--style=file:{style_file}", "--dry-run", "-Werror", *ranges, temp],
        capture_output=True, text=True,
    )
    os.unlink(temp)
    # clang-format reports every violating column; report each line once.
    bad_lines = sorted({
        int(match.group(1))
        for match in re.finditer(r":(\d+):\d+: (?:error|warning): code should be clang-formatted", result.stderr)
    })
    findings = [("error", path, n, "clang-format", "not formatted with clang-format") for n in bad_lines]
    if result.returncode != 0 and not findings:
        raise RuntimeError(f"clang-format failed on {path}: {result.stderr.strip()[:300]}")
    return findings


def main():
    style_file, report_path = sys.argv[1], sys.argv[2]
    repo, number = os.environ["REPO"], os.environ["PR_NUMBER"]
    sha = json.loads(gh_api(f"repos/{repo}/pulls/{number}"))["head"]["sha"]
    files = []
    for page in range(1, 31):
        batch = json.loads(gh_api(f"repos/{repo}/pulls/{number}/files?per_page=100&page={page}"))
        files += batch
        if len(batch) < 100:
            break

    findings = []
    for f in files:
        path = f["filename"]
        if not CPP_FILE.search(path) or f["status"] == "removed":
            continue
        content = gh_api(f"repos/{repo}/contents/{path}?ref={sha}", raw=True)
        file_lines = content.splitlines()
        added = added_lines(f.get("patch"))
        findings += clang_format_findings(path, content, added, style_file)
        if f["status"] == "added" and not re.search(r"Copyright", "\n".join(file_lines[:5])):
            findings.append(("warning", path, 1, "PP2", "new file without the QLever copyright header"))
        for number_ in added:
            line = file_lines[number_ - 1] if number_ <= len(file_lines) else ""
            previous = file_lines[max(0, number_ - 6):number_ - 1]
            for rule_id, message, path_filter, test in LINE_RULES:
                if path_filter(path) and test(line, previous):
                    findings.append(("warning", path, number_, rule_id, message))

    errors = [f for f in findings if f[0] == "error"]
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write(f"ok={'false' if errors else 'true'}\n")
        out.write(f"num_findings={len(findings)}\n")
    for f in findings:
        print(f"[stage1] {f[0]} {f[1]}:{f[2]} [{f[3]}] {f[4]}", file=sys.stderr)

    if findings:
        head = (
            f"**Review stopped at stage 1 (deterministic checks)** for `{sha[:9]}`; no model was called."
            if errors
            else f"**Stage 1 (deterministic checks)** for `{sha[:9]}`: {len(findings)} warning(s); the model review continues."
        )
        lines = [head, ""]
        for severity, path, line_number, rule_id, message in findings[:MAX_REPORTED]:
            lines.append(f"- {severity}: `{path}:{line_number}` [{rule_id}] {message}")
        if len(findings) > MAX_REPORTED:
            lines.append(f"- ... and {len(findings) - MAX_REPORTED} more")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    print(f"[stage1] {len(errors)} error(s), {len(findings) - len(errors)} warning(s)", file=sys.stderr)


if __name__ == "__main__":
    main()
