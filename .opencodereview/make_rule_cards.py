#!/usr/bin/env python3
"""Generate rule_cards.json: rule id -> title and knowledge-card URL.

Usage: make_rule_cards.py RULE_JSON OCR_RUNNERS_RULE_DOCS OUT_JSON

The titles are the cards' own first headings (read with gh, which must be
able to read the card repositories). OCR_RUNNERS_RULE_DOCS is a file listing the names in ocr-runners'
docs/rules/ (e.g. `CPP1.md`). A rule links its cpp-idioms card when one
covers it (CARDS below, matched by hand against the card titles), otherwise
its ocr-runners docs/rules/<ID>.md, otherwise nothing. IDM rules name their
cpp-idioms path in their text.
"""

import json
import os
import re
import subprocess
import sys

IDIOMS = "https://github.com/marvin7122/cpp-idioms/blob/main/"
RUNNERS = "https://github.com/marvin7122/ocr-runners/blob/main/docs/rules/"

# Rule id -> cpp-idioms card path. Matches marked `# unsure` cover the rule
# only partly or by a related principle.
CARDS = {
    "CPP1": "rules/functions/parameter-passing.md",
    "CPP2": "rules/ownership-and-lifetime/no-owning-raw-pointers.md",
    "CPP3": "rules/ownership-and-lifetime/non-owning-access-without-raw-pointers.md",
    "CPP4": "rules/move-semantics/move-sinks-forward-forwarding-references.md",
    "CPP5": "rules/move-semantics/rule-of-zero-or-five.md",
    "CPP6": "rules/initialization-and-types/const-correctness.md",
    "CPP9": "rules/exceptions-and-raii/errors-are-exceptions-or-expected-never-swallowed.md",
    "CPP10": "rules/templates/constrain-generic-interfaces.md",
    "CPP11": "rules/ownership-and-lifetime/returned-views-own-what-they-use.md",
    "CPP12": "rules/ownership-and-lifetime/views-are-parameters-not-storage.md",
    "CPP13": "rules/move-semantics/moved-from-is-valid-but-unspecified.md",
    "CPP14": "rules/ownership-and-lifetime/allocator-aware-interfaces.md",  # unsure
    "CPP15": "rules/move-semantics/return-locals-by-name.md",
    "CPP16": "rules/move-semantics/move-sinks-forward-forwarding-references.md",
    "CPP17": "rules/exceptions-and-raii/destructors-and-noexcept-never-throw.md",
    "CPP18": "rules/exceptions-and-raii/destructors-and-noexcept-never-throw.md",
    "CPP20": "rules/exceptions-and-raii/every-resource-has-an-raii-owner.md",
    "CPP21": "rules/exceptions-and-raii/state-the-exception-guarantee.md",
    "CPP22": "rules/exceptions-and-raii/every-resource-has-an-raii-owner.md",
    "CPP23": "rules/concurrency/locks-are-scoped-and-ordered.md",
    "CPP24": "rules/concurrency/locks-are-scoped-and-ordered.md",
    "CPP25": "rules/concurrency/shared-mutable-state-is-synchronized.md",
    "CPP26": "rules/concurrency/shared-mutable-state-is-synchronized.md",  # unsure
    "CPP27": "rules/concurrency/threads-are-joined-and-waits-have-predicates.md",
    "CPP28": "rules/concurrency/shared-mutable-state-is-synchronized.md",  # unsure
    "CPP29": "rules/concurrency/threads-are-joined-and-waits-have-predicates.md",
    "CPP30": "rules/concurrency/default-to-sequentially-consistent-atomics.md",
    "CPP31": "rules/ownership-and-lifetime/coroutine-frames-outlive-their-references.md",
    "CPP32": "rules/ownership-and-lifetime/coroutine-frames-outlive-their-references.md",
    "CPP33": "rules/ownership-and-lifetime/coroutine-frames-outlive-their-references.md",  # unsure
    "CPP34": "rules/templates/constrain-generic-interfaces.md",
    "CPP35": "rules/templates/constrain-generic-interfaces.md",
    "CPP36": "rules/templates/specialisations-keep-the-primary-contract.md",
    "CPP37": "rules/templates/specialisations-keep-the-primary-contract.md",
    "CPP38": "rules/operators/operators-are-hidden-friends-and-comparisons-are-defaulted.md",
    "CPP39": "rules/class-design/explicit-single-argument-constructors.md",
    "CPP40": "rules/arithmetic/no-signed-unsigned-mixing.md",
    "CPP41": "rules/arithmetic/no-overflow-shift-or-zero-division.md",
    "CPP42": "rules/arithmetic/no-signed-unsigned-mixing.md",
    "CPP43": "rules/arithmetic/no-overflow-shift-or-zero-division.md",
    "CPP44": "rules/arithmetic/no-overflow-shift-or-zero-division.md",
    "CPP45": "rules/arithmetic/floating-point-compares-with-tolerance.md",
    "CPP46": "rules/containers-and-ranges/choose-containers-by-access-pattern.md",
    "CPP47": "rules/ownership-and-lifetime/no-references-across-container-mutation.md",
    "CPP48": "rules/ownership-and-lifetime/no-references-across-container-mutation.md",
    "CPP49": "rules/ownership-and-lifetime/no-references-across-container-mutation.md",
    "CPP50": "rules/ownership-and-lifetime/no-references-across-container-mutation.md",  # unsure
    "CPP51": "rules/class-design/scoped-enums.md",
    "CPP53": "rules/ownership-and-lifetime/returned-views-own-what-they-use.md",
    "CPP54": "rules/ownership-and-lifetime/returned-views-own-what-they-use.md",
    "CPP54b": "rules/ownership-and-lifetime/returned-views-own-what-they-use.md",
    "CPP55": "rules/ownership-and-lifetime/returned-views-own-what-they-use.md",  # unsure
    "CPP56": "rules/exceptions-and-raii/destructors-and-noexcept-never-throw.md",
    "CPP57": "rules/class-design/polymorphic-bases-have-virtual-destructors-and-override.md",
    "CPP58": "rules/class-design/no-virtual-calls-in-constructors.md",
    "CPP59": "rules/class-design/polymorphic-bases-have-virtual-destructors-and-override.md",
    "CPP60": "guidelines/design/inheritance-is-for-substitutability.md",  # unsure
    "CPP61": "rules/class-design/no-slicing.md",
    "CPP62": "rules/class-design/no-slicing.md",
    "CPP64": "rules/initialization-and-types/always-initialize-and-prefer-braces.md",  # unsure
    "CPP65": "rules/initialization-and-types/always-initialize-and-prefer-braces.md",
    "CPP66": "rules/initialization-and-types/auto-drops-references-and-cv.md",
    "CPP67": "rules/operators/operators-are-hidden-friends-and-comparisons-are-defaulted.md",  # unsure
    "CPP68": "rules/operators/operators-are-hidden-friends-and-comparisons-are-defaulted.md",
    "CPP69": "rules/move-semantics/rule-of-zero-or-five.md",  # unsure
    "CPP70": "rules/operators/operators-are-hidden-friends-and-comparisons-are-defaulted.md",
    "CPP72": "rules/time/chrono-types-and-steady-clock.md",
    "CQ15": "guidelines/design/one-change-touches-one-place.md",
    "CQ16": "guidelines/design/one-change-touches-one-place.md",
    "CQ17": "guidelines/design/name-the-data-that-travels-together.md",
    "CQ18": "guidelines/design/name-the-data-that-travels-together.md",
    "CQ19": "guidelines/design/no-pass-through-layers.md",
    "CQ20": "guidelines/design/no-pass-through-layers.md",
    "CQ22": "guidelines/design/state-is-valid-for-the-whole-object.md",
    "CQ23": "guidelines/design/inheritance-is-for-substitutability.md",
    "CQ24": "guidelines/design/low-coupling-high-cohesion.md",
    "CQ25": "guidelines/design/one-change-touches-one-place.md",  # unsure
    "CQ26": "guidelines/design/inheritance-is-for-substitutability.md",
    "CQ27": "guidelines/design/inheritance-is-for-substitutability.md",
    "CQ32": "guidelines/design/no-pass-through-layers.md",
    "CQ38": "style/comments-say-why-and-stay-true.md",
    "CQ39": "guidelines/design/apis-are-hard-to-misuse.md",
    "CQ40": "guidelines/design/apis-are-hard-to-misuse.md",
    "CQ41": "guidelines/design/apis-are-hard-to-misuse.md",
    "CQ42": "guidelines/design/apis-are-hard-to-misuse.md",
    "CQ43": "idioms/control-flow/early-return.md",
    "CQ44": "rules/exceptions-and-raii/errors-are-exceptions-or-expected-never-swallowed.md",
    "CQ45": "rules/exceptions-and-raii/errors-are-exceptions-or-expected-never-swallowed.md",
    "CQ46": "rules/exceptions-and-raii/every-resource-has-an-raii-owner.md",
    "CQ47": "guidelines/design/testable-by-construction.md",
    "CQ48": "guidelines/design/testable-by-construction.md",
    "CQ49": "style/tests-are-direct-and-use-matchers.md",
    "CQ50": "guidelines/design/testable-by-construction.md",
    "CQ51": "guidelines/design/low-coupling-high-cohesion.md",
    "CQ52": "guidelines/design/low-coupling-high-cohesion.md",
    "CQ53": "guidelines/design/low-coupling-high-cohesion.md",
    "CQ56": "guidelines/design/constants-are-named-and-tunables-are-configuration.md",
    "CQ57": "style/logging-is-actionable.md",
    "CQ58": "style/logging-is-actionable.md",
    "CQ59": "style/logging-is-actionable.md",
    "CQ60": "style/todos-are-tracked-and-not-shipped-for-this-change.md",
    "CQ61": "style/todos-are-tracked-and-not-shipped-for-this-change.md",
    "CQ62": "style/one-change-per-diff-and-nothing-accidental.md",
    "CQ64": "style/one-change-per-diff-and-nothing-accidental.md",
    "PP4": "style/comments-say-why-and-stay-true.md",  # unsure
    "PP5": "style/reuse-before-writing.md",
    "PP7": "style/tests-are-direct-and-use-matchers.md",
    "PP9": "guidelines/design/no-speculative-generality.md",  # unsure
    "PP10": "style/comments-say-why-and-stay-true.md",
    "PP11": "style/public-apis-have-docstrings.md",
    "PP13": "rules/performance/avoid-needless-copies-and-lookups.md",
    "PP14": "guidelines/design/make-illegal-states-unrepresentable.md",
    "PP15": "style/reuse-before-writing.md",
    "PP16": "rules/functions/preconditions-are-checked.md",
    "PP17": "style/tests-are-direct-and-use-matchers.md",
    "PP19": "rules/ownership-and-lifetime/returned-views-own-what-they-use.md",
    "PP22": "rules/templates/constrain-generic-interfaces.md",
    "PP23": "style/tests-are-direct-and-use-matchers.md",
    "PP24": "guidelines/design/testable-by-construction.md",  # unsure
    "PP25": "style/build-and-ci-changes-are-narrow.md",
    "QC1": "rules/functions/preconditions-are-checked.md",
    "QC3": "rules/functions/preconditions-are-checked.md",
    "QC4": "rules/ownership-and-lifetime/views-are-parameters-not-storage.md",
    "QC9": "style/reuse-before-writing.md",
    "QC18": "style/comments-say-why-and-stay-true.md",
    "QC18b": "style/public-apis-have-docstrings.md",
    "QC22b": "rules/ownership-and-lifetime/returned-views-own-what-they-use.md",
    "QC24": "style/reuse-before-writing.md",  # unsure
    "QC25": "style/definitions-belong-in-the-source-file.md",
    "QC49": "style/logging-is-actionable.md",  # unsure
    "QC51": "rules/concurrency/shared-mutable-state-is-synchronized.md",  # unsure
    "QC52": "rules/exceptions-and-raii/destructors-and-noexcept-never-throw.md",  # unsure
    "QC53": "rules/time/chrono-types-and-steady-clock.md",  # unsure
    "rule:missing-check": "rules/functions/preconditions-are-checked.md",
    "rule:tests": "style/tests-are-direct-and-use-matchers.md",
    "rule:lifetime": "rules/ownership-and-lifetime/returned-views-own-what-they-use.md",
    "rule:move-semantics": "rules/move-semantics/move-sinks-forward-forwarding-references.md",
    "rule:const-ref": "rules/functions/parameter-passing.md",
    "rule:redundant-code": "style/reuse-before-writing.md",
    "rule:unused-code": "guidelines/design/no-speculative-generality.md",
    "rule:performance": "rules/performance/avoid-needless-copies-and-lookups.md",
    "rule:cpp-template-macro": "rules/templates/constrain-generic-interfaces.md",  # unsure
    "rule:helper-placement": "style/reuse-before-writing.md",  # unsure
    "rule:header-implementation-placement": "style/definitions-belong-in-the-source-file.md",
    "rule:naming": "style/names-declare-intent.md",
    "rule:naming-grammar": "style/names-say-what-a-value-is.md",  # unsure
    "rule:idiom": "idioms/control-flow/early-return.md",  # unsure
    "rule:comment-accuracy": "style/comments-say-why-and-stay-true.md",
    "rule:comment-precision": "style/comments-say-why-and-stay-true.md",
    "rule:comment-format": "style/comment-grammar.md",
    "rule:missing-doc": "style/public-apis-have-docstrings.md",
    "rule:stale-comment": "style/comments-say-why-and-stay-true.md",
    "rule:redundant-comment": "style/comments-say-why-and-stay-true.md",
    "rule:todo-needed": "style/todos-are-tracked-and-not-shipped-for-this-change.md",
    "rule:todo-resolved": "style/todos-are-tracked-and-not-shipped-for-this-change.md",
    "rule:pr-structure": "style/one-change-per-diff-and-nothing-accidental.md",
    "rule:migration-cleanup": "guidelines/design/no-speculative-generality.md",  # unsure
    "rule:accidental-formatting": "style/one-change-per-diff-and-nothing-accidental.md",
}


def card_heading(url, cache={}):
    """The first Markdown heading of the card at a GitHub blob URL (read
    with gh), without the leading #, or None."""
    if url in cache:
        return cache[url]
    match = re.match(r"^https://github\.com/([^/]+/[^/]+)/blob/([^/]+)/(.+)$", url)
    heading = None
    if match:
        repo, ref, path = match.groups()
        out = subprocess.run(["gh", "api", f"repos/{repo}/contents/{path}?ref={ref}",
                              "-H", "Accept: application/vnd.github.raw"], capture_output=True, text=True)
        if out.returncode == 0:
            body = re.sub(r"^---\n.*?\n---\n", "", out.stdout, flags=re.S)
            found = re.search(r"^#{1,3}\s+(.+?)\s*$", body, re.M)
            heading = found.group(1) if found else None
    cache[url] = heading
    return heading


def main():
    rule_path, runner_docs_path, out_path = sys.argv[1:4]
    runner_docs = {name[:-3] for name in open(runner_docs_path).read().split() if name.endswith(".md")}
    rules = json.load(open(rule_path))["rules"]
    preamble = os.path.commonprefix([r["rule"] for r in rules])
    start = preamble.rfind("\n\n") + 2 if "\n\n" in preamble else 0
    cards, without = {}, []
    for rule in rules:
        text = " ".join(rule["rule"][start:].split())
        rule_id = text.split(" ", 1)[0].rstrip(":—")
        if not re.match(r"^(CPP|CQ|PP|QC|IDM)\w+$|^rule:[a-z-]+$", rule_id) or rule_id in cards:
            continue
        title = re.sub(r"^\S+\s+(\[[A-Z]+\]\s+)?(—\s+)?", "", text)
        title = re.sub(r"\s*\(cpp-idioms [^)]*\)", "", title)
        title = title.strip()
        title = title[:110] + ("…" if len(title) > 110 else "")
        idm = re.search(r"\(cpp-idioms ([^)\s]+\.md)\)", text)
        if idm:
            url = IDIOMS + idm.group(1)
        elif rule_id in CARDS:
            url = IDIOMS + CARDS[rule_id]
        elif rule_id in runner_docs:
            url = RUNNERS + rule_id + ".md"
        else:
            url = None
            without.append(rule_id)
        # The card's own heading; the rule summary when the card has none or
        # its heading is only the rule id (the ocr-runners stubs).
        heading = card_heading(url) if url else None
        if not heading or heading.strip("` ") == rule_id:
            heading = title
        cards[rule_id] = {"title": heading, "url": url}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(cards, f, indent=1, ensure_ascii=False)
        f.write("\n")
    linked = sum(1 for c in cards.values() if c["url"])
    print(f"{len(cards)} rule ids, {linked} with a card; without: {' '.join(without)}")


if __name__ == "__main__":
    main()
