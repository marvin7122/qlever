#!/usr/bin/env python3
"""Stage 0a of the review pipeline: stop before anything else if the LLM
account cannot pay for the review.

Asks DeepSeek's balance endpoint (`GET <scheme>://<host>/user/balance`, which
costs no tokens) whether the account is available and holds at least the
review budget (`OCR_REVIEW_BUDGET_USD`) in USD. Other providers are not
checked. A failure of the endpoint itself (network, 5xx) does not stop the
review; a rejected key (401/403) or an empty account (402) does.

Usage: stage0_balance.py <report.md>; reads OCR_LLM_URL, OCR_LLM_AUTH_TOKEN
and OCR_REVIEW_BUDGET_USD, and writes `ok=true|false` to GITHUB_OUTPUT.
"""
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

# Answers of the balance endpoint that mean every model call would fail too.
ACCOUNT_ERRORS = {401, 402, 403}


def check_balance(url, token, budget):
    """Return (ok, reason)."""
    parts = urllib.parse.urlsplit(url)
    if parts.hostname != "api.deepseek.com":
        print(f"[stage0] balance check skipped: {parts.hostname} is not DeepSeek", file=sys.stderr)
        return True, ""
    request = urllib.request.Request(
        f"{parts.scheme}://{parts.netloc}/user/balance",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            info = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code in ACCOUNT_ERRORS:
            return False, f"the DeepSeek balance endpoint answered `{error.code} {error.reason}`"
        print(f"[stage0] balance check failed ({error.code}); not stopping", file=sys.stderr)
        return True, ""
    except (urllib.error.URLError, TimeoutError, ValueError) as error:
        print(f"[stage0] balance check failed ({error!r}); not stopping", file=sys.stderr)
        return True, ""
    usd = [float(b["total_balance"]) for b in info.get("balance_infos", []) if b.get("currency") == "USD"]
    print(f"[stage0] DeepSeek: available={info.get('is_available')}, USD balance={usd}", file=sys.stderr)
    if info.get("is_available") is False:
        return False, "DeepSeek reports the account as unavailable (`is_available: false`)"
    if usd and usd[0] < budget:
        return False, f"the DeepSeek balance is ${usd[0]:.2f}, below the review budget of ${budget:.2f}"
    return True, ""


def main():
    report_path = sys.argv[1]
    budget = float(os.environ.get("OCR_REVIEW_BUDGET_USD") or 0)
    ok, reason = check_balance(os.environ.get("OCR_LLM_URL", ""), os.environ.get("OCR_LLM_AUTH_TOKEN", ""), budget)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write(f"ok={'true' if ok else 'false'}\n")
    if not ok:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"**Review stopped at stage 0 (LLM account):** {reason}; no model was called.\n\n"
                    "Top up the balance, then trigger the review again.\n")


if __name__ == "__main__":
    main()
