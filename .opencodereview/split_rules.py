#!/usr/bin/env python3
"""Deterministically split .opencodereview/rule.json into N chunk files.

Usage: split_rules.py RULE_JSON NUM_CHUNKS CHUNK_INDEX OUT_PATH

Rules are dealt contiguously so every chunk keeps related rules together
and re-running with the same inputs yields byte-identical chunks. The
include/exclude globs are copied into every chunk unchanged.
"""

import json
import sys


def main() -> None:
    rule_path, num_chunks, chunk_index, out_path = (
        sys.argv[1],
        int(sys.argv[2]),
        int(sys.argv[3]),
        sys.argv[4],
    )
    if not 0 <= chunk_index < num_chunks:
        raise SystemExit(
            f"chunk index {chunk_index} out of range for {num_chunks} chunks"
        )
    with open(rule_path, encoding="utf-8") as f:
        data = json.load(f)
    rules = data.get("rules", [])
    chunk_size = (len(rules) + num_chunks - 1) // num_chunks
    part = rules[chunk_index * chunk_size : (chunk_index + 1) * chunk_size]
    chunk = {**data, "rules": part}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(chunk, f, indent=1)
        f.write("\n")
    print(f"wrote {len(part)}/{len(rules)} rules to {out_path}")


if __name__ == "__main__":
    main()
