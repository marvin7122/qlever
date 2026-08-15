#!/usr/bin/env python3
# Copyright 2026, University of Freiburg,
# Chair of Algorithms and Data Structures.
# Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

"""
W3C equivalence oracle for QLever export outputs (MVP).

Compare two QLever export output files and report whether they are
equivalent under the W3C data model, rather than byte-identical.  The
SPARQL 1.1 Query specification and the RDF 1.1 concepts it builds on
require the export to be an equivalent representation of the query
result, not a byte-identical one:

* A CONSTRUCT result is an RDF graph.  Two graphs are equivalent when
  they contain the same set of triples, where blank-node identity is
  taken up to isomorphism and triple/term ordering carries no semantic
  weight.
* A SELECT result is a multiset of solutions.  Two solution multisets
  are equivalent when they contain the same bindings with the same
  multiplicities, independent of the order in which the solutions were
  serialized.

This tool is the test oracle for the parallel export paths: the parallel
CONSTRUCT serializer may legitimately produce a different byte stream
from the serial path as long as the two outputs remain equivalent in the
W3C sense.  For RDF graph formats the comparison delegates to rdflib's
`compare.isomorphic`, which performs blank-node-aware graph isomorphism.
For tabular (TSV/CSV) SELECT output the tool compares the solution
multisets after canonicalising blank-node labels.

Exit status: 0 when the two outputs are equivalent, 1 when they differ,
2 on usage or parse errors.  The human-readable verdict is printed to
stdout.

Usage:
  check_export_equivalence.py --format turtle FILE_A FILE_B
  check_export_equivalence.py --format ntriples FILE_A FILE_B
  check_export_equivalence.py --format tsv FILE_A FILE_B
  check_export_equivalence.py --format csv FILE_A FILE_B

Format selection:
  turtle     RDF graph in Turtle syntax (CONSTRUCT result)
  ntriples   RDF graph in N-Triples syntax (CONSTRUCT result)
  tsv        SELECT result as tab-separated values
  csv        SELECT result as comma-separated values

Requires rdflib (pip install rdflib) for the RDF graph formats.
"""
import csv
import re
import sys
from typing import Iterable, List, Tuple

_BNODE_LABEL = re.compile(r"^_:[A-Za-z0-9_\-]+$")


def _is_bnode(term: str) -> bool:
    """True when `term` is an RDF blank-node label like `_:b0`."""
    return _BNODE_LABEL.match(term) is not None


def _canonicalise_bnodes(rows: Iterable[Tuple[str, ...]],
                         ) -> List[Tuple[str, ...]]:
    """Map blank-node labels in a solution table to canonical first-use
    labels so that the same logical blank node compares equal across the
    two files even when the serializer used different labels."""
    mapping: dict[str, str] = {}
    counter = 0
    canonical: List[Tuple[str, ...]] = []
    for row in rows:
        out = []
        for term in row:
            if _is_bnode(term):
                if term not in mapping:
                    mapping[term] = f"_:b{counter}"
                    counter += 1
                out.append(mapping[term])
            else:
                out.append(term)
        canonical.append(tuple(out))
    return canonical


def _load_tsv(path: str) -> List[Tuple[str, ...]]:
    with open(path, encoding="utf-8") as fh:
        # TSV in QLever uses a tab separator.  Skip the header line (the
        # first line, which lists the projected variable names).
        return [tuple(line.rstrip("\n").split("\t")) for line in fh][1:]


def _load_csv(path: str) -> List[Tuple[str, ...]]:
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        rows = [tuple(row) for row in reader]
    # Drop the header row (variable names).
    return rows[1:]


def _compare_multisets(a: List[Tuple[str, ...]],
                       b: List[Tuple[str, ...]]) -> bool:
    """True when `a` and `b` are the same multiset of solutions."""
    if len(a) != len(b):
        return False
    from collections import Counter
    return Counter(a) == Counter(b)


def _check_graph(path_a: str, path_b: str, fmt: str) -> int:
    import rdflib
    from rdflib import compare

    try:
        graph_a = rdflib.Graph().parse(path_a, format=fmt)
        graph_b = rdflib.Graph().parse(path_b, format=fmt)
    except Exception as exc:  # rdflib raises several parser-specific types
        sys.stderr.write(f"parse error: {exc}\n")
        return 2

    if compare.isomorphic(graph_a, graph_b):
        print("equivalent (RDF graphs are isomorphic)")
        return 0

    # Helpful diagnostic: report the symmetric set difference of triples
    # (best-effort; blank-node-heavy graphs may still differ by iso).
    print("NOT equivalent (RDF graphs are not isomorphic)")
    diff_a = set(graph_a) - set(graph_b)
    diff_b = set(graph_b) - set(graph_a)
    for t in list(diff_a)[:10]:
        print(f"  only in {path_a}: {t}")
    for t in list(diff_b)[:10]:
        print(f"  only in {path_b}: {t}")
    return 1


def _check_solutions(path_a: str, path_b: str, fmt: str) -> int:
    load = _load_csv if fmt == "csv" else _load_tsv
    try:
        rows_a = _canonicalise_bnodes(load(path_a))
        rows_b = _canonicalise_bnodes(load(path_b))
    except (OSError, csv.Error) as exc:
        sys.stderr.write(f"read error: {exc}\n")
        return 2

    if _compare_multisets(rows_a, rows_b):
        print("equivalent (solution multisets match)")
        return 0

    print("NOT equivalent (solution multisets differ)")
    print(f"  {path_a}: {len(rows_a)} solutions")
    print(f"  {path_b}: {len(rows_b)} solutions")
    return 1


def main(argv: List[str]) -> int:
    if len(argv) != 4 or argv[0] != "--format":
        sys.stderr.write(__doc__)
        return 2
    fmt = argv[1].lower()
    path_a, path_b = argv[2], argv[3]

    if fmt in ("turtle", "ntriples"):
        return _check_graph(path_a, path_b, fmt)
    if fmt in ("tsv", "csv"):
        return _check_solutions(path_a, path_b, fmt)
    sys.stderr.write(f"unknown format: {fmt}\n")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
