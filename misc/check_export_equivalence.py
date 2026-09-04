#!/usr/bin/env python3
# Copyright 2026, University of Freiburg,
# Chair of Algorithms and Data Structures.
# Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

"""
Export equivalence helper for QLever CONSTRUCT and SELECT outputs.

This is not a substitute for a byte-identity check. Graph isomorphism
of CONSTRUCT results is a different predicate from byte identity of an
order-preserving stream.

CONSTRUCT (turtle, ntriples)
  The result is an RDF graph, which is a set of triples. Comparison uses
  rdflib.compare.isomorphic. Duplicate triples are collapsed. A bug that
  emits the same triple twice will still pass.

SELECT (tsv, csv)
  The result is a multiset of solution mappings, keyed by variable name.
  Column order does not matter. Blank nodes shared across rows are
  compared by encoding each result as an RDF graph (one blank node per
  solution, predicates are variable names) and running isomorphism.
  An empty file is not equivalent to a header-only file.

Exit status: 0 equivalent, 1 different, 2 usage or parse error.

Usage:
  check_export_equivalence.py --format turtle FILE_A FILE_B
  check_export_equivalence.py --format ntriples FILE_A FILE_B
  check_export_equivalence.py --format tsv FILE_A FILE_B
  check_export_equivalence.py --format csv FILE_A FILE_B

Requires rdflib for every format.
"""
import csv
import re
import sys
from typing import List, Optional, Sequence, Tuple

_BNODE_LABEL = re.compile(r"^_:[A-Za-z0-9_\-]+$")
# Namespace for encoding variable names as predicate URIs when converting
# SELECT solution mappings to RDF graphs for isomorphism comparison.
_VAR_NS = "urn:qlever:var:"



def _is_bnode(term: str) -> bool:
    return _BNODE_LABEL.match(term) is not None


def _normalize_var(name: str) -> str:
    return name[1:] if name.startswith("?") else name


def _load_table(path: str, fmt: str) -> Tuple[Optional[Tuple[str, ...]],
                                              List[Tuple[str, ...]]]:
    """Return (header, rows). header is None when the file is empty."""
    if fmt == "csv":
        with open(path, encoding="utf-8", newline="") as fh:
            rows = [tuple(row) for row in csv.reader(fh)]
    else:
        with open(path, encoding="utf-8") as fh:
            rows = [tuple(line.rstrip("\n").split("\t")) for line in fh]
    if not rows:
        return None, []
    return rows[0], rows[1:]


def _cell_to_term(cell: str):
    from rdflib import BNode, Literal, URIRef

    if cell == "":
        return None
    if _is_bnode(cell):
        return BNode(cell[2:])
    if len(cell) >= 2 and cell[0] == "<" and cell[-1] == ">":
        return URIRef(cell[1:-1])
    if cell.startswith('"'):
        from rdflib import Graph

        graph = Graph()
        graph.parse(data=f"_:s <urn:p> {cell} .\n", format="nt")
        return next(graph.objects())
    if "://" in cell:
        return URIRef(cell)
    return Literal(cell)


def _solutions_to_graph(header: Sequence[str],
                        rows: Sequence[Sequence[str]]):
    """Encode SELECT solution mappings as an RDF graph for isomorphism
    comparison. Each row becomes a distinct blank node; variable names are
    used as predicate URIs so that blank-node correspondences across rows
    are detected by rdflib.compare.isomorphic."""
    from rdflib import BNode, Graph, URIRef


    graph = Graph()
    variables = [_normalize_var(name) for name in header]
    for row in rows:
        if len(row) != len(variables):
            raise ValueError(
                f"row has {len(row)} cells, header has {len(variables)}")
        row_node = BNode()
        for name, cell in zip(variables, row):
            term = _cell_to_term(cell)
            if term is None:
                continue
            graph.add((row_node, URIRef(_VAR_NS + name), term))
    return graph


def _check_graph(path_a: str, path_b: str, fmt: str) -> int:
    from rdflib import Graph
    from rdflib import compare

    try:
        graph_a = Graph().parse(path_a, format=fmt)
        graph_b = Graph().parse(path_b, format=fmt)
    except Exception as exc:
        sys.stderr.write(f"parse error: {exc}\n")
        return 2

    if compare.isomorphic(graph_a, graph_b):
        print("equivalent (RDF graphs are isomorphic; CONSTRUCT is a set, "
              "duplicate triples are collapsed)")
        return 0

    print("NOT equivalent (RDF graphs are not isomorphic)")
    diff_a = set(graph_a) - set(graph_b)
    diff_b = set(graph_b) - set(graph_a)
    for triple in list(diff_a)[:10]:
        print(f"  only in {path_a}: {triple}")
    for triple in list(diff_b)[:10]:
        print(f"  only in {path_b}: {triple}")
    return 1


def _check_solutions(path_a: str, path_b: str, fmt: str) -> int:
    from rdflib import compare

    try:
        header_a, rows_a = _load_table(path_a, fmt)
        header_b, rows_b = _load_table(path_b, fmt)
    except (OSError, csv.Error) as exc:
        sys.stderr.write(f"read error: {exc}\n")
        return 2

    if header_a is None or header_b is None:
        print("NOT equivalent (empty file is not a SELECT result)")
        return 1

    vars_a = {_normalize_var(name) for name in header_a}
    vars_b = {_normalize_var(name) for name in header_b}
    if vars_a != vars_b:
        print("NOT equivalent (projected variables differ)")
        print(f"  {path_a}: {sorted(vars_a)}")
        print(f"  {path_b}: {sorted(vars_b)}")
        return 1

    try:
        graph_a = _solutions_to_graph(header_a, rows_a)
        graph_b = _solutions_to_graph(header_b, rows_b)
    except Exception as exc:
        sys.stderr.write(f"parse error: {exc}\n")
        return 2

    if compare.isomorphic(graph_a, graph_b):
        print("equivalent (SELECT mappings are isomorphic)")
        return 0

    print("NOT equivalent (SELECT mappings differ)")
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
