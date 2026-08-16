# Parallel SELECT export serialization — design rationale

Status: draft (design only, no implementation yet). This document is the
design rationale for a follow-up workstream that parallelizes the SELECT
export path in QLever. It mirrors the approved parallel CONSTRUCT
serialization design (PR 28, marvin-parallel-export-serialization) and
adapts it to the row and column structure of the SELECT result.

## Motivation

The export-path benchmarks (tracking issue 82 and its successors) identified
serialization as a remaining bottleneck for CONSTRUCT output. The CONSTRUCT
serialization path was therefore parallelized. The SELECT export path was
deliberately left serial at that point; the review on PR 28 records that as
design debt item 4 (SELECT stays serial, keep it that way). SELECT also had
a separate optimization in flight (PR 46, batch SELECT CSV/TSV vocabulary
resolution), so a parallel-SELECT workstream was deferred until the CONSTRUCT
parallel layer and the SELECT batching both settled. This document now
captures the design for that workstream.

## Current SELECT export path

The SELECT result stream is produced by
ExportQueryExecutionTrees::selectQueryResultToStream, specialized per media
type:

- convertToCsvOrTsv<format> for CSV and TSV.
- convertToSparqlJson<format> for the SPARQL JSON binding format.

The common structure is:

1. qet.getResult(true) materializes (or fetches from cache) the query result
   as an IdTable.
2. selectedColumnIndices maps selected variables to their columns, in the
   output column order.
3. getRowIndices(limitAndOffset, *result, resultSize) yields slices of the
   result as (idTable slice, localVocab) pairs.
4. For each row in each slice, for each selected column, the cell is resolved
   from its Id to a string via
   ql::exportIds::idToStringAndType(index, id, localVocab, escapeFunction),
   and the escaped cell, the column separator, and the row newline are
   appended to the stream.
5. CSV and TSV additionally emit a header line before the first row.

PR 46 changed the CSV/TSV cell resolution from one idToStringAndType call per
cell to batched idsToStringAndType calls over fixed-size row groups
(kIdBatchSize), so vocabulary IDs share one lookupBatch instead of one system
call per cell. The serialized bytes are identical to the per-cell stream.

## Why the SELECT path can reuse the CONSTRUCT parallelism model

The parallel CONSTRUCT layer (computeExportGroups, splitBlocksIntoGroups,
serializeConstructGroup) splits the materialized result rows into contiguous
groups and serializes each group on a worker into its own output buffer, then
concatenates the group buffers in order. The byte-identical guarantee is
preserved because the group ranges carry the original global row indices. The
SELECT export already materializes its full result as an IdTable with
contiguous rows, so the same row-group split applies directly.

Two structural differences from CONSTRUCT must be handled:

1. CSV and TSV emit a header line once, before the first data row. The header
   is a single short line; it is serialized by the serial coordinator before
   the workers start, so it is not part of any group.
2. The SELECT result has a fixed column structure per row (the selected
   variables in a fixed order). Each group worker resolves the cells of its
   rows and formats them row by row, which is the same work the serial path
   does.

## Parallelism model

Mirror the CONSTRUCT layer:

- After the result is materialized, split the IdTable rows into contiguous
  groups. Reuse the group computation and block-splitting logic, adapted to
  rows only (no triples-per-row factor; the per-group output length is
  proportional to the row count times the average serialized row size).
- Each group is serialized by a worker into its own output std::string using
  the existing batched vocabulary resolution (idsToStringAndType) and the
  existing escaping functions. The worker reads only its own slice of the
  result IdTable and its own localVocab.
- The coordinator concatenates the group buffers in row order after all
  workers finish, yielding a byte-identical stream. The group ordering is the
  only thing the client observes, so the output is byte-identical to the
  serial path.
- The number of in-flight groups is bounded by the same budget rule as the
  CONSTRUCT layer: at most numThreads group buffers in flight, with a
  per-request buffer-memory budget, so memory stays bounded by the runtime
  parameters construct-export-num-threads and construct-export-buffer-memory
  (renamed or aliased for the SELECT path, e.g. select-export-num-threads and
  select-export-buffer-memory).

## Ordering contract

Byte-identical output. Because the group buffers are concatenated in row
order and each group preserves the global row indices, the exported stream is
identical to the serial path for the same result. This keeps differential
testing (the W3C equivalence oracle, PR 50) directly applicable.

## Thread safety of vocabulary reads

The CONSTRUCT parallel path already invokes idToStringAndType on multiple
workers against the same shared index and vocabulary, which establishes that
the shared vocabulary reads are safe to run concurrently. The SELECT parallel
path consumes the same vocabulary reads through the same batched resolution
functions, so no additional synchronization is introduced. localVocab
instances are per slice and per worker, so they are not shared across
workers.

## Interaction with the SELECT batching (PR 46)

The batched idsToStringAndType path from PR 46 is the natural cell resolution
primitive for each group worker: a worker resolves its rows in fixed-size
batches and formats them into its output buffer. The batching and the
parallel grouping are orthogonal. The parallel layer groups rows first; each
group is then batched internally exactly as PR 46 does on the serial path.
The serialized bytes are unchanged.

## Resource bounds

- Thread count is bounded by select-export-num-threads.
- The total in-flight serialized output is bounded by
  select-export-buffer-memory times the number of in-flight groups, kept
  within the per-request budget.
- Workers are created only after the full result is materialized, so the
  parallelism does not change the memory cost of the query result itself.

## Implementation plan

1. Adapt the CONSTRUCT group computation and block splitting to the SELECT
   row structure.
2. Add a per-format group serializer for CSV, TSV, and SPARQL JSON that uses
   the batched idsToStringAndType primitive.
3. Add the select-export-num-threads and select-export-buffer-memory runtime
   parameters.
4. Add unit tests covering the group split, the byte-identical guarantee, and
   the header handling, mirroring the CONSTRUCT tests in
   test/ExportQueryExecutionTreesTest.cpp.

## Open decisions

1. Whether to reuse the CONSTRUCT runtime parameter names with aliases or
   introduce dedicated SELECT parameter names.
2. Whether the SPARQL JSON serializer should stay streaming (it cannot
   trivially emit row groups as independent byte chunks without careful comma
   handling), or whether the JSON object should be assembled per group and
   joined by the coordinator.

## Out of scope

- Changes to the query engine or planner. The parallel layer touches only the
  export serialization.
- N-Triples (SELECT has no N-Triples output).
- Changes to the vocabulary or its batched read layer.
- io_uring or other I/O path changes.
