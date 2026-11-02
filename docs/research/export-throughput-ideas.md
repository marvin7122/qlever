# Export throughput: idea backlog

The backlog of the export throughput research (log: marvin7122/qlever#324). Each idea has an id, the data it rests on, a prediction, and a status. Results go into #324; this file keeps the ideas, including the ones not started yet.

## Setup and baseline

- **Workload.** CONSTRUCT `turtle_export` of German labels on Wikidata, on Ural (Ryzen 7 3700X, 16 threads, md1 RAID of two Samsung 990 PRO). Sequential query: 4.44 M triples, 504,927,346 bytes. Scattered query: 74,413,425 bytes.
- **Rule.** 5 interleaved reps per arm; "faster" only if the min–max ranges of the arms do not overlap; output bytes must be identical.
- **Current best (E8a: E4 rank lookup + 16 GB buffer pool).** Sequential cold 14.39 s, warm 5.18 s; scattered cold 7.57 s, warm 0.91 s.
- **Warm profile of E8a** (sequential, 5.2 s, one export thread): CONSTRUCT and Turtle formatting 46.9 %, decompression 17.3 %, lookup bookkeeping 14.0 %, memory allocation 11.5 %; `BufferPool::tryCopy` 10.6 % (9.3 % in `memcpy`); `evaluateVariableByColumn` 12.7 % self; `_Sp_counted_base::_M_release` 7.3 %; the result vector of `idsToStringAndType` 40.0 % inclusive.
- **Cold profile of E8a.** Waiting in `io_uring_submit`: 44.2 % sequential, 76.2 % scattered; about 55,000 `io_uring_enter` calls in the sequential export.

## Hardware counters (perfbox, 2026-11-01)

i7-6700, binary 6c11313b (E4 + pool), 3 reps, no multiplexing, output bytes as expected. Raw data: perfbox `/root/hwcounters-2026-11-01/`.

| | Seq. off warm | Seq. pool warm | Seq. pool cold | Scat. off warm | Scat. pool warm | Scat. pool cold |
|---|---|---|---|---|---|---|
| Query time | 18.91 s | 5.83 s | 15.79 s | 3.09 s | 1.04 s | 6.50 s |
| IPC | 0.97 | 1.82 | 1.38 | 1.09 | 1.74 | 1.32 |
| LLC load misses per word | 10.2 | 9.4 | 26.0 | – | – | – |
| dTLB load misses per word | 5.39 | 4.44 | 6.38 | – | – | – |
| Cycles in dTLB page walks | 1.0 % | 7.0 % | 5.1 % | 1.0 % | 4.9 % | 1.7 % |
| Cycles stalled on L3 misses | 8.3 % | 12.5 % | 13.2 % | 8.2 % | 9.8 % | 6.6 % |
| Branch misses per word | 28.6 | 23.1 | 40.2 | – | – | – |

- **Miss sites, sequential warm with the pool:** small `memcpy` from `tryCopy` 54 % of dTLB misses; the page-state load in `tryCopy` 29 % of L3 misses; `positionOfIndex` 14 % dTLB / 15 % L3; `fsst_decompress` 12 % dTLB / 18.5 % L3.
- **Huge pages:** THP mode `madvise` (also on Ural); `AnonHugePages` 0 kB at an RSS of 27.9 GB.
- **Top cycles:** `evaluateVariableByColumn` 11.2 %, `fsst_decompress` 6.4 %, jemalloc 5.3 %, `instantiateBatch` 4.7 %, `_M_release` 4.5 %, `__introsort_loop` 3.2 %.
- **Consequences:** E10 targets the largest L3-miss site; E47 promoted (no code); E18 only for the offsets pool (2 MB pages in the scattered words pool would load 2 MB per 4 KiB miss and break the budget accounting); E45 supported; E62 dropped (the cost is the memory access, not the call); E35 confirmed at about 3 %.

## Done

| Id | Change | Result | Status |
|---|---|---|---|
| E1 | No readahead on the vocabulary files | Scattered cold −9.8 %, rest within noise | partly confirmed |
| E2 | Sorted, deduplicated batches | Within noise | falsified |
| E3 | Galloping search over in-memory indices | Sequential cold +3.3 % | falsified |
| E4 | Rank lookup for in-memory words (#264) | −7 to −26 % | confirmed |
| E5 | Offsets through `mmap` (#229) | −17 to −27 %, scattered cold neutral | confirmed, not kept (evicted pages block on page faults) |
| E6b | E1 + E4 | −14.6 to −27.0 % | confirmed, baseline |
| E8a | Buffer pool 16 GB (#297) on E4 | Cold −20 to −31 %, warm −59 to −61 % | confirmed |
| E9 | Read the next batch ahead (*d* = 1) on the pool | Sequential cold −9.6 %, warm −2 to −3 %, scattered cold noise | prediction (−20 %) falsified; see E9b |
| E9b | Read-ahead depth *d* = 4 / 16 vs 1 | Seq. cold 13.03 → 6.91 / 6.81 s (−47 %), scat. cold 7.43 → 2.37 / 1.71 s (−68 / −77 %); warm +7 to +10 % (slower) | cold confirmed far beyond prediction; warm cost from duplicated lookups (→ E79, E81) |
| E10 | Pool copy prefetch 8 / 32 vs 0 | All within noise | falsified |
| E11 | Batch decompression into one arena | Warm −11.7 % seq., −9.2 % scat.; cold noise | confirmed; port #3522 |
| E12 | `IdCache` off | Warm −12.7 % seq., −11.2 % scat.; seq. cold −6.1 % | confirmed beyond prediction |
| E13 | Terms as views (#200) | Warm −4.2 % seq., −7.0 % scat.; cold noise | prediction (−15 %) falsified; smaller gain |
| E14 | Triples in ≥ 64 KiB chunks | Warm −4.9 % seq., −6.7 % scat.; cold noise | confirmed, smaller than predicted (−8 %) |
| E47 | Huge pages for the rank directory | All within noise | falsified |

## Running or built

| Id | Idea | Data | Prediction | Branch / status |
|---|---|---|---|---|

## Backlog

### I/O and the cold path

| Id | Idea | Data / reasoning | Prediction |
|---|---|---|---|
| E16 | Sequential readahead inside the pool (after a miss on page *p*−1, load *p…p*+15) | 44 % of cold sequential waits in `io_uring_submit` | Cold sequential −15 % |
| E17 | Read the offsets several batches ahead | Each batch waits for offsets, then for words | Cold −10 % on top of E9 |
| E19 | Export batch size sweep 1024 / 4096 / 16384 (#313/#315, never measured) | Fixed costs per batch; more misses per round trip | Cold −10 % at 4096 |
| E20 | io_uring sweep: ring size, reap wave, peek batch (#312/#314, never measured) | About 1000 misses per batch through a 256-entry ring | Scattered cold −15 % at ring 1024 |
| E25 | Registered file descriptors for the pool's reads (#252/#223) | About 55,000 submits per cold export | Cold −3 % |
| E33 | Offsets in RAM with Elias–Fano (12.6 GB → about 1.2 GB) | Removes step 1 and its I/O wait; verify first with the pools' `numPageLoads` split | Cold −20 %, warm −5 % |
| E34 | Chain each word read to its offset read's completion | A batch waits for its slowest offset read | Cold −15 % |
| E46 | 16 KiB pool pages for the words file | About 55,000 submits; 16 KiB costs about as much as 4 KiB on NVMe | Cold −10 % |
| E49 | Ring flags `SINGLE_ISSUER`, `DEFER_TASKRUN`, `COOP_TASKRUN` | One ring per manager and thread; kernel 7.0 | Cold −5 % |
| E50 | Pre-fault pool memory off the critical path | Every new pool page faults and is zero-filled before `O_DIRECT` overwrites it | Cold −5 % |
| E51 | Spin briefly on the completion queue before blocking | About 55,000 sleeps and wakeups per cold export | Cold −3 % |
| E58 | Time distribution per batch (`rdtsc` histogram) | All numbers so far are totals | Measurement; decides between E16, E17, E34 |
| E64 | Short words inline in a 16-byte offsets record | 12.6 bytes per compressed word on average | Cold −15 %, warm −5 %; new index layout |
| E65 | Warm restarts: save and reload the pool's resident page list | 7.24 GB/s sequential vs 5–15 % of IOPS for 4 KiB misses | First query after restart up to −60 % |
| — | `IOPOLL` | Ural's NVMe drives have no poll queues (`poll_queues=0`); needs root | ruled out |

### Pool and memory access

| Id | Idea | Data / reasoning | Prediction |
|---|---|---|---|
| E18 | 2 MB pages for the offsets pool | Only if E10 leaves `tryCopy` slow (TLB misses) | Warm −4 % |
| E36 | Views into pool memory with pinned pages instead of copies | `tryCopy` 10.6 % | Warm −5 % |
| E37 | E36 + E11: decompress straight from pool memory | Two copies per word become one | Warm −3 % on top |
| E47 | Huge pages for the rank directory (parameter exists, never measured) | In-memory words 1.1–2.9 % | Warm −1 % |
| E62 | Fixed-size copies on the pool's hot path | Dropped: the counters put the cost in the memory access (dTLB/L3 misses), not in the call | — |
| E63 | Validate once per page instead of per read | Two state loads per read; sorted reads share pages | Warm −2 % |
| E67 | Avoid TLB shootdowns under memory pressure (batched evictions) | #292: 6,446 `memory.high` events at +4 GiB | Under a limit −5 % |

### Decompression and vocabulary layout

| Id | Idea | Data / reasoning | Prediction |
|---|---|---|---|
| E21 | One check per batch instead of per word (#291) | Lookup bookkeeping 14 % | Warm −3 % |
| E45 | Decompress in decoder order within a batch | 1,575 decoder pairs, about 7 MB of symbol tables | Scattered warm −5 % |
| E60 | Output-ready (escaped, quoted) words in the vocabulary, built at index time | Every export re-parses and re-escapes every literal | Warm −15 % |
| E66 | One-level codec chosen for decode speed (microbenchmark on real words first) | Two FSST passes per word; decompression 17–26 % | Warm −8 % |

### Evaluation, formatting and allocation

| Id | Idea | Data / reasoning | Prediction |
|---|---|---|---|
| E22 | Format the template's constants once (#227) | Constant predicate formatted 4.44 M times | Warm −3 % |
| E23 | SIMD "needs escaping?" check (#285, #203) | Most labels need no escaping | Warm −3 to −5 % |
| E30 | Reuse the lookup's scratch vectors across batches (#284) | jemalloc on batch vectors 2.5 % | Warm −2 % |
| E35 | Skip the per-column sort when IDs are already sorted | Sort of 1024 pairs per column and batch inside the 12.7 % | Warm −3 % |
| E40 | Encoded IRIs: prefix once, then integer-to-digits | Subject IRIs rebuilt per row | Warm −3 % |
| E41 | Fast path for plain language-tagged literals | Parse and new string per literal | Warm −4 % |
| E42 | Terms without atomic reference counts | `_M_release` 7.3 % | Warm −5 % |
| E52 | Batch-sized ranges instead of type erasure per triple | 2 virtual calls × 4.44 M triples | Warm −3 % |
| E54 | Copy accounting: bytes copied per output byte | About 6 copies per label from pool to socket | Measurement; target ≤ 2 |
| E56 | Software prefetch along term pointer chains | Up to 3 dependent misses per term | Warm −3 % |
| E57 | One bump allocator per batch for all temporaries | Allocation 11.5 % | Warm −5 % |
| E61 | Compile the CONSTRUCT template into a specialized writer | Template interpreted per triple | Warm −5 % |

### Parallelism and output

| Id | Idea | Data / reasoning | Prediction |
|---|---|---|---|
| E15 | Evaluate batches on several threads (Export V2 stack #243–#249, #273) | 88 % of samples on one thread; 16 hardware threads | 4 threads: warm under 2 s |
| E24 | 8 MiB stream buffer (#195), then `SEND_ZC` (#209) | HTTP send and streaming 3.9 % | Warm −2 % |
| E27 | Three-stage pipeline: lookup → instantiate → format (#196, #224) | One CPU-bound thread | Warm 5.2 s → about 2.5 s |
| E28 | Parallel gzip of the response (#266) | Benchmark uses `identity`; real downloads use gzip | With gzip: compression no longer the limit |
| E29 | Adaptive batch size (#253/#225) | Follows E19 | Best of E19 in warm and cold |
| E31 | Extend the research to SELECT TSV/CSV (#159, #251, #228) | Only CONSTRUCT measured so far | Profile first |
| E32 | Throughput under 4 concurrent exports (#292) | Contention on page states, CLOCK, budget is untested | ≥ 3× one export |
| E39 | Prefixed Turtle output (opt-in) | 114 bytes per triple, about 45 with prefixes | Warm −15 %; changes output |
| E48 | Send on its own thread | Send and streaming 3.9 % on the export thread | Warm −3 % |
| E59 | Network as the next limit; offer `zstd` responses | 504.9 MB in 5.18 s = 97.5 MB/s = 0.78 Gbit/s | Remote clients bounded by the link |

### Build, system and measurement

| Id | Idea | Data / reasoning | Prediction |
|---|---|---|---|
| E26 | Measure the repeat rate of IDs (#290 probe) | E12 assumes almost no repeats | Decides E12's default |
| E38 | Pin the export thread and its memory to one CCX | 3700X: 2 CCX with separate L3 | Warm −2 to −5 % |
| E43 | PGO + LTO + `-march=native` build | 100 % CPU on branchy code | Warm −10 % |
| E44 | jemalloc tuning via `MALLOC_CONF` | Allocation 11.5 % | Warm −2 to −4 % |
| E53 | BOLT on top of PGO | Front-end stalls (to be confirmed by iTLB/L1i counters) | Warm −5 % |
| E55 | Check that the benchmark client is not the limit | Client hashes 505 MB in Python | Measurement |
| E68 | Measurement hygiene: record CPU frequency, pin both arms to one core, report noise | Gains of 2–5 % now in play | Measurement |

### Operating environment: concurrent, unknown queries

Constraint: no preparation at server startup; the server sees many concurrent queries of unknown shape.

| Id | Idea | Data / reasoning | Prediction |
|---|---|---|---|
| E69 | Replay a mixed workload (interactive queries + large exports, concurrent clients); throughput and p50/p99 per query class | All numbers so far: one export on an idle machine | Measurement; baseline for E70–E78 |
| E70 | Scan-resistant pool admission (probation segment, 2Q/LRU-K style) | A one-pass export can push the hot pages of interactive queries out of a CLOCK pool | Interactive latency after a large export unchanged |
| E71 | Split the pool budget by hit rate per byte, measured online, instead of by file size | E9 server log: 9.76 GB words + 6.24 GB offsets (proportional to file size); one offsets page serves 512 lookups | Same budget, cold-ish mixed workload −10 % |
| E72 | I/O priority: interactive reads before export reads (`ioprio` per request or a separate ring) | An export keeps up to 256 reads in flight | Interactive p99 under a concurrent export −50 % |
| E73 | Parallelism from current load: exports use idle cores only | Applies to E15/E27 | Alone full speedup; under load interactive p99 unchanged |
| E74 | Memory arbitration between the pool and query execution (`memoryForQueries_`) | The elastic budget (#295) reacts to free system memory, not to QLever's query allocator | No query fails for memory while the pool is above its floor |
| E75 | Shared scans for identical concurrent exports | Large dumps are requested repeatedly; needs query-log data | N identical exports cost about one |
| E76 | Process-wide cache of formatted terms with TinyLFU admission | The `IdCache` is per export; cross-query recurrence unknown (needs a query log) | Worth it if the top 1 % of IDs cover a large share of lookups |
| E77 | Prompt cancellation of prefetches, reads and pool loads on disconnect | E9's prefetcher and pool loads run to completion | Wasted work after a disconnect under 50 ms |
| E78 | Bounded production per slow connection (byte budget, then pause) | Slow clients hold batch results and pool memory | Constant memory per connection; no loss for fast clients |

### Follow-ups to E9 and more operating-environment ideas

E9's read-ahead costs 26–36 % more CPU per query (sequential warm 5.82 → 7.92 s, scattered warm 1.40 → 1.79 s, sequential cold 14.34 → 18.06 s) for a 2–10 % wall-time gain, because the prefetched lookups decompress and copy everything and are then discarded.

| Id | Idea | Data / reasoning | Prediction |
|---|---|---|---|
| E79 | Use the prefetched lookup result instead of discarding it | E9 looks up every batch twice; the lookup is 32 % inclusive | Warm 5.2 → about 3.6 s without extra CPU |
| E80 | Read ahead by loading pages only (no decompression, no copies) | Fallback if E79 is too invasive | E9's wall time with about 30 % less CPU |
| E81 | Adaptive read-ahead depth (grow while batches wait on I/O, 0 on hits) | Warm: read-ahead only costs CPU; cold: *d* = 1 hides too little | Cold as the best *d* of E9b, warm without overhead |
| E82 | Split-phase lookups on the export thread instead of executor tasks | E9 uses the global executor, shared with other queries; the ring supports many reads in flight | E9b's cold gain at zero extra threads |
| E83 | Read ahead across block boundaries | E9 never prefetches the first batch of a block; count blocks per export | Depends on block count |
| E84 | CLOCK reference bits in a separate array | `fetch_or` on state words that all readers read: cache-line bouncing under concurrency | 4 concurrent warm exports +20 % throughput |
| E85 | One pool budget for the whole server | #297: a fixed budget applies per vocabulary instance | Memory bounded as configured |
| E86 | Admission control for exports when working sets exceed the pool | #292: a 4 GiB pool lost to `mmap` under six concurrent working sets | Higher total throughput under overload |
| E87 | Flush the first bytes early (64 KB or 10 ms) | Time to first byte 0.35 s cold, 0.04 s warm (E8a profile) | Lower time to first byte, same throughput |
| E88 | Pool counters per query and in the metrics | E69–E71 and E86 need per-query pool usage | Measurement infrastructure |
