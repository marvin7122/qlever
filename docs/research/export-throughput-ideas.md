# Export throughput: idea backlog

The backlog of the export throughput research (log: marvin7122/qlever#324). Each idea has an id, the data it rests on, a prediction, and a status. Results go into #324; this file keeps the ideas, including the ones not started yet.

## Setup and baseline

- **Workload.** CONSTRUCT `turtle_export` of German labels on Wikidata, on Ural (Ryzen 7 3700X, 16 threads, md1 RAID of two Samsung 990 PRO). Sequential query: 4.44 M triples, 504,927,346 bytes. Scattered query: 74,413,425 bytes.
- **Rule.** 5 interleaved reps per arm; "faster" only if the min–max ranges of the arms do not overlap; output bytes must be identical.
- **Current best (E8a: E4 rank lookup + 16 GB buffer pool).** Sequential cold 14.39 s, warm 5.18 s; scattered cold 7.57 s, warm 0.91 s.
- **Warm profile of E8a** (sequential, 5.2 s, one export thread): CONSTRUCT and Turtle formatting 46.9 %, decompression 17.3 %, lookup bookkeeping 14.0 %, memory allocation 11.5 %; `BufferPool::tryCopy` 10.6 % (9.3 % in `memcpy`); `evaluateVariableByColumn` 12.7 % self; `_Sp_counted_base::_M_release` 7.3 %; the result vector of `idsToStringAndType` 40.0 % inclusive.
- **Cold profile of E8a.** Waiting in `io_uring_submit`: 44.2 % sequential, 76.2 % scattered; about 55,000 `io_uring_enter` calls in the sequential export.

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

## Running or built

| Id | Idea | Data | Prediction | Branch / status |
|---|---|---|---|---|
| E9 | Read the next CONSTRUCT batch ahead on the global executor | Cold − warm = 8.0 s of 20.8 s (sequential); reads and formatting never overlap | Cold −20 % | `research/pool-prefetch`, Ural #5615 |
| E10 | Prefetch the pool's page states and bytes *d* reads ahead | `tryCopy` 10.6 %, about 56 ns per 16-byte copy | Warm −5 % | `research/pool-copy-prefetch`, Ural #5616 |
| E11 | Batch decompression through reused buffers into one arena (the idea of #3522/#3526) | Decompression 17.3 / 25.7 %; two strings and two 8× zero-fills per word | Warm −8 / −10 % | `research/arena-decompress`, Ural #5617; port #3522 if confirmed |
| E12 | Switch off the CONSTRUCT `IdCache` | `evaluateVariableByColumn` 12.7 % self; LRU upkeep for IDs that almost never repeat | Warm −8 % | `research/no-id-cache`, building |
| E13 | Terms as views into the batch result (#200, #232, #268) | Result vector 40 % inclusive; `make_shared` + release per term | Warm −15 % | `research/terms-as-views`, building |
| E14 | Format triples straight into the stream buffer | One `std::string` per triple, then copied: libc 4.6 % + jemalloc 3.2 % + growth 2.4 % | Warm −8 % | `research/format-into-buffer`, building |
| HW | Hardware counters (IPC, L1/L3, dTLB/iTLB, walk cycles, stalls) with call sites | Ural and Wolga forbid `perf`; perfbox allows it | Decides E18, E36, E45, E46, E47, E50, E53, E56 | perfbox, running |

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
| E62 | Fixed-size copies on the pool's hot path | `memcpy` 9.3 % for 16 and about 12 bytes | Warm −2 to −4 % |
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
