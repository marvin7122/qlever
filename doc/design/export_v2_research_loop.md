# Export V2 research loop

Runs on this branch (`bench/export-v2-research-loop`), stacked on the V2
port (parts 29–35). One loop per bottleneck. Each iteration is one atomic
commit: conclusion, collapsed stacks, comparison numbers.

## The loop

1. **Design proposition.** One paragraph: what changes and where.
   Name the files.
2. **Expected improvement.** Which workload moves, by how much, why.
   A number before measuring. Anchors from fork PR #120:
   DBLP year-1990 slice (6.36 M rows) 28.96 s warm at ~25 MB/s;
   Wikidata title-large-select 5.20 s cold vs 0.48 s warm;
   R1-select 1.16 s cold vs 0.37 s warm.
3. **Baseline profile.** Profile the V1 arm on the exact A/B workload
   (see below). Record the frame share of the targeted component. A
   small share stops the iteration: the feature cannot move the total.
4. **Implement.** On this branch, one concern per commit.
5. **A/B.** Same binary, flag off vs on. Workloads: DBLP year-1990
   slice warm, title-large-select cold, R1-select cold. 1 warm plus
   3 cold repetitions per workload per arm; response bodies must
   agree (MD5, multiset for unordered rows). File the run directory.
6. **Feature profile.** Same workload, same capture flags as step 3.
   Same flags or the comparison is void.
7. **Compare and loop.** Measured delta against the step 2 number,
   frame shares against step 3. Match means done. Mismatch means a
   new step 1: the profiles name the actual bottleneck.

## Capture

- Flamegraph: `perf record -F 99 -p $SRV --call-graph fp` over the
  measured window only; binary needs frame pointers (scratch
  `-fno-omit-frame-pointer`, never merged). Dwarf second arm only
  for kernel-side detail. Compare shares, never raw counts.
- Hardware counters per arm (`perf stat` on the server PID over the
  same window): cycles, instructions (IPC both arms), cache-misses,
  branch-misses, page-faults, context-switches. A speedup with flat
  IPC moved work off the thread; a speedup with risen IPC removed
  waste. Counters decide which story is true.
- Repro record per profile: binary commit, capture mode, index path,
  kernel release, wall time, body hash, collapsed stacks, SVG.

## Comparison rules

- Matched pairs only: same workload, same flags, same index.
- Neutral (within ~2 % sequential, ~5 % scattered) needs a mechanism
  from the profiles, not a shrug: small targeted share, win moved
  workload, or cost offsets gain.
- An identical slowdown signature across unrelated changes is a
  method confound. Stop and fix the method before looping.
