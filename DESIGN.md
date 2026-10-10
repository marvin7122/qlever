# DESIGN: Anonymous VMCache subset for QLever vocabulary residency

Branch: `feat/vocab-anon-vmcache`, based on `origin/master` (default).
Gate: beat Idea-1 mix-4 tails on Ural, else PARK.

## 0. Base choice (required justification)

Based on `origin/master`, **not** on `perf/export-cpu-mmap-resident`.
The anonymous design needs none of the resident-mapping scaffold: there is
no file-backed mapping to reuse, no resident bitmap, no mincore probe. The
only shared concept with PR 283 is the CLOCK-over-unpinned-frames discipline,
which is ~20 lines to re-implement against anonymous memory. A fresh base
keeps the diff small (3 new/modified areas, no stack history), so the Ural
gate compares exactly one variable: anonymous residency on/off.

## 1. Why anonymous (context from PR 283)

PR 283 (`feat/vocab-capped-resident-mappings`) parked on parity: a
file-backed `mmap` + cap + CLOCK cannot demote on Ural because the kernel
ignores partial demotion (`MADV_DONTNEED`/`MADV_PAGEOUT`) at advise time on a
file mapping (confirmed by mincore probe: pages stay resident). Residency
state and kernel state disagree, so the "cap" is fiction.

Leis et al., SIGMOD 2023 (VMCache) working design, userspace subset:

1. **Reserve anonymous virtual memory** for residency (`MAP_PRIVATE |
   MAP_ANONYMOUS`). Anonymous pages are process-owned: `MADV_DONTNEED` on
   them is effective (physical pages freed immediately, next access
   zero-fills). No exmap/kernel work.
2. **Explicit promotion** via `pread` fills (byte copies, files immutable).
3. **Explicit demotion** via `MADV_DONTNEED` on the victim frame range.
4. **DBMS-owned replacement**: CLOCK second-chance over unpinned frames.

No file mapping exists anywhere in this design, so the PR-283 failure mode
(kernel ignoring demotion) is structurally impossible: the worst case is an
extra `pread`, never an unbounded major-fault storm.

## 2. Data structures (`src/util/AnonymousResidencyCache.h/.cpp`)

One instance per vocabulary file (words file, offsets file). Residency state
is owned by the vocabulary layer (`VocabularyOnDisk`); nothing leaks to
callers (callers keep passing indices/spans as today).

```cpp
class AnonymousResidencyCache {
 public:
  static constexpr size_t kPageSize = 4096;
  enum class FrameState { Free, Loading, Resident };

  // Move-only RAII pin guard. Pin lifetime is encoded in the type: the frame
  // stays pinned (unevictable) exactly while the guard is alive; the
  // destructor unpins. Copying is deleted, moving transfers ownership
  // (moved-from guard unpins nothing).
  class PinnedFrame { ... };

  // `numFrames` anonymous frames, `fileSize` bounds admission.
  AnonymousResidencyCache(size_t numFrames, uint64_t fileSize);
  ~AnonymousResidencyCache();  // munmap, asserts no live pins.

  // Fetch page `page`: hit -> pinned guard; miss -> CLOCK-evict one unpinned
  // frame, install as Loading (pinned), synchronously `pread` the page via
  // `fd`, mark Resident, return guard. Returns nullopt when (a) every frame
  // is pinned, (b) the wanted frame is Loading on another thread, or
  // (c) the `pread` fails (frame returned to Free). Callers fall back to
  // direct `pread` on nullopt: correctness never depends on the cache.
  std::optional<PinnedFrame> fetch(int fd, uint64_t page);

  // Convenience: serve `[offset, offset+numBytes)` into `target`, page by
  // page, via fetch(); per-page fallback to direct `pread`. Returns false
  // only when a `pread` itself fails. Empty ranges succeed trivially.
  bool readThrough(int fd, uint64_t offset, size_t numBytes, char* target);

  // Observability (atomics): hits, misses, evictions, fallbacks.
  struct Stats { ... }; Stats stats() const;
};
```

Internals: `mmap` reservation `frames_`; `std::vector<Frame>` table
`{state, pinCount, refBit, page}`; `unordered_map<page, frameIdx>`; one
`std::mutex` + `hand_` for CLOCK. Fill I/O (`pread`) runs **outside** the
lock: allocate-Loading under lock, fill, re-lock to mark-Resident. A fetcher
that finds its page Loading returns nullopt (no blocking, no condvar).

## 3. Fill path (which read sites switch)

`VocabularyOnDisk` gains two caches (`wordsCache_`, `offsetsCache_`,
`unique_ptr`, null unless enabled), sized from
`vocab-anon-vmcache-num-frames` at `open()`. Switched sites (all behind the
flag; flag OFF = today's code, one branch):

- `getOffsetAndSize`: 16-byte offsets read via `offsetsCache_->readThrough`.
- `operator[]`: word bytes via `wordsCache_->readThrough`.
- `lookupBatch` phases 1+2 (`readOffsetPairs`, `readStrings`): per-read
  `readThrough` when enabled (synchronous fill on miss; hits cost no
  syscall). The io_uring batch path is untouched and remains the fallback.
- `scanAll` (`readOffsetsInBatches`, `chunkToWords`): **bypass** (Postgres
  discipline: single-pass scan traffic must not evict the hot set; scans
  stream via direct reads as today).

`CompressedVocabulary`/`SplitVocabulary` need no changes: they bottom out in
the same `VocabularyOnDisk` reads.

## 4. Eviction path

`fetch` miss -> `clockVictimLocked()`: second chance via `refBit`, sweeps
only `pinCount == 0` frames (asserted; pinned frames are never victims, so
concurrent exports cannot evict each other's in-flight pages). Victim range
gets `MADV_DONTNEED` (effective: anonymous), old key erased from index, new
key installed as Loading. Eviction cost is O(frames) amortized per miss.

## 5. Thread-safety and pin-lifecycle rules (silent-corruption risk)

State invariants (all asserted in debug via `AD_CORRECTNESS_CHECK`):

1. **Serve-only-Resident**: bytes are copied to callers only from frames in
   `Resident` state while pinned. Loading frames are invisible to lookup
   (index entry installed at allocation, but `fetch` re-checks state under
   lock; a Loading hit returns nullopt).
2. **No evict-while-pinned**: victim selection skips `pinCount > 0`; if all
   pinned, miss returns nullopt and the caller reads directly. Pin counts
   only change under lock.
3. **Balanced pins**: only `PinnedFrame` (RAII) can pin; dtor unpins under
   lock and asserts `pinCount > 0`. No manual pin/unpin API exists, so leaks
   and double-unpins are compile-time/moved-from checked.
4. **No torn frames**: fill-then-`markResident` is a single locked
   transition `Loading -> Resident`; eviction of a Loading frame is
   impossible (it is pinned by its filler).
5. **Immutable files**: vocabulary files are never modified after build, so
   fills are pure copies; no dirty-page handling, byte identity holds by
   construction (tests `memcmp` frame vs file).
6. **Module boundary**: cache owns fd-free state (pages only); `File` fds
   stay with `VocabularyOnDisk`, which passes `fd()` per call. No
   bookkeeping leaks to query-layer callers.

## 6. Runtime gate (default OFF)

- `vocab-anon-vmcache-enabled` (Bool, default `false`): no behavior change
  unless set. When `false`, caches stay null; read paths execute today's
  code with one predictable branch.
- `vocab-anon-vmcache-num-frames` (SizeT, default `4096` = 16 MiB per file):
  applied at `open()`; 0 means enabled-with-... rejected (falls back to
  disabled when 0; documented).

## 7. Tests

- `test/AnonymousResidencyCacheTest.cpp`: fill + byte identity vs file;
  eviction drops victim and serves refilled bytes; all-pinned -> nullopt
  fallback; Loading-on-other-thread -> nullopt, no block; CLOCK second
  chance order; stats accounting; concurrent fetch/readThrough smoke
  (TSAN-clean); destructor with live pin asserts (death test only if cheap,
  else omitted).
- `VocabularyOnDiskTest.cpp`: enabled-vs-disabled byte identity for
  `operator[]`, `scanAll`, `lookupBatch` over a synthetic vocabulary larger
  than the frame budget (forces eviction); scan does not pollute stats.

## 8. Acceptance gate (Ural, byte identity)

SAME mix-4 heterogeneous benchmark that parked the cap (mem 32G, mix 2/3/4,
interleaved reps), base=Idea1 vs `vocab-anon-vmcache-enabled=true`.
GATE = beat Idea-1 mix-4 tails, else PARK and say so. Numbers posted even if
neutral. Enqueue via
`ural-wq bench ... --qlever-branch feat/vocab-anon-vmcache --purpose ci --direct-ural`.
