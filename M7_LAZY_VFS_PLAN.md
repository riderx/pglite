# M7 — lazy VFS capstone (all decisions FIXED; execute in order)

The one unbuilt design pillar (principle 2, §6.5). Every architecture
decision below is FINAL — implement, don't re-litigate. Each wave gates on
the full corpus (core 269 / cell 80 / gateway 51 / cell-server 77+M6).

## Fixed decisions

1. **Compute moves to a worker; the host thread serves faults.** Cell
   PGlite runs in a `worker_threads` Worker. Page faults are synchronous
   inside WASM, so: control SAB (Int32Array: seq/state/request fields) +
   data SAB (4 MiB staging ring). Worker writes the request, `Atomics.wait`s;
   host observes via `Atomics.waitAsync` (Node ≥16) — NO polling; host
   fetches, writes bytes + length into the data SAB, `Atomics.notify`;
   worker copies out into WASM memory after waking. **No shared-WASM-memory
   build needed for v1** (one extra copy, zero submodule work) — the
   `pglite-shared` variant is explicitly NOT a dependency. This same split
   resolves M5e's event-loop finding for any future design-shaped commit
   gate.
2. **Exec units stay async postMessage** (bytes in, output chunks out).
   The host-side Cell API is unchanged for callers; a `WorkerCell`
   implements the same surface as `Cell` (session/proxy code is agnostic).
   Salvage reference: the SAB bridge + control block in
   `codex/durable-vfs-plan` (`pglite-durable-vfs`) — same shape, port with
   provenance.
3. **Checkpoint object format v3 = per-file content-addressed + manifest.**
   Manifest JSON object: `{ v:3, files: [{ path, size, ref, kind:
   'eager'|'lazy' }] }`. Lazy = relation main/vm/init forks (path matches
   `base/*/[0-9]+*` etc.); eager = everything else (pg_control, SLRUs,
   relmap, pg_wal checkpoint segment, conf files — the §6.1 aux set,
   small). Unchanged files dedupe across checkpoints for free
   (content-addressing) — this is the incremental-checkpoint win too.
   Gateway adds ranged reads (`Range` header on `GET /v1/objects/:ref`;
   fs store uses positional read). Fetch granularity: 256 KiB aligned
   chunks, cached chunk-wise.
4. **`LazyCellFS`** (worker-side custom PGlite filesystem, salvaging
   `LazyReplicaFS`'s shape): eager files hydrated to a local skeleton dir
   at attach; lazy-file reads resolve per-chunk: local overlay (cell's own
   writes) → host chunk cache (via SAB bridge) → gateway ranged read.
   Writes go to the local overlay always (never through the bridge).
5. **Lazy attach recipe** (this is where M0-2's mint finally goes live):
   synthesize `pg_control` + minted checkpoint record at the checkpoint's
   snapEnd (datadir.ts mint functions, existing + tested), boot the cell
   with ZERO relation bytes moved, then advance to head via the EXISTING
   M5 live-apply pipeline (walscan eager set + generic redo + identity
   advancement + `pgl_set_wal_position`) — redo's base-page reads fault
   through LazyCellFS on demand. **No crash-boot materialize, no sync
   slices** on this path: a lazily-attached write cell reaches canonical
   head via `pgl_set_wal_position`, making the M1 sync-slice/read-write-
   attach machinery a legacy fallback (keep it; do not delete).
6. **Host caches** (host thread, per CellHost): content-addressed chunk
   cache (ref+chunkIdx → bytes; disk-backed LRU under dataRoot, byte cap
   dial) shared fleet-wide; per-db page overlay is the cell's own dir
   (v1: no cross-cell (block,LSN) cache — measure first; the design's
   §14.3 cache is a follow-on optimization once byte counters exist).

## Waves

- **W1 (pglite-cell only — parallel-safe):** `worker-cell/` — worker
  entry + `WorkerCell` (Cell-compatible surface) + SAB bridge with a
  PASSTHROUGH FS (plain NodeFS in the worker; no laziness yet). Gate:
  a worker-mode flag runs the existing cell test suite's core paths
  (open/exec/capture/commit/close) green; bridge round-trip + fault
  latency microbench recorded.
- **W2 (gateway):** checkpoint v3 pack/extract/manifest + ranged reads +
  backward compat (v1/v2 archives still restore); `packDatadirV3` used by
  the checkpoint worker behind a dial.
- **W3 (cell + cell-server):** LazyCellFS + lazy attach recipe + host
  chunk cache + wiring (`cellMode: 'nodefs' | 'lazy-worker'` host dial,
  default nodefs until W4); the §16 laziness byte-count suite: cold start
  moves O(eager-set) bytes and ZERO relation pages; a point query fetches
  exactly its chunks; repeat query fetches nothing; eviction recovers;
  wake-byte counter surfaced in the console.
- **W4 (BUILT; default deferred).** The `'auto'` cell mode (resolves to
  `'lazy-worker'` when the latest checkpoint is v3-capable AND the gateway
  supports ranged reads, else `'nodefs'`), the v3 checkpoint format, worker
  `resourceLimits` defaults (maxOldGenerationSizeMb 512, stackSizeMb 8) and
  the watchdog are all built, wired, and TESTED (opt in via `cellMode:
  'auto' | 'lazy-worker'` + `checkpointFormat: 3`; the §16 lazy suite runs
  them explicitly and is green). **The DEFAULTS were reverted to the
  conservative `cellMode: 'nodefs'` + `checkpointFormat: 2`** after the
  full-corpus gate: under lazy-worker's higher per-attach latency, era
  rotation exhausts its seal re-cut budget against a continuous same-host
  writer (`rotation.test.ts` #6 — a liveness failure, never data loss; the
  rotation throws and the DB keeps serving the old era).
  **Prerequisite to flipping the default to `'auto'` (precisely
  diagnosed):** a true committer quiesce. The re-cut race is that a sibling
  commit moves the head between the rotator's O-frame cut and its seal, and
  the `ifHeadOffset` guard correctly rejects the seal (a moved head breaks
  the O/S mirror invariant `finalLsn(N) == baseLsn(N+1)`). Fix = add
  `Committer.sealExclusive(critical)` that holds the append promise-mutex
  across one `this.run()` acquisition doing {read head → `critical(head)`
  [rotation's `registerEraAttempt` + O-frame PUT cut against this head] →
  seal era N at this head}; no sibling commit can interleave, so single-host
  rotation seals first-try (cross-host still re-cuts, rare and correct).
  Rotation moves its steps 3–5 into that critical section; `rotation.test.ts`
  #3's `sealEra`-race monkeypatch moves onto the new method. Then flip both
  defaults and confirm #6 green in lazy-worker mode.
  FINDING (independent, keep): `statement_timeout` does NOT fire in the
  single-backend WASM build (no interval-timer / signal delivery — pg_sleep(5)
  runs the full 5s under a 500ms timeout), so the JS worker-terminate
  watchdog is the ACTUAL line of defense in WASM.

## Explicitly post-goal (log, don't build)

Cross-cell (block,LSN) materialized cache; shared-WASM zero-copy bridge;
capability tokens + process pools (§11.3 tiers); multiplexer mode.
