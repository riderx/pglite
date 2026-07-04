# M2 implementation plan — storage lifecycle

Companion to `OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md` §2.4/§2.5/§6.1/§6.4
and `M1_PLAN.md` (whose formats and findings all still govern). M2 scope
(§15): hardened era rotation, forks over stream forks + `F` frames, GC
horizon AND execution, the checkpoint-cadence dial. Everything is JS-only —
no native changes.

## Rotation state machine → code (normative mapping of §6.1 steps 0–6)

Rotator lives in `pglite-cell-server` (`src/rotation.ts`), invoked by the
checkpoint worker when the era's byte length exceeds the per-database
`rotate_every_bytes` dial, or explicitly.

```text
0. REPAIR-WALK   control plane: while current era row is sealed with a
                 recorded next era → guarded UPDATE databases SET
                 current_era_ordinal = next WHERE current_era_ordinal = N
                 (0 rows ⇒ someone else repaired ⇒ re-read, continue)
1. QUIESCE       implicit: all appends serialize through the runtime
                 committer; rotation appends ride the same mutex
2. CHECKPOINT    existing checkpointDatabase (canonical ensure → object →
                 K frame → row); rotation reuses its canonical state
3. PUT era N+1   at a UNIQUE per-attempt URL /era/<pad6(N+1)>-<ulid>;
                 REGISTER THE ATTEMPT in control plane (era_attempts row)
                 BEFORE the PUT (GC needs a registry — the DS server has
                 no stream listing); body = O frame { eraId, ordinal N+1,
                 prevEraId, prevEraUrl, baseOffset: INITIAL, baseLsn:
                 <current head lsn>, snapEnd: <same>, checkpointRef }
4. K frame       part of step 2
5. SEAL era N    appendAndClose (CAS path — never close-only) with body =
                 S frame { eraId: N's, finalOffset: <expectedOffset of
                 this append>, finalLsn: <head lsn>, nextEraUrl,
                 nextEraId }.
                 409-seq (commit raced in) → catch up → RE-CUT: goto 3
                 with a fresh unique URL and updated baseLsn (the raced
                 commit is a legitimate era-N commit; the stale N+1
                 orphans; tail-copy is deliberately NOT implemented — the
                 design allows either, re-cut is always correct and
                 simpler).
                 409-closed → another rotator won: read era-N tail S,
                 ADOPT their next era; our N+1 orphans.
6. MANIFEST      one control-plane transaction: mark era N sealed
                 (+ final_offset/final_lsn/next), insert era N+1 row,
                 guarded UPDATE current_era_ordinal N→N+1; promote the
                 era_attempts row. Re-entrant via step 0.
```

Sealed-detection rule (§2.6): **presence of a valid terminal S frame means
sealed, regardless of the stream's closed bit**; a closed era without an S
is wedged — writers stop, repair runs.

## Era hop (tailer + committer, `pglite-cell`)

- **Tailer**: on S frame — verify `S.eraId == currentEra`; fetch next era's
  O frame at the initial token; verify the O/S mirror
  (`O.prevEraId == S.eraId && O.eraId == S.nextEraId && O.baseLsn ==
  S.finalLsn`); switch the reader to the new stream at the boundary after
  O; contiguity carries through `O.baseLsn`. A read past a closed era
  without S ⇒ typed WedgedEraError.
- **Committer**: a `closed` append result is no longer terminal — catch up
  (which hops), then: if the slice's baseLsn still equals the new head
  LSN, RE-CAS the same WAL bytes into the new era (re-framed: new eraId,
  new expectedOffset, new journal entry, SAME commitId; the old journal
  entry resolves as lost-to-rotation); else `landed:false` (ordinary
  re-execute path). CAS tokens are already era-qualified (W3) so the new
  era's floor arms correctly.
- **casToken ordinal** comes from the tailer's CURRENT era, not
  construction-time config.

## Forks (§2.5, M2 subset)

`gateway.forkDatabase(parentId, name, at?)` (default: parent's latest
checkpoint position — M2 restricts fork points to checkpointed positions;
arbitrary-LSN forks arrive with M3's page-version machinery):

1. Control-plane transaction: child `databases` row + `lineage(child,
   parent, fork_lsn, fork_offset)` + child era row + child checkpoint row
   pointing at the PARENT's checkpoint object (content-addressed — shared,
   refcounted via lineage joins).
2. Stream-fork PUT: child era stream = fork of the parent era at
   `fork_offset` (`Stream-Forked-From`/`Stream-Fork-Offset`). The copied
   prefix carries PARENT eraIds — readers must tolerate an eraId change
   across the prefix boundary as long as position (W4) and LSN chaining
   hold. Child era row keeps the parent's ordinal (offsets continue past
   the fork point, so W3 tokens stay monotone); new era_id names the
   child's writes.
3. CAS-append an `F` frame to the PARENT era `{ childDatabaseId,
   forkOffset, forkLsn }` — in-band announcement + GC pin signal.

Producer/seq state is not inherited (verified M0 research) — child
committers bootstrap fresh.

## GC (§6.4, execution)

`gateway` GcExecutor (explicit `runGc()` + optional interval):

- **Orphan era attempts**: era_attempts older than `gc_grace_ms` never
  promoted → DELETE stream, drop row.
- **Sealed eras**: deletable when a checkpoint exists with `snap_end >=
  era.final_lsn` on the SAME database, AND no live child fork references
  the era range (lineage join), AND no active pin covers it.
- **Checkpoints**: keep the latest per database, plus any referenced by a
  child's checkpoint row or covered by an active pin; prune the rest
  (rows first, then objects unreferenced by ANY checkpoint row —
  content-addressing means an object may serve many databases).
- **Pins**: `pins` control-plane table maintained by the host (gc-pin
  create/release + TTL expiry sweep) mirroring the stream's `L{gc-pin}`
  frames — the stream stays the in-band truth; the table is the queryable
  index.
- Grace windows on every destructive step.

## Dials (control plane `databases` columns, surfaced in the manifest)

- `checkpoint_every_bytes` (M1e auto-cadence, now per-db; 0 = disabled)
- `rotate_every_bytes` (era length trigger; 0 = never — M1 behavior)
- `gc_grace_ms`

## Checkpoint slimming (the 99 MB fix)

`packDatadir` v2: gzip the tar (`node:zlib`), and exclude every `pg_wal`
segment EXCEPT the one containing the checkpoint record (attach reads
exactly two pages of it; older segments are never read — M0-2 proved
attach with ALL other WAL deleted; `writeWalRange` recreates future
segments as zero-filled on demand). Extraction transparently handles both
v1 (plain tar) and v2 (gzip) by magic bytes.

## Wave split

- **M2a** (`pglite-cell`): tailer era-hop + O/S mirror verification +
  fork-prefix eraId tolerance + WedgedEraError; committer closed→hop→
  re-CAS; token ordinal from current era; tests with hand-built multi-era
  streams (including a hand-misplaced frame proving W4 voiding, and a
  closed-without-S wedge).
- **M2b** (`pglite-gateway`): control-plane v1 (current_era_ordinal,
  era_attempts, lineage, pins, dials), era registration/seal APIs,
  forkDatabase, GcExecutor, checkpoint slimming, HTTP routes; tests.
- **M2c** (`pglite-cell-server`): rotator (steps 0–6, re-cut,
  adopt-on-409, repair-walk), dial-driven triggers, fork wake path,
  rotation chaos suite (two concurrent rotators; commit racing the seal;
  crash between seal and manifest; 3-era joiner hop; 409-closed
  mid-commit re-CAS; fork E2E with pinned-parent GC; orphan sweep).

## Exit criteria

A database that has rotated through ≥3 eras, forked a child at a
checkpoint, had its parent eras GC'd down to the pin horizon, and served
writers through a mid-commit rotation — with every M1 suite still green
and joiners attaching via checkpoint + ≤1 era tail.
