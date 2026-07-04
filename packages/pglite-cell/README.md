# @electric-sql/pglite-cell

The **cell** layer of optimistic physical replication: the client-side
runtime that attaches one PGlite instance to a database's Durable-Streams WAL
and commits into it. A **cell** is a single PGlite instance whose WAL is
captured and CAS-appended to a per-database append-only stream (the "era").
Many cells — on one host or eventually many — converge on the same database by
racing physical WAL slices onto that one stream; the commit protocol decides
each race deterministically.

> Status: **M1**. Private, unpublished, APIs unstable and expected to change.
> This package is the substrate the host (`pglite-cell-server`) and gateway
> (`pglite-gateway`) build on.

For the full design see
[`OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md`](../../OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md)
and the milestone plan in [`M1_PLAN.md`](../../M1_PLAN.md).

## The commit protocol, in five lines

One captured WAL slice, committed under the sequencer's mutex
(`Committer.commitSlice`):

1. Assert the **capture-cursor invariant** `slice.baseLsn === tailer.head.lsn`
   (else `CaptureCursorError` — the caller must rebase and re-execute).
2. Journal the commit and fsync it **before** the POST (§3.8 recovery basis).
3. CAS-append the `W` frame with the dual headers (`Stream-Seq` token +
   `Stream-Expected-Offset`) and the producer tuple `{id, epoch, seq}`.
4. On `ok`, advance the tailer locally (our own bytes, no re-download) — or
   `catchUp()` if the append deduped; on `seq-conflict` (409), return
   `{ landed: false }` (rebase; **never** re-POST these slice bytes).
5. Resolve the journal entry.

## The capture-cursor invariant

A cell's next append base **must** equal the stream head it last observed
(`baseLsn === head.lsn`). Detection-noise and boot-noise ride the _next_ slice
rather than being filtered out, so the capture cursor stays contiguous and
unbroken. A slice that no longer bases at head cannot be published — the caller
rebases to head and re-executes. This is the single rule that makes optimistic
racing safe: a stale-based slice is structurally un-appendable, not merely
rejected after the fact.

## The four CAS invariants (W1–W4)

- **W1** — every era-stream append is CAS'd (commits, syncs, leases, fences,
  checkpoints — no exceptions). The per-stream seq floor is what protects
  everyone against stale bases, and it only advances when appends carry the
  token.
- **W2** — retries are byte-identical against the journaled URL. Nothing ever
  re-targets an in-flight payload at a newer era; the server dedups replays of
  the winning POST.
- **W3** — CAS tokens are era-qualified: `pad(eraOrdinal) + "," + offset`, so a
  stale token from era N can never pass on era N+1.
- **W4** — frames are self-describing and position-checked: every frame carries
  `{eraId, expectedOffset, …}` and readers void any frame whose
  `expectedOffset` differs from the position it occupies. Commit decisions
  derive from stream bytes at immutable positions, never from producer/seq
  state.

## §3.8 recovery, summarised

`Committer.create()` runs journal recovery **before** claiming a producer
epoch. Recovery reads the stream bytes to decide the outcome of every commit
the prior incarnation left pending (fsynced journal entry, POST outcome
unknown): a matching `W` frame at the journaled position means **landed**; its
absence means **lost**. It fences the previous incarnation and raises the epoch
floor, so the new incarnation starts at
`epoch = max(persisted epoch + 1, recovery epoch floor)`, seq 0 — persisted to
`meta.json` before any append. Landed/lost is thus always decided from
durable stream bytes, never from in-memory producer state (W4).

## Compatibility posture

Runs against **unmodified** Durable-Streams servers today via the
**dual-header posture** (§2.3): every commit POST carries both `Stream-Seq`
(the enforced cooperative floor W1 rides on now) and `Stream-Expected-Offset`
(advisory — unknown headers are ignored by current servers, so it costs
nothing and becomes enforced automatically if/when the strict append-iff-tail
extension lands upstream). The strict extension is deferred to the fleet
milestone.

## Module map

Public API is re-exported from `src/index.ts`.

| Module             | Purpose                                                                      |
| ------------------ | ---------------------------------------------------------------------------- |
| `frames.ts`        | The frame codec v1: `W`/`K`/`S`/`L`/`G`/`0`/`O` encode/decode, CAS tokens.   |
| `stream-client.ts` | `DsStreamClient` — CAS append/head/create over the Durable-Streams HTTP API. |
| `journal.ts`       | The commit journal + §3.8 recovery (`RecoveryReport`).                       |
| `committer.ts`     | The solo/host sequencer: capture-cursor + one-mutex serialized CAS appends.  |
| `tail.ts`          | `EraTailer` — reads the era stream, tracks head/leases/checkpoints.          |
| `datadir.ts`       | Pack/extract a PGlite datadir to/from the object store.                      |
| `materialize.ts`   | `materializeAtHead` — replay checkpoint + tail slices into a live datadir.   |
| `cell.ts`          | `Cell` — one attached PGlite instance: open, capture slices, close-clean.    |
| `lsn.ts`           | LSN parse/format helpers (`parseLsn`, `formatLsn`).                          |
| `crc32c.ts`        | CRC32C used by the frame codec.                                              |
| `errors.ts`        | Typed errors (`CaptureCursorError`, `FencedError`, `EraClosedError`, …).     |
