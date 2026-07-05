# @electric-sql/pglite-cell-server

The **cell host** (§14.3/§14.4) and its **session proxy** (§3.5): the server
that runs many PGlite cells for many databases against their Durable-Streams
WALs, enforces the §3.7 commit contract, and speaks the Postgres wire protocol
to real clients over TCP. It talks only to the gateway (`pglite-gateway`) and
builds on the cell runtime (`pglite-cell`).

> Status: **M1**. Private, unpublished, APIs unstable and expected to change.

For the full design see
[`OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md`](../../OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md)
and [`M1_PLAN.md`](../../M1_PLAN.md).

## The host (`CellHost` / `DatabaseRuntime`)

### Read-attach vs write-attach (and why)

A session starts **read-attached**: its cell advances to the tailer head with
**zero stream appends** and never publishes. A nonempty WAL capture on a
read-attached cell is the **write-upgrade** trigger — the speculative cell is
discarded, a fresh cell is attached at head, sequence floors are reapplied, and
only then does the slice publish. Write intent is sticky thereafter.

The point: **readers never invalidate writers.** Read cells only apply tail
metadata; they hold no serialization point and never CAS, so any number of
readers can race alongside a writer without perturbing its commit or its leases.
Only writers hold the actual serialization point (the CAS append).

### Watermark gate & sequence floors

The **watermark gate** (§7) refuses to begin a statement on a cell whose base is
behind the host watermark `W` — the session advances first — which gives
read-your-writes across connections (conn B immediately sees conn A's commit).
Read advances append nothing. **Sequence floors** cover the abort-only `nextval`
hazard: a value drawn in a rolled-back transaction is floored host-locally and
republished on the next write-attach, so a recycle never yields a duplicate
draw. (M1: in-memory floors, lost on host restart — native `nextval` clamps +
`G`-frame leases land at M4.)

### The §3.7 contract (condensed)

| Shape on CAS loss                                                  | Outcome                                                                          |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------- |
| One-shot statement / whole-txn batch, no session state             | transparent re-execute (§3.3) — client sees nothing                              |
| Session holding temp tables / `WITH HOLD` cursors / advisory locks | **fatal session reset** — `57P01` error + connection close                       |
| Interactive transaction                                            | `40001` at `COMMIT`; the session survives and rebases                            |
| DDL                                                                | serializes via the head lease; a stale-lease loser re-executes like any one-shot |
| Read-only (empty slice)                                            | never CAS'd; response flushes at txn end                                         |

### Checkpoint worker + scale-to-zero

`checkpointDatabase` materializes the head, packs the datadir to the object
store, CAS-appends a `K` frame, and registers the control-plane row; a second
run with no intervening writes is a no-op. Hibernation is governed by three
byte dials on the runtime: `hibernateAfterMs` (idle auto-hibernate),
`checkpointOnHibernateBytes` (**default 0 = always checkpoint on hibernate**, so
wake replays zero W slices), and `checkpointEveryBytes` (auto-cadence after
landed commits). A hibernated database scales to zero; the next connect wakes it
from checkpoint + tail.

### Cell mode & the lazy-worker default (M7 W4)

`opts.cellMode` selects how a cell attaches:

| mode | behavior |
| --- | --- |
| `'auto'` (**default**) | picks `'lazy-worker'` when the database's latest checkpoint is v3-capable AND the gateway supports ranged object reads, else `'nodefs'` (v1/v2 lineages) |
| `'lazy-worker'` | worker-hosted cell over `LazyCellFS`: eager skeleton hydrated at attach, relation chunks fault in on demand via the gateway's ranged reads (256 KiB, chunk-cached fleet-wide) |
| `'nodefs'` | plain in-process cell from a full local datadir copy |

`GatewayCore`'s `checkpointFormat` now defaults to **3** (per-file
content-addressed + manifest), so fresh lineages are lazy-worker-capable.
Override the mode with `PGLITE_CELL_MODE=nodefs|lazy-worker`.

### Slice spill (H3, §2.2)

A single commit whose WAL slice exceeds `opts.sliceSpillBytes` (**default
4 MiB**) uploads its bytes to the gateway object store and rides the era stream
as a spilled `W` frame (`objectRef` + `byteLength`, empty inline WAL). The
tailer / materializer resolve the bytes via the store and re-verify `sliceHash`.
This lifts the gateway append-cap limit on commit size — a multi-MiB bulk insert
commits in one slice.

### Watchdog (§11.2 basics)

`opts.statementTimeoutMs` arms two lines of defense per session:

1. **`statement_timeout`** (first line). NOTE: the single-backend WASM build has
   no interval-timer / signal delivery, so `statement_timeout` does **not**
   actually fire for CPU/sleep-bound statements — it is wired best-effort only.
2. **JS watchdog** (second, real line, lazy-worker mode). A statement that
   outlasts ~4× the timeout trips the host, which `terminate()`s the worker and
   fatally resets that session. The host stays healthy — other sessions and a
   fresh reconnect are unaffected.

Worker `resourceLimits` default to `maxOldGenerationSizeMb: 512`,
`stackSizeMb: 8` (overridable) so a runaway cannot exhaust host memory before
the watchdog fires.

## The proxy (`CellProxyServer`)

**Cell-per-connection** (§14.4): one TCP connection = one `HostSession` = one
cell. The proxy chops the frontend byte stream into protocol units (simple `Q` /
extended batch closed by `Sync`) and enforces the **buffering invariant** (§3.5):
no backend byte reaches the client until its commit CAS resolves. A one-shot
that loses re-executes behind the wire and only the winning re-execution's bytes
are flushed — zero bytes of the discarded attempt ever leak. Interactive
transactions stream mid-txn results by design; only the `COMMIT` response is
held.

**Response-size safety — the §3.5 ladder (H1).** A unit's buffered response
grows in memory up to `opts.bufferMemoryMax` (default 8 MiB), then spills to a
per-connection **spool file** up to `opts.bufferSpoolMax` (default 256 MiB);
past that the unit aborts with `40001` + a `HINT` naming the real fix. Spool
files are disposed on unit end and on connection close. (The §3.5 lease-probe
rungs 3–4 are spec'd but **deferred** — not faked; the `40001`+HINT is the v1
ceiling.)

**Declared read-only streaming (the true fix).** A transaction the client
*declares* read-only — `BEGIN READ ONLY`, `START TRANSACTION READ ONLY`, or a
plain `BEGIN` under a session `default_transaction_read_only = on` (text
classifier) — has its output **streamed to the client incrementally with no
buffering** (COPY … TO in such a txn rides the same path). At txn end the
capture MUST be empty; a nonempty capture is a loud protocol-bug error
(`XX000`), never a silent drop. Large read-only scans therefore run under
bounded host memory with no ladder and no `40001`.

**Replayed on recycle:** the connection's `StartupMessage` bytes and tracked
session-level `SET` statements (re-run on every fresh cell, output discarded).
**Not replayed (M1 limitation):** prepared statements — a conflict recycle loses
them; pooler-grade replay comes later.

## Operations (M6)

Three productization surfaces, all opt-in via `CellHostOpts.opts`:

### Janitor — background maintenance (`opts.janitor`, §6.4)

Per-active-database maintenance driven by the host; **every dial is OFF by
default**. Timers are cleared on hibernate/shutdown; the wake rebuilds them.

- `vacuumIntervalMs` — run `VACUUM (ANALYZE)` on this cadence through an
  ordinary internal session; the commit rides the normal CAS path and appears
  in the stream as an ordinary `W` frame. A vacuum is skipped when this host
  ran one within the interval (a duplicate vacuum across hosts is harmless).
- `freezeMaxAge` — when `age(datfrozenxid)` exceeds this, run `VACUUM (FREEZE)`
  (wraparound defence). Also checked once on the hibernate path.
- `gcIntervalMs` — call the gateway GC (`runGc`) for this database on an
  interval, and opportunistically once on hibernate.

### Advisory-lock policy (`opts.advisoryLocks`, §4.6)

`pg_advisory_*` locks are **cell-local**: two hosts' locks do not exclude each
other, so cross-cell mutual exclusion is not provided. Detection is a
statement-text scan for `pg_advisory_` (documented approximation).

- `'local-warn'` (default) — the first advisory-lock use per session injects a
  `WARNING` (`01000`) naming the cell-local scope ahead of the statement's
  output; the statement still runs. Fires once per session.
- `'error'` — any advisory-lock statement is rejected with `0A000` and **not
  executed**; the session survives.

### Feature policing (§9, H4)

Statement-text classifiers (simple-protocol approximation, consistent with the
unlogged-table and advisory-lock scans) reject unsupported features **loudly
with `0A000` before executing**:

| Statement | Verdict | Why |
| --------- | ------- | --- |
| `PREPARE TRANSACTION` | `0A000` "two-phase commit is not supported" | a prepared txn would strand on one cell that other cells/hosts cannot resolve |
| `CREATE DATABASE` | `0A000` | each cell stream serves exactly one database (use the gateway control plane) |
| `CREATE TABLESPACE` | `0A000` | no stable filesystem location under the stream/checkpoint layer |
| `ALTER SEQUENCE … RESTART` | `0A000` on leased sequences | RESTART rewinds below already-granted values and would reissue spent ids (§5.3) |

**`setval(...)` is ALLOWED** (migrations legitimately setval): after the
transaction lands, the host re-probes `pg_sequences`, re-grants **strictly
above** the set value, and republishes the sequence floors — keeping §5.3
rule 5 honest without breaking data-import migrations. Ids never move backwards.

### Graduation (`CellHost.graduateDatabase(db)`, §15 / OQ7)

Produces a **logical** export at a linearizable-fresh head: a `pg_dump` SQL
artifact (via `@electric-sql/pglite-tools`, `--inserts` so it restores by
`exec(sql)`) plus a `manifestSnapshot` (`databaseId`, `headLsn`, `headOffset`,
`checkpointRef`, `eraOrdinal`). OQ7 is closed: a PGlite (wasm32) datadir cannot
boot under stock 64-bit Postgres, so **logical dump/restore is THE graduation
path**. The dump is driven against a fresh throwaway PGlite opened on a
materialized-at-head scratch datadir (no serving cell is perturbed).

## Demo

```sh
pnpm demo   # scripts/demo-scale-to-zero.ts
```

Stands up the whole vertical slice — embedded `GatewayCore` + `CellHost` +
`CellProxyServer` — on a real TCP port and drives a real `pg` client through the
scale-to-zero story: create → connect → DDL + 1000 inserts → read-your-writes on
a second connection → interactive-txn conflict (`40001`) → hibernate
(checkpoint + detach) → reconnect (wake from checkpoint) → data intact, with
timings.

Measured on the demo: create ~1.4 s, wake-to-first-row ~940 ms, recycle ~100 ms.

## M1 limitations

- one era per database; **no era rotation** yet (M2);
- **single host** — cross-host leases with strict CAS deferred to M4;
- **prepared statements not replayed** across a conflict recycle;
- sequence floors are in-memory (lost on host restart) — native `nextval`
  clamps + `G`-frame leases land at M4.
