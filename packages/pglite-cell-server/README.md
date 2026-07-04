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

## The proxy (`CellProxyServer`)

**Cell-per-connection** (§14.4): one TCP connection = one `HostSession` = one
cell. The proxy chops the frontend byte stream into protocol units (simple `Q` /
extended batch closed by `Sync`) and enforces the **buffering invariant** (§3.5):
no backend byte reaches the client until its commit CAS resolves. A one-shot
that loses re-executes behind the wire and only the winning re-execution's bytes
are flushed — zero bytes of the discarded attempt ever leak. Interactive
transactions stream mid-txn results by design; only the `COMMIT` response is
held.

**Replayed on recycle:** the connection's `StartupMessage` bytes and tracked
session-level `SET` statements (re-run on every fresh cell, output discarded).
**Not replayed (M1 limitation):** prepared statements — a conflict recycle loses
them; pooler-grade replay comes later.

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
- unlogged tables **not policed** yet;
- sequence floors are in-memory (lost on host restart) — native `nextval`
  clamps + `G`-frame leases land at M4.
