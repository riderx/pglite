# M0 experiment 1: WAL round-trip proof — PASS (18/18)

The foundational claim of `OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md`,
proven on published `@electric-sql/pglite@0.5.4` (PG 17-line build) with no
Postgres changes:

> Contiguous WAL byte slices captured from one PGlite instance, laid into a
> copy of an earlier checkpoint snapshot, replay via ordinary crash recovery
> to logically identical state — and identity counters chain exactly (§5.1,
> no allocator).

```
npm install && npm test
```

## Scenarios

| | Scenario | Result |
| --- | --- | --- |
| R1 | whole-file WAL transplant (baseline) | table contents identical; nextXid + nextMulti chain exactly |
| R2 | **slice reassembly** — per-bookmark byte ranges written into snapshot segments at LSN offsets | identical; counters chain exactly |
| R3 | slice **prefix** → replay stops at a mid-workload bookmark | state == the bookmark-time dump (time travel) |

Workload includes every nasty case from the adversarial verification:
aborted transactions with writes, `ROLLBACK TO SAVEPOINT`, a temp-only
commit (forced commit record), abort-only `nextval`, serial/bigserial, a
minted multixact (subxact `FOR SHARE` + update), a 100 KB TOAST value, DDL
inside the slice, and post-DDL DML. Replicas are booted by flipping
`pg_control.state` to `DB_IN_PRODUCTION` (CRC offset discovered
empirically, not hardcoded) so ordinary crash recovery replays the tail.

## Findings for the design doc

1. **Published PGlite builds initdb with `data_checksums = on`** — and that
   is the entire explanation of the "boot WAL" (RESOLVED, see
   `inspect-boot-wal.mjs`): there is no bootstrap SQL. The ~48 KB (M0-1) /
   ~21 KB (M0-2) written on every reopen is 3–6 `XLOG/FPI_FOR_HINT`
   records — 8 KB full-page images emitted when the first catalog reads
   set hint bits, which checksums force to be WAL-logged. Zero
   transactions, zero xids consumed at boot. This is precisely the
   read-noise the design's §9 pin (`data_checksums = off`) exists to kill —
   encountered empirically on the very first experiment. Consequence: the
   pin is a **required initdb delta from stock PGlite** (PG 18 flipped
   initdb's default to checksums-on upstream), not an inherited default —
   and it is pure configuration, **verified**: with
   `initDbStartParams: ['--no-data-checksums']` the reopen boot WAL is
   exactly the 120-byte shutdown-checkpoint record — zero writes, no
   PGlite code changes. The §6.5 attach story holds exactly as written.
2. **Session teardown writes WAL after the last user transaction**
   (~1.2 KB: temp-table cleanup runs a final transaction). Slice capture
   must extend to the true durable end of WAL, not the last statement
   bookmark — the harness's R2 initially failed `nextXid` by exactly one
   until it did.
3. **Sequences follow the documented crash contract**: replayed replicas
   land on the logged-ahead value (`last_value` jumps to +32). Expected
   vanilla semantics; comparison must be `replica >= primary`, and it is
   why §5.3 sequence observability needs leases while xids need nothing.
4. **`nextOid` asymmetry is benign and explained**: R1 ends at the
   shutdown checkpoint (exact nextOid, 16432); R2 ends before it, so
   nextOid comes from the last `XLOG_NEXTOID` prefetch (24610). Uniqueness
   is index-enforced; the design already documents this (§5.2).
5. `pg_control` surgery from JS is trivial: state field at offset 16, CRC
   offset discoverable by scanning for a self-consistent CRC32C — no
   hardcoded struct layout needed beyond the state field.

## M0 experiment 2: attach, never recover — PASS (20/20)

`node attach.mjs` — the §6.5 mechanism, proven end to end on the same
published build, zero C changes:

- **All historical WAL deleted**; the cell boots from materialized pages +
  a **synthesized `pg_control`** (state `DB_SHUTDOWNED`, identity counters
  in the checkpoint copy) + a **host-minted 114-byte shutdown-checkpoint
  record** as the only WAL bytes in existence (one-struct rule: the same
  88-byte CheckPoint buffer feeds both the control copy and the record
  payload; record CRC = payload-then-header CRC32C; long page header on
  segment page 0, short header on the record's page).
- Two attach shapes both pass: **A1 continuity** (head = the datadir's
  previous EndOfLog, mid-segment) and **A2 jump-ahead** (head at `1/28` in
  a fresh, far segment — WAL-history continuity is not required, only
  record validity; the "host mints at head" model).
- Verified per shape: live `pg_control_checkpoint()` equals the minted
  record; `next_xid` installs from the synthesized control copy; 500-row
  table byte-identical (non-vacuous guard); first new WAL lands in the
  synthesized timeline; write + read work; a **plain reopen** afterwards
  persists everything — the attach leaves a fully sound cluster.

Additional findings:

6. **The same hint-bit FPIs appear after attach** (~21 KB past H+120) —
   finding 1 on the attach path. With the checksums-off pin they vanish;
   until then the first slice after any attach carries them.
7. **Harness bug worth remembering**: a multi-statement `exec` batch runs
   in ONE implicit transaction under the simple protocol — a trailing
   `rollback` rolled back the setup's CREATEs and made every comparison
   vacuously pass ({} == {}). Both experiments now carry non-vacuous
   guards. (Cost: one debugging detour via catalog forensics; the attach
   mechanism itself had been working the whole time.)

## What this de-risks

M1's commit path is now known-good end-to-end at the storage level: capture
slices (LSN-bounded byte ranges), append to a stream, lay onto snapshots,
recover. The remaining M0 items are the attach experiment (synthesized
clean-at-head control view — recipe in §6.5), the strict CAS server
extension, recycle timing, and FPI volume accounting.
