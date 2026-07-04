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

1. **PGlite reopen writes ~48 KB of WAL before the first user transaction**
   (bootstrap SQL). Slices are contiguous from the prior EndOfLog so the
   bytes simply ride in slice zero — but this must be identified and
   ideally eliminated before M1: it is per-attach write amplification, and
   the §6.5 attach story assumes boot writes nothing. (Confirms the
   control-view report's boot-time-WAL hazard, at much larger scale than
   the `PARAMETER_CHANGE` case it predicted.)
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

## What this de-risks

M1's commit path is now known-good end-to-end at the storage level: capture
slices (LSN-bounded byte ranges), append to a stream, lay onto snapshots,
recover. The remaining M0 items are the attach experiment (synthesized
clean-at-head control view — recipe in §6.5), the strict CAS server
extension, recycle timing, and FPI volume accounting.
