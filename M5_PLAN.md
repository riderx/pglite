# M5 implementation plan — the native wave (interactive rebase + live apply)

## STATUS: M5 COMPLETE (2026-07-05)

All five stages shipped: M5a (sequence-lease clamp + toolchain), M5b (WAL
introspection + read-set ring + live apply v1), M5c (in-place reset +
single-record redo), M5d (transparent interactive rebase), M5e (commit
gate + session-state taint lift — below). Every stage passed the
full-corpus gate on the locally rebuilt WASM.

## M5e — the commit gate (as built) + the §3.3 taint lift

### The mechanism decision (feasibility gate, run first, LOUD)

The design doc's sketch — a C hook blocking inside `CommitTransaction` on
a SAB/Atomics bridge awaiting the CAS (§14.2's "one deep hunk") — was
REJECTED on two hard findings:

1. **Event-loop deadlock.** Cells run PGlite ON the Node main thread
   (every `pgl_*` export is a synchronous `Module._pgl_*` call), and the
   committer, tailer, journal, AND the in-process gateway
   (`GatewayCore`, used by every suite and the single-process tier)
   share that same event loop. A synchronous JS import parked on
   `Atomics.wait` freezes the loop that must execute the CAS — deadlock
   by construction. Escaping it requires moving cells or the gateway to
   worker threads — a whole-architecture restructure. The salvaged
   shared-memory build variant (`codex/durable-vfs-postgres`) does not
   even address this: blocking would happen in the JS import, not in
   wasm; the SAB build solves wasm-heap sharing, which the gate never
   needed.
2. **It breaks M5d.** The rebase ladder is architecturally premised on
   local-commit-BEFORE-capture: harvest = post-commit same-session reads
   (net effect for free, detoasted for free). A gate that holds the
   commit un-committed at CAS time would destroy the harvest path the
   ladder ships on.

A literal two-phase park (early-return split of `CommitTransaction` /
`RecordTransactionCommit` between `XLogFlush` and
`TransactionIdCommitTree`) was assessed and rejected for the same M5d
reason plus the critical-section/state-splitting surgery it demands.

**ADOPTED: the deferred-truncate two-phase gate.** The honest §3.6 audit
against the reset-verdict machinery M5c already shipped: with the local
commit provisional and the CAS verdict applied AFTER it, every
pre-commit step is reversible-or-safe EXCEPT ONE:

| §3.6 step | Placement vs CAS (as built) | Why sound |
| --- | --- | --- |
| deferred triggers | before (local commit) | WAL'd + in-cell only (§3.7: no fs/network in cells); fully reversed by in-place reset |
| holdable portal materialization | before (local commit) | memory tuplestore; a REVERSED commit's new WITH HOLD cursors are CLOSEd by the host (vanilla failed-COMMIT semantics); landed/rebased commits keep them (their read set is ring-validated) |
| **ON COMMIT DELETE ROWS truncate** | **AFTER — deferred past the verdict** | the one physically irreversible step: `PgliteDeferOnCommitTruncates` captures the oid list at commit; `pgl_commit_gate_run()` truncates in its own txn post-landed; `pgl_commit_gate_discard()` (also called inside `pgl_reset_to_base`) drops it on loss |
| LO cleanup (`AtEOXact_LargeObject`) | before | descriptor close; pending writes are WAL'd → reversed by reset |
| NOTIFY (`PreCommit_Notify`) | before locally; delivery is post-verdict | delivery is tailer-driven from N frames that ride the WINNING POST (M3); a lost attempt's harvest dies with the attempt; queue SLRU dirt is reset-cleared |
| clog commit / ProcArrayEndTransaction / lock release | before (local commit) | single-backend cell: reset rewinds clog + counters; released locks protect nothing (no concurrency inside a cell) |

So "the gate returns the loss verdict into the transaction machinery" is
delivered as: the local commit is provisional; the verdict either
finalizes it (run the deferred truncates) or reverses it wholesale
(in-place reset + live advance), with the client observing exactly
40001-with-rollback semantics and the session surviving. No SAB, no
blocking, no wasm build variant, no commit-path park.

### Native changes (submodule, paired branch)

- **New file `src/backend/pglite/pgl_commit_gate.c`**: the pending-
  truncate list + `pgl_commit_gate_set/pending/run/discard` exports.
  `run` executes in its own transaction with a `SearchSysCacheExists1`
  guard (which also fixes a latent M5c hazard: a temp table created in a
  REVERSED commit leaves a stale `on_commits` entry whose rel no longer
  exists — vanilla `heap_truncate` would error at the next temp commit).
- **1 hunk in `tablecmds.c`** (`PreCommit_on_commit_actions`): hand
  `oids_to_truncate` to the gate when armed; vanilla otherwise. ON
  COMMIT DROP handling is untouched (transactional, WAL'd — must stay
  pre-record).
- **1 hunk (new function) in `localbuf.c`**: `PgliteFlushAllLocalBuffers`
  — dirty local buffers join `pgl_flush_base`'s local-durability-point
  contract, so the reset's wholesale local-buffer discard re-reads
  exactly the PRE-ATTEMPT temp content (and the losing attempt's temp
  writes vanish, matching abort semantics — this also retires the
  xid-rebind hazard for temp pages, since attempt-dirtied pages never
  survive the reset).
- **1 hunk in `smgr.c`**: TEMP-relation `smgrzeroextend` no longer bumps
  the reset-gate counter (suite-forced finding: a tainted session's
  FIRST insert into its own temp table zero-extends the temp file and
  would disqualify the very reset the taint lift depends on; the bytes
  are guaranteed zeros — dirty local pages reach disk only via
  `smgrwrite`, still counted — and trailing zero pages read back empty
  after the discard). SHARED-relation extension stays counted: a
  first-ever insert into a fresh regular table that loses its race
  still recycles (and a tainted session then still gets the fatal
  reset) — the documented boundary of the lift.
- `pgl_reset.c` (our file): base flush covers local buffers; reset
  discards pending truncates; scope notes updated.
- Submodule commit (paired branch, pushed): `94e13de32e`. Patch budget
  this commit: **3 hunks in existing Postgres files** (tablecmds.c,
  localbuf.c, smgr.c — the latter two are additions to already-hooked
  files); the rest is new-file / pglite.h / exports list.
- Full-corpus gates on the final WASM: core **269** (+1 skipped) /
  cell **80** / cell-server **77** (71 + the 6 new commit-gate modes;
  host.test.ts mode 4 updated from the pre-M5e fatal-reset contract to
  the survive-with-temp-intact contract) / gateway **51** — all green.

### The §3.3 taint REVISIT (deliverable 4) — taint-class end state

With gate + in-place reset + live advance, on a gated cell:

| Taint class | On CAS loss (as built) |
| --- | --- |
| temp tables | SURVIVES: reset restores pre-attempt temp content exactly (flush-at-base covers local buffers); interactive → 40001 + session alive; one-shot → transparent re-execute (when advanced to head) |
| holdable cursors | SURVIVES: tuplestores are memory state the reset never touches; the REVERSED commit's new WITH HOLD cursors are CLOSEd (`dropLostHoldables`) |
| session advisory locks | SURVIVES: in-memory lock table, untouched by reset; meaningful only within the cell (single backend) — unchanged semantics |
| ANY class, reset unsound (no base snapshot / storage writes escaped since base, e.g. attempt-time local-buffer eviction) | fatal session reset — the recycle fallback destroys exactly this state; unchanged §3.3 contract, now the exception rather than the rule |

Documented residual approximations: a lost transaction's SESSION-level
`SET`s persist (GUC state is not reset-scoped — pre-existing across all
M5c loss paths, now noted); tainted sessions still never enter the M5d
rebase ladder (§4.5 unchanged).

### JS wiring + tests

- `Cell.open({ commitGate })` (default ON) arms `pgl_commit_gate_set`;
  `RuntimeOpts.commitGate` is the per-runtime switch (`false` = vanilla
  commit sequence + pre-M5e tainted-loss contract).
- `session.ts drive()`: landed/read-only ⇒ `finishCommitGate` (run
  pendings; read cells swallow the truncate txn's catalog WAL like the
  abort path); every loss path ⇒ discard + (tainted) reset-first
  survival + `dropLostHoldables` (also fixes the latent M5c
  cursor-survives-reset re-execution bug for one-shots).
- `tests/commit-gate.test.ts` (6 modes): truncate PENDING at CAS time /
  run only post-verdict / vanilla end-state; REAL-race tainted loss ⇒
  40001 + session survives + PRESERVE ROWS content intact + retry lands;
  tainted one-shot transparent re-execute; reversed commit's pendings
  discarded not executed; holdable-cursor hygiene (new dropped, old
  kept); gate-off vanilla (truncate pre-CAS, fatal-reset contract back).


Companion to design doc §3.4/§3.6/§4/§5.3/§6.2–§6.3/§14.1–§14.2/§14.8 and
§15 M5, PLUS the live-apply scope deferred from M3 (M3_PLAN re-sequencing
note). All native work lands in the `postgres-pglite` submodule on the
paired branch `optimistic-physical-replication` under the §14.8 two-commit
ritual, built via the dockerized toolchain
(`build-with-docker.sh` → `electricsql/pglite-builder`), with the §14.1
patch budget tracked (hunks in existing files counted per commit).

The overriding gate for EVERY stage: **the full JS suite corpus (all
packages) re-runs green on the rebuilt WASM** — the fork must stay a
faithful vanilla Postgres with hooks off (§14.1: default-off,
vanilla-behaving when unconfigured).

## M5a — toolchain + the enforcement hooks (smallest first)

1. Validate the probe build (unmodified submodule builds; artifacts land
   in `dist/`; workspace `packages/pglite` rebuild via root
   `wasm:copy-*` + `build:js`; the ENTIRE existing test corpus green on
   the locally-built WASM — this is the M5 go/no-go).
2. Create the paired submodule branch; land hook set 1 (new files +
   tiny hunks, each GUC/export-gated):
   - **sequence grant clamp**: `nextval_internal` respects a
     `pglite.sequence_lease_end` limit (set per-sequence by the host via
     a `pgl_*` export or GUC table) — M4's grants become enforced-safe
     (§5.3 rule 1); plus a `pgl_reset_sequence_caches()` export (rule 3).
   - **WAL-LSN getters** + `PgliteDropRelationBuffersRange` /
     `FindAndDrop…Range` re-landed from `codex/durable-vfs-postgres`
     (salvage §13.1; provenance in commit messages).
3. Wire the sequence clamp into pglite-cell-server's grant machinery;
   abuse suite upgraded: clamp trips at the grant boundary under a
   deliberately tiny grant.

## M5b — WAL introspection + validation machinery

- **New C file: xlogreader wrapper** (`pgl_walscan`): iterate records of
  a byte range, emit per-record {lsn, rmgr, info, xl_xid, block refs,
  payload slices for commit/abort/invals/multixact/seq} to JS. This
  unlocks BOTH §4.3's three inval carriers and M3's eager special-record
  set (JS tailer upgraded from opaque slices to classified records).
- **Read-set capture**: the `PinBufferForBlock` ring-buffer hook (tiny
  hunk + new file), nblocks capture for seqscans, exclusions (FSM, local
  buffers, sequence pages), JS harvest at commit (§4.1).
- **Validation at rebase** (§4.2, JS over exports): `pageLSN(K) <= B`
  via a page-LSN peek export on pinned buffers; VM-bit content checks
  for IOS-skipped blocks (or `enable_indexonlyscan=off` in cells as the
  v1 escape hatch — decide by cost); schema-epoch fence from the
  classified invals.
- **Live tail apply v1 (the M3 deferral)**: followers apply W slices to
  a LIVE cell — clog/SLRU semantic pipeline + identity advancement via
  classified records, buffer invalidation via DropRelationBuffersRange,
  inval processing via a `pgl_process_invals` export, page content via
  lazy re-read (the VFS serves post-apply bytes; full page-version index
  + single-page redo can follow). Lifts the diverged-base re-materialize
  cost and the no-restart-DDL claim.

## M5c — in-place reset + single-page redo (SHIPPED; commit gate moved out)

SCOPE TRIM (2026-07): the commit gate (SAB/Atomics bridge, §3.6 reorder)
moved out of M5c — it follows the M5d rebase work. M5c as shipped:

- **In-place reset at crash-recovery-grade scope** (§3.4/§5.1):
  counters (`TransamVariables`, `MultiXactState`), SLRU state,
  sequence caches, relcache/syscache/plancache flush, losing-attempt
  temp storage, WAL insert position rewind — over the existing
  `__PGLITE__` anchors in clog/subtrans/transam/multixact. Replaces
  recycle for CAS-loss reset; recycle remains the fallback (gated on
  the storage-write counter — see pgl_reset.c's scope notes for why
  SLRU handling is speculative-range clears, not dirty discard).
- **Single-record redo** (`pgl_walscan_redo_current`, §14.2): the
  resident rm_redo driven against the LIVE buffer manager for the
  buffer-only rmgrs, plus `pgl_set_wal_position` (advance + rewind of
  the single-backend insert state). Lifts live-apply's FPI-only gate
  AND the write-cell restriction; temp-table pinned sessions now
  live-advance (the M5b deferred test, flipped).
- **Commit gate** (moved to follow M5d — SHIPPED as M5e, above): the
  SAB/Atomics blocking shape was rejected at the M5e feasibility gate
  (event-loop deadlock + it breaks M5d's harvest); the shipped gate is
  the deferred-truncate two-phase form — same property (no irreversible
  step finalized before the CAS verdict), no blocking, no build variant.

## M5d — logical re-apply + the full rebase ladder (SHIPPED)

As-built (2026-07-05) — the C-level re-apply was NOT needed; the shipped
design is JS-orchestrated over three small native additions:

- **Harvest = walscan enumeration + local heap re-read.** pgl_walscan's
  heap DML records now carry tuple offsets (submodule commit), so the
  host enumerates (rel, op, ctid, oldCtid) from the session's own WAL
  range (B, localEnd]. Net-effect filtering (aborted subxacts dropped,
  update chains collapsed) falls out of SAME-SESSION VISIBILITY: the
  txn committed locally before capture (M1 architecture), so one
  post-commit read by ctid returns exactly the committed net effect —
  detoasted for free. No WAL tuple extraction (§12 class 2 closed by
  construction).
- **Validation (§4.2)**: `pgl_page_lsn` (pin + BufferGetLSNAtomic +
  unpin — never executor paths) asserts pageLSN(K) <= B per captured
  page; `pgl_relation_nblocks` (fresh smgr lseek) asserts the seqscan
  freeze (min captured probe == nblocks(K)); the schema-epoch fence
  intersects winner-tail invals (3 carriers, from the classified
  walscan) with the txn's relation footprint. The read-set ring
  (M5b) is armed per transaction in drive(); overflow ⇒ 40001.
- **Re-apply = parameterized DML under
  `session_replication_role = replica`** in ONE fresh txn at K
  (triggers/RI suppressed — B-time effects are harvested data;
  volatile fns never re-run — values ride as parameters; fresh XID
  intrinsic). Pre-existing rows addressed by B-time ctid — sound
  because validation proved their pages untouched. 23505 ⇒ 40001
  (§4.0). Bounded: 2 rebase rounds, then 40001 (§4.7).
- **v1 escape hatches** (documented approximations):
  `enable_indexonlyscan = off` set at Cell.open (skips VM-bit content
  machinery entirely); ctid/xmin/cmin/cmax/txid taints via
  statement-text scan; identity/generated columns handled
  (OVERRIDING SYSTEM VALUE / recompute).
- §16 rebase soundness suite: tests/rebase.test.ts (12 modes — happy
  path w/ stable serial+now(), winner-touched page, extension phantom
  via nblocks, own-WAL masking, catalog fence (commit-record carrier;
  VACUUM-inplace carrier shares the decode path, not separately
  constructible in-suite), aborted-subxact filtering, TOAST 100KB,
  uniqueness ⇒ 40001-never-23505, ctid taint, temp-write taint,
  bounds exhaustion, proxy-level transparent success). Deferred-trigger
  ctid remap is MOOT in the shipped design (replica role suppresses
  the queues; harvested data already embodies their B-time effects).
- Session-state taint contract unchanged: tainted sessions never enter
  the ladder (fatal reset on loss, §3.3).
- Submodule commits (paired branch, pushed): 5dd6592255 (pgl_page_lsn /
  pgl_relation_nblocks / heap DML walscan decode — zero hunks in
  existing files) and 9ef93a3b04 (two one-hunk gap fixes the suite
  found: the M5b nblocks hook missed the table-AM branch, so seqscan
  freezes were never captured; the M5c reset gate missed
  smgrzeroextend, so bulk-extended file space survived in-place
  resets).

## Sequencing note

M5a and M5b are independently valuable and land first (M5a makes M4
enforcement-complete; M5b closes M3). M5c/M5d are the deepest cuts and
ship last, each behind the full-corpus regression gate.
