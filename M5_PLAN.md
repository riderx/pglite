# M5 implementation plan — the native wave (interactive rebase + live apply)

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
- **Commit gate** (moved to follow M5d): block inside
  `CommitTransaction` awaiting the CAS verdict via a synchronous JS
  import (the SAB/Atomics bridge — Tier-1 salvage of the shared-memory
  build variant), enabling the §3.6 commit-sequence reorder
  (temp-truncate after CAS) and mid-COMMIT loss handling without
  recycle.

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
