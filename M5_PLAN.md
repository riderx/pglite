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

## M5c — in-place reset + commit gate

- **In-place reset at crash-recovery-grade scope** (§3.4/§5.1):
  counters (`TransamVariables`, `MultiXactState`), ALL SLRU buffers,
  sequence caches, relcache/syscache/plancache flush, losing-attempt
  temp storage, WAL insert position rewind — over the existing
  `__PGLITE__` anchors in clog/subtrans/transam/multixact. Replaces
  recycle for CAS-loss reset; recycle remains the fallback.
- **Commit gate**: block inside `CommitTransaction` awaiting the CAS
  verdict via a synchronous JS import (the SAB/Atomics bridge — Tier-1
  salvage of the shared-memory build variant), enabling the §3.6
  commit-sequence reorder (temp-truncate after CAS) and mid-COMMIT
  loss handling without recycle.

## M5d — logical re-apply + the full rebase ladder

- Harvest → net-effect filter (local clog/subtrans) → re-apply through
  heapam/index-AM/TOAST with one fresh XID at K (§4.4, C files);
  deferred-trigger ctid remap; rebase taints (§4.5) incl. temp-write;
  bounded loop (§4.7); `40001` only past the bounds. Session-state
  taint lift (§3.3) via the in-place reset.
- §16 rebase soundness suite: every verified failure mode as a test
  (VM-bit deletes, extension phantoms, own-WAL masking, inval fencing ×3
  carriers, aborted-subxact filtering, TOAST re-chunking, trigger
  remap, sequence reset rules).

## Sequencing note

M5a and M5b are independently valuable and land first (M5a makes M4
enforcement-complete; M5b closes M3). M5c/M5d are the deepest cuts and
ship last, each behind the full-corpus regression gate.
