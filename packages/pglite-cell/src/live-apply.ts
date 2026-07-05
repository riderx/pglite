// Live tail apply v1 (M5b, design §6.2–§6.3): advance a LIVE read-attached
// cell past foreign commits without recycle+re-materialize.
//
// The slice bytes are first written into the live cell's own pg_wal
// (writeWalRange — the NODEFS passthrough makes them visible to the native
// scanner), then classified with pgl_walscan and — iff the batch passes the
// v1 safety gate — applied through the pgl_* primitives:
//
//   clog bits + identity advancement + multixact SLRU records + inval
//   processing + buffer eviction for touched blocks + smgr create handling
//   + full-page-image restore to disk for has_image blocks.
//
// Page-content soundness (the gate), v2 (M5c): records of buffer-only
// rmgrs (heap/heap2/btree/hash/gin/gist/spgist/brin/generic + xlog FPI)
// are applied through the native single-record redo harness
// (pgl_walscan_redo_current → the resident rm_redo against the live
// buffer manager), which handles images, record-data redo, init pages and
// relation extension alike. Remaining records must either be in the
// semantic eager set, be sequence records (§5.3: allocation authority is
// the lease, never the page — the local page must NOT be clobbered or
// evicted), or carry restorable full-page images for every block.
// Everything else falls back to the recycle-advance path. The WAL insert
// position is advanced to the applied head before redo (page LSNs set by
// redo must be covered by local flush state) — which is also what makes
// WRITE cells live-advanceable: they publish from the new head.

import { closeSync, existsSync, openSync, rmSync, writeSync } from 'node:fs'
import { join } from 'node:path'
import type { PGlite } from '@electric-sql/pglite'
import { walscan, walscanRange } from './walscan'
import type { WalRecord } from './walscan'

/** Kinds v1 never applies live (file/metadata effects beyond its scope). */
const FALLBACK_KINDS = new Set([
  'smgr_truncate', // synthesizing un-logged FSM/VM boundary mutations
  'dbase_create',
  'dbase_drop',
  'clog_truncate',
  'multixact_truncate',
  'parameter_change',
])

/** Rmgrs with record kinds outside the classified set that v1 must not
 *  skip silently: tablespace (5) and relmap (7) mutate files/state we do
 *  not handle. Standby (8) is metadata-noise and safe. */
const FALLBACK_RMIDS = new Set([5, 7])

/**
 * M5c single-record redo (design §14.2): rmgrs whose redo routines are
 * buffer-only, applied to the LIVE cell via the native rm_redo harness
 * (pgl_walscan_redo_current). Mirror of pglws_redo_whitelisted in
 * pgl_walscan.c: heap2 (9, except REWRITE 0x00 — writes mapping files),
 * heap (10), btree (11), hash (12), gin (13), gist (14), spgist (16),
 * brin (17), generic (20); xlog (0) FPI records are redoable too.
 */
const REDO_RMIDS = new Set([9, 10, 11, 12, 13, 14, 15, 16, 17, 20])
const XLOG_HEAP_OPMASK = 0x70

export function isRedoable(rec: WalRecord): boolean {
  if (rec.rmid === 0) return rec.kind === 'fpi'
  if (rec.rmid === 9) return (rec.info & XLOG_HEAP_OPMASK) !== 0x00
  return REDO_RMIDS.has(rec.rmid)
}

// SEQUENCE NOTE (M5c revision of the M5b skip): XLOG_SEQ_LOG records ARE
// redone (seq_redo, buffer-only page reinit from the logged tuple). The
// M5b "never touch the local sequence page" skip left sequences CREATED
// after base as zero-length files (their only page write is the seq_log
// record), which broke any later page read (pg_sequences probes). §5.3
// still holds: allocation AUTHORITY stays with leases/floors — after a
// write-cell advance the session re-applies floors (covers local
// abort-observed draws the foreign page state may sit below) and
// re-clamps leases with caches flushed; read cells' local draws never
// persist (a draw write-upgrades and recycles the cell).

/** Thrown when a batch failed mid-apply: the cell is part-advanced and
 *  MUST be recycled by the caller. */
export class LiveApplyAbortError extends Error {
  constructor(message: string) {
    super(`live-apply aborted mid-batch: ${message}`)
    this.name = 'LiveApplyAbortError'
  }
}

/** Cumulative counters (per process) — honest hit-rate data for the
 *  page-materialization follow-up decision. */
export const liveApplyStats = {
  attempts: 0,
  hits: 0,
  /** Distinct fallback reasons seen (reason -> count). */
  fallbacks: new Map<string, number>(),
  /** Most recent fallback reason (diagnostics). */
  lastFallback: '' as string,
}

function fallback(reason: string): false {
  liveApplyStats.fallbacks.set(
    reason,
    (liveApplyStats.fallbacks.get(reason) ?? 0) + 1,
  )
  liveApplyStats.lastFallback = reason
  return false
}

if (process.env.PGLITE_LIVE_APPLY_STATS === '1') {
  process.on('exit', () => {
    const fb = Object.fromEntries(liveApplyStats.fallbacks)

    console.log(
      `[live-apply] attempts=${liveApplyStats.attempts} hits=${liveApplyStats.hits} fallbacks=${JSON.stringify(fb)}`,
    )
  })
}

/**
 * The v1 gate: can this classified batch be applied to a live cell with
 * page-content correctness guaranteed by FPI restore alone?
 */
export function isLiveAppliable(records: WalRecord[]): boolean {
  for (const rec of records) {
    if (rec.kind !== undefined && FALLBACK_KINDS.has(rec.kind)) {
      return fallback(rec.kind)
    }
    if (FALLBACK_RMIDS.has(rec.rmid)) return fallback(`rmid-${rec.rmid}`)
    if (isRedoable(rec)) continue // M5c: applied via native rm_redo (incl. seq)
    for (const b of rec.blocks) {
      if (!b.img) {
        if (process.env.PGLITE_LIVE_APPLY_DEBUG === '1') {
          console.log(
            '[live-apply] reject',
            JSON.stringify(rec, (_k, v) =>
              typeof v === 'bigint' ? v.toString() : v,
            ),
          )
        }
        return fallback(b.init ? 'init-no-image' : 'no-image')
      }
      // FPI restore needs a computable file path: default or global
      // tablespace, known fork.
      if (b.rel[0] !== 1663 && b.rel[0] !== 1664) return fallback('tablespace')
      if (b.fork < 0 || b.fork > 3) return fallback('fork')
    }
  }
  return true
}

const FORK_SUFFIX = ['', '_fsm', '_vm', '_init']
const BLOCKS_PER_SEG = 131072 // 1 GiB segments / 8 KiB blocks

function relFilePath(
  dir: string,
  rel: [number, number, number],
  fork: number,
  seg: number,
): string | null {
  const [spc, db, relNumber] = rel
  let base: string
  if (spc === 1664) base = join(dir, 'global')
  else if (db !== 0) base = join(dir, 'base', String(db))
  else return null // non-default tablespace or odd locator: bail
  const suffix = (FORK_SUFFIX[fork] ?? null) as string | null
  if (suffix === null) return null
  return join(base, `${relNumber}${suffix}${seg > 0 ? `.${seg}` : ''}`)
}

function writeBlock(path: string, blkInSeg: number, page: Uint8Array): void {
  let fd: number
  try {
    fd = openSync(path, 'r+')
  } catch {
    fd = openSync(path, 'w+')
  }
  try {
    writeSync(fd, page, 0, 8192, blkInSeg * 8192)
  } finally {
    closeSync(fd)
  }
}

function withHeapBytes<T>(
  pg: PGlite,
  hex: string,
  fn: (ptr: number, byteLen: number) => T,
): T {
  const bytes = Buffer.from(hex, 'hex')
  const mod = pg.Module
  const ptr = mod._malloc(bytes.length)
  try {
    mod.HEAPU8.set(bytes, ptr)
    return fn(ptr, bytes.length)
  } finally {
    mod._free(ptr)
  }
}

const CLOG_COMMITTED = 1
const CLOG_ABORTED = 2

export interface LiveApplyResult {
  applied: boolean
  records: number
}

/**
 * Classify `[start, end)` (bytes already in the live cell's pg_wal) and,
 * if the gate passes, apply the §6.3 eager set to the live instance.
 * Returns `{ applied: false }` untouched when the gate rejects — the
 * caller falls back to recycle-advance.
 *
 * IMPORTANT: only call between transactions on a read-attached cell; on
 * success the cell's pg_wal contents past its own insert position are
 * scratch (never captured — read cells publish nothing).
 */
export function applyLiveTail(
  pg: PGlite,
  dir: string,
  start: bigint,
  end: bigint,
): LiveApplyResult {
  liveApplyStats.attempts++
  const records = walscanRange(pg, start, end)
  if (!isLiveAppliable(records)) {
    if (process.env.PGLITE_LIVE_APPLY_STATS === '1') {
      console.log(`[live-apply] MISS ${liveApplyStats.lastFallback}`)
    }
    return { applied: false, records: records.length }
  }
  if (process.env.PGLITE_LIVE_APPLY_STATS === '1') {
    console.log('[live-apply] HIT')
  }

  const mod = pg.Module

  // M5c: advance the WAL insert position to the applied head FIRST. Redo
  // sets page LSNs up to `end`; a later FlushBuffer would XLogFlush those
  // LSNs, which must already be covered by the local write/flush state.
  // (The slice bytes are in pg_wal; read cells never publish, and write
  // cells publish from the new head — this is what lifts the M5b
  // write-cell restriction.)
  const lastRec = records[records.length - 1].lsn
  if (mod._pgl_set_wal_position(end, lastRec) !== 1) {
    return { applied: false, records: records.length }
  }

  const touchedRels = new Map<string, [number, number, number]>()

  walscan(pg, start, end, (rec, imageOf) => {
    // Identity advancement, exactly as the recovery loop (§5.1): every
    // record's xl_xid, plus payload xids below.
    if (rec.xid > 0) mod._pgl_advance_xid_past(rec.xid)

    // M5c: buffer-only rmgrs go through the native rm_redo harness — the
    // real redo routine against the live buffer manager (images restored,
    // record data applied, pages extended, FSM/VM maintained). No eviction
    // and no direct file writes for these (incl. seq_log — see the
    // SEQUENCE NOTE above).
    if (isRedoable(rec)) {
      const r = mod._pgl_walscan_redo_current()
      if (r === 1) {
        for (const b of rec.blocks) touchedRels.set(b.rel.join('/'), b.rel)
        // heap_inplace carries invals redo does not process outside hot
        // standby — apply them exactly as the non-redo path does.
        if (
          rec.kind === 'heap_inplace' &&
          (rec.nmsgs ?? 0) > 0 &&
          rec.invals !== undefined
        ) {
          withHeapBytes(pg, rec.invals, (ptr) =>
            mod._pgl_process_invals(
              ptr,
              rec.nmsgs!,
              rec.relcacheInitFileInval ?? false,
              rec.dbId ?? 0,
              rec.tsId ?? 0,
            ),
          )
        }
        return
      }
      if (r < 0) {
        throw new LiveApplyAbortError(
          `rm_redo failed (${r}) at ${rec.lsn} rmid=${rec.rmid} info=${rec.info}`,
        )
      }
      // r === 0 (native whitelist narrower than ours): fall through to the
      // image path; the gate guaranteed images only for non-redoable
      // records, so this is a mirror bug — fail loudly.
      throw new LiveApplyAbortError(
        `whitelist mirror mismatch at ${rec.lsn} rmid=${rec.rmid} info=${rec.info}`,
      )
    }

    // Page images + buffer eviction for every touched block.
    {
      for (let i = 0; i < rec.blocks.length; i++) {
        const b = rec.blocks[i]
        const page = imageOf(i)
        if (page !== null) {
          const path = relFilePath(
            dir,
            b.rel,
            b.fork,
            Math.floor(b.blk / BLOCKS_PER_SEG),
          )
          if (path !== null) writeBlock(path, b.blk % BLOCKS_PER_SEG, page)
        }
        mod._pgl_drop_relation_buffers_range(
          b.rel[0],
          b.rel[1],
          b.rel[2],
          b.fork,
          b.blk,
          1,
        )
        touchedRels.set(b.rel.join('/'), b.rel)
      }
    }

    switch (rec.kind) {
      case 'commit': {
        mod._pgl_clog_set(rec.xid, CLOG_COMMITTED)
        for (const sub of rec.subxids ?? []) {
          mod._pgl_clog_set(sub, CLOG_COMMITTED)
          mod._pgl_advance_xid_past(sub)
        }
        for (const drop of rec.drops ?? []) {
          for (let fork = 0; fork <= 2; fork++) {
            mod._pgl_drop_relation_buffers_range(
              drop[0],
              drop[1],
              drop[2],
              fork,
              0,
              0,
            )
            const p = relFilePath(dir, drop, fork, 0)
            if (p !== null && existsSync(p)) rmSync(p, { force: true })
          }
          mod._pgl_smgr_release(drop[0], drop[1], drop[2])
        }
        if ((rec.nmsgs ?? 0) > 0 && rec.invals !== undefined) {
          withHeapBytes(pg, rec.invals, (ptr) =>
            mod._pgl_process_invals(
              ptr,
              rec.nmsgs!,
              rec.relcacheInitFileInval ?? false,
              rec.dbId ?? 0,
              rec.tsId ?? 0,
            ),
          )
        }
        break
      }
      case 'abort': {
        mod._pgl_clog_set(rec.xid, CLOG_ABORTED)
        for (const sub of rec.subxids ?? []) {
          mod._pgl_clog_set(sub, CLOG_ABORTED)
          mod._pgl_advance_xid_past(sub)
        }
        for (const drop of rec.drops ?? []) {
          for (let fork = 0; fork <= 2; fork++) {
            mod._pgl_drop_relation_buffers_range(
              drop[0],
              drop[1],
              drop[2],
              fork,
              0,
              0,
            )
            const p = relFilePath(dir, drop, fork, 0)
            if (p !== null && existsSync(p)) rmSync(p, { force: true })
          }
          mod._pgl_smgr_release(drop[0], drop[1], drop[2])
        }
        break
      }
      case 'invalidations':
      case 'heap_inplace': {
        if ((rec.nmsgs ?? 0) > 0 && rec.invals !== undefined) {
          withHeapBytes(pg, rec.invals, (ptr) =>
            mod._pgl_process_invals(
              ptr,
              rec.nmsgs!,
              rec.relcacheInitFileInval ?? false,
              rec.dbId ?? 0,
              rec.tsId ?? 0,
            ),
          )
        }
        break
      }
      case 'multixact_create': {
        withHeapBytes(pg, rec.members_raw ?? '', (ptr) =>
          mod._pgl_multixact_record(rec.mid!, rec.moff!, rec.nmembers!, ptr),
        )
        for (const [mxid] of rec.members ?? []) {
          mod._pgl_advance_xid_past(mxid)
        }
        mod._pgl_advance_identity(
          0n,
          0,
          rec.mid! + 1,
          rec.moff! + rec.nmembers!,
        )
        break
      }
      case 'multixact_zero_off_page':
        mod._pgl_multixact_zero_off_page(rec.pageno!)
        break
      case 'multixact_zero_mem_page':
        mod._pgl_multixact_zero_mem_page(rec.pageno!)
        break
      case 'clog_zero_page':
        mod._pgl_clog_zero_page(rec.pageno!)
        break
      case 'nextoid':
        mod._pgl_advance_identity(0n, rec.nextOid!, 0, 0)
        break
      case 'checkpoint':
        mod._pgl_advance_identity(
          rec.nextXid!,
          rec.nextOid!,
          rec.nextMulti!,
          rec.nextMultiOffset!,
        )
        break
      case 'smgr_create': {
        const p = relFilePath(dir, rec.rel!, rec.fork!, 0)
        if (p !== null && !existsSync(p)) closeSync(openSync(p, 'w'))
        break
      }
      default:
        break
    }
  })

  // Post-batch: drop cached xact-status state so lookups hit the updated
  // SLRUs, and forget cached relation sizes for touched relations so the
  // next nblocks probe re-lseeks (externally grown files).
  for (const rel of touchedRels.values()) {
    mod._pgl_smgr_release(rel[0], rel[1], rel[2])
  }
  mod._pgl_invalidate_xact_caches()

  liveApplyStats.hits++
  return { applied: true, records: records.length }
}
