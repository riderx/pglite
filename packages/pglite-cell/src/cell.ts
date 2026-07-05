// One cell = one PGlite instance plain-opened on a materialized datadir,
// plus the capture cursor. The cursor is the end LSN of the last bytes this
// cell published (or attached at); every captured slice is exactly
// `(cursor, insertLsn]` — contiguous, unfiltered, no gaps ever (M1_PLAN
// "The one invariant that makes slices compose").

import { PGlite } from '@electric-sql/pglite'
import { parseLsn } from './lsn'
import { readControl, readWalRange } from './datadir'
import { shutdownCheckpointEnd } from './lsn'
import { ConfigPinError, ZeroBootWalError } from './errors'
import { applyLiveTail } from './live-apply'
import type { LiveApplyResult } from './live-apply'
import { walscanRange } from './walscan'
import type { WalRecord } from './walscan'

/** One captured page pin (read-set ring entry, kind 0 — design §4.1). */
export interface ReadSetPage {
  spc: number
  db: number
  rel: number
  fork: number
  blk: number
}

/** One captured nblocks probe (read-set ring entry, kind 1). */
export interface ReadSetNblocks {
  spc: number
  db: number
  rel: number
  fork: number
  nblocks: number
}

/** The harvested read set of one transaction (M5d rebase validation). */
export interface ReadSetSnapshot {
  pins: ReadSetPage[]
  nblocks: ReadSetNblocks[]
  overflowed: boolean
}

/** A captured WAL byte range `(baseLsn, endLsn]`, ready to commit. */
export interface CapturedSlice {
  baseLsn: bigint
  endLsn: bigint
  bytes: Uint8Array
}

export interface CellOpenOpts {
  /**
   * The stream head LSN this datadir was materialized to. A plain open of
   * the cleanly-closed dir must leave the insert LSN exactly here (zero
   * boot WAL — M0 finding 1).
   */
  expectedHeadLsn: bigint
  /**
   * M5e commit gate (design §3.6): defer the ON COMMIT DELETE ROWS
   * temp-table truncate — the one irreversible pre-commit step — past
   * the CAS verdict. The host runs `commitGateRun()` after a landed
   * verdict and `commitGateDiscard()` on loss. Default true.
   */
  commitGate?: boolean
}

/** §9 configuration pins asserted at every open. */
const CONFIG_PINS: { name: string; expected: string }[] = [
  { name: 'wal_level', expected: 'replica' },
  { name: 'full_page_writes', expected: 'on' },
  { name: 'data_checksums', expected: 'off' },
]

/**
 * Base snapshot for in-place reset (M5c, design §3.4/§5.1): the identity
 * counters + WAL chain position + storage-write counter captured at a
 * moment when the WAL insert position sat exactly at the capture cursor.
 */
export interface BaseSnapshot {
  /** The base LSN this snapshot was taken at (== captureCursor then). */
  lsn: bigint
  /** Start LSN of the last record before base (xl_prev restore). */
  prevRecLsn: bigint
  nextXid: bigint
  nextOid: number
  nextMulti: number
  nextOffset: number
  /** pgl_storage_writes at snapshot time (reset soundness gate). */
  writes: bigint
}

/** Cumulative in-place reset counters (per process). */
export const resetStats = {
  inPlace: 0,
  fallbackRecycle: 0,
  /** reason -> count for reset fallbacks. */
  reasons: new Map<string, number>(),
}

if (process.env.PGLITE_LIVE_APPLY_STATS === '1') {
  process.on('exit', () => {
    console.log(
      `[reset] inPlace=${resetStats.inPlace} fallbackRecycle=${resetStats.fallbackRecycle} reasons=${JSON.stringify(Object.fromEntries(resetStats.reasons))}`,
    )
  })
}

function resetFallback(reason: string): void {
  resetStats.fallbackRecycle++
  resetStats.reasons.set(reason, (resetStats.reasons.get(reason) ?? 0) + 1)
}

/**
 * A solo cell: plain-opens a materialized datadir (no recovery, no
 * synthesis, zero boot WAL), asserts the §9 config pins and the
 * zero-boot-WAL invariant, and manages the capture cursor across
 * `captureSlice` / `confirmPublished` / `closeClean`.
 */
export class Cell {
  private cursor: bigint

  private constructor(
    public readonly dir: string,
    private readonly pg: PGlite,
    cursor: bigint,
  ) {
    this.cursor = cursor
  }

  /** The underlying PGlite instance (SQL execution passthrough). */
  get db(): PGlite {
    return this.pg
  }

  /** The capture cursor: end LSN of the last published bytes. */
  get captureCursor(): bigint {
    return this.cursor
  }

  /**
   * Plain-open `dir` and verify it is safe to capture from:
   *
   * - the §9 config pins hold (`wal_level=replica`, `full_page_writes=on`,
   *   `data_checksums=off`) — else `ConfigPinError` listing every mismatch;
   * - the boot wrote zero WAL: `pg_current_wal_insert_lsn()` equals
   *   `expectedHeadLsn` — else `ZeroBootWalError` (actual vs expected).
   *
   * On success the capture cursor starts at `expectedHeadLsn`.
   */
  static async open(dir: string, opts: CellOpenOpts): Promise<Cell> {
    const pg = new PGlite(dir)
    try {
      const mismatches: { name: string; expected: string; actual: string }[] =
        []
      for (const pin of CONFIG_PINS) {
        const rows = (
          await pg.query<Record<string, string>>(`show ${pin.name}`)
        ).rows
        const actual = rows[0][pin.name]
        if (actual !== pin.expected) {
          mismatches.push({ name: pin.name, expected: pin.expected, actual })
        }
      }
      if (mismatches.length > 0) throw new ConfigPinError(mismatches)

      // M5d v1 escape hatch (design §4.2 alternative): index-only scans
      // consume VM-bit CONTENT that page-LSN validation cannot see
      // (visibilitymap_clear does not stamp LSNs). Rather than build VM-bit
      // content checks, cells simply never plan IOS. Session-level GUC on a
      // single-backend instance; writes no WAL.
      await pg.exec('set enable_indexonlyscan = off')

      const insertLsn = await currentInsertLsn(pg)
      if (insertLsn !== opts.expectedHeadLsn) {
        throw new ZeroBootWalError(opts.expectedHeadLsn, insertLsn)
      }
      // M5e commit gate (§3.6): armed by default — vanilla behavior
      // returns only when explicitly disabled.
      if (opts.commitGate !== false) pg.Module._pgl_commit_gate_set(1)
      const cell = new Cell(dir, pg, opts.expectedHeadLsn)
      cell.maybeSnapshotBase()
      return cell
    } catch (err) {
      await pg.close().catch(() => undefined)
      throw err
    }
  }

  /** The current WAL insert LSN (a commit-time bookmark). */
  async bookmark(): Promise<bigint> {
    return currentInsertLsn(this.pg)
  }

  /**
   * Capture `(cursor, insertLsn]` from the local pg_wal. Returns null iff
   * the insert LSN still equals the cursor (read-only work generates no
   * slice). The cursor does NOT move — call `confirmPublished` only after
   * the slice's append landed.
   */
  async captureSlice(): Promise<CapturedSlice | null> {
    const end = await this.bookmark()
    if (end === this.cursor) return null
    // Temp-only transactions commit through the ASYNC path (no XLogFlush,
    // and no walwriter exists to catch up) — flush explicitly so the
    // captured bytes are never a torn tail (M5c finding; generalizes the
    // M4 abort-tail finding).
    this.flushWal()
    if (process.env.PGL_VALIDATE_SLICES === '1') {
      // Debug oracle: the captured range must parse end-to-end with the
      // native reader BEFORE it can be published.
      const mod = this.pg.Module
      if (mod._pgl_walscan_begin(this.cursor, end, 1) === 0) {
        let last = this.cursor
        for (;;) {
          const ptr = mod._pgl_walscan_next()
          if (ptr === 0) break
          const rec = JSON.parse(mod.UTF8ToString(ptr)) as {
            error?: string
            end?: string
          }
          if (rec.error !== undefined) {
            console.error(
              `[cell] TORN SLICE CAPTURED [${this.cursor}, ${end}): ${rec.error} (last good ${last})`,
            )
            break
          }
          last = BigInt(rec.end!)
        }
        mod._pgl_walscan_end_scan()
        if (last !== end) {
          console.error(
            `[cell] slice scan stopped at ${last}, expected ${end} (base ${this.cursor})`,
          )
        }
      }
    }
    return {
      baseLsn: this.cursor,
      endLsn: end,
      bytes: readWalRange(this.dir, this.cursor, end),
    }
  }

  /** Flush local WAL through the insert position (pgl_flush_wal). */
  flushWal(): void {
    this.pg.Module._pgl_flush_wal()
  }

  /**
   * Deferred ON COMMIT DELETE ROWS truncates awaiting a CAS verdict
   * (M5e commit gate, §3.6). Nonzero only between a gated local commit
   * and its verdict.
   */
  commitGatePending(): number {
    return this.pg.Module._pgl_commit_gate_pending()
  }

  /**
   * Landed verdict: execute the deferred truncates (their own native
   * transaction — strictly after the CAS). Throws on native failure;
   * the caller must then treat the cell as poisoned and recycle.
   */
  commitGateRun(): void {
    const rc = this.pg.Module._pgl_commit_gate_run()
    if (rc !== 0) {
      throw new Error(`commitGateRun: native truncate run failed (${rc})`)
    }
  }

  /**
   * Loss verdict: the local commit is being reversed — the deferred
   * truncates must never run. (pgl_reset_to_base also discards natively;
   * this covers the recycle path and belt-and-braces callers.)
   */
  commitGateDiscard(): void {
    this.pg.Module._pgl_commit_gate_discard()
  }

  /**
   * Advance the capture cursor to `endLsn`. Call ONLY after the slice
   * ending there landed on the stream (a landed CAS append).
   */
  confirmPublished(endLsn: bigint, opts: { flush?: boolean } = {}): void {
    this.cursor = endLsn
    this.maybeSnapshotBase(opts)
  }

  private base: BaseSnapshot | null = null

  /** The last successfully captured base snapshot (null before first). */
  get baseSnapshot(): BaseSnapshot | null {
    return this.base
  }

  /**
   * Capture a base snapshot IF the WAL insert position currently sits at
   * the capture cursor (i.e. nothing unpublished is pending). Cheap: one
   * synchronous WASM call, no SQL. Call between transactions — after
   * open, after every `confirmPublished`, after `advanceTo`. When the
   * insert position is past the cursor (e.g. locally-aborted WAL not yet
   * swept into a slice), the previous snapshot — still anchored at the
   * cursor — is deliberately kept.
   */
  maybeSnapshotBase(opts: { flush?: boolean } = {}): void {
    const mod = this.pg.Module
    let ptr = mod._pgl_get_identity()
    if (ptr === 0) return
    let raw = JSON.parse(mod.UTF8ToString(ptr)) as {
      nextXid: string
      nextOid: number
      nextMulti: number
      nextOffset: number
      prevRecLsn: string
      insertLsn: string
      writes: string
    }
    if (BigInt(raw.insertLsn) !== this.cursor) return
    if (opts.flush !== false) {
      // Make base a LOCAL DURABILITY POINT (see pgl_reset.c): flush all
      // dirty pages + SLRUs so a later in-place reset's discard+reread
      // lands exactly on base. Re-read the identity afterwards — the
      // flush itself bumps the storage-write counter.
      if (mod._pgl_flush_base() !== 0) return
      ptr = mod._pgl_get_identity()
      if (ptr === 0) return
      raw = JSON.parse(mod.UTF8ToString(ptr)) as typeof raw
    }
    this.base = {
      lsn: this.cursor,
      prevRecLsn: BigInt(raw.prevRecLsn),
      nextXid: BigInt(raw.nextXid),
      nextOid: raw.nextOid,
      nextMulti: raw.nextMulti,
      nextOffset: raw.nextOffset,
      writes: BigInt(raw.writes),
    }
  }

  /**
   * Is an in-place reset to the current base sound right now? Requires a
   * snapshot anchored at the capture cursor and NO storage writes (data
   * pages, SLRU pages, smgr extends) having escaped shared memory since —
   * otherwise on-disk state may hold speculative bytes a reset cannot
   * undo, and the caller must recycle.
   */
  canResetInPlace(): boolean {
    if (this.base === null || this.base.lsn !== this.cursor) {
      resetFallback('no-base-snapshot')
      return false
    }
    if (this.pg.Module._pgl_storage_write_count() !== this.base.writes) {
      resetFallback('storage-writes-since-base')
      return false
    }
    return true
  }

  /**
   * In-place reset to base (M5c, §3.4/§5.1 scope): discard speculative
   * shared buffers / SLRU ranges / counters / caches / temp storage and
   * rewind the WAL insert position to the capture cursor. Throws on any
   * native failure — the caller MUST recycle then (state may be
   * part-mutated). Only call between transactions after a CAS loss.
   */
  resetToBase(): void {
    const base = this.base
    if (base === null || base.lsn !== this.cursor) {
      throw new Error('resetToBase: no base snapshot at cursor')
    }
    const rc = this.pg.Module._pgl_reset_to_base(
      base.lsn,
      base.prevRecLsn,
      base.nextXid,
      base.nextOid,
      base.nextMulti,
      base.nextOffset,
    )
    if (rc !== 0) {
      resetFallback(`native-${rc}`)
      throw new Error(`resetToBase: native reset failed (${rc})`)
    }
    resetStats.inPlace++
    // The snapshot remains the valid base description post-reset.
  }

  /**
   * Advance the capture cursor to `endLsn` after a successful LIVE tail
   * apply (the local WAL now holds the foreign bytes and the insert
   * position was set to `endLsn`), then re-snapshot the base.
   */
  advanceTo(endLsn: bigint, opts: { flush?: boolean } = {}): void {
    this.cursor = endLsn
    this.maybeSnapshotBase(opts)
  }

  /**
   * Register (or update) the native sequence-allocation lease for a
   * sequence (§5.3 rule 1): `nextval` on this cell clamps its effective
   * MAXVALUE — and the 32-ahead pre-log target — to `leaseEnd`. Exhaustion
   * raises the standard reached-maximum error with the errdetail
   * `sequence lease exhausted` (the host's renew signal). `leaseEnd = 0n`
   * clears the lease for that sequence.
   */
  setSequenceLease(seqOid: number, leaseEnd: bigint): void {
    this.pg.Module._pgl_set_sequence_lease(seqOid, leaseEnd)
  }

  /** Drop every native sequence lease (vanilla nextval behavior resumes). */
  clearSequenceLeases(): void {
    this.pg.Module._pgl_clear_sequence_leases()
  }

  /**
   * Flush the backend-local sequence (SeqTable) cache (§5.3 rule 3).
   * Called after floors/leases apply on cell open — relfilenumber-keyed
   * invalidation cannot catch replayed foreign sequence records.
   */
  resetSequenceCaches(): void {
    this.pg.Module._pgl_reset_sequence_caches()
  }

  /**
   * Enable read-set capture (design §4.1, M5d): resets the native ring
   * and records every shared-buffer page pin + nblocks probe until
   * disabled. Call at transaction start; harvest with readSetSnapshot()
   * BEFORE anything else runs on the cell after a CAS loss.
   */
  readSetBegin(): void {
    this.pg.Module._pgl_readset_enable(1)
  }

  /** Disable read-set capture (ring content stays readable). */
  readSetEnd(): void {
    this.pg.Module._pgl_readset_enable(0)
  }

  /**
   * Copy the captured read set out of WASM memory. `overflowed` means the
   * ring dropped entries — the validator must treat the transaction as
   * unrebaseable (40001).
   */
  readSetSnapshot(): ReadSetSnapshot {
    const mod = this.pg.Module
    const n = mod._pgl_readset_count()
    const ptr = mod._pgl_readset_snapshot()
    const words = new Uint32Array(mod.HEAPU8.buffer, ptr, n * 5)
    const pins: ReadSetPage[] = []
    const nblocks: ReadSetNblocks[] = []
    for (let i = 0; i < n; i++) {
      const o = i * 5
      const kindFork = words[o + 3]
      const kind = kindFork >>> 24
      const fork = kindFork & 0xffffff
      if (kind === 0) {
        pins.push({
          spc: words[o],
          db: words[o + 1],
          rel: words[o + 2],
          fork,
          blk: words[o + 4],
        })
      } else {
        nblocks.push({
          spc: words[o],
          db: words[o + 1],
          rel: words[o + 2],
          fork,
          nblocks: words[o + 4],
        })
      }
    }
    return {
      pins,
      nblocks,
      overflowed: mod._pgl_readset_overflowed() === 1,
    }
  }

  /**
   * Page LSN of one block at the current local state (rebase validation,
   * §4.2): pinned-buffer BufferGetLSNAtomic — never an executor path.
   * Returns 0n when the page is missing/truncated (caller 40001s).
   */
  pageLsn(
    spc: number,
    db: number,
    rel: number,
    fork: number,
    blk: number,
  ): bigint {
    return this.pg.Module._pgl_page_lsn(spc, db, rel, fork, blk)
  }

  /** Current nblocks of a relation fork (fresh smgr lseek); 0xFFFFFFFF =
   *  missing fork. */
  relationNblocks(spc: number, db: number, rel: number, fork: number): number {
    return this.pg.Module._pgl_relation_nblocks(spc, db, rel, fork)
  }

  /**
   * M7 W3: the live-apply pipeline as a cell METHOD, so session code is
   * mode-agnostic (WorkerCell runs the same pipeline inside its worker).
   * Async for surface parity; the work is synchronous here.
   */
  async applyLiveTail(start: bigint, end: bigint): Promise<LiveApplyResult> {
    return applyLiveTail(this.pg, this.dir, start, end)
  }

  /** M7 W3: classified WAL records of [start, end) (mode-agnostic — see
   *  applyLiveTail). */
  async walscanRange(start: bigint, end: bigint): Promise<WalRecord[]> {
    return walscanRange(this.pg, start, end)
  }

  /**
   * Detach: clean-close the instance (writes session-teardown WAL + a real
   * shutdown checkpoint) and return the final `(cursor .. checkPoint+120]`
   * bytes as the detach slice for the caller to publish (kind `sync`).
   * Null if the cursor already sits at the shutdown record's end.
   */
  async closeClean(): Promise<{ detachSlice: CapturedSlice | null }> {
    await this.pg.close()
    const control = readControl(this.dir)
    const end = shutdownCheckpointEnd(control.checkPoint)
    if (end === this.cursor) return { detachSlice: null }
    return {
      detachSlice: {
        baseLsn: this.cursor,
        endLsn: end,
        bytes: readWalRange(this.dir, this.cursor, end),
      },
    }
  }
}

async function currentInsertLsn(pg: PGlite): Promise<bigint> {
  const rows = (
    await pg.query<{ lsn: string }>(
      `select pg_current_wal_insert_lsn()::text as lsn`,
    )
  ).rows
  return parseLsn(rows[0].lsn)
}
