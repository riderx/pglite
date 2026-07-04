// HostSession — the programmatic session API (the M1d proxy will drive a
// lower-level variant of this). One session = one PGlite cell, read-attached
// by default, write-attached (sticky) after its first write. Enforces the
// §3.7 contract row by row:
//
//   one-shot, no session state, CAS loss      -> transparent re-execute
//   read-attached cell captures a write       -> discard + write-upgrade +
//                                                re-execute (one-shots) or
//                                                40001 (interactive COMMIT)
//   interactive txn loses at COMMIT           -> 40001, session survives
//   tainted session loses                     -> fatal session reset
//   read-only (empty slice)                   -> never CAS'd
//
// plus the §7 watermark gate (advance before executing when the cell's base
// is behind the host watermark; tainted sessions are PINNED and never
// advance) and the post-transaction taint probe.

import { randomUUID } from 'node:crypto'
import { Cell } from '@electric-sql/pglite-cell'
import type { Results } from '@electric-sql/pglite'
import type { CellDirLease } from './base-dir'
import type { DatabaseRuntime } from './database-runtime'
import {
  AdvanceRaceError,
  FatalSessionResetError,
  SerializationConflictError,
  SessionClosedError,
  SessionPinnedExpiredError,
} from './errors'

export type ExecOutcome =
  | 'committed'
  | 'read-only'
  | 'aborted'
  | 'in-transaction'

export interface ExecResult {
  /** Per-statement results from PGlite's exec. */
  results: Results[]
  /** Rows of the last statement that returned any (convenience). */
  rows: Record<string, unknown>[]
  outcome: ExecOutcome
  /** The stream offset the commit landed at (outcome `committed` only). */
  landedOffset?: string
}

interface Taints {
  tempSchema: boolean
  holdableCursors: boolean
  advisoryLocks: boolean
}

/** Last non-empty statement is `ROLLBACK` / `ABORT` (not `ROLLBACK TO`). */
function endsWithRollback(sql: string): boolean {
  const statements = sql
    .split(';')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
  if (statements.length === 0) return false
  const last = statements[statements.length - 1].toLowerCase()
  if (last.startsWith('rollback to')) return false
  return last.startsWith('rollback') || last.startsWith('abort')
}

export class HostSession {
  readonly id = `s-${randomUUID()}`

  private cell: Cell | null = null
  private lease: CellDirLease | null = null
  private mode: 'read' | 'write' = 'read' // sessions start read-attached
  /** Stream position this session serves / last landed at. */
  private streamPos: { offset: string; lsn: bigint } = { offset: '', lsn: 0n }
  private taints: Taints = {
    tempSchema: false,
    holdableCursors: false,
    advisoryLocks: false,
  }
  private _tainted = false // latches; the gc-pin is appended once
  private pinExpiresAt = 0
  private dead: string | null = null
  private chain: Promise<unknown> = Promise.resolve()

  constructor(private readonly runtime: DatabaseRuntime) {}

  get tainted(): boolean {
    return this._tainted
  }

  get attachMode(): 'read' | 'write' {
    return this.mode
  }

  get closed(): boolean {
    return this.dead !== null
  }

  /** Idle = no open cell mid-transaction (hibernation eligibility). */
  isIdle(): boolean {
    return this.cell === null || !this.cell.db.isInTransaction()
  }

  /** Serialize public operations per session. */
  private run<T>(fn: () => Promise<T>): Promise<T> {
    const p = this.chain.then(fn)
    this.chain = p.then(
      () => undefined,
      () => undefined,
    )
    return p
  }

  /**
   * Execute SQL (a statement, a batch, or one step of an interactive
   * transaction). See the module header for the contract enforced here.
   */
  exec(sql: string): Promise<ExecResult> {
    return this.run(() => this.execInner(sql))
  }

  private async execInner(sql: string): Promise<ExecResult> {
    if (this.dead !== null) throw new SessionClosedError(this.dead)
    if (this._tainted && Date.now() > this.pinExpiresAt) {
      await this.destroyCell()
      this.dead = 'gc-pin expired'
      this.runtime.removeSession(this)
      throw new SessionPinnedExpiredError(this.runtime.opts.pinTtlMs)
    }
    await this.runtime.ensureActive()
    this.runtime.touch()

    let attempt = 0
    for (;;) {
      await this.ensureCell()
      const cell = this.cell
      if (cell === null) throw new SessionClosedError('cell attach failed')
      const wasInTxn = cell.db.isInTransaction()

      let execErr: unknown
      let results: Results[] = []
      try {
        results = await cell.db.exec(sql)
      } catch (err) {
        execErr = err
      }

      if (cell.db.isInTransaction()) {
        // Mid interactive transaction (possibly in aborted state after an
        // error): no capture, no probes — everything rides txn end.
        if (execErr !== undefined) throw execErr
        return { results, rows: lastRows(results), outcome: 'in-transaction' }
      }

      // ---- transaction over ----

      if (execErr !== undefined || endsWithRollback(sql)) {
        // Aborted: probe sequence floors FIRST (the abort-only nextval
        // hazard, §5.3 rule 7 — the only case where a drawn value was
        // observed with no slice ever publishing it).
        await this.runtime.probeFloors(cell)
        if (this.mode === 'read') {
          // Aborted transactions still write WAL (heap changes + abort
          // record, non-transactional sequence records). On a read cell
          // those bytes can never publish and would masquerade as a write
          // on the next capture — swallow them locally by advancing the
          // (local-only) cursor past them. The floors probe above covers
          // any observed nextval draw; the bytes themselves are rolled-back
          // noise on a dir that never publishes.
          const stray = await cell.captureSlice()
          if (stray !== null) cell.confirmPublished(stray.endLsn)
        }
        // Write cells: stray abort WAL stays local and rides the next
        // slice (capture-cursor invariant: contiguous, unfiltered).
        await this.probeTaints(cell)
        if (execErr !== undefined) throw execErr
        return { results, rows: lastRows(results), outcome: 'aborted' }
      }

      const slice = await cell.captureSlice()

      if (slice === null) {
        // Read-only: never CAS'd; response is immediately final.
        await this.probeTaints(cell)
        return { results, rows: lastRows(results), outcome: 'read-only' }
      }

      if (this.mode === 'read') {
        // A nonempty capture on a read-attached cell is the write-upgrade
        // trigger — publishing is mechanically forbidden (the cell's WAL
        // position diverges from the stream). The discarded execution's
        // sequence draws may have been observed (interactive mid-txn
        // output); floor them before the cell goes away.
        await this.runtime.probeFloors(cell)
        if (this._tainted) {
          // A tainted session's state (cursors, temp tables, locks) lives
          // in this cell and cannot survive the discard the upgrade needs:
          // fatal session reset, never a silent continuation (§3.3).
          await this.destroyCell()
          this.dead = 'tainted session wrote on a read-attached cell'
          this.runtime.removeSession(this)
          throw new FatalSessionResetError(this.taintNames())
        }
        await this.destroyCell()
        this.mode = 'write' // sticky write intent from the first write
        if (!wasInTxn && attempt < 1) {
          attempt++
          continue // transparent re-execute on the upgraded cell
        }
        // Interactive transaction that first wrote on a read-attached cell:
        // 40001 at COMMIT. Attach the write cell eagerly so the client's
        // retry lands upgraded.
        await this.ensureCell()
        throw new SerializationConflictError(
          'transaction wrote on a read-attached cell; its slice cannot be published',
        )
      }

      // ---- write-attached: publish through the host sequencer ----
      const res = await this.runtime.commitFromSession({
        commitId: randomUUID(),
        kind: 'commit',
        baseLsn: slice.baseLsn,
        endLsn: slice.endLsn,
        bytes: slice.bytes,
      })

      if (res.landed) {
        cell.confirmPublished(slice.endLsn)
        this.streamPos = { offset: res.nextOffset, lsn: slice.endLsn }
        await this.probeTaints(cell)
        return {
          results,
          rows: lastRows(results),
          outcome: 'committed',
          landedOffset: res.offset,
        }
      }

      // ---- CAS loss (§3.7 contract) ----
      // The lost transaction is an abort in disguise: its WAL (including
      // non-transactional sequence records whose values may have been
      // observed) dies with the recycled cell. Floor the sequences first.
      await this.runtime.probeFloors(cell)
      if (this._tainted) {
        // The recycle destroys exactly the state re-execution would need:
        // fatal session reset, never a silent continuation (§3.3/§3.6).
        await this.destroyCell()
        this.dead = 'tainted session lost a commit race'
        this.runtime.removeSession(this)
        throw new FatalSessionResetError(this.taintNames())
      }
      await this.destroyCell() // recycle-to-head happens on next attach
      if (wasInTxn) {
        // Interactive COMMIT loss: 40001; the session survives and its
        // next statement attaches a fresh cell at head.
        throw new SerializationConflictError(
          'interactive transaction lost the commit race at COMMIT',
        )
      }
      attempt++
      if (attempt > this.runtime.opts.maxRetries) {
        throw new SerializationConflictError(
          `one-shot re-execution budget exhausted (${this.runtime.opts.maxRetries} retries)`,
        )
      }
      // One-shot, untainted, nothing acked: transparent re-execute (§3.3).
    }
  }

  /**
   * Attach a cell if absent, and enforce the §7 watermark gate: if the
   * session is between transactions and its cell's base is behind the host
   * watermark, advance first (read cells advance with ZERO stream appends;
   * write cells re-ensure the canonical position). Tainted sessions are
   * PINNED: they never advance (their pinned base is gc-pin leased).
   * A session's base never decreases (advances always target the head).
   */
  private async ensureCell(): Promise<void> {
    if (this.cell !== null) {
      const inTxn = this.cell.db.isInTransaction()
      if (
        inTxn ||
        this._tainted ||
        this.streamPos.lsn >= this.runtime.watermark.lsn
      ) {
        return
      }
      await this.destroyCell()
    }

    if (this.mode === 'read') {
      await this.runtime.baseDirs.ensureAtHeadLocal(this.runtime.tailer)
      const lease = await this.runtime.baseDirs.takeCellDir('read')
      this.cell = await Cell.open(lease.dir, {
        // Read-attach: the cell's insert position is the dir's own LOCAL
        // clean head (past the stream head by the unpublished boot
        // records); its capture cursor tracks local position.
        expectedHeadLsn: lease.base.localHeadLsn,
      })
      this.lease = lease
      this.streamPos = { offset: lease.base.offset, lsn: lease.base.lsn }
      return
    }

    // Write-attach: canonical ensure + floors BEFORE the session runs
    // anything. A floors publish that loses its CAS restarts the attach.
    for (let i = 0; i < this.runtime.opts.attachAttempts; i++) {
      await this.runtime.baseDirs.ensureAtHeadCanonical(
        this.runtime.tailer,
        this.runtime.committer,
      )
      const lease = await this.runtime.baseDirs.takeCellDir('write')
      const cell = await Cell.open(lease.dir, {
        expectedHeadLsn: lease.base.localHeadLsn,
      })
      const floors = await this.runtime.applyFloors(cell)
      if (floors.lost) {
        await cell.db.close().catch(() => undefined)
        this.runtime.baseDirs.releaseCellDir(lease.dir)
        continue
      }
      this.cell = cell
      this.lease = lease
      this.streamPos = floors.pos ?? {
        offset: lease.base.offset,
        lsn: lease.base.lsn,
      }
      return
    }
    throw new AdvanceRaceError(this.runtime.opts.attachAttempts)
  }

  /**
   * One post-transaction catalog probe for session-state taints (§3.3):
   * temp schema, holdable cursors, session advisory locks. Tainting
   * latches; the gc-pin L frame is appended once, at the transition.
   */
  private async probeTaints(cell: Cell): Promise<void> {
    const row = (
      await cell.db.query<{ temp: boolean; cur: number; adv: number }>(
        `select pg_my_temp_schema()::oid <> 0 as temp,
                (select count(*)::int from pg_cursors where is_holdable) as cur,
                (select count(*)::int from pg_locks where locktype = 'advisory') as adv`,
      )
    ).rows[0]
    this.taints = {
      tempSchema: row.temp,
      holdableCursors: row.cur > 0,
      advisoryLocks: row.adv > 0,
    }
    const any = row.temp || row.cur > 0 || row.adv > 0
    if (any && !this._tainted) {
      this._tainted = true
      this.pinExpiresAt = Date.now() + this.runtime.opts.pinTtlMs
      await this.runtime.appendGcPin(this.id, this.streamPos)
    }
  }

  private taintNames(): string[] {
    const names: string[] = []
    if (this.taints.tempSchema) names.push('temp tables')
    if (this.taints.holdableCursors) names.push('holdable cursors')
    if (this.taints.advisoryLocks) names.push('advisory locks')
    return names.length > 0 ? names : ['session-local state']
  }

  private async destroyCell(): Promise<void> {
    const cell = this.cell
    const lease = this.lease
    this.cell = null
    this.lease = null
    if (cell) await cell.db.close().catch(() => undefined)
    if (lease) this.runtime.baseDirs.releaseCellDir(lease.dir)
  }

  /**
   * Close the session. An idle untainted read cell (or a stale write cell)
   * recycles silently; a canonical-at-head write cell close-cleans and
   * publishes the detach slice so the stream tail ends in a shutdown
   * record (making the next attach's materialize a no-op).
   */
  close(): Promise<void> {
    return this.run(async () => {
      if (this.dead !== null) return
      await this.detachCell()
      this.dead = 'closed'
      this.runtime.removeSession(this)
    })
  }

  /**
   * Hibernation path (runtime-driven). Idle untainted sessions survive
   * (their next exec re-attaches after wake); mid-transaction or tainted
   * sessions lose unreplayable state and are fatally reset.
   */
  _hibernateCell(): Promise<void> {
    return this.run(async () => {
      if (this.dead !== null) return
      const inTxn = this.cell !== null && this.cell.db.isInTransaction()
      if (inTxn || this._tainted) {
        await this.destroyCell()
        this.dead = this._tainted
          ? 'hibernated while pinned (tainted session state cannot survive)'
          : 'hibernated mid-transaction'
        this.runtime.removeSession(this)
        return
      }
      await this.detachCell()
    })
  }

  /** Shared close/hibernate cell teardown (session must be idle). */
  private async detachCell(): Promise<void> {
    const cell = this.cell
    const lease = this.lease
    if (cell === null) return
    const canonicalAtHead =
      this.mode === 'write' &&
      !this._tainted &&
      !cell.db.isInTransaction() &&
      cell.captureCursor === this.runtime.tailer.head.lsn
    if (!canonicalAtHead) {
      await this.destroyCell()
      return
    }
    this.cell = null
    this.lease = null
    const { detachSlice } = await cell.closeClean()
    if (detachSlice !== null) {
      // Best-effort: a CAS loss here just means the teardown WAL stays
      // local (it never carried user data past the last landed commit).
      const res = await this.runtime.commitFromSession({
        commitId: randomUUID(),
        kind: 'sync',
        baseLsn: detachSlice.baseLsn,
        endLsn: detachSlice.endLsn,
        bytes: detachSlice.bytes,
      })
      if (res.landed) {
        this.streamPos = { offset: res.nextOffset, lsn: detachSlice.endLsn }
      }
    }
    if (lease) this.runtime.baseDirs.releaseCellDir(lease.dir)
  }
}

function lastRows(results: Results[]): Record<string, unknown>[] {
  for (let i = results.length - 1; i >= 0; i--) {
    if (results[i].rows.length > 0) {
      return results[i].rows as Record<string, unknown>[]
    }
  }
  return []
}
