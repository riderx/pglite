// HostSession — the programmatic session API and the protocol-level unit
// executor the M1d proxy drives. One session = one PGlite cell, read-attached
// by default, write-attached (sticky) after its first write. Enforces the
// §3.7 contract row by row:
//
//   one-shot, no session state, CAS loss      -> transparent re-execute
//   read-attached cell captures a write       -> discard + write-upgrade +
//                                                re-execute (one-shots) or
//                                                40001 (interactive COMMIT)
//   interactive txn loses at COMMIT           -> M5d: transparent REBASE
//                                                (harvest → advance to K →
//                                                validate §4.2 → re-apply
//                                                §4.4 → CAS, ≤2 rounds);
//                                                any gate failure -> 40001,
//                                                session survives
//   interactive loss w/ rebase taint (§4.5)   -> 40001 (ctid/xmin/... scan,
//                                                temp-write-during-attempt,
//                                                ring overflow, DDL in txn)
//   tainted session loses                     -> M5e: in-place reset +
//                                                advance (temp content,
//                                                held cursors, advisory
//                                                locks all survive; the
//                                                deferred ON COMMIT
//                                                truncate never ran);
//                                                fatal reset ONLY when
//                                                the reset is unsound
//                                                (recycle fallback)
//   read-only (empty slice)                   -> never CAS'd
//
// plus the §7 watermark gate (advance before executing when the cell's base
// is behind the host watermark; tainted sessions are PINNED and never
// advance) and the post-transaction taint probe.
//
// The contract state machine lives ONCE, in `drive()`; `exec()` (SQL-level,
// results parsed by PGlite) and `execUnit()` (wire-level, §3.5 response
// buffering — bytes in, buffered bytes + a flush disposition out) are thin
// runners over it.

import { randomUUID } from 'node:crypto'
import {
  Cell,
  applyLiveTail,
  formatLsn,
  walscanRange,
  writeWalRange,
} from '@electric-sql/pglite-cell'
import type { CapturedSlice, WalRecord } from '@electric-sql/pglite-cell'
import {
  harvestRebasePlan,
  reapplyPlan,
  scanRebaseTaint,
  validateAtK,
} from './rebase'
import type { Results } from '@electric-sql/pglite'
import { serialize } from '@electric-sql/pg-protocol'
import type { CellDirLease } from './base-dir'
import type { DatabaseRuntime, DeliveredNotification } from './database-runtime'
import {
  AdvanceRaceError,
  FatalSessionResetError,
  PinnedWriteError,
  SerializationConflictError,
  SessionClosedError,
  SessionPinnedExpiredError,
} from './errors'
import {
  concatBytes,
  extractNotificationResponses,
  scanBackendOutput,
} from './proxy/wire'
import type { BackendScan } from './proxy/wire'

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
  /**
   * The commit's end LSN as pg_lsn text (outcome `committed` only) — the
   * cross-host session token (§7 M4): carry it to any other host and
   * `waitForLsn(parseLsn(token))` there before reading.
   */
  landedLsn?: string
}

/**
 * One protocol-level unit of work: a simple 'Q' message, an
 * extended-protocol batch closed by Sync, or the connection's
 * StartupMessage. The unit's bytes are re-executable as-is (§3.3: the
 * client observed nothing until the unit's disposition is known).
 */
export interface ProtocolUnit {
  bytes: Uint8Array
  /** Simple-protocol SQL text (SET tracking / diagnostics). */
  sqlForReplay?: string
  kind: 'simple' | 'extended' | 'startup'
}

export type UnitDisposition =
  | 'flushed-readonly' // empty slice: never CAS'd, response final (§3.5)
  | 'landed' // commit CAS'd and landed: buffered response is now true
  | 'held-conflict' // unrecoverable loss: output DISCARDED, proxy sends 40001
  | 'held-pinned' // pinned-mode write: output DISCARDED, proxy sends 0A000
  | 'mid-txn' // interactive transaction in progress: streams by design
  | 'aborted' // transaction aborted (error / ROLLBACK): nothing to publish

/**
 * Session freshness mode (§7 / M3): changes ONLY the gate step before an
 * idle-state unit. Set via the proxy-intercepted `SET pglite.freshness`
 * (never forwarded to the cell) or programmatically.
 */
export type Freshness =
  | { mode: 'session' }
  | { mode: 'linearizable' }
  | { mode: 'local' }
  | { mode: 'pinned'; lsn: bigint }
  | { mode: 'bounded-stale'; ms: number }

export interface UnitResult {
  /** The (possibly re-executed) buffered backend response to flush. */
  output: Uint8Array
  /** Trailing ReadyForQuery status of `output`. */
  rfqStatus: 'I' | 'T' | 'E'
  disposition: UnitDisposition
}

/** TEST HOOK event: one raw execution attempt / the final disposition. */
export interface UnitObservation {
  phase: 'attempt' | 'result'
  unitKind: ProtocolUnit['kind']
  attempt: number
  outputBytes: number
  disposition?: UnitDisposition
}

interface Taints {
  tempSchema: boolean
  holdableCursors: boolean
  advisoryLocks: boolean
}

/** What one execution attempt produced, as the shared driver sees it. */
interface AttemptOutcome<T> {
  payload: T
  /** Error thrown by the runner (SQL-level exec); rethrown by the caller. */
  threw?: unknown
  /** The transaction this unit ended ABORTED (only read at txn end). */
  aborted: boolean
  /**
   * The attempt died on the native `sequence lease exhausted` error (M5a,
   * §5.3 rule 1) — the reactive grant-renewal signal. Set from the thrown
   * PGlite error (SQL path) or the scanned wire ErrorResponse (unit path).
   */
  leaseExhausted?: boolean
}

/** The shared state machine's classification of a finished unit. */
type DriveResult<T> =
  | { kind: 'mid-txn'; payload: T; threw?: unknown }
  | { kind: 'aborted'; payload: T; threw?: unknown }
  | { kind: 'read-only'; payload: T }
  | { kind: 'landed'; payload: T; landedOffset: string; landedLsn: bigint }
  | { kind: 'conflict'; detail: string }
  | { kind: 'pinned-write' }

/** `LISTEN ch` / `UNLISTEN ch` / `UNLISTEN *` as a lone simple statement. */
function classifyListen(
  sql: string,
):
  | { op: 'listen' | 'unlisten'; channel: string }
  | { op: 'unlisten-all' }
  | null {
  const m =
    /^\s*(listen|unlisten)\s+(?:"([^"]+)"|(\*)|([a-zA-Z_][\w$]*))\s*;?\s*$/i.exec(
      sql,
    )
  if (!m) return null
  const op = m[1].toLowerCase() as 'listen' | 'unlisten'
  if (m[3] === '*') return op === 'unlisten' ? { op: 'unlisten-all' } : null
  const channel = m[2] ?? m[4].toLowerCase()
  return { op, channel }
}

/** Last non-empty statement is `ROLLBACK` / `ABORT` (not `ROLLBACK TO`). */
/**
 * True iff `threw` is the native sequence-lease-exhaustion error (M5a,
 * §5.3 rule 1): ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED decorated with
 * the errdetail the clamp attaches when the LEASE (not the catalog
 * MAXVALUE) capped the sequence. The renew signal for one-shot units.
 */
function isSequenceLeaseExhausted(threw: unknown): boolean {
  return (
    typeof threw === 'object' &&
    threw !== null &&
    (threw as { detail?: string }).detail === 'sequence lease exhausted'
  )
}

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

/**
 * Session-level `SET`s are replayed on recycle; transaction-scoped ones
 * (`SET TRANSACTION`, `SET LOCAL`) are not session state.
 */
function isReplayableSet(sql: string): boolean {
  return /^\s*set\b/i.test(sql) && !/^\s*set\s+(transaction|local)\b/i.test(sql)
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
  /**
   * Holdable cursor names as of the last completed transaction (M5e):
   * a lost transaction's WITH HOLD cursors materialize at LOCAL commit
   * and survive the in-place reset (they are memory tuplestores), but a
   * reversed commit must not leave them behind — vanilla's failed
   * COMMIT drops them. Compared against pg_cursors on every surviving
   * loss; strangers are CLOSEd.
   */
  private holdableNames = new Set<string>()
  private _tainted = false // latches; the gc-pin is appended once
  private pinExpiresAt = 0
  private dead: string | null = null
  private chain: Promise<unknown> = Promise.resolve()

  /**
   * Session-state replay on cell recycle (§3.5, M1 subset): the
   * connection's StartupMessage bytes + tracked session-level SET
   * statements, re-run on every fresh cell with output discarded.
   * Prepared statements are NOT replayed — a conflict recycle loses them
   * (documented M1 limitation, pooler-grade replay later).
   */
  private startupBytes: Uint8Array | null = null
  private setStatements: string[] = []

  /** Channels this session LISTENs (host registry + delivery filter, M3). */
  private readonly _listenSet = new Set<string>()
  /** Freshness mode (§7): changes only the idle-unit gate step. */
  private freshness: Freshness = { mode: 'session' }
  /** Wall-clock of the last cell attach/advance (bounded-stale gate). */
  private lastAdvanceAt = 0
  /** Notifications harvested during the CURRENT execution attempt (M3):
   *  cleared per attempt, so a lost CAS discards them with the attempt. */
  private notifBuffer: { channel: string; payload: string }[] = []
  /** The runtime listen-union version applied to the current cell. */
  private cellListenVersion = -1
  /**
   * M5d rebase taint (§4.5): set when any statement text of the CURRENT
   * transaction observed ctid/xmin/cmin/cmax/txid (statement-text scan —
   * documented v1 approximation). Cleared at every transaction start.
   */
  private rebaseTaint: string | null = null
  /** Statement text of the unit currently driving (taint scan input). */
  private currentUnitText: string | null = null
  /** Cumulative rebase counters (TEST HOOK / diagnostics). */
  readonly rebaseStats = { attempts: 0, landed: 0, failed: 0 }

  /** TEST HOOK (§16 client-observation property): unit execution events. */
  _unitObserver: ((ev: UnitObservation) => void) | null = null

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

  /** Channels this session currently LISTENs. */
  get listenChannels(): ReadonlySet<string> {
    return this._listenSet
  }

  /** The session's freshness mode (§7). */
  get freshnessMode(): Freshness {
    return this.freshness
  }

  /**
   * Set the freshness mode (§7). Applied at the next idle-unit gate; set
   * by the proxy's `SET pglite.freshness` interception (never forwarded
   * to the cell) or programmatically.
   */
  setFreshness(freshness: Freshness): void {
    this.freshness = freshness
  }

  /**
   * Tailer-driven notification delivery for THIS session (M3, §10.2):
   * `cb` fires, in stream order == global commit order, for every N frame
   * on a channel this session LISTENs at delivery time — the committing
   * session's own connection included. Returns the unsubscribe fn.
   */
  subscribeNotifications(cb: (n: DeliveredNotification) => void): () => void {
    return this.runtime.subscribeNotifications((n) => {
      if (this._listenSet.has(n.channel)) cb(n)
    })
  }

  /**
   * Cross-host read-your-writes (§7 M4): block until this session's HOST
   * has ingested the stream up to `lsn` (a `landedLsn` token from a
   * commit acked anywhere), raising the host watermark so the session's
   * next statement (in `session` freshness) serves at or past it.
   */
  waitForLsn(lsn: bigint, timeoutMs?: number): Promise<void> {
    return this.runtime.waitForLsn(lsn, timeoutMs)
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
    this.currentUnitText = sql
    const r = await this.drive<Results[]>(async (cell) => {
      let threw: unknown
      let results: Results[] = []
      try {
        results = await cell.db.exec(sql)
      } catch (err) {
        threw = err
      }
      return {
        payload: results,
        threw,
        aborted: threw !== undefined || endsWithRollback(sql),
        leaseExhausted: isSequenceLeaseExhausted(threw),
      }
    })

    switch (r.kind) {
      case 'mid-txn':
        // Mid interactive transaction (possibly in aborted state after an
        // error): no capture, no probes — everything rides txn end.
        if (r.threw !== undefined) throw r.threw
        return {
          results: r.payload,
          rows: lastRows(r.payload),
          outcome: 'in-transaction',
        }
      case 'aborted':
        if (r.threw !== undefined) throw r.threw
        return {
          results: r.payload,
          rows: lastRows(r.payload),
          outcome: 'aborted',
        }
      case 'read-only':
        return {
          results: r.payload,
          rows: lastRows(r.payload),
          outcome: 'read-only',
        }
      case 'landed':
        return {
          results: r.payload,
          rows: lastRows(r.payload),
          outcome: 'committed',
          landedOffset: r.landedOffset,
          landedLsn: formatLsn(r.landedLsn),
        }
      case 'conflict':
        throw new SerializationConflictError(r.detail)
      case 'pinned-write':
        throw new PinnedWriteError()
    }
  }

  /**
   * Execute one protocol-level unit (§3.5 response buffering): run the raw
   * frontend bytes on the cell, collect the raw backend response, and
   * resolve the §3.7 contract at byte level WITHOUT sending anything —
   * the returned disposition tells the proxy what to flush:
   *
   *   flushed-readonly / landed -> flush `output` (possibly a re-execution's)
   *   mid-txn / aborted         -> flush `output` (streams by design; only
   *                                the COMMIT response is ever held)
   *   held-conflict             -> `output` is EMPTY (the §3.5 invariant:
   *                                the buffer died unsent); the proxy
   *                                synthesizes the §4.0 40001 + ReadyForQuery
   *
   * One-shot CAS losses re-execute the same unit bytes INSIDE this call
   * (recycle + replay + re-run), so the proxy only ever sees the final
   * output. `FatalSessionResetError` propagates (§3.3): the proxy sends the
   * error and terminates the connection.
   */
  execUnit(unit: ProtocolUnit): Promise<UnitResult> {
    return this.run(() => this.execUnitInner(unit))
  }

  private async execUnitInner(unit: ProtocolUnit): Promise<UnitResult> {
    // Taint-scan input (§4.5): simple-protocol SQL when known; extended
    // protocol falls back to a raw byte decode (Parse messages carry the
    // SQL text — the scan only needs substrings).
    this.currentUnitText =
      unit.sqlForReplay ?? Buffer.from(unit.bytes).toString('latin1')
    let attempt = 0
    const r = await this.drive<{ output: Uint8Array; scan: BackendScan }>(
      async (cell) => {
        const chunks: Uint8Array[] = []
        let threw: unknown
        try {
          await cell.db.runExclusive(() =>
            cell.db.execProtocolRawStream(unit.bytes, {
              onRawData: (data) => {
                chunks.push(data.slice())
              },
            }),
          )
        } catch (err) {
          threw = err
        }
        // Uniform delivery (M3, §10.2 step 4): raw 'A' NotificationResponse
        // bytes are STRIPPED from unit output — all client-facing delivery
        // is tailer-driven, so nothing ever arrives twice and every
        // listener (committer included) hears the same global order. The
        // same walk IS the harvest: PGlite's raw-stream exec bypasses its
        // parser, so the 'A' bytes here are the only place the local
        // commit's notifications exist.
        const { stripped: output, notifications } =
          extractNotificationResponses(concatBytes(chunks))
        this.notifBuffer.push(...notifications)
        const scan = scanBackendOutput(output)
        this._unitObserver?.({
          phase: 'attempt',
          unitKind: unit.kind,
          attempt: attempt++,
          outputBytes: output.length,
        })
        return {
          payload: { output, scan },
          threw,
          // A trailing ROLLBACK tag at txn end is an abort in disguise
          // (M1c finding) — floors must be probed, nothing published.
          aborted:
            threw !== undefined ||
            scan.hasError ||
            scan.lastCommandTag === 'ROLLBACK',
          // Wire-level units don't throw on SQL errors — the ErrorResponse
          // rides the output; the scan carries the lease-renewal signal.
          leaseExhausted:
            isSequenceLeaseExhausted(threw) ||
            scan.errorDetail === 'sequence lease exhausted',
        }
      },
    )

    let result: UnitResult
    switch (r.kind) {
      case 'mid-txn':
        if (r.threw !== undefined) throw r.threw
        result = {
          output: r.payload.output,
          rfqStatus: r.payload.scan.rfqStatus ?? 'T',
          disposition: 'mid-txn',
        }
        break
      case 'aborted':
        if (r.threw !== undefined) throw r.threw
        result = {
          output: r.payload.output,
          rfqStatus: r.payload.scan.rfqStatus ?? 'I',
          disposition: 'aborted',
        }
        break
      case 'read-only':
        this.trackReplayState(unit)
        result = {
          output: r.payload.output,
          rfqStatus: r.payload.scan.rfqStatus ?? 'I',
          disposition: 'flushed-readonly',
        }
        break
      case 'landed':
        this.trackReplayState(unit)
        result = {
          output: r.payload.output,
          rfqStatus: r.payload.scan.rfqStatus ?? 'I',
          disposition: 'landed',
        }
        break
      case 'conflict':
        // The buffered output dies here, unsent — the §3.5 invariant. The
        // proxy synthesizes the §4.0 error in its place.
        result = {
          output: new Uint8Array(0),
          rfqStatus: 'I',
          disposition: 'held-conflict',
        }
        break
      case 'pinned-write':
        // Pinned freshness (§7): the write can never publish; the buffered
        // output dies unsent and the proxy synthesizes the 0A000 error.
        result = {
          output: new Uint8Array(0),
          rfqStatus: 'I',
          disposition: 'held-pinned',
        }
        break
    }
    this._unitObserver?.({
      phase: 'result',
      unitKind: unit.kind,
      attempt,
      outputBytes: result.output.length,
      disposition: result.disposition,
    })
    return result
  }

  /** Record replayable session state from a successfully finished unit. */
  private trackReplayState(unit: ProtocolUnit): void {
    if (unit.kind === 'startup') {
      this.startupBytes = unit.bytes.slice()
      return
    }
    if (unit.kind !== 'simple' || unit.sqlForReplay === undefined) return
    if (isReplayableSet(unit.sqlForReplay)) {
      this.setStatements.push(unit.sqlForReplay)
    }
    // LISTEN registry (M3, §10.2 step 1): classification happens only on a
    // SUCCESSFULLY finished unit (forwarded to the cell as normal; also
    // recorded here). Simple protocol only — extended-protocol
    // LISTEN/UNLISTEN is a documented M3 gap.
    const listen = classifyListen(unit.sqlForReplay)
    if (listen === null) return
    if (listen.op === 'unlisten-all') {
      for (const channel of [...this._listenSet]) {
        this._listenSet.delete(channel)
        this.runtime.releaseListen(channel)
      }
      return
    }
    if (listen.op === 'listen') {
      if (!this._listenSet.has(listen.channel)) {
        this._listenSet.add(listen.channel)
        this.runtime.acquireListen(listen.channel)
      }
    } else if (this._listenSet.has(listen.channel)) {
      this._listenSet.delete(listen.channel)
      this.runtime.releaseListen(listen.channel)
    }
  }

  /**
   * THE §3.7 contract state machine, shared by `exec` and `execUnit`. Runs
   * `runner` on an attached cell and resolves the transaction outcome:
   * watermark gate before idle units, capture at txn end, read-cell
   * write-upgrade, publish through the host sequencer, floors probe on
   * abort/discard, taint probe, transparent re-execution of one-shots
   * (bounded), fatal reset for tainted losses.
   */
  private async drive<T>(
    runner: (cell: Cell) => Promise<AttemptOutcome<T>>,
  ): Promise<DriveResult<T>> {
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
    let leaseRenewals = 0
    for (;;) {
      await this.ensureCell()
      const cell = this.cell
      if (cell === null) throw new SessionClosedError('cell attach failed')
      const wasInTxn = cell.db.isInTransaction()

      // M5c: refresh the in-place-reset base snapshot whenever we start a
      // unit between transactions (cheap; keeps the last cursor-anchored
      // snapshot when unpublished local WAL is pending).
      if (!wasInTxn) cell.maybeSnapshotBase({ flush: this.mode === 'write' })

      // M5d read-set capture (§4.1): (re)arm the native ring at every
      // between-transactions unit start, so an interactive transaction's
      // whole read set — BEGIN unit through COMMIT unit — is in the ring
      // when a CAS loss reaches the rebase ladder. One flag write + ring
      // reset; enabled for one-shots too (harmless, unused).
      if (!wasInTxn) {
        this.rebaseTaint = null
        cell.readSetBegin()
      }
      // Rebase taint (§4.5): statement-text scan, accumulated per txn.
      if (this.rebaseTaint === null && this.currentUnitText !== null) {
        this.rebaseTaint = scanRebaseTaint(this.currentUnitText)
      }

      // Cell auto-LISTEN delta (M3, §10.2 step 2): when the host LISTEN
      // union changed since this cell last synced, re-apply it between
      // units (never mid-transaction). Rides the session's own unit queue
      // (drive() is serialized per session) under runExclusive. LISTEN
      // writes no WAL (backend-local pg_listening_channels state; probed
      // in tests), so read cells stay publish-clean.
      if (!wasInTxn && this.cellListenVersion !== this.runtime.listenVersion) {
        await this.applyListenUnion(cell)
      }

      // Harvest window (M3): notifications fired during THIS attempt only.
      // Local commit precedes capture, so they are in hand before
      // commitSlice; a lost attempt's harvest dies with the attempt.
      this.notifBuffer = []

      const { payload, threw, aborted, leaseExhausted } = await runner(cell)
      const notifications = this.notifBuffer

      if (cell.db.isInTransaction()) {
        if (
          !wasInTxn &&
          !this._tainted &&
          leaseExhausted === true &&
          leaseRenewals < this.runtime.opts.maxRetries
        ) {
          // A SELF-CONTAINED unit (it began outside a transaction) opened
          // a transaction and died on native lease exhaustion inside it,
          // leaving the cell mid-aborted-txn. Nothing was acked to the
          // client: roll it back, renew, and re-execute the whole unit —
          // one-shot semantics. Truly interactive continuations (wasInTxn)
          // still surface the error.
          await cell.db.exec('rollback').catch(() => undefined)
          await this.runtime.probeFloors(cell)
          if (this.mode === 'read') {
            const stray = await cell.captureSlice()
            if (stray !== null) cell.confirmPublished(stray.endLsn)
          } else {
            await cell.db.exec('checkpoint') // flush abort-tail WAL (M4 finding)
          }
          await this.runtime.extendGrantsForRetry(cell, leaseRenewals)
          leaseRenewals++
          continue
        }
        // Mid interactive transaction (possibly in aborted state after an
        // error): no capture, no probes — everything rides txn end.
        return { kind: 'mid-txn', payload, threw }
      }

      // ---- transaction over ----

      if (threw !== undefined || aborted) {
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
        if (this.mode === 'write') {
          // Write cells: stray abort WAL stays local and rides the next
          // slice (capture-cursor invariant: contiguous, unfiltered).
          // M4 FINDING: an aborted transaction's tail (the abort record)
          // is NOT synchronously flushed to pg_wal — a later capture of
          // the stray range could read a torn tail and publish it, and
          // every consumer's materialize then stops right before the
          // unflushed record ("recovery did not replay to the head").
          // Force the WAL to disk: an online CHECKPOINT flushes through
          // the abort record; its own record joins the stray range and
          // rides the next slice like any other WAL.
          await cell.db.exec('checkpoint')
        }
        await this.probeTaints(cell)
        if (
          !wasInTxn &&
          !this._tainted &&
          leaseExhausted === true &&
          leaseRenewals < this.runtime.opts.maxRetries
        ) {
          // Native lease exhaustion mid-unit (M5a): the 50% machinery is
          // also reactive — the floors probe above already renewed the
          // grant; escalate headroom (a re-executed batch redraws all its
          // values) and transparently re-execute the one-shot. Interactive
          // transactions never land here (wasInTxn / mid-txn return): the
          // error surfaces to the client.
          await this.runtime.extendGrantsForRetry(cell, leaseRenewals)
          leaseRenewals++
          continue
        }
        return { kind: 'aborted', payload, threw }
      }

      const slice = await cell.captureSlice()

      if (slice === null) {
        // Read-only: never CAS'd; response is immediately final. A pure
        // NOTIFY commit lands here (notifications write no WAL): its
        // harvested notifications are CAS-appended as N frames ALONE so
        // the tailer fanout still distributes them (M4 fix of the M3
        // NOTIFY-only gap).
        if (notifications.length > 0) {
          await this.runtime.publishNotificationOnlyCommit(notifications)
        }
        // A temp-only commit with an empty slice still ran the gated
        // pre-commit sequence: its verdict is trivially "landed" (M5e).
        if (await this.finishCommitGate(cell)) {
          await this.probeTaints(cell)
        }
        return { kind: 'read-only', payload }
      }

      if (this.freshness.mode === 'pinned') {
        // Pinned sessions never advance and can never publish (§7): a
        // nonempty capture is a clean, typed rejection — not a conflict.
        // The cell committed locally, so its state diverged from the pin;
        // destroy it (the floors probe covers observed sequence draws).
        // The next attach serves the then-current head — the fixed-base
        // guarantee holds only until a rejected write (documented M3
        // recycle-based limitation).
        await this.runtime.probeFloors(cell)
        await this.destroyCell()
        return { kind: 'pinned-write' }
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
        return {
          kind: 'conflict',
          detail:
            'transaction wrote on a read-attached cell; its slice cannot be published',
        }
      }

      // ---- write-attached: publish through the host sequencer ----
      const res = await this.runtime.commitFromSession({
        commitId: randomUUID(),
        kind: 'commit',
        baseLsn: slice.baseLsn,
        endLsn: slice.endLsn,
        bytes: slice.bytes,
        // Harvested notifications ride the winning POST as N frames (M3,
        // §10.2 step 3) — atomic with the W frame by construction.
        notifications,
      })

      if (res.landed) {
        cell.confirmPublished(slice.endLsn)
        this.streamPos = { offset: res.nextOffset, lsn: slice.endLsn }
        // M5e commit gate: the CAS landed — NOW run the deferred
        // ON COMMIT DELETE ROWS truncates (§3.6 reorder, achieved).
        if (await this.finishCommitGate(cell)) {
          // Grant maintenance (M4, §5.3): committed draws are the probe
          // evidence that takes/renews this host's sequence grants.
          await this.runtime.probeGrants(cell)
          await this.probeTaints(cell)
        }
        return {
          kind: 'landed',
          payload,
          landedOffset: res.offset,
          landedLsn: slice.endLsn,
        }
      }

      // ---- CAS loss (§3.7 contract) ----
      // M5d transparent rebase (§4): an ELIGIBLE interactive COMMIT loss
      // (untainted session, no rebase taints, bounds available) runs the
      // rebase ladder — harvest, advance to K, validate, re-apply, CAS.
      // On success the unit LANDS: the client's buffered response (whose
      // RETURNING/mid-txn values are the harvested data by construction)
      // becomes true. Every failure inside the ladder degrades to the
      // pre-M5d contract: 40001, session survives.
      if (wasInTxn && !this._tainted) {
        const reb = await this.tryTransparentRebase(cell, slice, notifications)
        if (reb.landed) {
          return {
            kind: 'landed',
            payload,
            landedOffset: reb.offset,
            landedLsn: reb.lsn,
          }
        }
        return {
          kind: 'conflict',
          detail: `interactive transaction lost the commit race at COMMIT (rebase: ${reb.detail})`,
        }
      }
      // The lost transaction is an abort in disguise: its WAL (including
      // non-transactional sequence records whose values may have been
      // observed) dies with the recycled cell. Floor the sequences first.
      await this.runtime.probeFloors(cell)
      // M5e commit gate: a reversed commit's deferred truncates must
      // never run (§3.6) — belt and braces, the native reset discards too.
      cell.commitGateDiscard()
      if (this._tainted) {
        // M5e TAINT LIFT (§3.3, the §3.6 whole point): with the commit
        // gate deferring the temp truncate and pgl_flush_base covering
        // local buffers, an in-place reset restores the session's
        // PRE-ATTEMPT temp content exactly; holdable-cursor tuplestores
        // and advisory locks are memory state a reset never touches.
        // A tainted loss is therefore survivable whenever the reset is
        // sound — the fatal reset remains ONLY for the recycle fallback
        // (which destroys exactly that state).
        let survived = false
        if (cell.canResetInPlace()) {
          try {
            cell.resetToBase()
            // Advance to head if live-appliable; a rejected advance
            // (returns false, cell untouched) leaves the session PINNED
            // at base — the M1 pinned contract, still alive.
            await this.tryLiveAdvance()
            survived = this.cell !== null
          } catch (err) {
            if (err instanceof FatalSessionResetError) throw err
            survived = false
          }
        }
        if (!survived || this.cell === null) {
          await this.destroyCell()
          this.dead = 'tainted session lost a commit race (reset unsound)'
          this.runtime.removeSession(this)
          throw new FatalSessionResetError(this.taintNames())
        }
        // Vanilla failed-COMMIT semantics: WITH HOLD cursors held by the
        // reversed commit are dropped.
        await this.dropLostHoldables()
        if (wasInTxn) {
          return {
            kind: 'conflict',
            detail:
              'interactive transaction lost the commit race at COMMIT (session state preserved)',
          }
        }
        // One-shot on a surviving tainted session: nothing was acked and
        // the session state is intact — transparent re-execute, PROVIDED
        // the advance reached the head (the capture-cursor invariant).
        if (this.streamPos.lsn === this.runtime.tailer.head.lsn) {
          attempt++
          if (attempt > this.runtime.opts.maxRetries) {
            return {
              kind: 'conflict',
              detail: `one-shot re-execution budget exhausted (${this.runtime.opts.maxRetries} retries)`,
            }
          }
          continue
        }
        return {
          kind: 'conflict',
          detail:
            'tainted session lost the commit race and is pinned behind the head',
        }
      }
      // M5c in-place reset (§3.4/§5.1): discard the speculative state
      // without recycling when the soundness gate passes (base snapshot at
      // cursor + zero storage writes since). Belt and braces: ANY error
      // falls back to the recycle path.
      if (attempt === 0 && cell.canResetInPlace()) {
        try {
          cell.resetToBase()
          // Recycle semantics re-attach AT HEAD unconditionally; match
          // that by advancing the reset cell to the true head right away
          // (the watermark gate alone can lag non-commit winners like
          // checkpoint sync/K appends). Falls back to recycle inside.
          if (!(await this.tryLiveAdvance())) await this.destroyCell()
          // Repeat losses fall through to the recycle path (attempt > 0
          // above): the canonical re-attach publishes a sync slice, which
          // restores the M1 convergence pressure under hot contention.
        } catch {
          await this.destroyCell()
        }
      } else {
        await this.destroyCell() // recycle-to-head happens on next attach
      }
      // Vanilla failed-COMMIT semantics: drop WITH HOLD cursors the lost
      // transaction materialized (they survive an in-place reset; a
      // recycled cell has none). M5c latent-bug fix: without this, a
      // surviving reset cell re-executing `DECLARE .. WITH HOLD` would
      // hit "cursor already exists".
      await this.dropLostHoldables()
      if (wasInTxn) {
        // Interactive COMMIT loss: 40001; the session survives and its
        // next statement attaches a fresh cell at head.
        return {
          kind: 'conflict',
          detail: 'interactive transaction lost the commit race at COMMIT',
        }
      }
      attempt++
      if (attempt > this.runtime.opts.maxRetries) {
        return {
          kind: 'conflict',
          detail: `one-shot re-execution budget exhausted (${this.runtime.opts.maxRetries} retries)`,
        }
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
   * Every freshly attached cell gets the session-state replay (startup
   * bytes + tracked SETs) before it serves anything.
   */
  private async ensureCell(): Promise<void> {
    if (this.cell !== null) {
      const inTxn = this.cell.db.isInTransaction()
      if (inTxn) return
      if (this._tainted) {
        // M5b/M5c live tail apply: a tainted (pinned) session — read OR
        // write-attached (M5c lifts the write restriction via the WAL
        // insert-position set) — may still advance IN PLACE when the new
        // tail is live-appliable: its temp state survives because the
        // cell is never recycled. When the gate rejects (fallback would
        // need a recycle), the session simply STAYS PINNED at its base —
        // the M1 contract unchanged.
        if (
          this.freshness.mode !== 'local' &&
          this.freshness.mode !== 'pinned' &&
          this.streamPos.lsn < this.runtime.watermark.lsn
        ) {
          await this.tryLiveAdvance()
        }
        return
      }
      if (this.freshness.mode === 'linearizable') {
        // §7 linearizable: confirm the TRUE head (catch-up past the
        // observed tail) before the watermark gate — the only mode that
        // sees another host's just-acked commit.
        await this.runtime.linearizableSync()
      }
      if (!this.shouldAdvance()) return
      // M5b live tail apply: prefer advancing the LIVE cell (eager set via
      // pgl_* primitives + FPI restore) over recycle+re-materialize. Falls
      // through to the recycle path when the batch is not live-appliable.
      if (await this.tryLiveAdvance()) return
      await this.destroyCell()
    } else if (this.freshness.mode === 'linearizable' && !this._tainted) {
      await this.runtime.linearizableSync()
    }

    if (this.mode === 'read') {
      await this.runtime.baseDirs.ensureAtHeadLocal(this.runtime.tailer)
      const lease = await this.runtime.baseDirs.takeCellDir('read')
      const cell = await Cell.open(lease.dir, {
        // Read-attach: the cell's insert position is the dir's own LOCAL
        // clean head (past the stream head by the unpublished boot
        // records); its capture cursor tracks local position.
        expectedHeadLsn: lease.base.localHeadLsn,
        commitGate: this.runtime.opts.commitGate,
      })
      // Leases apply to read cells too (M5a): a discarded/aborted draw on
      // a read cell must still stay inside this incarnation's grants.
      this.runtime.applyLeases(cell, { resetCaches: true })
      this.cell = cell
      this.lease = lease
      this.streamPos = { offset: lease.base.offset, lsn: lease.base.lsn }
      await this.finishAttach(cell)
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
        commitGate: this.runtime.opts.commitGate,
      })
      const floors = await this.runtime.applyFloors(cell)
      if (floors.lost) {
        await cell.db.close().catch(() => undefined)
        this.runtime.baseDirs.releaseCellDir(lease.dir)
        continue
      }
      // Native sequence leases + cache flush AFTER floors apply (§5.3
      // rules 1+3): the fast path must never serve values replayed from a
      // foreign timeline, and every nextval clamps to this incarnation's
      // grants from the first draw.
      this.runtime.applyLeases(cell, { resetCaches: true })
      this.cell = cell
      this.lease = lease
      this.streamPos = floors.pos ?? {
        offset: lease.base.offset,
        lsn: lease.base.lsn,
      }
      await this.finishAttach(cell)
      return
    }
    throw new AdvanceRaceError(this.runtime.opts.attachAttempts)
  }

  /**
   * The freshness-gated advance decision (§7) for an idle, untainted,
   * already-attached cell. `session` (default) is the plain watermark
   * gate; `linearizable` runs the same gate AFTER `linearizableSync`;
   * `local` and `pinned` never advance; `bounded-stale` gates only when
   * the base's wall-clock age exceeds Δ.
   */
  /**
   * M5b live tail apply v1: advance the LIVE read-attached cell past the
   * foreign tail without recycling it. The slice bytes land in the cell's
   * own pg_wal (scratch space for a read cell — never captured), get
   * classified by pgl_walscan inside the cell's own WASM, and — iff every
   * touched block carries a restorable full-page image (the v1 gate) —
   * the §6.3 eager set is applied through the pgl_* primitives. Session
   * temp state survives the advance. Returns false (cell untouched, no
   * stream position change) whenever the gate rejects, the slices do not
   * chain from this session's base, or anything at all looks off — the
   * caller then uses the existing recycle-advance (or stays pinned).
   */
  private async tryLiveAdvance(): Promise<boolean> {
    const cell = this.cell
    if (cell === null) return false
    try {
      await this.runtime.tailer.catchUp()
      const head = this.runtime.tailer.head
      if (head.lsn <= this.streamPos.lsn) return false
      const slices = this.runtime.tailer.slicesSince(this.streamPos.lsn)
      if (slices.length === 0) return false
      // The batch must chain contiguously from this session's exact base.
      let expect = this.streamPos.lsn
      for (const s of slices) {
        if (s.baseLsn !== expect) return false
        expect = s.endLsn
      }
      if (expect !== head.lsn) return false
      // Transplant the bytes into the live cell's pg_wal (NODEFS
      // passthrough — probed by the M5b live-apply suite), then classify
      // and apply. Read cells never publish, so overwriting their local
      // (unpublished boot) WAL range with foreign bytes is safe: pg_wal
      // past the cell's own insert position is scratch.
      // Flush local WAL first: pending async-commit bytes or cached WAL
      // pages must never overwrite the transplanted foreign bytes later.
      cell.flushWal()
      for (const s of slices) {
        writeWalRange(cell.dir, s.baseLsn, s.bytes)
      }
      if (process.env.PGLITE_LIVE_APPLY_DEBUG === '1') {
        console.log(
          '[live-adv]',
          this.id,
          'pos',
          this.streamPos.lsn,
          'head',
          head.lsn,
          'slices',
          slices.map((x) => [x.baseLsn, x.endLsn, x.kind]),
        )
      }
      const res = applyLiveTail(cell.db, cell.dir, this.streamPos.lsn, head.lsn)
      if (!res.applied) return false
      // M5c: applyLiveTail set the WAL insert position to the new head —
      // move the capture cursor with it (write cells publish from here;
      // read cells keep cursor == insert for the write-upgrade probe) and
      // re-snapshot the reset base.
      cell.advanceTo(head.lsn, { flush: this.mode === 'write' })
      this.streamPos = { offset: head.offset, lsn: head.lsn }
      if (this.mode === 'write') {
        // Sequence discipline after a write-cell advance (§5.3): the
        // applied foreign page state may sit BELOW local abort-observed
        // draws — re-floor (publishes a floors slice when needed) and
        // re-clamp leases with the SeqTable cache flushed.
        const floors = await this.runtime.applyFloors(cell)
        if (floors.lost) {
          await this.destroyCell()
          return false
        }
        this.runtime.applyLeases(cell, { resetCaches: true })
        if (floors.pos !== null) {
          // applyFloors already confirmed the published floors slice
          // (cursor + base snapshot follow it inside confirmPublished).
          this.streamPos = floors.pos
        }
      }
      this.lastAdvanceAt = Date.now()
      return true
    } catch (err) {
      if (process.env.PGLITE_LIVE_APPLY_DEBUG === '1') {
        console.log('[live-apply] mid-apply error:', err)
      }
      // The gate rejects BEFORE anything mutates, so an exception here
      // means a mid-apply failure: the live cell can no longer be
      // trusted. Untainted sessions recycle (the caller's fallback);
      // tainted sessions cannot survive the recycle — fatal reset,
      // never a silent continuation (§3.3).
      await this.destroyCell()
      if (this._tainted) {
        this.dead = `live tail apply failed on a pinned session: ${String(err)}`
        this.runtime.removeSession(this)
        throw new FatalSessionResetError(this.taintNames())
      }
      return false
    }
  }

  /**
   * THE M5d REBASE LADDER (§4.2/§4.4/§4.7). Runs on an interactive COMMIT
   * unit's CAS loss, on an untainted session. Steps:
   *
   *  1. harvest FIRST (ring snapshot, then own-WAL enumeration + payload
   *     re-reads — the transaction committed LOCALLY before capture, so
   *     post-commit same-session reads see exactly its net effect);
   *  2. eligibility gates (rebase taints, ring overflow, temp writes);
   *  3. floors probe (observed sequence draws survive any outcome);
   *  4. advance to K: in-place reset + live tail apply, falling back to
   *     recycle-with-materialize (rebase proceeds either way — the
   *     harvested data is already in JS memory);
   *  5. validate at K (§4.2) — any failure => 40001, local txn already
   *     discarded by the reset;
   *  6. re-apply in ONE fresh transaction under
   *     session_replication_role=replica; 23505 => 40001 (§4.0);
   *  7. capture the NEW slice and CAS; a second loss loops once more
   *     (max 2 rounds, §4.7), then 40001.
   *
   * The session survives every outcome.
   */
  private async tryTransparentRebase(
    cell: Cell,
    slice: CapturedSlice,
    notifications: { channel: string; payload: string }[],
  ): Promise<
    | { landed: true; offset: string; lsn: bigint }
    | { landed: false; detail: string }
  > {
    this.rebaseStats.attempts++
    const B = slice.baseLsn

    // Ring snapshot BEFORE anything else touches the cell.
    const readSet = cell.readSetSnapshot()
    cell.readSetEnd()

    if (this.rebaseTaint !== null) {
      return this.discardLostInteractive(
        cell,
        `taint '${this.rebaseTaint}' observed (§4.5)`,
      )
    }
    if (readSet.overflowed) {
      return this.discardLostInteractive(cell, 'read-set ring overflow')
    }
    // Temp-write-during-attempt (§4.5): a temp schema appearing during an
    // untainted session's transaction means THIS attempt created temp
    // state — its pages hold discarded xids the winner stream would later
    // rebind. (Pre-existing temp state implies a tainted session, which
    // never reaches this ladder.)
    const temp = await cell.db.query<{ t: boolean }>(
      'select pg_my_temp_schema()::oid <> 0 as t',
    )
    if (temp.rows[0].t) {
      return this.discardLostInteractive(
        cell,
        'temp-table write during the attempt (§4.5)',
      )
    }

    const harvest = await harvestRebasePlan(
      cell,
      B,
      slice.endLsn,
      readSet.pins.map((p) => ({ db: p.db, rel: p.rel })),
    )
    if (!harvest.ok) {
      return this.discardLostInteractive(cell, harvest.reason)
    }
    const plan = harvest.plan

    let detail = 'rebase bounds exhausted'
    for (let round = 0; round < 2; round++) {
      const active = this.cell
      if (active === null) return { landed: false, detail: 'cell lost' }
      // Observed draws must be floored before the local txn is discarded.
      await this.runtime.probeFloors(active)
      // Advance to K: reset + live apply, else recycle-with-materialize.
      if (active.canResetInPlace()) {
        try {
          active.resetToBase()
          if (!(await this.tryLiveAdvance())) await this.destroyCell()
        } catch {
          await this.destroyCell()
        }
      } else {
        await this.destroyCell()
      }
      if (this.cell === null) {
        try {
          await this.ensureCell() // recycle at head (canonical for writers)
        } catch (err) {
          this.rebaseStats.failed++
          return { landed: false, detail: `re-attach failed: ${String(err)}` }
        }
      }
      const cellAtK = this.cell
      if (cellAtK === null) {
        this.rebaseStats.failed++
        return { landed: false, detail: 'cell lost during advance' }
      }
      const K = this.streamPos.lsn

      // Winner-tail records (B, K] for the schema-epoch fence: present in
      // the live cell's pg_wal after a live advance, and in a recycled
      // cell's materialized pg_wal (unless rotation trimmed it — fail
      // loud then).
      let winner: WalRecord[]
      try {
        winner = walscanRange(cellAtK.db, B, K)
      } catch (err) {
        this.rebaseStats.failed++
        return {
          landed: false,
          detail: `winner tail not scannable: ${String(err)}`,
        }
      }

      const v = validateAtK(cellAtK, B, readSet, plan, winner)
      if (!v.ok) {
        // Local txn already discarded by the reset; cell is clean at K.
        this.rebaseStats.failed++
        return { landed: false, detail: v.reason }
      }

      try {
        await reapplyPlan(cellAtK, plan)
      } catch (err) {
        // Rolled back inside; swallow the abort-tail WAL exactly like the
        // ordinary abort path (M4 torn-tail finding).
        if (this.mode === 'write') {
          await cellAtK.db.exec('checkpoint').catch(() => undefined)
        }
        this.rebaseStats.failed++
        return { landed: false, detail: (err as Error).message }
      }

      const newSlice = await cellAtK.captureSlice()
      if (newSlice === null) {
        this.rebaseStats.failed++
        return { landed: false, detail: 'empty re-apply slice' }
      }
      const res = await this.runtime.commitFromSession({
        commitId: randomUUID(),
        kind: 'commit',
        baseLsn: newSlice.baseLsn,
        endLsn: newSlice.endLsn,
        bytes: newSlice.bytes,
        // The original attempt's notifications ride the rebased commit —
        // exactly-once by construction (lost attempts emit nothing).
        notifications,
      })
      if (res.landed) {
        cellAtK.confirmPublished(newSlice.endLsn)
        this.streamPos = { offset: res.nextOffset, lsn: newSlice.endLsn }
        await this.runtime.probeGrants(cellAtK)
        await this.probeTaints(cellAtK)
        this.rebaseStats.landed++
        return { landed: true, offset: res.offset, lsn: newSlice.endLsn }
      }
      detail = 'rebase bounds exhausted (2 CAS losses)'
      // Loop: the harvest and read set stay valid (B unchanged); the next
      // round resets the re-applied txn and advances to the new head.
    }
    const active = this.cell
    if (active !== null) {
      return this.discardLostInteractive(active, detail)
    }
    this.rebaseStats.failed++
    return { landed: false, detail }
  }

  /**
   * Ineligible/exhausted interactive loss: the pre-M5d discard — floors
   * probe, then in-place reset + advance (or recycle). Returns the
   * conflict result for the ladder.
   */
  private async discardLostInteractive(
    cell: Cell,
    detail: string,
  ): Promise<{ landed: false; detail: string }> {
    this.rebaseStats.failed++
    await this.runtime.probeFloors(cell)
    // M5e: the reversed commit's deferred truncates die with it (§3.6).
    cell.commitGateDiscard()
    if (cell.canResetInPlace()) {
      try {
        cell.resetToBase()
        if (!(await this.tryLiveAdvance())) await this.destroyCell()
      } catch {
        await this.destroyCell()
      }
    } else {
      await this.destroyCell()
    }
    // Vanilla failed-COMMIT semantics: WITH HOLD cursors materialized by
    // the reversed commit are dropped (survivors of an in-place reset).
    await this.dropLostHoldables()
    return { landed: false, detail }
  }

  private shouldAdvance(): boolean {
    switch (this.freshness.mode) {
      case 'local':
      case 'pinned':
        return false
      case 'bounded-stale':
        if (Date.now() - this.lastAdvanceAt < this.freshness.ms) return false
        return this.streamPos.lsn < this.runtime.watermark.lsn
      case 'session':
      case 'linearizable':
        return this.streamPos.lsn < this.runtime.watermark.lsn
    }
  }

  /**
   * Post-attach hookup for a fresh cell: the notification harvest tap
   * (M3 — `onNotification` fires during unit execution, after local
   * commit, before capture), the bounded-stale clock, session-state
   * replay, and the cell auto-LISTEN of the host union.
   */
  private async finishAttach(cell: Cell): Promise<void> {
    this.lastAdvanceAt = Date.now()
    cell.db.onNotification((channel, payload) => {
      this.notifBuffer.push({ channel, payload })
    })
    await this.replaySessionState(cell)
    await this.applyListenUnion(cell)
  }

  /**
   * Apply the host LISTEN union to this cell (M3, §10.2 step 2): fresh
   * cells get plain `LISTEN`s (like the SET replay); open cells re-sync
   * with `UNLISTEN *` first when the union changed. LISTEN/UNLISTEN are
   * backend-local (no WAL — probed in tests), so read cells stay
   * publish-clean. Output discarded.
   */
  private async applyListenUnion(cell: Cell): Promise<void> {
    const version = this.runtime.listenVersion
    const channels = this.runtime.listenUnion
    const fresh = this.cellListenVersion === -1
    this.cellListenVersion = version
    const stmts = channels.map((c) => `listen "${c.replace(/"/g, '""')}"`)
    if (!fresh) stmts.unshift('unlisten *')
    if (stmts.length === 0) return
    const discard = { onRawData: () => {} }
    await cell.db.runExclusive(() =>
      cell.db.execProtocolRawStream(serialize.query(stmts.join('; ')), discard),
    )
  }

  /**
   * Re-establish wire-session state on a fresh cell: replay the recorded
   * StartupMessage, then every tracked session-level SET, discarding all
   * output. None of it writes WAL, so read cells stay publish-clean.
   * No-op for purely programmatic (SQL-level) sessions.
   */
  private async replaySessionState(cell: Cell): Promise<void> {
    if (this.startupBytes === null && this.setStatements.length === 0) return
    const discard = { onRawData: () => {} }
    await cell.db.runExclusive(async () => {
      if (this.startupBytes !== null) {
        await cell.db.execProtocolRawStream(this.startupBytes, discard)
      }
      for (const sql of this.setStatements) {
        await cell.db.execProtocolRawStream(serialize.query(sql), discard)
      }
    })
  }

  /**
   * M5e commit gate, landed verdict: execute the deferred ON COMMIT
   * DELETE ROWS truncates strictly AFTER the CAS (§3.6 reorder). On a
   * read cell the truncate's catalog WAL can never publish — swallow it
   * like the abort path does. Returns false when the run failed and the
   * cell was recycled (tainted sessions get the fatal reset then: the
   * recycle destroys their state, and the NEXT transaction's DELETE
   * ROWS contract would otherwise be silently broken).
   */
  private async finishCommitGate(cell: Cell): Promise<boolean> {
    if (cell.commitGatePending() === 0) return true
    try {
      cell.commitGateRun()
      if (this.mode === 'read') {
        const stray = await cell.captureSlice()
        if (stray !== null) cell.confirmPublished(stray.endLsn)
      }
      return true
    } catch {
      await this.destroyCell()
      if (this._tainted) {
        this.dead = 'commit-gate truncate failed; cell recycled'
        this.runtime.removeSession(this)
        throw new FatalSessionResetError(this.taintNames())
      }
      return false
    }
  }

  /**
   * Drop WITH HOLD cursors materialized by a REVERSED local commit
   * (M5e): they are memory tuplestores that survive the in-place reset,
   * but vanilla's failed COMMIT destroys them — a bare survivor would
   * be a silent continuation against aborted state. Compares pg_cursors
   * against the last completed transaction's holdable set. No-op when
   * the cell was recycled (cursors died with it). CLOSE writes no WAL.
   */
  private async dropLostHoldables(): Promise<void> {
    const cell = this.cell
    if (cell === null || cell.db.isInTransaction()) return
    try {
      const rows = (
        await cell.db.query<{ name: string }>(
          `select name from pg_cursors where is_holdable`,
        )
      ).rows
      for (const r of rows) {
        if (!this.holdableNames.has(r.name)) {
          await cell.db.exec(`close "${r.name.replace(/"/g, '""')}"`)
        }
      }
    } catch {
      // Best effort: a failure here leaves a stale cursor, never a
      // wrong result — and the cell may legitimately be mid-teardown.
    }
  }

  /**
   * One post-transaction catalog probe for session-state taints (§3.3):
   * temp schema, holdable cursors, session advisory locks. Tainting
   * latches; the gc-pin L frame is appended once, at the transition.
   */
  private async probeTaints(cell: Cell): Promise<void> {
    const row = (
      await cell.db.query<{ temp: boolean; cur: string[]; adv: number }>(
        `select pg_my_temp_schema()::oid <> 0 as temp,
                (select coalesce(array_agg(name), '{}') from pg_cursors where is_holdable) as cur,
                (select count(*)::int from pg_locks where locktype = 'advisory') as adv`,
      )
    ).rows[0]
    this.taints = {
      tempSchema: row.temp,
      holdableCursors: row.cur.length > 0,
      advisoryLocks: row.adv > 0,
    }
    // The survivor set dropLostHoldables compares against (M5e).
    this.holdableNames = new Set(row.cur)
    const any = row.temp || row.cur.length > 0 || row.adv > 0
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
    this.cellListenVersion = -1
    this.holdableNames = new Set() // cursors die with the instance
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
    this.cellListenVersion = -1
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
