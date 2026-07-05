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
  formatLsn,
  liveApplyStats,
  writeWalRange,
} from '@electric-sql/pglite-cell'
import type { CapturedSlice, WalRecord } from '@electric-sql/pglite-cell'
import {
  lazyAttach,
  LazyAttachFallbackError,
  WorkerCell,
} from '@electric-sql/pglite-cell/worker-cell'
import { rmSync } from 'node:fs'
import type { SessionCell } from './cell-kind'
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
  AdvisoryLockDisabledError,
  FatalSessionResetError,
  PinnedWriteError,
  ReadOnlyCaptureError,
  SerializationConflictError,
  SessionClosedError,
  SessionWatchdogError,
  SessionPinnedExpiredError,
} from './errors'
import {
  concatBytes,
  extractNotificationResponses,
  noticeResponse,
  scanBackendOutput,
} from './proxy/wire'
import type { BackendScan } from './proxy/wire'
import { ResponseBuffer, ResponseTooLargeError } from './proxy/response-buffer'

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
  | 'streamed-readonly' // H1: declared read-only, output ALREADY streamed
  | 'landed' // commit CAS'd and landed: buffered response is now true
  | 'held-conflict' // unrecoverable loss: output DISCARDED, proxy sends 40001
  | 'held-pinned' // pinned-mode write: output DISCARDED, proxy sends 0A000
  | 'held-too-large' // H1: response exceeded the spool cap, proxy sends 40001
  | 'mid-txn' // interactive transaction in progress: streams by design
  | 'aborted' // transaction aborted (error / ROLLBACK): nothing to publish
  | 'held-advisory' // advisoryLocks='error': not executed, proxy sends 0A000

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

/**
 * Advisory-lock statement detection (M6, §4.6): a case-insensitive scan for
 * a `pg_advisory_` function reference in the statement text. Documented v1
 * approximation, consistent with the other statement-text classifiers here
 * (rebase taints, LISTEN) — a string literal mentioning the name yields a
 * false positive, accepted as harmless-conservative.
 */
function mentionsAdvisoryLock(text: string): boolean {
  return /pg_advisory_/i.test(text)
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

// H1 declared-read-only classification (§3.5). A statement-text scan,
// consistent with the other simple-protocol classifiers here. It recognizes
// only the DECLARED read-only forms — a transaction the client PROMISES is
// read-only — so that we can stream its output with no buffering and assert
// an empty capture at the end (a nonempty capture is then a loud protocol
// bug, not a silent write). It does NOT try to prove an arbitrary statement
// read-only; the buffering ladder is the safety net for everything else.

/**
 * `BEGIN READ ONLY` / `START TRANSACTION READ ONLY` (in any word order of
 * the READ-ONLY / ISOLATION / DEFERRABLE modes) as the opening statement of
 * a simple unit — the client declares the whole transaction read-only.
 */
function opensReadOnlyTransaction(sql: string): boolean {
  const m = /^\s*(begin|start\s+transaction)\b([^;]*)/i.exec(sql)
  if (m === null) return false
  return /\bread\s+only\b/i.test(m[2])
}

/** `SET [SESSION] default_transaction_read_only = on/true` (session state). */
function setsDefaultReadOnly(sql: string): boolean {
  return /^\s*set\s+(?:session\s+)?default_transaction_read_only\s*(?:=|\s+to\s+)\s*(?:'?on'?|'?true'?|1)\s*;?\s*$/i.test(
    sql,
  )
}

/** `SET [SESSION] default_transaction_read_only = off/false` — clears it. */
function setsDefaultReadWrite(sql: string): boolean {
  return /^\s*set\s+(?:session\s+)?default_transaction_read_only\s*(?:=|\s+to\s+)\s*(?:'?off'?|'?false'?|0)\s*;?\s*$/i.test(
    sql,
  )
}

/**
 * A lone `BEGIN` / `START TRANSACTION` with NO explicit read-write/read-only
 * mode — inherits `default_transaction_read_only`. Used to decide whether a
 * transaction opened while the session default is read-only is itself
 * read-only (so `SET default_transaction_read_only=on; BEGIN; SELECT …` also
 * streams). An explicit `READ WRITE` opts back out.
 */
function opensDefaultModeTransaction(sql: string): {
  opens: boolean
  explicitReadWrite: boolean
} {
  const m = /^\s*(begin|start\s+transaction)\b([^;]*)/i.exec(sql)
  if (m === null) return { opens: false, explicitReadWrite: false }
  return { opens: true, explicitReadWrite: /\bread\s+write\b/i.test(m[2]) }
}

export class HostSession {
  readonly id = `s-${randomUUID()}`

  private cell: SessionCell | null = null
  private lease: CellDirLease | null = null
  /** Work dir of a lazy-attached worker cell (owned; rm'd on destroy). */
  private lazyWorkDir: string | null = null
  /**
   * M7 W3: after a repeat one-shot CAS loss the next WRITE attach takes
   * the MATERIALIZE path once instead of a lazy attach — the canonical
   * re-attach publishes a sync slice, restoring the M1 convergence
   * pressure under hot cross-host contention (a lazy attach appends
   * nothing and would livelock symmetric writers).
   */
  private forceMaterializeAttach = false
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
  /** M6 §4.6: the cell-local advisory-lock WARNING fires once per session. */
  private advisoryWarned = false
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

  /**
   * H1 declared-read-only streaming (§3.5). `sessionReadOnly` tracks the
   * session `default_transaction_read_only` GUC (SET-driven, replayed on
   * recycle like any SET). `txnReadOnly` is true while inside a transaction
   * the client DECLARED read-only (BEGIN READ ONLY / START TRANSACTION READ
   * ONLY / a plain BEGIN under the read-only default). A unit executing in a
   * read-only context streams its output to the client with NO buffering and
   * asserts an empty capture at txn end.
   */
  private sessionReadOnly = false
  private txnReadOnly = false
  /** True while the CURRENT unit is streaming under the read-only promise —
   *  drive() asserts an empty capture at txn end (else ReadOnlyCaptureError). */
  private streamingReadOnly = false
  /** Cumulative rebase counters (TEST HOOK / diagnostics). */
  readonly rebaseStats = { attempts: 0, landed: 0, failed: 0 }

  /** TEST HOOK (§16 client-observation property): unit execution events. */
  _unitObserver: ((ev: UnitObservation) => void) | null = null

  /**
   * M7 W3 laziness byte counters (§16 suite + console): the attached
   * worker cell's fault/overlay stats, or null when the session has no
   * lazy worker cell attached.
   */
  async lazyStats(): Promise<{
    chunkFaults: number
    bytesFaulted: number
    overlayHits: number
    hostFaults: number
    hostFaultBytes: number
  } | null> {
    const cell = this.cell
    if (cell === null || !('lazyStats' in cell)) return null
    return cell.lazyStats()
  }

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
    // M6 §4.6: strict advisory-lock policy rejects without executing. The
    // 'local-warn' notice is a wire-level NoticeResponse with no SQL-result
    // analogue, so the programmatic path only enforces the 'error' mode
    // (`advisoryWarned` still latches so a later proxy unit fires once).
    if (
      this.runtime.opts.advisoryLocks === 'error' &&
      mentionsAdvisoryLock(sql)
    ) {
      throw new AdvisoryLockDisabledError()
    }
    const r = await this.drive<Results[]>(async (cell) => {
      let threw: unknown
      let results: Results[] = []
      try {
        results = (await cell.db.exec(sql)) as Results[]
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
   *
   * H1 (§3.5): when the unit runs in a DECLARED read-only context (BEGIN
   * READ ONLY / default_transaction_read_only), and a `stream` sink is
   * provided, the unit's output is written to the client INCREMENTALLY with
   * no buffering; the returned `output` is empty and the disposition is
   * `streamed-readonly`. Otherwise the output is buffered through the §3.5
   * ladder (memory → spool file → 40001) so a large response in a
   * non-read-only transaction cannot OOM the host.
   */
  execUnit(
    unit: ProtocolUnit,
    stream?: (bytes: Uint8Array) => void,
  ): Promise<UnitResult> {
    return this.run(() => this.execUnitInner(unit, stream))
  }

  private async execUnitInner(
    unit: ProtocolUnit,
    stream?: (bytes: Uint8Array) => void,
  ): Promise<UnitResult> {
    // Taint-scan input (§4.5): simple-protocol SQL when known; extended
    // protocol falls back to a raw byte decode (Parse messages carry the
    // SQL text — the scan only needs substrings).
    this.currentUnitText =
      unit.sqlForReplay ?? Buffer.from(unit.bytes).toString('latin1')

    // M6 §4.6 advisory-lock policy: 'error' rejects the statement WITHOUT
    // executing (the proxy synthesizes 0A000). 'local-warn' lets it run and
    // prepends a one-per-session WARNING to the output (applied below).
    const advisory = mentionsAdvisoryLock(this.currentUnitText)
    if (advisory && this.runtime.opts.advisoryLocks === 'error') {
      return {
        output: new Uint8Array(0),
        rfqStatus: 'I',
        disposition: 'held-advisory',
      }
    }

    // H1 read-only context for THIS unit (§3.5): compute BEFORE executing,
    // from the unit text and the standing txn/session read-only state. When
    // read-only AND a stream sink is available, output is streamed with no
    // buffering; a nonempty capture at txn end is then a loud protocol bug.
    const streaming = stream !== undefined && this.unitIsReadOnly(unit)
    this.streamingReadOnly = streaming

    let attempt = 0
    let tooLarge = false
    const r = await this.drive<{ output: Uint8Array; scan: BackendScan }>(
      async (cell) => {
        // Re-executions get a fresh buffer (the discarded attempt's spool
        // file was disposed with it). Streaming units never buffer.
        const buffer = streaming
          ? null
          : new ResponseBuffer({
              memoryMax: this.runtime.opts.bufferMemoryMax,
              spoolMax: this.runtime.opts.bufferSpoolMax,
            })
        // The non-streaming path assembles the WHOLE response first (through
        // the ladder) and strips 'A' frames once, over the complete byte run
        // — so message boundaries are never split. The streaming path
        // forwards chunks raw (a declared read-only txn emits no 'A' frames).
        let overflow: ResponseTooLargeError | null = null
        let threw: unknown
        try {
          await cell.db.runExclusive(() =>
            cell.db.execProtocolRawStream(unit.bytes, {
              onRawData: (data) => {
                if (streaming) {
                  // A DECLARED read-only transaction can never NOTIFY (NOTIFY
                  // writes WAL and is a write), so there are no 'A' frames to
                  // strip — forward raw. This also sidesteps the chunk-
                  // boundary hazard: raw-stream chunks are not guaranteed to
                  // align to message boundaries, so a per-chunk 'A' walk
                  // could mis-parse; we simply never need it here.
                  if (data.length > 0) stream(data.slice())
                  return
                }
                try {
                  buffer!.push(data.slice())
                } catch (err) {
                  if (err instanceof ResponseTooLargeError) {
                    // Record and stop feeding; we cannot abort the native
                    // exec mid-stream, so drain the rest into the void (the
                    // buffer's push is now a no-op guard) — the whole unit
                    // is discarded as 40001 below.
                    overflow ??= err
                    return
                  }
                  throw err
                }
              },
            }),
          )
        } catch (err) {
          threw = err
        }
        if (overflow !== null) {
          buffer?.dispose()
          tooLarge = true
          // A too-large response is treated as an aborted unit: nothing is
          // published, floors are probed, and the proxy synthesizes 40001.
          // Roll back any open transaction the oversized statement began.
          if (cell.db.isInTransaction()) {
            await cell.db.exec('rollback').catch(() => undefined)
          }
          const empty = new Uint8Array(0)
          return {
            payload: { output: empty, scan: scanBackendOutput(empty) },
            threw: undefined,
            aborted: true,
            leaseExhausted: false,
          }
        }
        // Streaming units produced no buffer: their output already went to
        // the client. The scan is over an empty buffer (RFQ was streamed
        // too); drive() classifies via the cell's txn state and capture.
        if (streaming) {
          const empty = new Uint8Array(0)
          this._unitObserver?.({
            phase: 'attempt',
            unitKind: unit.kind,
            attempt: attempt++,
            outputBytes: 0,
          })
          return {
            payload: { output: empty, scan: scanBackendOutput(empty) },
            threw,
            aborted: threw !== undefined,
            leaseExhausted: isSequenceLeaseExhausted(threw),
          }
        }
        // Uniform delivery (M3, §10.2 step 4): raw 'A' NotificationResponse
        // bytes are STRIPPED from unit output — all client-facing delivery
        // is tailer-driven, so nothing ever arrives twice and every
        // listener (committer included) hears the same global order. The
        // same walk IS the harvest: PGlite's raw-stream exec bypasses its
        // parser, so the 'A' bytes here are the only place the local
        // commit's notifications exist.
        const assembled = buffer!.finalize()
        buffer!.dispose()
        const { stripped: output, notifications } =
          extractNotificationResponses(assembled)
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

    // H1 spool-cap overflow (§3.5 rung 3): the unit was discarded; surface a
    // clean 40001 + HINT. Nothing reached the client (the response died in
    // the buffer). Update the read-only txn tracking first (below) is moot —
    // the txn was rolled back.
    if (tooLarge) {
      this.streamingReadOnly = false
      this.updateReadOnlyState(unit)
      return {
        output: new Uint8Array(0),
        rfqStatus: 'I',
        disposition: 'held-too-large',
      }
    }

    this.streamingReadOnly = false

    // H1: a streamed unit already sent every byte (incl. its RFQ). Whatever
    // the drive() verdict — read-only, aborted, or mid-txn — the proxy has
    // nothing left to flush. A read-only txn can never land/conflict/
    // pinned-write (the capture is empty by the read-only promise, asserted
    // in drive()), so those verdicts are unreachable here.
    if (streaming) {
      this.trackReplayState(unit)
      this.updateReadOnlyState(unit)
      if (
        (r.kind === 'mid-txn' || r.kind === 'aborted') &&
        r.threw !== undefined
      )
        throw r.threw
      const streamed: UnitResult = {
        output: new Uint8Array(0),
        rfqStatus:
          r.kind === 'mid-txn' ? 'T' : r.kind === 'aborted' ? 'E' : 'I',
        disposition: 'streamed-readonly',
      }
      this._unitObserver?.({
        phase: 'result',
        unitKind: unit.kind,
        attempt,
        outputBytes: 0,
        disposition: streamed.disposition,
      })
      return streamed
    }

    let result: UnitResult
    switch (r.kind) {
      case 'mid-txn':
        if (r.threw !== undefined) throw r.threw
        this.updateReadOnlyState(unit)
        result = {
          output: r.payload.output,
          rfqStatus: r.payload.scan.rfqStatus ?? 'T',
          disposition: 'mid-txn',
        }
        break
      case 'aborted':
        if (r.threw !== undefined) throw r.threw
        this.updateReadOnlyState(unit)
        result = {
          output: r.payload.output,
          rfqStatus: r.payload.scan.rfqStatus ?? 'I',
          disposition: 'aborted',
        }
        break
      case 'read-only':
        this.trackReplayState(unit)
        this.updateReadOnlyState(unit)
        result = {
          output: r.payload.output,
          rfqStatus: r.payload.scan.rfqStatus ?? 'I',
          disposition: 'flushed-readonly',
        }
        break
      case 'landed':
        this.trackReplayState(unit)
        this.updateReadOnlyState(unit)
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
    // M6 §4.6 'local-warn': prepend the one-per-session advisory WARNING
    // ahead of the statement's own output (the client sees the statement
    // succeed plus the notice). Only when the output is actually flushed —
    // a held/discarded disposition carries no client-visible bytes to fix.
    if (
      advisory &&
      this.runtime.opts.advisoryLocks === 'local-warn' &&
      (result.disposition === 'flushed-readonly' ||
        result.disposition === 'landed' ||
        result.disposition === 'mid-txn' ||
        result.disposition === 'aborted')
    ) {
      const notice = this.advisoryNoticeBytes()
      if (notice.length > 0) {
        result = { ...result, output: concatBytes([notice, result.output]) }
      }
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
   * H1 (§3.5): is THIS unit executing in a declared-read-only context? True
   * when the session is currently INSIDE a declared-read-only transaction
   * (`txnReadOnly`), OR when this very unit OPENS one — a simple `BEGIN READ
   * ONLY` / `START TRANSACTION READ ONLY`, or a plain `BEGIN` inheriting a
   * `default_transaction_read_only = on` session default. Extended-protocol
   * units carry no reliable simple-SQL text, so they never open a read-only
   * txn on their own (a documented approximation, same class as the other
   * text classifiers) — but they DO ride an already-open read-only txn.
   */
  private unitIsReadOnly(unit: ProtocolUnit): boolean {
    if (this.txnReadOnly) return true
    if (unit.kind !== 'simple' || unit.sqlForReplay === undefined) return false
    const sql = unit.sqlForReplay
    if (opensReadOnlyTransaction(sql)) return true
    if (this.sessionReadOnly) {
      const open = opensDefaultModeTransaction(sql)
      if (open.opens && !open.explicitReadWrite) return true
    }
    return false
  }

  /**
   * H1 (§3.5): fold the just-finished unit into the read-only tracking
   * state. `default_transaction_read_only` SETs move the session default;
   * BEGIN/START open a txn whose read-only-ness is fixed at open; the txn
   * ending (RFQ 'I' — the drive() outcome is read-only/aborted/landed with
   * the cell idle) clears `txnReadOnly`. Only meaningful for simple units.
   */
  private updateReadOnlyState(unit: ProtocolUnit): void {
    const inTxn = this.cell !== null && this.cell.db.isInTransaction()
    if (unit.kind === 'simple' && unit.sqlForReplay !== undefined) {
      const sql = unit.sqlForReplay
      if (setsDefaultReadOnly(sql)) this.sessionReadOnly = true
      else if (setsDefaultReadWrite(sql)) this.sessionReadOnly = false
      // A transaction OPENING: fix its read-only-ness now (only if we are
      // actually mid-txn afterwards — a single-statement `BEGIN; …; COMMIT`
      // simple unit opens and closes in one shot and stays idle).
      if (inTxn && !this.txnReadOnly) {
        if (opensReadOnlyTransaction(sql)) this.txnReadOnly = true
        else {
          const open = opensDefaultModeTransaction(sql)
          if (open.opens && !open.explicitReadWrite && this.sessionReadOnly) {
            this.txnReadOnly = true
          }
        }
      }
    }
    // The transaction has ended (cell idle): clear the txn read-only flag.
    if (!inTxn) this.txnReadOnly = false
  }

  /**
   * THE §3.7 contract state machine, shared by `exec` and `execUnit`. Runs
   * `runner` on an attached cell and resolves the transaction outcome:
   * watermark gate before idle units, capture at txn end, read-cell
   * write-upgrade, publish through the host sequencer, floors probe on
   * abort/discard, taint probe, transparent re-execution of one-shots
   * (bounded), fatal reset for tainted losses.
   */
  /**
   * W4 watchdog second line (§11.2): run `fn` under a JS deadline. If it
   * outlasts ~4× `statementTimeoutMs` — i.e. the statement ignored the
   * `statement_timeout` cancel (a tight non-interruptible C loop) — terminate
   * the worker (lazy-worker mode only; nodefs cells have no worker to kill and
   * rely solely on statement_timeout) and fatally reset this session, then
   * throw `SessionWatchdogError`. The host stays healthy. No timeout configured
   * ⇒ `fn` runs unwrapped.
   */
  private async runWatchdogged<T>(
    cell: SessionCell,
    fn: () => Promise<T>,
  ): Promise<T> {
    const stMs = this.runtime.opts.statementTimeoutMs
    if (stMs === undefined || stMs <= 0 || !(cell instanceof WorkerCell)) {
      return fn()
    }
    // 4× the clean-cancel budget (plus a small floor) before the hard kill —
    // statement_timeout should always win first for a well-behaved statement.
    const deadlineMs = Math.max(Math.floor(stMs) * 4, 250)
    let timer: ReturnType<typeof setTimeout> | undefined
    let tripped = false
    const trip = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        tripped = true
        this.dead = 'statement watchdog terminated the worker'
        // Fire-and-forget the hard kill; failAll() rejects fn's in-flight
        // request so the race below settles.
        void (cell as WorkerCell).terminate().catch(() => undefined)
        reject(new SessionWatchdogError(deadlineMs))
      }, deadlineMs)
    })
    try {
      return await Promise.race([fn(), trip])
    } finally {
      if (timer !== undefined) clearTimeout(timer)
      if (tripped) {
        // Drop the dead cell + session from the runtime (mirrors gc-pin
        // expiry): the connection is reset, a reconnect gets a fresh cell.
        await this.destroyCell().catch(() => undefined)
        this.runtime.removeSession(this)
      }
    }
  }

  private async drive<T>(
    runner: (cell: SessionCell) => Promise<AttemptOutcome<T>>,
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
      if (!wasInTxn) {
        await cell.maybeSnapshotBase({ flush: this.mode === 'write' })
      }

      // M5d read-set capture (§4.1): (re)arm the native ring at every
      // between-transactions unit start, so an interactive transaction's
      // whole read set — BEGIN unit through COMMIT unit — is in the ring
      // when a CAS loss reaches the rebase ladder. One flag write + ring
      // reset; enabled for one-shots too (harmless, unused).
      if (!wasInTxn) {
        this.rebaseTaint = null
        await cell.readSetBegin()
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

      const { payload, threw, aborted, leaseExhausted } =
        await this.runWatchdogged(cell, () => runner(cell))
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

      let slice = await cell.captureSlice()

      // M7 W3: a cell at CANONICAL position (lazy attach / live advance)
      // can emit pure maintenance WAL from a read-only unit — e.g. an
      // opportunistic heap2 PRUNE of a catalog page during the first scan
      // after redo (xid 0, no commit record). That is not a user write:
      // never a write-upgrade trigger, never a 'committed' outcome. Read
      // cells swallow it locally (they never publish; the local cursor
      // diverges exactly like the nodefs read-attach divergence); write
      // cells publish it as a `sync` slice to keep their canonical
      // position (a lost CAS just leaves the bytes to ride the next
      // capture — contiguity preserved).
      if (slice !== null && (await this.isMaintenanceOnlySlice(cell, slice))) {
        if (this.mode === 'read') {
          cell.confirmPublished(slice.endLsn)
          slice = null
        } else {
          const res = await this.runtime.commitFromSession({
            commitId: randomUUID(),
            kind: 'sync',
            baseLsn: slice.baseLsn,
            endLsn: slice.endLsn,
            bytes: slice.bytes,
          })
          if (res.landed) {
            cell.confirmPublished(slice.endLsn)
            this.streamPos = { offset: res.nextOffset, lsn: slice.endLsn }
          }
          slice = null
        }
      }

      // H1 (§3.5): a unit that STREAMED under the read-only promise must
      // never leave a real (non-maintenance) user-write slice — its output
      // already reached the client and cannot be reversed. A nonempty slice
      // here means a write slipped past both our classifier and Postgres's
      // own read-only enforcement: fail LOUD (XX000), never silently drop.
      if (this.streamingReadOnly && slice !== null) {
        const err = new ReadOnlyCaptureError(slice.bytes.length)
        await this.runtime.probeFloors(cell).catch(() => undefined)
        await this.destroyCell()
        throw err
      }

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
      await cell.commitGateDiscard()
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
        if (await cell.canResetInPlace()) {
          try {
            await cell.resetToBase()
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
      if (attempt === 0 && (await cell.canResetInPlace())) {
        try {
          await cell.resetToBase()
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
      // Repeat losses re-attach via the materialize path in lazy mode
      // (sync-slice convergence pressure — see forceMaterializeAttach).
      if (attempt > 1) this.forceMaterializeAttach = true
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

    // M7 W3 lazy-worker attach (read AND write): worker cell over
    // LazyCellFS at the checkpoint skeleton, advanced to head via the
    // live-apply pipeline — canonical position, no sync slice. Any gap
    // falls through to the existing materialize paths below.
    if (this.runtime.cellMode === 'lazy-worker') {
      const skipLazy = this.forceMaterializeAttach && this.mode === 'write'
      this.forceMaterializeAttach = false
      if (!skipLazy && (await this.tryLazyAttach())) return
      await this.runtime.ensureCanonicalHydrated()
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
        // H2 (§14.8): a read-attached cell must emit no WAL — suppress the
        // one everyday source, opportunistic HOT pruning during seqscans.
        suppressReadWal: true,
      })
      // Leases apply to read cells too (M5a): a discarded/aborted draw on
      // a read cell must still stay inside this incarnation's grants.
      await this.runtime.applyLeases(cell, { resetCaches: true })
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
      await this.runtime.applyLeases(cell, { resetCaches: true })
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
   * True iff every record of the captured slice is buffer-maintenance
   * noise a read-only unit can legitimately produce: heap2 PRUNE/VACUUM/
   * VISIBLE with no xid and no eager-set classification. Conservative —
   * any scan failure or unexpected record means "real write".
   */
  private async isMaintenanceOnlySlice(
    cell: SessionCell,
    slice: CapturedSlice,
  ): Promise<boolean> {
    try {
      const recs = await cell.walscanRange(slice.baseLsn, slice.endLsn)
      return (
        recs.length > 0 &&
        recs.every(
          (r) => r.xid === 0 && r.kind === undefined && r.rmid === 9, // heap2
        )
      )
    } catch {
      return false
    }
  }

  /**
   * M7 W3: attach a lazy worker cell at the stream head. Returns false —
   * leaving the session cell-less — when the mode/context is unavailable,
   * the tail since the checkpoint is not live-appliable (the recipe's
   * fallback), or the tail slices are not retrievable; the caller then
   * uses the existing materialize paths.
   */
  private async tryLazyAttach(): Promise<boolean> {
    const ctx = this.runtime.lazyAttachContext()
    if (ctx === null) return false
    const head = this.runtime.tailer.head
    let slices: { baseLsn: bigint; endLsn: bigint; bytes: Uint8Array }[] = []
    if (head.lsn > ctx.snapEnd) {
      slices = this.runtime.tailer.slicesSince(ctx.snapEnd)
      let expect = ctx.snapEnd
      for (const sl of slices) {
        if (sl.baseLsn !== expect) return false // gap: fallback
        expect = sl.endLsn
      }
      if (expect !== head.lsn) return false
    } else if (head.lsn < ctx.snapEnd) {
      return false // checkpoint ahead of the tailer view: fallback
    }
    const workDir = this.runtime.newLazyCellDir()
    let cell: SessionCell
    let headLsn: bigint
    try {
      const attached = await lazyAttach({
        skeletonDir: ctx.skeletonDir,
        lazyFiles: ctx.lazyFiles,
        workDir,
        snapEnd: ctx.snapEnd,
        slices,
        readChunk: ctx.readChunk,
        commitGate: this.runtime.opts.commitGate,
        // H2 (§14.8): a read-mode lazy cell suppresses opportunistic pruning.
        suppressReadWal: this.mode === 'read',
      })
      cell = attached.cell
      headLsn = attached.headLsn
    } catch (err) {
      rmSync(workDir, { recursive: true, force: true })
      if (err instanceof LazyAttachFallbackError) {
        console.log(
          `[pglite-cell-server] db ${this.runtime.databaseId}: lazy attach ` +
            `fell back to materialize (${err.reason})`,
        )
        return false
      }
      throw err
    }
    this.cell = cell
    this.lease = null
    this.lazyWorkDir = workDir
    this.streamPos = {
      offset: head.lsn === headLsn ? head.offset : ctx.offset,
      lsn: headLsn,
    }
    if (this.mode === 'write') {
      // Floors BEFORE the session runs anything (write-attach parity).
      const floors = await this.runtime.applyFloors(cell)
      if (floors.lost) {
        await this.destroyCell()
        return false // re-attach via the caller's loop / fallback
      }
      if (floors.pos !== null) this.streamPos = floors.pos
    }
    await this.runtime.applyLeases(cell, { resetCaches: true })
    await this.finishAttach(cell)
    return true
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
      await cell.flushWal()
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
      const res = await cell.applyLiveTail(this.streamPos.lsn, head.lsn)
      // Worker cells run live-apply in their own thread (their OWN module
      // instance of liveApplyStats) — mirror the counters into the host
      // process instance so the M5b stats surface stays truthful.
      if (!(cell instanceof Cell)) {
        liveApplyStats.attempts++
        if (res.applied) liveApplyStats.hits++
        else if (res.reason !== undefined) {
          liveApplyStats.fallbacks.set(
            res.reason,
            (liveApplyStats.fallbacks.get(res.reason) ?? 0) + 1,
          )
          liveApplyStats.lastFallback = res.reason
        }
      }
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
        await this.runtime.applyLeases(cell, { resetCaches: true })
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
    cell: SessionCell,
    slice: CapturedSlice,
    notifications: { channel: string; payload: string }[],
  ): Promise<
    | { landed: true; offset: string; lsn: bigint }
    | { landed: false; detail: string }
  > {
    this.rebaseStats.attempts++
    const B = slice.baseLsn

    // Ring snapshot BEFORE anything else touches the cell.
    const readSet = await cell.readSetSnapshot()
    await cell.readSetEnd()

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
      if (await active.canResetInPlace()) {
        try {
          await active.resetToBase()
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
        winner = await cellAtK.walscanRange(B, K)
      } catch (err) {
        this.rebaseStats.failed++
        return {
          landed: false,
          detail: `winner tail not scannable: ${String(err)}`,
        }
      }

      const v = await validateAtK(cellAtK, B, readSet, plan, winner)
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
    cell: SessionCell,
    detail: string,
  ): Promise<{ landed: false; detail: string }> {
    this.rebaseStats.failed++
    await this.runtime.probeFloors(cell)
    // M5e: the reversed commit's deferred truncates die with it (§3.6).
    await cell.commitGateDiscard()
    if (await cell.canResetInPlace()) {
      try {
        await cell.resetToBase()
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
  private async finishAttach(cell: SessionCell): Promise<void> {
    this.lastAdvanceAt = Date.now()
    cell.db.onNotification((channel, payload) => {
      this.notifBuffer.push({ channel, payload })
    })
    // W4 watchdog first line (§11.2): statement_timeout cancels a long
    // statement cleanly. Backend-local GUC (no WAL — read cells stay
    // publish-clean). Re-applied on every (re)attach so recycled cells keep it.
    const stMs = this.runtime.opts.statementTimeoutMs
    if (stMs !== undefined && stMs > 0) {
      await cell.db.exec(`set statement_timeout = ${Math.floor(stMs)}`)
    }
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
  private async applyListenUnion(cell: SessionCell): Promise<void> {
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
  private async replaySessionState(cell: SessionCell): Promise<void> {
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
  private async finishCommitGate(cell: SessionCell): Promise<boolean> {
    if ((await cell.commitGatePending()) === 0) return true
    try {
      await cell.commitGateRun()
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
  private async probeTaints(cell: SessionCell): Promise<void> {
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

  /**
   * The synthesized advisory-lock WARNING (M6 §4.6 'local-warn'), naming the
   * cell-local scope. Returns the NoticeResponse bytes to prepend before the
   * statement's output on the FIRST advisory-lock use in this session; empty
   * on subsequent uses (fires once). Assumes 'local-warn' policy.
   */
  private advisoryNoticeBytes(): Uint8Array {
    if (this.advisoryWarned) return new Uint8Array(0)
    this.advisoryWarned = true
    return noticeResponse({
      severity: 'WARNING',
      code: '01000', // warning
      message:
        'advisory lock scope is cell-local: this session serves one PGlite ' +
        'cell, and advisory locks held here do NOT exclude locks on other ' +
        'cells or hosts of the same database',
      hint: 'do not rely on pg_advisory_* for cross-connection mutual exclusion',
    })
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
    const lazyDir = this.lazyWorkDir
    this.cell = null
    this.lease = null
    this.lazyWorkDir = null
    this.cellListenVersion = -1
    this.holdableNames = new Set() // cursors die with the instance
    if (cell) await cell.db.close().catch(() => undefined)
    if (lease) this.runtime.baseDirs.releaseCellDir(lease.dir)
    if (lazyDir) rmSync(lazyDir, { recursive: true, force: true })
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
    const lazyDir = this.lazyWorkDir
    this.lazyWorkDir = null
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
    if (lazyDir) rmSync(lazyDir, { recursive: true, force: true })
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
