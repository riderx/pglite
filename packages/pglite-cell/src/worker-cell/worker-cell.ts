// M7 W1 — `WorkerCell`: the Cell-compatible surface backed by a PGlite
// instance living in a `worker_threads` Worker (M7 fixed decisions 1+2).
// Exec units travel by async postMessage; the host thread services the SAB
// fault bridge via `Atomics.waitAsync` (no polling). Bookmark/captureSlice
// stay HOST-side: in passthrough mode the datadir is a real NodeFS dir, so
// `readWalRange`/`readControl` read it directly, exactly like `Cell`.
//
// Surface deviations from `Cell` (worker mode — W3 planning input):
//  - members that touch the WASM Module are ASYNC here (flushWal,
//    commitGate*, setSequenceLease, clearSequenceLeases,
//    resetSequenceCaches, readSetBegin/End/Snapshot, pageLsn,
//    relationNblocks, maybeSnapshotBase, canResetInPlace, resetToBase);
//    `confirmPublished`/`advanceTo` stay sync but re-snapshot the base in
//    the background — `await cell.settled()` to observe it.
//  - `db` is a facade (query/exec/execProtocolRawStream/runExclusive/
//    isInTransaction/close/onNotification), NOT a real PGlite. W3 closed
//    the W1 notification gap: worker-side onNotification relays over
//    postMessage. `db.close()` also terminates the worker thread.

import { Worker } from 'node:worker_threads'
import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { readControl, readWalRange } from '../datadir'
import { parseLsn, shutdownCheckpointEnd } from '../lsn'
import { ConfigPinError, ZeroBootWalError } from '../errors'
import { resetStats } from '../cell'
import type {
  BaseSnapshot,
  CapturedSlice,
  CellOpenOpts,
  ReadSetSnapshot,
} from '../cell'
import type { LiveApplyResult } from '../live-apply'
import type { WalRecord } from '../walscan'
import type { LazyCellFsStats, LazyFileSpec } from '../lazy-fs'
import { LAZY_CHUNK_BYTES } from '../lazy-fs'
import {
  CTL_STATE,
  CTL_REQ_SEQ,
  CTL_OP,
  CTL_REQ_LEN,
  CTL_PARAM0,
  CTL_RESP_STATUS,
  CTL_RESP_LEN,
  STATE_REQUEST,
  STATE_RESPONSE,
  STATUS_OK,
  STATUS_UNKNOWN_OP,
  STATUS_TOO_LARGE,
  STATUS_HOST_ERROR,
  STATUS_NOT_FOUND,
  OP_PING,
  OP_HOSTCALL,
  OP_FAULT_READ,
  createBridgeBuffers,
} from './protocol'
import type {
  BurstResult,
  HostToWorker,
  NativeFn,
  WorkerIdentity,
  WorkerToHost,
} from './protocol'

/** Same §9 pins as cell.ts (single source would couple the modules — keep
 *  in lockstep with CONFIG_PINS there). */
const CONFIG_PINS: { name: string; expected: string }[] = [
  { name: 'wal_level', expected: 'replica' },
  { name: 'full_page_writes', expected: 'on' },
  { name: 'data_checksums', expected: 'off' },
]

/** W3 lazy boot: the manifest's lazy file list + the host-side chunk
 *  reader that serves OP_FAULT_READ (chunk cache -> gateway ranged read). */
export interface WorkerCellLazyOpts {
  files: LazyFileSpec[]
  /** Fault granularity; default 256 KiB (fixed decision 3). */
  chunkBytes?: number
  /** Serve `length` bytes at `offset` of object `ref` (chunk-aligned). */
  readChunk: (
    ref: string,
    chunkIdx: number,
    offset: number,
    length: number,
  ) => Promise<Uint8Array>
}

export interface WorkerCellOpenOpts extends CellOpenOpts {
  /** Override the worker entry (tests use a tsx bootstrap for .ts src). */
  workerUrl?: URL | string
  /** node:worker_threads resourceLimits passthrough (§11.2 basics). */
  resourceLimits?: {
    maxOldGenerationSizeMb?: number
    maxYoungGenerationSizeMb?: number
    codeRangeSizeMb?: number
    stackSizeMb?: number
  }
  /** Data-SAB staging size override (default 4 MiB). */
  dataSabBytes?: number
  /** W3: open with a LazyCellFS in the worker instead of plain NodeFS. */
  lazy?: WorkerCellLazyOpts
}

/** A host-registered handler for OP_HOSTCALL bridge requests (v1: tests;
 *  W3: fault reads get their own op + handler). May be async — the worker
 *  stays blocked until it resolves. */
export type HostcallHandler = (
  payload: Uint8Array,
  params: [number, number, number, number],
) => Uint8Array | Promise<Uint8Array>

/**
 * Resolve the built worker artifact next to this module. tsup emits both
 * formats; ESM resolves `worker-entry.js`, CJS `worker-entry.cjs` (the
 * `import.meta.url` here is shimmed by tsup in the CJS build).
 */
function resolveWorkerEntry(): URL {
  const base = import.meta.url
  for (const name of ['worker-entry.js', 'worker-entry.cjs']) {
    const url = new URL(`./${name}`, base)
    if (url.protocol === 'file:' && existsSync(fileURLToPath(url))) return url
  }
  throw new Error(
    'WorkerCell: cannot locate the built worker-entry artifact next to ' +
      base +
      ' — build the package (tsup emits worker-entry.js/.cjs) or pass ' +
      'opts.workerUrl explicitly',
  )
}

/** Distributive Omit (plain Omit collapses a discriminated union). */
type WorkerRequest<T = HostToWorker> = T extends { id: number }
  ? Omit<T, 'id'>
  : never

interface Pending {
  resolve: (v: { value: unknown; inTx: boolean }) => void
  reject: (e: Error) => void
  onChunk?: (bytes: Uint8Array) => void
}

/** The `cell.db` facade: the PGlite-shaped subset session/proxy code uses
 *  (grep of pglite-cell-server: query/exec/execProtocolRawStream/
 *  runExclusive/isInTransaction/close/onNotification). */
export class WorkerCellDb {
  constructor(private readonly cell: WorkerCell) {}

  async query<T>(
    sql: string,
    params?: unknown[],
  ): Promise<{ rows: T[]; fields?: unknown[]; affectedRows?: number }> {
    return (await this.cell._request({ t: 'query', sql, params })) as {
      rows: T[]
    }
  }

  /** Same shape as PGlite.exec: per-statement results. */
  async exec(sql: string): Promise<{ rows: unknown[] }[]> {
    return (await this.cell._request({ t: 'exec', sql })) as {
      rows: unknown[]
    }[]
  }

  /** One exec unit: bytes in, output chunks out via `onRawData`. */
  async execProtocolRawStream(
    message: Uint8Array,
    opts: { syncToFs?: boolean; onRawData?: (data: Uint8Array) => void } = {},
  ): Promise<void> {
    await this.cell._request(
      { t: 'exec-unit', bytes: message, syncToFs: opts.syncToFs },
      opts.onRawData,
    )
  }

  /** Host-side mutex for API parity (the worker serializes anyway). */
  async runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    return this.cell._exclusive(fn)
  }

  /** Mirrored from the worker on every completed request (PGlite shape:
   *  a method, not a getter). */
  isInTransaction(): boolean {
    return this.cell._inTx
  }

  /** Clean-close the instance AND tear down the worker thread — the
   *  PGlite-shaped contract session code relies on (`db.close()` frees
   *  everything; nothing else would ever terminate the worker). */
  async close(): Promise<void> {
    await this.cell._closeDb()
    await this.cell.terminate()
  }

  /** W3 (the W1 relay gap): notifications fired inside the worker are
   *  relayed by postMessage and fan out here — same shape as PGlite's. */
  onNotification(
    callback: (channel: string, payload: string) => void,
  ): () => void {
    return this.cell._onNotification(callback)
  }
}

export class WorkerCell {
  private cursor: bigint
  private base: BaseSnapshot | null = null
  private readonly dbFacade: WorkerCellDb
  private pending = new Map<number, Pending>()
  private nextId = 1
  private chain: Promise<unknown> = Promise.resolve()
  private snapshotChain: Promise<void> = Promise.resolve()
  private closed = false
  /** @internal */
  _inTx = false
  private hostcallHandlers = new Map<number, HostcallHandler>()
  private bridgeStopped = false
  private notifyListeners = new Set<
    (channel: string, payload: string) => void
  >()
  private lazyOpts: WorkerCellLazyOpts | null = null
  private lazyChunkBytes = LAZY_CHUNK_BYTES
  /** Host-side fault counters (OP_FAULT_READ served over the bridge). */
  private hostFaults = 0
  private hostFaultBytes = 0

  private constructor(
    public readonly dir: string,
    private readonly worker: Worker,
    private readonly ctl: Int32Array,
    private readonly dataSab: Uint8Array,
    cursor: bigint,
  ) {
    this.cursor = cursor
    this.dbFacade = new WorkerCellDb(this)
  }

  /** The PGlite-shaped facade (SQL execution passthrough). */
  get db(): WorkerCellDb {
    return this.dbFacade
  }

  get captureCursor(): bigint {
    return this.cursor
  }

  get baseSnapshot(): BaseSnapshot | null {
    return this.base
  }

  /**
   * Spawn the worker, open `dir` in PASSTHROUGH mode (plain NodeFS inside
   * the worker), and run the SAME open-time asserts as `Cell.open`: §9
   * config pins, `enable_indexonlyscan = off`, zero boot WAL, commit gate.
   */
  static async open(
    dir: string,
    opts: WorkerCellOpenOpts,
  ): Promise<WorkerCell> {
    const buffers = createBridgeBuffers(opts.dataSabBytes)
    const entry = opts.workerUrl ?? resolveWorkerEntry()
    // §11.2 basics: bound the worker's heap + stack by default so a runaway
    // query cannot exhaust host memory before the watchdog fires. Caller
    // overrides win per-field.
    const resourceLimits = {
      maxOldGenerationSizeMb: 512,
      stackSizeMb: 8,
      ...opts.resourceLimits,
    }
    const worker = new Worker(entry, {
      workerData: { control: buffers.control, data: buffers.data },
      resourceLimits,
    })
    const cell = new WorkerCell(
      dir,
      worker,
      new Int32Array(buffers.control),
      new Uint8Array(buffers.data),
      opts.expectedHeadLsn,
    )
    worker.on('message', (msg: WorkerToHost) => cell.onMessage(msg))
    worker.on('error', (err) => cell.failAll(err))
    worker.on('exit', (code) =>
      cell.failAll(new Error(`worker exited (code ${code})`)),
    )
    if (opts.lazy !== undefined) {
      cell.lazyOpts = opts.lazy
      cell.lazyChunkBytes = opts.lazy.chunkBytes ?? LAZY_CHUNK_BYTES
    }
    cell.serveBridge() // Atomics.waitAsync loop — alive before any exec
    try {
      await cell._request({
        t: 'open',
        dir,
        lazy:
          opts.lazy === undefined
            ? undefined
            : {
                files: opts.lazy.files,
                chunkBytes: opts.lazy.chunkBytes ?? LAZY_CHUNK_BYTES,
              },
      })

      const mismatches: { name: string; expected: string; actual: string }[] =
        []
      for (const pin of CONFIG_PINS) {
        const rows = (
          await cell.dbFacade.query<Record<string, string>>(`show ${pin.name}`)
        ).rows
        const actual = rows[0][pin.name]
        if (actual !== pin.expected) {
          mismatches.push({ name: pin.name, expected: pin.expected, actual })
        }
      }
      if (mismatches.length > 0) throw new ConfigPinError(mismatches)

      // M5d IOS escape hatch — same as Cell.open.
      await cell.dbFacade.exec('set enable_indexonlyscan = off')

      const insertLsn = await cell.bookmark()
      if (insertLsn !== opts.expectedHeadLsn) {
        throw new ZeroBootWalError(opts.expectedHeadLsn, insertLsn)
      }
      if (opts.commitGate !== false) {
        await cell.native('commit_gate_set', [1])
      }
      // H2 (§14.8): suppress read-cell WAL (opportunistic pruning) at open.
      if (opts.suppressReadWal) {
        await cell.native('set_suppress_read_wal', [1])
      }
      await cell.maybeSnapshotBase()
      return cell
    } catch (err) {
      // Clean-close the instance before killing the worker — a bare
      // terminate would leave the datadir uncleanly shut down (recovery +
      // boot WAL on the next open), unlike Cell.open's error path.
      await cell._closeDb().catch(() => undefined)
      await cell.terminate().catch(() => undefined)
      throw err
    }
  }

  // -------------------------------------------------------------------
  // SAB bridge, host side (Atomics.waitAsync — M7 fixed decision 1)
  // -------------------------------------------------------------------

  /** Register a handler for OP_HOSTCALL requests keyed by param0 (v1:
   *  test traffic; W3 fault reads will get a dedicated op). */
  onHostcall(key: number, handler: HostcallHandler): void {
    this.hostcallHandlers.set(key, handler)
  }

  private serveBridge(): void {
    // Atomics.waitAsync (Node >= 16; typed here because the repo tsconfig
    // lib predates ES2024). Fallback: a tight setImmediate poll ONLY if
    // waitAsync is genuinely unavailable (fixed decision 1 forbids polling
    // as the primary path).
    const waitAsync = (
      Atomics as unknown as {
        waitAsync?: (
          ta: Int32Array,
          index: number,
          value: number,
        ) => { async: boolean; value: Promise<string> | string }
      }
    ).waitAsync?.bind(Atomics)
    const waitForSeqChange = async (seen: number): Promise<void> => {
      if (waitAsync !== undefined) {
        const wait = waitAsync(this.ctl, CTL_REQ_SEQ, seen)
        if (wait.async) await (wait.value as Promise<string>)
        return
      }
      while (
        !this.bridgeStopped &&
        Atomics.load(this.ctl, CTL_REQ_SEQ) === seen
      ) {
        await new Promise<void>((r) => setImmediate(r))
      }
    }
    const loop = async (): Promise<void> => {
      let seen = Atomics.load(this.ctl, CTL_REQ_SEQ)
      while (!this.bridgeStopped) {
        await waitForSeqChange(seen)
        if (this.bridgeStopped) return
        seen = Atomics.load(this.ctl, CTL_REQ_SEQ)
        if (Atomics.load(this.ctl, CTL_STATE) !== STATE_REQUEST) continue
        await this.serveOne()
      }
    }
    void loop()
  }

  private async serveOne(): Promise<void> {
    const op = this.ctl[CTL_OP]
    const len = this.ctl[CTL_REQ_LEN]
    const params: [number, number, number, number] = [
      this.ctl[CTL_PARAM0],
      this.ctl[CTL_PARAM0 + 1],
      this.ctl[CTL_PARAM0 + 2],
      this.ctl[CTL_PARAM0 + 3],
    ]
    const payload = new Uint8Array(len)
    payload.set(this.dataSab.subarray(0, len))
    let status = STATUS_OK
    let response: Uint8Array = new Uint8Array(0)
    try {
      if (op === OP_PING) {
        response = payload
      } else if (op === OP_FAULT_READ) {
        const spec = this.lazyOpts?.files[params[0]]
        if (spec === undefined) {
          status = STATUS_NOT_FOUND
        } else {
          const chunkIdx = params[1]
          const length = params[2]
          response = await this.lazyOpts!.readChunk(
            spec.ref,
            chunkIdx,
            chunkIdx * this.lazyChunkBytes,
            length,
          )
          this.hostFaults++
          this.hostFaultBytes += response.byteLength
        }
      } else if (op === OP_HOSTCALL) {
        const handler = this.hostcallHandlers.get(params[0])
        if (handler === undefined) status = STATUS_UNKNOWN_OP
        else response = await handler(payload, params)
      } else {
        status = STATUS_UNKNOWN_OP
      }
    } catch {
      status = STATUS_HOST_ERROR
    }
    if (response.byteLength > this.dataSab.byteLength) {
      status = STATUS_TOO_LARGE
      response = new Uint8Array(0)
    }
    this.dataSab.set(response, 0)
    this.ctl[CTL_RESP_STATUS] = status
    this.ctl[CTL_RESP_LEN] = response.byteLength
    Atomics.store(this.ctl, CTL_STATE, STATE_RESPONSE)
    Atomics.notify(this.ctl, CTL_STATE)
  }

  /** Test-only: drive `count` bridge round-trips from inside the worker. */
  async hostcallBurst(opts: {
    count: number
    payloadBytes: number
    key?: number
    ping?: boolean
  }): Promise<BurstResult> {
    const { value } = await this.requestRaw({
      t: 'hostcall-burst',
      op: opts.ping === false ? OP_HOSTCALL : OP_PING,
      param0: opts.key ?? 0,
      payloadBytes: opts.payloadBytes,
      count: opts.count,
    })
    return value as BurstResult
  }

  // -------------------------------------------------------------------
  // postMessage plumbing
  // -------------------------------------------------------------------

  private onMessage(msg: WorkerToHost): void {
    if (msg.t === 'ready') return
    if (msg.t === 'notify') {
      for (const cb of [...this.notifyListeners]) {
        try {
          cb(msg.channel, msg.payload)
        } catch {
          // listener failures never poison the message pump
        }
      }
      return
    }
    const p = this.pending.get(msg.id)
    if (p === undefined) return
    if (msg.t === 'chunk') {
      p.onChunk?.(msg.bytes)
      return
    }
    this.pending.delete(msg.id)
    if (msg.t === 'done') {
      this._inTx = msg.inTx
      p.resolve({ value: msg.value, inTx: msg.inTx })
    } else {
      // Re-decorate: session code keys on `detail`/`code` (e.g. the
      // sequence-lease renewal signal) — postMessage flattened them.
      const err = new Error(msg.error) as Error & {
        detail?: string
        code?: string
      }
      if (msg.detail !== undefined) err.detail = msg.detail
      if (msg.code !== undefined) err.code = msg.code
      p.reject(err)
    }
  }

  private failAll(err: Error): void {
    for (const p of this.pending.values()) p.reject(err)
    this.pending.clear()
  }

  private requestRaw(
    msg: WorkerRequest,
    onChunk?: (bytes: Uint8Array) => void,
  ): Promise<{ value: unknown; inTx: boolean }> {
    if (this.closed) return Promise.reject(new Error('WorkerCell is closed'))
    const id = this.nextId++
    const promise = new Promise<{ value: unknown; inTx: boolean }>(
      (resolve, reject) => {
        this.pending.set(id, { resolve, reject, onChunk })
      },
    )
    this.worker.postMessage({ ...msg, id })
    return promise
  }

  /** @internal */
  async _request(
    msg: WorkerRequest,
    onChunk?: (bytes: Uint8Array) => void,
  ): Promise<unknown> {
    return (await this.requestRaw(msg, onChunk)).value
  }

  /** @internal host-side mutex for db.runExclusive parity. */
  _exclusive<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.chain.then(fn)
    this.chain = run.catch(() => undefined)
    return run
  }

  private async native(
    fn: NativeFn,
    args: (number | bigint)[] = [],
  ): Promise<unknown> {
    return this._request({ t: 'native', fn, args })
  }

  // -------------------------------------------------------------------
  // Cell surface
  // -------------------------------------------------------------------

  async bookmark(): Promise<bigint> {
    const res = (await this.dbFacade.query<{ lsn: string }>(
      `select pg_current_wal_insert_lsn()::text as lsn`,
    )) as { rows: { lsn: string }[] }
    return parseLsn(res.rows[0].lsn)
  }

  /** Same contract as Cell.captureSlice; WAL bytes are read HOST-side from
   *  the real datadir (passthrough mode). */
  async captureSlice(): Promise<CapturedSlice | null> {
    const end = await this.bookmark()
    if (end === this.cursor) return null
    await this.flushWal()
    return {
      baseLsn: this.cursor,
      endLsn: end,
      bytes: readWalRange(this.dir, this.cursor, end),
    }
  }

  async flushWal(): Promise<void> {
    await this.native('flush_wal')
  }

  async commitGatePending(): Promise<number> {
    return (await this.native('commit_gate_pending')) as number
  }

  async commitGateRun(): Promise<void> {
    const rc = (await this.native('commit_gate_run')) as number
    if (rc !== 0) {
      throw new Error(`commitGateRun: native truncate run failed (${rc})`)
    }
  }

  async commitGateDiscard(): Promise<void> {
    await this.native('commit_gate_discard')
  }

  /** Sync like Cell; the base re-snapshot runs in the background — await
   *  `settled()` before relying on `baseSnapshot`. */
  confirmPublished(endLsn: bigint, opts: { flush?: boolean } = {}): void {
    this.cursor = endLsn
    this.scheduleSnapshot(opts)
  }

  advanceTo(endLsn: bigint, opts: { flush?: boolean } = {}): void {
    this.cursor = endLsn
    this.scheduleSnapshot(opts)
  }

  private scheduleSnapshot(opts: { flush?: boolean }): void {
    this.snapshotChain = this.snapshotChain
      .then(() => this.maybeSnapshotBase(opts))
      .catch(() => undefined)
  }

  /** All background base-snapshot work has completed. */
  async settled(): Promise<void> {
    await this.snapshotChain
  }

  async maybeSnapshotBase(opts: { flush?: boolean } = {}): Promise<void> {
    // Same logic as Cell.maybeSnapshotBase, over the message channel. One
    // identity probe first (cheap); flush_base + re-read only if anchored.
    let id = (await this._request({
      t: 'identity',
      flushBase: false,
    })) as WorkerIdentity | null
    if (id === null || id.insertLsn !== this.cursor) return
    if (opts.flush !== false) {
      id = (await this._request({
        t: 'identity',
        flushBase: true,
      })) as WorkerIdentity | null
      if (id === null) return
    }
    this.base = {
      lsn: this.cursor,
      prevRecLsn: id.prevRecLsn,
      nextXid: id.nextXid,
      nextOid: id.nextOid,
      nextMulti: id.nextMulti,
      nextOffset: id.nextOffset,
      writes: id.writes,
    }
  }

  async canResetInPlace(): Promise<boolean> {
    await this.settled()
    if (this.base === null || this.base.lsn !== this.cursor) {
      resetStats.fallbackRecycle++
      resetStats.reasons.set(
        'no-base-snapshot',
        (resetStats.reasons.get('no-base-snapshot') ?? 0) + 1,
      )
      return false
    }
    const writes = (await this.native('storage_write_count')) as bigint
    if (writes !== this.base.writes) {
      resetStats.fallbackRecycle++
      resetStats.reasons.set(
        'storage-writes-since-base',
        (resetStats.reasons.get('storage-writes-since-base') ?? 0) + 1,
      )
      return false
    }
    return true
  }

  async resetToBase(): Promise<void> {
    await this.settled()
    const base = this.base
    if (base === null || base.lsn !== this.cursor) {
      throw new Error('resetToBase: no base snapshot at cursor')
    }
    const rc = (await this.native('reset_to_base', [
      base.lsn,
      base.prevRecLsn,
      base.nextXid,
      base.nextOid,
      base.nextMulti,
      base.nextOffset,
    ])) as number
    if (rc !== 0) {
      throw new Error(`resetToBase: native reset failed (${rc})`)
    }
    resetStats.inPlace++
  }

  async setSequenceLease(seqOid: number, leaseEnd: bigint): Promise<void> {
    await this.native('set_sequence_lease', [seqOid, leaseEnd])
  }

  async clearSequenceLeases(): Promise<void> {
    await this.native('clear_sequence_leases')
  }

  async resetSequenceCaches(): Promise<void> {
    await this.native('reset_sequence_caches')
  }

  async readSetBegin(): Promise<void> {
    await this.native('readset_enable', [1])
  }

  async readSetEnd(): Promise<void> {
    await this.native('readset_enable', [0])
  }

  async readSetSnapshot(): Promise<ReadSetSnapshot> {
    return (await this._request({ t: 'read-set' })) as ReadSetSnapshot
  }

  async pageLsn(
    spc: number,
    db: number,
    rel: number,
    fork: number,
    blk: number,
  ): Promise<bigint> {
    return (await this.native('page_lsn', [spc, db, rel, fork, blk])) as bigint
  }

  async relationNblocks(
    spc: number,
    db: number,
    rel: number,
    fork: number,
  ): Promise<number> {
    return (await this.native('relation_nblocks', [
      spc,
      db,
      rel,
      fork,
    ])) as number
  }

  /** @internal notification relay registration (W1 gap closed). */
  _onNotification(cb: (channel: string, payload: string) => void): () => void {
    this.notifyListeners.add(cb)
    return () => this.notifyListeners.delete(cb)
  }

  /**
   * W3: run the M5 live-apply pipeline INSIDE the worker (the Module and
   * the LazyCellFS overlay hooks live there). The slice bytes must already
   * sit in the datadir's pg_wal (host-side writeWalRange — pg_wal files
   * are plain local files in both modes).
   */
  async applyLiveTail(start: bigint, end: bigint): Promise<LiveApplyResult> {
    return (await this._request({
      t: 'live-apply',
      start,
      end,
    })) as LiveApplyResult
  }

  /** W3: classified WAL records of [start, end), scanned worker-side. */
  async walscanRange(start: bigint, end: bigint): Promise<WalRecord[]> {
    return (await this._request({
      t: 'walscan-range',
      start,
      end,
    })) as WalRecord[]
  }

  /** W3 byte counters: worker-side overlay stats + host-served faults. */
  async lazyStats(): Promise<
    LazyCellFsStats & { hostFaults: number; hostFaultBytes: number }
  > {
    const fsStats = (await this._request({
      t: 'lazy-stats',
    })) as LazyCellFsStats
    return {
      ...fsStats,
      hostFaults: this.hostFaults,
      hostFaultBytes: this.hostFaultBytes,
    }
  }

  /** Same contract as Cell.closeClean: clean-close the instance in the
   *  worker, then read the shutdown checkpoint + detach slice HOST-side
   *  from the real datadir. Also terminates the worker. */
  async closeClean(): Promise<{ detachSlice: CapturedSlice | null }> {
    await this.settled()
    await this._closeDb()
    await this.terminate()
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

  /** @internal clean-close the PGlite instance only (worker stays up). */
  async _closeDb(): Promise<void> {
    await this._request({ t: 'close' })
  }

  /** Test-only: make the worker die mid-flight (no reply ever arrives). */
  crashSimulate(): void {
    if (this.closed) return
    this.worker.postMessage({ t: 'crash-simulate', id: this.nextId++ })
  }

  /** Hard-kill the worker. In-flight requests reject; the host stays
   *  healthy. Idempotent. */
  async terminate(): Promise<void> {
    if (this.closed) return
    this.closed = true
    this.bridgeStopped = true
    // Unblock the bridge loop if it is awaiting a seq change.
    Atomics.add(this.ctl, CTL_REQ_SEQ, 1)
    Atomics.notify(this.ctl, CTL_REQ_SEQ)
    await this.worker.terminate()
    this.failAll(new Error('WorkerCell terminated'))
  }
}
