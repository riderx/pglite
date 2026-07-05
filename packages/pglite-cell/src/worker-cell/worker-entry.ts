// M7 W1 — the worker side: hosts the PGlite instance on a plain NodeFS
// datadir (PASSTHROUGH mode — laziness is W3), services exec/query messages
// strictly sequentially, and implements the worker half of the SAB fault
// bridge: `hostcall()` stages a request, `Atomics.wait`s, and copies the
// response out of the data SAB after waking (M7 fixed decisions 1+2).

import { parentPort, workerData } from 'node:worker_threads'
import { PGlite } from '@electric-sql/pglite'
import {
  CTL_STATE,
  CTL_REQ_SEQ,
  CTL_OP,
  CTL_REQ_LEN,
  CTL_PARAM0,
  CTL_PARAM1,
  CTL_PARAM2,
  CTL_PARAM3,
  CTL_RESP_STATUS,
  CTL_RESP_LEN,
  STATE_IDLE,
  STATE_REQUEST,
  STATE_RESPONSE,
} from './protocol'
import type {
  HostToWorker,
  WorkerBoot,
  WorkerToHost,
  WorkerIdentity,
  BurstResult,
  NativeFn,
} from './protocol'

const port = parentPort
if (port === null) throw new Error('worker-entry must run in a Worker')
const boot = workerData as WorkerBoot
const ctl = new Int32Array(boot.control)
const dataSab = new Uint8Array(boot.data)

let pg: PGlite | null = null

function post(msg: WorkerToHost, transfer?: ArrayBuffer[]): void {
  port!.postMessage(msg, transfer)
}

// ---------------------------------------------------------------------------
// SAB bridge, worker side (synchronous — usable from inside WASM callbacks)
// ---------------------------------------------------------------------------

/**
 * Synchronous call INTO the host thread: stage request bytes + params in
 * the data SAB, bump the request seq (the host `Atomics.waitAsync`es on
 * it), block on the state word, then copy the response out. At most one
 * hostcall is ever in flight (this thread blocks), so offset 0 staging is
 * exclusive by construction.
 */
export function hostcall(
  op: number,
  payload: Uint8Array,
  params: [number, number, number, number] = [0, 0, 0, 0],
): { status: number; bytes: Uint8Array } {
  if (payload.byteLength > dataSab.byteLength) {
    throw new Error(
      `hostcall payload ${payload.byteLength} exceeds data SAB ${dataSab.byteLength}`,
    )
  }
  dataSab.set(payload, 0)
  ctl[CTL_OP] = op
  ctl[CTL_REQ_LEN] = payload.byteLength
  ctl[CTL_PARAM0] = params[0]
  ctl[CTL_PARAM1] = params[1]
  ctl[CTL_PARAM2] = params[2]
  ctl[CTL_PARAM3] = params[3]
  Atomics.store(ctl, CTL_STATE, STATE_REQUEST)
  Atomics.add(ctl, CTL_REQ_SEQ, 1)
  Atomics.notify(ctl, CTL_REQ_SEQ)
  // Wait until the host flips REQUEST -> RESPONSE (spurious-wake safe).
  while (Atomics.load(ctl, CTL_STATE) === STATE_REQUEST) {
    Atomics.wait(ctl, CTL_STATE, STATE_REQUEST)
  }
  if (Atomics.load(ctl, CTL_STATE) !== STATE_RESPONSE) {
    throw new Error(`hostcall: unexpected bridge state ${ctl[CTL_STATE]}`)
  }
  const status = ctl[CTL_RESP_STATUS]
  const len = ctl[CTL_RESP_LEN]
  const bytes = new Uint8Array(len)
  bytes.set(dataSab.subarray(0, len))
  Atomics.store(ctl, CTL_STATE, STATE_IDLE)
  return { status, bytes }
}

// ---------------------------------------------------------------------------
// Message handlers (sequential — the worker's own runExclusive)
// ---------------------------------------------------------------------------

function db(): PGlite {
  if (pg === null) throw new Error('worker: no open PGlite instance')
  return pg
}

function readIdentity(flushBase: boolean): WorkerIdentity | null {
  const mod = db().Module
  if (flushBase && mod._pgl_flush_base() !== 0) return null
  const ptr = mod._pgl_get_identity()
  if (ptr === 0) return null
  const raw = JSON.parse(mod.UTF8ToString(ptr)) as {
    nextXid: string
    nextOid: number
    nextMulti: number
    nextOffset: number
    prevRecLsn: string
    insertLsn: string
    writes: string
  }
  return {
    nextXid: BigInt(raw.nextXid),
    nextOid: raw.nextOid,
    nextMulti: raw.nextMulti,
    nextOffset: raw.nextOffset,
    prevRecLsn: BigInt(raw.prevRecLsn),
    insertLsn: BigInt(raw.insertLsn),
    writes: BigInt(raw.writes),
  }
}

function callNative(fn: NativeFn, args: (number | bigint)[]): unknown {
  const mod = db().Module
  const n = (i: number): number => Number(args[i])
  const b = (i: number): bigint => BigInt(args[i])
  switch (fn) {
    case 'flush_wal':
      return mod._pgl_flush_wal()
    case 'commit_gate_set':
      return mod._pgl_commit_gate_set(n(0))
    case 'commit_gate_pending':
      return mod._pgl_commit_gate_pending()
    case 'commit_gate_run':
      return mod._pgl_commit_gate_run()
    case 'commit_gate_discard':
      return mod._pgl_commit_gate_discard()
    case 'set_sequence_lease':
      return mod._pgl_set_sequence_lease(n(0), b(1))
    case 'clear_sequence_leases':
      return mod._pgl_clear_sequence_leases()
    case 'reset_sequence_caches':
      return mod._pgl_reset_sequence_caches()
    case 'readset_enable':
      return mod._pgl_readset_enable(n(0))
    case 'page_lsn':
      return mod._pgl_page_lsn(n(0), n(1), n(2), n(3), n(4))
    case 'relation_nblocks':
      return mod._pgl_relation_nblocks(n(0), n(1), n(2), n(3))
    case 'storage_write_count':
      return mod._pgl_storage_write_count()
    case 'reset_to_base':
      return mod._pgl_reset_to_base(b(0), b(1), b(2), n(3), n(4), n(5))
  }
}

function readSetSnapshot(): {
  pins: {
    spc: number
    db: number
    rel: number
    fork: number
    blk: number
  }[]
  nblocks: {
    spc: number
    db: number
    rel: number
    fork: number
    nblocks: number
  }[]
  overflowed: boolean
} {
  // Same decode as Cell.readSetSnapshot (cell.ts) — runs worker-side
  // because the ring lives in this thread's WASM heap.
  const mod = db().Module
  const count = mod._pgl_readset_count()
  const ptr = mod._pgl_readset_snapshot()
  const words = new Uint32Array(mod.HEAPU8.buffer, ptr, count * 5)
  const pins = []
  const nblocks = []
  for (let i = 0; i < count; i++) {
    const o = i * 5
    const kindFork = words[o + 3]
    const kind = kindFork >>> 24
    const fork = kindFork & 0xffffff
    const entry = {
      spc: words[o],
      db: words[o + 1],
      rel: words[o + 2],
      fork,
    }
    if (kind === 0) pins.push({ ...entry, blk: words[o + 4] })
    else nblocks.push({ ...entry, nblocks: words[o + 4] })
  }
  return { pins, nblocks, overflowed: mod._pgl_readset_overflowed() === 1 }
}

function runBurst(msg: {
  op: number
  param0?: number
  payloadBytes: number
  count: number
}): BurstResult {
  const payload = new Uint8Array(msg.payloadBytes)
  for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff
  const lat: number[] = []
  let verified = true
  for (let i = 0; i < msg.count; i++) {
    payload[0] = i & 0xff // vary so echo checks are per-call
    const t0 = process.hrtime.bigint()
    const res = hostcall(msg.op, payload, [msg.param0 ?? 0, i, 0, 0])
    lat.push(Number(process.hrtime.bigint() - t0))
    if (
      res.status !== 0 ||
      res.bytes.length !== payload.length ||
      res.bytes[0] !== (i & 0xff) ||
      (payload.length > 1 &&
        res.bytes[payload.length - 1] !== payload[payload.length - 1])
    ) {
      verified = false
    }
  }
  lat.sort((a, b) => a - b)
  const pick = (q: number): number =>
    lat[Math.min(lat.length - 1, Math.floor(q * lat.length))]
  return {
    count: msg.count,
    medianNs: pick(0.5),
    meanNs: lat.reduce((a, v) => a + v, 0) / lat.length,
    p99Ns: pick(0.99),
    maxNs: lat[lat.length - 1],
    verified,
  }
}

async function handle(msg: HostToWorker): Promise<void> {
  const done = (value: unknown): void =>
    post({ t: 'done', id: msg.id, value, inTx: pg?.isInTransaction() ?? false })
  try {
    switch (msg.t) {
      case 'open': {
        if (pg !== null) throw new Error('worker: already open')
        pg = new PGlite(msg.dir)
        await pg.waitReady
        done(null)
        return
      }
      case 'query':
        done(await db().query(msg.sql, msg.params))
        return
      case 'exec':
        await db().exec(msg.sql)
        done(null)
        return
      case 'exec-unit': {
        await db().runExclusive(() =>
          db().execProtocolRawStream(msg.bytes, {
            syncToFs: msg.syncToFs,
            onRawData: (chunk) => {
              // Copy: chunk may alias WASM memory / an internal buffer.
              const bytes = new Uint8Array(chunk.byteLength)
              bytes.set(chunk)
              post({ t: 'chunk', id: msg.id, bytes }, [bytes.buffer])
            },
          }),
        )
        done(null)
        return
      }
      case 'native':
        done(callNative(msg.fn, msg.args))
        return
      case 'identity':
        done(readIdentity(msg.flushBase))
        return
      case 'read-set':
        done(readSetSnapshot())
        return
      case 'hostcall-burst':
        done(runBurst(msg))
        return
      case 'close':
        if (pg !== null) {
          await pg.close()
          pg = null
        }
        done(null)
        return
      case 'crash-simulate':
        // Handled out-of-band in the message listener (must not queue
        // behind in-flight work). Nothing to do here.
        return
    }
  } catch (err) {
    post({
      t: 'fail',
      id: msg.id,
      error: err instanceof Error ? (err.stack ?? err.message) : String(err),
    })
  }
}

// Strictly sequential service loop: messages queue behind `chain`.
// Exception: crash-simulate bypasses the queue — it must kill the thread
// even while a long exec is in flight (that is the point of the test).
let chain: Promise<void> = Promise.resolve()
port.on('message', (msg: HostToWorker) => {
  if (msg.t === 'crash-simulate') {
    process.exit(1)
  }
  chain = chain.then(() => handle(msg))
})
post({ t: 'ready' })
