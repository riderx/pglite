// M7 W1 — worker-cell wire protocol: the SAB fault-bridge control block and
// the postMessage lifecycle/exec protocol between the host (`WorkerCell`)
// and the worker (`worker-entry`).
//
// Control-block shape ported (with changes) from the salvage reference
// `codex/durable-vfs-plan`:`packages/pglite-durable-vfs/src/sab/
// sab-control-block.ts` — same idea (Int32Array state/status/length words
// over a SharedArrayBuffer + a data SAB for payload bytes), inverted
// direction: THERE the main thread blocked and a fetch worker served; HERE
// the worker blocks (`Atomics.wait`) and the HOST serves via
// `Atomics.waitAsync` (M7 fixed decision 1).

// ---------------------------------------------------------------------------
// Control SAB layout (Int32Array indices)
// ---------------------------------------------------------------------------

/** Bridge state word: IDLE -> (worker) REQUEST -> (host) RESPONSE -> IDLE. */
export const CTL_STATE = 0
/** Request sequence: worker increments AFTER staging a request. The host
 *  `Atomics.waitAsync`es on this word — never on a poll. */
export const CTL_REQ_SEQ = 1
/** Requested operation (one of the OP_* codes). */
export const CTL_OP = 2
/** Request payload length in the data SAB (bytes, from offset 0). */
export const CTL_REQ_LEN = 3
/** Four raw i32 request params (op-specific; fault-read will use these for
 *  ref/chunk addressing in W3). */
export const CTL_PARAM0 = 4
export const CTL_PARAM1 = 5
export const CTL_PARAM2 = 6
export const CTL_PARAM3 = 7
/** Response status (0 = OK; see STATUS_*). */
export const CTL_RESP_STATUS = 8
/** Response payload length in the data SAB (bytes, from offset 0). */
export const CTL_RESP_LEN = 9

export const CONTROL_WORDS = 16 // headroom for W3 params

// CTL_STATE values.
export const STATE_IDLE = 0
export const STATE_REQUEST = 1
export const STATE_RESPONSE = 2

// Response statuses (superset reserved for W3 fault reads; provenance:
// SAB_STATUS_* in the salvage control block).
export const STATUS_OK = 0
export const STATUS_UNKNOWN_OP = 1
export const STATUS_NOT_FOUND = 2
export const STATUS_TOO_LARGE = 3
export const STATUS_HOST_ERROR = 4

// ---------------------------------------------------------------------------
// Hostcall op space (v1: ping + generic hostcall; fault-read RESERVED)
// ---------------------------------------------------------------------------

/** Echo: response payload = request payload. Bridge liveness + latency. */
export const OP_PING = 1
/** Generic hostcall: payload in/out, dispatched to a host-registered
 *  handler keyed by CTL_PARAM0. Exercised by tests in v1 so the bridge is
 *  proven under load before any FS faults ride it. */
export const OP_HOSTCALL = 2
/** RESERVED (W3): LazyCellFS chunk fault. params = (refIdx, chunkIdx,
 *  offsetLo, offsetHi) — do not reuse. */
export const OP_FAULT_READ = 3

/** Default data-SAB staging size. 4 MiB per M7 fixed decision 1. The worker
 *  blocks on every hostcall, so at most ONE request is ever staged — the
 *  "ring" degenerates to a single slot at offset 0 in v1 (W3 keeps the same
 *  layout; multi-slot only if a second requester ever exists). */
export const DEFAULT_DATA_BYTES = 4 * 1024 * 1024

export interface BridgeBuffers {
  control: SharedArrayBuffer
  data: SharedArrayBuffer
}

export function createBridgeBuffers(
  dataBytes: number = DEFAULT_DATA_BYTES,
): BridgeBuffers {
  return {
    control: new SharedArrayBuffer(CONTROL_WORDS * 4),
    data: new SharedArrayBuffer(dataBytes),
  }
}

// ---------------------------------------------------------------------------
// postMessage protocol (exec units + lifecycle; M7 fixed decision 2)
// ---------------------------------------------------------------------------

/** workerData handed to the worker at spawn. */
export interface WorkerBoot {
  control: SharedArrayBuffer
  data: SharedArrayBuffer
}

/** Host -> worker requests. Every request carries a unique `id`; the worker
 *  replies with exactly one `done`/`fail`, preceded by zero or more `chunk`
 *  messages for `exec-unit`. Requests are serviced STRICTLY sequentially
 *  (the worker's own runExclusive). */
export type HostToWorker =
  | { t: 'open'; id: number; dir: string }
  | { t: 'query'; id: number; sql: string; params?: unknown[] }
  | { t: 'exec'; id: number; sql: string }
  | { t: 'exec-unit'; id: number; bytes: Uint8Array; syncToFs?: boolean }
  | { t: 'native'; id: number; fn: NativeFn; args: (number | bigint)[] }
  | { t: 'identity'; id: number; flushBase: boolean }
  | { t: 'read-set'; id: number }
  | {
      // Test-only in v1: drive `count` hostcalls of `payloadBytes` through
      // the SAB bridge from inside the worker, returning per-call latencies.
      t: 'hostcall-burst'
      id: number
      op: number
      param0?: number
      payloadBytes: number
      count: number
    }
  | { t: 'close'; id: number }
  | { t: 'crash-simulate'; id: number }

/** The native calls WorkerCell routes to the worker (the Module handle
 *  lives there). Args are i32s except where noted bigint. */
export type NativeFn =
  | 'flush_wal'
  | 'commit_gate_set'
  | 'commit_gate_pending'
  | 'commit_gate_run'
  | 'commit_gate_discard'
  | 'set_sequence_lease' // (oid, leaseEnd: bigint)
  | 'clear_sequence_leases'
  | 'reset_sequence_caches'
  | 'readset_enable'
  | 'page_lsn' // -> bigint
  | 'relation_nblocks'
  | 'storage_write_count' // -> bigint
  | 'reset_to_base' // (lsn, prevRecLsn, nextXid: bigint; oid, multi, offset)

/** Worker -> host messages. */
export type WorkerToHost =
  | { t: 'ready' }
  | { t: 'chunk'; id: number; bytes: Uint8Array }
  | { t: 'done'; id: number; value: unknown; inTx: boolean }
  | { t: 'fail'; id: number; error: string }

/** Identity snapshot as returned by the worker (parsed pgl_get_identity). */
export interface WorkerIdentity {
  nextXid: bigint
  nextOid: number
  nextMulti: number
  nextOffset: number
  prevRecLsn: bigint
  insertLsn: bigint
  writes: bigint
}

/** hostcall-burst result: latencies in nanoseconds (bigint-safe). */
export interface BurstResult {
  count: number
  medianNs: number
  meanNs: number
  p99Ns: number
  maxNs: number
  /** Each response was verified to echo the request payload. */
  verified: boolean
}
