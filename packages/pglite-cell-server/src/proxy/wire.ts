// Low-level wire helpers for the M1d session proxy (§3.5): frontend message
// framing (incl. the length-first startup/SSL/cancel special cases),
// frontend classification into protocol UNITS (one simple 'Q', or an
// extended-protocol batch closed by Sync), hand-rolled backend synthesis
// (ErrorResponse / ReadyForQuery — `@electric-sql/pg-protocol` parses
// backend messages but cannot serialize them), and backend response
// scanning (trailing ReadyForQuery status, error presence, last command
// tag) via the pg-protocol `Parser`.

import { Parser } from '@electric-sql/pg-protocol'

export const SSL_REQUEST_CODE = 80877103
export const CANCEL_REQUEST_CODE = 80877102
export const STARTUP_PROTOCOL_VERSION = 196608 // PostgreSQL protocol 3.0

/** Frontend message type bytes the proxy branches on. */
export const FRONTEND = {
  Query: 0x51, // 'Q'
  Terminate: 0x58, // 'X'
  Sync: 0x53, // 'S'
  Parse: 0x50, // 'P'
  Bind: 0x42, // 'B'
  Describe: 0x44, // 'D'
  Execute: 0x45, // 'E'
  Close: 0x43, // 'C'
  FunctionCall: 0x46, // 'F'
  Flush: 0x48, // 'H'
  CopyFail: 0x66, // 'f'
  CopyData: 0x64, // 'd'
  CopyDone: 0x63, // 'c'
} as const

const EXTENDED_UNIT_PARTS = new Set<number>([
  FRONTEND.Parse,
  FRONTEND.Bind,
  FRONTEND.Describe,
  FRONTEND.Execute,
  FRONTEND.Close,
  FRONTEND.FunctionCall,
  FRONTEND.Flush,
  FRONTEND.CopyFail,
  FRONTEND.CopyData,
  FRONTEND.CopyDone,
])

/** Extended-protocol messages that accumulate until a Sync closes the unit. */
export function isExtendedUnitPart(code: number): boolean {
  return EXTENDED_UNIT_PARTS.has(code)
}

/** A malformed or unsupported frontend byte stream (connection is dropped). */
export class WireProtocolError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'WireProtocolError'
  }
}

export type FrontendFrame =
  | { kind: 'ssl' }
  | { kind: 'cancel' }
  | { kind: 'startup'; bytes: Uint8Array; params: Record<string, string> }
  | { kind: 'typed'; code: number; bytes: Uint8Array }

/**
 * Accumulates raw socket bytes and yields complete frontend messages.
 * Before the StartupMessage, packets are length-first (SSLRequest,
 * CancelRequest, StartupMessage); after it, every message is
 * `type byte + i32 length` (length includes itself, excludes the type).
 */
export class FrontendFrameReader {
  private buf: Buffer = Buffer.alloc(0)
  private startupSeen = false

  push(data: Uint8Array): void {
    this.buf =
      this.buf.length === 0
        ? Buffer.from(data)
        : Buffer.concat([this.buf, Buffer.from(data)])
  }

  private consume(n: number): Uint8Array {
    const out = new Uint8Array(this.buf.subarray(0, n))
    this.buf = this.buf.subarray(n)
    return out
  }

  /** The next complete frame, or null when more bytes are needed. */
  next(): FrontendFrame | null {
    if (!this.startupSeen) {
      if (this.buf.length < 8) return null
      const len = this.buf.readInt32BE(0)
      const code = this.buf.readInt32BE(4)
      if (len === 8 && code === SSL_REQUEST_CODE) {
        this.consume(8)
        return { kind: 'ssl' }
      }
      if (len === 16 && code === CANCEL_REQUEST_CODE) {
        if (this.buf.length < 16) return null
        this.consume(16)
        return { kind: 'cancel' }
      }
      if (code === STARTUP_PROTOCOL_VERSION) {
        if (len < 9 || len > 1024 * 1024) {
          throw new WireProtocolError(`implausible startup length ${len}`)
        }
        if (this.buf.length < len) return null
        const bytes = this.consume(len)
        this.startupSeen = true
        return { kind: 'startup', bytes, params: parseStartupParams(bytes) }
      }
      throw new WireProtocolError(
        `unsupported pre-startup packet (length ${len}, code ${code})`,
      )
    }

    if (this.buf.length < 5) return null
    const len = this.buf.readInt32BE(1)
    if (len < 4) throw new WireProtocolError(`invalid message length ${len}`)
    const total = 1 + len
    if (this.buf.length < total) return null
    const bytes = this.consume(total)
    return { kind: 'typed', code: bytes[0], bytes }
  }
}

/** Parse the key/value parameter block of a StartupMessage. */
export function parseStartupParams(bytes: Uint8Array): Record<string, string> {
  const params: Record<string, string> = {}
  const buf = Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  let off = 8
  while (off < buf.length) {
    const keyEnd = buf.indexOf(0, off)
    if (keyEnd < 0 || keyEnd === off) break // terminator (empty key) or junk
    const key = buf.toString('utf8', off, keyEnd)
    const valEnd = buf.indexOf(0, keyEnd + 1)
    if (valEnd < 0) break
    params[key] = buf.toString('utf8', keyEnd + 1, valEnd)
    off = valEnd + 1
  }
  return params
}

/** The SQL text of a simple-protocol 'Q' message. */
export function simpleQueryText(bytes: Uint8Array): string {
  // 'Q' + i32 len + cstring
  const end =
    bytes.length > 5 && bytes[bytes.length - 1] === 0
      ? bytes.length - 1
      : bytes.length
  return Buffer.from(bytes.buffer, bytes.byteOffset + 5, end - 5).toString(
    'utf8',
  )
}

export function concatBytes(chunks: Uint8Array[]): Uint8Array {
  let total = 0
  for (const c of chunks) total += c.length
  const out = new Uint8Array(total)
  let off = 0
  for (const c of chunks) {
    out.set(c, off)
    off += c.length
  }
  return out
}

// ---------------------------------------------------------------------------
// Backend synthesis (pg-protocol cannot serialize backend messages).
// ---------------------------------------------------------------------------

export interface ErrorFields {
  severity?: string
  code: string
  message: string
  detail?: string
  hint?: string
}

/** The §4.0 serialization-failure wire error, text verbatim. */
export const SERIALIZATION_CONFLICT_FIELDS: ErrorFields = {
  severity: 'ERROR',
  code: '40001',
  message: 'could not serialize access due to concurrent update',
  detail: 'transaction conflicted with a concurrent commit on this database',
  hint: 'retry the transaction',
}

/**
 * Hand-rolled ErrorResponse: 'E' + i32 length + one byte-tagged cstring per
 * field (S severity, V non-localized severity, C SQLSTATE, M message,
 * D detail, H hint) + NUL terminator.
 */
export function errorResponse(fields: ErrorFields): Uint8Array {
  const severity = fields.severity ?? 'ERROR'
  const parts: [string, string][] = [
    ['S', severity],
    ['V', severity],
    ['C', fields.code],
    ['M', fields.message],
  ]
  if (fields.detail !== undefined) parts.push(['D', fields.detail])
  if (fields.hint !== undefined) parts.push(['H', fields.hint])

  let payload = 4 // the i32 length itself
  for (const [, value] of parts) payload += 1 + Buffer.byteLength(value) + 1
  payload += 1 // trailing NUL

  const out = Buffer.alloc(1 + payload)
  out.write('E', 0)
  out.writeInt32BE(payload, 1)
  let off = 5
  for (const [tag, value] of parts) {
    off += out.write(tag, off)
    off += out.write(value, off)
    out[off++] = 0
  }
  out[off] = 0
  return new Uint8Array(out)
}

/**
 * Hand-rolled NoticeResponse ('N'): identical field encoding to
 * ErrorResponse, different message type byte. Used to inject the M6
 * advisory-lock WARNING ahead of a statement's output (§4.6 'local-warn').
 */
export function noticeResponse(fields: ErrorFields): Uint8Array {
  const out = errorResponse(fields)
  out[0] = 0x4e // 'N'
  return out
}

/** Hand-rolled CommandComplete: 'C' + i32 length + tag cstring. */
export function commandComplete(tag: string): Uint8Array {
  const tagLen = Buffer.byteLength(tag)
  const out = Buffer.alloc(1 + 4 + tagLen + 1)
  out.write('C', 0)
  out.writeInt32BE(4 + tagLen + 1, 1)
  out.write(tag, 5)
  out[5 + tagLen] = 0
  return new Uint8Array(out)
}

/**
 * Hand-rolled NotificationResponse ('A'): i32 pid + channel cstring +
 * payload cstring. The proxy synthesizes these for tailer-driven delivery
 * (M3, §10.2) — `pid` is a fixed synthetic backend pid.
 */
export function notificationResponse(
  pid: number,
  channel: string,
  payload: string,
): Uint8Array {
  const chLen = Buffer.byteLength(channel)
  const plLen = Buffer.byteLength(payload)
  const len = 4 + 4 + chLen + 1 + plLen + 1
  const out = Buffer.alloc(1 + len)
  out.write('A', 0)
  out.writeInt32BE(len, 1)
  out.writeInt32BE(pid, 5)
  out.write(channel, 9)
  out[9 + chLen] = 0
  out.write(payload, 9 + chLen + 1)
  out[9 + chLen + 1 + plLen] = 0
  return new Uint8Array(out)
}

/**
 * Strip NotificationResponse ('A') messages from a buffered backend
 * response (M3, §10.2 uniform delivery): ALL client-facing notification
 * delivery is tailer-driven, so raw 'A' bytes a unit execution produced
 * locally must never reach the client — a light type+length walk, no
 * parsing. Returns the input array unchanged when no 'A' is present.
 */
export function stripNotificationResponses(output: Uint8Array): Uint8Array {
  return extractNotificationResponses(output).stripped
}

/**
 * The harvest + strip walk in one pass (M3): PGlite's raw-stream exec path
 * bypasses its protocol parser, so the notifications a unit's local commit
 * fired exist ONLY as 'A' messages in the raw output — decode them (i32
 * pid + channel cstring + payload cstring) into the harvest list and strip
 * them from the client-bound bytes. Returns the input array unchanged
 * (zero-copy) when no 'A' is present.
 */
export function extractNotificationResponses(output: Uint8Array): {
  stripped: Uint8Array
  notifications: { channel: string; payload: string }[]
} {
  const view = new DataView(output.buffer, output.byteOffset, output.byteLength)
  let hasA = false
  for (let pos = 0; pos + 5 <= output.length; ) {
    const len = view.getInt32(pos + 1)
    if (output[pos] === 0x41 /* 'A' */) {
      hasA = true
      break
    }
    pos += 1 + len
  }
  if (!hasA) return { stripped: output, notifications: [] }
  const kept: Uint8Array[] = []
  const notifications: { channel: string; payload: string }[] = []
  const buf = Buffer.from(output.buffer, output.byteOffset, output.byteLength)
  for (let pos = 0; pos + 5 <= output.length; ) {
    const len = view.getInt32(pos + 1)
    const end = Math.min(pos + 1 + len, output.length)
    if (output[pos] === 0x41) {
      // 'A' + i32 len + i32 pid + channel cstring + payload cstring
      const chStart = pos + 9
      const chEnd = buf.indexOf(0, chStart)
      const plStart = chEnd + 1
      const plEnd = buf.indexOf(0, plStart)
      if (chEnd >= 0 && plEnd >= 0 && plEnd < end) {
        notifications.push({
          channel: buf.toString('utf8', chStart, chEnd),
          payload: buf.toString('utf8', plStart, plEnd),
        })
      }
    } else {
      kept.push(output.subarray(pos, end))
    }
    pos = end
  }
  return { stripped: concatBytes(kept), notifications }
}

/** Hand-rolled ReadyForQuery: 'Z' + i32(5) + status byte. */
export function readyForQuery(status: 'I' | 'T' | 'E'): Uint8Array {
  const out = Buffer.alloc(6)
  out.write('Z', 0)
  out.writeInt32BE(5, 1)
  out.write(status, 5)
  return new Uint8Array(out)
}

// ---------------------------------------------------------------------------
// Backend response scanning.
// ---------------------------------------------------------------------------

export interface BackendScan {
  /** Status of the LAST ReadyForQuery in the buffer (null if none). */
  rfqStatus: 'I' | 'T' | 'E' | null
  /** Any ErrorResponse present. */
  hasError: boolean
  /** SQLSTATE of the first ErrorResponse (if any). */
  errorCode: string | null
  /** DETAIL of the first ErrorResponse (if any) — carries the native
   *  `sequence lease exhausted` renew signal (M5a, §5.3). */
  errorDetail: string | null
  /** Tag of the last CommandComplete (e.g. 'ROLLBACK', 'INSERT 0 1'). */
  lastCommandTag: string | null
}

/**
 * Scan a complete buffered backend response with the pg-protocol Parser:
 * the trailing ReadyForQuery status, ErrorResponse presence (whether the
 * transaction the unit ran aborted), and the last command tag (a trailing
 * `ROLLBACK` at transaction end is an abort in disguise).
 */
export function scanBackendOutput(output: Uint8Array): BackendScan {
  const scan: BackendScan = {
    rfqStatus: null,
    hasError: false,
    errorCode: null,
    errorDetail: null,
    lastCommandTag: null,
  }
  if (output.length === 0) return scan
  const parser = new Parser()
  parser.parse(Buffer.from(output), (msg) => {
    if (msg.name === 'readyForQuery') {
      const status = (msg as unknown as { status: string }).status
      if (status === 'I' || status === 'T' || status === 'E') {
        scan.rfqStatus = status
      }
    } else if (msg.name === 'error') {
      scan.hasError = true
      if (scan.errorCode === null) {
        scan.errorCode = (msg as unknown as { code?: string }).code ?? 'XX000'
        scan.errorDetail =
          (msg as unknown as { detail?: string }).detail ?? null
      }
    } else if (msg.name === 'commandComplete') {
      scan.lastCommandTag = (msg as unknown as { text: string }).text
    }
  })
  return scan
}
