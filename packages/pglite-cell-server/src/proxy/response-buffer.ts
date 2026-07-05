// H1 response-size safety — the §3.5 buffering ladder, v1 rungs.
//
// A protocol unit's backend response must be BUFFERED entirely before the
// proxy can decide its §3.7 disposition (a lost CAS discards it unsent). The
// naive "collect every chunk in a JS array" buffer is unbounded: a big
// SELECT in an interactive (non-read-only) transaction would OOM the host.
// This buffer bounds it:
//
//   rung 1  in-memory, up to `memoryMax` (default 8 MiB)
//   rung 2  spill to a per-connection temp file, up to `spoolMax` (256 MiB)
//   rung 3  past spoolMax: throw ResponseTooLargeError -> the session aborts
//           the unit with 40001 + HINT "use a READ ONLY transaction or cursor"
//
//   (rungs 3–4 of §3.5 proper — the lease-probe re-attach rungs — are SPEC'D
//   but DEFERRED; they are not faked here. The 40001+HINT is the v1 ceiling.)
//
// The TRUE fix for large reads is declared-read-only STREAMING (see
// session.ts / read-only classifier), which never buffers at all. This
// ladder is the safety net for large output in transactions that are NOT
// declared read-only.

import { closeSync, openSync, readSync, unlinkSync, writeSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { randomUUID } from 'node:crypto'
import { concatBytes } from './wire'

/**
 * A unit's backend response exceeded the §3.5 spool cap. The session turns
 * this into a clean 40001 + HINT — nothing was sent to the client, so the
 * transaction is a blind-retryable serialization failure from the client's
 * view (and the HINT names the real fix).
 */
export class ResponseTooLargeError extends Error {
  readonly code = '40001'
  constructor(
    readonly spooledBytes: number,
    readonly spoolMax: number,
  ) {
    super(
      `proxy response buffer exceeded ${spoolMax} bytes (spooled ` +
        `${spooledBytes})`,
    )
    this.name = 'ResponseTooLargeError'
  }
}

/**
 * The §3.5 buffering ladder for ONE protocol unit's response. Feed raw
 * backend chunks with `push`; read the assembled bytes with `finalize`;
 * always call `dispose` (idempotent) to unlink any spool file. One instance
 * is used per unit and thrown away — spool files never outlive their unit
 * (the connection close path also disposes any in-flight buffer, belt and
 * braces).
 */
export class ResponseBuffer {
  private readonly memoryMax: number
  private readonly spoolMax: number
  private mem: Uint8Array[] = []
  private memBytes = 0
  private total = 0
  private spoolPath: string | null = null
  private spoolFd: number | null = null
  private disposed = false

  constructor(opts: { memoryMax: number; spoolMax: number }) {
    this.memoryMax = opts.memoryMax
    this.spoolMax = opts.spoolMax
  }

  /** Total bytes accepted so far (memory + spooled). */
  get byteLength(): number {
    return this.total
  }

  /** True once the response spilled past memory into the spool file. */
  get spooled(): boolean {
    return this.spoolFd !== null
  }

  /**
   * Append a raw backend chunk. Grows in memory until `memoryMax`, then
   * opens a spool file and streams there. Throws ResponseTooLargeError once
   * the spooled total would exceed `spoolMax` (the unit is then aborted).
   */
  push(chunk: Uint8Array): void {
    if (this.disposed) throw new Error('ResponseBuffer used after dispose')
    if (chunk.length === 0) return
    if (this.total + chunk.length > this.spoolMax) {
      throw new ResponseTooLargeError(this.total + chunk.length, this.spoolMax)
    }
    this.total += chunk.length
    if (
      this.spoolFd === null &&
      this.memBytes + chunk.length <= this.memoryMax
    ) {
      // Still on rung 1: keep it in memory.
      this.mem.push(chunk)
      this.memBytes += chunk.length
      return
    }
    // Rung 2: spool. Open lazily and flush the in-memory prefix first so the
    // file holds the response contiguously.
    if (this.spoolFd === null) this.openSpool()
    this.writeSpool(chunk)
  }

  private openSpool(): void {
    const path = join(tmpdir(), `pgl-proxy-spool-${randomUUID()}`)
    const fd = openSync(path, 'w+')
    this.spoolPath = path
    this.spoolFd = fd
    // Drain the in-memory prefix into the file, then release it.
    for (const c of this.mem) this.writeSpool(c)
    this.mem = []
    this.memBytes = 0
  }

  private writeSpool(chunk: Uint8Array): void {
    const fd = this.spoolFd
    if (fd === null) throw new Error('spool not open')
    writeSync(fd, chunk, 0, chunk.length)
  }

  /**
   * The assembled response bytes. In the memory case this is a plain concat;
   * in the spooled case the file is read back whole (still bounded by
   * spoolMax). Safe to call once; `dispose` afterwards.
   */
  finalize(): Uint8Array {
    if (this.disposed) throw new Error('ResponseBuffer used after dispose')
    if (this.spoolFd === null) return concatBytes(this.mem)
    const out = new Uint8Array(this.total)
    let off = 0
    // Read the whole file back. Node has no positional stream read helper
    // that fills a subarray cleanly across all versions, so loop readSync.
    const CHUNK = 1 << 20
    const tmp = Buffer.allocUnsafe(CHUNK)
    let pos = 0
    for (;;) {
      const n = readSync(this.spoolFd, tmp, 0, CHUNK, pos)
      if (n === 0) break
      out.set(tmp.subarray(0, n), off)
      off += n
      pos += n
    }
    return out
  }

  /** Unlink the spool file and drop all buffers. Idempotent. */
  dispose(): void {
    if (this.disposed) return
    this.disposed = true
    this.mem = []
    this.memBytes = 0
    if (this.spoolFd !== null) {
      try {
        closeSync(this.spoolFd)
      } catch {
        /* already closed */
      }
      this.spoolFd = null
    }
    if (this.spoolPath !== null) {
      try {
        unlinkSync(this.spoolPath)
      } catch {
        /* already gone */
      }
      this.spoolPath = null
    }
  }
}
