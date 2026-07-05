// CellProxyServer — the M1d session proxy (§3.5): a TCP wire-protocol
// server that owns client connections, maps each onto a HostSession
// (cell-per-connection, §14.4), chops the frontend byte stream into
// protocol UNITS (simple 'Q' / extended batch closed by Sync), and flushes
// each unit's buffered response per its §3.7 disposition. The governing
// invariant: no byte reaches the client except from a landed commit, an
// execution under a held lease (N/A at M1), or a declared/actual read-only
// statement — interactive transactions stream mid-txn results by design,
// only the COMMIT response is held.

import { createServer } from 'node:net'
import type { Server, Socket } from 'node:net'
import { parseLsn } from '@electric-sql/pglite-cell'
import type { CellHost } from '../host'
import type { Freshness, HostSession, ProtocolUnit } from '../session'
import {
  FatalSessionResetError,
  SessionClosedError,
  SessionPinnedExpiredError,
} from '../errors'
import {
  FRONTEND,
  FrontendFrameReader,
  SERIALIZATION_CONFLICT_FIELDS,
  WireProtocolError,
  commandComplete,
  concatBytes,
  errorResponse,
  isExtendedUnitPart,
  notificationResponse,
  readyForQuery,
  simpleQueryText,
} from './wire'
import type { FrontendFrame } from './wire'

/** Synthetic backend pid carried by proxy-synthesized 'A' messages (M3):
 *  notifications are tailer-driven, not tied to any real backend. */
export const SYNTHETIC_NOTIFY_PID = 424242

/** `SET pglite.freshness = '<mode>'` as a lone simple statement (§7 M3):
 *  intercepted by the proxy BEFORE unit dispatch, never forwarded. */
const FRESHNESS_SET_RE =
  /^\s*set\s+(?:session\s+)?pglite\.freshness\s*(?:=|\s+to\s+)\s*(?:'([^']*)'|"([^"]*)"|([^\s;'"]+))\s*;?\s*$/i

/** Parse the freshness mode string; null on an unrecognized value. */
export function parseFreshness(value: string): Freshness | null {
  const v = value.trim().toLowerCase()
  if (v === 'session' || v === 'linearizable' || v === 'local') {
    return { mode: v }
  }
  const pinned = /^pinned:\s*([0-9a-f]+\/[0-9a-f]+)$/i.exec(v)
  if (pinned) {
    try {
      return { mode: 'pinned', lsn: parseLsn(pinned[1]) }
    } catch {
      return null
    }
  }
  const bounded = /^bounded-stale:\s*(\d+)$/.exec(v)
  if (bounded) return { mode: 'bounded-stale', ms: Number(bounded[1]) }
  return null
}

/** `CREATE [GLOBAL|LOCAL] UNLOGGED TABLE` anywhere in a simple unit (§9):
 *  the tested policy is a loud ERROR, never execution. */
const UNLOGGED_RE = /\bcreate\s+(?:(?:global|local)\s+)?unlogged\s+table\b/i

export interface CellProxyServerOpts {
  host: CellHost
  /** TCP port (default 0 = OS-assigned; `start()` resolves the real one). */
  port?: number
  /** Bind address (default 127.0.0.1). */
  listenHost?: string
  /** Maximum concurrent client connections (default 32). */
  maxConnections?: number
  /** Database served when the StartupMessage names none. */
  defaultDatabase?: string
  /** TEST HOOK: observe each connection's HostSession as it is created. */
  onSession?: (session: HostSession, info: { database: string }) => void
}

interface Conn {
  socket: Socket
  reader: FrontendFrameReader
  session: HostSession | null
  /** Extended-protocol messages accumulated until Sync closes the unit. */
  extended: Uint8Array[]
  /** FIFO unit pipeline: one in-flight unit per connection. */
  tasks: (() => Promise<void>)[]
  pumping: boolean
  closed: boolean
  /** Trailing RFQ status of the last flushed unit (synthesized replies
   *  reuse it so the client's view of txn state stays coherent). */
  lastRfq: 'I' | 'T' | 'E'
  /** Unsubscribe from tailer-driven notification delivery (M3). */
  unsubscribeNotifications: (() => void) | null
}

export class CellProxyServer {
  private readonly host: CellHost
  private readonly listenHost: string
  private readonly maxConnections: number
  private readonly defaultDatabase?: string
  private readonly onSession?: CellProxyServerOpts['onSession']
  private requestedPort: number
  private server: Server | null = null
  private actualPort: number | null = null
  private readonly conns = new Set<Conn>()

  constructor(opts: CellProxyServerOpts) {
    this.host = opts.host
    this.requestedPort = opts.port ?? 0
    this.listenHost = opts.listenHost ?? '127.0.0.1'
    this.maxConnections = opts.maxConnections ?? 32
    this.defaultDatabase = opts.defaultDatabase
    this.onSession = opts.onSession
  }

  /** The bound port (available after `start()` resolves). */
  get port(): number {
    if (this.actualPort === null) throw new Error('proxy not started')
    return this.actualPort
  }

  /** Start listening; resolves the actual bound port. */
  async start(): Promise<number> {
    if (this.server) throw new Error('proxy already started')
    const server = createServer((socket) => this.accept(socket))
    server.maxConnections = this.maxConnections
    this.server = server
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject)
      server.listen(this.requestedPort, this.listenHost, () => {
        server.removeListener('error', reject)
        resolve()
      })
    })
    const addr = server.address()
    if (addr === null || typeof addr !== 'object') {
      throw new Error('expected address info')
    }
    this.actualPort = addr.port
    return addr.port
  }

  /** Stop listening and tear down every connection (sessions closed). */
  async stop(): Promise<void> {
    const server = this.server
    this.server = null
    for (const conn of [...this.conns]) {
      conn.closed = true
      conn.socket.destroy()
      void conn.session?.close().catch(() => undefined)
    }
    this.conns.clear()
    if (server) {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
    this.actualPort = null
  }

  private accept(socket: Socket): void {
    const conn: Conn = {
      socket,
      reader: new FrontendFrameReader(),
      session: null,
      extended: [],
      tasks: [],
      pumping: false,
      closed: false,
      lastRfq: 'I',
      unsubscribeNotifications: null,
    }
    this.conns.add(conn)
    socket.setNoDelay(true)

    socket.on('data', (data) => {
      try {
        conn.reader.push(data)
        for (;;) {
          const frame = conn.reader.next()
          if (frame === null) break
          this.onFrame(conn, frame)
        }
      } catch (err) {
        // Malformed byte stream: report and drop the connection. A socket
        // close mid-transaction rolls back via session.close() below.
        this.write(
          conn,
          errorResponse({
            code: '08P01', // protocol_violation
            message:
              err instanceof WireProtocolError
                ? err.message
                : `protocol error: ${String(err)}`,
          }),
        )
        this.teardown(conn)
      }
    })
    socket.on('error', () => this.teardown(conn))
    socket.on('close', () => this.teardown(conn))
  }

  /** Close the socket and the session (rollback + recycle ride on close). */
  private teardown(conn: Conn): void {
    if (conn.closed) return
    conn.closed = true
    conn.tasks.length = 0
    conn.unsubscribeNotifications?.()
    conn.unsubscribeNotifications = null
    this.conns.delete(conn)
    conn.socket.destroy()
    void conn.session?.close().catch(() => undefined)
  }

  private write(conn: Conn, bytes: Uint8Array): void {
    if (conn.closed || !conn.socket.writable || bytes.length === 0) return
    conn.socket.write(Buffer.from(bytes))
  }

  private onFrame(conn: Conn, frame: FrontendFrame): void {
    switch (frame.kind) {
      case 'ssl':
        // No TLS: reply 'N' immediately; the client continues in the clear.
        this.write(conn, new Uint8Array([0x4e]))
        return
      case 'cancel':
        // CancelRequest arrives on its own connection; there is no backend
        // process to signal — swallow it (the protocol expects no reply).
        return
      case 'startup':
        this.enqueue(conn, () => this.handleStartup(conn, frame))
        return
      case 'typed':
        this.onTypedFrame(conn, frame)
        return
    }
  }

  private onTypedFrame(
    conn: Conn,
    frame: { code: number; bytes: Uint8Array },
  ): void {
    if (frame.code === FRONTEND.Query) {
      const sql = simpleQueryText(frame.bytes)

      // §7 (M3): `SET pglite.freshness` is proxy state, intercepted BEFORE
      // unit dispatch and never forwarded to the cell — the reply is
      // synthesized (SET CommandComplete + RFQ).
      const freshnessMatch = FRESHNESS_SET_RE.exec(sql)
      if (freshnessMatch) {
        const raw =
          freshnessMatch[1] ?? freshnessMatch[2] ?? freshnessMatch[3] ?? ''
        this.enqueue(conn, async () => {
          const freshness = parseFreshness(raw)
          if (freshness === null) {
            this.write(
              conn,
              errorResponse({
                code: '22023', // invalid_parameter_value
                message: `invalid value for pglite.freshness: "${raw}"`,
                hint: `valid modes: session, linearizable, local, pinned:<lsn>, bounded-stale:<ms>`,
              }),
            )
            this.write(conn, readyForQuery(conn.lastRfq === 'I' ? 'I' : 'E'))
            return
          }
          conn.session?.setFreshness(freshness)
          this.write(conn, commandComplete('SET'))
          this.write(conn, readyForQuery(conn.lastRfq))
        })
        return
      }

      // §9 unlogged-table policy (M3): classified in simple-protocol units
      // and rejected LOUDLY without executing (extended-protocol bypass is
      // a documented gap).
      if (UNLOGGED_RE.test(sql)) {
        this.enqueue(conn, async () => {
          this.write(
            conn,
            errorResponse({
              code: '0A000', // feature_not_supported
              message:
                'unlogged tables are not supported: their storage is never ' +
                'WAL-logged and every cell recycle is a recovery, so their ' +
                'contents would silently vanish',
              hint: 'use a regular (logged) table',
            }),
          )
          this.write(conn, readyForQuery(conn.lastRfq === 'I' ? 'I' : 'E'))
        })
        return
      }

      const unit: ProtocolUnit = {
        kind: 'simple',
        bytes: frame.bytes,
        sqlForReplay: sql,
      }
      this.enqueue(conn, () => this.handleUnit(conn, unit))
      return
    }
    if (frame.code === FRONTEND.Terminate) {
      this.enqueue(conn, async () => {
        await conn.session?.close().catch(() => undefined)
        conn.session = null
        conn.socket.end()
      })
      return
    }
    if (frame.code === FRONTEND.Sync) {
      const bytes = concatBytes([...conn.extended, frame.bytes])
      conn.extended = []
      const unit: ProtocolUnit = { kind: 'extended', bytes }
      this.enqueue(conn, () => this.handleUnit(conn, unit))
      return
    }
    if (isExtendedUnitPart(frame.code)) {
      conn.extended.push(frame.bytes)
      return
    }
    throw new WireProtocolError(
      `unexpected frontend message type ${JSON.stringify(
        String.fromCharCode(frame.code),
      )}`,
    )
  }

  /** FIFO pipeline: units queue per connection; one in flight at a time. */
  private enqueue(conn: Conn, task: () => Promise<void>): void {
    conn.tasks.push(task)
    if (conn.pumping) return
    conn.pumping = true
    void (async () => {
      while (conn.tasks.length > 0 && !conn.closed) {
        const next = conn.tasks.shift()
        if (next === undefined) break
        try {
          await next()
        } catch {
          // Tasks handle their own errors; anything escaping is a proxy
          // bug or a torn-down socket — drop the connection.
          this.teardown(conn)
          break
        }
      }
      conn.pumping = false
    })()
  }

  private async handleStartup(
    conn: Conn,
    frame: { bytes: Uint8Array; params: Record<string, string> },
  ): Promise<void> {
    const database =
      frame.params.database ?? this.defaultDatabase ?? frame.params.user
    if (conn.session !== null || database === undefined) {
      this.write(
        conn,
        errorResponse({
          code: '08P01',
          message:
            conn.session !== null
              ? 'duplicate StartupMessage'
              : 'no database specified',
        }),
      )
      this.teardown(conn)
      return
    }
    let session: HostSession
    try {
      session = await this.host.connect(database)
    } catch (err) {
      const unknownDb =
        err instanceof Error && err.message.includes('database not found')
      this.write(
        conn,
        errorResponse(
          unknownDb
            ? {
                severity: 'FATAL',
                code: '3D000', // invalid_catalog_name
                message: `database "${database}" does not exist`,
              }
            : {
                severity: 'FATAL',
                code: 'XX000',
                message: `could not attach database "${database}": ${String(
                  err instanceof Error ? err.message : err,
                )}`,
              },
        ),
      )
      this.teardown(conn)
      return
    }
    if (conn.closed) {
      void session.close().catch(() => undefined)
      return
    }
    conn.session = session
    // Tailer-driven notification delivery (M3, §10.2): synthesized 'A'
    // messages written OUTSIDE units — pg clients handle async
    // NotificationResponse at any point after startup. The session filter
    // (its LISTEN set) is applied inside subscribeNotifications.
    conn.unsubscribeNotifications = session.subscribeNotifications((n) => {
      this.write(
        conn,
        notificationResponse(SYNTHETIC_NOTIFY_PID, n.channel, n.payload),
      )
    })
    this.onSession?.(session, { database })
    // The startup unit's output (auth OK, parameter statuses, backend key
    // data, ReadyForQuery) is read-only by construction: flush immediately.
    // execUnit records the bytes for session-state replay on recycle.
    await this.handleUnit(conn, { kind: 'startup', bytes: frame.bytes })
  }

  private async handleUnit(conn: Conn, unit: ProtocolUnit): Promise<void> {
    const session = conn.session
    if (session === null) {
      this.write(
        conn,
        errorResponse({
          code: '08P01',
          message: 'message received before StartupMessage',
        }),
      )
      this.teardown(conn)
      return
    }
    try {
      const res = await session.execUnit(unit)
      if (conn.closed) return
      conn.lastRfq = res.rfqStatus
      if (res.disposition === 'held-conflict') {
        // The buffered output died unsent (§3.5); surface the one §4.0
        // client-visible failure mode in its place.
        conn.lastRfq = 'I'
        this.write(conn, errorResponse(SERIALIZATION_CONFLICT_FIELDS))
        this.write(conn, readyForQuery('I'))
        return
      }
      if (res.disposition === 'held-pinned') {
        // Pinned freshness write rejection (§7 M3): clean 0A000, session
        // survives.
        conn.lastRfq = 'I'
        this.write(
          conn,
          errorResponse({
            code: '0A000', // feature_not_supported
            message:
              'cannot execute a write in pinned freshness mode: this ' +
              'session serves a fixed base and never advances',
            hint: `SET pglite.freshness = 'session' to write`,
          }),
        )
        this.write(conn, readyForQuery('I'))
        return
      }
      if (res.disposition === 'held-advisory') {
        // M6 §4.6 advisoryLocks='error': the statement was NOT executed;
        // synthesize the strict-mode rejection. Session survives.
        conn.lastRfq = conn.lastRfq === 'T' ? 'E' : 'I'
        this.write(
          conn,
          errorResponse({
            code: '0A000', // feature_not_supported
            message:
              'advisory locks are disabled on this database: their scope is ' +
              'cell-local and does not provide cross-cell/host mutual exclusion',
            hint: "set the host option advisoryLocks='local-warn' to allow them",
          }),
        )
        this.write(conn, readyForQuery(conn.lastRfq))
        return
      }
      // flushed-readonly / landed: final by contract. mid-txn / aborted:
      // interactive transactions stream mid-txn results by design; only
      // the COMMIT response is held.
      this.write(conn, res.output)
    } catch (err) {
      if (conn.closed) return
      if (
        err instanceof FatalSessionResetError ||
        err instanceof SessionPinnedExpiredError
      ) {
        // §3.3 fatal session reset: an ERROR naming the cause, then
        // connection termination (vanilla precedent: crash recovery
        // closes connections; every driver handles reconnect).
        this.write(
          conn,
          errorResponse({
            severity: 'FATAL',
            code: '57P01', // admin_shutdown: the backend is going away
            message: err.message,
          }),
        )
        this.teardown(conn)
        return
      }
      if (process.env.PGL_DEBUG_ERR === '1')
        console.error('[proxy] fatal:', err)
      this.write(
        conn,
        errorResponse({
          severity: 'FATAL',
          code: err instanceof SessionClosedError ? '08006' : 'XX000',
          message: err instanceof Error ? err.message : String(err),
        }),
      )
      this.teardown(conn)
    }
  }
}
