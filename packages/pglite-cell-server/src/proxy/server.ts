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
import type { CellHost } from '../host'
import type { HostSession, ProtocolUnit } from '../session'
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
  concatBytes,
  errorResponse,
  isExtendedUnitPart,
  readyForQuery,
  simpleQueryText,
} from './wire'
import type { FrontendFrame } from './wire'

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
      const unit: ProtocolUnit = {
        kind: 'simple',
        bytes: frame.bytes,
        sqlForReplay: simpleQueryText(frame.bytes),
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
      if (res.disposition === 'held-conflict') {
        // The buffered output died unsent (§3.5); surface the one §4.0
        // client-visible failure mode in its place.
        this.write(conn, errorResponse(SERIALIZATION_CONFLICT_FIELDS))
        this.write(conn, readyForQuery('I'))
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
