// M1d exit tests: REAL `pg` (node-postgres) clients over TCP against the
// CellProxyServer — the §3.7 contract enforced at the wire, row by row,
// plus the §16 client-observation property v0 (zero bytes of a discarded
// first attempt ever reach the client socket) and session-state (SET)
// replay across conflict recycles.
//
// Each test builds its own GatewayCore + CellHost + proxy (many PGlite
// boots — generous timeouts; the suite is serialized in vitest.config.ts).

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { Socket } from 'node:net'
import { Client } from 'pg'
import { Parser } from '@electric-sql/pg-protocol'
import {
  Cell,
  EraTailer,
  materializeAtHead,
  parseLsn,
} from '@electric-sql/pglite-cell'
import { GatewayCore, extractDatadir } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'
import { CellProxyServer } from '../src/proxy/server'
import type { HostSession, UnitObservation } from '../src/session'

const TEST_TIMEOUT = 240_000

interface Ctx {
  root: string
  core: GatewayCore
  host: CellHost
  proxy: CellProxyServer
  port: number
  manifest: Manifest
  dbId: string
  /** HostSessions in connection order (proxy onSession test hook). */
  sessions: HostSession[]
  clients: Client[]
  teardown: () => Promise<void>
}

let oracleN = 0

async function setup(): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-proxy-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host'),
    hostId: 'h1',
  })
  const sessions: HostSession[] = []
  const proxy = new CellProxyServer({
    host,
    port: 0,
    onSession: (s) => sessions.push(s),
  })
  const port = await proxy.start()
  const clients: Client[] = []
  return {
    root,
    core,
    host,
    proxy,
    port,
    manifest,
    dbId: manifest.databaseId,
    sessions,
    clients,
    teardown: async () => {
      for (const c of clients) {
        await c.end().catch(() => undefined)
      }
      await proxy.stop().catch(() => undefined)
      await host.shutdown().catch(() => undefined)
      await core.stop()
      rmSync(root, { recursive: true, force: true })
    },
  }
}

async function connect(ctx: Ctx, database = 'appdb'): Promise<Client> {
  const client = new Client({
    host: '127.0.0.1',
    port: ctx.port,
    database,
    user: 'postgres',
  })
  // Fatal session resets terminate the socket server-side; without a
  // listener node-postgres turns that into an uncaught 'error' event.
  client.on('error', () => undefined)
  await client.connect()
  ctx.clients.push(client)
  return client
}

/** The client connection's raw TCP socket (byte-level observation). */
function clientStream(client: Client): Socket {
  return (client as unknown as { connection: { stream: Socket } }).connection
    .stream
}

/** Record every raw byte the server flushes to this client. */
function tapClient(client: Client) {
  const chunks: Buffer[] = []
  clientStream(client).on('data', (d: Buffer) => chunks.push(Buffer.from(d)))
  return {
    clear: () => {
      chunks.length = 0
    },
    total: () => chunks.reduce((n, c) => n + c.length, 0),
    bytes: () => Buffer.concat(chunks),
  }
}

/** Parse a raw backend byte log into a message census. */
function census(bytes: Buffer): {
  commandTags: string[]
  rfq: number
  errorCodes: string[]
} {
  const commandTags: string[] = []
  const errorCodes: string[] = []
  let rfq = 0
  new Parser().parse(bytes, (msg) => {
    if (msg.name === 'commandComplete') {
      commandTags.push((msg as unknown as { text: string }).text)
    } else if (msg.name === 'readyForQuery') {
      rfq++
    } else if (msg.name === 'error') {
      errorCodes.push((msg as unknown as { code?: string }).code ?? '?')
    }
  })
  return { commandTags, rfq, errorCodes }
}

function newTailer(ctx: Ctx): EraTailer {
  return new EraTailer(ctx.core.streamClientFor(ctx.dbId), {
    path: ctx.manifest.era.path,
    eraId: ctx.manifest.era.id,
    ordinal: ctx.manifest.era.ordinal,
    baseOffset: ctx.manifest.era.baseOffset,
    baseLsn: parseLsn(ctx.manifest.era.baseLsn),
  })
}

/** The stream tail offset (HEAD through the same client the host uses). */
async function streamHead(ctx: Ctx): Promise<string> {
  const head = await ctx.core
    .streamClientFor(ctx.dbId)
    .head(ctx.manifest.era.path)
  return head.nextOffset
}

/**
 * Convergence oracle v0: a never-before-seen materialize of the FULL
 * stream (checkpoint + every W slice), queried directly.
 */
async function oracle<T>(ctx: Ctx, sql: string): Promise<T[]> {
  const dir = join(ctx.root, `oracle-${++oracleN}`)
  await extractDatadir(
    await ctx.core.getObject(ctx.manifest.checkpoint.ref),
    dir,
  )
  const tailer = newTailer(ctx)
  await tailer.catchUp()
  const mat = await materializeAtHead({
    baseDir: dir,
    slices: tailer.slicesSince(parseLsn(ctx.manifest.checkpoint.snapEnd)),
  })
  const cell = await Cell.open(dir, { expectedHeadLsn: mat.headLsn })
  const rows = (await cell.db.query<T>(sql)).rows
  await cell.db.close()
  return rows
}

describe('CellProxyServer (M1d exit)', () => {
  it(
    '1. round-trip: real pg client connects by database name, DDL + insert + select land with correct values',
    async () => {
      const ctx = await setup()
      try {
        const c = await connect(ctx)
        await c.query(`create table t1 (id serial primary key, v text, n int)`)
        const ins = await c.query(
          `insert into t1 (v, n) values ('alpha', 1), ('beta', 2)`,
        )
        expect(ins.rowCount).toBe(2)
        const r = await c.query(`select v, n from t1 order by id`)
        expect(r.rows).toEqual([
          { v: 'alpha', n: 1 },
          { v: 'beta', n: 2 },
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '2. sibling-race transparency: the client committing from a stale base still gets a clean CommandComplete; oracle shows exactly-once rows',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table t2 (id serial primary key, v text)`)

        const a = await connect(ctx)
        const b = await connect(ctx)
        // Prime BOTH with read cells so B's is genuinely stale after A's
        // commit.
        await a.query(`select count(*) from t2`)
        await b.query(`select count(*) from t2`)

        const ra = await a.query(`insert into t2 (v) values ('from-a')`)
        expect(ra.rowCount).toBe(1)
        // B commits from a stale base: watermark advance + write-upgrade +
        // transparent re-execute behind the wire — the client just sees
        // INSERT 0 1.
        const rb = await b.query(`insert into t2 (v) values ('from-b')`)
        expect(rb.rowCount).toBe(1)

        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from t2 order by id`,
        )
        expect(rows).toEqual([{ v: 'from-a' }, { v: 'from-b' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '3. interactive 40001: COMMIT after a foreign commit fails with the exact §4.0 error; the connection survives; the txn is absent from the oracle',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table t3 (id serial primary key, v text)`)

        const a = await connect(ctx)
        // Sticky write intent first, so the loss is a genuine COMMIT-time
        // CAS loss (not the read-attach upgrade conflict).
        await a.query(`insert into t3 (v) values ('a-seed')`)

        await a.query(`begin`)
        // Mid-transaction results stream by design.
        const mid = await a.query(`insert into t3 (v) values ('a-txn')`)
        expect(mid.rowCount).toBe(1)

        const b = await connect(ctx)
        await b.query(`insert into t3 (v) values ('b1')`)

        let err: unknown
        try {
          await a.query(`commit`)
        } catch (e) {
          err = e
        }
        expect(err).toBeDefined()
        const dbErr = err as {
          code?: string
          message: string
          detail?: string
          hint?: string
        }
        expect(dbErr.code).toBe('40001')
        expect(dbErr.message).toBe(
          'could not serialize access due to concurrent update',
        )
        expect(dbErr.detail).toBe(
          'transaction conflicted with a concurrent commit on this database',
        )
        expect(dbErr.hint).toBe('retry the transaction')

        // The session survives and is usable.
        const alive = await a.query(`select 1 as one`)
        expect(alive.rows).toEqual([{ one: 1 }])

        // The rolled-back insert is absent from a fresh oracle.
        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from t3 order by id`,
        )
        expect(rows).toEqual([{ v: 'a-seed' }, { v: 'b1' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '4. tainted fatal reset: temp-table session that loses a race gets an error AND connection termination; reconnect works',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table t4 (x int)`)

        const a = await connect(ctx)
        const observed = { error: false, end: false }
        a.on('error', () => {
          observed.error = true
        })
        a.on('end', () => {
          observed.end = true
        })
        await a.query(`create temp table tt (x int)`)
        expect(ctx.sessions[1].tainted).toBe(true)

        const b = await connect(ctx)
        await b.query(`insert into t4 values (1)`)

        // A is pinned at its stale base; its next publish loses and the
        // taint routes the loss to a FATAL session reset (§3.3), never a
        // silent continuation against vanished temp state.
        let err: unknown
        try {
          await a.query(`insert into t4 values (2)`)
        } catch (e) {
          err = e
        }
        expect(err).toBeDefined()
        const dbErr = err as { code?: string; message: string }
        expect(dbErr.code).toBe('57P01')
        expect(dbErr.message).toContain('session reset')

        // The connection is gone (driver-visible).
        await new Promise((r) => setTimeout(r, 300))
        expect(observed.error || observed.end).toBe(true)
        await expect(a.query(`select 1`)).rejects.toThrow()

        // Reconnect works fine.
        const c = await connect(ctx)
        const rows = await c.query(`select x from t4 order by x`)
        expect(rows.rows).toEqual([{ x: 1 }])

        // Exactly-once: only B's row landed.
        const orows = await oracle<{ x: number }>(
          ctx,
          `select x from t4 order by x`,
        )
        expect(orows).toEqual([{ x: 1 }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '5. read-only never CAS: N selects on a second connection leave the stream head untouched',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table t5r (v text)`)
        await s.query(`insert into t5r values ('r')`)

        const b = await connect(ctx)
        const head0 = await streamHead(ctx)
        for (let i = 0; i < 5; i++) {
          const r = await b.query(`select v from t5r`)
          expect(r.rows).toEqual([{ v: 'r' }])
          expect(await streamHead(ctx)).toBe(head0)
        }
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '6. client observation (§16 v0): a one-shot that loses its first attempt flushes ONLY the re-execution — zero first-attempt bytes on the client socket',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table t6 (id serial primary key, v text)`)

        const a = await connect(ctx)
        const b = await connect(ctx)
        await b.query(`select count(*) from t6`) // prime B's (read) cell
        await a.query(`insert into t6 (v) values ('a1')`) // stales B's base

        const events: UnitObservation[] = []
        ctx.sessions[2]._unitObserver = (ev) => events.push(ev)
        const tap = tapClient(b)
        tap.clear()

        const rb = await b.query(`insert into t6 (v) values ('b1')`)
        expect(rb.rowCount).toBe(1)
        await new Promise((r) => setImmediate(r))

        // The unit executed TWICE (discarded first attempt + re-execution).
        const attempts = events.filter((e) => e.phase === 'attempt')
        expect(attempts.length).toBe(2)
        expect(attempts[0].outputBytes).toBeGreaterThan(0)
        const result = events.find((e) => e.phase === 'result')
        expect(result?.disposition).toBe('landed')

        // The client socket saw EXACTLY the re-execution's output: not one
        // byte of the first attempt leaked before its CAS resolved.
        expect(tap.total()).toBe(result?.outputBytes)
        const seen = census(tap.bytes())
        expect(seen.commandTags).toEqual(['INSERT 0 1'])
        expect(seen.rfq).toBe(1)
        expect(seen.errorCodes).toEqual([])

        // Exactly-once at the stream level.
        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from t6 order by id`,
        )
        expect(rows).toEqual([{ v: 'a1' }, { v: 'b1' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    "7. SET replay: application_name survives a conflict recycle (loss forced via a sibling race), SHOW still returns 'm1d'",
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table t7 (v text)`)

        const b = await connect(ctx)
        await b.query(`set application_name = 'm1d'`)

        const a = await connect(ctx)
        await a.query(`insert into t7 values ('a1')`)

        // B's insert rides a watermark advance + write-upgrade recycle:
        // TWO fresh cells, each getting the startup + SET replay.
        const events: UnitObservation[] = []
        ctx.sessions[1]._unitObserver = (ev) => events.push(ev)
        await b.query(`insert into t7 values ('b1')`)
        expect(
          events.filter((e) => e.phase === 'attempt').length,
        ).toBeGreaterThanOrEqual(2) // the loss + re-execution actually happened
        expect(ctx.sessions[1].attachMode).toBe('write')

        const r = await b.query(`show application_name`)
        expect(r.rows).toEqual([{ application_name: 'm1d' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '8. extended protocol: parameterized insert + select work, including a byte-identical re-execution through a race loss',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table t8x (id serial primary key, v text)`)

        const a = await connect(ctx)
        const b = await connect(ctx)
        await b.query(`select count(*) from t8x`) // prime B's (read) cell
        await a.query({
          text: `insert into t8x (v) values ($1)`,
          values: ['a-ext'],
        })

        const events: UnitObservation[] = []
        ctx.sessions[2]._unitObserver = (ev) => events.push(ev)
        const rb = await b.query({
          text: `insert into t8x (v) values ($1)`,
          values: ['b-ext'],
        })
        expect(rb.rowCount).toBe(1)
        // The extended unit (Parse..Sync bytes) re-executed byte-identically
        // after the first attempt's discard.
        const attempts = events.filter(
          (e) => e.phase === 'attempt' && e.unitKind === 'extended',
        )
        expect(attempts.length).toBe(2)

        const sel = await b.query({
          text: `select v from t8x where v = $1`,
          values: ['b-ext'],
        })
        expect(sel.rows).toEqual([{ v: 'b-ext' }])

        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from t8x order by id`,
        )
        expect(rows).toEqual([{ v: 'a-ext' }, { v: 'b-ext' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '9. unknown database name: clean 3D000 at connect',
    async () => {
      const ctx = await setup()
      try {
        const c = new Client({
          host: '127.0.0.1',
          port: ctx.port,
          database: 'no-such-db',
          user: 'postgres',
        })
        c.on('error', () => undefined)
        let err: unknown
        try {
          await c.connect()
        } catch (e) {
          err = e
        }
        expect(err).toBeDefined()
        const dbErr = err as { code?: string; message: string }
        expect(dbErr.code).toBe('3D000')
        expect(dbErr.message).toContain('does not exist')
        await c.end().catch(() => undefined)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
