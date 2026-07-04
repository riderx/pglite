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
    '10. raced DDL one-shot re-execute: a stale-lease loser CREATE TABLE re-executes transparently; the catalog is consistent and the new table is usable',
    async () => {
      const ctx = await setup()
      try {
        // Prime a base table so A and B both attach at a non-empty head.
        const s = await connect(ctx)
        await s.query(`create table seed (id serial primary key)`)

        const a = await connect(ctx)
        const b = await connect(ctx)
        // Prime BOTH with read cells so B's base is genuinely stale after A's
        // commit (same interleaving as test 2).
        await a.query(`select count(*) from seed`)
        await b.query(`select count(*) from seed`)

        // A lands a commit, stalings B's base.
        const ra = await a.query(`insert into seed default values`)
        expect(ra.rowCount).toBe(1)

        // B's CREATE TABLE is a one-shot DDL committing from a stale base:
        // watermark advance + write-upgrade + transparent re-execute behind
        // the wire. The client just sees a clean CommandComplete (CREATE
        // TABLE), never an error.
        const events: UnitObservation[] = []
        ctx.sessions[2]._unitObserver = (ev) => events.push(ev)
        const tap = tapClient(b)
        tap.clear()

        const rb = await b.query(
          `create table dtab (id serial primary key, v text not null, n int)`,
        )
        // node-postgres reports the leading token; the raw tag ('CREATE
        // TABLE') is asserted on the socket census below.
        expect(rb.command).toBe('CREATE')
        await new Promise((r) => setImmediate(r))

        // The unit executed TWICE (discarded first attempt + re-execution)
        // and landed; the client saw exactly the re-execution's bytes.
        const attempts = events.filter((e) => e.phase === 'attempt')
        expect(attempts.length).toBe(2)
        const result = events.find((e) => e.phase === 'result')
        expect(result?.disposition).toBe('landed')
        const seen = census(tap.bytes())
        expect(seen.commandTags).toEqual(['CREATE TABLE'])
        expect(seen.errorCodes).toEqual([])
        expect(seen.rfq).toBe(1)

        // The table exists with the correct shape (queried on B's own cell).
        const shape = await b.query(
          `select column_name, data_type, is_nullable
             from information_schema.columns
            where table_name = 'dtab'
            order by ordinal_position`,
        )
        expect(shape.rows).toEqual([
          {
            column_name: 'id',
            data_type: 'integer',
            is_nullable: 'NO',
          },
          {
            column_name: 'v',
            data_type: 'text',
            is_nullable: 'NO',
          },
          {
            column_name: 'n',
            data_type: 'integer',
            is_nullable: 'YES',
          },
        ])

        // A THIRD fresh connection materializes the catalog through the
        // watermark gate and finds the table immediately usable: insert +
        // select round-trips against the raced-in DDL.
        const c = await connect(ctx)
        const ins = await c.query(`insert into dtab (v, n) values ('x', 7)`)
        expect(ins.rowCount).toBe(1)
        const sel = await c.query(`select v, n from dtab order by id`)
        expect(sel.rows).toEqual([{ v: 'x', n: 7 }])

        // Oracle: a never-before-seen materialize of the FULL stream shows a
        // consistent catalog — the table is present with the row landed.
        const orows = await oracle<{ v: string; n: number }>(
          ctx,
          `select v, n from dtab order by id`,
        )
        expect(orows).toEqual([{ v: 'x', n: 7 }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '11. client observation under injected commit-path failure (§16 kill-the-CAS v0): a retried transient append lands exactly once; an all-attempts append failure is a clean ERROR with zero partial rows',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table t11 (id serial primary key, v text)`)

        // ---- (a) transient failure on the FIRST append attempt ----
        // The committer's postWithRetry must re-POST byte-identically and
        // land; the client sees EXACTLY one successful response.
        const a = await connect(ctx)
        // Make A write-attached and canonical-at-head FIRST, so the very next
        // append is the commit's W frame (not an attach-time sync/floors/lease
        // control append). This isolates the injection to the commit path.
        await a.query(`insert into t11 (v) values ('seed')`)

        const runtime = ctx.host.runtimeFor(ctx.dbId)!
        const committer = runtime.committer
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const client = (committer as any).client
        const realAppend = client.append.bind(client)

        let appendCalls = 0
        let failedOnce = false

        client.append = async (...args: any[]) => {
          appendCalls++
          if (!failedOnce) {
            failedOnce = true
            // A network-style exception (NOT a StreamHttpError) — the exact
            // shape postWithRetry treats as "outcome unknown, retry W2".
            throw new Error('injected transient network failure')
          }
          return realAppend(...args)
        }

        const tapA = tapClient(a)
        tapA.clear()
        const ra = await a.query(`insert into t11 (v) values ('retry-lands')`)

        client.append = realAppend
        expect(ra.rowCount).toBe(1)
        await new Promise((r) => setImmediate(r))

        // The first attempt threw; the retry landed the SAME bytes.
        expect(failedOnce).toBe(true)
        expect(appendCalls).toBeGreaterThanOrEqual(2)
        // The client saw exactly one INSERT 0 1 — no duplicate, no error.
        const seenA = census(tapA.bytes())
        expect(seenA.commandTags).toEqual(['INSERT 0 1'])
        expect(seenA.rfq).toBe(1)
        expect(seenA.errorCodes).toEqual([])

        // Exactly-once at the stream level.
        const rows1 = await oracle<{ v: string }>(
          ctx,
          `select v from t11 order by id`,
        )
        expect(rows1).toEqual([{ v: 'seed' }, { v: 'retry-lands' }])

        // ---- (b) hard failure of ALL append attempts ----
        // Every attempt throws a network-style error; postWithRetry exhausts
        // its budget and rethrows. The client must receive a clean ERROR
        // (never a hang, never partial rows) and the connection is cleanly
        // reset (§3.3 fatal path). No DataRow bytes leak before the error.
        const b = await connect(ctx)
        const observed = { error: false, end: false }
        b.on('error', () => {
          observed.error = true
        })
        b.on('end', () => {
          observed.end = true
        })
        // Prime B's cell (a read) so the failure is isolated to the commit
        // append, not the attach.
        await b.query(`select count(*) from t11`)

        client.append = async () => {
          throw new Error('injected hard append failure (all attempts)')
        }

        const tapB = tapClient(b)
        tapB.clear()
        let err: unknown
        try {
          await b.query(`insert into t11 (v) values ('never-lands')`)
        } catch (e) {
          err = e
        }

        client.append = realAppend
        expect(err).toBeDefined()
        await new Promise((r) => setTimeout(r, 200))

        // The bytes the client saw before the error: NO DataRow, NO
        // CommandComplete for the failed insert — only a clean ERROR (the
        // §3.5 buffer died unsent), and the connection is torn down.
        const seenB = census(tapB.bytes())
        expect(seenB.commandTags).toEqual([]) // no partial CommandComplete
        expect(seenB.errorCodes.length).toBe(1) // exactly one clean ERROR
        let dataRows = 0
        new Parser().parse(tapB.bytes(), (msg) => {
          if (msg.name === 'dataRow') dataRows++
        })
        expect(dataRows).toBe(0) // no partial DataRows leaked
        expect(observed.error || observed.end).toBe(true) // cleanly reset

        // The failed insert is absent from a fresh oracle: exactly-once holds.
        const rows2 = await oracle<{ v: string }>(
          ctx,
          `select v from t11 order by id`,
        )
        expect(rows2).toEqual([{ v: 'seed' }, { v: 'retry-lands' }])

        // A fresh connection is fully usable after the reset.
        const c = await connect(ctx)
        const alive = await c.query(`select v from t11 order by id`)
        expect(alive.rows).toEqual([{ v: 'seed' }, { v: 'retry-lands' }])
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
