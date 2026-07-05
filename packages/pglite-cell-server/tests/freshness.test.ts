// M3 freshness-mode, unlogged-policy, read-replica soak and DDL-propagation
// exit tests (§7 / §8.1 / §9): real `pg` clients; `SET pglite.freshness`
// intercepted by the proxy (never forwarded); the linearizable check runs
// against a SECOND CellHost on the same gateway — the real cross-host case.

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Client } from 'pg'
import {
  Cell,
  EraTailer,
  materializeAtHead,
  parseLsn,
} from '@electric-sql/pglite-cell'
import { GatewayCore, extractCheckpoint } from '@electric-sql/pglite-gateway'
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
  sessions: HostSession[]
  clients: Client[]
  extra: (() => Promise<void>)[]
  teardown: () => Promise<void>
}

let oracleN = 0

async function setup(): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-fresh-'))
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
  const extra: (() => Promise<void>)[] = []
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
    extra,
    teardown: async () => {
      for (const c of clients) {
        await c.end().catch(() => undefined)
      }
      for (const fn of extra) {
        await fn().catch(() => undefined)
      }
      await proxy.stop().catch(() => undefined)
      await host.shutdown().catch(() => undefined)
      await core.stop()
      if (!process.env.PGL_KEEP) rmSync(root, { recursive: true, force: true })
    },
  }
}

async function connect(ctx: Ctx, port = ctx.port): Promise<Client> {
  const client = new Client({
    host: '127.0.0.1',
    port,
    database: 'appdb',
    user: 'postgres',
  })
  client.on('error', () => undefined)
  await client.connect()
  ctx.clients.push(client)
  return client
}

/** A SECOND CellHost + proxy on the SAME gateway (the cross-host case). */
async function secondHost(ctx: Ctx): Promise<number> {
  const host2 = new CellHost({
    gateway: ctx.core,
    dataRoot: join(ctx.root, 'host2'),
    hostId: 'h2',
  })
  const proxy2 = new CellProxyServer({ host: host2, port: 0 })
  const port2 = await proxy2.start()
  ctx.extra.push(async () => {
    await proxy2.stop().catch(() => undefined)
    await host2.shutdown().catch(() => undefined)
  })
  return port2
}

/** The stream tail offset (HEAD through the same client the host uses). */
async function streamHead(ctx: Ctx): Promise<string> {
  const head = await ctx.core
    .streamClientFor(ctx.dbId)
    .head(ctx.manifest.era.path)
  return head.nextOffset
}

/** Convergence oracle: a fresh materialize of the FULL stream, queried. */
async function oracle<T>(ctx: Ctx, sql: string): Promise<T[]> {
  const dir = join(ctx.root, `oracle-${++oracleN}`)
  await extractCheckpoint(ctx.manifest.checkpoint.ref, dir, {
    store: {
      get: (r: string) => ctx.core.getObject(r),
      put: (b: Uint8Array) => ctx.core.putObject(b),
    },
  })
  const tailer = new EraTailer(ctx.core.streamClientFor(ctx.dbId), {
    path: ctx.manifest.era.path,
    eraId: ctx.manifest.era.id,
    ordinal: ctx.manifest.era.ordinal,
    baseOffset: ctx.manifest.era.baseOffset,
    baseLsn: parseLsn(ctx.manifest.era.baseLsn),
  })
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

describe('freshness modes (§7 M3)', () => {
  it(
    "1. local vs session: `local` skips the gate (stale read demonstrably possible) while `session` never serves stale; the SET is intercepted (a synthesized 'SET', nothing forwarded)",
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table tf (v text)`)

        const b = await connect(ctx)
        await b.query(`select count(*) from tf`) // prime B's read cell
        const set = await b.query(`set pglite.freshness = 'local'`)
        expect(set.command).toBe('SET')
        // Intercepted, never forwarded: the cell has no such GUC — a SHOW
        // through the cell would 42704. The proxy owns the setting.

        const a = await connect(ctx)
        await a.query(`insert into tf values ('a1')`)

        // local: the watermark advanced (A's commit) but B serves its
        // current base — the stale read IS possible, by contract.
        const stale = await b.query(`select v from tf`)
        expect(stale.rows).toEqual([])

        // session: the same statement through the watermark gate sees it.
        await b.query(`set pglite.freshness = 'session'`)
        const fresh = await b.query(`select v from tf`)
        expect(fresh.rows).toEqual([{ v: 'a1' }])

        // Unknown mode: clean 22023, session survives.
        let err: unknown
        try {
          await b.query(`set pglite.freshness = 'nonsense'`)
        } catch (e) {
          err = e
        }
        expect((err as { code?: string }).code).toBe('22023')
        expect((await b.query(`select 1 as one`)).rows).toEqual([{ one: 1 }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '2. linearizable sees a commit landed via a SECOND CellHost on the same gateway; session mode on the first host provably does not',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table tx (v text)`)

        // Reader on host 1, primed.
        const r = await connect(ctx)
        await r.query(`select count(*) from tx`)

        // Writer on host 2 — a different CellHost, different tailer,
        // different watermark; only the STREAM connects them.
        const port2 = await secondHost(ctx)
        const w = await connect(ctx, port2)
        await w.query(`insert into tx values ('cross-host')`)

        // session mode on host 1: its watermark never saw host 2's commit
        // — the read is stale (this is exactly what linearizable fixes).
        const stale = await r.query(`select v from tx`)
        expect(stale.rows).toEqual([])

        // linearizable: stream catch-up past the observed tail, then the
        // gate — the just-acked cross-host commit is visible.
        await r.query(`set pglite.freshness = 'linearizable'`)
        const fresh = await r.query(`select v from tx`)
        expect(fresh.rows).toEqual([{ v: 'cross-host' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '3. pinned: serves the old state, ERRORs (0A000) on write, session survives, nothing lands',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table tp (v text)`)
        await s.query(`insert into tp values ('base')`)

        const b = await connect(ctx)
        await b.query(`select count(*) from tp`) // prime the base
        await b.query(`set pglite.freshness = 'pinned:0/0'`)

        const a = await connect(ctx)
        await a.query(`insert into tp values ('after-pin')`)

        // Old state: the pinned session never advances.
        const pinned = await b.query(`select v from tp order by v`)
        expect(pinned.rows).toEqual([{ v: 'base' }])

        // Writes are cleanly rejected — 0A000 feature_not_supported.
        let err: unknown
        try {
          await b.query(`insert into tp values ('from-pinned')`)
        } catch (e) {
          err = e
        }
        const dbErr = err as { code?: string; message: string }
        expect(dbErr.code).toBe('0A000')
        expect(dbErr.message).toContain('pinned')

        // The session survives; the rejected write is nowhere.
        expect((await b.query(`select 1 as one`)).rows).toEqual([{ one: 1 }])
        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from tp order by v`,
        )
        expect(rows).toEqual([{ v: 'after-pin' }, { v: 'base' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '4. bounded-stale advances only past Δ: a large Δ serves stale, Δ=0 advances immediately',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table tb (v text)`)

        const b = await connect(ctx)
        await b.query(`select count(*) from tb`) // prime (sets the clock)
        await b.query(`set pglite.freshness = 'bounded-stale:600000'`)

        const a = await connect(ctx)
        await a.query(`insert into tb values ('a1')`)

        // Inside Δ: the base is young enough — no advance, stale by
        // contract.
        const stale = await b.query(`select v from tb`)
        expect(stale.rows).toEqual([])

        // Δ = 0: every idle unit re-gates — fresh.
        await b.query(`set pglite.freshness = 'bounded-stale:0'`)
        const fresh = await b.query(`select v from tb`)
        expect(fresh.rows).toEqual([{ v: 'a1' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '5. read-replica byte assertion: N read sessions with mixed freshness against one writer append NOTHING — the stream head is byte-identical across all their advances',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table tr (id serial primary key, v text)`)
        await s.query(`insert into tr (v) values ('seed')`)

        const readers = await Promise.all([
          connect(ctx),
          connect(ctx),
          connect(ctx),
        ])
        await readers[0].query(`set pglite.freshness = 'session'`)
        await readers[1].query(`set pglite.freshness = 'local'`)
        await readers[2].query(`set pglite.freshness = 'linearizable'`)

        for (let round = 0; round < 3; round++) {
          await s.query(`insert into tr (v) values ('w-${round}')`)
          const head = await streamHead(ctx)
          for (const r of readers) {
            const res = await r.query(`select count(*)::int as n from tr`)
            expect(res.rows[0].n).toBeGreaterThanOrEqual(1)
            // Byte-identical head after every reader advance: reads (and
            // read-cell watermark advances, and linearizable catch-ups)
            // never append a single byte.
            expect(await streamHead(ctx)).toBe(head)
          }
        }
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})

describe('unlogged-table policy (§9 M3)', () => {
  it(
    'CREATE UNLOGGED TABLE is rejected loudly (0A000) without executing; nothing lands; the session survives',
    async () => {
      const ctx = await setup()
      try {
        const c = await connect(ctx)
        await c.query(`create table ok_t (x int)`)
        const head0 = await streamHead(ctx)

        let err: unknown
        try {
          await c.query(`create unlogged table nope (x int)`)
        } catch (e) {
          err = e
        }
        const dbErr = err as { code?: string; message: string }
        expect(dbErr.code).toBe('0A000')
        expect(dbErr.message).toContain('unlogged tables are not supported')

        // Nothing landed — head unchanged, table absent, session alive.
        expect(await streamHead(ctx)).toBe(head0)
        const t = await c.query(
          `select count(*)::int as n from pg_class where relname = 'nope'`,
        )
        expect(t.rows).toEqual([{ n: 0 }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})

describe('DDL propagation (§8.1 M3, recycle-based)', () => {
  it(
    'CREATE + post-DDL DML on A is visible to B (separate cell) on its next statement; ALTER TABLE propagates the same way; a raced ALTER re-executes; the oracle stays clean',
    async () => {
      const ctx = await setup()
      try {
        const a = await connect(ctx)
        const b = await connect(ctx)
        await b.query(`select 1`) // prime B's read cell BEFORE the DDL

        // One-shot CREATE + post-DDL DML on A.
        await a.query(`create table d1 (id serial primary key, v text)`)
        await a.query(`insert into d1 (v) values ('first')`)

        // B's next statement rides the watermark advance: new schema AND
        // rows, no reconnect, no restart.
        const seen = await b.query(`select v from d1 order by id`)
        expect(seen.rows).toEqual([{ v: 'first' }])

        // ALTER on A; B sees the new column next statement.
        await a.query(`alter table d1 add column n int default 7`)
        const cols = await b.query(
          `select column_name from information_schema.columns
            where table_name = 'd1' order by ordinal_position`,
        )
        expect(cols.rows.map((r) => r.column_name)).toEqual(['id', 'v', 'n'])

        // Raced ALTER: B commits the ALTER from a stale base (A landed a
        // commit in between) — transparent one-shot re-execution.
        await a.query(`insert into d1 (v) values ('staler')`)
        const events: UnitObservation[] = []
        ctx.sessions[1]._unitObserver = (ev) => events.push(ev)
        const rb = await b.query(`alter table d1 add column extra text`)
        expect(rb.command).toBe('ALTER')
        const attempts = events.filter((e) => e.phase === 'attempt')
        expect(attempts.length).toBe(2)
        const result = events.find((e) => e.phase === 'result')
        expect(result?.disposition).toBe('landed')

        // Convergence oracle: a fresh full-stream materialize has the
        // final catalog + rows.
        const rows = await oracle<{ v: string; n: number }>(
          ctx,
          `select v, n from d1 order by id`,
        )
        expect(rows).toEqual([
          { v: 'first', n: 7 },
          { v: 'staler', n: 7 },
        ])
        const ocols = await oracle<{ column_name: string }>(
          ctx,
          `select column_name from information_schema.columns
            where table_name = 'd1' order by ordinal_position`,
        )
        expect(ocols.map((r) => r.column_name)).toEqual([
          'id',
          'v',
          'n',
          'extra',
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
