// M5d exit tests — the §16 rebase soundness suite: every verified failure
// mode of transparent interactive rebase (design §4.2/§4.4/§4.5/§4.7),
// each asserting loud failure (40001) where transparency is impossible
// and byte-stable client observations where it is not. The convergence
// oracle (a fresh full-stream materialize) closes every test.

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
import { SerializationConflictError } from '../src/errors'
import type { HostSession } from '../src/session'
import type { DatabaseRuntime } from '../src/database-runtime'

const TEST_TIMEOUT = 240_000

interface Ctx {
  root: string
  core: GatewayCore
  host: CellHost
  manifest: Manifest
  dbId: string
  teardown: () => Promise<void>
}

let oracleN = 0

async function setup(): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-rebase-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host'),
    hostId: 'h1',
  })
  return {
    root,
    core,
    host,
    manifest,
    dbId: manifest.databaseId,
    teardown: async () => {
      await host.shutdown().catch(() => undefined)
      await core.stop()
      if (!process.env.PGL_KEEP) rmSync(root, { recursive: true, force: true })
    },
  }
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

/** Expect a SerializationConflictError (40001) from an exec. */
async function expect40001(p: Promise<unknown>): Promise<string> {
  let err: unknown
  try {
    await p
  } catch (e) {
    err = e
  }
  expect(err).toBeInstanceOf(SerializationConflictError)
  expect((err as SerializationConflictError).code).toBe('40001')
  return String((err as Error).message)
}

/** Write-attach a session (sticky) and return it. */
async function writer(ctx: Ctx, seedSql: string): Promise<HostSession> {
  const s = await ctx.host.connect('appdb')
  expect((await s.exec(seedSql)).outcome).toBe('committed')
  return s
}

describe('§16 rebase soundness suite (M5d)', () => {
  it(
    '1. happy path: think-time txn vs an UNRELATED foreign commit ⇒ transparent rebase — COMMIT lands, client-observed serial ids and now() are byte-stable, oracle exactly-once',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(
          `create table ta (id serial primary key, v text, ts timestamptz);
           create table tb (id serial primary key, v text)`,
        )

        const a = await writer(ctx, `insert into ta (v) values ('a-seed')`)
        await a.exec(`begin`)
        const ins = await a.exec(
          `insert into ta (v, ts) values ('a-txn', now()) returning id, ts::text as ts`,
        )
        expect(ins.outcome).toBe('in-transaction')
        const seenId = ins.rows[0].id as number
        const seenTs = ins.rows[0].ts as string

        // Foreign commit to an UNRELATED table while A is thinking.
        const b = await ctx.host.connect('appdb')
        expect((await b.exec(`insert into tb (v) values ('b1')`)).outcome).toBe(
          'committed',
        )

        // A's COMMIT loses the CAS — and transparently rebases.
        const commit = await a.exec(`commit`)
        expect(commit.outcome).toBe('committed')
        expect(
          (a as unknown as { rebaseStats: { landed: number } }).rebaseStats
            .landed,
        ).toBe(1)

        // Exactly-once with STABLE observed values: the serial id and the
        // volatile now() the client saw at B are what landed at K
        // (harvested data — nothing re-evaluated, §4.4).
        const rows = await oracle<{ id: number; v: string; ts: string }>(
          ctx,
          `select id, v, ts::text as ts from ta order by id`,
        )
        expect(rows).toHaveLength(2)
        expect(rows[0]).toMatchObject({ v: 'a-seed', ts: null })
        expect(rows[1]).toEqual({ id: seenId, v: 'a-txn', ts: seenTs })
        const bRows = await oracle<{ v: string }>(ctx, `select v from tb`)
        expect(bRows).toEqual([{ v: 'b1' }])

        // The session is fully alive at the new head.
        const after = await a.exec(`select count(*)::int as n from tb`)
        expect(after.rows).toEqual([{ n: 1 }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '2. winner-touched-page: the foreign commit modifies a page the transaction read ⇒ 40001 (pageLSN(K) <= B)',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(
          `create table tc (id int primary key, v text);
           insert into tc values (1, 'one'), (2, 'two');
           create table td (v text)`,
        )

        const a = await writer(ctx, `insert into td values ('a-seed')`)
        await a.exec(`begin`)
        // Read tc (pins its pages), write only td.
        expect((await a.exec(`select v from tc where id = 1`)).rows).toEqual([
          { v: 'one' },
        ])
        await a.exec(`insert into td values ('a-txn')`)

        const b = await ctx.host.connect('appdb')
        expect(
          (await b.exec(`update tc set v = 'two!' where id = 2`)).outcome,
        ).toBe('committed')

        const msg = await expect40001(a.exec(`commit`))
        expect(msg).toMatch(/rebase/)

        // Session survives; the raced write is absent, the winner present.
        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from td order by v`,
        )
        expect(rows).toEqual([{ v: 'a-seed' }])
        expect(await oracle(ctx, `select v from tc where id = 2`)).toEqual([
          { v: 'two!' },
        ])
        expect((await a.exec(`select 1 as one`)).rows).toEqual([{ one: 1 }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '3. relation-extension phantom: a seqscanned table grown by the winner onto a FRESH page ⇒ 40001 (nblocks rule — no read page changed)',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        // No indexes; fat rows + low fillfactor force the winner's insert
        // onto a NEW page (no page the loser's seqscan read is touched —
        // only the nblocks rule can see this, §4.1's verified false
        // negative without it).
        await s.exec(
          `create table te (v text) with (fillfactor = 10);
           insert into te values (repeat('x', 1400));
           create table tf (v text)`,
        )

        const a = await writer(ctx, `insert into tf values ('a-seed')`)
        await a.exec(`begin`)
        expect(
          (await a.exec(`select count(*)::int as n from te`)).rows,
        ).toEqual([{ n: 1 }])
        await a.exec(`insert into tf values ('a-txn')`)

        const b = await ctx.host.connect('appdb')
        expect(
          (await b.exec(`insert into te values (repeat('y', 1400))`)).outcome,
        ).toBe('committed')

        const msg = await expect40001(a.exec(`commit`))
        expect(msg).toMatch(/nblocks grown/)

        expect(await oracle(ctx, `select v from tf`)).toEqual([{ v: 'a-seed' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    "4. own-WAL masking: the loser also wrote the contested page (its own LSNs sit past B) ⇒ still 40001 — the pageLSN(K) <= B form is immune to the loser's own WAL",
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(
          `create table tg (id int primary key, v text);
           insert into tg values (1, 'one'), (2, 'two')`,
        )

        const a = await writer(ctx, `update tg set v = 'one-seed' where id = 1`)
        await a.exec(`begin`)
        // A writes page 0 of tg — its own WAL stamps the page past B.
        await a.exec(`update tg set v = 'one-a' where id = 1`)

        // B updates ANOTHER row on the same page.
        const b = await ctx.host.connect('appdb')
        expect(
          (await b.exec(`update tg set v = 'two-b' where id = 2`)).outcome,
        ).toBe('committed')

        await expect40001(a.exec(`commit`))

        const rows = await oracle<{ id: number; v: string }>(
          ctx,
          `select id, v from tg order by id`,
        )
        expect(rows).toEqual([
          { id: 1, v: 'one-seed' },
          { id: 2, v: 'two-b' },
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '5. catalog fence: winner DDL on a relation in the transaction footprint ⇒ 40001 (commit-record invals ∩ footprint; page validation on pinned catalog pages backstops). VACUUM-inplace carrier: not cheaply constructible in-suite — covered by the same decode path (heap_inplace invals), documented.',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(
          `create table th (id int primary key, v text);
           insert into th values (1, 'one');
           create table ti (v text)`,
        )

        const a = await writer(ctx, `insert into ti values ('a-seed')`)
        await a.exec(`begin`)
        expect((await a.exec(`select v from th where id = 1`)).rows).toEqual([
          { v: 'one' },
        ])
        await a.exec(`insert into ti values ('a-txn')`)

        const b = await ctx.host.connect('appdb')
        expect(
          (await b.exec(`alter table th add column extra int`)).outcome,
        ).toBe('committed')

        await expect40001(a.exec(`commit`))

        expect(await oracle(ctx, `select v from ti`)).toEqual([{ v: 'a-seed' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '6. aborted-subxact filtering: rows from a ROLLBACK TO branch do NOT re-apply; the committed branch does — exactly-once, values stable',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(
          `create table tj (v text);
           create table tk (v text)`,
        )

        const a = await writer(ctx, `insert into tj values ('a-seed')`)
        await a.exec(`begin`)
        await a.exec(`insert into tj values ('keep-1')`)
        await a.exec(`savepoint sp1`)
        await a.exec(`insert into tj values ('drop-1')`)
        await a.exec(`update tj set v = 'drop-2' where v = 'keep-1'`)
        await a.exec(`rollback to sp1`)
        await a.exec(`insert into tj values ('keep-2')`)
        await a.exec(`update tj set v = 'a-seed!' where v = 'a-seed'`)

        const b = await ctx.host.connect('appdb')
        expect((await b.exec(`insert into tk values ('b1')`)).outcome).toBe(
          'committed',
        )

        expect((await a.exec(`commit`)).outcome).toBe('committed')

        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from tj order by v`,
        )
        expect(rows).toEqual([
          { v: 'a-seed!' },
          { v: 'keep-1' },
          { v: 'keep-2' },
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '7. TOAST: a 100KB value survives rebase byte-identically (harvested detoasted, re-chunked at K — §4.4)',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(
          `create table tl (id int primary key, blob text);
           create table tm (v text)`,
        )

        const a = await writer(ctx, `insert into tm values ('a-seed')`)
        await a.exec(`begin`)
        // ~100KB of poorly-compressible data (forces out-of-line TOAST).
        await a.exec(
          `insert into tl values (1, (select string_agg(md5(g::text), '') from generate_series(1, 3200) g))`,
        )
        const local = await a.exec(
          `select md5(blob) as h, length(blob)::int as len from tl where id = 1`,
        )

        const b = await ctx.host.connect('appdb')
        expect((await b.exec(`insert into tm values ('b1')`)).outcome).toBe(
          'committed',
        )

        expect((await a.exec(`commit`)).outcome).toBe('committed')

        const rows = await oracle<{ h: string; len: number }>(
          ctx,
          `select md5(blob) as h, length(blob)::int as len from tl where id = 1`,
        )
        expect(rows).toEqual(local.rows)
        expect(rows[0].len).toBe(3200 * 32)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '8. uniqueness at K: a same-key race surfaces as 40001, never 23505 (§4.0) — end-to-end the read-set catches it; the re-apply 23505→40001 conversion is exercised at unit level in reset-redo/rebase internals',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table tn (k text unique, v text)`)

        const a = await writer(ctx, `insert into tn values ('seed', 's')`)
        await a.exec(`begin`)
        await a.exec(`insert into tn values ('dup', 'a')`)

        const b = await ctx.host.connect('appdb')
        expect(
          (await b.exec(`insert into tn values ('dup', 'b')`)).outcome,
        ).toBe('committed')

        const msg = await expect40001(a.exec(`commit`))
        expect(msg).not.toMatch(/23505|duplicate key/)

        const rows = await oracle<{ k: string; v: string }>(
          ctx,
          `select k, v from tn order by k`,
        )
        expect(rows).toEqual([
          { k: 'dup', v: 'b' },
          { k: 'seed', v: 's' },
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '9. ctid-projection taint (§4.5): a transaction that observed ctid loses ⇒ 40001, never rebase (statement-text scan)',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table tp (v text); create table tq (v text)`)

        const a = await writer(ctx, `insert into tq values ('a-seed')`)
        await a.exec(`begin`)
        await a.exec(`select ctid, v from tp`)
        await a.exec(`insert into tq values ('a-txn')`)

        const b = await ctx.host.connect('appdb')
        expect((await b.exec(`insert into tp values ('b1')`)).outcome).toBe(
          'committed',
        )

        const msg = await expect40001(a.exec(`commit`))
        expect(msg).toMatch(/taint 'ctid'/)

        expect(await oracle(ctx, `select v from tq`)).toEqual([{ v: 'a-seed' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '10. temp-write-during-attempt (§4.5): temp state created inside the losing attempt ⇒ 40001 (zombie-xid hazard); the session SURVIVES (now pinned-tainted)',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table tr (v text); create table ts (v text)`)

        const a = await writer(ctx, `insert into ts values ('a-seed')`)
        await a.exec(`begin`)
        await a.exec(`create temp table scratch (x int)`)
        await a.exec(`insert into scratch values (1)`)
        await a.exec(`insert into ts values ('a-txn')`)

        const b = await ctx.host.connect('appdb')
        expect((await b.exec(`insert into tr values ('b1')`)).outcome).toBe(
          'committed',
        )

        const msg = await expect40001(a.exec(`commit`))
        expect(msg).toMatch(/temp|catalog/)

        // Session survives (§3.7: fatal is reserved for SESSION-state
        // taint losing with no rebase window).
        expect((await a.exec(`select 1 as one`)).rows).toEqual([{ one: 1 }])
        expect(await oracle(ctx, `select v from ts`)).toEqual([{ v: 'a-seed' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '11. bounds (§4.7): two consecutive rebase CAS losses ⇒ 40001; the harvest is never replayed a third time',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table tu (v text); create table tv (v text)`)

        const a = await writer(ctx, `insert into tu values ('a-seed')`)
        const b = await writer(ctx, `insert into tv values ('b-seed')`)

        await a.exec(`begin`)
        await a.exec(`insert into tu values ('a-txn')`)
        expect((await b.exec(`insert into tv values ('b1')`)).outcome).toBe(
          'committed',
        )

        // Interpose on A's commit path: land a fresh foreign commit
        // immediately BEFORE every rebase CAS, so each round loses again.
        const runtime = (a as unknown as { runtime: DatabaseRuntime }).runtime
        const original = runtime.commitFromSession.bind(runtime)
        let interposed = 0
        let inInterpose = false // B's own commit must pass straight through
        runtime.commitFromSession = (async (input) => {
          if (input.kind === 'commit' && !inInterpose && interposed < 3) {
            interposed++
            inInterpose = true
            try {
              await b.exec(`insert into tv values ('b-race-${interposed}')`)
            } finally {
              inInterpose = false
            }
          }
          return original(input)
        }) as typeof runtime.commitFromSession

        try {
          const msg = await expect40001(a.exec(`commit`))
          expect(msg).toMatch(/bounds exhausted/)
        } finally {
          runtime.commitFromSession = original
        }
        // COMMIT attempt + 2 rebase rounds — never a third re-apply.
        expect(interposed).toBe(3)

        const stats = (
          a as unknown as {
            rebaseStats: { attempts: number; landed: number; failed: number }
          }
        ).rebaseStats
        expect(stats.landed).toBe(0)
        expect(stats.failed).toBe(1)

        expect(await oracle(ctx, `select count(*)::int as c from tu`)).toEqual([
          { c: 1 },
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '12. proxy-level: a real pg client BEGIN/read/write/COMMIT with a foreign commit mid-think succeeds with NO client-visible error',
    async () => {
      const ctx = await setup()
      const sessions: HostSession[] = []
      const proxy = new CellProxyServer({
        host: ctx.host,
        port: 0,
        onSession: (sess) => sessions.push(sess),
      })
      const port = await proxy.start()
      const clients: Client[] = []
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(
          `create table tw (id serial primary key, v text);
           create table tx (v text)`,
        )

        const a = new Client({
          host: '127.0.0.1',
          port,
          database: 'appdb',
          user: 'postgres',
        })
        a.on('error', () => undefined)
        await a.connect()
        clients.push(a)

        // Write-attach the wire session.
        await a.query(`insert into tw (v) values ('a-seed')`)
        await a.query(`begin`)
        const r = await a.query(`select v from tw order by id`)
        expect(r.rows).toEqual([{ v: 'a-seed' }])
        const ins = await a.query(
          `insert into tw (v) values ('a-txn') returning id`,
        )
        const seenId = ins.rows[0].id as number

        // Foreign commit mid-think, UNRELATED table.
        const b = await ctx.host.connect('appdb')
        expect((await b.exec(`insert into tx values ('b1')`)).outcome).toBe(
          'committed',
        )

        // COMMIT: no error, normal tag.
        const commit = await a.query(`commit`)
        expect(commit.command).toBe('COMMIT')

        const rows = await oracle<{ id: number; v: string }>(
          ctx,
          `select id, v from tw order by id`,
        )
        expect(rows).toHaveLength(2)
        expect(rows[0]).toMatchObject({ v: 'a-seed' })
        expect(rows[1]).toEqual({ id: seenId, v: 'a-txn' })
        // The wire session's ladder landed exactly once.
        const wireSession = sessions[sessions.length - 1]
        expect(
          (wireSession as unknown as { rebaseStats: { landed: number } })
            .rebaseStats.landed,
        ).toBe(1)
      } finally {
        for (const c of clients) await c.end().catch(() => undefined)
        await proxy.stop().catch(() => undefined)
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
