// H4 §9 feature policing: REAL `pg` clients over TCP, each classifier
// surfacing its 0A000 at the wire, plus the setval() allowed-with-regrant
// path (§5.3 rule 5 kept honest without breaking migrations that setval).

import { describe, it, expect } from 'vitest'
import { rmSync } from 'node:fs'
import { join } from 'node:path'
import { Client } from 'pg'
import { EraTailer, parseLsn } from '@electric-sql/pglite-cell'
import { GatewayCore } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { scratchDir } from '@electric-sql/pglite-cell/testing'
import { CellHost } from '../src/host'
import { CellProxyServer } from '../src/proxy/server'
import type { RuntimeOpts } from '../src/database-runtime'

const TEST_TIMEOUT = 240_000

interface Ctx {
  root: string
  core: GatewayCore
  host: CellHost
  port: number
  manifest: Manifest
  dbId: string
  clients: Client[]
  teardown: () => Promise<void>
}

async function setup(opts: RuntimeOpts = {}): Promise<Ctx> {
  const root = scratchDir('pgl-police-')
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host'),
    hostId: 'h1',
    opts,
  })
  const proxy = new CellProxyServer({ host, port: 0 })
  const port = await proxy.start()
  const clients: Client[] = []
  return {
    root,
    core,
    host,
    port,
    manifest,
    dbId: manifest.databaseId,
    clients,
    teardown: async () => {
      for (const c of clients) await c.end().catch(() => undefined)
      await proxy.stop().catch(() => undefined)
      await host.shutdown().catch(() => undefined)
      await core.stop()
      rmSync(root, { recursive: true, force: true })
    },
  }
}

async function connect(ctx: Ctx): Promise<Client> {
  const client = new Client({
    host: '127.0.0.1',
    port: ctx.port,
    database: 'appdb',
    user: 'postgres',
  })
  client.on('error', () => undefined)
  await client.connect()
  ctx.clients.push(client)
  return client
}

async function expect0A000(c: Client, sql: string): Promise<string> {
  let err: unknown
  try {
    await c.query(sql)
  } catch (e) {
    err = e
  }
  expect(err, `expected ${sql} to error`).toBeDefined()
  const e = err as { code?: string; message?: string }
  expect(e.code, `wrong SQLSTATE for: ${sql}`).toBe('0A000')
  return e.message ?? ''
}

describe('H4 §9 statement policing (0A000)', () => {
  it(
    'PREPARE TRANSACTION is rejected as unsupported two-phase commit',
    async () => {
      const ctx = await setup()
      try {
        const c = await connect(ctx)
        await c.query('begin')
        await c
          .query('create temp table t (x int) on commit drop')
          .catch(() => undefined)
        const msg = await expect0A000(c, "prepare transaction 'gid1'")
        expect(msg.toLowerCase()).toContain('two-phase commit')
        // The connection survives; roll back and continue.
        await c.query('rollback').catch(() => undefined)
        const ok = await c.query('select 1 as n')
        expect(ok.rows[0].n).toBe(1)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'CREATE DATABASE and CREATE TABLESPACE are rejected (single-database stream)',
    async () => {
      const ctx = await setup()
      try {
        const c = await connect(ctx)
        const m1 = await expect0A000(c, 'create database other')
        expect(m1.toLowerCase()).toContain('create database')
        const m2 = await expect0A000(
          c,
          "create tablespace ts location '/tmp/ts'",
        )
        expect(m2.toLowerCase()).toContain('create tablespace')
        const ok = await c.query('select 1 as n')
        expect(ok.rows[0].n).toBe(1)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'ALTER SEQUENCE ... RESTART is rejected on a leased sequence',
    async () => {
      const ctx = await setup()
      try {
        const c = await connect(ctx)
        await c.query('create table t (id serial primary key, v int)')
        await c.query('insert into t (v) values (1), (2)')
        const m1 = await expect0A000(c, 'alter sequence t_id_seq restart')
        expect(m1.toLowerCase()).toContain('restart')
        await expect0A000(c, 'alter sequence t_id_seq restart with 1000')
        // A NON-restart ALTER SEQUENCE still works (only RESTART is policed).
        const ok = await c.query('alter sequence t_id_seq increment by 1')
        expect(ok).toBeDefined()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})

describe('H4 §5.3 rule 5: setval() allowed with post-txn re-grant', () => {
  it(
    'setval above the current grant re-grants strictly above and floors republish',
    async () => {
      // Tiny grant so the first inserts take a small grant we can see moved.
      const ctx = await setup({ sequenceGrantSize: 16n })
      try {
        const c = await connect(ctx)
        await c.query('create table t (id bigint primary key, v int)')
        // Draw a few values so a grant exists.
        await c
          .query(
            "insert into t (id, v) select nextval('__none__'), 0 where false",
          )
          .catch(() => undefined)
        await c.query('create sequence s')
        await c.query("select nextval('s')") // grant taken around 1..16
        // setval far above the current grant.
        await c.query('select setval($1, $2)', ['s', 5_000_000])
        // Every subsequent nextval must be STRICTLY above the set value —
        // the host re-probed and re-granted above 5,000,000.
        const r = await c.query("select nextval('s') as n")
        expect(BigInt(r.rows[0].n)).toBeGreaterThan(5_000_000n)

        // The floors republished at/above the set value: a fresh materialize
        // of the era chain sees the sequence at >= the set value.
        const tailer = new EraTailer(ctx.core.streamClientFor(ctx.dbId), {
          path: ctx.manifest.era.path,
          eraId: ctx.manifest.era.id,
          ordinal: ctx.manifest.era.ordinal,
          baseOffset: ctx.manifest.era.baseOffset,
          baseLsn: parseLsn(ctx.manifest.era.baseLsn),
        })
        await tailer.catchUp()
        // A G-frame (grant) for `s` must exist above the set value.
        const grants = tailer.grantHighWaters().get('public.s')
        expect(grants ?? 0n).toBeGreaterThan(5_000_000n)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'a migration that setval()s a sequence mid-flow is not broken',
    async () => {
      const ctx = await setup({ sequenceGrantSize: 16n })
      try {
        const c = await connect(ctx)
        // Simulate a data-import migration: create table+sequence, load rows
        // with explicit ids, then setval to the max id, then continue with
        // nextval-driven inserts (the classic pg_dump restore shape).
        await c.query('create table users (id bigint primary key, name text)')
        await c.query('create sequence users_id_seq owned by users.id')
        await c.query(
          "alter table users alter column id set default nextval('users_id_seq')",
        )
        await c.query(
          "insert into users (id, name) select g, 'u'||g from generate_series(1, 100) g",
        )
        const setr = await c.query("select setval('users_id_seq', 100)")
        expect(BigInt(setr.rows[0].setval)).toBe(100n)
        // Continue with default-driven inserts: ids must be > 100, unique.
        const ins = await c.query(
          "insert into users (name) values ('a'), ('b') returning id",
        )
        const ids = ins.rows.map((r) => BigInt(r.id))
        for (const id of ids) expect(id).toBeGreaterThan(100n)
        expect(new Set(ids).size).toBe(ids.length)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
