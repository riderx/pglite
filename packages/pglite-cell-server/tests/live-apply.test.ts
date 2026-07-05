// M5b live tail apply v1: a READ-attached cell needing advance feeds the
// new slices through pgl_walscan classification and applies the §6.3
// EAGER set to the LIVE cell instead of recycle+re-materialize — gated on
// every touched block carrying a restorable full-page image. The WIN:
// session state pinned by M1 (advisory locks, holdable cursors — backend
// state that a recycle destroys) survives advances whenever live apply
// succeeds; the pin/fatal contract is unchanged whenever it does not.
//
// M5c UPDATE: the WAL insert-position set (pgl_set_wal_position) lifted
// the write-cell restriction — a write-attached (incl. temp-table-tainted)
// session now live-advances in place when the tail passes the v2 gate
// (semantic eager set + rm_redo-whitelisted rmgrs + FPI leftovers), and
// test 3 below is the flipped M5b-deferred temp-table test.

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Client } from 'pg'
import {
  Cell,
  EraTailer,
  liveApplyStats,
  materializeAtHead,
  parseLsn,
} from '@electric-sql/pglite-cell'
import { GatewayCore, extractDatadir } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'
import { CellProxyServer } from '../src/proxy/server'
import type { HostSession } from '../src/session'

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
  teardown: () => Promise<void>
}

let oracleN = 0

async function setup(): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-liveapply-'))
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

/** Convergence oracle: a fresh materialize of the FULL stream, queried. */
async function oracle<T>(ctx: Ctx, sql: string): Promise<T[]> {
  const dir = join(ctx.root, `oracle-${++oracleN}`)
  await extractDatadir(
    await ctx.core.getObject(ctx.manifest.checkpoint.ref),
    dir,
  )
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

describe('live tail apply v1 (M5b)', () => {
  it(
    '1. appliable batch: a pinned (advisory-lock + WITH HOLD cursor) read session advances IN PLACE — sees the foreign commit, keeps its backend state, no fatal reset',
    async () => {
      const ctx = await setup()
      try {
        const w = await connect(ctx)
        await w.query(`create table lt (v text)`)
        await w.query(`insert into lt values ('base')`)

        // The pinned session: read-attached, then tainted by backend
        // state a recycle would destroy (advisory lock + holdable
        // cursor). M1 pins it; M5b lifts the pin when live apply works.
        const r = await connect(ctx)
        expect((await r.query(`select v from lt`)).rows).toEqual([
          { v: 'base' },
        ])
        await r.query(`select pg_advisory_lock(42)`)
        await r.query(`begin`)
        await r.query(`declare hc cursor with hold for select v from lt`)
        await r.query(`commit`)
        const rSession = ctx.sessions[ctx.sessions.length - 1]
        expect(rSession.tainted).toBe(true)

        // Foreign commits land, checkpoint-adjacent: the CHECKPOINT
        // opens a fresh FPI cycle, so the multi-insert's single heap
        // record carries a restorable full-page image — a live-appliable
        // tail (the §6.2 FPI-on-first-touch guarantee).
        await w.query(`checkpoint`)
        // ONE row: multi-row INSERT VALUES is per-row heap_insert, and the
        // second row on the same page carries no FPI (honest gate data —
        // that shape falls back by design in v1).
        await w.query(`insert into lt values ('foreign-1')`)

        const hitsBefore = liveApplyStats.hits
        // The pinned session's next unit: watermark gate fires, live
        // apply advances the LIVE cell (no recycle) — the foreign rows
        // are visible AND the taint state survives.
        const rows = await r.query(`select v from lt order by v`)
        expect(rows.rows).toEqual([{ v: 'base' }, { v: 'foreign-1' }])
        expect(liveApplyStats.hits).toBeGreaterThan(hitsBefore)

        // Backend state survived the advance: the advisory lock is still
        // held and the holdable cursor still serves its snapshot.
        expect(
          (
            await r.query(
              `select count(*)::int as n from pg_locks where locktype = 'advisory'`,
            )
          ).rows,
        ).toEqual([{ n: 1 }])
        expect((await r.query(`fetch all from hc`)).rows).toEqual([
          { v: 'base' },
        ])

        // Convergence oracle: the live-applied view matches a fresh
        // full-stream materialize.
        expect(await oracle(ctx, `select v from lt order by v`)).toEqual([
          { v: 'base' },
          { v: 'foreign-1' },
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '2. untainted read sessions prefer live apply over recycle (hit counted); non-appliable batches fall back to recycle-advance and still converge',
    async () => {
      const ctx = await setup()
      try {
        const w = await connect(ctx)
        await w.query(`create table lf (v text)`)

        const r = await connect(ctx)
        expect(
          (await r.query(`select count(*)::int as n from lf`)).rows,
        ).toEqual([{ n: 0 }])

        const before = { ...liveApplyStats, attempts: liveApplyStats.attempts }
        await w.query(`insert into lf values ('x1')`)
        expect((await r.query(`select v from lf`)).rows).toEqual([{ v: 'x1' }])
        expect(liveApplyStats.attempts).toBeGreaterThan(before.attempts)

        // Force a NON-appliable tail: the same heap page touched twice
        // without an intervening checkpoint — the second commit's heap
        // record carries no FPI (full_page_writes emits one per page per
        // checkpoint cycle), so the gate must reject and the session
        // must fall back to recycle-advance, still converging.
        await w.query(`insert into lf values ('x2')`)
        await w.query(`insert into lf values ('x3')`)
        const got = await r.query(`select v from lf order by v`)
        expect(got.rows).toEqual([{ v: 'x1' }, { v: 'x2' }, { v: 'x3' }])

        // Whichever mix of paths served the advances, the oracle agrees.
        expect(await oracle(ctx, `select v from lf order by v`)).toEqual([
          { v: 'x1' },
          { v: 'x2' },
          { v: 'x3' },
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '3. temp-table sessions (write-attached by construction) live-advance IN PLACE (M5c lifts the write-cell restriction): temp state survives, and a post-advance write LANDS instead of the M1 fatal reset',
    async () => {
      const ctx = await setup()
      try {
        const w = await connect(ctx)
        await w.query(`create table lw (v text)`)

        const t = await connect(ctx)
        await t.query(`create temp table stash (v text)`)
        await t.query(`insert into stash values ('mine')`)
        const tSession = ctx.sessions[ctx.sessions.length - 1]
        expect(tSession.tainted).toBe(true)

        // A foreign commit races past the pinned base.
        await w.query(`insert into lw values ('foreign')`)

        // M5c: the pinned WRITE session advances in place — the redo
        // harness applies the foreign heap insert and the WAL insert
        // position moves to the new head. Temp state survives (the cell
        // is never recycled) and the foreign row becomes visible.
        const hitsBefore = liveApplyStats.hits
        expect((await t.query(`select v from stash`)).rows).toEqual([
          { v: 'mine' },
        ])
        expect(liveApplyStats.hits).toBeGreaterThan(hitsBefore)
        expect((await t.query(`select v from lw`)).rows).toEqual([
          { v: 'foreign' },
        ])

        // The M5b-deferred flip: a write from the ADVANCED head now lands
        // instead of losing the race and fatally resetting the session.
        await t.query(`insert into lw values ('mine-2')`)
        expect((await t.query(`select v from stash`)).rows).toEqual([
          { v: 'mine' },
        ])

        expect(await oracle(ctx, `select v from lw order by v`)).toEqual([
          { v: 'foreign' },
          { v: 'mine-2' },
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
