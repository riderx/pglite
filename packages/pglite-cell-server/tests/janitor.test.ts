// M6 janitor tests (§6.4): background maintenance on the host, all OFF by
// default. A write-heavy database under a fast vacuum janitor keeps working
// (oracle green; the vacuum commits show up as ordinary W frames in the
// stream); the freeze-age query path is exercised against a mocked threshold
// (real xids cannot be aged in a unit test); GC fires on schedule (spy).
//
// Each test builds its own GatewayCore + CellHost (many PGlite boots —
// generous timeouts).

import { describe, it, expect, vi } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  Cell,
  EraTailer,
  materializeAtHead,
  parseLsn,
} from '@electric-sql/pglite-cell'
import { GatewayCore, extractDatadir } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'
import type { RuntimeOpts } from '../src/database-runtime'

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

async function setup(opts?: RuntimeOpts): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-janitor-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host'),
    hostId: 'h1',
    opts,
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
      rmSync(root, { recursive: true, force: true })
    },
  }
}

/** Full-stream materialize oracle, re-reading the LATEST manifest (a vacuum
 *  may have moved the checkpoint the janitor auto-fired). */
async function oracle<T>(ctx: Ctx, sql: string): Promise<T[]> {
  const manifest = await ctx.core.getManifest(ctx.dbId)
  const dir = join(ctx.root, `oracle-${++oracleN}`)
  await extractDatadir(await ctx.core.getObject(manifest.checkpoint.ref), dir)
  const tailer = new EraTailer(ctx.core.streamClientFor(ctx.dbId), {
    path: manifest.era.path,
    eraId: manifest.era.id,
    ordinal: manifest.era.ordinal,
    baseOffset: manifest.era.baseOffset,
    baseLsn: parseLsn(manifest.era.baseLsn),
  })
  await tailer.catchUp()
  const mat = await materializeAtHead({
    baseDir: dir,
    slices: tailer.slicesSince(parseLsn(manifest.checkpoint.snapEnd)),
  })
  const cell = await Cell.open(dir, { expectedHeadLsn: mat.headLsn })
  const rows = (await cell.db.query<T>(sql)).rows
  await cell.db.close()
  return rows
}

/** Count W frames whose kind is 'commit' currently on the stream. */
async function commitFrameCount(ctx: Ctx): Promise<number> {
  const manifest = await ctx.core.getManifest(ctx.dbId)
  const tailer = new EraTailer(ctx.core.streamClientFor(ctx.dbId), {
    path: manifest.era.path,
    eraId: manifest.era.id,
    ordinal: manifest.era.ordinal,
    baseOffset: manifest.era.baseOffset,
    baseLsn: parseLsn(manifest.era.baseLsn),
  })
  await tailer.catchUp()
  return tailer.slicesSince(0n).filter((s) => s.kind === 'commit').length
}

describe('Janitor (M6 §6.4)', () => {
  it(
    'is fully off by default: no timers, no maintenance',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec('create table t (id serial primary key, v int)')
        const rt = ctx.host.runtimeFor(ctx.dbId)!
        expect(rt.janitor).toBeNull()
        await s.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'write-heavy db under a fast vacuum janitor stays correct; vacuum commits ride the stream',
    async () => {
      const ctx = await setup({ janitor: { vacuumIntervalMs: 5 } })
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec('create table t (id serial primary key, v int)')
        const rt = ctx.host.runtimeFor(ctx.dbId)!
        expect(rt.janitor).not.toBeNull()

        const before = await commitFrameCount(ctx)
        // Interleave writes with explicit vacuum runs (deterministic — the
        // timer would also fire, but we drive it to avoid flake).
        for (let i = 0; i < 20; i++) {
          const r = await s.exec(`insert into t (v) values (${i})`)
          expect(r.outcome).toBe('committed')
          if (i % 5 === 0) await rt.janitor!.runVacuumNow()
        }
        // The oracle (a fresh full-stream materialize) must see every row —
        // the vacuum commits interleaved in the stream did not corrupt it.
        const rows = await oracle<{ n: string }>(
          ctx,
          'select count(*)::text as n from t',
        )
        expect(rows[0].n).toBe('20')
        expect(rt.janitor!.stats.vacuums).toBeGreaterThan(0)
        // Vacuum's ANALYZE writes WAL → the commit landed as a W frame.
        expect(await commitFrameCount(ctx)).toBeGreaterThan(before)
        await s.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'freeze-age query path runs; a mocked threshold below the observed age forces VACUUM (FREEZE)',
    async () => {
      const ctx = await setup({ janitor: { freezeMaxAge: 1 } })
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec('create table t (id int)')
        const rt = ctx.host.runtimeFor(ctx.dbId)!
        const j = rt.janitor!
        // First check with the real (small) age but freezeMaxAge=1: the age
        // is queried and recorded; whether it freezes depends on the real
        // frozen horizon, so force the branch with the test override.
        j._forceAgeForTest = 1_000_000_000
        await j.runFreezeCheckNow()
        expect(j.stats.lastFrozenAge).toBeGreaterThanOrEqual(0) // query ran
        expect(j.stats.freezes).toBe(1) // forced age past threshold → froze
        await s.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'freeze check does NOT freeze when the observed age is below the threshold',
    async () => {
      const ctx = await setup({ janitor: { freezeMaxAge: 2_000_000_000 } })
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec('create table t (id int)')
        const rt = ctx.host.runtimeFor(ctx.dbId)!
        const j = rt.janitor!
        await j.runFreezeCheckNow() // real age is tiny, threshold huge
        expect(j.stats.lastFrozenAge).toBeGreaterThanOrEqual(0)
        expect(j.stats.freezes).toBe(0)
        await s.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'GC fires on schedule (runGc spy)',
    async () => {
      const ctx = await setup({ janitor: { gcIntervalMs: 10 } })
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec('create table t (id int)')
        const rt = ctx.host.runtimeFor(ctx.dbId)!
        const spy = vi.spyOn(rt.gateway, 'runGc')
        // Wait for a couple of scheduled ticks.
        await new Promise((r) => setTimeout(r, 60))
        expect(spy).toHaveBeenCalledWith(ctx.dbId)
        expect(rt.janitor!.stats.gcRuns).toBeGreaterThan(0)
        await s.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'janitor timers stop on hibernate; onHibernate freeze hook runs',
    async () => {
      const ctx = await setup({
        janitor: { freezeMaxAge: 1 },
        // deterministic explicit hibernate below
      })
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec('create table t (id int)')
        const rt = ctx.host.runtimeFor(ctx.dbId)!
        const j = rt.janitor!
        j._forceAgeForTest = 1_000_000_000
        await s.close()
        await ctx.host.hibernateDatabase('appdb')
        // After hibernate the janitor is torn down and the run-once freeze
        // hook fired (forced age → a freeze happened on the hibernate path).
        expect(rt.state).toBe('hibernated')
        expect(rt.janitor).toBeNull()
        expect(j.stats.freezes).toBe(1)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
