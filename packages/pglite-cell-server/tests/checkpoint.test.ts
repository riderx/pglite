// M1e checkpoint worker + wake-from-checkpoint tests. Each test builds its
// own GatewayCore + CellHost (many PGlite boots — generous timeouts).
//
//   (a) checkpoint -> K frame on the stream + control-plane row + manifest
//       serves it as latest;
//   (b) wake-from-checkpoint: write, checkpoint, write more (tail), hibernate
//       WITHOUT a second checkpoint, wake -> all rows; hydration replayed
//       only slices with baseLsn >= snapEnd (the checkpoint's streamOffset);
//   (c) idempotency: a second immediate run is skipped;
//   (d) checkpoint-on-hibernate default: hibernate -> wake replays ZERO W
//       slices past the checkpoint;
//   (e) a commit racing the K frame: land a commit between the sync publish
//       and the K append -> checkpoint still completes, joiner gets both.

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  Cell,
  EraTailer,
  materializeAtHead,
  parseLsn,
} from '@electric-sql/pglite-cell'
import { GatewayCore, extractCheckpoint } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'

const TEST_TIMEOUT = 240_000

interface Ctx {
  root: string
  core: GatewayCore
  host: CellHost
  manifest: Manifest
  dbId: string
  teardown: () => Promise<void>
}

async function setup(hostOpts?: {
  checkpointOnHibernateBytes?: number
  checkpointEveryBytes?: number
}): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-ckpt-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host'),
    hostId: 'h1',
    opts: hostOpts,
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

/** A fresh tailer over the current era, caught up to head. */
async function freshTailer(ctx: Ctx): Promise<EraTailer> {
  const tailer = new EraTailer(ctx.core.streamClientFor(ctx.dbId), {
    path: ctx.manifest.era.path,
    eraId: ctx.manifest.era.id,
    ordinal: ctx.manifest.era.ordinal,
    baseOffset: ctx.manifest.era.baseOffset,
    baseLsn: parseLsn(ctx.manifest.era.baseLsn),
  })
  await tailer.catchUp()
  return tailer
}

/**
 * Materialize the LATEST manifest checkpoint + its tail directly, exactly as
 * the runtime does at wake, and return both the row count for a query AND the
 * number of W slices replayed past the checkpoint (the wake-cost instrument).
 */
async function wakeOracle<T>(
  ctx: Ctx,
  sql: string,
): Promise<{ rows: T[]; replayedSlices: number; snapEnd: bigint }> {
  const manifest = await ctx.core.getManifest(ctx.dbId)
  const dir = join(ctx.root, `wake-${Math.random().toString(16).slice(2)}`)
  await extractCheckpoint(manifest.checkpoint.ref, dir, {
    store: {
      get: (r: string) => ctx.core.getObject(r),
      put: (b: Uint8Array) => ctx.core.putObject(b),
    },
  })
  const tailer = await freshTailer(ctx)
  const snapEnd = parseLsn(manifest.checkpoint.snapEnd)
  const slices = tailer.slicesSince(snapEnd)
  // Every replayed slice must sit AT or PAST the checkpoint snapEnd — the
  // hydration invariant (tail from the checkpoint's streamOffset).
  for (const s of slices) expect(s.baseLsn >= snapEnd).toBe(true)
  const mat = await materializeAtHead({ baseDir: dir, slices })
  const cell = await Cell.open(dir, { expectedHeadLsn: mat.headLsn })
  const rows = (await cell.db.query<T>(sql)).rows
  await cell.db.close()
  return { rows, replayedSlices: slices.length, snapEnd }
}

describe('checkpoint worker (M1e)', () => {
  it(
    '(a) checkpoint -> K frame on the stream + control-plane row + manifest serves it as latest',
    async () => {
      const ctx = await setup({ checkpointOnHibernateBytes: -1 })
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t (id serial primary key, v text)`)
        await s.exec(`insert into t (v) values ('one')`)

        const before = await ctx.core.getManifest(ctx.dbId)
        const report = await ctx.host.checkpointDatabase('appdb')
        expect(report.skipped).toBe(false)
        expect(report.objectBytes).toBeGreaterThan(0)

        // Control-plane row: getManifest now serves the NEW checkpoint.
        const after = await ctx.core.getManifest(ctx.dbId)
        expect(after.checkpoint.ref).toBe(report.checkpointRef)
        expect(after.checkpoint.snapEnd).toBe(report.snapEnd)
        expect(after.checkpoint.streamOffset).toBe(report.streamOffset)
        expect(after.checkpoint.ref).not.toBe(before.checkpoint.ref)

        // K frame visible through a fresh tailer.
        const tailer = await freshTailer(ctx)
        expect(tailer.latestCheckpoint).not.toBeNull()
        expect(tailer.latestCheckpoint?.checkpointRef).toBe(
          report.checkpointRef,
        )
        expect(tailer.latestCheckpoint?.snapEnd).toBe(report.snapEnd)
        expect(tailer.latestCheckpoint?.lsn).toBe(report.lsn)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '(b) wake from checkpoint: write 50, checkpoint, write 10 more, hibernate (no 2nd ckpt), wake -> all 60',
    async () => {
      // Disable checkpoint-on-hibernate so the wake genuinely replays the
      // post-checkpoint tail (the 10 extra rows).
      const ctx = await setup({ checkpointOnHibernateBytes: -1 })
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t (id serial primary key, v int)`)
        for (let i = 0; i < 50; i++) {
          await s.exec(`insert into t (v) values (${i})`)
        }
        const report = await ctx.host.checkpointDatabase('appdb')
        expect(report.skipped).toBe(false)

        // Ten more commits AFTER the checkpoint — these live only in the tail.
        for (let i = 50; i < 60; i++) {
          await s.exec(`insert into t (v) values (${i})`)
        }
        await ctx.host.hibernateDatabase('appdb') // no checkpoint (disabled)

        const woken = await wakeOracle<{ c: string }>(
          ctx,
          `select count(*)::text as c from t`,
        )
        expect(woken.rows[0].c).toBe('60')
        // Hydration used the checkpoint: only the 10 post-checkpoint commits
        // replay (plus at most the detach sync slice). Never the first 50.
        expect(woken.replayedSlices).toBeGreaterThanOrEqual(10)
        expect(woken.replayedSlices).toBeLessThan(50)

        // And the live host wakes with the data intact too.
        const s2 = await ctx.host.connect('appdb')
        const r = await s2.exec(`select count(*)::int as c from t`)
        expect(r.rows[0].c).toBe(60)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '(c) idempotency: a second immediate checkpoint with no intervening writes is skipped',
    async () => {
      const ctx = await setup({ checkpointOnHibernateBytes: -1 })
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t (id serial primary key, v text)`)
        await s.exec(`insert into t (v) values ('x')`)

        const first = await ctx.host.checkpointDatabase('appdb')
        expect(first.skipped).toBe(false)

        const second = await ctx.host.checkpointDatabase('appdb')
        expect(second.skipped).toBe(true)
        // Same position; no new object minted.
        expect(second.lsn).toBe(first.lsn)
        expect(second.snapEnd).toBe(first.snapEnd)
        expect(second.checkpointRef).toBe(first.checkpointRef)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '(d) checkpoint-on-hibernate default: wake replays ZERO W slices past the checkpoint',
    async () => {
      const ctx = await setup() // default checkpointOnHibernateBytes = 0
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t (id serial primary key, v int)`)
        for (let i = 0; i < 20; i++) {
          await s.exec(`insert into t (v) values (${i})`)
        }
        // Hibernate checkpoints first (default = always), so the tail past
        // the final checkpoint is empty at wake.
        await ctx.host.hibernateDatabase('appdb')

        const woken = await wakeOracle<{ c: string }>(
          ctx,
          `select count(*)::text as c from t`,
        )
        expect(woken.rows[0].c).toBe('20')
        expect(woken.replayedSlices).toBe(0)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '(e) a commit racing the K frame: land a commit between sync publish and K append -> checkpoint completes, joiner gets both',
    async () => {
      const ctx = await setup({ checkpointOnHibernateBytes: -1 })
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t (id serial primary key, v text)`)
        const base = await s.exec(`insert into t (v) values ('base')`)
        expect(base.outcome).toBe('committed')

        // Deterministically land a foreign commit BETWEEN the sync publish
        // (inside ensureAtHeadCanonical) and the K append: wrap the runtime
        // committer's appendControl so the FIRST K-frame append first commits
        // 'racer' through the same committer (stealing the CAS position),
        // then proceeds. That first append loses its CAS (position moved) and
        // the checkpoint's bounded retry loop re-advances and re-appends. The
        // racing commit's baseLsn >= snapEnd, so a joiner replays it on top.
        const runtime = ctx.host.runtimeFor(ctx.dbId)!
        const committer = runtime.committer
        const realAppend = committer.appendControl.bind(committer)
        let injected = false
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ;(committer as any).appendControl = async (
          build: (o: string) => unknown[],
        ) => {
          if (!injected) {
            injected = true
            // Land the racing commit through the normal session path, before
            // the real K append observes the tail head.
            await s.exec(`insert into t (v) values ('racer')`)
          }
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          return realAppend(build as any)
        }

        const report = await ctx.host.checkpointDatabase('appdb')
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ;(committer as any).appendControl = realAppend
        expect(injected).toBe(true)
        expect(report.skipped).toBe(false)

        // Joiner (fresh wake) sees BOTH rows: the checkpoint captured 'base'
        // (and possibly 'racer'), the tail supplies whatever the checkpoint
        // did not.
        const woken = await wakeOracle<{ v: string }>(
          ctx,
          `select v from t order by id`,
        )
        const vs = woken.rows.map((r) => r.v)
        expect(vs).toContain('base')
        expect(vs).toContain('racer')
        expect(vs.length).toBe(2)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
