// M2c exit tests: the era rotator (§6.1 steps 0–6) and its chaos suite —
// happy rotation, 3 eras + GC, a commit racing the seal (re-cut), two
// concurrent rotators (adopt-on-closed), a crash between the seal and the
// manifest (repair-walk), mid-commit rotation at the proxy level, fork E2E
// with pinned-parent GC, and scale-to-zero across a rotation.
//
// Each test builds its own GatewayCore + CellHost(s) (MANY PGlite boots —
// generous timeouts; the suite is serialized in vitest.config.ts).

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
import type { Committer, SealEraResult, Frame } from '@electric-sql/pglite-cell'
import { GatewayCore, extractDatadir } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'
import { CellProxyServer } from '../src/proxy/server'

const TEST_TIMEOUT = 420_000

interface Ctx {
  root: string
  core: GatewayCore
  host: CellHost
  dbId: string
  teardown: () => Promise<void>
}

let oracleN = 0

async function setup(): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-rot-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host'),
    hostId: 'h1',
  })
  const extraTeardowns: (() => Promise<void>)[] = []
  const ctx: Ctx & { onTeardown: (fn: () => Promise<void>) => void } = {
    root,
    core,
    host,
    dbId: manifest.databaseId,
    onTeardown: (fn) => extraTeardowns.push(fn),
    teardown: async () => {
      for (const fn of extraTeardowns) await fn().catch(() => undefined)
      await host.shutdown().catch(() => undefined)
      await core.stop()
      rmSync(root, { recursive: true, force: true })
    },
  }
  return ctx
}

/** A second CellHost over the same gateway (concurrent-rotator tests). */
function secondHost(ctx: Ctx, name: string): CellHost {
  return new CellHost({
    gateway: ctx.core,
    dataRoot: join(ctx.root, name),
    hostId: name,
  })
}

/**
 * Convergence oracle: a never-before-seen materialize from the CURRENT
 * manifest (latest checkpoint + current era tail, hopping any seals),
 * queried directly. Era-aware — refetches the manifest every call.
 */
async function oracle<T>(ctx: Ctx, sql: string, dbId?: string): Promise<T[]> {
  const id = dbId ?? ctx.dbId
  const m = await ctx.core.getManifest(id)
  const dir = join(ctx.root, `oracle-${++oracleN}`)
  await extractDatadir(await ctx.core.getObject(m.checkpoint.ref), dir)
  const tailer = new EraTailer(ctx.core.streamClientFor(id), {
    path: m.era.path,
    eraId: m.era.id,
    ordinal: m.era.ordinal,
    baseOffset: m.era.baseOffset,
    baseLsn: parseLsn(m.era.baseLsn),
  })
  await tailer.catchUp()
  const mat = await materializeAtHead({
    baseDir: dir,
    slices: tailer.slicesSince(parseLsn(m.checkpoint.snapEnd)),
  })
  const cell = await Cell.open(dir, { expectedHeadLsn: mat.headLsn })
  const rows = (await cell.db.query<T>(sql)).rows
  await cell.db.close()
  return rows
}

/** Patch a committer's sealEra with a wrapper (chaos injection). */
function wrapSealEra(
  committer: Committer,
  wrap: (
    orig: (
      build: (expectedOffset: string) => Frame[],
      opts?: { ifHeadOffset?: string },
    ) => Promise<SealEraResult>,
    build: (expectedOffset: string) => Frame[],
    opts?: { ifHeadOffset?: string },
  ) => Promise<SealEraResult>,
): () => void {
  const orig = committer.sealEra.bind(committer)
  ;(committer as unknown as Record<string, unknown>).sealEra = (
    build: (expectedOffset: string) => Frame[],
    opts?: { ifHeadOffset?: string },
  ) => wrap(orig, build, opts)
  return () => {
    delete (committer as unknown as Record<string, unknown>).sealEra
  }
}

describe('era rotation (M2c exit)', () => {
  it(
    '1. happy rotation: era 2 live, era 1 sealed with a valid O/S mirror; writers hop without reconnect; a fresh joiner attaches via checkpoint + era-2 tail only',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t (id serial primary key, v text)`)
        expect((await s.exec(`insert into t (v) values ('one')`)).outcome).toBe(
          'committed',
        )

        const report = await ctx.host.rotateDatabase('appdb')
        expect(report.fromOrdinal).toBe(1)
        expect(report.toOrdinal).toBe(2)
        expect(report.adopted).toBe(false)
        expect(report.repaired).toBe(false)
        expect(report.reCuts).toBe(0)

        // Control plane: current era advanced, era 1 sealed, era 2 row
        // mirrors the seal, the attempt was promoted (no orphans).
        const db = await ctx.core.catalog.getDatabaseById(ctx.dbId)
        expect(db?.currentEraOrdinal).toBe(2)
        const era1 = await ctx.core.catalog.eraByOrdinal(ctx.dbId, 1)
        const era2 = await ctx.core.catalog.eraByOrdinal(ctx.dbId, 2)
        expect(era1?.sealed).toBe(true)
        expect(era1?.nextEraOrdinal).toBe(2)
        expect(era2?.eraId).toBe(report.eraId)
        expect(parseLsn(era2!.baseLsn)).toBe(parseLsn(era1!.sealedFinalLsn!))
        expect(await ctx.core.catalog.listOrphanAttempts(0)).toEqual([])

        // Stream: a fresh tailer over era 1 hops the S/O chain into era 2
        // (the mirror is verified inside the hop).
        const m0 = await ctx.core.getManifest(ctx.dbId)
        const hopTailer = new EraTailer(ctx.core.streamClientFor(ctx.dbId), {
          path: era1!.path,
          eraId: era1!.eraId,
          ordinal: 1,
          baseOffset: era1!.baseOffset,
          baseLsn: parseLsn(era1!.baseLsn),
        })
        await hopTailer.catchUp()
        expect(hopTailer.currentEra.ordinal).toBe(2)
        expect(hopTailer.currentEra.id).toBe(era2!.eraId)
        expect(m0.era.id).toBe(era2!.eraId)

        // The writer continues WITHOUT a reconnect — its committer hopped.
        expect((await s.exec(`insert into t (v) values ('two')`)).outcome).toBe(
          'committed',
        )
        const runtime = ctx.host.runtimeFor(ctx.dbId)!
        expect(runtime.tailer.currentEra.ordinal).toBe(2)

        // A FRESH joiner (new host) attaches via checkpoint + era-2 tail
        // only: its tailer starts (and stays) in era 2.
        const host2 = secondHost(ctx, 'h-joiner')
        const j = await host2.connect('appdb')
        const rj = await j.exec(`select v from t order by id`)
        expect(rj.rows).toEqual([{ v: 'one' }, { v: 'two' }])
        const rt2 = host2.runtimeFor(ctx.dbId)!
        expect(rt2.tailer.currentEra.ordinal).toBe(2)
        expect(rt2.tailer.currentEra.path).toBe(era2!.path)
        await host2.shutdown()

        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from t order by id`,
        )
        expect(rows).toEqual([{ v: 'one' }, { v: 'two' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '2. three eras + GC: covered sealed-era streams and stale checkpoints deleted, current era kept, orphan attempt swept; pins mirror to the control plane',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t (id serial primary key, v text)`)
        await s.exec(`insert into t (v) values ('e1')`)
        expect((await ctx.host.rotateDatabase('appdb')).toOrdinal).toBe(2)
        await s.exec(`insert into t (v) values ('e2')`)
        expect((await ctx.host.rotateDatabase('appdb')).toOrdinal).toBe(3)
        await s.exec(`insert into t (v) values ('e3')`)

        // Orphan attempt: fail a rotation mid-way (crash injected between
        // the era-4 PUT and the seal) — the attempt row + stream orphan.
        const runtime = ctx.host.runtimeFor(ctx.dbId)!
        const restore = wrapSealEra(runtime.committer, async () => {
          throw new Error('injected crash before seal')
        })
        await expect(ctx.host.rotateDatabase('appdb')).rejects.toThrow(
          'injected crash before seal',
        )
        restore()
        const orphansBefore = await ctx.core.catalog.listOrphanAttempts(0)
        expect(orphansBefore.length).toBe(1)
        expect(orphansBefore[0].ordinal).toBe(4)

        // The re-run rotates cleanly (fresh unique URL).
        const r3 = await ctx.host.rotateDatabase('appdb')
        expect(r3.toOrdinal).toBe(4)
        expect(r3.eraId).not.toBe(orphansBefore[0].eraId)
        await s.exec(`insert into t (v) values ('e4')`)

        // Pins glue: a tainted session mirrors a gc-pin row.
        const tainted = await ctx.host.connect('appdb')
        await tainted.exec(`create temp table tt (x int)`)
        expect(tainted.tainted).toBe(true)
        const pins = await ctx.core.catalog.livePins(ctx.dbId)
        expect(pins.some((p) => p.holder === tainted.id)).toBe(true)
        // Hibernate releases the host's pins (and resets the tainted
        // session); sessions were mid-nothing so this is clean.
        await ctx.host.hibernateDatabase('appdb')
        expect(await ctx.core.catalog.livePins(ctx.dbId)).toEqual([])

        // GC with zero grace: eras 1–3 (sealed + checkpoint-covered) and
        // the orphan stream go; era 4 stays; checkpoints prune to latest.
        await ctx.core.setDials(ctx.dbId, { gcGraceMs: 0 })
        const report = await ctx.core.runGc(ctx.dbId)
        expect(report.deletedStreams).toBeGreaterThanOrEqual(4) // eras 1-3 + orphan
        expect(await ctx.core.catalog.listOrphanAttempts(0)).toEqual([])
        const client = ctx.core.streamClientFor(ctx.dbId)
        const era1 = await ctx.core.catalog.eraByOrdinal(ctx.dbId, 1)
        const era4 = await ctx.core.catalog.eraByOrdinal(ctx.dbId, 4)
        await expect(client.head(era1!.path)).rejects.toThrow()
        await expect(client.head(era4!.path)).resolves.toBeDefined()
        const ckpts = await ctx.core.catalog.checkpointsOf(ctx.dbId)
        expect(ckpts.length).toBe(1)

        // A joiner (wake) still attaches from the latest checkpoint + the
        // current era only — the deleted eras are never needed.
        const w = await ctx.host.connect('appdb')
        const rw = await w.exec(`select v from t order by id`)
        expect(rw.rows).toEqual([
          { v: 'e1' },
          { v: 'e2' },
          { v: 'e3' },
          { v: 'e4' },
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '3. a commit races the seal: the rotator re-cuts (orphaning the stale attempt); the raced commit is a legitimate era-N commit and the next era bases on it',
    async () => {
      const ctx = await setup()
      try {
        const sa = await ctx.host.connect('appdb')
        await sa.exec(`create table t (id serial primary key, v text)`)
        await sa.exec(`insert into t (v) values ('pre')`)

        // A sibling HOST whose commit will land between the era-2 PUT and
        // the seal (a genuine server-side 409-seq on the seal).
        const hostB = secondHost(ctx, 'h2')
        const sb = await hostB.connect('appdb')
        await sb.exec(`insert into t (v) values ('b-pre')`)

        const runtime = ctx.host.runtimeFor(ctx.dbId)!
        let injected = false
        const restore = wrapSealEra(
          runtime.committer,
          async (orig, build, opts) => {
            if (!injected) {
              injected = true
              // The raced commit: lands on era 1 AFTER the rotator cut its
              // era-2 attempt, BEFORE the seal posts.
              const r = await sb.exec(`insert into t (v) values ('raced')`)
              expect(r.outcome).toBe('committed')
            }
            return orig(build, opts)
          },
        )

        const report = await ctx.host.rotateDatabase('appdb')
        restore()
        expect(report.toOrdinal).toBe(2)
        expect(report.adopted).toBe(false)
        expect(report.reCuts).toBeGreaterThanOrEqual(1)

        // The orphaned attempt row from the re-cut exists (unpromoted).
        const orphans = await ctx.core.catalog.listOrphanAttempts(0)
        expect(orphans.length).toBeGreaterThanOrEqual(1)
        expect(orphans.every((o) => o.ordinal === 2)).toBe(true)
        expect(orphans.every((o) => o.eraId !== report.eraId)).toBe(true)

        // The raced commit sits in era 1 and era 2 bases exactly on it:
        // era1.finalLsn == era2.baseLsn, and the raced row is visible.
        const era1 = await ctx.core.catalog.eraByOrdinal(ctx.dbId, 1)
        const era2 = await ctx.core.catalog.eraByOrdinal(ctx.dbId, 2)
        expect(parseLsn(era1!.sealedFinalLsn!)).toBe(parseLsn(era2!.baseLsn))

        // Both hosts keep writing (their committers hop), and everything
        // converges exactly once.
        expect(
          (await sa.exec(`insert into t (v) values ('post-a')`)).outcome,
        ).toBe('committed')
        expect(
          (await sb.exec(`insert into t (v) values ('post-b')`)).outcome,
        ).toBe('committed')
        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from t order by id`,
        )
        expect(rows).toEqual([
          { v: 'pre' },
          { v: 'b-pre' },
          { v: 'raced' },
          { v: 'post-a' },
          { v: 'post-b' },
        ])
        await hostB.shutdown()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '4. two concurrent rotators: exactly one next era wins, the loser adopts, exactly one current-era advance, no wedge; the orphan sweeps later',
    async () => {
      const ctx = await setup()
      try {
        const sa = await ctx.host.connect('appdb')
        await sa.exec(`create table t (id serial primary key, v text)`)
        await sa.exec(`insert into t (v) values ('pre')`)

        const hostB = secondHost(ctx, 'h2')
        const sb = await hostB.connect('appdb')
        await sb.exec(`insert into t (v) values ('b-pre')`)

        const rtA = ctx.host.runtimeFor(ctx.dbId)!
        const rtB = hostB.runtimeFor(ctx.dbId)!

        // Barrier: both rotators must CUT their era-2 attempts before
        // EITHER seals, so they genuinely race at step 5.
        let firstArrived: (() => void) | null = null
        let rendezvousDone = false
        const gate = async (): Promise<void> => {
          if (rendezvousDone) return
          if (firstArrived === null) {
            await Promise.race([
              new Promise<void>((resolve) => {
                firstArrived = resolve
              }),
              new Promise<void>((resolve) => setTimeout(resolve, 20_000)),
            ])
          } else {
            rendezvousDone = true
            firstArrived()
          }
        }
        const restoreA = wrapSealEra(rtA.committer, async (o, b, op) => {
          await gate()
          return o(b, op)
        })
        const restoreB = wrapSealEra(rtB.committer, async (o, b, op) => {
          await gate()
          return o(b, op)
        })

        const [ra, rb] = await Promise.all([
          ctx.host.rotateDatabase(ctx.dbId),
          hostB.rotateDatabase(ctx.dbId),
        ])
        restoreA()
        restoreB()

        // Exactly ONE rotation happened: both finish at ordinal 2, exactly
        // one of them sealed it, the other adopted.
        expect(ra.toOrdinal).toBe(2)
        expect(rb.toOrdinal).toBe(2)
        expect(ra.eraId).toBe(rb.eraId)
        const adoptedCount = [ra, rb].filter((r) => r.adopted).length
        expect(adoptedCount).toBe(1)

        const db = await ctx.core.catalog.getDatabaseById(ctx.dbId)
        expect(db?.currentEraOrdinal).toBe(2)
        const eras = await ctx.core.catalog.erasOf(ctx.dbId)
        expect(eras.map((e) => e.ordinal)).toEqual([1, 2])
        expect(eras[1].eraId).toBe(ra.eraId)

        // Both hosts' tailers hopped; both keep writing without a wedge.
        expect(rtA.tailer.currentEra.ordinal).toBe(2)
        expect(rtB.tailer.currentEra.ordinal).toBe(2)
        expect(
          (await sa.exec(`insert into t (v) values ('post-a')`)).outcome,
        ).toBe('committed')
        expect(
          (await sb.exec(`insert into t (v) values ('post-b')`)).outcome,
        ).toBe('committed')

        // The loser's attempt orphaned; the sweep removes it (and its
        // stream) after grace.
        const orphans = await ctx.core.catalog.listOrphanAttempts(0)
        expect(orphans.length).toBe(1)
        expect(orphans[0].eraId).not.toBe(ra.eraId)
        await ctx.core.setDials(ctx.dbId, { gcGraceMs: 0 })
        await ctx.core.runGc(ctx.dbId)
        expect(await ctx.core.catalog.listOrphanAttempts(0)).toEqual([])

        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from t order by id`,
        )
        expect(rows).toEqual([
          { v: 'pre' },
          { v: 'b-pre' },
          { v: 'post-a' },
          { v: 'post-b' },
        ])
        await hostB.shutdown()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '5. crash between the seal and the manifest: writers hop via the stream S regardless of control-plane lag; re-running rotateDatabase repair-walks the transition to completion',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t (id serial primary key, v text)`)
        await s.exec(`insert into t (v) values ('pre')`)

        // Inject the crash: the seal append lands, then the first
        // control-plane write of step 6 throws.
        const runtime = ctx.host.runtimeFor(ctx.dbId)!
        const gw = runtime.gateway as unknown as Record<string, unknown>
        const origSeal = gw.sealEraRow as (...a: unknown[]) => Promise<void>
        gw.sealEraRow = async () => {
          throw new Error('injected crash between seal and manifest')
        }
        await expect(ctx.host.rotateDatabase('appdb')).rejects.toThrow(
          'injected crash between seal and manifest',
        )
        gw.sealEraRow = origSeal

        // The control plane lags: still era 1, unsealed row.
        const db0 = await ctx.core.catalog.getDatabaseById(ctx.dbId)
        expect(db0?.currentEraOrdinal).toBe(1)
        const era1Before = await ctx.core.catalog.eraByOrdinal(ctx.dbId, 1)
        expect(era1Before?.sealed).toBe(false)
        expect(await ctx.core.catalog.eraByOrdinal(ctx.dbId, 2)).toBeNull()

        // But the STREAM is sealed — the writer's committer hops via the
        // S/O chain and lands in era 2, unaffected by the lag.
        expect(
          (await s.exec(`insert into t (v) values ('during-lag')`)).outcome,
        ).toBe('committed')
        expect(runtime.tailer.currentEra.ordinal).toBe(2)

        // Re-run: step 0's repair-walk completes the transition; no new
        // era is cut.
        const report = await ctx.host.rotateDatabase('appdb')
        expect(report.repaired).toBe(true)
        expect(report.toOrdinal).toBe(2)
        const db1 = await ctx.core.catalog.getDatabaseById(ctx.dbId)
        expect(db1?.currentEraOrdinal).toBe(2)
        const era1 = await ctx.core.catalog.eraByOrdinal(ctx.dbId, 1)
        const era2 = await ctx.core.catalog.eraByOrdinal(ctx.dbId, 2)
        expect(era1?.sealed).toBe(true)
        expect(era2?.eraId).toBe(report.eraId)
        expect(await ctx.core.catalog.listOrphanAttempts(0)).toEqual([])

        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from t order by id`,
        )
        expect(rows).toEqual([{ v: 'pre' }, { v: 'during-lag' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '6. mid-commit rotation at the proxy level: a pg client keeps inserting while the era rotates; every acknowledged statement lands exactly once',
    async () => {
      const ctx = await setup()
      const proxy = new CellProxyServer({ host: ctx.host, port: 0 })
      const port = await proxy.start()
      const client = new Client({
        host: '127.0.0.1',
        port,
        database: 'appdb',
        user: 'postgres',
      })
      client.on('error', () => undefined)
      try {
        await client.connect()
        await client.query(`create table t (i int primary key)`)
        await client.query(`insert into t values (-1)`) // write-attach

        const inserted: number[] = []
        let rotation: Promise<unknown> | null = null
        for (let i = 0; i < 12; i++) {
          if (i === 2) rotation = ctx.host.rotateDatabase('appdb')
          // §3.7: one-shots re-execute transparently; a surfaced 40001 is
          // the documented "ordinary re-execute" path — the client retries.
          for (let attempt = 0; ; attempt++) {
            try {
              await client.query(`insert into t values (${i})`)
              break
            } catch (err) {
              const code = (err as { code?: string }).code
              if (code === '40001' && attempt < 3) continue
              throw err
            }
          }
          inserted.push(i)
        }
        const report = (await rotation) as { toOrdinal: number }
        expect(report.toOrdinal).toBe(2)
        expect(inserted.length).toBe(12)

        // Exactly-once via the oracle: every acknowledged value present
        // exactly once, nothing else.
        const rows = await oracle<{ i: number }>(
          ctx,
          `select i from t order by i`,
        )
        expect(rows.map((r) => r.i)).toEqual([-1, ...inserted])
      } finally {
        await client.end().catch(() => undefined)
        await proxy.stop().catch(() => undefined)
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '7. fork E2E: sessions on parent and child diverge; GC keeps the parent era containing the fork offset while the child lives; both oracles correct',
    async () => {
      const ctx = await setup()
      try {
        const p = await ctx.host.connect('appdb')
        await p.exec(`create table t (id serial primary key, v text)`)
        await p.exec(`insert into t (v) values ('pre-fork')`)
        await ctx.host.checkpointDatabase('appdb')

        const childManifest = await ctx.core.forkDatabase(ctx.dbId, 'childdb')
        const childId = childManifest.databaseId

        // Connect sessions to BOTH; diverging writes.
        const c = await ctx.host.connect('childdb')
        const rc = await c.exec(`select v from t`)
        expect(rc.rows).toEqual([{ v: 'pre-fork' }]) // hydrated from the shared checkpoint
        expect(
          (await c.exec(`insert into t (v) values ('child-1')`)).outcome,
        ).toBe('committed')
        expect(
          (await p.exec(`insert into t (v) values ('parent-1')`)).outcome,
        ).toBe('committed')

        // Rotate the parent so era 1 is sealed and checkpoint-covered —
        // WITHOUT the child it would be GC-deletable; the child's fork
        // point pins it.
        const rot = await ctx.host.rotateDatabase('appdb')
        expect(rot.toOrdinal).toBe(2)
        await ctx.core.setDials(ctx.dbId, { gcGraceMs: 0 })
        const report = await ctx.core.runGc(ctx.dbId)
        expect(report.kept['sealed-era:child-fork']).toBeGreaterThanOrEqual(1)
        const era1 = await ctx.core.catalog.eraByOrdinal(ctx.dbId, 1)
        const client = ctx.core.streamClientFor(ctx.dbId)
        await expect(client.head(era1!.path)).resolves.toBeDefined()

        // Both lineages stay writable and independent.
        expect(
          (await p.exec(`insert into t (v) values ('parent-2')`)).outcome,
        ).toBe('committed')
        expect(
          (await c.exec(`insert into t (v) values ('child-2')`)).outcome,
        ).toBe('committed')

        const parentRows = await oracle<{ v: string }>(
          ctx,
          `select v from t order by id`,
        )
        expect(parentRows).toEqual([
          { v: 'pre-fork' },
          { v: 'parent-1' },
          { v: 'parent-2' },
        ])
        const childRows = await oracle<{ v: string }>(
          ctx,
          `select v from t order by id`,
          childId,
        )
        expect(childRows).toEqual([
          { v: 'pre-fork' },
          { v: 'child-1' },
          { v: 'child-2' },
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '8. scale-to-zero across a rotation: hibernate after rotating, wake — the fresh runtime attaches from the latest checkpoint + the CURRENT era only',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t (id serial primary key, v text)`)
        await s.exec(`insert into t (v) values ('e1')`)
        expect((await ctx.host.rotateDatabase('appdb')).toOrdinal).toBe(2)
        await s.exec(`insert into t (v) values ('e2')`)

        await ctx.host.hibernateDatabase('appdb')
        expect(ctx.host.listActive().length).toBe(0)

        // Wake: the manifest serves era 2 + the (hibernate) checkpoint —
        // the runtime attaches directly into era 2, never touching era 1.
        const w = await ctx.host.connect('appdb')
        const r = await w.exec(`select v from t order by id`)
        expect(r.rows).toEqual([{ v: 'e1' }, { v: 'e2' }])
        const runtime = ctx.host.runtimeFor(ctx.dbId)!
        const era2 = await ctx.core.catalog.eraByOrdinal(ctx.dbId, 2)
        expect(runtime.tailer.currentEra.ordinal).toBe(2)
        expect(runtime.tailer.currentEra.path).toBe(era2!.path)
        // The wake checkpoint sits inside era 2: the tail replayed on wake
        // came from the current era only.
        const m = await ctx.core.getManifest(ctx.dbId)
        expect(parseLsn(m.checkpoint.snapEnd)).toBeGreaterThanOrEqual(
          parseLsn(era2!.baseLsn),
        )

        // And the woken database still rotates (re-entrancy after wake).
        expect((await ctx.host.rotateDatabase('appdb')).toOrdinal).toBe(3)
        expect((await w.exec(`insert into t (v) values ('e3')`)).outcome).toBe(
          'committed',
        )
        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from t order by id`,
        )
        expect(rows).toEqual([{ v: 'e1' }, { v: 'e2' }, { v: 'e3' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
