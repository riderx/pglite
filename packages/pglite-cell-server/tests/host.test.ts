// M1c exit tests: two connections' cells on one host — the sequencer
// serializes, the watermark gate gives read-your-writes, the abort-only
// sequence floor survives recycle/hibernate, hibernate/wake round-trips —
// plus the §3.7 contract rows the host enforces (taint ⇒ reset-first
// survival since M5e, fatal only on the recycle fallback,
// interactive 40001, read-only never CAS'd) and lease-frame visibility.
//
// Each test builds its own GatewayCore + CellHost (many PGlite boots —
// generous timeouts).

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
import { GatewayCore, extractDatadir } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'
import {
  SerializationConflictError,
} from '../src/errors'

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
  const root = mkdtempSync(join(tmpdir(), 'pgl-host-'))
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
      rmSync(root, { recursive: true, force: true })
    },
  }
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

describe('CellHost (M1c exit)', () => {
  it(
    '1. sibling race: A and B both land their inserts exactly once (transparent write-upgrade + re-execute)',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        const created = await s.exec(
          `create table t (id serial primary key, v text)`,
        )
        expect(created.outcome).toBe('committed')

        const a = await ctx.host.connect('appdb')
        const b = await ctx.host.connect('appdb')
        // Prime BOTH with read cells so B's is genuinely stale after A's
        // commit.
        expect((await a.exec(`select count(*) from t`)).outcome).toBe(
          'read-only',
        )
        expect((await b.exec(`select count(*) from t`)).outcome).toBe(
          'read-only',
        )

        const ra = await a.exec(`insert into t (v) values ('from-a')`)
        expect(ra.outcome).toBe('committed')
        expect(a.attachMode).toBe('write')

        // B's read cell is stale; the watermark gate advances it, the
        // write triggers the upgrade, and the re-execution lands.
        const rb = await b.exec(`insert into t (v) values ('from-b')`)
        expect(rb.outcome).toBe('committed')
        expect(b.attachMode).toBe('write')

        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from t order by id`,
        )
        expect(rows).toEqual([{ v: 'from-a' }, { v: 'from-b' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    "2. watermark gate: B reads A's commit immediately, with zero stream appends from B's advance",
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t (id serial primary key, v text)`)

        const a = await ctx.host.connect('appdb')
        const b = await ctx.host.connect('appdb')
        await b.exec(`select 1`) // B attaches (read) at the pre-commit head

        const ra = await a.exec(`insert into t (v) values ('a1')`)
        expect(ra.outcome).toBe('committed')

        const head0 = await streamHead(ctx)
        const rb = await b.exec(`select v from t`)
        expect(rb.outcome).toBe('read-only')
        // Read-your-writes across connections: the gate advanced B first.
        expect(rb.rows).toEqual([{ v: 'a1' }])
        // Read-attach = zero stream appends: the head did not move.
        expect(await streamHead(ctx)).toBe(head0)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '3. sequence floors: an abort-only nextval on a read cell survives hibernate — no duplicate draw',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create sequence seq_f`)
        await s.close()

        // A draws in a rolled-back transaction on a READ-attached cell:
        // the observed value never reaches the stream in any form. Only
        // the host floor map covers it.
        const a = await ctx.host.connect('appdb')
        const r1 = await a.exec(
          `begin; select nextval('seq_f')::text as n; rollback`,
        )
        expect(r1.outcome).toBe('aborted')
        expect(a.attachMode).toBe('read') // aborts never trigger the upgrade
        const observed = Number(r1.rows[0].n)
        expect(observed).toBeGreaterThanOrEqual(1)

        // Recycle everything A ever touched.
        await ctx.host.hibernateDatabase('appdb')

        // A fresh session after wake write-upgrades; floors are applied and
        // published (kind `floors`) before it runs anything.
        const c = await ctx.host.connect('appdb')
        const r2 = await c.exec(`select nextval('seq_f')::text as n`)
        expect(r2.outcome).toBe('committed')
        expect(Number(r2.rows[0].n)).toBeGreaterThan(observed)

        // The floors slice is on the stream.
        const tailer = newTailer(ctx)
        await tailer.catchUp()
        expect(tailer.slices.some((sl) => sl.kind === 'floors')).toBe(true)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '4. taint: temp tables pin the session (gc-pin frame); a lost race SURVIVES via the M5e gate + in-place reset (temp content intact)',
    async () => {
      const ctx = await setup()
      try {
        const a = await ctx.host.connect('appdb')
        await a.exec(`create table t4 (x int)`)
        const r = await a.exec(`create temp table tt (x int)`)
        expect(r.outcome).toBe('committed')
        expect(a.tainted).toBe(true)

        // The gc-pin L frame is on the stream.
        const tailer = newTailer(ctx)
        await tailer.catchUp()
        expect(tailer.leases['gc-pin']?.holder).toBe(a.id)
        expect(tailer.leases['gc-pin']?.base).toBeDefined()

        // B advances the head past pinned A.
        const b = await ctx.host.connect('appdb')
        expect((await b.exec(`insert into t4 values (1)`)).outcome).toBe(
          'committed',
        )

        // M5c: a one-shot publish from the stale pinned base no longer
        // loses when the tail is live-appliable — the write cell advances
        // in place, keeping temp state, and the publish LANDS. The fatal
        // contract still governs losses with no advance window: race the
        // foreign commit inside A's interactive transaction.
        expect((await a.exec(`insert into tt values (1)`)).outcome).toBe(
          'committed',
        )
        expect((await a.exec(`insert into t4 values (2)`)).outcome).toBe(
          'committed',
        )
        expect((await a.exec(`select x from tt`)).rows).toEqual([{ x: 1 }])

        await a.exec(`begin`)
        await a.exec(`insert into t4 values (3)`)
        const c = await ctx.host.connect('appdb')
        expect((await c.exec(`insert into t4 values (4)`)).outcome).toBe(
          'committed',
        )
        // M5e taint lift (§3.3/§3.6): the tainted interactive loss is no
        // longer a fatal reset — the commit gate deferred the only
        // irreversible pre-commit step and the in-place reset restores
        // the pre-attempt temp content, so the session gets the ordinary
        // 40001 and SURVIVES with its temp state intact. (The fatal
        // contract still governs the recycle fallback, i.e. when the
        // reset is unsound.)
        let conflict: unknown
        try {
          await a.exec(`commit`)
        } catch (err) {
          conflict = err
        }
        expect(conflict).toBeInstanceOf(SerializationConflictError)

        // The session is ALIVE and its temp content survived the loss.
        expect((await a.exec(`select x from tt`)).rows).toEqual([{ x: 1 }])

        // Exactly-once: B's, A's landed one-shots and C's row — never the
        // raced interactive insert.
        const rows = await oracle<{ x: number }>(
          ctx,
          `select x from t4 order by x`,
        )
        expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 4 }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '5. interactive transaction: COMMIT after a foreign commit ⇒ 40001; session survives and reads the winner',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t5 (id serial primary key, v text)`)

        const a = await ctx.host.connect('appdb')
        // Make A write-attached first (sticky write intent).
        expect(
          (await a.exec(`insert into t5 (v) values ('a-seed')`)).outcome,
        ).toBe('committed')

        expect((await a.exec(`begin`)).outcome).toBe('in-transaction')
        expect(
          (await a.exec(`insert into t5 (v) values ('a-txn')`)).outcome,
        ).toBe('in-transaction')

        // B lands a commit while A is mid-transaction.
        const b = await ctx.host.connect('appdb')
        expect((await b.exec(`insert into t5 (v) values ('b1')`)).outcome).toBe(
          'committed',
        )

        // A's COMMIT loses its CAS: 40001, rolled back cleanly.
        let err: unknown
        try {
          await a.exec(`commit`)
        } catch (e) {
          err = e
        }
        expect(err).toBeInstanceOf(SerializationConflictError)
        expect((err as SerializationConflictError).code).toBe('40001')

        // The session survives; its next statement sees B's row.
        const after = await a.exec(`select v from t5 order by id`)
        expect(after.outcome).toBe('read-only')
        expect(after.rows).toEqual([{ v: 'a-seed' }, { v: 'b1' }])

        // The rolled-back insert is absent everywhere.
        const rows = await oracle<{ v: string }>(
          ctx,
          `select v from t5 order by id`,
        )
        expect(rows).toEqual([{ v: 'a-seed' }, { v: 'b1' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '6. hibernate → wake: detach slice seals the tail; wake from checkpoint+tail; data intact',
    async () => {
      const ctx = await setup()
      try {
        const a = await ctx.host.connect('appdb')
        await a.exec(`create table t6 (v text)`)
        await a.exec(`insert into t6 values ('kept')`)
        expect(ctx.host.listActive().length).toBe(1)

        await ctx.host.hibernateDatabase('appdb')
        expect(ctx.host.listActive().length).toBe(0)

        // The stream tail ends with the detach sync slice (a shutdown
        // record), so the next attach's materialize replays a sealed tail.
        const tailer = newTailer(ctx)
        await tailer.catchUp()
        const last = tailer.slices[tailer.slices.length - 1]
        expect(last.kind).toBe('sync')
        expect(last.endLsn).toBe(tailer.head.lsn)

        // Wake via a new connect; data intact.
        const t0 = Date.now()
        const b = await ctx.host.connect('appdb')
        const r = await b.exec(`select v from t6`)

        console.log(`[test] wake-to-first-row: ${Date.now() - t0}ms`)
        expect(r.rows).toEqual([{ v: 'kept' }])
        expect(ctx.host.listActive().length).toBe(1)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '7. lease frames: activation appends L{head}, visible to a fresh tailer',
    async () => {
      const ctx = await setup()
      try {
        await ctx.host.connect('appdb')
        const tailer = newTailer(ctx)
        await tailer.catchUp()
        expect(tailer.leases['head']?.holder).toBe('h1')
        expect(tailer.leases['head']?.ttlMs).toBe(30000)
        expect(typeof tailer.leases['head']?.epoch).toBe('number')
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '8. read-only sessions never CAS: N selects leave the stream head untouched',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t8 (v text)`)
        await s.exec(`insert into t8 values ('r')`)

        const b = await ctx.host.connect('appdb')
        const head0 = await streamHead(ctx)
        for (let i = 0; i < 5; i++) {
          const r = await b.exec(`select v from t8`)
          expect(r.outcome).toBe('read-only')
          expect(r.rows).toEqual([{ v: 'r' }])
          expect(await streamHead(ctx)).toBe(head0)
        }
        expect(b.attachMode).toBe('read')
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '9. interactive first-write on a read-attached cell: 40001 at COMMIT even uncontended; the retry lands upgraded',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        await s.exec(`create table t9 (v text)`)

        const a = await ctx.host.connect('appdb')
        expect((await a.exec(`begin`)).outcome).toBe('in-transaction')
        expect((await a.exec(`insert into t9 values ('x')`)).outcome).toBe(
          'in-transaction',
        )
        // The txn's slice exists only on a read-attached (diverged) cell:
        // publishing is forbidden, so COMMIT surfaces the documented 40001.
        let err: unknown
        try {
          await a.exec(`commit`)
        } catch (e) {
          err = e
        }
        expect(err).toBeInstanceOf(SerializationConflictError)

        // The client-style retry runs on the upgraded (write-attached) cell.
        expect(a.attachMode).toBe('write')
        await a.exec(`begin`)
        await a.exec(`insert into t9 values ('x')`)
        expect((await a.exec(`commit`)).outcome).toBe('committed')

        const rows = await oracle<{ v: string }>(ctx, `select v from t9`)
        expect(rows).toEqual([{ v: 'x' }])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
