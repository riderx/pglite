// M7 W3 §16 laziness byte-count suite: lazy-worker cells over a v3
// checkpoint must move O(eager-set) bytes on cold start, fault exactly the
// chunks a query touches, fault NOTHING on a repeat, scan ~size/256KiB on a
// full pass, survive tiny-cache eviction with correct results, and commit
// writes that converge across hosts.

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { GatewayCore } from '@electric-sql/pglite-gateway'
import { LAZY_CHUNK_BYTES } from '@electric-sql/pglite-cell'
import { CellHost } from '../src/host'
import type { RuntimeOpts } from '../src/database-runtime'

const TEST_TIMEOUT = 240_000

// ~50 MB heap: 50k rows x ~1 KB padding.
const SEED_ROWS = 50_000
const SEED_BATCH = 5_000

interface Ctx {
  root: string
  core: GatewayCore
  dbId: string
  hosts: CellHost[]
  newHost: (id: string, opts?: RuntimeOpts) => CellHost
  teardown: () => Promise<void>
}

async function setupSeeded(): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-lazy-'))
  // v3 checkpoints from day one (M7 fixed decision 3).
  const core = new GatewayCore({
    dataRoot: join(root, 'gw'),
    checkpointFormat: 3,
  })
  await core.start()
  const manifest = await core.createDatabase('lazydb')
  const hosts: CellHost[] = []
  const newHost = (id: string, opts: RuntimeOpts = {}): CellHost => {
    const host = new CellHost({
      gateway: core,
      dataRoot: join(root, `host-${id}`),
      hostId: id,
      opts: { cellMode: 'lazy-worker', ...opts },
    })
    hosts.push(host)
    return host
  }

  // Seed a ~50 MB table through a lazy host, then checkpoint (v3) so the
  // measured cold start attaches the big table lazily.
  const seeder = newHost('seed')
  const s = await seeder.connect('lazydb')
  await s.exec(`create table t (id int primary key, pad text)`)
  for (let base = 0; base < SEED_ROWS; base += SEED_BATCH) {
    const r = await s.exec(
      `insert into t select g, repeat('x', 1000) ` +
        `from generate_series(${base + 1}, ${base + SEED_BATCH}) g`,
    )
    expect(r.outcome).toBe('committed')
  }
  await s.close()
  const report = await seeder.checkpointDatabase('lazydb')
  expect(report.skipped).toBe(false)
  await seeder.shutdown()

  return {
    root,
    core,
    dbId: manifest.databaseId,
    hosts,
    newHost,
    teardown: async () => {
      for (const h of hosts) await h.shutdown().catch(() => undefined)
      await core.stop()
      rmSync(root, { recursive: true, force: true })
    },
  }
}

describe('§16 laziness byte counts (lazy-worker mode, v3 checkpoints)', () => {
  it(
    'cold start is O(eager-set); point queries fault their chunks; repeats fault nothing; a scan faults ~size/256KiB',
    async () => {
      const ctx = await setupSeeded()
      try {
        const host = ctx.newHost('cold')
        const session = await host.connect('lazydb')

        // ---- cold start ----
        const r0 = await session.exec(`select 1 as one`)
        expect(r0.rows).toEqual([{ one: 1 }])
        const s0 = (await session.lazyStats())!
        expect(s0).not.toBeNull()
        const cache0 = host.chunkCache!.stats()
        const tableBytes = SEED_ROWS * 1000
        const coldBytes = s0.bytesFaulted
        console.log(
          `[lazy §16] COLD START: ${coldBytes} bytes faulted ` +
            `(${s0.chunkFaults} chunks; cache fetched ${cache0.fetchedBytes} B) ` +
            `vs ~${tableBytes} B table — ` +
            `${((100 * coldBytes) / tableBytes).toFixed(1)}% of table size`,
        )
        // O(eager-set): far below the table size (catalog chunks only).
        expect(coldBytes).toBeLessThan(tableBytes / 4)

        // ---- point query: exactly its index+heap chunks (small bound) ----
        const p1 = await session.exec(`select id from t where id = 1`)
        expect(p1.rows).toEqual([{ id: 1 }])
        const s1 = (await session.lazyStats())!
        const pointFaults = s1.chunkFaults - s0.chunkFaults
        console.log(`[lazy §16] point query faulted ${pointFaults} chunks`)
        expect(pointFaults).toBeGreaterThan(0)
        expect(pointFaults).toBeLessThanOrEqual(16)

        // ---- identical repeat: faults NOTHING ----
        const p2 = await session.exec(`select id from t where id = 1`)
        expect(p2.rows).toEqual([{ id: 1 }])
        const s2 = (await session.lazyStats())!
        expect(s2.chunkFaults - s1.chunkFaults).toBe(0)

        // ---- full-table scan: ~tablesize/256KiB chunks ----
        const c = await session.exec(`select count(*)::int as n from t`)
        expect(c.rows).toEqual([{ n: SEED_ROWS }])
        const s3 = (await session.lazyStats())!
        const scanFaults = s3.chunkFaults - s2.chunkFaults
        const expectChunks = Math.floor(tableBytes / LAZY_CHUNK_BYTES)
        console.log(
          `[lazy §16] full scan faulted ${scanFaults} chunks ` +
            `(~${expectChunks} expected for the heap)`,
        )
        expect(scanFaults).toBeGreaterThanOrEqual(
          Math.floor(expectChunks * 0.8),
        )
        // Bounded above: the heap + its index + slack, never a re-fetch storm.
        expect(scanFaults).toBeLessThanOrEqual(Math.ceil(expectChunks * 1.5))

        // ---- second scan: the overlay serves everything ----
        await session.exec(`select count(*) from t`)
        const s4 = (await session.lazyStats())!
        expect(s4.chunkFaults - s3.chunkFaults).toBe(0)

        await session.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'eviction under a tiny host-cache cap recovers with correct results',
    async () => {
      const ctx = await setupSeeded()
      try {
        // 2 MiB cap: a 50 MB scan MUST evict along the way.
        const host = ctx.newHost('tiny', { chunkCacheBytes: 2 * 1024 * 1024 })
        const session = await host.connect('lazydb')
        const c = await session.exec(`select count(*)::int as n from t`)
        expect(c.rows).toEqual([{ n: SEED_ROWS }])
        const stats = host.chunkCache!.stats()
        console.log(
          `[lazy §16] tiny cache: evictions=${stats.evictions} ` +
            `resident=${stats.residentBytes}`,
        )
        expect(stats.evictions).toBeGreaterThan(0)
        expect(stats.residentBytes).toBeLessThanOrEqual(2 * 1024 * 1024)
        // Correctness after eviction: a fresh point read still answers.
        const p = await session.exec(`select id from t where id = ${SEED_ROWS}`)
        expect(p.rows).toEqual([{ id: SEED_ROWS }])
        await session.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'a write txn commits in lazy mode and converges across a fresh host',
    async () => {
      const ctx = await setupSeeded()
      try {
        const hostA = ctx.newHost('wa')
        const a = await hostA.connect('lazydb')
        const w = await a.exec(
          `insert into t values (${SEED_ROWS + 1}, 'lazy-write') returning id`,
        )
        expect(w.outcome).toBe('committed')
        expect(w.rows).toEqual([{ id: SEED_ROWS + 1 }])

        // Read-your-writes on the same host.
        const rb = await a.exec(`select pad from t where id = ${SEED_ROWS + 1}`)
        expect(rb.rows).toEqual([{ pad: 'lazy-write' }])
        await a.close()

        // Convergence oracle: a COLD lazy host sees the committed row at
        // the linearizable head.
        const hostB = ctx.newHost('wb')
        const b = await hostB.connect('lazydb')
        b.setFreshness({ mode: 'linearizable' })
        const rc = await b.exec(`select pad from t where id = ${SEED_ROWS + 1}`)
        expect(rc.rows).toEqual([{ pad: 'lazy-write' }])
        await b.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
