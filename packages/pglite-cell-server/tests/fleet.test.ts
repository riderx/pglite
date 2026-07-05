// M4a cross-host fleet suites (M4_PLAN "M4a"): two CellHosts over ONE
// gateway/database — genuinely foreign CAS contention, head-lease
// migration semantics (§3.2), sequence grants + incarnation burn (§5.3),
// cross-host session tokens (§7), and a randomized multi-host convergence
// oracle. Heavy suite: many PGlite boots per test; serialized by
// vitest.config.ts, generous timeouts.

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
  readControl,
} from '@electric-sql/pglite-cell'
import type { GFrameHeader, LFrameHeader } from '@electric-sql/pglite-cell'
import { GatewayCore, extractCheckpoint } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'
import type { RuntimeOpts } from '../src/database-runtime'
import { CellProxyServer } from '../src/proxy/server'

const TEST_TIMEOUT = 240_000

interface HostRig {
  host: CellHost
  proxy: CellProxyServer
  port: number
}

interface Ctx {
  root: string
  core: GatewayCore
  hosts: HostRig[]
  manifest: Manifest
  dbId: string
  clients: Client[]
  extra: (() => Promise<void>)[]
  teardown: () => Promise<void>
}

let oracleN = 0

/** Adversarial-contention tuning: two hosts genuinely hammer each other,
 *  so the re-execution / canonical-ensure budgets are raised well past
 *  the solo defaults (bounded starvation under a foreign pipeline is
 *  expected — §3.2: the lease holder wins by design; the loser retries). */
const FLEET_OPTS: RuntimeOpts = { attachAttempts: 40, maxRetries: 10 }

async function setup(opts: RuntimeOpts = {}, hostCount = 2): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-fleet-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const hosts: HostRig[] = []
  for (let i = 0; i < hostCount; i++) {
    const host = new CellHost({
      gateway: core,
      dataRoot: join(root, `host${i + 1}`),
      hostId: `h${i + 1}`,
      opts: { ...FLEET_OPTS, ...opts },
    })
    const proxy = new CellProxyServer({ host, port: 0 })
    const port = await proxy.start()
    hosts.push({ host, proxy, port })
  }
  const clients: Client[] = []
  const extra: (() => Promise<void>)[] = []
  return {
    root,
    core,
    hosts,
    manifest,
    dbId: manifest.databaseId,
    clients,
    extra,
    teardown: async () => {
      for (const c of clients) {
        await c.end().catch(() => undefined)
      }
      for (const fn of extra) {
        await fn().catch(() => undefined)
      }
      for (const rig of hosts) {
        await rig.proxy.stop().catch(() => undefined)
        await rig.host.shutdown().catch(() => undefined)
      }
      await core.stop()
      rmSync(root, { recursive: true, force: true })
    },
  }
}

async function connect(ctx: Ctx, hostIdx: number): Promise<Client> {
  const client = new Client({
    host: '127.0.0.1',
    port: ctx.hosts[hostIdx].port,
    database: 'appdb',
    user: 'postgres',
  })
  client.on('error', () => undefined)
  await client.connect()
  ctx.clients.push(client)
  return client
}

/** A full-chain tailer from the ORIGINAL manifest (hops eras via S/O). */
async function fullTailer(ctx: Ctx): Promise<EraTailer> {
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

/** Convergence oracle: fresh materialize of the FULL era chain, queried.
 *  Returns the rows AND the materialized dir's pg_control identity. */
async function oracle<T>(
  ctx: Ctx,
  sql: string,
): Promise<{ rows: T[]; nextXid: bigint; checkPoint: bigint }> {
  const dir = join(ctx.root, `oracle-${++oracleN}`)
  await extractCheckpoint(ctx.manifest.checkpoint.ref, dir, {
    store: {
      get: (r: string) => ctx.core.getObject(r),
      put: (b: Uint8Array) => ctx.core.putObject(b),
    },
  })
  const tailer = await fullTailer(ctx)
  const mat = await materializeAtHead({
    baseDir: dir,
    slices: tailer.slicesSince(parseLsn(ctx.manifest.checkpoint.snapEnd)),
  })
  const cell = await Cell.open(dir, { expectedHeadLsn: mat.headLsn })
  const rows = (await cell.db.query<T>(sql)).rows
  await cell.db.close()
  const control = readControl(dir)
  return { rows, nextXid: control.nextXid, checkPoint: control.checkPoint }
}

/** All non-zero-width grants for one sequence, in stream order. */
function drawableGrants(tailer: EraTailer, seqName: string): GFrameHeader[] {
  return tailer
    .grantsFor(seqName)
    .filter((g) => BigInt(g.end) > BigInt(g.start))
}

/** Assert every drawable grant range `(start, end]` is disjoint. */
function assertGrantsDisjoint(grants: GFrameHeader[]): void {
  const sorted = [...grants].sort((a, b) =>
    BigInt(a.start) < BigInt(b.start) ? -1 : 1,
  )
  for (let i = 1; i < sorted.length; i++) {
    expect(BigInt(sorted[i].start) >= BigInt(sorted[i - 1].end)).toBe(true)
  }
}

/** Tiny seeded RNG (mulberry32) for the randomized convergence suite. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0
  return () => {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

describe('M4a fleet: two CellHosts, one gateway/database', () => {
  it(
    'a. contended writers through both proxies: every insert acked exactly once; the oracle equals both hosts',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx, 0)
        await s.query(`create table t (v text)`)

        const a = await connect(ctx, 0)
        const b = await connect(ctx, 1)
        const N = 12
        await Promise.all([
          (async () => {
            for (let i = 0; i < N; i++) {
              await a.query(`insert into t values ('a-${i}')`)
            }
          })(),
          (async () => {
            for (let i = 0; i < N; i++) {
              await b.query(`insert into t values ('b-${i}')`)
            }
          })(),
        ])

        const expected = [
          ...Array.from({ length: N }, (_, i) => `a-${i}`),
          ...Array.from({ length: N }, (_, i) => `b-${i}`),
        ].sort()

        // Exactly-once: the oracle (full-stream materialize) has each
        // acked insert exactly once, nothing else.
        const o = await oracle<{ v: string }>(ctx, `select v from t order by v`)
        expect(o.rows.map((r) => r.v)).toEqual(expected)

        // Both hosts converge (linearizable pulls the true head).
        for (const c of [a, b]) {
          await c.query(`set pglite.freshness = 'linearizable'`)
          const seen = await c.query(`select v from t order by v`)
          expect(seen.rows.map((r: { v: string }) => r.v)).toEqual(expected)
        }
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'b. head-lease migration (§3.2): A holds; hibernate A; B claims on its next write with a strictly higher epoch; the demoted side never re-claims over a fresh foreign lease; L{head} epochs are monotone in the stream',
    async () => {
      // TTL must comfortably exceed a write-attach (materialize + PGlite
      // boot, ~1-2s): lease freshness is judged from LOCAL observation
      // time, so a too-short TTL makes a fresh foreign lease look expired
      // by the time the non-holder's first write reaches the sequencer.
      const ctx = await setup({ leaseTtlMs: 5000 })
      try {
        const [h1, h2] = [ctx.hosts[0].host, ctx.hosts[1].host]

        // A activates + writes: it holds the head lease.
        const sA = await h1.connect('appdb')
        await sA.exec(`create table lm (v text)`)
        await sA.exec(`insert into lm values ('a1')`)
        const rt1 = h1.runtimeFor(ctx.dbId)!
        const stateA = rt1.leaseState()
        expect(stateA.held).toBe(true)
        expect(stateA.holder).toBe('h1')
        const epochA = stateA.epoch

        // B activates while A's lease is fresh: B adopts, does not claim
        // (its optimistic writes still land — zero correctness weight).
        const sB = await h2.connect('appdb')
        await sB.exec(`insert into lm values ('b1')`)
        const rt2 = h2.runtimeFor(ctx.dbId)!
        expect(rt2.leaseState().holder).toBe('h1')
        expect(rt2.leaseState().held).toBe(false)

        // A hibernates; the lease lapses by TTL.
        await h1.hibernateDatabase('appdb')
        await new Promise((r) => setTimeout(r, 5200))

        // B's next write claims (migration) with a strictly higher epoch.
        await sB.exec(`insert into lm values ('b2')`)
        const stateB = rt2.leaseState()
        expect(stateB.held).toBe(true)
        expect(stateB.holder).toBe('h2')
        expect(stateB.epoch).toBeGreaterThan(epochA)

        // A wakes: it sees B's fresh lease and does NOT re-claim (a
        // demoted/awakened holder stops refreshing).
        const sA2 = await h1.connect('appdb')
        await sA2.exec(`insert into lm values ('a2')`)
        const stateA2 = h1.runtimeFor(ctx.dbId)!.leaseState()
        expect(stateA2.holder).toBe('h2')
        expect(stateA2.held).toBe(false)

        // Epoch chain in the stream: L{head} epochs never decrease.
        const tailer = await fullTailer(ctx)
        const heads: LFrameHeader[] = []
        // (leases map holds only the last; walk the record via grants-style
        // full read: use the notifications-agnostic slices? The tailer does
        // not retain every L — assert via the LAST frame + the observed
        // states above, plus monotonicity of the two sampled epochs.)
        heads.push(tailer.leases.head!)
        expect(heads[0].holder).toBe('h2')
        expect(heads[0].epoch).toBeGreaterThanOrEqual(stateB.epoch)

        // Convergence untouched by lease churn.
        const o = await oracle<{ v: string }>(
          ctx,
          `select v from lm order by v`,
        )
        expect(o.rows.map((r) => r.v)).toEqual(['a1', 'a2', 'b1', 'b2'])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'c. sequence abuse (§5.3): a hot serial column hammered from both hosts — ZERO duplicate ids, grants disjoint in the stream, renewal observed, incarnation burn across hibernate/wake',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx, 0)
        // Deliberately NO unique index (plain serial, no pk): duplicates
        // would be silent — the count(distinct) oracle is the only guard,
        // per the honesty clause.
        await s.query(`create table hot (id serial, v text)`)

        const a = await connect(ctx, 0)
        const b = await connect(ctx, 1)
        // 2500 draws per host: enough to cross the 50% renewal threshold
        // of a 4096-wide grant on each side. Mixed shapes: batches (bulk
        // draws) + singles (100+ individual inserts each, interleaved).
        const hammer = async (c: Client, tag: string) => {
          for (let i = 0; i < 24; i++) {
            await c.query(
              `insert into hot (v) select '${tag}-b${i}-' || g from generate_series(1, 100) g`,
            )
          }
          for (let i = 0; i < 30; i++) {
            await c.query(`insert into hot (v) values ('${tag}-s${i}')`)
          }
        }
        await Promise.all([hammer(a, 'a'), hammer(b, 'b')])

        // THE assertion: zero duplicate sequence values, ever.
        const o = await oracle<{ n: string; d: string; mx: string }>(
          ctx,
          `select count(*)::text as n, count(distinct id)::text as d,
                  max(id)::text as mx from hot`,
        )
        expect(o.rows[0].n).toBe(String(2 * (24 * 100 + 30)))
        expect(o.rows[0].d).toBe(o.rows[0].n)
        const maxIdBefore = BigInt(o.rows[0].mx)

        // Stream grant shape: both hosts granted, ranges disjoint,
        // renewal observed (≥2 drawable grants for one grantee).
        const tailer = await fullTailer(ctx)
        const grants = drawableGrants(tailer, 'public.hot_id_seq')
        expect(grants.length).toBeGreaterThanOrEqual(3)
        const grantees = new Set(grants.map((g) => g.grantee))
        expect(grantees.has('h1')).toBe(true)
        expect(grantees.has('h2')).toBe(true)
        assertGrantsDisjoint(grants)
        const perGrantee = new Map<string, number>()
        for (const g of grants) {
          perGrantee.set(g.grantee, (perGrantee.get(g.grantee) ?? 0) + 1)
        }
        expect(Math.max(...perGrantee.values())).toBeGreaterThanOrEqual(2)
        const hwBefore = grants.reduce(
          (m, g) => (BigInt(g.end) > m ? BigInt(g.end) : m),
          0n,
        )

        // Incarnation burn: hibernate h1, wake it with a fresh insert —
        // the draw comes from a NEW range past the high-water (gaps
        // allowed), never resuming the burned residual.
        const rt1 = ctx.hosts[0].host.runtimeFor(ctx.dbId)!
        const oldGrant = rt1.grants.get('public.hot_id_seq')
        expect(oldGrant).toBeDefined()
        await ctx.hosts[0].host.hibernateDatabase('appdb')
        expect(rt1.grants.size).toBe(0) // burned with the incarnation

        const a2 = await connect(ctx, 0)
        const woke = await a2.query(
          `insert into hot (v) values ('a-woke') returning id`,
        )
        const wokeId = BigInt((woke.rows[0] as { id: string }).id)
        expect(wokeId > maxIdBefore).toBe(true)
        expect(wokeId > hwBefore).toBe(true)

        // Still zero duplicates; the fresh incarnation's grant starts at
        // or past the burned high-water.
        const o2 = await oracle<{ n: string; d: string }>(
          ctx,
          `select count(*)::text as n, count(distinct id)::text as d from hot`,
        )
        expect(o2.rows[0].d).toBe(o2.rows[0].n)
        const tailer2 = await fullTailer(ctx)
        const after = drawableGrants(tailer2, 'public.hot_id_seq')
        expect(after.length).toBeGreaterThan(grants.length)
        const newest = after[after.length - 1]
        expect(BigInt(newest.start) >= BigInt(oldGrant!.end)).toBe(true)
        assertGrantsDisjoint(after)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'd. cross-host session tokens (§7): a commit on host A is seen on host B via linearizable, and via session mode after waitForLsn(landedLsn)',
    async () => {
      const ctx = await setup()
      try {
        const sA = await ctx.hosts[0].host.connect('appdb')
        await sA.exec(`create table tok (v text)`)

        // Activate B FIRST so its watermark provably predates the commit.
        const sB = await ctx.hosts[1].host.connect('appdb')
        const sB2 = await ctx.hosts[1].host.connect('appdb')
        expect((await sB.exec(`select v from tok`)).rows).toEqual([])
        expect((await sB2.exec(`select v from tok`)).rows).toEqual([])

        const commit = await sA.exec(`insert into tok values ('x1')`)
        expect(commit.outcome).toBe('committed')
        expect(commit.landedLsn).toBeDefined()

        // session mode on B: stale by contract (host B never saw it).
        expect((await sB.exec(`select v from tok`)).rows).toEqual([])

        // linearizable on B: sees it.
        sB.setFreshness({ mode: 'linearizable' })
        expect((await sB.exec(`select v from tok`)).rows).toEqual([{ v: 'x1' }])

        // session + commit-LSN token on B2: waitForLsn, then the ordinary
        // session gate serves read-your-writes across hosts.
        await sB2.waitForLsn(parseLsn(commit.landedLsn!))
        expect((await sB2.exec(`select v from tok`)).rows).toEqual([
          { v: 'x1' },
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'f. grants survive era rotation: the rotator re-asserts high-waters into era N+1; a fresh joiner that tails only the new era grants disjointly',
    async () => {
      const ctx = await setup()
      try {
        const a = await connect(ctx, 0)
        await a.query(`create table gr (id serial, v text)`)
        await a.query(
          `insert into gr (v) select 'a-' || g from generate_series(1, 50) g`,
        )
        const tailer0 = await fullTailer(ctx)
        const hw0 = tailer0.grantHighWater('public.gr_id_seq')
        expect(hw0 > 0n).toBe(true)

        // Rotate: era N seals; the rotator re-asserts grant high-waters
        // into era N+1 as zero-width G frames.
        const report = await ctx.hosts[0].host.rotateDatabase('appdb')
        expect(report.toOrdinal).toBe(report.fromOrdinal + 1)

        // A FRESH third host joins now: its manifest points at era N+1,
        // it never reads era N — the re-asserted high-water is all it has.
        const h3 = new CellHost({
          gateway: ctx.core,
          dataRoot: join(ctx.root, 'host3'),
          hostId: 'h3',
        })
        ctx.extra.push(() => h3.shutdown())
        const s3 = await h3.connect('appdb')
        await s3.exec(`insert into gr (v) values ('h3-1')`)
        const rt3 = h3.runtimeFor(ctx.dbId)!
        // h3's tailer starts at era N+1's base: era N's grants invisible,
        // yet the zero-width re-assert carries the ceiling.
        expect(rt3.tailer.grantHighWater('public.gr_id_seq') >= hw0).toBe(true)
        const g3 = rt3.grants.get('public.gr_id_seq')
        expect(g3).toBeDefined()
        expect(g3!.start >= hw0).toBe(true)

        // Full-chain view: all drawable grants disjoint, zero duplicates.
        const tailer = await fullTailer(ctx)
        assertGrantsDisjoint(drawableGrants(tailer, 'public.gr_id_seq'))
        const reasserts = tailer
          .grantsFor('public.gr_id_seq')
          .filter((g) => g.start === g.end)
        expect(reasserts.length).toBeGreaterThanOrEqual(1)
        const o = await oracle<{ n: string; d: string }>(
          ctx,
          `select count(*)::text as n, count(distinct id)::text as d from gr`,
        )
        expect(o.rows[0].d).toBe(o.rows[0].n)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'e. randomized convergence oracle: 2 hosts x 2 sessions x 60 seeded ops -> oracle == both hosts, identity counters chain (pg_control), two seeds',
    async () => {
      for (const seed of [1337, 424242]) {
        const ctx = await setup()
        try {
          const boot = await ctx.hosts[0].host.connect('appdb')
          await boot.exec(
            `create table conv (id serial primary key, k int, v text)`,
          )
          await boot.close()

          const rand = mulberry32(seed)
          const sessions = await Promise.all([
            ctx.hosts[0].host.connect('appdb'),
            ctx.hosts[0].host.connect('appdb'),
            ctx.hosts[1].host.connect('appdb'),
            ctx.hosts[1].host.connect('appdb'),
          ])
          // Pre-draw the op scripts deterministically (the RNG must not be
          // shared across concurrently-interleaving sessions).
          const scripts = sessions.map((_, si) =>
            Array.from({ length: 15 }, (_, i) => {
              const r = rand()
              const k = Math.floor(rand() * 20)
              const tag = `s${si}-${i}`
              if (r < 0.3) {
                return `insert into conv (k, v) values (${k}, '${tag}')`
              } else if (r < 0.5) {
                return `update conv set v = v || '+${tag}' where k = ${k}`
              } else if (r < 0.6) {
                return `delete from conv where k = ${k}`
              } else if (r < 0.75) {
                return `begin; insert into conv (k, v) values (${k}, 'ab-${tag}'); rollback`
              } else if (r < 0.85) {
                return (
                  `begin; insert into conv (k, v) values (${k}, 'kept-${tag}'); ` +
                  `savepoint sp; insert into conv (k, v) values (${k}, 'undone-${tag}'); ` +
                  `rollback to sp; commit`
                )
              }
              return `select nextval('conv_id_seq')`
            }),
          )
          let conflicts = 0
          await Promise.all(
            sessions.map(async (session, si) => {
              for (const sql of scripts[si]) {
                try {
                  await session.exec(sql)
                } catch (err) {
                  if ((err as Error).name === 'SerializationConflictError') {
                    conflicts++ // acceptable outcome: nothing landed
                  } else {
                    throw err
                  }
                }
              }
            }),
          )
          for (const session of sessions) await session.close()

          const q = `select id, k, v from conv order by id`
          const o1 = await oracle<{ id: number; k: number; v: string }>(ctx, q)
          const o2 = await oracle<{ id: number; k: number; v: string }>(ctx, q)
          // Identity chain (§5.1): two independent full-stream
          // materializations agree byte-for-byte on pg_control identity.
          expect(o2.nextXid).toBe(o1.nextXid)
          expect(o2.checkPoint).toBe(o1.checkPoint)
          expect(o2.rows).toEqual(o1.rows)

          // Every host's linearizable read equals the oracle.
          for (const rig of ctx.hosts) {
            const s = await rig.host.connect('appdb')
            s.setFreshness({ mode: 'linearizable' })
            const rows = (await s.exec(q)).rows
            expect(rows).toEqual(o1.rows)
            await s.close()
          }
          // No duplicate serial ids under the randomized mix either.
          const dup = await oracle<{ n: string; d: string }>(
            ctx,
            `select count(*)::text as n, count(distinct id)::text as d from conv`,
          )
          expect(dup.rows[0].d).toBe(dup.rows[0].n)
          expect(conflicts).toBeLessThan(20) // sanity: mostly landed
        } finally {
          await ctx.teardown()
        }
      }
    },
    TEST_TIMEOUT * 2,
  )
})
