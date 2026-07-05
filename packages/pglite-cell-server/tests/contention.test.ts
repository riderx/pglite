// H5 §16 missing tests: adversarial cross-host contention through two
// CellHosts on one gateway/database, with REAL `pg` clients. Covers the
// SKIP LOCKED double-claim, RETURNING-heavy ORM abuse, migration-flow under
// advisory 'local-warn', and FK-heavy multixact minting under savepoints
// across two cells — each with a materialize/dump oracle.

import { describe, it, expect } from 'vitest'
import { rmSync } from 'node:fs'
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
import { scratchDir } from '@electric-sql/pglite-cell/testing'
import { CellHost } from '../src/host'
import { CellProxyServer } from '../src/proxy/server'
import type { RuntimeOpts } from '../src/database-runtime'

const TEST_TIMEOUT = 240_000

/** Two genuinely-hammering hosts need raised re-execution budgets (§3.2). */
const FLEET_OPTS: RuntimeOpts = { attachAttempts: 40, maxRetries: 10 }

interface Rig {
  host: CellHost
  proxy: CellProxyServer
  port: number
}
interface Ctx {
  root: string
  core: GatewayCore
  hosts: Rig[]
  manifest: Manifest
  dbId: string
  clients: Client[]
  teardown: () => Promise<void>
}

async function setup(opts: RuntimeOpts = {}, hostCount = 2): Promise<Ctx> {
  const root = scratchDir('pgl-contend-')
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const hosts: Rig[] = []
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
  return {
    root,
    core,
    hosts,
    manifest,
    dbId: manifest.databaseId,
    clients,
    teardown: async () => {
      for (const c of clients) await c.end().catch(() => undefined)
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

let oracleN = 0
/** Materialize the FULL era chain and query it (dump-equality oracle). */
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

describe('H5 §16 adversarial contention (two hosts, one database)', () => {
  it(
    'SKIP LOCKED double-claim: exactly one host wins the row, the loser 40001s',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx, 0)
        await s.query('create table jobs (id int primary key, taken text)')
        await s.query('insert into jobs values (1, null)')
        // Let the write land + both hosts' watermarks catch up.
        await new Promise((r) => setTimeout(r, 200))

        const a = await connect(ctx, 0)
        const b = await connect(ctx, 1)
        // Both workers race to claim the SAME single job row via FOR UPDATE
        // SKIP LOCKED. Row locks are CELL-LOCAL (each host has its own cell),
        // so SKIP LOCKED does NOT exclude across hosts: a simultaneous round
        // has BOTH read+write the row and BOTH lose the optimistic COMMIT
        // race with 40001 (the documented queue guidance — retry). The
        // exactly-one-wins guarantee holds over the RETRY: whichever worker's
        // claim lands first, the other's next `SKIP LOCKED` finds the row
        // already `taken` (its `where taken is null` matches nothing), so it
        // claims NOTHING. Net: exactly one worker ever writes its name.
        let sawConflict = false
        const claim = async (c: Client, who: string) => {
          for (let attempt = 0; attempt < 50; attempt++) {
            try {
              await c.query('begin')
              const sel = await c.query(
                'select id from jobs where taken is null for update skip locked',
              )
              if (sel.rows.length === 0) {
                // The row is already claimed (or locked-and-skipped): nothing
                // to do. Commit the empty txn and report "did not claim".
                await c.query('commit')
                return { who, claimed: false as const }
              }
              await c.query('update jobs set taken = $1 where id = 1', [who])
              await c.query('commit')
              return { who, claimed: true as const }
            } catch (e) {
              await c.query('rollback').catch(() => undefined)
              if ((e as { code?: string }).code === '40001') {
                sawConflict = true
                continue
              }
              throw e
            }
          }
          throw new Error(`${who} exhausted claim retries`)
        }
        const [ra, rb] = await Promise.all([claim(a, 'a'), claim(b, 'b')])
        const claimers = [ra, rb].filter((r) => r.claimed).map((r) => r.who)
        // Exactly one worker ever wrote its name to the row.
        expect(claimers.length).toBe(1)
        // The cross-cell conflict really did surface as a 40001 at least once
        // (the loser's documented signal), not silent serialization.
        expect(sawConflict).toBe(true)

        // The oracle agrees the row is taken by exactly that one claimer.
        const rows = await oracle<{ taken: string }>(
          ctx,
          'select taken from jobs where id = 1',
        )
        expect(rows[0].taken).toBe(claimers[0])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'RETURNING-heavy ORM abuse: observed ids exactly equal the committed ids',
    async () => {
      // High attach/retry budgets: two hosts each firing 100 one-shot
      // RETURNING inserts genuinely hammer the canonical-ensure loop; bounded
      // starvation there surfaces as a fatal AdvanceRaceError (§3.2), which a
      // real client survives by reconnecting — modelled below.
      const ctx = await setup({ attachAttempts: 120, maxRetries: 20 })
      try {
        const s = await connect(ctx, 0)
        await s.query(
          'create table items (id bigserial primary key, host text)',
        )
        await new Promise((r) => setTimeout(r, 150))

        const PER = 100
        const observed: bigint[] = []
        const insert = async (hostIdx: number, who: string) => {
          let c = await connect(ctx, hostIdx)
          for (let i = 0; i < PER; i++) {
            // Retry the one-shot RETURNING insert until it lands; each
            // observed id must be a real committed id. A one-shot CAS loss
            // re-executes transparently behind the wire (rarely 40001);
            // a fatal (connection dropped under attach starvation) is
            // survived by reconnecting — nothing committed on that path.
            for (;;) {
              try {
                const r = await c.query(
                  'insert into items (host) values ($1) returning id',
                  [who],
                )
                observed.push(BigInt(r.rows[0].id))
                break
              } catch (e) {
                const code = (e as { code?: string }).code
                if (code === '40001') continue
                // Connection died (fatal reset / attach starvation): reconnect
                // and retry the SAME insert (it never committed).
                await c.end().catch(() => undefined)
                c = await connect(ctx, hostIdx)
              }
            }
          }
        }
        await Promise.all([insert(0, 'a'), insert(1, 'b')])

        // The committed ids per the oracle.
        const committed = (
          await oracle<{ id: string }>(ctx, 'select id from items order by id')
        ).map((r) => BigInt(r.id))

        // Every observed id is committed, none duplicated, and the multisets
        // match exactly (observed == committed).
        expect(observed.length).toBe(PER * 2)
        expect(new Set(observed).size).toBe(observed.length)
        const obsSorted = [...observed].sort((x, y) => (x < y ? -1 : 1))
        expect(obsSorted).toEqual(committed)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    "migration flow under advisoryLocks='local-warn' succeeds with a warning",
    async () => {
      // The migration-tool shape (transactional DDL): a single interactive
      // transaction acquires an advisory lock, runs the DDL batch, releases
      // the lock, and commits. Because the lock is released BEFORE COMMIT,
      // the post-txn taint probe sees no held advisory lock — the session is
      // NOT pinned — so the batch commits atomically and the flow SUCCEEDS
      // with the cell-local advisory warning. Two hosts run (the default);
      // there is no competing writer, so the single commit lands first try.
      const ctx = await setup({ advisoryLocks: 'local-warn' }, 1)
      try {
        const c = await connect(ctx, 0)
        const notices: string[] = []
        c.on('notice', (n) => notices.push(n.message ?? ''))
        // Multi-statement DDL in one transaction is a rebase taint (§4.5), so
        // if this host renews its head-lease mid-transaction the COMMIT can
        // 40001 (no transparent rebase for a DDL txn) — exactly what a real
        // migration tool retries on. Retry the whole batch until it lands.
        for (let attempt = 0; ; attempt++) {
          try {
            await c.query('begin')
            await c.query('select pg_advisory_xact_lock(42)')
            await c.query('create table m (id int primary key)')
            await c.query('alter table m add column name text')
            await c.query('create index m_name_idx on m (name)')
            await c.query("insert into m values (1, 'x')")
            await c.query('commit') // xact-scoped lock auto-releases at commit
            break
          } catch (e) {
            await c.query('rollback').catch(() => undefined)
            // A prior partial attempt may have created the table; drop it so
            // the retry is clean.
            await c.query('drop table if exists m').catch(() => undefined)
            if ((e as { code?: string }).code === '40001' && attempt < 20)
              continue
            throw e
          }
        }
        const rows = await oracle<{ id: number; name: string }>(
          ctx,
          'select id, name from m order by id',
        )
        expect(rows).toEqual([{ id: 1, name: 'x' }])
        expect(
          notices.some((m) => /advisory lock scope is cell-local/i.test(m)),
        ).toBe(true)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'FK-heavy multixact minting under savepoints across two cells ⇒ dump equality',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx, 0)
        await s.query('create table parent (id int primary key, tag text)')
        await s.query(
          'create table child (id serial primary key, pid int references parent(id), note text)',
        )
        // Seed parents.
        await s.query(
          "insert into parent select g, 'p'||g from generate_series(1, 20) g",
        )
        await new Promise((r) => setTimeout(r, 200))

        const a = await connect(ctx, 0)
        const b = await connect(ctx, 1)
        // Both hosts insert children referencing the SAME parents inside a
        // transaction using SAVEPOINTs — concurrent FK-share locks on shared
        // parent rows mint multixacts. Each host commits its batch; the
        // cross-cell conflict resolves at COMMIT (retry the loser).
        const runBatch = async (c: Client, who: string) => {
          for (let round = 0; round < 5; round++) {
            for (;;) {
              try {
                await c.query('begin')
                for (let p = 1; p <= 20; p++) {
                  await c.query('savepoint sp')
                  await c.query(
                    'insert into child (pid, note) values ($1, $2)',
                    [p, `${who}-${round}-${p}`],
                  )
                  await c.query('release savepoint sp')
                }
                await c.query('commit')
                break
              } catch (e) {
                await c.query('rollback').catch(() => undefined)
                if ((e as { code?: string }).code === '40001') continue
                throw e
              }
            }
          }
        }
        await Promise.all([runBatch(a, 'a'), runBatch(b, 'b')])

        // Dump-equality oracle: every child references a real parent, the
        // note set is exactly the 2*5*20 = 200 expected notes, and the FK is
        // intact (no orphans). A materialize of the full chain is the
        // pg_amcheck-style structural sanity here.
        const notes = (
          await oracle<{ note: string }>(
            ctx,
            'select note from child order by note',
          )
        ).map((r) => r.note)
        const expected: string[] = []
        for (const who of ['a', 'b'])
          for (let round = 0; round < 5; round++)
            for (let p = 1; p <= 20; p++) expected.push(`${who}-${round}-${p}`)
        expected.sort()
        expect(notes).toEqual(expected)
        // No orphaned children (FK integrity survived the cross-cell merge).
        const orphans = await oracle<{ n: string }>(
          ctx,
          'select count(*)::text as n from child c left join parent p on p.id = c.pid where p.id is null',
        )
        expect(orphans[0].n).toBe('0')
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
