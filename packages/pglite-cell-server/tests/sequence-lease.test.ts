// M5a native sequence-lease enforcement (§5.3 rule 1 + rule 3): the
// nextval_internal clamp makes M4's grants enforced-safe. A deliberately
// tiny grant (size 8) forces a batch to trip the clamp MID-batch; the
// session renews reactively (the 50% machinery turned reactive) and
// re-executes the one-shot; every committed value must lie inside some
// G-frame grant range — the property M4's unenforced subset couldn't have.

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Client } from 'pg'
import { EraTailer, parseLsn } from '@electric-sql/pglite-cell'
import type { GFrameHeader } from '@electric-sql/pglite-cell'
import { GatewayCore } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'
import { CellProxyServer } from '../src/proxy/server'

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

async function setup(): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-seqlease-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host1'),
    hostId: 'h1',
    // Tiny grant: a 20-value batch must trip the native clamp mid-batch
    // and complete purely through reactive renewal + re-execution.
    opts: { sequenceGrantSize: 8n, maxRetries: 10, attachAttempts: 40 },
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

async function grantsFor(ctx: Ctx, seqName: string): Promise<GFrameHeader[]> {
  const tailer = new EraTailer(ctx.core.streamClientFor(ctx.dbId), {
    path: ctx.manifest.era.path,
    eraId: ctx.manifest.era.id,
    ordinal: ctx.manifest.era.ordinal,
    baseOffset: ctx.manifest.era.baseOffset,
    baseLsn: parseLsn(ctx.manifest.era.baseLsn),
  })
  await tailer.catchUp()
  return tailer
    .grantsFor(seqName)
    .filter((g) => BigInt(g.end) > BigInt(g.start))
}

describe('M5a native sequence-lease clamp (§5.3)', () => {
  it(
    'a. a 20-value batch trips a size-8 lease mid-batch, renews, completes; every committed value lies inside a G-frame grant range',
    async () => {
      const ctx = await setup()
      try {
        const c = await connect(ctx)
        await c.query(`create table t (id bigserial, v text)`)

        // First evidence draw: takes the first grant post-commit and
        // registers the native lease on the live cell.
        await c.query(`insert into t (v) values ('seed')`)

        // The batch: 20 draws against 8 of lease headroom. The native
        // clamp MUST error mid-batch ("sequence lease exhausted"); the
        // session renews the grant reactively and re-executes the
        // one-shot until it completes. The client sees only success.
        await c.query(
          `insert into t (v) select 'b-' || g from generate_series(1, 20) g`,
        )

        const rows = (await c.query(`select id from t order by id`)).rows.map(
          (r: { id: string }) => BigInt(r.id),
        )
        expect(rows.length).toBe(21)
        expect(new Set(rows.map(String)).size).toBe(21) // all distinct

        // Enforced-safe: every committed value lies within some drawable
        // grant range (start, end] recorded in G frames. The single
        // bootstrap draw that CONSTITUTES the first grant's probe
        // evidence is the one allowed exception (it equals the first
        // grant's start).
        const grants = await grantsFor(ctx, 'public.t_id_seq')
        expect(grants.length).toBeGreaterThanOrEqual(3) // renewal(s) happened
        const first = [...grants].sort((a, b) =>
          BigInt(a.start) < BigInt(b.start) ? -1 : 1,
        )[0]
        const covered = (v: bigint) =>
          grants.some((g) => v > BigInt(g.start) && v <= BigInt(g.end))
        for (const v of rows) {
          if (v <= BigInt(first.start)) {
            expect(v).toBe(BigInt(first.start)) // the bootstrap draw only
            continue
          }
          expect(covered(v)).toBe(true)
        }

        // Grant ranges disjoint in the stream.
        const sorted = [...grants].sort((a, b) =>
          BigInt(a.start) < BigInt(b.start) ? -1 : 1,
        )
        for (let i = 1; i < sorted.length; i++) {
          expect(BigInt(sorted[i].start) >= BigInt(sorted[i - 1].end)).toBe(
            true,
          )
        }
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'b. interactive transactions surface the lease-exhaustion error (no transparent retry); the renew signal is client-visible',
    async () => {
      const ctx = await setup()
      try {
        const c = await connect(ctx)
        await c.query(`create table t2 (id bigserial, v text)`)
        await c.query(`insert into t2 (v) values ('seed')`) // grant taken

        await c.query('begin')
        let threw: unknown = null
        try {
          await c.query(
            `insert into t2 (v) select 'x-' || g from generate_series(1, 50) g`,
          )
        } catch (e) {
          threw = e
        }
        expect(threw).not.toBeNull()
        const err = threw as { message: string; detail?: string }
        expect(err.message).toContain('reached maximum value')
        expect(err.detail).toBe('sequence lease exhausted')
        await c.query('rollback')

        // The session survives; a one-shot retries transparently and the
        // renewed lease admits fresh draws.
        await c.query(
          `insert into t2 (v) select 'y-' || g from generate_series(1, 20) g`,
        )
        const o = (
          await c.query(
            `select count(*)::text as n, count(distinct id)::text as d from t2`,
          )
        ).rows[0] as { n: string; d: string }
        expect(o.n).toBe('21')
        expect(o.d).toBe(o.n)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
