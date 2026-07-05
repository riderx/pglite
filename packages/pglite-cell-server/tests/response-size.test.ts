// H1 §3.5 response-size safety: the buffering ladder (memory → spool file →
// 40001+HINT) and declared-read-only STREAMING. REAL `pg` clients over TCP.
//
// The scenarios:
//  - a big result in a NON-read-only transaction that fits the memory rung
//    returns normally (control);
//  - a result larger than a tiny `bufferMemoryMax` spills to a spool file
//    and still returns intact (the spool round-trips);
//  - a result larger than a tiny `bufferSpoolMax` aborts the unit with
//    40001 + the HINT naming the READ ONLY fix — and nothing partial leaks;
//  - the SAME big query under BEGIN READ ONLY streams fine under a bounded
//    memory delta (the true fix): no ladder, no 40001;
//  - kill-at-step in the spooled regime (socket destroyed mid-unit) leaves
//    no stranded spool files and rolls back cleanly;
//  - a large read-only SELECT streams under a loosely-asserted RSS bound.

import { describe, it, expect } from 'vitest'
import { rmSync, readdirSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Client } from 'pg'
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
  clients: Client[]
  teardown: () => Promise<void>
}

async function setup(opts: RuntimeOpts = {}): Promise<Ctx> {
  const root = scratchDir('pgl-respsize-')
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

/** A SELECT that generates roughly `bytes` of text output. */
function bigSelect(bytes: number): string {
  // ~200-byte rows: a 190-char padded string per generate_series row.
  const rows = Math.ceil(bytes / 200)
  return `select g, repeat('x', 190) as pad from generate_series(1, ${rows}) g`
}

/** Count leftover proxy spool files in the OS tmpdir (H1 cleanup oracle). */
function spoolFiles(): string[] {
  return readdirSync(tmpdir()).filter((f) => f.startsWith('pgl-proxy-spool-'))
}

describe('H1 §3.5 buffering ladder + read-only streaming', () => {
  it(
    'small result in a plain (non-read-only) transaction returns normally',
    async () => {
      const ctx = await setup()
      try {
        const c = await connect(ctx)
        const r = await c.query(bigSelect(10_000))
        expect(r.rows.length).toBeGreaterThan(0)
        expect(r.rows[0].pad).toBe('x'.repeat(190))
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'result past bufferMemoryMax spills to a spool file and round-trips intact',
    async () => {
      // 64 KiB memory rung, 64 MiB spool: a ~2 MiB result must spool.
      const ctx = await setup({
        bufferMemoryMax: 64 * 1024,
        bufferSpoolMax: 64 * 1024 * 1024,
      })
      const before = spoolFiles().length
      try {
        const c = await connect(ctx)
        const r = await c.query(bigSelect(2 * 1024 * 1024))
        expect(r.rows.length).toBeGreaterThan(9_000)
        // The last row's data is intact — the whole spooled response was read
        // back correctly (not just a prefix).
        expect(r.rows[r.rows.length - 1].pad).toBe('x'.repeat(190))
      } finally {
        // Spool file must be gone after the unit finished.
        expect(spoolFiles().length).toBe(before)
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'result past bufferSpoolMax aborts with 40001 + READ ONLY hint, nothing partial',
    async () => {
      // Tiny caps: 16 KiB memory, 256 KiB spool. A ~4 MiB result overflows.
      const ctx = await setup({
        bufferMemoryMax: 16 * 1024,
        bufferSpoolMax: 256 * 1024,
      })
      const before = spoolFiles().length
      try {
        const c = await connect(ctx)
        let err: unknown
        try {
          await c.query(bigSelect(4 * 1024 * 1024))
        } catch (e) {
          err = e
        }
        expect(err).toBeDefined()
        const e = err as { code?: string; hint?: string; message?: string }
        expect(e.code).toBe('40001')
        expect((e.hint ?? '').toLowerCase()).toContain('read only')
        // The connection survives (a clean RFQ followed the error): a small
        // follow-up query works.
        const ok = await c.query('select 42 as n')
        expect(ok.rows[0].n).toBe(42)
      } finally {
        expect(spoolFiles().length).toBe(before)
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'the SAME oversized read streams fine under BEGIN READ ONLY (no ladder)',
    async () => {
      const ctx = await setup({
        bufferMemoryMax: 16 * 1024,
        bufferSpoolMax: 256 * 1024,
      })
      try {
        const c = await connect(ctx)
        await c.query('begin read only')
        // 4 MiB result: would 40001 under the ladder, but streams here.
        const r = await c.query(bigSelect(4 * 1024 * 1024))
        expect(r.rows.length).toBeGreaterThan(19_000)
        expect(r.rows[r.rows.length - 1].pad).toBe('x'.repeat(190))
        await c.query('commit')
        // Session healthy afterwards.
        const ok = await c.query('select 1 as n')
        expect(ok.rows[0].n).toBe(1)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'default_transaction_read_only = on also streams a plain BEGIN',
    async () => {
      const ctx = await setup({
        bufferMemoryMax: 16 * 1024,
        bufferSpoolMax: 256 * 1024,
      })
      try {
        const c = await connect(ctx)
        await c.query('set default_transaction_read_only = on')
        await c.query('begin')
        const r = await c.query(bigSelect(3 * 1024 * 1024))
        expect(r.rows.length).toBeGreaterThan(14_000)
        await c.query('commit')
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'kill-at-step in the spooled regime strands no spool file and rolls back',
    async () => {
      const ctx = await setup({
        bufferMemoryMax: 16 * 1024,
        bufferSpoolMax: 128 * 1024 * 1024,
      })
      const before = spoolFiles().length
      try {
        // Fire a big spooling query and destroy the socket mid-flight,
        // repeatedly — each iteration exercises the connection-close spool
        // cleanup path.
        for (let i = 0; i < 3; i++) {
          const c = await connect(ctx)
          const stream = (
            c as unknown as {
              connection: { stream: import('node:net').Socket }
            }
          ).connection.stream
          // Do not await: destroy the socket a beat after sending.
          const p = c.query(bigSelect(8 * 1024 * 1024)).catch(() => undefined)
          setTimeout(() => stream.destroy(), 5)
          await p
          await c.end().catch(() => undefined)
        }
        // Give the server a moment to run its teardown/dispose.
        await new Promise((r) => setTimeout(r, 50))
        // A clean session still works and the DB is consistent.
        const c2 = await connect(ctx)
        const ok = await c2.query('select 7 as n')
        expect(ok.rows[0].n).toBe(7)
      } finally {
        expect(spoolFiles().length).toBe(before)
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'a large read-only SELECT streams under a bounded RSS delta',
    async () => {
      const ctx = await setup({
        // Small buffer caps: if streaming secretly buffered, a 40001 or an
        // RSS blowup would out it.
        bufferMemoryMax: 64 * 1024,
        bufferSpoolMax: 1 * 1024 * 1024,
      })
      try {
        const c = await connect(ctx)
        await c.query('begin read only')
        if (global.gc) global.gc()
        const rssBefore = process.memoryUsage().rss
        // ~30 MiB of result text streamed row by row. node-postgres still
        // materializes the final rows array client-side, so the bound is
        // loose (client buffering dominates) — the assertion is only that
        // the SERVER did not additionally spool/buffer the whole thing on
        // top (which would roughly double the peak). A 6x-of-result ceiling
        // is comfortably loose but still catches a full server-side buffer.
        const targetBytes = 30 * 1024 * 1024
        const r = await c.query(bigSelect(targetBytes))
        const rssAfter = process.memoryUsage().rss
        expect(r.rows.length).toBeGreaterThan(140_000)
        await c.query('commit')
        const delta = rssAfter - rssBefore
        expect(delta).toBeLessThan(targetBytes * 6)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
