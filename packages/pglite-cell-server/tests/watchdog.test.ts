// W4 watchdog (§11.2 basics): defense against a runaway query.
//
//   1. statement_timeout (first line): set per session. NOTE (finding): in the
//      single-backend WASM build there is no interval-timer / signal delivery,
//      so statement_timeout does NOT actually fire for CPU/sleep-bound
//      statements (verified: pg_sleep(5) runs the full 5s under a 500ms
//      timeout). It is wired as best-effort — it may still cancel statements
//      that reach an interrupt check via I/O — but the JS watchdog is the
//      ACTUAL line of defense in WASM.
//   2. JS watchdog (second line): a statement that outlasts the deadline
//      (whether a sleep or a tight C loop) trips the host; the host terminates
//      the worker and fatally resets the session, and STAYS HEALTHY — a fresh
//      connection works immediately after.
//
// Runs in lazy-worker mode (the default now, W4) so the watchdog has a worker
// to terminate.

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Client } from 'pg'
import { GatewayCore } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'
import type { RuntimeOpts } from '../src/database-runtime'
import { CellProxyServer } from '../src/proxy/server'

const TEST_TIMEOUT = 120_000

interface Ctx {
  root: string
  core: GatewayCore
  host: CellHost
  proxy: CellProxyServer
  port: number
  clients: Client[]
  teardown: () => Promise<void>
}

async function setup(opts: RuntimeOpts): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-watchdog-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  await core.createDatabase('appdb')
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host1'),
    hostId: 'h1',
    // The watchdog's real line of defense is worker.terminate(), which only
    // exists when the cell runs in a worker — so force lazy-worker mode
    // explicitly (it hosts the cell in a worker on any checkpoint format).
    // The default reverted to 'nodefs' (see database-runtime cellMode note),
    // where there is no worker to terminate and statement_timeout does not
    // fire in WASM, so a runaway query is unkillable — a documented limit of
    // main-thread cells, and the reason lazy-worker exists.
    opts: { ...opts, cellMode: 'lazy-worker' },
  })
  const proxy = new CellProxyServer({ host, port: 0 })
  const port = await proxy.start()
  const clients: Client[] = []
  return {
    root,
    core,
    host,
    proxy,
    port,
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

describe('W4 watchdog', () => {
  it(
    'the watchdog terminates a pg_sleep past the deadline; host stays healthy',
    async () => {
      // 500ms statement_timeout ⇒ ~2s watchdog deadline. pg_sleep(60) is not
      // interrupted by statement_timeout in WASM, so the watchdog fires: the
      // worker is terminated well before 60s and the session is reset.
      const ctx = await setup({ statementTimeoutMs: 500 })
      try {
        const c = await connect(ctx)
        const t0 = Date.now()
        await expect(c.query(`select pg_sleep(60)`)).rejects.toThrow()
        expect(Date.now() - t0).toBeLessThan(15_000) // nowhere near 60s
        // A FRESH connection works — the host stayed healthy.
        const c2 = await connect(ctx)
        const r = await c2.query<{ n: string }>(`select 1::text as n`)
        expect(r.rows[0].n).toBe('1')
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'the JS watchdog terminates a non-interruptible loop; host stays healthy',
    async () => {
      // Tiny statement_timeout ⇒ ~2s watchdog deadline (4x, floored at 250ms).
      // A tight generate_series aggregate does not poll for interrupts every
      // row, so statement_timeout cannot cancel it promptly — the JS watchdog
      // must fire and reset the connection.
      const ctx = await setup({ statementTimeoutMs: 500 })
      try {
        const c = await connect(ctx)
        // A large pure-C aggregate loop. This blows past the ~2s deadline
        // without yielding, so the worker is terminated + the session reset.
        await expect(
          c.query(
            `select count(*) from generate_series(1, 2000000000) g
               where g % 2 = 0`,
          ),
        ).rejects.toThrow()
        // The connection is dead; a FRESH connection to the same host works —
        // proving the host itself stayed healthy (only the one session died).
        const c2 = await connect(ctx)
        const r = await c2.query<{ n: string }>(`select 42::text as n`)
        expect(r.rows[0].n).toBe('42')
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
