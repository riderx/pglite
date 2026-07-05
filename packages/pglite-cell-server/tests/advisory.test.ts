// M6 advisory-lock policy tests (§4.6): the host option
// `advisoryLocks: 'local-warn' | 'error'` enforced at the wire against a REAL
// node-postgres client (which surfaces NoticeResponse via the 'notice' event).
//
//   - 'local-warn' (default): the FIRST advisory-lock use per session raises
//     a WARNING (01000) naming the cell-local scope; the statement succeeds;
//     the warning fires ONCE per session.
//   - 'error': any advisory-lock statement is rejected with 0A000 and never
//     executed (the lock is not taken); the session survives.
//
// Each test builds its own GatewayCore + CellHost + proxy (generous timeouts).

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Client } from 'pg'
import { GatewayCore } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'
import { CellProxyServer } from '../src/proxy/server'
import type { RuntimeOpts } from '../src/database-runtime'

const TEST_TIMEOUT = 240_000

interface Ctx {
  port: number
  clients: Client[]
  teardown: () => Promise<void>
}

async function setup(opts?: RuntimeOpts): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-advisory-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  await core.createDatabase('appdb')
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

interface Captured {
  notices: { code?: string; message: string }[]
}

async function connect(ctx: Ctx): Promise<{ c: Client; cap: Captured }> {
  const c = new Client({
    host: '127.0.0.1',
    port: ctx.port,
    database: 'appdb',
    user: 'postgres',
  })
  const cap: Captured = { notices: [] }
  c.on('error', () => undefined)
  c.on('notice', (n) =>
    cap.notices.push({
      code: (n as unknown as { code?: string }).code,
      message: (n as unknown as { message?: string }).message ?? '',
    }),
  )
  await c.connect()
  ctx.clients.push(c)
  return { c, cap }
}

describe('advisory-lock policy (M6 §4.6)', () => {
  it(
    "'local-warn' (default): first advisory use warns once, statement succeeds",
    async () => {
      const ctx = await setup() // default local-warn
      try {
        const { c, cap } = await connect(ctx)
        const r1 = await c.query('select pg_advisory_lock(42) as locked')
        // Statement succeeded (returns a row).
        expect(r1.rows.length).toBe(1)
        // A single cell-local warning fired.
        const warns = cap.notices.filter((n) => n.code === '01000')
        expect(warns.length).toBe(1)
        expect(warns[0].message.toLowerCase()).toContain('cell-local')

        // Second advisory use: NO further warning (once per session).
        await c.query('select pg_advisory_unlock(42)')
        await c.query('select pg_advisory_lock(43)')
        expect(cap.notices.filter((n) => n.code === '01000').length).toBe(1)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    "'local-warn' does not warn for non-advisory statements",
    async () => {
      const ctx = await setup()
      try {
        const { c, cap } = await connect(ctx)
        await c.query('create table t (id int)')
        await c.query('insert into t values (1)')
        expect(cap.notices.filter((n) => n.code === '01000').length).toBe(0)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    "'error': advisory statement is rejected 0A000 and NOT executed; session survives",
    async () => {
      const ctx = await setup({ advisoryLocks: 'error' })
      try {
        const { c } = await connect(ctx)
        let code: string | undefined
        try {
          await c.query('select pg_advisory_lock(42)')
        } catch (err) {
          code = (err as { code?: string }).code
        }
        expect(code).toBe('0A000')
        // The lock was never taken: no advisory lock is held (session
        // survives and serves ordinary queries).
        const held = await c.query(
          `select count(*)::int as n from pg_locks where locktype = 'advisory'`,
        )
        expect(held.rows[0].n).toBe(0)
        const ok = await c.query('select 1 as one')
        expect(ok.rows[0].one).toBe(1)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
