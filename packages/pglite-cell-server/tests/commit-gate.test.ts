// M5e exit tests — the commit gate (design §3.6/§14.2) + the §3.3 taint
// lift. The gate defers the ONE irreversible pre-commit step (the
// ON COMMIT DELETE ROWS physical truncate) past the CAS verdict, and the
// M5c in-place reset (with pgl_flush_base now covering local buffers)
// reverses everything else — so temp-table sessions survive CAS losses
// with their pre-attempt temp content intact, holdable cursors of a
// reversed commit are dropped (vanilla failed-COMMIT semantics), and the
// fatal session reset remains only for the recycle fallback.

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { GatewayCore } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import type { Cell, CommitResult } from '@electric-sql/pglite-cell'
import { CellHost } from '../src/host'
import { SerializationConflictError } from '../src/errors'
import type { HostSession } from '../src/session'
import type { RuntimeOpts } from '../src/database-runtime'

const TEST_TIMEOUT = 240_000

interface Ctx {
  root: string
  core: GatewayCore
  host: CellHost
  manifest: Manifest
  dbId: string
  teardown: () => Promise<void>
}

async function setup(opts: RuntimeOpts = {}): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-cgate-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host'),
    hostId: 'h1',
    opts,
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
      if (!process.env.PGL_KEEP) rmSync(root, { recursive: true, force: true })
    },
  }
}

/** The session's live cell (test hook via structural cast). M7 W3: the
 *  cell may be a WorkerCell whose gate members are async — call sites
 *  await (an awaited sync value is a no-op). */
function cellOf(s: HostSession): Cell {
  const cell = (s as unknown as { cell: Cell | null }).cell
  if (cell === null) throw new Error('session has no attached cell')
  return cell
}

/** Runtime of a session (test hook). */
function runtimeOf(s: HostSession) {
  return (
    s as unknown as {
      runtime: {
        commitFromSession: (input: unknown) => Promise<CommitResult>
      }
    }
  ).runtime
}

/**
 * Force the next `n` commit CASes of this session's runtime to LOSE
 * (landed: false) without touching the stream — a deterministic
 * simulation of a race loss at exactly the verdict point. Returns a
 * probe that records commitGatePending() AT CAS TIME plus a restore fn.
 */
function forceLosses(s: HostSession, n: number) {
  const rt = runtimeOf(s)
  const original = rt.commitFromSession.bind(rt)
  const state = { losses: 0, pendingAtCas: [] as number[] }
  rt.commitFromSession = async (input: unknown) => {
    // Only interfere with COMMIT slices — floors/sync probes ride the
    // real sequencer untouched.
    if ((input as { kind?: string }).kind !== 'commit') {
      return original(input)
    }
    try {
      state.pendingAtCas.push(await cellOf(s).commitGatePending())
    } catch {
      state.pendingAtCas.push(-1)
    }
    if (state.losses < n) {
      state.losses++
      return { landed: false } as CommitResult
    }
    return original(input)
  }
  return {
    state,
    restore: () => {
      rt.commitFromSession = original
    },
  }
}

async function expect40001(p: Promise<unknown>): Promise<string> {
  let err: unknown
  try {
    await p
  } catch (e) {
    err = e
  }
  expect(err).toBeInstanceOf(SerializationConflictError)
  expect((err as SerializationConflictError).code).toBe('40001')
  return String((err as Error).message)
}

describe('M5e commit gate + taint lift', () => {
  it(
    '1. §3.6 reorder: the ON COMMIT DELETE ROWS truncate is PENDING at CAS time, runs only after the landed verdict, and vanilla end-state semantics hold',
    async () => {
      const ctx = await setup()
      try {
        const s = await ctx.host.connect('appdb')
        expect(
          (
            await s.exec(
              `create table reg (id serial primary key, v text);
               create temp table stage (v text) on commit delete rows`,
            )
          ).outcome,
        ).toBe('committed')

        const probe = forceLosses(s, 0) // no losses: just the CAS-time probe
        const r = await s.exec(
          `begin;
           insert into stage values ('staged-1'), ('staged-2');
           insert into reg (v) select v from stage;
           commit`,
        )
        probe.restore()
        expect(r.outcome).toBe('committed')

        // THE reorder assertion: at the CAS point the truncate had NOT
        // run — it was deferred (pending >= 1 covers the stage table).
        expect(probe.state.pendingAtCas.length).toBe(1)
        expect(probe.state.pendingAtCas[0]).toBeGreaterThanOrEqual(1)
        // After the landed verdict the pendings were executed.
        expect(await cellOf(s).commitGatePending()).toBe(0)

        // Vanilla ON COMMIT DELETE ROWS semantics preserved end-to-end.
        expect(
          (await s.exec(`select count(*)::int as n from stage`)).rows[0].n,
        ).toBe(0)
        expect(
          (await s.exec(`select count(*)::int as n from reg`)).rows[0].n,
        ).toBe(2)
        await s.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '2. taint lift, REAL race: a temp-table (tainted) session loses an interactive COMMIT — 40001, session SURVIVES, pre-attempt PRESERVE ROWS content intact, DELETE ROWS staging NOT pre-truncated but rolled back, retry lands',
    async () => {
      const ctx = await setup()
      try {
        const a = await ctx.host.connect('appdb')
        expect(
          (
            await a.exec(
              `create table reg (id serial primary key, v text);
               create temp table keep (v text) on commit preserve rows;
               create temp table stage (v text) on commit delete rows`,
            )
          ).outcome,
        ).toBe('committed')
        expect(
          (await a.exec(`insert into keep values ('k1'), ('k2'), ('k3')`))
            .outcome,
        ).toBe('committed')
        // Seed reg: a first-ever insert would EXTEND the file on disk
        // (smgrzeroextend), which the reset soundness gate rejects — the
        // documented recycle-fallback boundary of the taint lift.
        expect(
          (await a.exec(`insert into reg (v) values ('seed0')`)).outcome,
        ).toBe('committed')
        expect(a.tainted).toBe(true) // temp schema latched

        // Interactive transaction with staged temp data + a regular write.
        await a.exec(`begin`)
        await a.exec(`insert into stage values ('doomed')`)
        await a.exec(`insert into reg (v) values ('a-interactive')`)

        // A REAL foreign commit while A is thinking — A's COMMIT loses.
        const b = await ctx.host.connect('appdb')
        expect(
          (await b.exec(`insert into reg (v) values ('b1')`)).outcome,
        ).toBe('committed')

        // Pre-M5e contract: fatal session reset. M5e: 40001, session alive.
        await expect40001(a.exec(`commit`))
        expect(a.closed).toBe(false)

        // §3.6's whole point: pre-attempt temp content survived the loss.
        expect(
          (await a.exec(`select count(*)::int as n from keep`)).rows[0].n,
        ).toBe(3)
        // The losing attempt's staged rows are gone via ABORT semantics
        // (reset discarded the attempt's local-buffer writes), NOT via a
        // pre-CAS physical truncate; the table is intact and usable.
        expect(
          (await a.exec(`select count(*)::int as n from stage`)).rows[0].n,
        ).toBe(0)

        // Client-driven retry of the whole transaction lands.
        await a.exec(`begin`)
        await a.exec(`insert into stage values ('staged')`)
        await a.exec(`insert into reg (v) select 'a-retry-' || v from stage`)
        expect((await a.exec(`commit`)).outcome).toBe('committed')

        expect(
          (await a.exec(`select v from reg order by v`)).rows.map((r) => r.v),
        ).toEqual(['a-retry-staged', 'b1', 'seed0'])
        await a.close()
        await b.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '3. taint lift, one-shot: a tainted session transparently RE-EXECUTES a lost one-shot with temp state intact (pre-M5e: fatal reset)',
    async () => {
      const ctx = await setup()
      try {
        const a = await ctx.host.connect('appdb')
        expect(
          (
            await a.exec(
              `create table reg (id serial primary key, v text);
               create temp table keep (v text) on commit preserve rows;
               insert into keep values ('kept')`,
            )
          ).outcome,
        ).toBe('committed')
        expect(
          (await a.exec(`insert into reg (v) values ('seed0')`)).outcome,
        ).toBe('committed')
        expect(a.tainted).toBe(true)

        const probe = forceLosses(a, 1)
        const r = await a.exec(`insert into reg (v) select v from keep`)
        probe.restore()
        // Transparent re-execution: the unit committed despite the loss.
        expect(r.outcome).toBe('committed')
        expect(probe.state.losses).toBe(1)
        expect(a.closed).toBe(false)
        expect((await a.exec(`select v from keep`)).rows[0].v).toBe('kept')
        expect(
          (await a.exec(`select count(*)::int as n from reg`)).rows[0].n,
        ).toBe(2)
        await a.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '4. a reversed commit never runs its deferred truncate: forced loss of a DELETE ROWS commit leaves pendings DISCARDED, not executed',
    async () => {
      const ctx = await setup()
      try {
        const a = await ctx.host.connect('appdb')
        expect(
          (
            await a.exec(
              `create table reg (id serial primary key, v text);
               create temp table stage (v text) on commit delete rows`,
            )
          ).outcome,
        ).toBe('committed')
        expect(
          (await a.exec(`insert into reg (v) values ('seed0')`)).outcome,
        ).toBe('committed')

        await a.exec(`begin`)
        await a.exec(`insert into stage values ('x')`)
        await a.exec(`insert into reg (v) values ('r')`)
        const probe = forceLosses(a, 99) // every CAS loses -> 40001
        await expect40001(a.exec(`commit`))
        probe.restore()

        // The truncate was pending at CAS time and must now be gone
        // WITHOUT having run (the reset discarded it natively).
        expect(probe.state.pendingAtCas[0]).toBeGreaterThanOrEqual(1)
        expect(a.closed).toBe(false)
        expect(await cellOf(a).commitGatePending()).toBe(0)

        // Session continues; the next gated commit works end-to-end.
        expect(
          (
            await a.exec(
              `begin; insert into stage values ('y');
               insert into reg (v) select v from stage; commit`,
            )
          ).outcome,
        ).toBe('committed')
        expect(
          (await a.exec(`select count(*)::int as n from stage`)).rows[0].n,
        ).toBe(0)
        await a.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '5. holdable-cursor hygiene: WITH HOLD cursors materialized by a REVERSED commit are dropped (vanilla failed-COMMIT semantics); pre-existing holdables survive',
    async () => {
      const ctx = await setup()
      try {
        const a = await ctx.host.connect('appdb')
        expect(
          (
            await a.exec(
              `create table reg (id serial primary key, v text);
               insert into reg (v) values ('seed')`,
            )
          ).outcome,
        ).toBe('committed')

        // A pre-existing holdable cursor from a LANDED commit (taints).
        await a.exec(`begin`)
        await a.exec(`declare c_old cursor with hold for select v from reg`)
        await a.exec(`insert into reg (v) values ('with-c-old')`)
        expect((await a.exec(`commit`)).outcome).toBe('committed')
        expect(a.tainted).toBe(true)

        // A new holdable cursor inside a LOSING transaction.
        await a.exec(`begin`)
        await a.exec(`declare c_new cursor with hold for select v from reg`)
        await a.exec(`insert into reg (v) values ('with-c-new')`)
        const probe = forceLosses(a, 99)
        await expect40001(a.exec(`commit`))
        probe.restore()
        expect(a.closed).toBe(false)

        const names = (
          await a.exec(`select name from pg_cursors order by name`)
        ).rows.map((r) => r.name)
        expect(names).toEqual(['c_old']) // c_new dropped, c_old intact

        expect(
          (await a.exec(`fetch all from c_old`)).rows.length,
        ).toBeGreaterThanOrEqual(1)
        await a.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '6. gate off (per-cell option) = vanilla commit sequence: the truncate runs pre-CAS, nothing is ever pending, and the pre-M5e tainted contract (fatal reset on loss) returns',
    async () => {
      const ctx = await setup({ commitGate: false })
      try {
        const a = await ctx.host.connect('appdb')
        expect(
          (
            await a.exec(
              `create table reg (id serial primary key, v text);
               create temp table stage (v text) on commit delete rows`,
            )
          ).outcome,
        ).toBe('committed')

        const probe = forceLosses(a, 0)
        expect(
          (
            await a.exec(
              `begin; insert into stage values ('x');
               insert into reg (v) select v from stage; commit`,
            )
          ).outcome,
        ).toBe('committed')
        probe.restore()
        // Vanilla: truncate already ran BEFORE the CAS; never deferred.
        expect(probe.state.pendingAtCas[0]).toBe(0)
        expect(
          (await a.exec(`select count(*)::int as n from stage`)).rows[0].n,
        ).toBe(0)
        await a.close()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
