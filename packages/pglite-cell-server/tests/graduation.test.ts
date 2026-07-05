// M6 graduation tests (§15 / OQ7): graduateDatabase produces a logical
// pg_dump at a linearizable-fresh head that RESTORES into a brand-new plain
// PGlite; tables / rows / a sequence's floor / an index survive, and the
// manifest snapshot fields are sane. OQ7 recorded: physical graduation is
// closed, logical dump/restore is THE path.
//
// Each test builds its own GatewayCore + CellHost (many PGlite boots —
// generous timeouts).

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { PGlite } from '@electric-sql/pglite'
import { GatewayCore } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'

const TEST_TIMEOUT = 240_000

interface Ctx {
  root: string
  core: GatewayCore
  host: CellHost
  manifest: Manifest
  dbId: string
  teardown: () => Promise<void>
}

async function setup(): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-graduate-test-'))
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

describe('graduateDatabase (M6 §15 / OQ7)', () => {
  it(
    'logical dump restores into a fresh plain PGlite; tables/rows/sequence/index match; manifest snapshot sane',
    async () => {
      const ctx = await setup()
      let restored: PGlite | null = null
      try {
        const s = await ctx.host.connect('appdb')
        // Schema: a SERIAL sequence + an explicit index.
        await s.exec(`
          create table item (
            id serial primary key,
            name text not null,
            qty int not null default 0
          );
          create index item_name_idx on item (name);
        `)
        for (let i = 1; i <= 25; i++) {
          const r = await s.exec(
            `insert into item (name, qty) values ('item-${i}', ${i * 10})`,
          )
          expect(r.outcome).toBe('committed')
        }
        // Advance the sequence a little further then delete to leave a floor
        // above the live max(id) (so a naive re-derive would differ).
        await s.exec(`insert into item (name, qty) values ('gap', 0)`)
        await s.exec(`delete from item where name = 'gap'`)

        const srcRows = (
          await s.exec(`select id, name, qty from item order by id`)
        ).rows
        const srcSeq = (
          await s.exec(
            `select last_value::text as v, is_called from item_id_seq`,
          )
        ).rows[0] as { v: string; is_called: boolean }

        // Graduate: logical export at a linearizable-fresh head.
        const result = await ctx.host.graduateDatabase('appdb')
        expect(typeof result.sql).toBe('string')
        expect(result.sql.length).toBeGreaterThan(0)
        expect(result.sql).toContain('CREATE TABLE')
        // Manifest snapshot fields sane.
        const snap = result.manifestSnapshot
        expect(snap.databaseId).toBe(ctx.dbId)
        expect(snap.checkpointRef).toMatch(/^sha256:/)
        expect(snap.eraOrdinal).toBeGreaterThanOrEqual(1)
        expect(snap.headLsn).toMatch(/^[0-9A-F]+\/[0-9A-F]+$/)
        expect(snap.headOffset.length).toBeGreaterThan(0)

        // Restore into a brand-new plain PGlite and compare.
        restored = new PGlite(join(ctx.root, 'restored'))
        await restored.exec(result.sql)
        // pg_dump sets an empty search_path; restore public for the
        // unqualified queries below (the documented restore caveat).
        await restored.exec(`set search_path to public`)

        const dstRows = (
          await restored.query<{ id: number; name: string; qty: number }>(
            `select id, name, qty from item order by id`,
          )
        ).rows
        expect(dstRows).toEqual(srcRows)

        // The sequence floor survives: nextval on the restored db must be
        // ABOVE the source's last_value (dump carries the setval).
        const dstSeq = (
          await restored.query<{ v: string; is_called: boolean }>(
            `select last_value::text as v, is_called from item_id_seq`,
          )
        ).rows[0]
        expect(BigInt(dstSeq.v)).toBeGreaterThanOrEqual(BigInt(srcSeq.v))
        const nextId = (
          await restored.query<{ nv: string }>(
            `select nextval('item_id_seq')::text as nv`,
          )
        ).rows[0]
        expect(BigInt(nextId.nv)).toBeGreaterThan(BigInt(srcSeq.v))

        // The index survives the restore.
        const idx = (
          await restored.query<{ n: string }>(
            `select count(*)::text as n from pg_indexes
              where tablename = 'item' and indexname = 'item_name_idx'`,
          )
        ).rows[0]
        expect(idx.n).toBe('1')

        await s.close()
      } finally {
        if (restored) await restored.close().catch(() => undefined)
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
