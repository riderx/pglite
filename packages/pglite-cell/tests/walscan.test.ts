// M5b: WAL introspection golden tests — pgl_walscan's classification of a
// known workload's WAL range is checked against pg_walinspect's view of
// the SAME range (record boundaries, xids, rmgrs), plus decoded-payload
// assertions for the §6.3 eager set, the identity/clog primitives
// round-trip, FPI restore, and the read-set capture ring.

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { PGlite } from '@electric-sql/pglite'
import { pg_walinspect } from '@electric-sql/pglite/contrib/pg_walinspect'
import { parseLsn } from '../src/lsn'
import { walscan, walscanRange } from '../src/walscan'
import type { WalRecord } from '../src/walscan'

const TEST_TIMEOUT = 120_000

let root: string
let pg: PGlite
let start: bigint
let end: bigint
let records: WalRecord[]

async function insertLsn(db: PGlite): Promise<bigint> {
  const rows = (
    await db.query<{ l: string }>(
      'select pg_current_wal_insert_lsn()::text as l',
    )
  ).rows
  return parseLsn(rows[0].l)
}

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-walscan-'))
  pg = new PGlite(join(root, 'db'), { extensions: { pg_walinspect } })
  await pg.exec(`
    create table wk(a int primary key, b text);
    create sequence wseq;
    checkpoint;
  `)
  start = await insertLsn(pg)
  // The known workload: commits, an abort, a subtransaction, sequence
  // draws, DDL (inval-carrying commit), and a checkpoint.
  await pg.exec(`insert into wk values (1, 'one'), (2, 'two')`)
  await pg.exec(`
    begin;
    insert into wk values (3, 'three');
    savepoint sp1;
    insert into wk values (4, 'four');
    release savepoint sp1;
    commit;
  `)
  await pg.exec(`
    begin;
    insert into wk values (99, 'doomed');
    rollback;
  `)
  await pg.exec(`select nextval('wseq')`)
  await pg.exec(`alter table wk add column c int`)
  await pg.exec(`checkpoint`)
  end = await insertLsn(pg)
  records = walscanRange(pg, start, end)
}, TEST_TIMEOUT)

afterAll(async () => {
  await pg.close().catch(() => undefined)
  rmSync(root, { recursive: true, force: true })
})

describe('pgl_walscan classification (golden vs pg_walinspect)', () => {
  it(
    'record boundaries, xids and rmgr ids match pg_walinspect exactly',
    async () => {
      await pg.exec('create extension if not exists pg_walinspect')
      const inspect = (
        await pg.query<{
          start_lsn: string
          end_lsn: string
          xid: string
          resource_manager: string
          record_type: string
        }>(
          `select start_lsn::text, end_lsn::text, xid::text,
                  resource_manager, record_type
             from pg_get_wal_records_info($1::pg_lsn, $2::pg_lsn)
            order by start_lsn`,
          [
            // format bigint lsn as pg_lsn text
            `${(start >> 32n).toString(16).toUpperCase()}/${(start & 0xffffffffn).toString(16).toUpperCase()}`,
            `${(end >> 32n).toString(16).toUpperCase()}/${(end & 0xffffffffn).toString(16).toUpperCase()}`,
          ],
        )
      ).rows
      expect(records.length).toBe(inspect.length)
      const RMGR_IDS: Record<string, number> = {
        XLOG: 0,
        Transaction: 1,
        Storage: 2,
        CLOG: 3,
        Database: 4,
        Tablespace: 5,
        MultiXact: 6,
        RelMap: 7,
        Standby: 8,
        Heap2: 9,
        Heap: 10,
        Btree: 11,
      }
      for (let i = 0; i < records.length; i++) {
        expect(parseLsn(inspect[i].start_lsn)).toBe(records[i].lsn)
        expect(parseLsn(inspect[i].end_lsn)).toBe(records[i].end)
        expect(Number(inspect[i].xid)).toBe(records[i].xid)
        const rmid = RMGR_IDS[inspect[i].resource_manager]
        if (rmid !== undefined) expect(rmid).toBe(records[i].rmid)
      }
    },
    TEST_TIMEOUT,
  )

  it('commit/abort payloads: subxids, invals, kinds', () => {
    const commits = records.filter((r) => r.kind === 'commit')
    const aborts = records.filter((r) => r.kind === 'abort')
    // 2-row insert, subxact txn, DDL commit (+ possibly inval-only ones)
    expect(commits.length).toBeGreaterThanOrEqual(3)
    expect(aborts.length).toBe(1)
    // The DDL (alter table) commit carries invalidation messages.
    const withInvals = commits.filter((r) => (r.nmsgs ?? 0) > 0)
    expect(withInvals.length).toBeGreaterThanOrEqual(1)
    expect(withInvals[0].invals!.length).toBe(withInvals[0].nmsgs! * 32)
    // Every commit/abort carries dense LSN bookkeeping.
    for (const r of [...commits, ...aborts]) {
      expect(r.end > r.lsn).toBe(true)
      expect(r.xid).toBeGreaterThan(0)
    }
  })

  it('sequence, checkpoint and block-ref classification', () => {
    const seq = records.filter((r) => r.kind === 'seq_log')
    expect(seq.length).toBeGreaterThanOrEqual(1)
    expect(seq[0].seqRel).toBeDefined()
    const ckpt = records.filter((r) => r.kind === 'checkpoint')
    expect(ckpt.length).toBe(1)
    expect(ckpt[0].nextXid! > 0n).toBe(true)
    expect(ckpt[0].nextOid).toBeGreaterThan(0)
    // Insert records reference heap blocks; the first post-checkpoint
    // touch is either an FPI (existing page) or a will_init rebuild
    // (brand-new page — the first insert of this workload).
    const heapBlocks = records.flatMap((r) => (r.rmid === 10 ? r.blocks : []))
    expect(heapBlocks.length).toBeGreaterThan(0)
    expect(heapBlocks.some((b) => b.img || b.init)).toBe(true)
    // And the range contains restorable images somewhere (catalog FPIs).
    const anyImg = records.some((r) => r.blocks.some((b) => b.img))
    expect(anyImg).toBe(true)
  })

  it('slices of the range chain: every record end is the next start', () => {
    for (let i = 1; i < records.length; i++) {
      // Contiguous modulo page-header skips: next lsn >= previous end.
      expect(records[i].lsn >= records[i - 1].end).toBe(true)
    }
    expect(records[0].lsn).toBe(start)
    expect(records[records.length - 1].end).toBe(end)
  })

  it('FPI restore: has_image blocks yield full 8192-byte pages', () => {
    let images = 0
    walscan(pg, start, end, (rec, imageOf) => {
      rec.blocks.forEach((b, i) => {
        if (b.img) {
          const page = imageOf(i)
          expect(page).not.toBeNull()
          expect(page!.length).toBe(8192)
          expect(page!.some((x) => x !== 0)).toBe(true)
          images++
        } else {
          expect(imageOf(i)).toBeNull()
        }
      })
    })
    expect(images).toBeGreaterThan(0)
  })
})

describe('identity + clog primitives round-trip', () => {
  it(
    'pgl_advance_identity moves the snapshot horizon; pgl_clog_set flips status',
    async () => {
      const mod = pg.Module
      const xmax = Number(
        (
          await pg.query<{ x: string }>(
            'select pg_snapshot_xmax(pg_current_snapshot())::text as x',
          )
        ).rows[0].x,
      )
      // Advance nextXid by 50: the fake xids in between become "past" ids
      // on already-zeroed clog pages.
      mod._pgl_advance_identity(BigInt(xmax) + 50n, 0, 0, 0)
      const xmax2 = Number(
        (
          await pg.query<{ x: string }>(
            'select pg_snapshot_xmax(pg_current_snapshot())::text as x',
          )
        ).rows[0].x,
      )
      expect(xmax2).toBe(xmax + 50)

      // Foreign-commit application: mark one of the burned xids committed
      // and one aborted; pg_xact_status must read the bits back.
      const committed = xmax + 10
      const aborted = xmax + 11
      mod._pgl_clog_set(committed, 1)
      mod._pgl_clog_set(aborted, 2)
      mod._pgl_invalidate_xact_caches()
      const status = async (xid: number) =>
        (
          await pg.query<{ s: string }>(
            `select pg_xact_status('${xid}'::text::xid8) as s`,
          )
        ).rows[0].s
      expect(await status(committed)).toBe('committed')
      expect(await status(aborted)).toBe('aborted')
    },
    TEST_TIMEOUT,
  )

  it('advance-only guards: identity never regresses', async () => {
    const mod = pg.Module
    const xmax = Number(
      (
        await pg.query<{ x: string }>(
          'select pg_snapshot_xmax(pg_current_snapshot())::text as x',
        )
      ).rows[0].x,
    )
    mod._pgl_advance_identity(3n, 0, 0, 0) // far in the past: ignored
    mod._pgl_advance_xid_past(3)
    const xmax2 = Number(
      (
        await pg.query<{ x: string }>(
          'select pg_snapshot_xmax(pg_current_snapshot())::text as x',
        )
      ).rows[0].x,
    )
    expect(xmax2).toBe(xmax)
  })
})

describe('read-set capture ring (§4.1 machinery)', () => {
  it('captures pins + nblocks probes; reset clears; disable stops', async () => {
    const mod = pg.Module
    const relfilenode = Number(
      (
        await pg.query<{ r: string }>(
          `select relfilenode::text as r from pg_class where relname = 'wk'`,
        )
      ).rows[0].r,
    )
    mod._pgl_readset_enable(1)
    await pg.query('select count(*) from wk')
    const count = mod._pgl_readset_count()
    expect(count).toBeGreaterThan(0)
    expect(mod._pgl_readset_overflowed()).toBe(0)
    const base = mod._pgl_readset_snapshot()
    const entries = new Uint32Array(mod.HEAPU8.buffer, base, count * 5).slice()
    const rels = new Set<number>()
    let sawNblocks = false
    for (let i = 0; i < count; i++) {
      rels.add(entries[i * 5 + 2])
      if (entries[i * 5 + 3] >>> 24 === 1) sawNblocks = true
    }
    // The seqscan pinned wk's pages and froze its nblocks.
    expect(rels.has(relfilenode)).toBe(true)
    expect(sawNblocks).toBe(true)

    mod._pgl_readset_reset()
    expect(mod._pgl_readset_count()).toBe(0)
    mod._pgl_readset_enable(0)
    await pg.query('select count(*) from wk')
    expect(mod._pgl_readset_count()).toBe(0)
  })
})
