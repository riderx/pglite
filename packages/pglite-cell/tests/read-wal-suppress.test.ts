// H2 (design §14.8): read-cell WAL suppression. A read-attached cell that
// only seqscans replicated relations must emit NO WAL of its own; the one
// everyday source — opportunistic HOT pruning during scans — is disabled by
// the native `pgl_set_suppress_read_wal` guard in heap_page_prune_opt().
//
// Construction: a writer churns a small table with many UPDATEs (leaving
// prunable dead tuples on hot pages), settles, and the datadir is opened as a
// read cell. Repeatedly seqscanning it would let vanilla Postgres prune (and
// write a prune record) — with suppression on, the cell's WAL insert position
// must not move across 50 scans.

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { PGlite } from '@electric-sql/pglite'
import { mkdtempSync, rmSync, cpSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Cell } from '../src/cell'
import { readControl, SHUTDOWN_CKPT_ALIGNED } from '../src/datadir'

const TEST_TIMEOUT = 120_000

let root: string
let template: string

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgcell-h2-'))
  template = join(root, 'template')
  // Churn a hot table so pages carry dead tuples (prune candidates), then
  // settle to a clean shutdown so a plain open writes zero boot WAL.
  const db = new PGlite(template, {
    initDbStartParams: ['--no-data-checksums'],
  })
  await db.exec(`create table hot (id int primary key, v int)`)
  await db.exec(`insert into hot select g, 0 from generate_series(1, 200) g`)
  for (let i = 0; i < 40; i++) {
    await db.exec(`update hot set v = v + 1 where id % 3 = ${i % 3}`)
  }
  await db.exec(`vacuum hot`) // establish a visibility horizon; leaves prune_xid churn
  for (let i = 0; i < 40; i++) {
    await db.exec(`update hot set v = v + 1 where id % 5 = ${i % 5}`)
  }
  await db.close()
  const settle = new PGlite(template)
  await settle.query(`select 1`)
  await settle.close()
}, TEST_TIMEOUT)

afterAll(() => {
  rmSync(root, { recursive: true, force: true })
})

/** A fresh copy of the settled template (each cell opens its own datadir). */
function freshDir(name: string): string {
  const dir = join(root, name)
  cpSync(template, dir, { recursive: true })
  return dir
}

describe('H2 read-cell WAL suppression', () => {
  it(
    'a hot-page read loop captures nothing with suppression on',
    async () => {
      const dir = freshDir('read-suppressed')
      const cell = await Cell.open(dir, {
        expectedHeadLsn: bookmarkOf(dir),
        suppressReadWal: true,
      })
      const openLsn = cell.captureCursor
      for (let scan = 0; scan < 50; scan++) {
        // Force a seqscan over the churned table (the pruning trigger path).
        await cell.db.exec(`set enable_indexscan = off`)
        await cell.db.exec(`set enable_bitmapscan = off`)
        await cell.db.query(`select count(*) from hot`)
        // No WAL may have been written by the scan.
        expect(await cell.bookmark()).toBe(openLsn)
        const slice = await cell.captureSlice()
        expect(slice).toBeNull()
      }
      await cell.db.close()
    },
    TEST_TIMEOUT,
  )

  it(
    'suppression toggles off at runtime (write-upgrade parity)',
    async () => {
      const dir = freshDir('toggle')
      const cell = await Cell.open(dir, {
        expectedHeadLsn: bookmarkOf(dir),
        suppressReadWal: true,
      })
      // Turning it off restores vanilla pruning; a subsequent WRITE captures.
      cell.setSuppressReadWal(false)
      await cell.db.exec(`update hot set v = v + 1 where id = 1`)
      const slice = await cell.captureSlice()
      expect(slice).not.toBeNull()
      await cell.db.close()
    },
    TEST_TIMEOUT,
  )
})

/** The clean-shutdown head LSN of a settled datadir (checkpoint + aligned
 *  shutdown record), matching Cell.open's zero-boot-WAL expectation. */
function bookmarkOf(dir: string): bigint {
  return readControl(dir).checkPoint + BigInt(SHUTDOWN_CKPT_ALIGNED)
}
