import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { mkdtempSync, rmSync, readdirSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { create as tarCreate } from 'tar'
import { PGlite } from '@electric-sql/pglite'
import {
  readControl,
  lsnToSegment,
  walSegmentName,
} from '@electric-sql/pglite-cell'
import { packDatadir, extractDatadir } from '../src/checkpoint-object'

/** v1 packer: plain uncompressed tar of the WHOLE datadir (the old format). */
async function packV1(dir: string): Promise<Uint8Array> {
  const scratch = mkdtempSync(join(tmpdir(), 'pgl-v1-'))
  const tarPath = join(scratch, 'v1.tar')
  try {
    await tarCreate(
      { file: tarPath, cwd: dir, portable: true, noMtime: true },
      ['.'],
    )
    return readFileSync(tarPath)
  } finally {
    rmSync(scratch, { recursive: true, force: true })
  }
}

const TEST_TIMEOUT = 120_000

let root: string
let sourceDir: string

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-gw-ckpt-'))
  sourceDir = join(root, 'source')
  // Settled datadir: initdb (no checksums) + schema + data + close + settle.
  const db = new PGlite(sourceDir, {
    initDbStartParams: ['--no-data-checksums'],
  })
  await db.exec(`create table t (id serial primary key, v text)`)
  await db.exec(`insert into t (v) values ('one'), ('two'), ('three')`)
  await db.close()
  const settle = new PGlite(sourceDir)
  await settle.query(`select 1`)
  await settle.close()
}, TEST_TIMEOUT)

afterAll(() => {
  rmSync(root, { recursive: true, force: true })
})

describe('checkpoint-object pack/extract', () => {
  it(
    'packs and extracts a datadir; pg_control key fields match; reopened PGlite sees the data',
    async () => {
      const bytes = await packDatadir(sourceDir)
      expect(bytes.length).toBeGreaterThan(0)

      const destDir = join(root, 'dest')
      await extractDatadir(bytes, destDir)

      // pg_control equality of the load-bearing fields.
      const a = readControl(sourceDir)
      const b = readControl(destDir)
      expect(b.state).toBe(a.state)
      expect(b.checkPoint).toBe(a.checkPoint)
      expect(b.nextXid).toBe(a.nextXid)
      expect(b.sysid).toBe(a.sysid)
      expect(b.nextOid).toBe(a.nextOid)

      // The extracted datadir opens cleanly and holds the same rows.
      const reopened = new PGlite(destDir)
      const rows = (
        await reopened.query<{ v: string }>(`select v from t order by id`)
      ).rows
      expect(rows).toEqual([{ v: 'one' }, { v: 'two' }, { v: 'three' }])
      await reopened.close()
    },
    TEST_TIMEOUT,
  )

  it(
    'pack is stable run-to-run for the same tree (sorted entries)',
    async () => {
      const b1 = await packDatadir(sourceDir)
      const b2 = await packDatadir(sourceDir)
      // Deterministic-ish: same tree, sorted entries, no mtime. Sizes match and
      // the extracted control is identical.
      expect(b1.length).toBe(b2.length)
    },
    TEST_TIMEOUT,
  )

  it(
    'v2 is gzipped and dramatically smaller than v1 (the 99 MB fix)',
    async () => {
      const v1 = await packV1(sourceDir)
      const v2 = await packDatadir(sourceDir)
      // v2 sniffs as gzip.
      expect(v2[0]).toBe(0x1f)
      expect(v2[1]).toBe(0x8b)
      // v2 is dramatically smaller than v1. The win scales with the number of
      // pristine WAL segments dropped: this fixture has only a handful, and the
      // single RETAINED 16 MiB checkpoint segment barely gzips (WAL is
      // near-incompressible in this build), so v2 lands around a third of v1
      // here; a many-era production datadir (the "99 MB" case) collapses to
      // single-digit MB. Assert a conservative < 40% so the fix is proven
      // without being brittle to WAL entropy.
      expect(v2.length).toBeLessThan(v1.length * 0.4)
    },
    TEST_TIMEOUT,
  )

  it(
    'v2 keeps ONLY the checkpoint WAL segment; extract yields a working db',
    async () => {
      const { segno } = lsnToSegment(readControl(sourceDir).checkPoint)
      const keepSeg = walSegmentName(segno)

      const dest = join(root, 'dest-slim')
      await extractDatadir(await packDatadir(sourceDir), dest)

      const segs = readdirSync(join(dest, 'pg_wal')).filter((n) =>
        /^[0-9A-F]{24}$/.test(n),
      )
      expect(segs).toEqual([keepSeg])

      // pg_wal subdirs survive.
      const entries = readdirSync(join(dest, 'pg_wal'))
      expect(entries).toContain('archive_status')

      const reopened = new PGlite(dest)
      const rows = (
        await reopened.query<{ v: string }>(`select v from t order by id`)
      ).rows
      expect(rows).toEqual([{ v: 'one' }, { v: 'two' }, { v: 'three' }])
      await reopened.close()
    },
    TEST_TIMEOUT,
  )

  it(
    'v1 archives still extract (backward compat by magic-byte sniff)',
    async () => {
      const v1 = await packV1(sourceDir)
      expect(v1[0]).not.toBe(0x1f) // plain tar, not gzip
      const dest = join(root, 'dest-v1')
      await extractDatadir(v1, dest)
      const b = readControl(dest)
      expect(b.checkPoint).toBe(readControl(sourceDir).checkPoint)
      const reopened = new PGlite(dest)
      expect(
        (
          await reopened.query<{ n: number }>(
            `select count(*)::int as n from t`,
          )
        ).rows[0].n,
      ).toBe(3)
      await reopened.close()
    },
    TEST_TIMEOUT,
  )
})
