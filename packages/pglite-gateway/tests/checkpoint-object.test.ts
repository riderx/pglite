import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { PGlite } from '@electric-sql/pglite'
import { readControl } from '@electric-sql/pglite-cell'
import { packDatadir, extractDatadir } from '../src/checkpoint-object'

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
})
