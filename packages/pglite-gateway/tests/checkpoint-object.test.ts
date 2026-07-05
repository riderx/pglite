import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import {
  mkdtempSync,
  rmSync,
  readdirSync,
  readFileSync,
  existsSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { create as tarCreate } from 'tar'
import { PGlite } from '@electric-sql/pglite'
import {
  readControl,
  lsnToSegment,
  walSegmentName,
} from '@electric-sql/pglite-cell'
import {
  packDatadir,
  extractDatadir,
  packDatadirV3,
  extractDatadirV3,
  extractCheckpoint,
  readCheckpointManifest,
  classifyDatadirFile,
} from '../src/checkpoint-object'
import { FsObjectStore } from '../src/object-store'

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

describe('checkpoint-object v3 (per-file content-addressed + manifest)', () => {
  it('classifies eager vs lazy per the fixed rules', () => {
    // Lazy: relation forks under base/<db>/ and global/.
    expect(classifyDatadirFile('base/5/1259')).toBe('lazy')
    expect(classifyDatadirFile('base/5/1259_vm')).toBe('lazy')
    expect(classifyDatadirFile('base/5/1259_fsm')).toBe('lazy')
    expect(classifyDatadirFile('base/5/1259_init')).toBe('lazy')
    expect(classifyDatadirFile('base/5/1259.1')).toBe('lazy')
    expect(classifyDatadirFile('global/1262')).toBe('lazy')
    expect(classifyDatadirFile('global/1262_vm')).toBe('lazy')
    // Eager: non-numeric basenames and non-relation dirs.
    expect(classifyDatadirFile('global/pg_control')).toBe('eager')
    expect(classifyDatadirFile('global/pg_filenode.map')).toBe('eager')
    expect(classifyDatadirFile('global/pg_internal.init')).toBe('eager')
    expect(classifyDatadirFile('base/5/pg_internal.init')).toBe('eager')
    expect(classifyDatadirFile('base/5/PG_VERSION')).toBe('eager')
    expect(classifyDatadirFile('PG_VERSION')).toBe('eager')
    expect(classifyDatadirFile('postgresql.conf')).toBe('eager')
    expect(classifyDatadirFile('pg_xact/0000')).toBe('eager')
    expect(classifyDatadirFile('pg_wal/000000010000000000000001')).toBe('eager')
  })

  it(
    'packs v3 and round-trips a full restore: control fields + rows intact',
    async () => {
      const store = new FsObjectStore(join(root, 'store-v3-rt'))
      const { manifestRef, files, eagerBytes, lazyBytes } = await packDatadirV3(
        sourceDir,
        store,
      )
      expect(manifestRef).toMatch(/^sha256:[0-9a-f]{64}$/)
      expect(files.length).toBeGreaterThan(0)
      expect(lazyBytes).toBeGreaterThan(0)
      expect(eagerBytes).toBeGreaterThan(0)
      // Manifest reads back with a v:3 shape and the same file list.
      const manifest = await readCheckpointManifest(manifestRef, store)
      expect(manifest.v).toBe(3)
      expect(manifest.files.length).toBe(files.length)

      const dest = join(root, 'dest-v3')
      const { lazyFiles } = await extractDatadirV3(manifestRef, store, dest)
      expect(lazyFiles.length).toBeGreaterThan(0)

      const a = readControl(sourceDir)
      const b = readControl(dest)
      expect(b.state).toBe(a.state)
      expect(b.checkPoint).toBe(a.checkPoint)
      expect(b.nextXid).toBe(a.nextXid)
      expect(b.sysid).toBe(a.sysid)
      expect(b.nextOid).toBe(a.nextOid)

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
    'lazySkip extract materializes eager skeleton only (no numeric relation files)',
    async () => {
      const store = new FsObjectStore(join(root, 'store-v3-skip'))
      const { manifestRef } = await packDatadirV3(sourceDir, store)
      const dest = join(root, 'dest-v3-skip')
      const { lazyFiles } = await extractDatadirV3(manifestRef, store, dest, {
        lazySkip: true,
      })
      // Every returned lazy entry classifies lazy and is NOT on disk.
      expect(lazyFiles.length).toBeGreaterThan(0)
      for (const f of lazyFiles) {
        expect(f.kind).toBe('lazy')
        expect(existsSync(join(dest, f.path))).toBe(false)
      }
      // Eager skeleton present: pg_control, an SLRU (pg_xact), the pg_wal
      // checkpoint segment.
      const { segno } = lsnToSegment(readControl(sourceDir).checkPoint)
      const keepSeg = walSegmentName(segno)
      expect(existsSync(join(dest, 'global', 'pg_control'))).toBe(true)
      expect(existsSync(join(dest, 'pg_xact'))).toBe(true)
      expect(existsSync(join(dest, 'pg_wal', keepSeg))).toBe(true)

      // NO numeric relation files landed under base/<db> or global.
      const numericRelation = (dir: string): string[] => {
        if (!existsSync(dir)) return []
        return readdirSync(dir).filter((n) =>
          /^[0-9]+(_(fsm|vm|init))?$/.test(n),
        )
      }
      expect(numericRelation(join(dest, 'global'))).toEqual([])
      const baseDb = join(dest, 'base')
      for (const db of existsSync(baseDb) ? readdirSync(baseDb) : []) {
        expect(numericRelation(join(baseDb, db))).toEqual([])
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'dedupes unchanged relation objects across two checkpoints of the same db',
    async () => {
      // Second datadir: same schema/data, then a tiny change (one extra row).
      const store = new FsObjectStore(join(root, 'store-v3-dedup'))
      const before = await packDatadirV3(sourceDir, store)
      const objCountAfterFirst = (await store.list()).length

      const dir2 = join(root, 'source-dedup')
      await extractDatadirV3(before.manifestRef, store, dir2)
      const db = new PGlite(dir2)
      await db.exec(`insert into t (v) values ('four')`)
      await db.close()
      const settle = new PGlite(dir2)
      await settle.query(`select 1`)
      await settle.close()

      const after = await packDatadirV3(dir2, store)
      const objCountAfterSecond = (await store.list()).length

      // A small, targeted delta: the changed heap fork + its vm/fsm + pg_control
      // + pg_wal segment + the new manifest — a handful of NEW objects, not the
      // whole datadir re-uploaded. The vast majority of ~catalog relation
      // objects are SHARED (identical bytes ⇒ identical refs).
      const delta = objCountAfterSecond - objCountAfterFirst
      expect(delta).toBeGreaterThan(0)
      expect(delta).toBeLessThan(30)
      // Sanity: far more files in a manifest than new objects (proves sharing).
      expect(after.files.length).toBeGreaterThan(delta * 3)

      // Count shared refs between the two manifests directly.
      const refs1 = new Set(before.files.map((f) => f.ref))
      const shared = after.files.filter((f) => refs1.has(f.ref)).length
      expect(shared).toBeGreaterThan(after.files.length - 30)
    },
    TEST_TIMEOUT,
  )

  it(
    'extractCheckpoint dispatches v1/v2/v3 from one entry point',
    async () => {
      const store = new FsObjectStore(join(root, 'store-dispatch'))
      // v3 by ref.
      const { manifestRef } = await packDatadirV3(sourceDir, store)
      const d3 = join(root, 'dispatch-v3')
      const r3 = await extractCheckpoint(manifestRef, d3, { store })
      expect(r3.lazyFiles.length).toBeGreaterThan(0)
      expect(readControl(d3).checkPoint).toBe(readControl(sourceDir).checkPoint)

      // v2 by raw bytes.
      const v2 = await packDatadir(sourceDir)
      const d2 = join(root, 'dispatch-v2')
      const r2 = await extractCheckpoint(v2, d2)
      expect(r2.lazyFiles).toEqual([])
      expect(readControl(d2).checkPoint).toBe(readControl(sourceDir).checkPoint)

      // v1 by raw bytes.
      const v1 = await packV1(sourceDir)
      const d1 = join(root, 'dispatch-v1')
      const r1 = await extractCheckpoint(v1, d1)
      expect(r1.lazyFiles).toEqual([])
      expect(readControl(d1).checkPoint).toBe(readControl(sourceDir).checkPoint)

      // v2/v3 archive object stored then extracted by ref.
      const v2Ref = (await store.put(v2)).ref
      const d2r = join(root, 'dispatch-v2-ref')
      await extractCheckpoint(v2Ref, d2r, { store })
      expect(readControl(d2r).checkPoint).toBe(
        readControl(sourceDir).checkPoint,
      )
    },
    TEST_TIMEOUT,
  )

  it(
    'packDatadir(format:3) returns the manifest ref bytes',
    async () => {
      const store = new FsObjectStore(join(root, 'store-fmt3'))
      const bytes = await packDatadir(sourceDir, { format: 3, store })
      const ref = new TextDecoder().decode(bytes)
      expect(ref).toMatch(/^sha256:[0-9a-f]{64}$/)
      const manifest = await readCheckpointManifest(ref, store)
      expect(manifest.v).toBe(3)
    },
    TEST_TIMEOUT,
  )
})
