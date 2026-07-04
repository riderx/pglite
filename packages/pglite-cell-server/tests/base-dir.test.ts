// BaseDirManager unit tests: read-attach materialize (publish nothing),
// advance past an already-diverged read base (the case that PANICs under a
// naive "lay slices onto the diverged dir" advance — see the module header
// of src/base-dir.ts), canonical ensure with a publish-race retry, and
// staging-swap atomicity (a failed materialize leaves every base intact).

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { mkdtempSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { PGlite } from '@electric-sql/pglite'
import {
  Cell,
  hydrateDatadir,
  readControl,
  SliceChainError,
  SHUTDOWN_CKPT_ALIGNED,
} from '@electric-sql/pglite-cell'
import type {
  CapturedSlice,
  CommitResult,
  CommitSliceInput,
  TailSlice,
} from '@electric-sql/pglite-cell'
import { BaseDirManager } from '../src/base-dir'
import type { SyncPublisher, TailView } from '../src/base-dir'

const TEST_TIMEOUT = 240_000

let root: string
let checkpointDir: string
let snapEnd: bigint
let slice1: CapturedSlice
let slice2: CapturedSlice

/** An in-memory TailView over pre-captured slices. */
class FakeTail implements TailView {
  readonly slices: TailSlice[] = []
  private lsn: bigint
  private offset: string

  constructor(baseLsn: bigint, baseOffset: string) {
    this.lsn = baseLsn
    this.offset = baseOffset
  }

  get head(): { offset: string; lsn: bigint } {
    return { offset: this.offset, lsn: this.lsn }
  }

  slicesSince(lsn: bigint): TailSlice[] {
    return this.slices.filter((s) => s.baseLsn >= lsn)
  }

  async catchUp(): Promise<number> {
    return 0
  }

  push(
    slice: { baseLsn: bigint; endLsn: bigint; bytes: Uint8Array },
    offset: string,
    kind: 'commit' | 'sync' | 'floors' = 'commit',
  ): void {
    this.slices.push({ ...slice, kind, commitId: `c-${offset}` })
    this.lsn = slice.endLsn
    this.offset = offset
  }
}

function newManager(dirName: string): { mgr: BaseDirManager; root: string } {
  const mgrRoot = join(root, dirName)
  const canonicalDir = join(mgrRoot, 'canonical-0')
  hydrateDatadir(checkpointDir, canonicalDir)
  const mgr = new BaseDirManager({
    root: mgrRoot,
    canonicalDir,
    canonicalLsn: snapEnd,
    canonicalOffset: 'off-0',
  })
  return { mgr, root: mgrRoot }
}

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-basedir-'))

  // Checkpoint 0: initdb (no data checksums) + schema + settling boot.
  checkpointDir = join(root, 'checkpoint')
  const db = new PGlite(checkpointDir, {
    initDbStartParams: ['--no-data-checksums'],
  })
  await db.exec(`create table t (id serial primary key, v text)`)
  await db.close()
  const settle = new PGlite(checkpointDir)
  await settle.query(`select 1`)
  await settle.close()
  snapEnd =
    readControl(checkpointDir).checkPoint + BigInt(SHUTDOWN_CKPT_ALIGNED)

  // A writer at the canonical position produces two contiguous slices.
  const workDir = join(root, 'writer')
  hydrateDatadir(checkpointDir, workDir)
  const cell = await Cell.open(workDir, { expectedHeadLsn: snapEnd })
  await cell.db.exec(`insert into t (v) values ('one')`)
  slice1 = (await cell.captureSlice())!
  cell.confirmPublished(slice1.endLsn)
  await cell.db.exec(`insert into t (v) values ('two')`)
  slice2 = (await cell.captureSlice())!
  cell.confirmPublished(slice2.endLsn)
  await cell.db.close()
}, TEST_TIMEOUT)

afterAll(() => {
  rmSync(root, { recursive: true, force: true })
})

describe('BaseDirManager', () => {
  it(
    'read-attach materializes locally, and advances PAST a diverged read base',
    async () => {
      const { mgr } = newManager('m1')
      const tail = new FakeTail(snapEnd, 'off-0')
      tail.push(slice1, 'off-1')

      const r1 = await mgr.ensureAtHeadLocal(tail)
      expect(r1.baseLsn).toBe(slice1.endLsn)
      // Local divergence: the materialize boot's own recovery/shutdown
      // records put the dir's clean position PAST the stream head.
      expect(r1.localHeadLsn).toBeGreaterThan(r1.baseLsn)

      const lease1 = await mgr.takeCellDir('read')
      expect(lease1.canonical).toBe(false)
      expect(lease1.base.lsn).toBe(slice1.endLsn)
      expect(lease1.base.localHeadLsn).toBe(r1.localHeadLsn)
      const cell1 = await Cell.open(lease1.dir, {
        expectedHeadLsn: lease1.base.localHeadLsn,
      })
      const rows1 = (await cell1.db.query<{ v: string }>(`select v from t`))
        .rows
      expect(rows1).toEqual([{ v: 'one' }])
      await cell1.db.close()
      mgr.releaseCellDir(lease1.dir)

      // Advance again: the read base is now DIVERGED. A naive incremental
      // advance (copy the read base + lay slice2 at its stream position)
      // overwrites its pg_control checkpoint anchor and PANICs — verified
      // empirically. The manager re-materializes from the canonical base.
      tail.push(slice2, 'off-2')
      const r2 = await mgr.ensureAtHeadLocal(tail)
      expect(r2.baseLsn).toBe(slice2.endLsn)

      const lease2 = await mgr.takeCellDir('read')
      const cell2 = await Cell.open(lease2.dir, {
        expectedHeadLsn: lease2.base.localHeadLsn,
      })
      const rows2 = (
        await cell2.db.query<{ v: string }>(`select v from t order by id`)
      ).rows
      expect(rows2).toEqual([{ v: 'one' }, { v: 'two' }])
      await cell2.db.close()
      mgr.releaseCellDir(lease2.dir)
      mgr.destroy()
    },
    TEST_TIMEOUT,
  )

  it(
    'swap atomicity: a failed staging materialize leaves every base intact',
    async () => {
      const { mgr, root: mgrRoot } = newManager('m2')
      const tail = new FakeTail(snapEnd, 'off-0')
      tail.push(slice1, 'off-1')
      await mgr.ensureAtHeadLocal(tail) // good read base at slice1

      // A corrupt continuation: correctly chained LSNs but zeroed bytes.
      // Recovery stops at the garbage, the shutdown checkpoint lands below
      // the claimed slice end, and materializeAtHead throws AFTER the
      // staging boot — the worst-placed failure for the swap.
      tail.push(
        {
          baseLsn: slice1.endLsn,
          endLsn: slice1.endLsn + 4096n,
          bytes: new Uint8Array(4096),
        },
        'off-corrupt',
      )
      await expect(mgr.ensureAtHeadLocal(tail)).rejects.toBeInstanceOf(
        SliceChainError,
      )

      // The previous read base survived untouched and still serves.
      const lease = await mgr.takeCellDir('read')
      expect(lease.base.lsn).toBe(slice1.endLsn)
      const cell = await Cell.open(lease.dir, {
        expectedHeadLsn: lease.base.localHeadLsn,
      })
      expect(
        (await cell.db.query<{ v: string }>(`select v from t`)).rows,
      ).toEqual([{ v: 'one' }])
      await cell.db.close()
      mgr.releaseCellDir(lease.dir)

      // No staging debris.
      const leftovers = readdirSync(mgrRoot).filter((d) =>
        d.startsWith('staging'),
      )
      expect(leftovers).toEqual([])
      mgr.destroy()
    },
    TEST_TIMEOUT,
  )

  it(
    'canonical ensure: publishes the sync slice, retries a lost CAS, and is idempotent at head',
    async () => {
      const { mgr } = newManager('m3')
      const tail = new FakeTail(snapEnd, 'off-0')
      tail.push(slice1, 'off-1')

      let calls = 0
      const committer: SyncPublisher = {
        async commitSlice(input: CommitSliceInput): Promise<CommitResult> {
          calls++
          if (calls === 1) return { landed: false } // forced publish race
          tail.push(
            {
              baseLsn: input.baseLsn,
              endLsn: input.endLsn,
              bytes: input.bytes,
            },
            `off-sync-${calls}`,
            input.kind,
          )
          return {
            landed: true,
            offset: tail.head.offset,
            nextOffset: `off-sync-${calls}`,
          }
        },
      }

      const res = await mgr.ensureAtHeadCanonical(tail, committer)
      expect(calls).toBe(2) // first attempt lost, second landed
      expect(res.lsn).toBe(tail.head.lsn) // canonical position == stream head

      const lease = await mgr.takeCellDir('write')
      expect(lease.canonical).toBe(true)
      expect(lease.base.lsn).toBe(lease.base.localHeadLsn)
      // Zero-boot-WAL must hold on the canonical base: the published sync
      // position is genuinely clean.
      const cell = await Cell.open(lease.dir, {
        expectedHeadLsn: lease.base.localHeadLsn,
      })
      expect(
        (await cell.db.query<{ v: string }>(`select v from t`)).rows,
      ).toEqual([{ v: 'one' }])
      await cell.db.close()
      mgr.releaseCellDir(lease.dir)

      // Already canonical at head: no further publish.
      const again = await mgr.ensureAtHeadCanonical(tail, committer)
      expect(calls).toBe(2)
      expect(again.lsn).toBe(res.lsn)
      mgr.destroy()
    },
    TEST_TIMEOUT,
  )
})
