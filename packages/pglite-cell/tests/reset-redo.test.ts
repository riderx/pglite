// M5c exit tests: single-record redo live apply (no-FPI tails) + in-place
// reset to base. Stages of ONE scenario, run in order (vitest config: no
// parallelism), against a real embedded DurableStreamTestServer and real
// PGlite datadirs — with the convergence oracle (fresh full-stream
// materialize) as the golden reference.

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { DurableStreamTestServer } from '@durable-streams/server'
import { PGlite } from '@electric-sql/pglite'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import { DsStreamClient } from '../src/stream-client'
import { encodeAppend, INITIAL_OFFSET_TOKEN } from '../src/frames'
import type { OFrame } from '../src/frames'
import { formatLsn } from '../src/lsn'
import {
  readControl,
  writeWalRange,
  SHUTDOWN_CKPT_ALIGNED,
} from '../src/datadir'
import { EraTailer } from '../src/tail'
import { Committer } from '../src/committer'
import type { CommitResult } from '../src/committer'
import { materializeAtHead, hydrateDatadir } from '../src/materialize'
import { Cell, resetStats } from '../src/cell'
import { applyLiveTail, liveApplyStats } from '../src/live-apply'

const TEST_TIMEOUT = 120_000

const ERA_ORDINAL = 1
const eraId = `000001-${randomUUID().replace(/-/g, '').toUpperCase()}`
const eraPath = `/db/m5c/era/${eraId}`
const era = { path: eraPath, id: eraId, ordinal: ERA_ORDINAL }

let root: string
let checkpointDir: string
let snapEnd: bigint
let server: DurableStreamTestServer
let client: DsStreamClient
let eraBase: string

function assertLanded(
  r: CommitResult,
): asserts r is { landed: true; offset: string; nextOffset: string } {
  expect(r.landed).toBe(true)
}

function newTailer(): EraTailer {
  return new EraTailer(client, {
    path: eraPath,
    eraId,
    ordinal: ERA_ORDINAL,
    baseOffset: eraBase,
    baseLsn: snapEnd,
  })
}

/** Live-advance `cell` from `fromLsn` to the tailer's head IN PLACE. */
function liveAdvance(cell: Cell, tailer: EraTailer, fromLsn: bigint): bigint {
  const head = tailer.head
  const slices = tailer.slicesSince(fromLsn)
  expect(slices.length).toBeGreaterThan(0)
  for (const s of slices) writeWalRange(cell.dir, s.baseLsn, s.bytes)
  const res = applyLiveTail(cell.db, cell.dir, fromLsn, head.lsn)
  expect(res.applied).toBe(true)
  cell.advanceTo(head.lsn)
  return head.lsn
}

/** Fresh full-stream materialize + query (the convergence oracle). */
let oracleN = 0
async function oracle<T>(tailer: EraTailer, sql: string): Promise<T[]> {
  const dir = join(root, `oracle-${++oracleN}`)
  hydrateDatadir(checkpointDir, dir)
  const mat = await materializeAtHead({
    baseDir: dir,
    slices: tailer.slicesSince(snapEnd),
  })
  const cell = await Cell.open(dir, { expectedHeadLsn: mat.headLsn })
  const rows = (await cell.db.query<T>(sql)).rows
  await cell.db.close()
  return rows
}

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgcell-m5c-'))

  checkpointDir = join(root, 'checkpoint')
  const db = new PGlite(checkpointDir, {
    initDbStartParams: ['--no-data-checksums'],
  })
  await db.exec(`create table t (id int primary key, v text, cell text)`)
  await db.close()
  const settle = new PGlite(checkpointDir)
  await settle.query(`select 1`)
  await settle.close()
  snapEnd =
    readControl(checkpointDir).checkPoint + BigInt(SHUTDOWN_CKPT_ALIGNED)

  server = new DurableStreamTestServer({ port: 0, longPollTimeout: 500 })
  const url = await server.start()
  client = new DsStreamClient(url)
  const oFrame: OFrame = {
    type: 'O',
    header: {
      v: 1,
      eraId,
      expectedOffset: INITIAL_OFFSET_TOKEN,
      ordinal: ERA_ORDINAL,
      prevEraId: null,
      prevEraUrl: null,
      baseOffset: INITIAL_OFFSET_TOKEN,
      baseLsn: formatLsn(snapEnd),
      snapEnd: formatLsn(snapEnd),
      checkpointRef: 'local:' + checkpointDir,
    },
  }
  const created = await client.createStream(eraPath, {
    body: encodeAppend([oFrame]),
  })
  expect(created.created).toBe(true)
  eraBase = created.nextOffset
}, TEST_TIMEOUT)

afterAll(async () => {
  await server?.stop()
  rmSync(root, { recursive: true, force: true })
})

let tailerA: EraTailer
let committerA: Committer
let cellA: Cell
let posA: bigint // A's view of the stream head it sits at

let tailerB: EraTailer
let committerB: Committer
let cellB: Cell
let posB: bigint

async function landCommit(cell: Cell, committer: Committer): Promise<bigint> {
  const slice = await cell.captureSlice()
  expect(slice).not.toBeNull()
  const res = await committer.commitSlice({
    commitId: randomUUID(),
    kind: 'commit',
    ...slice!,
  })
  assertLanded(res)
  cell.confirmPublished(slice!.endLsn)
  return slice!.endLsn
}

describe('M5c: single-record redo live apply + in-place reset', () => {
  it(
    'A lands commits whose tails carry NO usable FPIs; B live-advances in place through the rm_redo harness and matches the oracle',
    async () => {
      const workA = join(root, 'workA')
      hydrateDatadir(checkpointDir, workA)
      tailerA = newTailer()
      expect(await tailerA.catchUp()).toBe(0)
      committerA = await Committer.create({
        client,
        era,
        tailer: tailerA,
        journalDir: join(root, 'journalA'),
      })
      cellA = await Cell.open(workA, { expectedHeadLsn: snapEnd })

      // Commit 1: first touch after the checkpoint — carries FPIs.
      await cellA.db.exec(`insert into t values (1, 'a1', 'A')`)
      posA = await landCommit(cellA, committerA)
      // Commits 2+3: SAME heap/btree pages, no intervening checkpoint —
      // their records carry NO full-page images. The M5b gate rejected
      // exactly this shape (the ~96% miss profile); the redo harness
      // applies it.
      await cellA.db.exec(`insert into t values (2, 'a2', 'A')`)
      posA = await landCommit(cellA, committerA)
      await cellA.db.exec(`insert into t values (3, 'a3', 'A')`)
      posA = await landCommit(cellA, committerA)

      // B attaches at snapEnd and live-advances across all three commits.
      const workB = join(root, 'workB')
      hydrateDatadir(checkpointDir, workB)
      tailerB = newTailer()
      await tailerB.catchUp()
      committerB = await Committer.create({
        client,
        era,
        tailer: tailerB,
        journalDir: join(root, 'journalB'),
      })
      cellB = await Cell.open(workB, { expectedHeadLsn: snapEnd })

      const hitsBefore = liveApplyStats.hits
      posB = liveAdvance(cellB, tailerB, snapEnd)
      expect(liveApplyStats.hits).toBe(hitsBefore + 1)
      expect(posB).toBe(posA)

      const got = (
        await cellB.db.query<{ v: string }>(`select v from t order by id`)
      ).rows
      expect(got).toEqual([{ v: 'a1' }, { v: 'a2' }, { v: 'a3' }])
      // Golden: the live-applied cell agrees with a fresh full-stream
      // materialize (the recovery-loop reference for the same records).
      expect(await oracle(tailerB, `select v from t order by id`)).toEqual(got)
    },
    TEST_TIMEOUT,
  )

  it(
    'B is a WRITE cell: after the in-place advance its next slice chains from the new head and lands (the M5b write-cell restriction, lifted)',
    async () => {
      await cellB.db.exec(`insert into t values (4, 'b1', 'B')`)
      const slice = await cellB.captureSlice()
      expect(slice).not.toBeNull()
      expect(slice!.baseLsn).toBe(posB) // chains from the advanced head
      const res = await committerB.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...slice!,
      })
      assertLanded(res)
      cellB.confirmPublished(slice!.endLsn)
      posB = slice!.endLsn

      expect(await oracle(tailerB, `select v from t order by id`)).toEqual([
        { v: 'a1' },
        { v: 'a2' },
        { v: 'a3' },
        { v: 'b1' },
      ])
    },
    TEST_TIMEOUT,
  )

  it(
    'in-place reset: A loses the race, resets to base WITHOUT recycling (speculative row + xids gone, landed state intact), live-advances, and its next commit chains',
    async () => {
      // A live-advances over B's commit first (stale after b1).
      await tailerA.catchUp()
      posA = liveAdvance(cellA, tailerA, posA)
      expect(posA).toBe(posB)

      // Snapshot sanity: base anchored at the cursor.
      expect(cellA.baseSnapshot?.lsn).toBe(posA)
      const baseXid = cellA.baseSnapshot!.nextXid

      // A speculates; B lands first; A's CAS loses server-side.
      await cellA.db.exec(`insert into t values (99, 'doomed', 'A')`)
      await cellB.db.exec(`insert into t values (5, 'b2', 'B')`)
      posB = await landCommit(cellB, committerB)

      const sliceA = await cellA.captureSlice()
      expect(sliceA).not.toBeNull()
      const res = await committerA.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...sliceA!,
      })
      expect(res).toEqual({ landed: false })

      // In-place reset instead of recycle.
      expect(cellA.canResetInPlace()).toBe(true)
      const inPlaceBefore = resetStats.inPlace
      cellA.resetToBase()
      expect(resetStats.inPlace).toBe(inPlaceBefore + 1)

      // Speculative state is gone; landed state is intact.
      const after = (
        await cellA.db.query<{ v: string }>(`select v from t order by id`)
      ).rows
      expect(after).toEqual([
        { v: 'a1' },
        { v: 'a2' },
        { v: 'a3' },
        { v: 'b1' },
      ])
      // Counters are exactly at base: the identity snapshot reads back
      // the rewound nextXid (crash-recovery-grade scope, §5.1).
      const mod = cellA.db.Module
      const ident = JSON.parse(mod.UTF8ToString(mod._pgl_get_identity())) as {
        nextXid: string
        insertLsn: string
      }
      expect(BigInt(ident.nextXid)).toBe(baseXid)
      expect(BigInt(ident.insertLsn)).toBe(posA)

      // Advance over the winner and commit again: the chain is unbroken.
      await tailerA.catchUp()
      posA = liveAdvance(cellA, tailerA, posA)
      expect(posA).toBe(posB)
      await cellA.db.exec(`insert into t values (6, 'a4', 'A')`)
      posA = await landCommit(cellA, committerA)

      // Convergence oracle: the loser's row never reached the stream; the
      // post-reset commit did.
      expect(
        await oracle(tailerA, `select v, cell from t order by id`),
      ).toEqual([
        { v: 'a1', cell: 'A' },
        { v: 'a2', cell: 'A' },
        { v: 'a3', cell: 'A' },
        { v: 'b1', cell: 'B' },
        { v: 'b2', cell: 'B' },
        { v: 'a4', cell: 'A' },
      ])
    },
    TEST_TIMEOUT,
  )

  it(
    'reset soundness gate: once storage writes escape shared memory (checkpoint after an aborted txn), canResetInPlace refuses and the caller must recycle',
    async () => {
      // Dirty a page speculatively, roll back, then force it to disk.
      await cellA.db.exec(`begin`)
      await cellA.db.exec(`insert into t values (98, 'never', 'A')`)
      await cellA.db.exec(`rollback`)
      await cellA.db.exec(`checkpoint`)
      expect(cellA.canResetInPlace()).toBe(false)

      await cellA.db.close()
      await cellB.db.close()
    },
    TEST_TIMEOUT,
  )
})
