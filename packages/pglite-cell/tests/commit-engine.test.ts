// M1a exit test: two solo cells racing through one raw Durable Streams era
// stream — the loser re-executes, dumps converge, and the stream tail obeys
// the capture-cursor invariant end to end. Runs against a real embedded
// DurableStreamTestServer (in-memory) and real PGlite datadirs.
//
// The tests in this file are stages of ONE scenario and run in order
// (vitest config: no parallelism), sharing fixture state.

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
import { readControl, SHUTDOWN_CKPT_ALIGNED } from '../src/datadir'
import { EraTailer } from '../src/tail'
import { Committer } from '../src/committer'
import type { CommitResult } from '../src/committer'
import { materializeAtHead, hydrateDatadir } from '../src/materialize'
import { Cell } from '../src/cell'
import { CaptureCursorError } from '../src/errors'

const TEST_TIMEOUT = 120_000

const ERA_ORDINAL = 1
const eraId = `000001-${randomUUID().replace(/-/g, '').toUpperCase()}`
const eraPath = `/db/m1a/era/${eraId}`
const era = { path: eraPath, id: eraId, ordinal: ERA_ORDINAL }

let root: string
let checkpointDir: string
let snapEnd: bigint
let server: DurableStreamTestServer
let client: DsStreamClient
let eraBase: string

function fmtXid8(n: bigint): string {
  return `${n >> 32n}:${n & 0xffffffffn}`
}

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

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgcell-m1a-'))

  // Checkpoint 0: initdb (no data checksums), schema, clean close.
  checkpointDir = join(root, 'checkpoint')
  const db = new PGlite(checkpointDir, {
    initDbStartParams: ['--no-data-checksums'],
  })
  await db.exec(`create table t (id serial primary key, v text, cell text)`)
  await db.close()
  // Settling boot (measured here, 2026-07): the FIRST reopen after initdb
  // writes ~8 KB of bootstrap WAL even with --no-data-checksums; every
  // subsequent plain reopen is WAL-silent (M0 finding 1). A checkpoint dir
  // must therefore be at least one reopen past initdb before its snapEnd
  // can serve as a zero-boot-WAL attach point.
  const settle = new PGlite(checkpointDir)
  await settle.query(`select 1`)
  await settle.close()
  snapEnd =
    readControl(checkpointDir).checkPoint + BigInt(SHUTDOWN_CKPT_ALIGNED)

  // Era stream: PUT with the O frame as the creating body.
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

// Shared scenario state, threaded through the ordered tests below.
let tailerA: EraTailer
let committerA: Committer
let cellA: Cell
let aEnd: bigint // A's first commit endLsn

let tailerB: EraTailer
let committerB: Committer
let cellB: Cell
let cellB2: Cell
let bHeadAfterSync: bigint
let bFinalEnd: bigint // stream head LSN after B's last commit

let detachEnd: bigint // stream head after A's detach slice
let expectedNextXid: bigint // last writer's local nextXid (from pg_control)

describe('commit engine M1a exit: racing solo cells over one era stream', () => {
  it(
    'A attaches at snapEnd and lands the first commit',
    async () => {
      const workA = join(root, 'workA')
      hydrateDatadir(checkpointDir, workA)
      tailerA = newTailer()
      expect(await tailerA.catchUp()).toBe(0) // fresh era: empty tail
      committerA = await Committer.create({
        client,
        era,
        tailer: tailerA,
        journalDir: join(root, 'journalA'),
      })
      expect(committerA.recovery.outcomes).toEqual([])

      // Plain open asserts config pins + zero boot WAL.
      cellA = await Cell.open(workA, { expectedHeadLsn: snapEnd })
      await cellA.db.exec(`insert into t (v, cell) values ('from-a', 'A')`)

      const slice = await cellA.captureSlice()
      expect(slice).not.toBeNull()
      expect(slice!.baseLsn).toBe(snapEnd)
      const res = await committerA.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...slice!,
      })
      assertLanded(res)
      expect(res.offset).toBe(eraBase)
      cellA.confirmPublished(slice!.endLsn)
      aEnd = slice!.endLsn

      // advanceLocal kept the tailer view consistent without re-downloading.
      expect(tailerA.head.lsn).toBe(aEnd)
      expect(tailerA.slices.length).toBe(1)
    },
    TEST_TIMEOUT,
  )

  it(
    'B racing at the stale base loses server-side (seq-conflict), then locally (contiguity)',
    async () => {
      const workB = join(root, 'workB')
      hydrateDatadir(checkpointDir, workB)
      tailerB = newTailer() // deliberately NOT caught up: stale view of the head
      committerB = await Committer.create({
        client,
        era,
        tailer: tailerB,
        journalDir: join(root, 'journalB'),
      })
      cellB = await Cell.open(workB, { expectedHeadLsn: snapEnd })
      await cellB.db.exec(`insert into t (v, cell) values ('doomed', 'B')`)
      const slice = await cellB.captureSlice()
      expect(slice!.baseLsn).toBe(snapEnd)

      // (i) The true server-side race: B's tailer still believes the head is
      // (eraBase, snapEnd), so the local contiguity assert passes and the
      // append goes out with the stale CAS token — the server rejects it.
      const res = await committerB.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...slice!,
      })
      expect(res).toEqual({ landed: false })
      // A definitive reject leaves nothing pending in B's journal.
      expect(committerB.journal.listPending()).toEqual([])

      // (ii) After catch-up the same stale slice is refused locally: the
      // capture-cursor invariant fires before any bytes reach the wire.
      expect(await tailerB.catchUp()).toBe(1)
      expect(tailerB.head.lsn).toBe(aEnd)
      await expect(
        committerB.commitSlice({
          commitId: randomUUID(),
          kind: 'commit',
          ...slice!,
        }),
      ).rejects.toBeInstanceOf(CaptureCursorError)

      // B abandons the speculative cell (its WAL never reaches the stream).
      await cellB.db.close()
    },
    TEST_TIMEOUT,
  )

  it(
    'B re-executes: materialize at head, publish the sync slice, land the commit',
    async () => {
      const workB2 = join(root, 'workB2')
      hydrateDatadir(checkpointDir, workB2)
      const slices = tailerB.slicesSince(snapEnd)
      expect(slices.length).toBe(1) // A's commit

      const mat = await materializeAtHead({ baseDir: workB2, slices })
      expect(mat.syncSlice).not.toBeNull()
      // Sync-slice contiguity: genuine boot WAL starts at the last slice end.
      expect(mat.syncSlice!.baseLsn).toBe(aEnd)
      expect(mat.syncSlice!.endLsn).toBe(mat.headLsn)

      // Step 4 of the attach algorithm: the sync slice is never skipped.
      const syncRes = await committerB.commitSlice({
        commitId: randomUUID(),
        kind: 'sync',
        ...mat.syncSlice!,
      })
      assertLanded(syncRes)
      expect(tailerB.head.lsn).toBe(mat.headLsn)
      bHeadAfterSync = mat.headLsn

      // Plain open at the new head (zero boot WAL) and re-execute B's txn.
      cellB2 = await Cell.open(workB2, { expectedHeadLsn: mat.headLsn })
      await cellB2.db.exec(`insert into t (v, cell) values ('from-b', 'B')`)
      const slice = await cellB2.captureSlice()
      expect(slice!.baseLsn).toBe(bHeadAfterSync)
      const res = await committerB.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...slice!,
      })
      assertLanded(res)
      cellB2.confirmPublished(slice!.endLsn)
      bFinalEnd = slice!.endLsn
    },
    TEST_TIMEOUT,
  )

  it(
    'cursor discipline: read-only work captures null; cursor advances only on landed commits',
    async () => {
      // Nothing new since the landed commit.
      expect(await cellB2.captureSlice()).toBeNull()

      // Read-only statements and transactions generate no slice.
      await cellB2.db.query(`select * from t`)
      await cellB2.db.exec(`begin; select count(*) from t; commit`)
      expect(await cellB2.captureSlice()).toBeNull()

      // A write produces a slice from the cursor; capture does not move the
      // cursor — only confirmPublished (after a landed append) does.
      await cellB2.db.exec(`insert into t (v, cell) values ('from-b-2', 'B')`)
      const s1 = await cellB2.captureSlice()
      expect(s1).not.toBeNull()
      expect(s1!.baseLsn).toBe(bFinalEnd)
      const again = await cellB2.captureSlice()
      expect(again!.baseLsn).toBe(s1!.baseLsn) // cursor unmoved

      const res = await committerB.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...s1!,
      })
      assertLanded(res)
      cellB2.confirmPublished(s1!.endLsn)
      bFinalEnd = s1!.endLsn

      // From the new cursor, nothing to capture until the next write.
      expect(await cellB2.captureSlice()).toBeNull()

      // B is done: plain close; its teardown WAL stays local and unpublished
      // (the stream head remains at bFinalEnd — stray WAL never leaks).
      await cellB2.db.close()
    },
    TEST_TIMEOUT,
  )

  it(
    'detach: A re-attaches at head, closes clean, publishes the detach slice',
    async () => {
      // A's original cell is stale (it never saw B's commits) — abandon it.
      await cellA.db.close()

      // Advance = recycle-with-materialize (M1): catch up, fresh workdir,
      // materialize, publish the sync slice, plain open at the head.
      await tailerA.catchUp()
      expect(tailerA.head.lsn).toBe(bFinalEnd)
      const workA2 = join(root, 'workA2')
      hydrateDatadir(checkpointDir, workA2)
      const mat = await materializeAtHead({
        baseDir: workA2,
        slices: tailerA.slicesSince(snapEnd),
      })
      const syncRes = await committerA.commitSlice({
        commitId: randomUUID(),
        kind: 'sync',
        ...mat.syncSlice!,
      })
      assertLanded(syncRes)
      const cellA2 = await Cell.open(workA2, { expectedHeadLsn: mat.headLsn })

      // Detach: clean close writes session-teardown WAL + a real shutdown
      // checkpoint; the (cursor .. checkPoint+120] bytes are the detach slice.
      const { detachSlice } = await cellA2.closeClean()
      expect(detachSlice).not.toBeNull()
      expect(detachSlice!.baseLsn).toBe(mat.headLsn)
      const control = readControl(workA2)
      expect(detachSlice!.endLsn).toBe(
        control.checkPoint + BigInt(SHUTDOWN_CKPT_ALIGNED),
      )
      const detachCommitId = randomUUID()
      const detachRes = await committerA.commitSlice({
        commitId: detachCommitId,
        kind: 'sync',
        ...detachSlice!,
      })
      assertLanded(detachRes)
      detachEnd = detachSlice!.endLsn
      expectedNextXid = control.nextXid // the last writer's chained identity

      // A fresh tailer sees the whole era; the head ends at the shutdown
      // record (headLsn === last slice endLsn === detach slice end).
      const tailer = newTailer()
      await tailer.catchUp()
      expect(tailer.head.lsn).toBe(detachEnd)
      const last = tailer.slices[tailer.slices.length - 1]
      expect(last.commitId).toBe(detachCommitId)
      expect(last.kind).toBe('sync')
      expect(last.endLsn).toBe(tailer.head.lsn)
    },
    TEST_TIMEOUT,
  )

  it(
    'convergence oracle v0: full-stream materialize converges; identity chains',
    async () => {
      // A third, never-before-seen cell: hydrate checkpoint 0, tail the whole
      // era, materialize every W slice (commits + syncs + detach), open.
      const oracleDir = join(root, 'oracle')
      hydrateDatadir(checkpointDir, oracleDir)
      const tailer = newTailer()
      await tailer.catchUp()
      const slices = tailer.slicesSince(snapEnd)
      // A commit, B sync, B commit, B commit2, A2 sync, A2 detach.
      expect(slices.length).toBe(6)
      expect(tailer.head.lsn).toBe(detachEnd)

      const mat = await materializeAtHead({ baseDir: oracleDir, slices })
      const cell = await Cell.open(oracleDir, { expectedHeadLsn: mat.headLsn })

      // Every landed row exactly once, in commit order; the doomed row from
      // B's lost race is nowhere.
      const rows = (
        await cell.db.query<{ v: string; cell: string }>(
          `select v, cell from t order by id`,
        )
      ).rows
      expect(rows).toEqual([
        { v: 'from-a', cell: 'A' },
        { v: 'from-b', cell: 'B' },
        { v: 'from-b-2', cell: 'B' },
      ])

      // Chained identity (M0-1 style): the oracle's nextXid equals the last
      // writer's local nextXid, parsed straight from its pg_control.
      const xid = (
        await cell.db.query<{ x: string }>(
          `select next_xid::text as x from pg_control_checkpoint()`,
        )
      ).rows[0].x
      expect(xid).toBe(fmtXid8(expectedNextXid))

      await cell.db.close()
    },
    TEST_TIMEOUT,
  )
})
