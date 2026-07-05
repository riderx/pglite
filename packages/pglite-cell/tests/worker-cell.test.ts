// M7 W1 exit tests: the worker-hosted cell + SAB fault bridge.
//
// (a) bridge unit tests — hostcall round-trip, concurrency under exec
//     traffic, host-side latency microbench (logged; target <1ms median);
// (b) Cell-parity — the essential cell lifecycle against WorkerCell, with
//     a twin Cell run on an identical datadir copy for comparison;
// (c) mini commit round-trip — WorkerCell + the existing Committer/EraTailer
//     against an embedded DS server (mirrors commit-engine stage 1);
// (d) terminate()/crash mid-flight leaves the host process healthy.

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { DurableStreamTestServer } from '@durable-streams/server'
import { PGlite, protocol } from '@electric-sql/pglite'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import { DsStreamClient } from '../src/stream-client'
import { encodeAppend, INITIAL_OFFSET_TOKEN } from '../src/frames'
import type { OFrame } from '../src/frames'
import { formatLsn } from '../src/lsn'
import { readControl, SHUTDOWN_CKPT_ALIGNED } from '../src/datadir'
import { hydrateDatadir } from '../src/materialize'
import { EraTailer } from '../src/tail'
import { Committer } from '../src/committer'
import type { CommitResult } from '../src/committer'
import { Cell } from '../src/cell'
import { WorkerCell } from '../src/worker-cell/worker-cell'

const TEST_TIMEOUT = 120_000

// The worker runs the TS entry via a tsx bootstrap (see worker-entry-dev.mjs);
// the built dist artifact path is exercised by the build gate, not here.
const workerUrl = new URL('./worker-entry-dev.mjs', import.meta.url)

let root: string
let checkpointDir: string
let snapEnd: bigint

function openWorkerCell(dir: string): Promise<WorkerCell> {
  return WorkerCell.open(dir, { expectedHeadLsn: snapEnd, workerUrl })
}

function assertLanded(
  r: CommitResult,
): asserts r is { landed: true; offset: string; nextOffset: string } {
  expect(r.landed).toBe(true)
}

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgcell-m7w1-'))
  // Settled checkpoint dir: initdb + schema + clean close + one settling
  // reopen (M0 finding 1 — same recipe as commit-engine.test.ts).
  checkpointDir = join(root, 'checkpoint')
  const db = new PGlite(checkpointDir, {
    initDbStartParams: ['--no-data-checksums'],
  })
  await db.exec(`create table t (id serial primary key, v text, cell text)`)
  await db.close()
  const settle = new PGlite(checkpointDir)
  await settle.query(`select 1`)
  await settle.close()
  snapEnd =
    readControl(checkpointDir).checkPoint + BigInt(SHUTDOWN_CKPT_ALIGNED)
}, TEST_TIMEOUT)

afterAll(() => {
  rmSync(root, { recursive: true, force: true })
})

describe('SAB fault bridge (a)', () => {
  it(
    'hostcall ping round-trips under a latency budget',
    async () => {
      const dir = join(root, 'bridge-ping')
      hydrateDatadir(checkpointDir, dir)
      const cell = await openWorkerCell(dir)
      try {
        // Warm-up, then the measured run.
        await cell.hostcallBurst({ count: 50, payloadBytes: 1024 })
        const res = await cell.hostcallBurst({ count: 500, payloadBytes: 1024 })
        expect(res.verified).toBe(true)
        expect(res.count).toBe(500)
        const ms = (ns: number): string => (ns / 1e6).toFixed(3)
        console.log(
          `[bridge] ping 1KiB x500: median=${ms(res.medianNs)}ms ` +
            `mean=${ms(res.meanNs)}ms p99=${ms(res.p99Ns)}ms max=${ms(res.maxNs)}ms`,
        )
        // Target <1ms median; hard gate is looser to avoid CI flake.
        expect(res.medianNs).toBeLessThan(20e6)
      } finally {
        await cell.terminate()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'generic hostcalls dispatch to host handlers; unknown keys fail cleanly',
    async () => {
      const dir = join(root, 'bridge-hostcall')
      hydrateDatadir(checkpointDir, dir)
      const cell = await openWorkerCell(dir)
      try {
        const seenParams: number[][] = []
        cell.onHostcall(7, (payload, params) => {
          seenParams.push([...params])
          return payload // echo (the burst verifier expects it)
        })
        const ok = await cell.hostcallBurst({
          count: 8,
          payloadBytes: 64,
          key: 7,
          ping: false,
        })
        expect(ok.verified).toBe(true)
        expect(seenParams.length).toBe(8)
        expect(seenParams[3]).toEqual([7, 3, 0, 0])

        // Unregistered key: non-OK status surfaces as verified=false, and
        // the bridge stays serviceable afterwards.
        const bad = await cell.hostcallBurst({
          count: 2,
          payloadBytes: 16,
          key: 99,
          ping: false,
        })
        expect(bad.verified).toBe(false)
        const again = await cell.hostcallBurst({ count: 2, payloadBytes: 16 })
        expect(again.verified).toBe(true)

        // Async handlers hold the worker blocked until they resolve.
        cell.onHostcall(8, async (payload) => {
          await new Promise((r) => setTimeout(r, 5))
          return payload
        })
        const slow = await cell.hostcallBurst({
          count: 3,
          payloadBytes: 8,
          key: 8,
          ping: false,
        })
        expect(slow.verified).toBe(true)
        expect(slow.medianNs).toBeGreaterThan(4e6)
      } finally {
        await cell.terminate()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'bridge traffic and exec traffic interleave without loss',
    async () => {
      const dir = join(root, 'bridge-mixed')
      hydrateDatadir(checkpointDir, dir)
      const cell = await openWorkerCell(dir)
      try {
        const bursts = (async () => {
          const results = []
          for (let i = 0; i < 5; i++) {
            results.push(
              await cell.hostcallBurst({ count: 50, payloadBytes: 4096 }),
            )
          }
          return results
        })()
        const queries = (async () => {
          let sum = 0
          for (let i = 0; i < 25; i++) {
            const r = await cell.db.query<{ n: number }>(
              `select ${i}::int as n`,
            )
            sum += r.rows[0].n
          }
          return sum
        })()
        const [burstResults, sum] = await Promise.all([bursts, queries])
        expect(sum).toBe(300)
        for (const b of burstResults) expect(b.verified).toBe(true)
      } finally {
        await cell.terminate()
      }
    },
    TEST_TIMEOUT,
  )
})

describe('Cell parity (b)', () => {
  it(
    'runs the essential cell lifecycle with a twin Cell for comparison',
    async () => {
      const dirW = join(root, 'parity-worker')
      const dirC = join(root, 'parity-cell')
      hydrateDatadir(checkpointDir, dirW)
      hydrateDatadir(checkpointDir, dirC)

      // Open asserts: pins + zero boot WAL (a wrong head must throw). Own
      // dir: the error path clean-closes, which itself writes shutdown WAL.
      const dirBad = join(root, 'parity-badhead')
      hydrateDatadir(checkpointDir, dirBad)
      await expect(
        WorkerCell.open(dirBad, { expectedHeadLsn: snapEnd + 8n, workerUrl }),
      ).rejects.toThrow(/insert LSN|boot/i)

      const wc = await openWorkerCell(dirW)
      const cc = await Cell.open(dirC, { expectedHeadLsn: snapEnd })
      expect(wc.captureCursor).toBe(snapEnd)
      expect((await wc.db.query(`show wal_level`)).rows).toEqual(
        (await cc.db.query(`show wal_level`)).rows,
      )

      // Identical write txn on both.
      const sql = `insert into t (v, cell) values ('parity', 'X')`
      await wc.db.exec(sql)
      await cc.db.exec(sql)

      const sw = await wc.captureSlice()
      const sc = await cc.captureSlice()
      expect(sw).not.toBeNull()
      expect(sw!.baseLsn).toBe(snapEnd)
      // Byte-identical modulo the commit-record timestamp: assert the LSN
      // geometry and length match exactly (timestamps preclude full
      // byte-equality across two separate executions).
      expect(sw!.endLsn).toBe(sc!.endLsn)
      expect(sw!.bytes.length).toBe(sc!.bytes.length)

      // Exec-unit path (the wire-protocol surface session code uses).
      const chunks: Uint8Array[] = []
      await wc.db.runExclusive(() =>
        wc.db.execProtocolRawStream(
          protocol.serialize.query(`select count(*)::int as n from t`),
          { onRawData: (d) => chunks.push(d) },
        ),
      )
      expect(chunks.length).toBeGreaterThan(0)
      expect(wc.db.isInTransaction()).toBe(false)

      // M5 members: read set, page LSN, nblocks, commit gate, leases.
      await wc.readSetBegin()
      await wc.db.query(`select * from t`)
      // Harvest BEFORE disabling — mirrors session.ts order.
      const rs = await wc.readSetSnapshot()
      await wc.readSetEnd()
      expect(rs.overflowed).toBe(false)
      expect(rs.pins.length).toBeGreaterThan(0)
      const pin = rs.pins[0]
      const lsnW = await wc.pageLsn(pin.spc, pin.db, pin.rel, pin.fork, pin.blk)
      expect(lsnW).toBeGreaterThan(0n)
      expect(
        await wc.relationNblocks(pin.spc, pin.db, pin.rel, pin.fork),
      ).toBeGreaterThan(0)
      expect(await wc.commitGatePending()).toBe(0)
      await wc.setSequenceLease(0, 0n)
      await wc.clearSequenceLeases()
      await wc.resetSequenceCaches()

      // confirmPublished + base snapshot + in-place reset soundness gate.
      wc.confirmPublished(sw!.endLsn)
      await wc.settled()
      expect(wc.baseSnapshot).not.toBeNull()
      expect(wc.baseSnapshot!.lsn).toBe(sw!.endLsn)
      expect(await wc.canResetInPlace()).toBe(true)
      await wc.resetToBase()
      expect(await wc.bookmark()).toBe(sw!.endLsn)

      // closeClean: detach slices from both cells match in geometry.
      cc.confirmPublished(sc!.endLsn)
      const dw = await wc.closeClean()
      const dc = await cc.closeClean()
      expect(dw.detachSlice).not.toBeNull()
      expect(dw.detachSlice!.baseLsn).toBe(sw!.endLsn)
      expect(dw.detachSlice!.endLsn - dw.detachSlice!.baseLsn).toBe(
        dc.detachSlice!.endLsn - dc.detachSlice!.baseLsn,
      )

      // Reopen the worker-cell datadir plainly at the shutdown head — the
      // zero-boot-WAL invariant must hold again (clean close proof).
      const reopenHead = dw.detachSlice!.endLsn
      const wc2 = await WorkerCell.open(dirW, {
        expectedHeadLsn: reopenHead,
        workerUrl,
      })
      const rows = await wc2.db.query<{ v: string }>(`select v from t`)
      expect(rows.rows).toEqual([{ v: 'parity' }])
      expect((await wc2.closeClean()).detachSlice).not.toBeNull()
    },
    TEST_TIMEOUT,
  )
})

describe('mini commit round-trip (c)', () => {
  it(
    'WorkerCell + Committer/EraTailer land a commit on an embedded DS server',
    async () => {
      const dir = join(root, 'commit-work')
      hydrateDatadir(checkpointDir, dir)
      const eraId = `000001-${randomUUID().replace(/-/g, '').toUpperCase()}`
      const eraPath = `/db/m7w1/era/${eraId}`
      const era = { path: eraPath, id: eraId, ordinal: 1 }
      const server = new DurableStreamTestServer({
        port: 0,
        longPollTimeout: 500,
      })
      const url = await server.start()
      try {
        const client = new DsStreamClient(url)
        const oFrame: OFrame = {
          type: 'O',
          header: {
            v: 1,
            eraId,
            expectedOffset: INITIAL_OFFSET_TOKEN,
            ordinal: 1,
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

        const tailer = new EraTailer(client, {
          path: eraPath,
          eraId,
          ordinal: 1,
          baseOffset: created.nextOffset,
          baseLsn: snapEnd,
        })
        expect(await tailer.catchUp()).toBe(0)
        const committer = await Committer.create({
          client,
          era,
          tailer,
          journalDir: join(root, 'commit-journal'),
        })

        const cell = await openWorkerCell(dir)
        await cell.db.exec(`insert into t (v, cell) values ('via-worker', 'W')`)
        const slice = await cell.captureSlice()
        expect(slice).not.toBeNull()
        expect(slice!.baseLsn).toBe(snapEnd)
        const res = await committer.commitSlice({
          commitId: randomUUID(),
          kind: 'commit',
          ...slice!,
        })
        assertLanded(res)
        cell.confirmPublished(slice!.endLsn)
        await cell.settled()
        expect(tailer.head.lsn).toBe(slice!.endLsn)
        expect(cell.captureCursor).toBe(slice!.endLsn)

        // Detach publishes the shutdown slice, closing the loop.
        const { detachSlice } = await cell.closeClean()
        expect(detachSlice).not.toBeNull()
        const res2 = await committer.commitSlice({
          commitId: randomUUID(),
          kind: 'sync',
          ...detachSlice!,
        })
        assertLanded(res2)
        expect(tailer.head.lsn).toBe(detachSlice!.endLsn)
      } finally {
        await server.stop()
      }
    },
    TEST_TIMEOUT,
  )
})

describe('hard-kill safety (d)', () => {
  it(
    'terminate() mid-query rejects in-flight work and leaves the host healthy',
    async () => {
      const dir = join(root, 'kill-terminate')
      hydrateDatadir(checkpointDir, dir)
      const cell = await openWorkerCell(dir)
      const inFlight = cell.db.query(
        `select count(*) from generate_series(1, 200000000)`,
      )
      await new Promise((r) => setTimeout(r, 200))
      await cell.terminate()
      await expect(inFlight).rejects.toThrow(/terminated|exited/)
      // Host process is fine: a fresh worker cell on a fresh copy works.
      const dir2 = join(root, 'kill-after')
      hydrateDatadir(checkpointDir, dir2)
      const cell2 = await openWorkerCell(dir2)
      expect(
        (await cell2.db.query<{ n: number }>(`select 1 as n`)).rows,
      ).toEqual([{ n: 1 }])
      await cell2.terminate()
    },
    TEST_TIMEOUT,
  )

  it(
    'crash-simulate kills the worker thread only; pending requests reject',
    async () => {
      const dir = join(root, 'kill-crash')
      hydrateDatadir(checkpointDir, dir)
      const cell = await openWorkerCell(dir)
      // A CPU-bound unit can't be preempted by a message (the worker's
      // event loop is busy) — that is terminate()'s job. crash-simulate
      // models the worker dying between units: the crash message lands
      // first, the queued query never gets an answer.
      cell.crashSimulate()
      const pending = cell.db.query(`select 1`)
      await expect(pending).rejects.toThrow(/exited|terminated/)
      await cell.terminate()
    },
    TEST_TIMEOUT,
  )
})
