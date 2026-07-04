// §3.8 commit recovery: fence-then-read against a real embedded Durable
// Streams server. Each test uses its own era stream + journal dir; slice
// bytes are synthetic (recovery decides from frames, not WAL validity).

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { DurableStreamTestServer } from '@durable-streams/server'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createHash, randomUUID } from 'node:crypto'
import { DsStreamClient } from '../src/stream-client'
import {
  casToken,
  encodeAppend,
  INITIAL_OFFSET_TOKEN,
  PositionCheckedReader,
} from '../src/frames'
import type { OFrame, WFrame } from '../src/frames'
import { parseLsn, formatLsn } from '../src/lsn'
import { CommitJournal } from '../src/journal'
import type { JournalEntry } from '../src/journal'
import { EraTailer } from '../src/tail'
import { Committer } from '../src/committer'

const BASE_LSN = parseLsn('0/1000000')

let root: string
let server: DurableStreamTestServer
let client: DsStreamClient

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgcell-journal-'))
  server = new DurableStreamTestServer({ port: 0, longPollTimeout: 500 })
  client = new DsStreamClient(await server.start())
})

afterAll(async () => {
  await server?.stop()
  rmSync(root, { recursive: true, force: true })
})

let counter = 0

/** Create a fresh era stream (O-frame PUT) and return its coordinates. */
async function freshEra(): Promise<{
  path: string
  eraId: string
  base: string
}> {
  const eraId = `000001-${randomUUID().replace(/-/g, '').toUpperCase()}`
  const path = `/db/journal-${counter++}/era/${eraId}`
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
      baseLsn: formatLsn(BASE_LSN),
      snapEnd: formatLsn(BASE_LSN),
      checkpointRef: 'local:none',
    },
  }
  const created = await client.createStream(path, {
    body: encodeAppend([oFrame]),
  })
  return { path, eraId, base: created.nextOffset }
}

/** Build the exact W-frame append body a committer would POST. */
function buildW(
  eraId: string,
  expectedOffset: string,
  commitId: string,
  baseLsn: bigint,
  payload: string,
): { body: Uint8Array; sliceHash: string; endLsn: bigint } {
  const wal = new TextEncoder().encode(payload)
  const sliceHash = 'sha256:' + createHash('sha256').update(wal).digest('hex')
  const endLsn = baseLsn + BigInt(wal.length)
  const frame: WFrame = {
    type: 'W',
    header: {
      v: 1,
      eraId,
      expectedOffset,
      commitId,
      kind: 'commit',
      baseLsn: formatLsn(baseLsn),
      endLsn: formatLsn(endLsn),
      sliceHash,
    },
    wal,
  }
  return { body: encodeAppend([frame]), sliceHash, endLsn }
}

function entryFor(opts: {
  commitId: string
  eraId: string
  eraPath: string
  expectedOffset: string
  sliceHash: string
  endLsn: bigint
  producerId: string
  epoch: number
  seq: number
}): JournalEntry {
  return {
    commitId: opts.commitId,
    eraId: opts.eraId,
    eraPath: opts.eraPath,
    eraOrdinal: 1,
    expectedOffset: opts.expectedOffset,
    casToken: casToken(1, opts.expectedOffset),
    baseLsn: formatLsn(BASE_LSN),
    endLsn: formatLsn(opts.endLsn),
    sliceHash: opts.sliceHash,
    producerId: opts.producerId,
    producerEpoch: opts.epoch,
    producerSeq: opts.seq,
    fenceEpoch: opts.epoch,
  }
}

describe('§3.8 journal recovery: fence-then-read', () => {
  it('(a) journaled but never POSTed ⇒ LOST, with a CAS fence at the tail', async () => {
    const { path, eraId, base } = await freshEra()
    const journal = new CommitJournal(join(root, 'j-a'))
    const pid = randomUUID()
    journal.writeMeta({ producerId: pid, epoch: 5 })

    const commitId = randomUUID()
    const w = buildW(eraId, base, commitId, BASE_LSN, 'never-posted')
    journal.record(
      entryFor({
        commitId,
        eraId,
        eraPath: path,
        expectedOffset: base,
        sliceHash: w.sliceHash,
        endLsn: w.endLsn,
        producerId: pid,
        epoch: 5,
        seq: 0,
      }),
    )

    const report = await journal.recover(client)
    expect(report.outcomes).toEqual([
      { commitId, outcome: 'lost', selfFenced: false, fenceEpoch: 6 },
    ])
    expect(report.epochFloor).toBe(7)
    expect(journal.listPending()).toEqual([]) // lost ⇒ resolved
    expect(journal.readMeta()).toEqual({ producerId: pid, epoch: 6 })

    // A '0' fence frame sits at the (old) tail, position-checked.
    const read = await client.read(path, { offset: base })
    const reader = new PositionCheckedReader(base)
    const groups = [...reader.feed(read.bytes)]
    expect(groups.length).toBe(1)
    expect(groups[0].offset).toBe(base)
    expect(groups[0].frames[0].type).toBe('0')
    expect(groups[0].frames[0].header).toEqual({
      v: 1,
      eraId,
      expectedOffset: base,
    })
    reader.expectBoundary(read.nextOffset)

    // The fence armed the bumped epoch: the crashed incarnation's epoch is
    // now stale server-side.
    const stale = await client.append(path, w.body, {
      producer: { id: pid, epoch: 5, seq: 0 },
    })
    expect(stale).toEqual({ kind: 'stale-epoch', currentEpoch: 6 })
  })

  it('(b) POSTed but unresolved (crash before ack) ⇒ LANDED at the journaled offset', async () => {
    const { path, eraId, base } = await freshEra()
    const journal = new CommitJournal(join(root, 'j-b'))
    const pid = randomUUID()
    journal.writeMeta({ producerId: pid, epoch: 3 })

    const commitId = randomUUID()
    const w = buildW(eraId, base, commitId, BASE_LSN, 'landed-before-crash')
    journal.record(
      entryFor({
        commitId,
        eraId,
        eraPath: path,
        expectedOffset: base,
        sliceHash: w.sliceHash,
        endLsn: w.endLsn,
        producerId: pid,
        epoch: 3,
        seq: 0,
      }),
    )
    // The POST that the crash hid from us:
    const posted = await client.append(path, w.body, {
      seq: casToken(1, base),
      expectedOffset: base,
      producer: { id: pid, epoch: 3, seq: 0 },
    })
    expect(posted.kind).toBe('ok')

    const report = await journal.recover(client)
    expect(report.outcomes).toEqual([
      { commitId, outcome: 'landed', selfFenced: false, fenceEpoch: 4 },
    ])
    expect(report.epochFloor).toBe(5)
    expect(journal.listPending()).toEqual([])
  })

  it('(c) recovery fences the old epoch; a new Committer starts above it and commits', async () => {
    const { path, eraId, base } = await freshEra()
    const journalDir = join(root, 'j-c')
    const journal = new CommitJournal(journalDir)
    const pid = randomUUID()
    journal.writeMeta({ producerId: pid, epoch: 2 })

    const commitId = randomUUID()
    const w = buildW(eraId, base, commitId, BASE_LSN, 'lost-to-a-crash')
    journal.record(
      entryFor({
        commitId,
        eraId,
        eraPath: path,
        expectedOffset: base,
        sliceHash: w.sliceHash,
        endLsn: w.endLsn,
        producerId: pid,
        epoch: 2,
        seq: 0,
      }),
    )
    const report = await journal.recover(client)
    expect(report.outcomes[0].outcome).toBe('lost')
    expect(report.outcomes[0].fenceEpoch).toBe(3)

    // An append using the OLD epoch is fenced.
    const stale = await client.append(path, w.body, {
      producer: { id: pid, epoch: 2, seq: 1 },
    })
    expect(stale).toEqual({ kind: 'stale-epoch', currentEpoch: 3 })

    // A new incarnation on the same journal dir starts above the fence
    // epoch and can commit (the tailer sees + ignores the fence frame).
    const tailer = new EraTailer(client, {
      path,
      eraId,
      ordinal: 1,
      baseOffset: base,
      baseLsn: BASE_LSN,
    })
    await tailer.catchUp()
    expect(tailer.head.lsn).toBe(BASE_LSN) // '0' fences advance no WAL
    const committer = await Committer.create({
      client,
      era: { path, id: eraId, ordinal: 1 },
      tailer,
      journalDir,
    })
    expect(committer.producerId).toBe(pid)
    expect(committer.epoch).toBe(4)
    expect(committer.recovery.outcomes).toEqual([]) // already recovered

    const bytes = new TextEncoder().encode('re-executed-txn')
    const res = await committer.commitSlice({
      commitId: randomUUID(),
      kind: 'commit',
      baseLsn: tailer.head.lsn,
      endLsn: tailer.head.lsn + BigInt(bytes.length),
      bytes,
    })
    expect(res.landed).toBe(true)
  })

  it('(d) multiple pending entries are decided independently', async () => {
    const { path, eraId, base } = await freshEra()
    const journal = new CommitJournal(join(root, 'j-d'))
    const pid = randomUUID()
    journal.writeMeta({ producerId: pid, epoch: 1 })

    // Entry 1: journaled and POSTed (landed), never resolved.
    const commitId1 = randomUUID()
    const w1 = buildW(eraId, base, commitId1, BASE_LSN, 'first-landed')
    journal.record(
      entryFor({
        commitId: commitId1,
        eraId,
        eraPath: path,
        expectedOffset: base,
        sliceHash: w1.sliceHash,
        endLsn: w1.endLsn,
        producerId: pid,
        epoch: 1,
        seq: 0,
      }),
    )
    const posted = await client.append(path, w1.body, {
      seq: casToken(1, base),
      expectedOffset: base,
      producer: { id: pid, epoch: 1, seq: 0 },
    })
    expect(posted.kind).toBe('ok')
    const tail1 = (posted as { nextOffset: string }).nextOffset

    // Entry 2: journaled at the post-entry-1 tail, never POSTed.
    const commitId2 = randomUUID()
    const w2 = buildW(eraId, tail1, commitId2, w1.endLsn, 'second-lost')
    journal.record(
      entryFor({
        commitId: commitId2,
        eraId,
        eraPath: path,
        expectedOffset: tail1,
        sliceHash: w2.sliceHash,
        endLsn: w2.endLsn,
        producerId: pid,
        epoch: 1,
        seq: 1,
      }),
    )

    const report = await journal.recover(client)
    expect(report.outcomes).toEqual([
      {
        commitId: commitId1,
        outcome: 'landed',
        selfFenced: false,
        fenceEpoch: 2,
      },
      {
        commitId: commitId2,
        outcome: 'lost',
        selfFenced: false,
        fenceEpoch: 3,
      },
    ])
    expect(report.epochFloor).toBe(4)
    expect(journal.listPending()).toEqual([])
    expect(journal.readMeta()).toEqual({ producerId: pid, epoch: 3 })
  })

  it('(e) stream gone ⇒ INDETERMINATE, entry stays pending', async () => {
    const journal = new CommitJournal(join(root, 'j-e'))
    const pid = randomUUID()
    journal.writeMeta({ producerId: pid, epoch: 1 })
    const commitId = randomUUID()
    const eraId = `000001-${randomUUID().replace(/-/g, '').toUpperCase()}`
    const w = buildW(eraId, INITIAL_OFFSET_TOKEN, commitId, BASE_LSN, 'gone')
    journal.record(
      entryFor({
        commitId,
        eraId,
        eraPath: '/db/never-created/era/' + eraId,
        expectedOffset: INITIAL_OFFSET_TOKEN,
        sliceHash: w.sliceHash,
        endLsn: w.endLsn,
        producerId: pid,
        epoch: 1,
        seq: 0,
      }),
    )
    const report = await journal.recover(client)
    expect(report.outcomes).toEqual([
      { commitId, outcome: 'indeterminate', selfFenced: false, fenceEpoch: 2 },
    ])
    // Indeterminate is never resolved silently: the entry stays pending
    // (and the fence epoch bump was journaled durably).
    const pending = journal.listPending()
    expect(pending.length).toBe(1)
    expect(pending[0].fenceEpoch).toBe(2)
  })
})
