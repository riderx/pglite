// M2a exit tests: era rotation on the CELL side — tailer era-hop through the
// S→O chain (O/S mirror verification, wedge detection), fork-prefix eraId
// tolerance, and the committer's closed→hop→re-CAS path. Multi-era fixtures
// are built BY HAND with DsStreamClient against an embedded
// DurableStreamTestServer — no rotator involved (that's M2c).

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
  FramePositionError,
  INITIAL_OFFSET_TOKEN,
  PositionCheckedReader,
} from '../src/frames'
import type {
  FenceFrame,
  OFrame,
  SFrame,
  WFrame,
  WFrameHeader,
} from '../src/frames'
import { formatLsn } from '../src/lsn'
import { EraTailer } from '../src/tail'
import { Committer } from '../src/committer'
import { EraChainError, WedgedEraError } from '../src/errors'

const TEST_TIMEOUT = 30_000

/** Synthetic era-base LSN (any aligned-looking value works — the tailer only
 *  checks chaining, never WAL content). */
const L0 = 0x1000000n

let root: string
let server: DurableStreamTestServer
let client: DsStreamClient

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgcell-m2a-'))
  server = new DurableStreamTestServer({ port: 0, longPollTimeout: 500 })
  client = new DsStreamClient(await server.start())
})

afterAll(async () => {
  await server?.stop()
  rmSync(root, { recursive: true, force: true })
})

// ---------------------------------------------------------------------------
// Hand-built fixture helpers
// ---------------------------------------------------------------------------

interface EraRef {
  id: string
  ordinal: number
  path: string
}

function sha256Hex(bytes: Uint8Array): string {
  return 'sha256:' + createHash('sha256').update(bytes).digest('hex')
}

function era(scenario: string, ordinal: number): EraRef {
  const id = `${String(ordinal).padStart(6, '0')}-${randomUUID()
    .replace(/-/g, '')
    .toUpperCase()}`
  return { id, ordinal, path: `/db/${scenario}/era/${id}` }
}

function oFrame(e: EraRef, prev: EraRef | null, baseLsn: bigint): OFrame {
  return {
    type: 'O',
    header: {
      v: 1,
      eraId: e.id,
      expectedOffset: INITIAL_OFFSET_TOKEN,
      ordinal: e.ordinal,
      prevEraId: prev ? prev.id : null,
      prevEraUrl: prev ? prev.path : null,
      baseOffset: INITIAL_OFFSET_TOKEN,
      baseLsn: formatLsn(baseLsn),
      snapEnd: formatLsn(baseLsn),
      checkpointRef: 'test:none',
    },
  }
}

/** PUT-create an era stream with its O frame; returns the base offset. */
async function putEra(
  e: EraRef,
  prev: EraRef | null,
  baseLsn: bigint,
): Promise<string> {
  const created = await client.createStream(e.path, {
    body: encodeAppend([oFrame(e, prev, baseLsn)]),
  })
  expect(created.created).toBe(true)
  return created.nextOffset
}

function wFrame(
  eraId: string,
  at: { offset: string; lsn: bigint },
  walLen = 24,
  commitId = randomUUID(),
): WFrame {
  const wal = new Uint8Array(walLen)
  for (let i = 0; i < walLen; i++) wal[i] = (i * 13 + 7) & 0xff
  return {
    type: 'W',
    header: {
      v: 1,
      eraId,
      expectedOffset: at.offset,
      commitId,
      kind: 'commit',
      baseLsn: formatLsn(at.lsn),
      endLsn: formatLsn(at.lsn + BigInt(walLen)),
      sliceHash: sha256Hex(wal),
    },
    wal,
  }
}

/** CAS-append one W slice at the era tail; returns the new tail coords. */
async function appendW(
  e: EraRef,
  at: { offset: string; lsn: bigint },
  walLen = 24,
): Promise<{ offset: string; lsn: bigint; frame: WFrame }> {
  const frame = wFrame(e.id, at, walLen)
  const res = await client.append(e.path, encodeAppend([frame]), {
    seq: casToken(e.ordinal, at.offset),
    expectedOffset: at.offset,
  })
  expect(res.kind).toBe('ok')
  if (res.kind !== 'ok') throw new Error('unreachable')
  return { offset: res.nextOffset, lsn: at.lsn + BigInt(walLen), frame }
}

/** Seal an era: terminal S via appendAndClose (the CAS path, §6.1 step 5). */
async function sealEra(
  e: EraRef,
  at: { offset: string; lsn: bigint },
  next: EraRef,
  overrides: Partial<SFrame['header']> = {},
): Promise<void> {
  const s: SFrame = {
    type: 'S',
    header: {
      v: 1,
      eraId: e.id,
      expectedOffset: at.offset,
      ordinal: e.ordinal,
      finalOffset: at.offset,
      finalLsn: formatLsn(at.lsn),
      nextEraUrl: next.path,
      nextEraId: next.id,
      ...overrides,
    },
  }
  const res = await client.appendAndClose(e.path, encodeAppend([s]), {
    seq: casToken(e.ordinal, at.offset),
    expectedOffset: at.offset,
  })
  expect(res.kind).toBe('ok')
}

function newTailer(e: EraRef, baseOffset: string, baseLsn: bigint): EraTailer {
  return new EraTailer(client, {
    path: e.path,
    eraId: e.id,
    ordinal: e.ordinal,
    baseOffset,
    baseLsn,
  })
}

/** Decode every W frame of a whole stream (position-checked from INITIAL). */
async function readAllW(path: string): Promise<WFrameHeader[]> {
  const res = await client.read(path, { offset: INITIAL_OFFSET_TOKEN })
  const reader = new PositionCheckedReader(INITIAL_OFFSET_TOKEN)
  const out: WFrameHeader[] = []
  for (const group of reader.feed(res.bytes)) {
    for (const frame of group.frames) {
      if (frame.type === 'W') out.push(frame.header)
    }
  }
  return out
}

// ---------------------------------------------------------------------------
// Tailer era-hop
// ---------------------------------------------------------------------------

describe('tailer era-hop through the S→O chain', () => {
  it(
    '3-era hop: a fresh catch-up walks all three eras, slices contiguous',
    async () => {
      const e1 = era('hop3', 1)
      const e2 = era('hop3', 2)
      const e3 = era('hop3', 3)

      // Era 1: O + 2 W slices + terminal S (append+close CAS path).
      const b1 = await putEra(e1, null, L0)
      const w1 = await appendW(e1, { offset: b1, lsn: L0 })
      const w2 = await appendW(e1, { offset: w1.offset, lsn: w1.lsn })
      // Era 2: O + 1 W + terminal S. Era 3: open.
      const b2 = await putEra(e2, e1, w2.lsn)
      await sealEra(e1, { offset: w2.offset, lsn: w2.lsn }, e2)
      const w3 = await appendW(e2, { offset: b2, lsn: w2.lsn })
      const b3 = await putEra(e3, e2, w3.lsn)
      await sealEra(e2, { offset: w3.offset, lsn: w3.lsn }, e3)

      // Fresh tailer from the era-1 base hops the whole chain.
      const tailer = newTailer(e1, b1, L0)
      expect(await tailer.catchUp()).toBe(3)
      expect(tailer.currentEra).toEqual({
        id: e3.id,
        ordinal: 3,
        path: e3.path,
      })
      expect(tailer.head.lsn).toBe(w3.lsn)
      expect(tailer.head.offset).toBe(b3)
      expect(tailer.closed).toBe(false)
      // W-slice LSN contiguity carried across both hops via O.baseLsn.
      expect(tailer.slices.map((s) => [s.baseLsn, s.endLsn])).toEqual([
        [L0, w1.lsn],
        [w1.lsn, w2.lsn],
        [w2.lsn, w3.lsn],
      ])
      // The current era's O frame was read at the initial token.
      expect(tailer.eraOpen?.eraId).toBe(e3.id)
      expect(tailer.foreignEraFrames).toBe(0)
    },
    TEST_TIMEOUT,
  )

  it(
    'long-poll hop: a parked poll wakes on the seal and hops to the new era',
    async () => {
      const e1 = era('hoplp', 1)
      const e2 = era('hoplp', 2)
      const e3 = era('hoplp', 3)

      const b1 = await putEra(e1, null, L0)
      const w1 = await appendW(e1, { offset: b1, lsn: L0 })
      await putEra(e2, e1, w1.lsn)
      await sealEra(e1, { offset: w1.offset, lsn: w1.lsn }, e2)

      const tailer = newTailer(e1, b1, L0)
      await tailer.catchUp()
      expect(tailer.currentEra.ordinal).toBe(2)

      // Park a long-poll on era 2, then seal it behind the tailer's back.
      const parked = tailer.pollOnce({ live: 'long-poll' })
      await new Promise((r) => setTimeout(r, 50))
      const tail2 = await client.head(e2.path)
      await putEra(e3, e2, w1.lsn)
      await sealEra(e2, { offset: tail2.nextOffset, lsn: w1.lsn }, e3)
      await parked

      // The wake may have been a 204 timeout race — poll again if needed.
      for (let i = 0; i < 5 && tailer.currentEra.ordinal < 3; i++) {
        await tailer.pollOnce({ live: 'long-poll' })
      }
      expect(tailer.currentEra).toEqual({
        id: e3.id,
        ordinal: 3,
        path: e3.path,
      })
      expect(tailer.head.lsn).toBe(w1.lsn)
    },
    TEST_TIMEOUT,
  )

  it(
    'O/S mirror violation ⇒ EraChainError',
    async () => {
      const e1 = era('mirror', 1)
      const e2 = era('mirror', 2)
      const impostor = era('mirror', 9)

      const b1 = await putEra(e1, null, L0)
      const w1 = await appendW(e1, { offset: b1, lsn: L0 })
      // Era 2's O claims a DIFFERENT parent than the S that points at it.
      await putEra(e2, impostor, w1.lsn)
      await sealEra(e1, { offset: w1.offset, lsn: w1.lsn }, e2)

      const tailer = newTailer(e1, b1, L0)
      await expect(tailer.catchUp()).rejects.toBeInstanceOf(EraChainError)
    },
    TEST_TIMEOUT,
  )

  it(
    'closed WITHOUT a terminal S ⇒ WedgedEraError',
    async () => {
      const e1 = era('wedge', 1)
      const b1 = await putEra(e1, null, L0)
      const w1 = await appendW(e1, { offset: b1, lsn: L0 })

      // Close the stream with a non-S body: sealed-detection (§2.6) says
      // this era is wedged, no matter what the closed bit claims.
      const fence: FenceFrame = {
        type: '0',
        header: { v: 1, eraId: e1.id, expectedOffset: w1.offset },
      }
      const res = await client.appendAndClose(e1.path, encodeAppend([fence]), {
        seq: casToken(e1.ordinal, w1.offset),
        expectedOffset: w1.offset,
      })
      expect(res.kind).toBe('ok')

      const tailer = newTailer(e1, b1, L0)
      await expect(tailer.catchUp()).rejects.toBeInstanceOf(WedgedEraError)
      expect(tailer.closed).toBe(true)
    },
    TEST_TIMEOUT,
  )
})

// ---------------------------------------------------------------------------
// Committer era-awareness (closed → hop → re-CAS)
// ---------------------------------------------------------------------------

describe('committer mid-commit rotation', () => {
  it(
    're-CAS lands the same commit in the new era after a hop',
    async () => {
      const e1 = era('recas', 1)
      const e2 = era('recas', 2)

      const b1 = await putEra(e1, null, L0)
      const tailer = newTailer(e1, b1, L0)
      await tailer.catchUp()
      const committer = await Committer.create({
        client,
        era: e1,
        tailer,
        journalDir: join(root, 'journal-recas'),
      })

      // Establish one landed commit in era 1.
      const first = wFrame(e1.id, { offset: b1, lsn: L0 })
      const r1 = await committer.commitSlice({
        commitId: first.header.commitId,
        kind: 'commit',
        baseLsn: L0,
        endLsn: L0 + 24n,
        bytes: first.wal,
      })
      expect(r1.landed).toBe(true)
      const headLsn = tailer.head.lsn

      // Fixture seals era 1 → era 2 behind the committer's back.
      const tail1 = await client.head(e1.path)
      await putEra(e2, e1, headLsn)
      await sealEra(e1, { offset: tail1.nextOffset, lsn: headLsn }, e2)

      // The next commit hits `closed`, hops, and re-CASes into era 2 with
      // the SAME commitId + sliceHash (new frame, new journal entry).
      const commitId = randomUUID()
      const wal = new Uint8Array(32).fill(0xa5)
      const r2 = await committer.commitSlice({
        commitId,
        kind: 'commit',
        baseLsn: headLsn,
        endLsn: headLsn + 32n,
        bytes: wal,
      })
      expect(r2.landed).toBe(true)

      // The W frame is present in ERA 2 with the same commitId + sliceHash.
      const era2W = await readAllW(e2.path)
      expect(era2W.length).toBe(1)
      expect(era2W[0].commitId).toBe(commitId)
      expect(era2W[0].sliceHash).toBe(sha256Hex(wal))
      expect(era2W[0].eraId).toBe(e2.id)

      // Journal clean; tailer hopped and its head is consistent.
      expect(committer.journal.listPending()).toEqual([])
      expect(tailer.currentEra.ordinal).toBe(2)
      expect(tailer.head.lsn).toBe(headLsn + 32n)

      // The committer keeps working in the new era without further hops.
      const w3 = wFrame(e2.id, tailer.head)
      const r3 = await committer.commitSlice({
        commitId: w3.header.commitId,
        kind: 'commit',
        baseLsn: headLsn + 32n,
        endLsn: headLsn + 32n + 24n,
        bytes: w3.wal,
      })
      expect(r3.landed).toBe(true)
    },
    TEST_TIMEOUT,
  )

  it(
    'a foreign W landed first in the new era ⇒ { landed: false }',
    async () => {
      const e1 = era('recas-lost', 1)
      const e2 = era('recas-lost', 2)

      const b1 = await putEra(e1, null, L0)
      const tailer = newTailer(e1, b1, L0)
      await tailer.catchUp()
      const committer = await Committer.create({
        client,
        era: e1,
        tailer,
        journalDir: join(root, 'journal-recas-lost'),
      })

      // Rotate era 1 → era 2 and land a FOREIGN commit in era 2 first.
      const b2 = await putEra(e2, e1, L0)
      await sealEra(e1, { offset: b1, lsn: L0 }, e2)
      await appendW(e2, { offset: b2, lsn: L0 })

      // The committer's slice was captured at the old head: after the hop
      // its baseLsn no longer matches ⇒ ordinary re-execute path.
      const wal = new Uint8Array(16).fill(0x5a)
      const res = await committer.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        baseLsn: L0,
        endLsn: L0 + 16n,
        bytes: wal,
      })
      expect(res).toEqual({ landed: false })
      expect(committer.journal.listPending()).toEqual([])
      expect(tailer.currentEra.ordinal).toBe(2)
    },
    TEST_TIMEOUT,
  )
})

// ---------------------------------------------------------------------------
// Fork-prefix tolerance (§2.5)
// ---------------------------------------------------------------------------

describe('fork-prefix eraId tolerance', () => {
  it(
    'a forked stream is readable end-to-end, tolerating parent eraIds',
    async () => {
      const parent = era('fork', 1)
      const b1 = await putEra(parent, null, L0)
      const w1 = await appendW(parent, { offset: b1, lsn: L0 })

      // Fork the parent era at its tail; the copied prefix (O + W) carries
      // the PARENT's eraId. The child keeps the parent's ordinal (W3 tokens
      // stay monotone — offsets continue past the fork point).
      const child: EraRef = {
        id: `${String(parent.ordinal).padStart(6, '0')}-CHILD`,
        ordinal: parent.ordinal,
        path: '/db/fork/era/child',
      }
      const fork = await client.forkStream(parent.path, child.path, w1.offset)
      expect(fork.created).toBe(true)
      expect(fork.nextOffset).toBe(w1.offset)

      // Child-era writes continue the W chain under the child's eraId.
      const w2 = await appendW(child, { offset: w1.offset, lsn: w1.lsn })

      // A tailer on the child stream reads the WHOLE stream: parent prefix
      // frames are foreign (counted), position + LSN chaining stay strict.
      const tailer = newTailer(child, INITIAL_OFFSET_TOKEN, L0)
      expect(await tailer.catchUp()).toBe(2)
      expect(tailer.foreignEraFrames).toBe(2) // parent O + parent W
      expect(tailer.slices.map((s) => [s.baseLsn, s.endLsn])).toEqual([
        [L0, w1.lsn],
        [w1.lsn, w2.lsn],
      ])
      expect(tailer.head.lsn).toBe(w2.lsn)
      expect(tailer.head.offset).toBe(w2.offset)
      // The parent's O frame was recorded (advisory) despite the foreign id.
      expect(tailer.eraOpen?.eraId).toBe(parent.id)
    },
    TEST_TIMEOUT,
  )
})

// ---------------------------------------------------------------------------
// W4 survives the refactor
// ---------------------------------------------------------------------------

describe('W4 position check is not relaxed', () => {
  it(
    'a frame appended with a wrong expectedOffset poisons the reader',
    async () => {
      const e1 = era('w4', 1)
      const b1 = await putEra(e1, null, L0)

      // Hand-append a W whose header LIES about its position (raw client:
      // Stream-Seq is the real tail so the CAS lands, but the header claims
      // the initial token).
      const frame = wFrame(e1.id, { offset: INITIAL_OFFSET_TOKEN, lsn: L0 })
      const res = await client.append(e1.path, encodeAppend([frame]), {
        seq: casToken(e1.ordinal, b1),
        expectedOffset: b1,
      })
      expect(res.kind).toBe('ok')

      const tailer = newTailer(e1, b1, L0)
      await expect(tailer.catchUp()).rejects.toBeInstanceOf(FramePositionError)
    },
    TEST_TIMEOUT,
  )
})
