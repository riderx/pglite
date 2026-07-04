import { describe, it, expect } from 'vitest'
import {
  encodeFrame,
  decodeFrame,
  encodeAppend,
  parseOffsetToken,
  formatOffsetToken,
  nextBoundary,
  casToken,
  INITIAL_OFFSET_TOKEN,
  APPEND_OVERHEAD,
  PositionCheckedReader,
  FramePositionError,
  type Frame,
  type WFrame,
  type OFrame,
  type SFrame,
  type KFrame,
  type LFrame,
  type FenceFrame,
  type GenericFrame,
  type GFrame,
  type NFrame,
} from '../src/frames'

const ERA = '000001-01H0000000000000000000000000'

function wFrame(offset: string, walLen = 8, commitId = 'c1'): WFrame {
  const wal = new Uint8Array(walLen)
  for (let i = 0; i < walLen; i++) wal[i] = (i * 7 + 3) & 0xff
  return {
    type: 'W',
    header: {
      v: 1,
      eraId: ERA,
      expectedOffset: offset,
      commitId,
      kind: 'commit',
      baseLsn: '0/1A2B3C',
      endLsn: '0/1A2B44',
      sliceHash: 'sha256:' + '00'.repeat(32),
    },
    wal,
  }
}

describe('frame codec round-trips', () => {
  it('W frame with WAL bytes', () => {
    const f = wFrame(INITIAL_OFFSET_TOKEN, 20)
    const enc = encodeFrame(f)
    const dec = decodeFrame(enc, 0)!
    expect(dec.next).toBe(enc.length)
    expect(dec.frame.type).toBe('W')
    const w = dec.frame as WFrame
    expect(w.header).toEqual(f.header)
    expect([...w.wal]).toEqual([...f.wal])
  })

  it('O frame', () => {
    const f: OFrame = {
      type: 'O',
      header: {
        v: 1,
        eraId: ERA,
        expectedOffset: INITIAL_OFFSET_TOKEN,
        ordinal: 1,
        prevEraId: null,
        prevEraUrl: null,
        baseOffset: INITIAL_OFFSET_TOKEN,
        baseLsn: '0/0',
        snapEnd: '0/1000000',
        checkpointRef: 'sha256:' + 'ab'.repeat(32),
      },
    }
    const dec = decodeFrame(encodeFrame(f), 0)!
    expect(dec.frame).toEqual(f)
  })

  it('K frame', () => {
    const f: KFrame = {
      type: 'K',
      header: {
        v: 1,
        eraId: ERA,
        expectedOffset: '0000000000000000_0000000000000010',
        lsn: '0/2000000',
        snapEnd: '0/2000000',
        checkpointRef: 'sha256:' + 'cd'.repeat(32),
        sha256: 'sha256:' + 'ef'.repeat(32),
      },
    }
    expect(decodeFrame(encodeFrame(f), 0)!.frame).toEqual(f)
  })

  it('L frame', () => {
    const f: LFrame = {
      type: 'L',
      header: {
        v: 1,
        eraId: ERA,
        expectedOffset: INITIAL_OFFSET_TOKEN,
        kind: 'head',
        holder: 'host-a',
        epoch: 3,
        ttlMs: 5000,
        base: { offset: INITIAL_OFFSET_TOKEN, lsn: '0/1000000' },
      },
    }
    expect(decodeFrame(encodeFrame(f), 0)!.frame).toEqual(f)
  })

  it('fence (0) frame', () => {
    const f: FenceFrame = {
      type: '0',
      header: { v: 1, eraId: ERA, expectedOffset: INITIAL_OFFSET_TOKEN },
    }
    expect(decodeFrame(encodeFrame(f), 0)!.frame).toEqual(f)
  })

  it('S frame', () => {
    const f: SFrame = {
      type: 'S',
      header: {
        v: 1,
        eraId: ERA,
        expectedOffset: '0000000000000000_0000000000000020',
        ordinal: 1,
        finalOffset: '0000000000000000_0000000000000020',
        finalLsn: '0/2000000',
        nextEraUrl: '/db/x/era/000002-NEXT',
        nextEraId: '000002-NEXT',
      },
    }
    expect(decodeFrame(encodeFrame(f), 0)!.frame).toEqual(f)
  })

  it('typed N (notification) frame round-trips', () => {
    const f: NFrame = {
      type: 'N',
      header: {
        v: 1,
        eraId: ERA,
        expectedOffset: INITIAL_OFFSET_TOKEN,
        commitId: 'c-notify',
        channel: 'orders',
        payload: '{"id":1}',
        commitLsn: '0/1A2B3C4',
      },
    }
    expect(decodeFrame(encodeFrame(f), 0)!.frame).toEqual(f)
  })

  it('G (sequence grant) frame round-trips (M4 §5.3)', () => {
    const f: GFrame = {
      type: 'G',
      header: {
        v: 1,
        eraId: ERA,
        expectedOffset: INITIAL_OFFSET_TOKEN,
        kind: 'sequence',
        seqName: 'public.orders_id_seq',
        start: '4096',
        end: '8192',
        grantee: 'host-a',
        granteeEpoch: 3,
      },
    }
    expect(decodeFrame(encodeFrame(f), 0)!.frame).toEqual(f)
  })

  it('reserved generic frames (F,X)', () => {
    for (const type of ['F', 'X'] as const) {
      const f: GenericFrame = {
        type,
        header: {
          v: 1,
          eraId: ERA,
          expectedOffset: INITIAL_OFFSET_TOKEN,
          extra: 'payload-' + type,
        },
      }
      expect(decodeFrame(encodeFrame(f), 0)!.frame).toEqual(f)
    }
  })

  it('decodeFrame returns null on partial input', () => {
    const enc = encodeFrame(wFrame(INITIAL_OFFSET_TOKEN, 30))
    for (let cut = 1; cut < enc.length; cut++) {
      expect(decodeFrame(enc.subarray(0, cut), 0)).toBeNull()
    }
    expect(decodeFrame(enc, 0)!.next).toBe(enc.length)
  })
})

describe('encodeAppend', () => {
  it('concatenates frames sharing one expectedOffset', () => {
    const a = wFrame(INITIAL_OFFSET_TOKEN, 4, 'a')
    const b = wFrame(INITIAL_OFFSET_TOKEN, 6, 'b')
    const enc = encodeAppend([a, b])
    const d1 = decodeFrame(enc, 0)!
    const d2 = decodeFrame(enc, d1.next)!
    expect(d2.next).toBe(enc.length)
    expect((d1.frame as WFrame).header.commitId).toBe('a')
    expect((d2.frame as WFrame).header.commitId).toBe('b')
  })

  it('throws on mixed expectedOffset', () => {
    const a = wFrame(INITIAL_OFFSET_TOKEN, 4)
    const b = wFrame('0000000000000000_0000000000000099', 4)
    expect(() => encodeAppend([a, b])).toThrow(/mixed expectedOffset/)
  })

  it('throws on empty list', () => {
    expect(() => encodeAppend([])).toThrow(/empty/)
  })
})

describe('offset token helpers', () => {
  it('parse / format round-trip', () => {
    const tok = '0000000000000012_0000000000034567'
    const { readSeq, byteOffset } = parseOffsetToken(tok)
    expect(readSeq).toBe(12n)
    expect(byteOffset).toBe(34567n)
    expect(formatOffsetToken(readSeq, byteOffset)).toBe(tok)
  })

  it('nextBoundary advances byteOffset by 5 + payloadLen, readSeq fixed', () => {
    // matches the real DS server: readSeq unchanged
    expect(nextBoundary(INITIAL_OFFSET_TOKEN, 5)).toBe(
      '0000000000000000_0000000000000010',
    )
    expect(nextBoundary('0000000000000000_0000000000000010', 10)).toBe(
      '0000000000000000_0000000000000025',
    )
    expect(APPEND_OVERHEAD).toBe(5)
  })

  it('casToken is fixed-width and lexicographically monotone', () => {
    const t1 = casToken(1, '0000000000000000_0000000000000010')
    const t2 = casToken(1, '0000000000000000_0000000000000025')
    expect(t1).toBe('0000000001,0000000000000000_0000000000000010')
    expect(t1 < t2).toBe(true)
    // higher ordinal always sorts after
    expect(
      casToken(1, '9999999999999999_9999999999999999') <
        casToken(2, INITIAL_OFFSET_TOKEN),
    ).toBe(true)
  })
})

describe('PositionCheckedReader', () => {
  it('reads a single append group at the initial token', () => {
    const frame = wFrame(INITIAL_OFFSET_TOKEN, 12)
    const body = encodeAppend([frame])
    const reader = new PositionCheckedReader(INITIAL_OFFSET_TOKEN)
    const groups = [...reader.feed(body)]
    expect(groups.length).toBe(1)
    expect(groups[0].offset).toBe(INITIAL_OFFSET_TOKEN)
    expect(groups[0].frames.length).toBe(1)
    // boundary advanced by encoded length + 5
    expect(reader.boundary).toBe(
      nextBoundary(INITIAL_OFFSET_TOKEN, body.length),
    )
  })

  it('reconstructs boundaries across multiple appends (hand-computed)', () => {
    // append 1: two frames at initial token
    const b1 = encodeAppend([
      wFrame(INITIAL_OFFSET_TOKEN, 4, 'a1'),
      wFrame(INITIAL_OFFSET_TOKEN, 4, 'a2'),
    ])
    const off1 = nextBoundary(INITIAL_OFFSET_TOKEN, b1.length)
    // append 2: one frame at off1
    const b2 = encodeAppend([wFrame(off1, 10, 'b1')])
    const off2 = nextBoundary(off1, b2.length)
    // append 3: one frame at off2
    const b3 = encodeAppend([wFrame(off2, 2, 'c1')])
    const off3 = nextBoundary(off2, b3.length)

    const all = new Uint8Array(b1.length + b2.length + b3.length)
    all.set(b1, 0)
    all.set(b2, b1.length)
    all.set(b3, b1.length + b2.length)

    const reader = new PositionCheckedReader(INITIAL_OFFSET_TOKEN)
    const groups = [...reader.feed(all)]
    expect(groups.map((g) => g.offset)).toEqual([
      INITIAL_OFFSET_TOKEN,
      off1,
      off2,
    ])
    expect(groups[0].frames.length).toBe(2)
    expect(groups[1].frames.length).toBe(1)
    expect(groups[2].frames.length).toBe(1)
    expect(reader.boundary).toBe(off3)
  })

  it('handles partial frame feeds: split at every byte position', () => {
    // Two appends at distinct offsets so grouping is unambiguous.
    const b1 = encodeAppend([wFrame(INITIAL_OFFSET_TOKEN, 7, 'x')])
    const off1 = nextBoundary(INITIAL_OFFSET_TOKEN, b1.length)
    const b2 = encodeAppend([wFrame(off1, 9, 'y')])
    const full = new Uint8Array(b1.length + b2.length)
    full.set(b1, 0)
    full.set(b2, b1.length)

    // reference output: one feed
    const ref = new PositionCheckedReader(INITIAL_OFFSET_TOKEN)
    const refGroups = [...ref.feed(full)].map((g) => ({
      offset: g.offset,
      commits: g.frames.map((f) => (f as WFrame).header.commitId),
    }))
    expect(refGroups.length).toBe(2)

    for (let split = 0; split <= full.length; split++) {
      const reader = new PositionCheckedReader(INITIAL_OFFSET_TOKEN)
      const out: { offset: string; commits: string[] }[] = []
      for (const g of reader.feed(full.subarray(0, split))) {
        out.push({
          offset: g.offset,
          commits: g.frames.map((f) => (f as WFrame).header.commitId),
        })
      }
      for (const g of reader.feed(full.subarray(split))) {
        out.push({
          offset: g.offset,
          commits: g.frames.map((f) => (f as WFrame).header.commitId),
        })
      }
      expect(out).toEqual(refGroups)
      expect(reader.boundary).toBe(nextBoundary(off1, b2.length))
    }
  })

  it('poisons and throws on a mis-positioned group', () => {
    // Frame claims the wrong expectedOffset (not the initial token).
    const bad = encodeAppend([wFrame('0000000000000000_0000000000000999', 4)])
    const reader = new PositionCheckedReader(INITIAL_OFFSET_TOKEN)
    expect(() => [...reader.feed(bad)]).toThrow(FramePositionError)
    expect(reader.poisoned).toBe(true)
    // further reads refuse
    expect(() => [...reader.feed(new Uint8Array([1]))]).toThrow(
      FramePositionError,
    )
  })

  it('expectBoundary cross-check throws + poisons on mismatch', () => {
    const frame = wFrame(INITIAL_OFFSET_TOKEN, 4)
    const body = encodeAppend([frame])
    const reader = new PositionCheckedReader(INITIAL_OFFSET_TOKEN)
    ;[...reader.feed(body)]
    // correct boundary passes
    reader.expectBoundary(nextBoundary(INITIAL_OFFSET_TOKEN, body.length))
    // wrong boundary poisons
    expect(() =>
      reader.expectBoundary('0000000000000000_9999999999999999'),
    ).toThrow(FramePositionError)
    expect(reader.poisoned).toBe(true)
  })
})

// keep the Frame union import meaningful for typecheck
const _typecheckOnly: Frame | null = null
void _typecheckOnly
