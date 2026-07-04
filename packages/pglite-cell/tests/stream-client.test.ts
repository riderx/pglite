import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { DurableStreamTestServer } from '@durable-streams/server'
import { DsStreamClient, StreamConfigConflictError } from '../src/stream-client'
import {
  encodeAppend,
  nextBoundary,
  casToken,
  INITIAL_OFFSET_TOKEN,
  PositionCheckedReader,
  type OFrame,
  type WFrame,
} from '../src/frames'

const ERA = '000001-01H0000000000000000000000000'

let server: DurableStreamTestServer
let url: string

beforeAll(async () => {
  server = new DurableStreamTestServer({ port: 0, longPollTimeout: 500 })
  url = await server.start()
})

afterAll(async () => {
  await server.stop()
})

let counter = 0
function freshPath(): string {
  return `/test/stream-${Date.now()}-${counter++}`
}

function oFrameBody(offset = INITIAL_OFFSET_TOKEN): Uint8Array {
  const f: OFrame = {
    type: 'O',
    header: {
      v: 1,
      eraId: ERA,
      expectedOffset: offset,
      ordinal: 1,
      prevEraId: null,
      prevEraUrl: null,
      baseOffset: offset,
      baseLsn: '0/0',
      snapEnd: '0/1000000',
      checkpointRef: 'sha256:' + '00'.repeat(32),
    },
  }
  return encodeAppend([f])
}

function wBody(offset: string, walLen: number, commitId: string): Uint8Array {
  const wal = new Uint8Array(walLen)
  for (let i = 0; i < walLen; i++) wal[i] = (i + commitId.charCodeAt(0)) & 0xff
  const f: WFrame = {
    type: 'W',
    header: {
      v: 1,
      eraId: ERA,
      expectedOffset: offset,
      commitId,
      kind: 'commit',
      baseLsn: '0/1000000',
      endLsn: '0/1000100',
      sliceHash: 'sha256:' + '11'.repeat(32),
    },
    wal,
  }
  return encodeAppend([f])
}

describe('DsStreamClient against a real embedded server', () => {
  it('create (201) then create-again identical (200)', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    const r1 = await client.createStream(path)
    expect(r1.created).toBe(true)
    const r2 = await client.createStream(path)
    expect(r2.created).toBe(false)
  })

  it('create with mismatched config throws StreamConfigConflictError', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    await client.createStream(path, { contentType: 'application/octet-stream' })
    await expect(
      client.createStream(path, { contentType: 'text/plain' }),
    ).rejects.toBeInstanceOf(StreamConfigConflictError)
  })

  it('PUT with O-frame body reads back as the first append and validates', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    const body = oFrameBody()
    const created = await client.createStream(path, { body })
    expect(created.created).toBe(true)
    // read from start
    const read = await client.read(path, { offset: '-1' })
    expect(read.status).toBe(200)
    const reader = new PositionCheckedReader(INITIAL_OFFSET_TOKEN)
    const groups = [...reader.feed(read.bytes)]
    expect(groups.length).toBe(1)
    expect(groups[0].offset).toBe(INITIAL_OFFSET_TOKEN)
    expect(groups[0].frames[0].type).toBe('O')
    reader.expectBoundary(read.nextOffset)
    // server's next offset == our computed boundary
    expect(read.nextOffset).toBe(
      nextBoundary(INITIAL_OFFSET_TOKEN, body.length),
    )
  })

  it('CAS: two appends racing with the same seq token — first ok, second seq-conflict', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    await client.createStream(path)
    const head = await client.head(path)
    const seq = casToken(1, head.nextOffset)
    const r1 = await client.append(path, wBody(head.nextOffset, 4, 'a'), {
      seq,
    })
    expect(r1.kind).toBe('ok')
    const r2 = await client.append(path, wBody(head.nextOffset, 4, 'b'), {
      seq,
    })
    expect(r2.kind).toBe('seq-conflict')
  })

  it('seq tokens computed from real offsets are lexicographically increasing', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    await client.createStream(path)
    let head = (await client.head(path)).nextOffset
    const seqs: string[] = []
    for (let i = 0; i < 5; i++) {
      const seq = casToken(1, head)
      seqs.push(seq)
      const r = await client.append(path, wBody(head, 3 + i, 'w'), {
        seq,
        expectedOffset: head,
      })
      expect(r.kind).toBe('ok')
      head = (r as { nextOffset: string }).nextOffset
    }
    const sorted = [...seqs].sort()
    expect(seqs).toEqual(sorted)
    // strictly increasing
    for (let i = 1; i < seqs.length; i++)
      expect(seqs[i - 1] < seqs[i]).toBe(true)
  })

  it('producer dedup: same tuple + same body re-POST ⇒ deduped, same nextOffset', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    await client.createStream(path)
    const head = (await client.head(path)).nextOffset
    const body = wBody(head, 6, 'p')
    const producer = { id: 'prod-1', epoch: 0, seq: 0 }
    const r1 = await client.append(path, body, { producer })
    expect(r1.kind).toBe('ok')
    const off1 = (r1 as { nextOffset: string }).nextOffset
    const r2 = await client.append(path, body, { producer })
    expect(r2.kind).toBe('ok')
    expect((r2 as { deduped: boolean }).deduped).toBe(true)
    expect((r2 as { nextOffset: string }).nextOffset).toBe(off1)
  })

  it('stale epoch ⇒ stale-epoch result kind with current epoch', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    await client.createStream(path)
    const head = (await client.head(path)).nextOffset
    // establish epoch 2
    await client.append(path, wBody(head, 4, 'e'), {
      producer: { id: 'q', epoch: 2, seq: 0 },
    })
    const h2 = (await client.head(path)).nextOffset
    const r = await client.append(path, wBody(h2, 4, 'f'), {
      producer: { id: 'q', epoch: 1, seq: 1 },
    })
    expect(r.kind).toBe('stale-epoch')
    expect((r as { currentEpoch: number }).currentEpoch).toBe(2)
  })

  it('append+close (CAS path) then append ⇒ closed kind', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    await client.createStream(path)
    const head = (await client.head(path)).nextOffset
    const rc = await client.append(path, wBody(head, 4, 'z'), { close: true })
    // OUR successful append+close ⇒ ok with closed flag — distinct from the
    // 409 'closed' rejection an append AFTER closure gets.
    expect(rc.kind).toBe('ok')
    expect((rc as { closed?: boolean }).closed).toBe(true)
    const r2 = await client.append(path, wBody(head, 4, 'z2'))
    expect(r2.kind).toBe('closed')
  })

  it('appendAndClose (CAS seal) returns final offset; HEAD reports closed', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    await client.createStream(path)
    const head = (await client.head(path)).nextOffset
    const rc = await client.appendAndClose(path, wBody(head, 5, 'c'), {
      seq: casToken(1, head),
    })
    expect(rc.kind).toBe('ok')
    expect((rc as { closed?: boolean }).closed).toBe(true)
    expect((rc as { nextOffset: string }).nextOffset).not.toBe('')
    const h = await client.head(path)
    expect(h.closed).toBe(true)
  })

  it('deleteStream (official delete()) removes the stream', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    await client.createStream(path)
    await client.deleteStream(path)
    await expect(client.head(path)).rejects.toThrow()
  })

  it('read at tail ⇒ 200 empty upToDate', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    await client.createStream(path)
    const head = (await client.head(path)).nextOffset
    await client.append(path, wBody(head, 5, 't'))
    const tail = (await client.head(path)).nextOffset
    const r = await client.read(path, { offset: tail })
    expect(r.status).toBe(200)
    expect(r.bytes.length).toBe(0)
    expect(r.upToDate).toBe(true)
  })

  it('long-poll timeout ⇒ 204 empty', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    await client.createStream(path)
    const tail = (await client.head(path)).nextOffset
    const r = await client.read(path, { offset: tail, live: 'long-poll' })
    expect(r.status).toBe(204)
    expect(r.bytes.length).toBe(0)
  })

  it('full loop: N appends then read-from-start chains through the reader', async () => {
    const client = new DsStreamClient(url)
    const path = freshPath()
    await client.createStream(path, { body: oFrameBody() })

    const expectedOffsets: string[] = [INITIAL_OFFSET_TOKEN]
    let head = (await client.head(path)).nextOffset

    const N = 6
    for (let i = 0; i < N; i++) {
      expectedOffsets.push(head)
      const body = wBody(head, 4 + i, 'c' + i)
      const seq = casToken(1, head)
      const r = await client.append(path, body, { seq, expectedOffset: head })
      expect(r.kind).toBe('ok')
      head = (r as { nextOffset: string }).nextOffset
    }

    const finalHead = (await client.head(path)).nextOffset
    expect(finalHead).toBe(head)

    // read everything from the start and validate the chain
    const read = await client.read(path, { offset: '-1' })
    const reader = new PositionCheckedReader(INITIAL_OFFSET_TOKEN)
    const groups = [...reader.feed(read.bytes)]
    expect(groups.map((g) => g.offset)).toEqual(expectedOffsets)
    // final computed boundary equals head nextOffset
    reader.expectBoundary(read.nextOffset)
    expect(reader.boundary).toBe(finalHead)
  })
})
