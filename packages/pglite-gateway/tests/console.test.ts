// M6 console tests (no browser). Four parts:
//   (a) GET /console serves the HTML page (200 + marker strings);
//   (b) GET /v1/console-info round-trips the constructor option;
//   (c) decoder parity: the shared browser decoder `decodeStreamBytes`
//       decodes bytes produced by pglite-cell's REAL `encodeAppend` for every
//       frame type (incl. a W+N group and an S) — identical header fields out;
//   (d) mini integration: create a db over HTTP, append a real-encoded commit
//       group through the stream proxy the same way, then GET the era stream
//       exactly as the console feed does (offset + long-poll params + reading
//       Stream-Next-Offset) and run the shared decoder over the bytes — the
//       frames match what a PositionCheckedReader sees over the same bytes.

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  DsStreamClient,
  encodeAppend,
  casToken,
  nextBoundary,
  PositionCheckedReader,
  INITIAL_OFFSET_TOKEN,
} from '@electric-sql/pglite-cell'
import type { Frame } from '@electric-sql/pglite-cell'
import { GatewayCore } from '../src/core'
import { GatewayServer } from '../src/http'
import { decodeStreamBytes, CONSOLE_HTML } from '../src/console'
import type { Manifest } from '../src/core'

const TEST_TIMEOUT = 120_000

let root: string
let core: GatewayCore
let server: GatewayServer
let gatewayUrl: string

const CONSOLE_INFO = { proxy: 'localhost:5432', note: 'demo gateway' }

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-gw-console-'))
  core = new GatewayCore({ dataRoot: join(root, 'gateway') })
  await core.start()
  server = new GatewayServer({ core, consoleInfo: CONSOLE_INFO })
  gatewayUrl = await server.listen(0)
}, TEST_TIMEOUT)

afterAll(async () => {
  await server?.close()
  await core?.stop()
  rmSync(root, { recursive: true, force: true })
})

describe('(a) GET /console serves the page', () => {
  it('returns 200 HTML with the expected marker strings', async () => {
    const res = await fetch(`${gatewayUrl}/console`)
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toContain('text/html')
    const html = await res.text()
    expect(html).toContain('PGlite Gateway Console')
    expect(html).toContain('decodeStreamBytes')
    expect(html).toContain('Live frame feed')
    expect(html).toContain('/v1/console-info')
    expect(html).toBe(CONSOLE_HTML)
  })
})

describe('(b) GET /v1/console-info round-trips the option', () => {
  it('returns the constructor consoleInfo blob verbatim', async () => {
    const res = await fetch(`${gatewayUrl}/v1/console-info`)
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual(CONSOLE_INFO)
  })

  it('defaults to null when no consoleInfo is passed', async () => {
    const s2 = new GatewayServer({ core })
    const res = await s2.app.request('/v1/console-info')
    expect(res.status).toBe(200)
    expect(await res.json()).toBeNull()
  })
})

// A representative header carrying the common base fields.
function base(expectedOffset: string, eraId = 'ERA1') {
  return { v: 1 as const, eraId, expectedOffset }
}

describe('(c) decoder parity against the real encodeAppend', () => {
  it('round-trips every frame type with identical header fields', () => {
    const off = INITIAL_OFFSET_TOKEN
    // Build one representative frame of every type. W and N share an offset
    // (one CAS append group — the atomic commit+notify ride-along).
    const wal = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])
    const frames: Frame[] = [
      {
        type: 'W',
        header: {
          ...base(off),
          commitId: 'commit-abc',
          kind: 'commit',
          baseLsn: '0/100',
          endLsn: '0/200',
          sliceHash: 'sha256:deadbeef',
        },
        wal,
      },
      {
        type: 'N',
        header: {
          ...base(off),
          commitId: 'commit-abc',
          channel: 'chan',
          payload: 'hello',
          commitLsn: '0/200',
        },
      },
    ]
    const wnBytes = encodeAppend(frames)
    const { groups, consumed } = decodeStreamBytes(wnBytes)
    expect(consumed).toBe(wnBytes.length)
    expect(groups).toHaveLength(1)
    const g = groups[0]
    expect(g.offset).toBe(off)
    expect(g.frames.map((f) => f.type)).toEqual(['W', 'N'])
    // W header identical + wal byte count.
    expect(g.frames[0].header).toEqual(frames[0].header)
    expect(g.frames[0].walBytes).toBe(wal.length)
    // N header identical.
    expect(g.frames[1].header).toEqual(frames[1].header)

    // Each remaining single-frame type in its own append group.
    const off2 = nextBoundary(off, wnBytes.length)
    const singles: Frame[] = [
      {
        type: 'K',
        header: {
          ...base(off2),
          lsn: '0/300',
          snapEnd: '0/2FF',
          checkpointRef: 'sha256:ck',
          sha256: 'sha256:ck',
        },
      },
      {
        type: 'L',
        header: {
          ...base(off2),
          kind: 'head',
          holder: 'host-1',
          epoch: 3,
          ttlMs: 5000,
        },
      },
      {
        type: 'G',
        header: {
          ...base(off2),
          kind: 'sequence',
          seqName: 'public.s',
          start: '0',
          end: '1000',
          grantee: 'host-1',
          granteeEpoch: 3,
        },
      },
      {
        type: 'O',
        header: {
          ...base(off2),
          ordinal: 2,
          prevEraId: 'ERA0',
          prevEraUrl: '/era/000001-x',
          baseOffset: off2,
          baseLsn: '0/300',
          snapEnd: '0/300',
          checkpointRef: 'sha256:ck',
        },
      },
      {
        type: 'S',
        header: {
          ...base(off2),
          ordinal: 2,
          finalOffset: off2,
          finalLsn: '0/400',
          nextEraUrl: '/era/000002-y',
          nextEraId: 'ERA2',
        },
      },
    ]
    for (const f of singles) {
      const bytes = encodeAppend([f])
      const out = decodeStreamBytes(bytes)
      expect(out.consumed).toBe(bytes.length)
      expect(out.groups).toHaveLength(1)
      expect(out.groups[0].frames).toHaveLength(1)
      const df = out.groups[0].frames[0]
      expect(df.type).toBe(f.type)
      expect(df.header).toEqual(f.header)
      expect(df.walBytes).toBe(0)
    }
  })

  it('leaves a trailing partial frame unconsumed', () => {
    const bytes = encodeAppend([
      {
        type: 'K',
        header: {
          ...base(INITIAL_OFFSET_TOKEN),
          lsn: '0/1',
          snapEnd: '0/1',
          checkpointRef: 'r',
          sha256: 's',
        },
      },
    ])
    const truncated = bytes.subarray(0, bytes.length - 3)
    const out = decodeStreamBytes(truncated)
    expect(out.groups).toHaveLength(0)
    expect(out.consumed).toBe(0)
  })
})

describe('(d) integration: append through the proxy, read it the console way', () => {
  it(
    'decodes proxy stream bytes identically to a PositionCheckedReader',
    async () => {
      // Create a database over HTTP; its manifest gives the era path + base
      // offset (the O frame already landed at baseOffset).
      const created = (await (
        await fetch(`${gatewayUrl}/v1/db`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: 'console-int' }),
        })
      ).json()) as Manifest
      const dbId = created.databaseId
      const eraPath = created.era.path
      const ordinal = created.era.ordinal
      const baseOffset = created.era.baseOffset

      // Append a real commit group (W + N) at the era base via the proxy,
      // exactly the way the committer does: DsStreamClient CAS append with a
      // Stream-Seq CAS token + Stream-Expected-Offset.
      const client = new DsStreamClient(`${gatewayUrl}/v1/db/${dbId}/stream`)
      const wal = new Uint8Array([9, 8, 7, 6, 5, 4, 3, 2, 1, 0])
      const commitFrames: Frame[] = [
        {
          type: 'W',
          header: {
            v: 1,
            eraId: created.era.id,
            expectedOffset: baseOffset,
            commitId: 'int-commit',
            kind: 'commit',
            baseLsn: created.era.baseLsn,
            endLsn: 'FF/DEAD',
            sliceHash: 'sha256:int',
          },
          wal,
        },
        {
          type: 'N',
          header: {
            v: 1,
            eraId: created.era.id,
            expectedOffset: baseOffset,
            commitId: 'int-commit',
            channel: 'events',
            payload: 'row-inserted',
            commitLsn: 'FF/DEAD',
          },
        },
      ]
      const body = encodeAppend(commitFrames)
      const appendRes = await client.append(eraPath, body, {
        seq: casToken(ordinal, baseOffset),
        expectedOffset: baseOffset,
      })
      expect(appendRes.kind).toBe('ok')

      // Now read the era stream the way the console feed does: plain GET with
      // ?offset=<baseOffset>&live=long-poll, read Stream-Next-Offset.
      const url =
        `${gatewayUrl}/v1/db/${dbId}/stream${eraPath}` +
        `?offset=${encodeURIComponent(baseOffset)}&live=long-poll`
      const res = await fetch(url)
      expect(res.status).toBe(200)
      const nextOffset = res.headers.get('Stream-Next-Offset')
      expect(nextOffset).toBeTruthy()
      const bytes = new Uint8Array(await res.arrayBuffer())
      expect(bytes.length).toBeGreaterThan(0)

      // Console decoder over the proxy bytes.
      const { groups, consumed } = decodeStreamBytes(bytes)
      expect(consumed).toBe(bytes.length)
      expect(groups).toHaveLength(1)
      expect(groups[0].offset).toBe(baseOffset)
      expect(groups[0].frames.map((f) => f.type)).toEqual(['W', 'N'])
      expect(groups[0].frames[0].walBytes).toBe(wal.length)
      expect(groups[0].frames[1].header.payload).toBe('row-inserted')

      // The console's boundary math must equal the server's reported next.
      expect(nextBoundary(baseOffset, bytes.length)).toBe(nextOffset)

      // Parity with the authoritative PositionCheckedReader over the same
      // bytes: same group, same frame types, same header fields.
      const reader = new PositionCheckedReader(baseOffset)
      const pcrGroups = [...reader.feed(bytes)]
      expect(pcrGroups).toHaveLength(1)
      expect(pcrGroups[0].offset).toBe(groups[0].offset)
      expect(pcrGroups[0].frames.map((f) => f.type)).toEqual(
        groups[0].frames.map((f) => f.type),
      )
      // W + N header fields match between the two decoders.
      expect(pcrGroups[0].frames[0].header).toEqual(groups[0].frames[0].header)
      expect(pcrGroups[0].frames[1].header).toEqual(groups[0].frames[1].header)
      reader.expectBoundary(nextOffset!)
    },
    TEST_TIMEOUT,
  )
})
