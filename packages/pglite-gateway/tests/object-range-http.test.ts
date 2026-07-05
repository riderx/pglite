// Ranged object reads over HTTP: `GET /v1/objects/:ref` honors a single-range
// `Range: bytes=a-b` header (206 + Content-Range), 416 on an unsatisfiable
// range, 200 (full body) when absent, plus the immutable cache header. The
// `fetchObjectRange` client helper (W3's host cache) is exercised end to end.

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { GatewayCore } from '../src/core'
import { GatewayServer, fetchObjectRange, parseByteRange } from '../src/http'

const TEST_TIMEOUT = 60_000

let root: string
let core: GatewayCore
let server: GatewayServer
let url: string
let ref: string
const payload = new TextEncoder().encode(
  '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ',
)

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-gw-range-'))
  core = new GatewayCore({ dataRoot: join(root, 'data') })
  await core.start()
  server = new GatewayServer({ core })
  url = await server.listen(0)
  ref = (await core.putObject(payload)).ref
}, TEST_TIMEOUT)

afterAll(async () => {
  await server.close()
  await core.stop()
  rmSync(root, { recursive: true, force: true })
})

describe('parseByteRange', () => {
  it('parses a-b, a-, and -n forms; flags invalid/unsatisfiable', () => {
    expect(parseByteRange('bytes=0-9', 100)).toEqual({ start: 0, end: 9 })
    expect(parseByteRange('bytes=10-', 100)).toEqual({ start: 10, end: 99 })
    expect(parseByteRange('bytes=-5', 100)).toEqual({ start: 95, end: 99 })
    // clamp end to size-1
    expect(parseByteRange('bytes=90-999', 100)).toEqual({ start: 90, end: 99 })
    // past EOF -> unsatisfiable
    expect(parseByteRange('bytes=200-300', 100)).toBe('unsatisfiable')
    // garbage / multi-range -> invalid
    expect(parseByteRange('bytes=abc', 100)).toBe('invalid')
    expect(parseByteRange('bytes=0-9,20-29', 100)).toBe('invalid')
    expect(parseByteRange('bytes=-', 100)).toBe('invalid')
  })
})

describe('GET /v1/objects/:ref with Range', () => {
  it(
    'no Range -> 200 full body + immutable cache header',
    async () => {
      const res = await fetch(`${url}/v1/objects/${ref}`)
      expect(res.status).toBe(200)
      expect(res.headers.get('Cache-Control')).toContain('immutable')
      expect(res.headers.get('Accept-Ranges')).toBe('bytes')
      const body = new Uint8Array(await res.arrayBuffer())
      expect(Buffer.from(body)).toEqual(Buffer.from(payload))
    },
    TEST_TIMEOUT,
  )

  it(
    'Range -> 206 + Content-Range + exact range bytes + immutable header',
    async () => {
      const res = await fetch(`${url}/v1/objects/${ref}`, {
        headers: { Range: 'bytes=10-19' },
      })
      expect(res.status).toBe(206)
      expect(res.headers.get('Content-Range')).toBe(
        `bytes 10-19/${payload.length}`,
      )
      expect(res.headers.get('Cache-Control')).toContain('immutable')
      const body = new Uint8Array(await res.arrayBuffer())
      expect(Buffer.from(body)).toEqual(Buffer.from(payload.subarray(10, 20)))
    },
    TEST_TIMEOUT,
  )

  it(
    'unsatisfiable Range -> 416 + Content-Range: bytes */size',
    async () => {
      const res = await fetch(`${url}/v1/objects/${ref}`, {
        headers: {
          Range: `bytes=${payload.length + 10}-${payload.length + 20}`,
        },
      })
      expect(res.status).toBe(416)
      expect(res.headers.get('Content-Range')).toBe(`bytes */${payload.length}`)
    },
    TEST_TIMEOUT,
  )

  it(
    'fetchObjectRange returns the exact chunk bytes through the server',
    async () => {
      const chunk = await fetchObjectRange(url, ref, 5, 8)
      expect(Buffer.from(chunk)).toEqual(Buffer.from(payload.subarray(5, 13)))
      // A range past EOF yields the available tail (server clamps).
      const tail = await fetchObjectRange(url, ref, payload.length - 3, 100)
      expect(Buffer.from(tail)).toEqual(
        Buffer.from(payload.subarray(payload.length - 3)),
      )
    },
    TEST_TIMEOUT,
  )
})
