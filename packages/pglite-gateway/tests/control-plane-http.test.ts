// M2c: the era-row + pin HTTP routes that close the fleet-mode gap. Every
// control-plane operation the cell-server's GatewayHandle uses in in-process
// mode must be reachable over HTTP with 1:1 semantics. This exercises each
// new route against a running GatewayServer:
//   - POST /v1/db/:id/era           (addEra)          round-trip via GET
//   - GET  /v1/db/:id/era/:ordinal  (eraByOrdinal)    incl. 404
//   - PUT  /v1/db/:id/pin           (upsertPin)       round-trip via GET pins
//   - DELETE /v1/db/:id/pin/:pinId  (deletePin)
//   - GET  /v1/db/:id/pins          (livePins)
//   - POST /v1/db/:id/pins/expire   (expirePins TTL sweep)
// Plus: the guarded era/advance returns advanced:false on a wrong `from`.

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import { GatewayCore } from '../src/core'
import { GatewayServer } from '../src/http'
import type { Manifest } from '../src/core'

const TEST_TIMEOUT = 120_000

let root: string
let core: GatewayCore
let server: GatewayServer
let url: string
let dbId: string
let manifest: Manifest

async function postJson(path: string, body: unknown): Promise<Response> {
  return fetch(url + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
}

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-gw-cp-http-'))
  core = new GatewayCore({ dataRoot: join(root, 'gateway') })
  await core.start()
  server = new GatewayServer({ core })
  url = await server.listen(0)

  const res = await fetch(`${url}/v1/db`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: 'cp-http' }),
  })
  manifest = (await res.json()) as Manifest
  dbId = manifest.databaseId
}, TEST_TIMEOUT)

afterAll(async () => {
  await server?.close()
  await core?.stop()
  rmSync(root, { recursive: true, force: true })
})

describe('control-plane HTTP routes (M2c fleet-mode completion)', () => {
  it(
    'GET era/:ordinal returns the era-1 row created by createDatabase',
    async () => {
      const res = await fetch(`${url}/v1/db/${dbId}/era/1`)
      expect(res.status).toBe(200)
      const row = (await res.json()) as {
        ordinal: number
        eraId: string
        path: string
        baseOffset: string
        baseLsn: string
        sealed: boolean
        sealedFinalOffset: string | null
        nextEraOrdinal: number | null
      }
      expect(row.ordinal).toBe(1)
      expect(row.eraId).toBe(manifest.era.id)
      expect(row.path).toBe(manifest.era.path)
      expect(row.baseOffset).toBe(manifest.era.baseOffset)
      expect(row.baseLsn).toBe(manifest.era.baseLsn)
      expect(row.sealed).toBe(false)
      expect(row.sealedFinalOffset).toBeNull()
      expect(row.nextEraOrdinal).toBeNull()
    },
    TEST_TIMEOUT,
  )

  it(
    'GET era/:ordinal for a missing ordinal is 404; non-integer is 400',
    async () => {
      expect((await fetch(`${url}/v1/db/${dbId}/era/999`)).status).toBe(404)
      expect((await fetch(`${url}/v1/db/${dbId}/era/nope`)).status).toBe(400)
    },
    TEST_TIMEOUT,
  )

  it(
    'POST era inserts an era-2 row that reads back over HTTP (addEra round-trip)',
    async () => {
      const era2 = {
        ordinal: 2,
        eraId: '000002-DEADBEEF',
        path: '/era/000002-DEADBEEF',
        baseOffset: manifest.era.baseOffset,
        baseLsn: manifest.era.baseLsn,
      }
      const res = await postJson(`/v1/db/${dbId}/era`, era2)
      expect(res.status).toBe(204)

      const back = await fetch(`${url}/v1/db/${dbId}/era/2`)
      expect(back.status).toBe(200)
      const row = (await back.json()) as { eraId: string; path: string }
      expect(row.eraId).toBe(era2.eraId)
      expect(row.path).toBe(era2.path)
    },
    TEST_TIMEOUT,
  )

  it(
    'POST era/advance is guarded: wrong `from` returns advanced:false, right `from` advances',
    async () => {
      // db is at current_era_ordinal 1 (createDatabase). Wrong from=5 -> no-op.
      const wrong = await (
        await postJson(`/v1/db/${dbId}/era/advance`, { from: 5, to: 6 })
      ).json()
      expect(wrong).toEqual({ advanced: false })

      const right = await (
        await postJson(`/v1/db/${dbId}/era/advance`, { from: 1, to: 2 })
      ).json()
      expect(right).toEqual({ advanced: true })

      // Re-running the same transition now fails (already at 2).
      const again = await (
        await postJson(`/v1/db/${dbId}/era/advance`, { from: 1, to: 2 })
      ).json()
      expect(again).toEqual({ advanced: false })
    },
    TEST_TIMEOUT,
  )

  it(
    'PUT pin upserts a pin that appears in GET pins; DELETE removes it',
    async () => {
      const pinId = randomUUID()
      const put = await fetch(`${url}/v1/db/${dbId}/pin`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: pinId,
          kind: 'gc-pin',
          holder: 'host-1',
          pinnedOffset: manifest.era.baseOffset,
          pinnedLsn: manifest.era.baseLsn,
          expiresAt: new Date(Date.now() + 60_000).toISOString(),
        }),
      })
      expect(put.status).toBe(204)

      const pins = (await (
        await fetch(`${url}/v1/db/${dbId}/pins`)
      ).json()) as { id: string; holder: string; pinnedLsn: string }[]
      const hit = pins.find((p) => p.id === pinId)
      expect(hit).toBeDefined()
      expect(hit!.holder).toBe('host-1')
      expect(hit!.pinnedLsn).toBe(manifest.era.baseLsn)

      // Upsert (same id) updates the holder rather than duplicating.
      await fetch(`${url}/v1/db/${dbId}/pin`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: pinId,
          kind: 'gc-pin',
          holder: 'host-2',
          pinnedOffset: manifest.era.baseOffset,
          pinnedLsn: manifest.era.baseLsn,
          expiresAt: new Date(Date.now() + 60_000).toISOString(),
        }),
      })
      const pins2 = (await (
        await fetch(`${url}/v1/db/${dbId}/pins`)
      ).json()) as { id: string; holder: string }[]
      expect(pins2.filter((p) => p.id === pinId)).toHaveLength(1)
      expect(pins2.find((p) => p.id === pinId)!.holder).toBe('host-2')

      const del = await fetch(`${url}/v1/db/${dbId}/pin/${pinId}`, {
        method: 'DELETE',
      })
      expect(del.status).toBe(204)
      const pins3 = (await (
        await fetch(`${url}/v1/db/${dbId}/pins`)
      ).json()) as { id: string }[]
      expect(pins3.some((p) => p.id === pinId)).toBe(false)
    },
    TEST_TIMEOUT,
  )

  it(
    'POST pins/expire sweeps a TTL-expired pin (expirePins over HTTP)',
    async () => {
      const pinId = randomUUID()
      await fetch(`${url}/v1/db/${dbId}/pin`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: pinId,
          kind: 'gc-pin',
          holder: 'host-x',
          pinnedOffset: manifest.era.baseOffset,
          pinnedLsn: manifest.era.baseLsn,
          // Already expired.
          expiresAt: new Date(Date.now() - 1_000).toISOString(),
        }),
      })

      // An expired pin never appears in livePins (expires_at > now() filter).
      const live = (await (
        await fetch(`${url}/v1/db/${dbId}/pins`)
      ).json()) as { id: string }[]
      expect(live.some((p) => p.id === pinId)).toBe(false)

      // The sweep deletes it and reports at least one swept.
      const swept = (await (
        await postJson(`/v1/db/${dbId}/pins/expire`, {})
      ).json()) as { swept: number }
      expect(swept.swept).toBeGreaterThanOrEqual(1)

      // A second sweep finds nothing new from this pin.
      const again = (await (
        await postJson(`/v1/db/${dbId}/pins/expire`, {})
      ).json()) as { swept: number }
      expect(again.swept).toBe(0)
    },
    TEST_TIMEOUT,
  )
})
