// GET /v1/db/:id/stats — operator wake-byte counter source. When the latest
// checkpoint ref is a v3 manifest, reports {eagerBytes, lazyBytes, fileCount}
// (sizes summed by kind); otherwise the fields are omitted.

import { describe, it, expect, afterEach } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { GatewayCore } from '../src/core'
import { GatewayServer } from '../src/http'
import { readCheckpointManifest } from '../src/checkpoint-object'

const TEST_TIMEOUT = 120_000

let root: string
let core: GatewayCore
let server: GatewayServer

async function bootV3(): Promise<void> {
  root = mkdtempSync(join(tmpdir(), 'pgl-gw-stats-'))
  core = new GatewayCore({
    dataRoot: join(root, 'gateway'),
    checkpointFormat: 3,
  })
  await core.start()
  server = new GatewayServer({ core })
}

afterEach(async () => {
  await core?.stop()
  rmSync(root, { recursive: true, force: true })
})

describe('GET /v1/db/:id/stats', () => {
  it(
    'reports eager/lazy byte split + file count for a v3 latest checkpoint',
    async () => {
      await bootV3()
      const m = await core.createDatabase('stats-v3')
      const manifest = await readCheckpointManifest(
        m.checkpoint.ref,
        core.objectGetStore,
      )
      let expEager = 0
      let expLazy = 0
      for (const f of manifest.files) {
        if (f.kind === 'lazy') expLazy += f.size
        else expEager += f.size
      }

      const res = await server.app.request(`/v1/db/${m.databaseId}/stats`)
      expect(res.status).toBe(200)
      const body = (await res.json()) as {
        latestCheckpoint: {
          eagerBytes?: number
          lazyBytes?: number
          fileCount?: number
        }
      }
      expect(body.latestCheckpoint.eagerBytes).toBe(expEager)
      expect(body.latestCheckpoint.lazyBytes).toBe(expLazy)
      expect(body.latestCheckpoint.fileCount).toBe(manifest.files.length)
      expect(body.latestCheckpoint.eagerBytes).toBeGreaterThan(0)
    },
    TEST_TIMEOUT,
  )

  it(
    'omits the per-kind fields when the latest checkpoint is not a v3 manifest',
    async () => {
      await bootV3()
      const m = await core.createDatabase('stats-v2ish')
      // Register a plain-blob (non-manifest) checkpoint as the latest.
      const { ref } = await core.putObject(
        new TextEncoder().encode('not a manifest'),
      )
      // High LSN so this blob checkpoint is the LATEST (above createDatabase C0).
      await core.registerCheckpoint(m.databaseId, {
        lsn: 'FF/FF000000',
        snapEnd: 'FF/FF000000',
        streamOffset: '0000000000000000_00000000FFFF0000',
        objectRef: ref,
      })
      const res = await server.app.request(`/v1/db/${m.databaseId}/stats`)
      expect(res.status).toBe(200)
      const body = (await res.json()) as {
        latestCheckpoint: Record<string, unknown>
      }
      expect(body.latestCheckpoint.eagerBytes).toBeUndefined()
      expect(body.latestCheckpoint.fileCount).toBeUndefined()
    },
    TEST_TIMEOUT,
  )
})
