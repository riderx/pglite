// §14.5 THE STATELESSNESS INVARIANT, tested directly: two GatewayServer
// instances over ONE GatewayCore backing (same object store, control plane, and
// DS server) are indistinguishable. We run flow (a)-(e) alternating EVERY
// request between the two instances (round-robin), assert identical outcomes,
// then kill one instance mid-flow and finish on the other. If any gateway
// feature ever held authoritative state, one of these steps would diverge.

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import {
  DsStreamClient,
  EraTailer,
  Committer,
  Cell,
  hydrateDatadir,
  parseLsn,
} from '@electric-sql/pglite-cell'
import type { FetchImpl } from '@electric-sql/pglite-cell'
import { GatewayCore } from '../src/core'
import { GatewayServer } from '../src/http'
import { extractDatadir } from '../src/checkpoint-object'
import type { Manifest } from '../src/core'

const TEST_TIMEOUT = 120_000

let root: string
let core: GatewayCore
let serverA: GatewayServer
let serverB: GatewayServer
let urls: string[]

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-gw-stateless-'))
  // Pinned to v2 (tar) checkpoints: this suite extracts the object bytes
  // directly via extractDatadir. The W4 default is v3 (manifest); v3 extract
  // is covered by the checkpoint-v3 suite.
  core = new GatewayCore({
    dataRoot: join(root, 'gateway'),
    checkpointFormat: 2,
  })
  await core.start()
  // Two independent HTTP fronts sharing the ONE core's backing stores.
  serverA = new GatewayServer({ core })
  serverB = new GatewayServer({ core })
  urls = [await serverA.listen(0), await serverB.listen(0)]
}, TEST_TIMEOUT)

afterAll(async () => {
  await serverA?.close().catch(() => undefined)
  await serverB?.close().catch(() => undefined)
  await core?.stop()
  rmSync(root, { recursive: true, force: true })
})

describe('statelessness: two gateways over one core are indistinguishable', () => {
  it(
    'round-robins every request across both instances through a full commit flow, then survives killing one',
    async () => {
      // A round-robin fetch that alternates instances on EVERY call. Paths are
      // instance-relative (/v1/...), so we just swap the origin.
      let rr = 0
      const liveUrls = [...urls]
      const roundRobinFetch: FetchImpl = async (input, init) => {
        const origin = liveUrls[rr % liveUrls.length]
        rr++
        const path = new URL(String(input)).pathname
        const search = new URL(String(input)).search
        return fetch(origin + path + search, init)
      }

      // (a) create a database — hits instance A (rr=0).
      const createRes = await roundRobinFetch(`${urls[0]}/v1/db`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'stateless' }),
      })
      expect(createRes.status).toBe(201)
      const manifest = (await createRes.json()) as Manifest
      const dbId = manifest.databaseId

      // (b) fetch manifest from the OTHER instance — must be identical.
      const manRes = await roundRobinFetch(`${urls[0]}/v1/db/${dbId}/manifest`)
      const manifest2 = (await manRes.json()) as Manifest
      expect(manifest2).toEqual(manifest)

      // (c) download + extract the checkpoint object (round-robined).
      const objRes = await roundRobinFetch(
        `${urls[0]}/v1/objects/${manifest.checkpoint.ref}`,
      )
      const tarBytes = new Uint8Array(await objRes.arrayBuffer())
      const checkpointDir = join(root, 'checkpoint')
      await extractDatadir(tarBytes, checkpointDir)

      // (d) a DsStreamClient whose fetch round-robins the two gateways. The
      // stream base path is instance-relative, so the client cannot tell which
      // gateway serves any given request.
      const streamBase = `${urls[0]}/v1/db/${dbId}/stream`
      const client = new DsStreamClient(streamBase, roundRobinFetch)

      const snapEnd = parseLsn(manifest.checkpoint.snapEnd)
      const workDir = join(root, 'work')
      hydrateDatadir(checkpointDir, workDir)
      const tailer = new EraTailer(client, {
        path: manifest.era.path,
        eraId: manifest.era.id,
        ordinal: manifest.era.ordinal,
        baseOffset: manifest.era.baseOffset,
        baseLsn: snapEnd,
      })
      expect(await tailer.catchUp()).toBe(0)
      const committer = await Committer.create({
        client,
        era: {
          path: manifest.era.path,
          id: manifest.era.id,
          ordinal: manifest.era.ordinal,
        },
        tailer,
        journalDir: join(root, 'journal'),
      })

      // (e) commit through the round-robin — the CAS append and the follow-up
      // reads land on alternating instances, yet resolve to one linear stream.
      const cell = await Cell.open(workDir, { expectedHeadLsn: snapEnd })
      await cell.db.exec(
        `create table t (id serial primary key, v text);
         insert into t (v) values ('one')`,
      )
      const slice1 = await cell.captureSlice()
      const res1 = await committer.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...slice1!,
      })
      expect(res1.landed).toBe(true)
      cell.confirmPublished(slice1!.endLsn)

      // Kill instance B mid-flow; the client keeps going on A alone.
      await serverB.close()
      liveUrls.splice(1, 1) // remove B from the round-robin
      rr = 0

      await cell.db.exec(`insert into t (v) values ('two')`)
      const slice2 = await cell.captureSlice()
      const res2 = await committer.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...slice2!,
      })
      expect(res2.landed).toBe(true)
      cell.confirmPublished(slice2!.endLsn)

      // Both rows landed on the single shared stream, regardless of which
      // gateway carried which request.
      const rows = (
        await cell.db.query<{ v: string }>(`select v from t order by id`)
      ).rows
      expect(rows).toEqual([{ v: 'one' }, { v: 'two' }])
      await cell.db.close()
    },
    TEST_TIMEOUT,
  )
})
