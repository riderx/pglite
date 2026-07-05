// THE EXIT TEST (M1b): createDatabase -> attach via the gateway over HTTP ->
// commits -> conflict/re-execute -> cold re-attach from checkpoint 0 + tail.
//
// The whole cell commit engine (EraTailer + Committer + Cell + materialize)
// runs UNCHANGED against a DsStreamClient whose baseUrl is
// `<gatewayUrl>/v1/db/<id>/stream` — the acceptance criterion that CAS commits
// work through the stateless proxy exactly as they do against a raw DS server.

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
  materializeAtHead,
  hydrateDatadir,
  parseLsn,
} from '@electric-sql/pglite-cell'
import type { CommitResult } from '@electric-sql/pglite-cell'
import { GatewayCore } from '../src/core'
import { GatewayServer } from '../src/http'
import { extractDatadir } from '../src/checkpoint-object'
import type { Manifest } from '../src/core'

const TEST_TIMEOUT = 120_000

let root: string
let core: GatewayCore
let server: GatewayServer
let gatewayUrl: string
let manifest: Manifest
let dbId: string

function assertLanded(
  r: CommitResult,
): asserts r is { landed: true; offset: string; nextOffset: string } {
  expect(r.landed).toBe(true)
}

/** Build the era-tailer + committer + client trio pointed at the gateway. */
function eraCoords(m: Manifest) {
  return {
    path: m.era.path, // relative — joined onto the client base URL
    id: m.era.id,
    ordinal: m.era.ordinal,
  }
}

function newClient(): DsStreamClient {
  return new DsStreamClient(`${gatewayUrl}/v1/db/${dbId}/stream`)
}

function newTailer(m: Manifest, client: DsStreamClient): EraTailer {
  return new EraTailer(client, {
    path: m.era.path,
    eraId: m.era.id,
    ordinal: m.era.ordinal,
    baseOffset: m.era.baseOffset,
    baseLsn: parseLsn(m.era.baseLsn),
  })
}

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-gw-e2e-'))
  // Pinned to v2 (tar) checkpoints: this suite extracts the object bytes
  // directly via extractDatadir. The W4 default is v3 (manifest); v3 extract
  // is covered by the checkpoint-v3 suite.
  core = new GatewayCore({ dataRoot: join(root, 'gateway'), checkpointFormat: 2 })
  await core.start()
  server = new GatewayServer({ core })
  gatewayUrl = await server.listen(0)
}, TEST_TIMEOUT)

afterAll(async () => {
  await server?.close()
  await core?.stop()
  rmSync(root, { recursive: true, force: true })
})

describe('gateway E2E: create -> attach -> commit -> conflict -> cold re-attach', () => {
  it(
    '(a) POST /v1/db creates a database and returns a manifest',
    async () => {
      const res = await fetch(`${gatewayUrl}/v1/db`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'e2e' }),
      })
      expect(res.status).toBe(201)
      manifest = (await res.json()) as Manifest
      dbId = manifest.databaseId
      expect(manifest.name).toBe('e2e')
      expect(manifest.era.ordinal).toBe(1)
      expect(manifest.era.path).toMatch(/^\/era\/000001-/)
      expect(manifest.checkpoint.ref).toMatch(/^sha256:/)
      expect(manifest.checkpoint.snapEnd).toBe(manifest.era.baseLsn)
    },
    TEST_TIMEOUT,
  )

  it(
    '(b) GET manifest over HTTP matches the created manifest',
    async () => {
      const res = await fetch(`${gatewayUrl}/v1/db/${dbId}/manifest`)
      expect(res.status).toBe(200)
      const got = (await res.json()) as Manifest
      expect(got).toEqual(manifest)

      const list = await (await fetch(`${gatewayUrl}/v1/db`)).json()
      expect((list as { id: string }[]).some((d) => d.id === dbId)).toBe(true)

      // HEAD through the stream proxy forwards Stream-Next-Offset verbatim (the
      // era base offset, since only the O frame has landed).
      const headRes = await fetch(
        `${gatewayUrl}/v1/db/${dbId}/stream${manifest.era.path}`,
        { method: 'HEAD' },
      )
      expect(headRes.status).toBe(200)
      expect(headRes.headers.get('Stream-Next-Offset')).toBe(
        manifest.era.baseOffset,
      )
    },
    TEST_TIMEOUT,
  )

  let workA: string
  let snapEnd: bigint
  let clientA: DsStreamClient
  let tailerA: EraTailer
  let committerA: Committer
  let cellA: Cell
  let aEnd: bigint

  it(
    '(c) download the checkpoint object via /v1/objects/:ref and extract it, then attach and commit',
    async () => {
      snapEnd = parseLsn(manifest.checkpoint.snapEnd)

      // (c) download + extract the checkpoint object over HTTP.
      const objRes = await fetch(
        `${gatewayUrl}/v1/objects/${manifest.checkpoint.ref}`,
      )
      expect(objRes.status).toBe(200)
      expect(objRes.headers.get('Cache-Control')).toContain('immutable')
      const tarBytes = new Uint8Array(await objRes.arrayBuffer())
      const checkpointDir = join(root, 'checkpoint')
      await extractDatadir(tarBytes, checkpointDir)

      // (d) construct a DsStreamClient at the gateway stream path.
      workA = join(root, 'workA')
      hydrateDatadir(checkpointDir, workA)
      clientA = newClient()
      tailerA = newTailer(manifest, clientA)
      expect(await tailerA.catchUp()).toBe(0) // fresh era: empty tail

      committerA = await Committer.create({
        client: clientA,
        era: eraCoords(manifest),
        tailer: tailerA,
        journalDir: join(root, 'journalA'),
      })

      // Cell.open at snapEnd: the zero-boot-WAL assert must pass through the
      // whole HTTP + proxy chain (nothing above this line touched the datadir
      // except the extracted checkpoint).
      cellA = await Cell.open(workA, { expectedHeadLsn: snapEnd })

      // (e) create schema + insert as the first commit through the proxy. The
      // database created by the gateway is empty (createDatabase cuts an empty
      // checkpoint 0), so the schema rides in cell A's first slice — later
      // joiners get it by materializing A's tail.
      await cellA.db.exec(
        `create table t (id serial primary key, v text, cell text);
         insert into t (v, cell) values ('from-a', 'A')`,
      )
      const slice = await cellA.captureSlice()
      expect(slice).not.toBeNull()
      expect(slice!.baseLsn).toBe(snapEnd)
      const res = await committerA.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...slice!,
      })
      assertLanded(res)
      expect(res.offset).toBe(manifest.era.baseOffset)
      cellA.confirmPublished(slice!.endLsn)
      aEnd = slice!.endLsn
      expect(tailerA.head.lsn).toBe(aEnd)
    },
    TEST_TIMEOUT,
  )

  let bFinalEnd: bigint

  it(
    '(f) a second cell from the same checkpoint: stale commit -> seq-conflict through the proxy -> catch-up + re-execute -> landed',
    async () => {
      const checkpointDir = join(root, 'checkpoint')
      const workB = join(root, 'workB')
      hydrateDatadir(checkpointDir, workB)
      const clientB = newClient()
      const tailerB = newTailer(manifest, clientB) // deliberately stale
      const committerB = await Committer.create({
        client: clientB,
        era: eraCoords(manifest),
        tailer: tailerB,
        journalDir: join(root, 'journalB'),
      })
      const cellB = await Cell.open(workB, { expectedHeadLsn: snapEnd })
      // workB hydrated the EMPTY checkpoint (no schema), and its stale tailer
      // has not seen A's schema commit — so B speculatively creates the schema
      // itself as part of its doomed transaction, exactly the same-base race
      // the design serializes.
      await cellB.db.exec(
        `create table t (id serial primary key, v text, cell text);
         insert into t (v, cell) values ('doomed', 'B')`,
      )
      const doomed = await cellB.captureSlice()
      expect(doomed!.baseLsn).toBe(snapEnd)

      // Stale base -> the proxy forwards the CAS append, the DS server rejects
      // it (seq-conflict). The gateway NEVER enforced CAS; the DS server did.
      const staleRes = await committerB.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...doomed!,
      })
      expect(staleRes).toEqual({ landed: false })
      await cellB.db.close()

      // Catch up, materialize at head, publish the sync slice, re-execute.
      expect(await tailerB.catchUp()).toBe(1)
      expect(tailerB.head.lsn).toBe(aEnd)

      const workB2 = join(root, 'workB2')
      hydrateDatadir(checkpointDir, workB2)
      const mat = await materializeAtHead({
        baseDir: workB2,
        slices: tailerB.slicesSince(snapEnd),
      })
      expect(mat.syncSlice).not.toBeNull()
      const syncRes = await committerB.commitSlice({
        commitId: randomUUID(),
        kind: 'sync',
        ...mat.syncSlice!,
      })
      assertLanded(syncRes)

      const cellB2 = await Cell.open(workB2, { expectedHeadLsn: mat.headLsn })
      await cellB2.db.exec(`insert into t (v, cell) values ('from-b', 'B')`)
      const slice = await cellB2.captureSlice()
      expect(slice!.baseLsn).toBe(mat.headLsn)
      const res = await committerB.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...slice!,
      })
      assertLanded(res)
      cellB2.confirmPublished(slice!.endLsn)
      bFinalEnd = slice!.endLsn
      await cellB2.db.close()
    },
    TEST_TIMEOUT,
  )

  it(
    '(g) cold re-attach: a third workdir hydrates + materializes the full tail and sees both rows',
    async () => {
      // Re-fetch the manifest and checkpoint object fresh over HTTP (a truly
      // cold joiner knows only the database id).
      const m = (await (
        await fetch(`${gatewayUrl}/v1/db/${dbId}/manifest`)
      ).json()) as Manifest
      const objRes = await fetch(`${gatewayUrl}/v1/objects/${m.checkpoint.ref}`)
      const tarBytes = new Uint8Array(await objRes.arrayBuffer())
      const coldCkpt = join(root, 'coldCkpt')
      await extractDatadir(tarBytes, coldCkpt)

      const oracleDir = join(root, 'oracle')
      hydrateDatadir(coldCkpt, oracleDir)
      const client = newClient()
      const tailer = newTailer(m, client)
      await tailer.catchUp()
      expect(tailer.head.lsn).toBe(bFinalEnd)

      const mat = await materializeAtHead({
        baseDir: oracleDir,
        slices: tailer.slicesSince(parseLsn(m.checkpoint.snapEnd)),
      })
      const cell = await Cell.open(oracleDir, { expectedHeadLsn: mat.headLsn })
      const rows = (
        await cell.db.query<{ v: string; cell: string }>(
          `select v, cell from t order by id`,
        )
      ).rows
      expect(rows).toEqual([
        { v: 'from-a', cell: 'A' },
        { v: 'from-b', cell: 'B' },
      ])
      await cell.db.close()
    },
    TEST_TIMEOUT,
  )

  it(
    '(h) frame validation: POST garbage bytes to the stream proxy -> 422',
    async () => {
      const res = await fetch(
        `${gatewayUrl}/v1/db/${dbId}/stream${manifest.era.path}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/octet-stream' },
          body: new Uint8Array([0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02]),
        },
      )
      expect(res.status).toBe(422)
      const text = await res.text()
      expect(text.length).toBeGreaterThan(0)
    },
    TEST_TIMEOUT,
  )

  it(
    '(i) 32 MiB cap: an oversized append body -> 422',
    async () => {
      const tooBig = new Uint8Array(32 * 1024 * 1024 + 1)
      // Give it a valid-looking first byte so the reason is the size cap.
      tooBig[0] = 'W'.charCodeAt(0)
      const res = await fetch(
        `${gatewayUrl}/v1/db/${dbId}/stream${manifest.era.path}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/octet-stream' },
          body: tooBig,
        },
      )
      expect(res.status).toBe(422)
      expect(await res.text()).toContain('exceeds')
    },
    TEST_TIMEOUT,
  )
})
