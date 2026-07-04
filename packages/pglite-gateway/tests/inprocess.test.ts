// In-process embedding (the Supabase-lite shape): GatewayCore used directly,
// no HTTP. createDatabase -> streamClientFor -> one commit round-trip.

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import {
  EraTailer,
  Committer,
  Cell,
  hydrateDatadir,
  parseLsn,
} from '@electric-sql/pglite-cell'
import { GatewayCore } from '../src/core'
import { extractDatadir } from '../src/checkpoint-object'

const TEST_TIMEOUT = 120_000

let root: string
let core: GatewayCore

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-gw-inproc-'))
  core = new GatewayCore({ dataRoot: join(root, 'gateway') })
  await core.start()
}, TEST_TIMEOUT)

afterAll(async () => {
  await core?.stop()
  rmSync(root, { recursive: true, force: true })
})

describe('in-process GatewayCore embedding', () => {
  it(
    'createDatabase + streamClientFor + one commit round-trip, no HTTP',
    async () => {
      const manifest = await core.createDatabase('inproc')
      const dbId = manifest.databaseId

      // Manifest is re-readable from the core.
      const again = await core.getManifest(dbId)
      expect(again).toEqual(manifest)

      // Objects are reachable directly.
      const tarBytes = await core.getObject(manifest.checkpoint.ref)
      const checkpointDir = join(root, 'checkpoint')
      await extractDatadir(tarBytes, checkpointDir)

      // In-process stream client scoped to this database.
      const client = core.streamClientFor(dbId)
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

      const cell = await Cell.open(workDir, { expectedHeadLsn: snapEnd })
      await cell.db.exec(
        `create table t (id serial primary key, v text);
         insert into t (v) values ('hello')`,
      )
      const slice = await cell.captureSlice()
      expect(slice).not.toBeNull()
      expect(slice!.baseLsn).toBe(snapEnd)
      const res = await committer.commitSlice({
        commitId: randomUUID(),
        kind: 'commit',
        ...slice!,
      })
      expect(res.landed).toBe(true)
      cell.confirmPublished(slice!.endLsn)

      const rows = (
        await cell.db.query<{ v: string }>(`select v from t order by id`)
      ).rows
      expect(rows).toEqual([{ v: 'hello' }])
      await cell.db.close()

      // The commit is visible on the stream to a fresh tailer.
      const tailer2 = new EraTailer(core.streamClientFor(dbId), {
        path: manifest.era.path,
        eraId: manifest.era.id,
        ordinal: manifest.era.ordinal,
        baseOffset: manifest.era.baseOffset,
        baseLsn: snapEnd,
      })
      expect(await tailer2.catchUp()).toBe(1)
      expect(tailer2.head.lsn).toBe(slice!.endLsn)
    },
    TEST_TIMEOUT,
  )
})
