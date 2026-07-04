// GcExecutor (M2): the four sweeps, each honoring grace, fork lineage, and
// pins.
//   (a) orphan attempt swept after grace, kept within grace;
//   (b) sealed-era stream deleted once a covering checkpoint exists; kept
//       while a child fork's offset lies inside it;
//   (c) old checkpoint rows pruned, latest kept, child-referenced object
//       survives parent checkpoint-row deletion;
//   (d) pins block sweeps until expired.

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { encodeAppend, INITIAL_OFFSET_TOKEN } from '@electric-sql/pglite-cell'
import type { OFrame } from '@electric-sql/pglite-cell'
import { GatewayCore } from '../src/core'

const TEST_TIMEOUT = 120_000

let root: string
let core: GatewayCore

beforeEach(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-gw-gc-'))
  core = new GatewayCore({ dataRoot: join(root, 'gateway') })
  await core.start()
})

afterEach(async () => {
  await core?.stop()
  rmSync(root, { recursive: true, force: true })
})

/** Minimal O-frame stream create at a given path, returning its tail offset. */
async function createEraStream(
  dbId: string,
  path: string,
  eraId: string,
  ordinal: number,
  baseLsn: string,
): Promise<string> {
  const oFrame: OFrame = {
    type: 'O',
    header: {
      v: 1,
      eraId,
      expectedOffset: INITIAL_OFFSET_TOKEN,
      ordinal,
      prevEraId: null,
      prevEraUrl: null,
      baseOffset: INITIAL_OFFSET_TOKEN,
      baseLsn,
      snapEnd: baseLsn,
      checkpointRef: 'sha256:none',
    },
  }
  const client = core.streamClientFor(dbId)
  const created = await client.createStream(path, {
    body: encodeAppend([oFrame]),
  })
  return created.nextOffset
}

describe('GcExecutor', () => {
  it(
    '(a) orphan era attempts: swept after grace, kept within grace',
    async () => {
      const m = await core.createDatabase('orphan')
      const id = m.databaseId
      // grace 0 -> orphans are immediately eligible.
      await core.setDials(id, { gcGraceMs: 0 })

      // Register an attempt AND actually PUT its stream (the pre-PUT registry).
      const eraId = '000002-ORPH'
      const path = `/era/${eraId}`
      await core.registerEraAttempt(id, { ordinal: 2, eraId, path })
      await createEraStream(id, path, eraId, 2, '0/9000')

      // Within a LARGE grace: kept.
      await core.setDials(id, { gcGraceMs: 3_600_000 })
      let report = await core.runGc(id)
      expect(report.deletedStreams).toBe(0)
      // Stream still present.
      const client = core.streamClientFor(id)
      expect((await client.head(path)).nextOffset).toBeTruthy()

      // With grace 0: swept (stream deleted + row dropped).
      await core.setDials(id, { gcGraceMs: 0 })
      report = await core.runGc(id)
      expect(report.deletedStreams).toBeGreaterThanOrEqual(1)
      expect(await core.catalog.listOrphanAttempts(0)).toHaveLength(0)
    },
    TEST_TIMEOUT,
  )

  it(
    '(b) sealed-era stream: deleted once a covering checkpoint exists; kept while a child fork lies inside it',
    async () => {
      const m = await core.createDatabase('sealed')
      const id = m.databaseId
      await core.setDials(id, { gcGraceMs: 0 })

      // Era 1 already exists from createDatabase. Build a second era stream and
      // seal era 1 with a final LSN, then register a checkpoint that COVERS it.
      const era1 = await core.catalog.currentEra(id)
      const era2Id = '000002-NEXT'
      const era2Path = `/era/${era2Id}`
      const era2Base = await createEraStream(id, era2Path, era2Id, 2, '0/A000')
      await core.catalog.addEra({
        databaseId: id,
        ordinal: 2,
        eraId: era2Id,
        path: era2Path,
        baseOffset: era2Base,
        baseLsn: '0/A000',
      })
      // finalOffset >= the real (post-O) baseOffset so the era range is
      // non-empty and contains a fork/pin at baseOffset.
      await core.catalog.sealEra(id, era1!.ordinal, {
        finalOffset: '9000000000000000_0000000000000100',
        finalLsn: '0/5000',
        nextOrdinal: 2,
      })
      // Covering checkpoint: snapEnd (0/A000) >= era1.finalLsn (0/5000).
      await core.registerCheckpoint(id, {
        lsn: '0/A000',
        snapEnd: '0/A000',
        streamOffset: era2Base,
        objectRef: 'sha256:cover',
      })

      // First: pin era 1 with a live child fork whose offset lies inside its
      // range [baseOffset, finalOffset]. The parent era must be KEPT.
      const childM = await core.createDatabase('sealed-child')
      await core.catalog.insertLineage({
        childId: childM.databaseId,
        parentId: id,
        forkLsn: '0/3000',
        forkOffset: era1!.baseOffset, // inside era 1's range
      })
      let report = await core.runGc(id)
      expect(report.kept['sealed-era:child-fork']).toBeGreaterThanOrEqual(1)
      // Era-1 stream still readable.
      expect(
        (await core.streamClientFor(id).head(era1!.path)).nextOffset,
      ).toBeTruthy()

      // Remove the fork pin (mark child deleted) -> era 1 now deletable.
      await core.catalog.setDatabaseStatus(childM.databaseId, 'deleted')
      report = await core.runGc(id)
      expect(report.deletedStreams).toBeGreaterThanOrEqual(1)
      await expect(core.streamClientFor(id).head(era1!.path)).rejects.toThrow()
    },
    TEST_TIMEOUT,
  )

  it(
    '(c) checkpoint rows: latest kept, old pruned, child-referenced object survives',
    async () => {
      const m = await core.createDatabase('ckpt-gc')
      const id = m.databaseId
      await core.setDials(id, { gcGraceMs: 0 })

      // Parent gets two extra checkpoints; oldest object is shared by a child.
      await core.registerCheckpoint(id, {
        lsn: '0/2000',
        snapEnd: '0/2000',
        streamOffset: '0000000000000000_0000000000000020',
        objectRef: 'sha256:old-shared',
      })
      await core.registerCheckpoint(id, {
        lsn: '0/3000',
        snapEnd: '0/3000',
        streamOffset: '0000000000000000_0000000000000030',
        objectRef: 'sha256:mid',
      })
      // A child references the OLD object via its own checkpoint row.
      const childM = await core.createDatabase('ckpt-gc-child')
      await core.catalog.insertLineage({
        childId: childM.databaseId,
        parentId: id,
        forkLsn: '0/2000',
        forkOffset: '0000000000000000_0000000000000020',
      })
      await core.registerCheckpoint(childM.databaseId, {
        lsn: '0/2000',
        snapEnd: '0/2000',
        streamOffset: '0000000000000000_0000000000000020',
        objectRef: 'sha256:old-shared',
      })

      const before = await core.catalog.checkpointsOf(id)
      expect(before.length).toBeGreaterThanOrEqual(3)
      const latestBefore = before[before.length - 1] // createDatabase ckpt 0

      const report = await core.runGc(id)
      expect(report.deletedCheckpoints).toBeGreaterThanOrEqual(1)

      const after = await core.catalog.checkpointsOf(id)
      // Latest (checkpoint 0 from createDatabase) is kept.
      expect(after[after.length - 1].lsn).toBe(latestBefore.lsn)
      // The child-referenced OLD parent row survives (its object is shared).
      expect(report.kept['checkpoint:child-referenced']).toBeGreaterThanOrEqual(
        1,
      )
      expect(after.some((c) => c.objectRef === 'sha256:old-shared')).toBe(true)
      // The non-referenced middle row was pruned.
      expect(after.some((c) => c.objectRef === 'sha256:mid')).toBe(false)
    },
    TEST_TIMEOUT,
  )

  it(
    '(d) pins block a sealed-era sweep until expired',
    async () => {
      const m = await core.createDatabase('pin-gc')
      const id = m.databaseId
      await core.setDials(id, { gcGraceMs: 0 })

      const era1 = await core.catalog.currentEra(id)
      // finalOffset must be >= the real baseOffset (post-O-frame tail) so era
      // 1's range [baseOffset, finalOffset] is non-empty and contains it.
      const finalOffset = '9000000000000000_0000000000000100'
      await core.catalog.sealEra(id, era1!.ordinal, {
        finalOffset,
        finalLsn: '0/5000',
        nextOrdinal: 2,
      })
      // The createDatabase checkpoint 0 already covers finalLsn (0/5000).

      // A live pin whose offset lies inside era 1's range blocks the sweep.
      await core.catalog.upsertPin({
        id: '33333333-3333-3333-3333-333333333333',
        databaseId: id,
        kind: 'gc-pin',
        holder: 'joiner',
        pinnedOffset: era1!.baseOffset,
        pinnedLsn: '0/1000',
        expiresAt: new Date(Date.now() + 3_600_000),
      })
      let report = await core.runGc(id)
      expect(report.kept['sealed-era:pin']).toBeGreaterThanOrEqual(1)
      expect(
        (await core.streamClientFor(id).head(era1!.path)).nextOffset,
      ).toBeTruthy()

      // Expire the pin -> the sweep proceeds.
      await core.catalog.upsertPin({
        id: '33333333-3333-3333-3333-333333333333',
        databaseId: id,
        kind: 'gc-pin',
        holder: 'joiner',
        pinnedOffset: era1!.baseOffset,
        pinnedLsn: '0/1000',
        expiresAt: new Date(Date.now() - 1000),
      })
      report = await core.runGc(id)
      expect(report.deletedStreams).toBeGreaterThanOrEqual(1)
    },
    TEST_TIMEOUT,
  )
})
