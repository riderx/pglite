// forkDatabase (M2): fork at the parent's latest checkpoint.
//   - child manifest correct (parent ordinal kept, NEW era_id, checkpoint
//     object SHARED by content-address);
//   - child stream readable — its shared prefix carries the parent's frames
//     (read raw via DsStreamClient + PositionCheckedReader from the child's
//     checkpoint offset);
//   - the F frame is announced in the PARENT era;
//   - a second fork of the same parent works.

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  DsStreamClient,
  PositionCheckedReader,
  INITIAL_OFFSET_TOKEN,
} from '@electric-sql/pglite-cell'
import type { Frame } from '@electric-sql/pglite-cell'
import { GatewayCore } from '../src/core'
import type { Manifest } from '../src/core'

const TEST_TIMEOUT = 120_000

let root: string
let core: GatewayCore

beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-gw-fork-'))
  core = new GatewayCore({ dataRoot: join(root, 'gateway') })
  await core.start()
}, TEST_TIMEOUT)

afterAll(async () => {
  await core?.stop()
  rmSync(root, { recursive: true, force: true })
})

/** Read every frame of a stream from `offset` via a position-checked reader. */
async function readAllFrames(
  client: DsStreamClient,
  path: string,
  offset: string,
): Promise<Frame[]> {
  const reader = new PositionCheckedReader(offset)
  const out: Frame[] = []
  let cursor = offset
  for (;;) {
    const res = await client.read(path, { offset: cursor })
    if (res.bytes.length > 0) {
      for (const group of reader.feed(res.bytes)) out.push(...group.frames)
    }
    cursor = res.nextOffset
    if (res.upToDate || res.status === 204) break
  }
  return out
}

describe('forkDatabase', () => {
  let parent: Manifest
  let child: Manifest

  it(
    'forks a database at its latest checkpoint into a correct child manifest',
    async () => {
      parent = await core.createDatabase('fork-parent')
      child = await core.forkDatabase(parent.databaseId, 'fork-child')

      // Child keeps the PARENT's era ordinal, gets a NEW era_id.
      expect(child.era.ordinal).toBe(parent.era.ordinal)
      expect(child.era.id).not.toBe(parent.era.id)
      expect(child.era.path).toMatch(/^\/era\/000001-/)

      // Fork point = parent's latest checkpoint position.
      expect(child.era.baseOffset).toBe(parent.checkpoint.streamOffset)
      expect(child.era.baseLsn).toBe(parent.checkpoint.snapEnd)

      // Checkpoint OBJECT is shared by content-address.
      expect(child.checkpoint.ref).toBe(parent.checkpoint.ref)
      expect(child.checkpoint.snapEnd).toBe(parent.checkpoint.snapEnd)
    },
    TEST_TIMEOUT,
  )

  it(
    'child stream is readable and carries the parent prefix; F frame is in the parent era',
    async () => {
      // The child stream, read from INITIAL, returns the shared parent prefix
      // (the parent era's O frame) — proving the fork PUT stitched the prefix.
      const childClient = core.streamClientFor(child.databaseId)
      const childFrames = await readAllFrames(
        childClient,
        child.era.path,
        INITIAL_OFFSET_TOKEN,
      )
      expect(childFrames.some((f) => f.type === 'O')).toBe(true)

      // The F frame lives in the PARENT era, announcing this child.
      const parentClient = core.streamClientFor(parent.databaseId)
      const parentFrames = await readAllFrames(
        parentClient,
        parent.era.path,
        INITIAL_OFFSET_TOKEN,
      )
      const fFrames = parentFrames.filter((f) => f.type === 'F')
      expect(fFrames.length).toBeGreaterThanOrEqual(1)
      expect(
        fFrames.some(
          (f) =>
            (f.header as { childDatabaseId?: string }).childDatabaseId ===
            child.databaseId,
        ),
      ).toBe(true)
    },
    TEST_TIMEOUT,
  )

  it(
    'a second fork of the same parent works (another F frame, distinct child)',
    async () => {
      const child2 = await core.forkDatabase(parent.databaseId, 'fork-child-2')
      expect(child2.databaseId).not.toBe(child.databaseId)
      expect(child2.era.id).not.toBe(child.era.id)

      const kids = await core.catalog.childrenOf(parent.databaseId)
      expect(kids.map((k) => k.childId).sort()).toEqual(
        [child.databaseId, child2.databaseId].sort(),
      )

      const parentClient = core.streamClientFor(parent.databaseId)
      const parentFrames = await readAllFrames(
        parentClient,
        parent.era.path,
        INITIAL_OFFSET_TOKEN,
      )
      const childIds = parentFrames
        .filter((f) => f.type === 'F')
        .map((f) => (f.header as { childDatabaseId?: string }).childDatabaseId)
      expect(childIds).toContain(child.databaseId)
      expect(childIds).toContain(child2.databaseId)
    },
    TEST_TIMEOUT,
  )
})
