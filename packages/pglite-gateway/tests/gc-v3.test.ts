// GcExecutor v3-manifest correctness (M7-W3 lazy-suite, data-loss grade).
//
// The bug: sweep 4 deleted objects unreferenced by any checkpoint ROW, but a v3
// checkpoint row references only the MANIFEST object — the per-file objects the
// manifest lists are live yet invisible to a naive row scan, so GC deleted them
// while the checkpoint still needed them. These tests reproduce that (a real v3
// checkpoint that stops extracting after GC) and prove the fix + its dedup
// interactions:
//   (1) repro/fix: a lone v3 checkpoint survives GC and still extracts;
//   (2) two v3 checkpoints sharing most refs — pruning the older frees ONLY the
//       delta objects (shared file objects survive via the surviving manifest);
//   (3) mixed v2 + v3 database: GC keeps both worlds' live objects;
//   (4) a v3 fork child (shared manifest object) pins the parent's file objects
//       via the child's own checkpoint row.

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { GatewayCore } from '../src/core'
import {
  extractDatadirV3,
  readCheckpointManifest,
} from '../src/checkpoint-object'

const TEST_TIMEOUT = 120_000

let root: string
let core: GatewayCore

beforeEach(async () => {
  root = mkdtempSync(join(tmpdir(), 'pgl-gw-gcv3-'))
  core = new GatewayCore({
    dataRoot: join(root, 'gateway'),
    checkpointFormat: 3,
  })
  await core.start()
})

afterEach(async () => {
  await core?.stop()
  rmSync(root, { recursive: true, force: true })
})

/** The set of file refs a v3 checkpoint ref transitively depends on. */
async function fileRefsOf(ref: string): Promise<Set<string>> {
  const m = await readCheckpointManifest(ref, core.objectGetStore)
  return new Set(m.files.map((f) => f.ref))
}

describe('GcExecutor v3 manifests', () => {
  it(
    '(1) repro+fix: GC does NOT delete a live v3 checkpoint’s file objects; it still extracts',
    async () => {
      const m = await core.createDatabase('v3-live')
      const id = m.databaseId
      await core.setDials(id, { gcGraceMs: 0 })

      const ckptRef = m.checkpoint.ref
      const manifest = await readCheckpointManifest(
        ckptRef,
        core.objectGetStore,
      )
      expect(manifest.v).toBe(3)
      expect(manifest.files.length).toBeGreaterThan(0)

      await core.runGc(id)

      // The fix: every file object the manifest lists is still present, so a
      // full v3 restore succeeds. (Before the fix this threw ObjectNotFound.)
      const dest = mkdtempSync(join(tmpdir(), 'pgl-gw-gcv3-extract-'))
      try {
        await expect(
          extractDatadirV3(ckptRef, core.objectGetStore, dest),
        ).resolves.toBeDefined()
      } finally {
        rmSync(dest, { recursive: true, force: true })
      }
      // The manifest object itself also survives.
      expect(await core.hasObject(ckptRef)).toBe(true)
    },
    TEST_TIMEOUT,
  )

  it(
    '(2) two v3 checkpoints sharing most refs: pruning the older frees ONLY the delta objects',
    async () => {
      const m = await core.createDatabase('v3-dedup')
      const id = m.databaseId
      await core.setDials(id, { gcGraceMs: 0 })

      const oldRef = m.checkpoint.ref
      const oldFiles = await fileRefsOf(oldRef)

      // A second v3 checkpoint at a higher LSN that shares MOST of the old file
      // objects but adds a small delta (a handful of brand-new file objects).
      // We synthesize it by copying the old manifest and swapping a few refs for
      // freshly-stored unique objects — content-addressing makes the rest shared.
      const oldManifest = await readCheckpointManifest(
        oldRef,
        core.objectGetStore,
      )
      const deltaRefs = new Set<string>()
      const newFiles = await Promise.all(
        oldManifest.files.map(async (f, i) => {
          // Replace ~a fixed few files with unique new objects (the "delta").
          if (i < 3) {
            const bytes = new TextEncoder().encode(
              `delta-${i}-${Math.random()}`,
            )
            const { ref } = await core.putObject(bytes)
            deltaRefs.add(ref)
            return { ...f, ref, size: bytes.length }
          }
          return f
        }),
      )
      const newManifestBytes = new TextEncoder().encode(
        JSON.stringify({ v: 3, files: newFiles, dirs: oldManifest.dirs }),
      )
      const { ref: newRef } = await core.putObject(newManifestBytes)
      // A high LSN so this is unambiguously the LATEST checkpoint (above the
      // createDatabase C0, which is a large real LSN).
      await core.registerCheckpoint(id, {
        lsn: 'FF/FF000000',
        snapEnd: 'FF/FF000000',
        streamOffset: '0000000000000000_00000000FFFF0000',
        objectRef: newRef,
      })

      // Sanity: the two manifests overlap heavily.
      const newFileRefs = new Set(newFiles.map((f) => f.ref))
      const shared = [...oldFiles].filter((r) => newFileRefs.has(r))
      expect(shared.length).toBeGreaterThan(0)

      const before = await core.listObjects()
      const report = await core.runGc(id)

      // The old checkpoint ROW is pruned (only latest + pinned kept).
      expect(report.deletedCheckpoints).toBeGreaterThanOrEqual(1)

      // Object sweep freed ONLY objects no surviving manifest references: the
      // old manifest object plus the old delta file objects that the NEW
      // manifest replaced. Shared file objects survive.
      const after = new Set(await core.listObjects())
      // Old manifest object gone.
      expect(after.has(oldRef)).toBe(false)
      // Old delta objects (in old manifest, not in new) gone.
      const oldOnly = [...oldFiles].filter((r) => !newFileRefs.has(r))
      for (const r of oldOnly) expect(after.has(r)).toBe(false)
      // Every ref the NEW (surviving) manifest lists is still present.
      for (const r of newFileRefs) expect(after.has(r)).toBe(true)
      // The new manifest object itself survives.
      expect(after.has(newRef)).toBe(true)

      // The surviving checkpoint still extracts.
      const dest = mkdtempSync(join(tmpdir(), 'pgl-gw-gcv3-extract2-'))
      try {
        await expect(
          extractDatadirV3(newRef, core.objectGetStore, dest),
        ).resolves.toBeDefined()
      } finally {
        rmSync(dest, { recursive: true, force: true })
      }
      // We actually deleted SOMETHING (the delta + old manifest), not nothing.
      expect(before.length).toBeGreaterThan(after.size)
    },
    TEST_TIMEOUT,
  )

  it(
    '(3) mixed v2 + v3 database: GC keeps both worlds’ live objects',
    async () => {
      const m = await core.createDatabase('mixed')
      const id = m.databaseId
      await core.setDials(id, { gcGraceMs: 0 })

      const v3Ref = m.checkpoint.ref // v3 manifest (createDatabase, format 3)
      const v3Files = await fileRefsOf(v3Ref)

      // Register a later v2-style archive checkpoint (a plain blob object) as
      // the LATEST — so the v3 one becomes a prunable older row, but it is
      // pinned below via a live pin so its file objects must survive.
      const v2Blob = new TextEncoder().encode('this is a v2 archive blob body')
      const { ref: v2Ref } = await core.putObject(v2Blob)
      await core.registerCheckpoint(id, {
        lsn: 'FF/FE000000',
        snapEnd: 'FF/FE000000',
        streamOffset: '0000000000000000_00000000FFFE0000',
        objectRef: v2Ref,
      })
      // Pin the v3 checkpoint's snapEnd so its ROW survives the row sweep.
      await core.catalog.upsertPin({
        id: '44444444-4444-4444-4444-444444444444',
        databaseId: id,
        kind: 'gc-pin',
        holder: 'test',
        pinnedOffset: m.checkpoint.streamOffset,
        pinnedLsn: m.checkpoint.snapEnd,
        expiresAt: new Date(Date.now() + 3_600_000),
      })

      await core.runGc(id)

      const after = new Set(await core.listObjects())
      // v2 blob (latest) survives.
      expect(after.has(v2Ref)).toBe(true)
      // v3 manifest + all its file objects survive (row pinned).
      expect(after.has(v3Ref)).toBe(true)
      for (const r of v3Files) expect(after.has(r)).toBe(true)
    },
    TEST_TIMEOUT,
  )

  it(
    '(4) v3 fork child pins the parent’s file objects via the child’s checkpoint row',
    async () => {
      const parent = await core.createDatabase('v3-parent')
      const pid = parent.databaseId
      await core.setDials(pid, { gcGraceMs: 0 })

      const sharedRef = parent.checkpoint.ref
      const sharedFiles = await fileRefsOf(sharedRef)

      // Fork at the parent's latest checkpoint — child's checkpoint row shares
      // the parent's manifest OBJECT (content-addressed).
      const child = await core.forkDatabase(pid, 'v3-child')
      expect(child.checkpoint.ref).toBe(sharedRef)
      await core.setDials(child.databaseId, { gcGraceMs: 0 })

      // Give the parent a NEWER checkpoint so its original row becomes prunable.
      const newerBlob = new TextEncoder().encode('parent-newer-checkpoint')
      const { ref: newerRef } = await core.putObject(newerBlob)
      await core.registerCheckpoint(pid, {
        lsn: 'FF/FD000000',
        snapEnd: 'FF/FD000000',
        streamOffset: '0000000000000000_00000000FFFD0000',
        objectRef: newerRef,
      })

      await core.runGc() // global GC

      const after = new Set(await core.listObjects())
      // The shared manifest object survives (child's row references it).
      expect(after.has(sharedRef)).toBe(true)
      // AND all its file objects survive (transitively referenced via the
      // child's checkpoint row -> manifest -> files[]).
      for (const r of sharedFiles) expect(after.has(r)).toBe(true)

      // The child can still fully restore from the shared checkpoint.
      const dest = mkdtempSync(join(tmpdir(), 'pgl-gw-gcv3-fork-'))
      try {
        await expect(
          extractDatadirV3(sharedRef, core.objectGetStore, dest),
        ).resolves.toBeDefined()
      } finally {
        rmSync(dest, { recursive: true, force: true })
      }
    },
    TEST_TIMEOUT,
  )
})
