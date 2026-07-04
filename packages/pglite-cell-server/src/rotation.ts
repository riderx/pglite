// rotateDatabase — the M2 era rotator (M2_PLAN "Rotation state machine",
// the normative mapping of design §6.1 steps 0–6). Cuts era N+1 at a unique
// per-attempt URL, seals era N with a CAS append+close S frame, and records
// the transition in the control plane. Race outcomes:
//
//   seal 409-seq    → a commit raced in (a legitimate era-N commit): catch
//                     up and RE-CUT from step 3 with a fresh URL and an
//                     updated baseLsn. Tail-copy is deliberately NOT
//                     implemented — re-cut is always correct and simpler.
//                     The stale attempt orphans (GC sweeps it later).
//   seal 409-closed → another rotator won: catch up (the tailer hops via
//                     their S/O chain, verifying the mirror), ADOPT their
//                     era, complete their manifest idempotently; our
//                     attempt orphans.
//
// Idempotent + re-entrant: step 0's repair-walk completes any rotation a
// prior run (or another host) left half-done — the terminal S frame on the
// stream is the truth (§2.6 sealed-detection rule), the control plane only
// mirrors it. A crash between the seal and the manifest is repaired by
// simply re-running rotateDatabase.

import { randomBytes } from 'node:crypto'
import {
  INITIAL_OFFSET_TOKEN,
  PositionCheckedReader,
  encodeAppend,
  formatLsn,
  parseLsn,
} from '@electric-sql/pglite-cell'
import type {
  DsStreamClient,
  OFrame,
  OFrameHeader,
  SFrame,
  SFrameHeader,
} from '@electric-sql/pglite-cell'
import type { DatabaseRuntime } from './database-runtime'
import type { GatewayHandle } from './gateway'

/** Bound on seal re-cuts (409-seq loops) before the rotator gives up. */
const MAX_RECUTS = 5
/** Bound on repair-walk steps (each step advances one era boundary). */
const MAX_REPAIR_STEPS = 10

export interface RotationReport {
  /** Ordinal of the era that was current when this run started. */
  fromOrdinal: number
  /** Ordinal of the era that is current when this run finished. */
  toOrdinal: number
  /** Id and path of the era now current. */
  eraId: string
  eraPath: string
  /** Seal re-cuts this run performed (each left an orphan attempt). */
  reCuts: number
  /** True when another rotator won the seal and this run adopted theirs. */
  adopted: boolean
  /**
   * True when step 0's repair-walk found and completed a rotation a prior
   * run left half-done — no NEW era was cut by this run.
   */
  repaired: boolean
}

/**
 * A tiny sortable id (ULID-shaped, no dependency) — the per-attempt era-URL
 * uniqueness the design's PUT-per-attempt rule (§2.4) needs. Mirrors the
 * gateway's createDatabase id shape.
 */
function sortableId(): string {
  const ts = Date.now().toString(16).padStart(10, '0')
  const rand = randomBytes(6).toString('hex')
  return (ts + rand).toUpperCase()
}

function pad6(n: number): string {
  return String(n).padStart(6, '0')
}

/**
 * Scan an era stream from `fromOffset` for its terminal S frame. Returns the
 * seal header (null when the era is live) and the stream's closed bit — a
 * closed era WITHOUT an S is wedged (§2.6).
 */
async function scanForSeal(
  client: DsStreamClient,
  path: string,
  fromOffset: string,
): Promise<{ seal: SFrameHeader | null; closed: boolean }> {
  const reader = new PositionCheckedReader(fromOffset)
  let seal: SFrameHeader | null = null
  let closed = false
  for (;;) {
    const res = await client.read(path, { offset: reader.boundary })
    if (res.bytes.length > 0) {
      for (const group of reader.feed(res.bytes)) {
        for (const frame of group.frames) {
          if (frame.type === 'S') seal = frame.header
        }
      }
    }
    if (res.nextOffset !== '') reader.expectBoundary(res.nextOffset)
    closed = res.closed
    if (res.upToDate || res.bytes.length === 0) break
  }
  return { seal, closed }
}

/**
 * Read an era stream's opening O frame (group zero at the initial token)
 * and the boundary immediately AFTER it — the era row's `baseOffset` (the
 * position a joiner starts tailing from, past the O).
 */
async function readEraOpen(
  client: DsStreamClient,
  path: string,
): Promise<{ open: OFrameHeader; baseOffset: string }> {
  const reader = new PositionCheckedReader(INITIAL_OFFSET_TOKEN)
  let open: OFrameHeader | null = null
  let baseOffset: string | null = null
  for (;;) {
    const res = await client.read(path, { offset: reader.boundary })
    if (res.bytes.length > 0) {
      for (const group of reader.feed(res.bytes)) {
        if (open === null) {
          const frame = group.frames[0]
          if (!frame || frame.type !== 'O') {
            throw new Error(`era stream ${path} does not open with an O frame`)
          }
          open = frame.header
          // The boundary after group zero (the O) is the era's baseOffset.
          baseOffset = reader.boundary
        }
      }
    }
    if (res.nextOffset !== '') reader.expectBoundary(res.nextOffset)
    if (res.upToDate || res.bytes.length === 0) break
  }
  if (open === null || baseOffset === null) {
    throw new Error(`era stream ${path} is empty (no O frame)`)
  }
  return { open, baseOffset }
}

/** The S/O mirror check (§6.1 joiner tolerance / M2a hop rule). */
function verifyMirror(seal: SFrameHeader, open: OFrameHeader): void {
  if (
    open.prevEraId !== seal.eraId ||
    open.eraId !== seal.nextEraId ||
    open.baseLsn !== seal.finalLsn
  ) {
    throw new Error(
      `O/S mirror violation completing rotation ${seal.eraId} → ` +
        `${seal.nextEraId}: O{prevEraId:${open.prevEraId}, ` +
        `eraId:${open.eraId}, baseLsn:${open.baseLsn}} vs ` +
        `S{eraId:${seal.eraId}, nextEraId:${seal.nextEraId}, ` +
        `finalLsn:${seal.finalLsn}}`,
    )
  }
}

/**
 * Step 6, idempotently: seal era N's row, insert era N+1's row, promote the
 * attempt, and advance current_era_ordinal (guarded; 0 rows ⇒ someone else
 * did it ⇒ verified by re-read). Safe to run repeatedly and concurrently —
 * every sub-step is guarded or conflict-tolerant.
 */
async function completeManifest(
  gw: GatewayHandle,
  databaseId: string,
  seal: SFrameHeader,
  next: {
    ordinal: number
    eraId: string
    path: string
    baseOffset: string
    baseLsn: string
  },
): Promise<void> {
  await gw.sealEraRow(databaseId, seal.ordinal, {
    finalOffset: seal.finalOffset,
    finalLsn: seal.finalLsn,
    nextOrdinal: next.ordinal,
  })
  const existing = await gw.eraByOrdinal(databaseId, next.ordinal)
  if (existing === null) {
    try {
      await gw.addEra(databaseId, next)
    } catch (err) {
      // (database, ordinal) PK race with a concurrent repairer/rotator —
      // benign iff the winner inserted the SAME era (verified below).
      const after = await gw.eraByOrdinal(databaseId, next.ordinal)
      if (after === null) throw err
    }
  }
  const row = await gw.eraByOrdinal(databaseId, next.ordinal)
  if (row === null || row.eraId !== next.eraId) {
    throw new Error(
      `LOUD: era ${next.ordinal} row diverged from the stream seal chain — ` +
        `control plane has ${row?.eraId ?? 'nothing'}, the sealed stream ` +
        `points at ${next.eraId}. The stream is the truth; the control ` +
        `plane is corrupt.`,
    )
  }
  await gw.promoteEraAttempt(databaseId, next.eraId)
  await gw.advanceCurrentEra(databaseId, seal.ordinal, next.ordinal)
}

/**
 * Step 0: the repair-walk. While the current era's STREAM carries a terminal
 * S (the §2.6 truth — the control plane may lag arbitrarily), complete the
 * manifest transition and continue. Also settles a lagging
 * current_era_ordinal pointer. Returns the number of repair steps performed.
 */
async function repairWalk(
  gw: GatewayHandle,
  client: DsStreamClient,
  databaseId: string,
): Promise<number> {
  let steps = 0
  for (let i = 0; i < MAX_REPAIR_STEPS; i++) {
    const manifest = await gw.getManifest(databaseId)
    const era = manifest.era
    const { seal, closed } = await scanForSeal(client, era.path, era.baseOffset)
    if (seal === null) {
      if (closed) {
        throw new Error(
          `era ${era.id} of ${databaseId} is closed WITHOUT a terminal S — ` +
            `wedged (§2.6); refusing to rotate`,
        )
      }
      // Live era. Settle a lagging pointer (manifest.era is the highest era
      // ROW; the dial pointer may still sit at an earlier ordinal).
      if (manifest.dials.currentEraOrdinal < era.ordinal) {
        await gw.advanceCurrentEra(
          databaseId,
          manifest.dials.currentEraOrdinal,
          era.ordinal,
        )
        steps++
        continue
      }
      return steps
    }
    if (seal.eraId !== era.id) {
      throw new Error(
        `terminal S on ${era.path} names era ${seal.eraId}, expected ${era.id}`,
      )
    }
    const { open, baseOffset } = await readEraOpen(client, seal.nextEraUrl)
    verifyMirror(seal, open)
    await completeManifest(gw, databaseId, seal, {
      ordinal: open.ordinal,
      eraId: open.eraId,
      path: seal.nextEraUrl,
      baseOffset,
      baseLsn: open.baseLsn,
    })
    steps++
  }
  throw new Error(
    `repair-walk did not converge after ${MAX_REPAIR_STEPS} steps on ${databaseId}`,
  )
}

/**
 * The joiner attach invariant (M2 exit criterion: "joiners attach via
 * checkpoint + ≤1 era tail"): the latest checkpoint must cover the current
 * era's base. A raced seal breaks this — the step-2 checkpoint predates the
 * raced commit(s), so the new era bases PAST it and the gap lives in the
 * sealed era. Repair: cut a fresh checkpoint in the new era. (This is also
 * what unlocks GC of the sealed era — sweep 2 needs a covering checkpoint.)
 */
async function ensureCheckpointCoversEraBase(
  runtime: DatabaseRuntime,
): Promise<void> {
  const m = runtime.manifest
  if (parseLsn(m.checkpoint.snapEnd) < parseLsn(m.era.baseLsn)) {
    await runtime.checkpoint()
    await runtime.refreshManifest()
  }
}

/**
 * Rotate a database's current era (§6.1 steps 0–6). Serialized per runtime
 * by `DatabaseRuntime.rotate()` (the in-flight guard); safe to re-run after
 * any crash — step 0 completes half-done transitions, and a run that only
 * repaired returns `repaired: true` without cutting a new era.
 */
export async function rotateDatabase(
  runtime: DatabaseRuntime,
): Promise<RotationReport> {
  await runtime.ensureActive()
  const gw = runtime.gateway
  const databaseId = runtime.databaseId
  const client = gw.streamClientFor(databaseId)

  // --- 0. REPAIR-WALK ----------------------------------------------------
  const repairedSteps = await repairWalk(gw, client, databaseId)
  // Hop the runtime's own tailer through any seals the walk (or anyone
  // else) recorded, so the committer continues in the current era.
  await runtime.tailer.catchUp()
  if (repairedSteps > 0) {
    await runtime.refreshManifest()
    await ensureCheckpointCoversEraBase(runtime)
    const era = runtime.tailer.currentEra
    return {
      fromOrdinal: era.ordinal - repairedSteps,
      toOrdinal: era.ordinal,
      eraId: era.id,
      eraPath: era.path,
      reCuts: 0,
      adopted: false,
      repaired: true,
    }
  }

  const startOrdinal = runtime.tailer.currentEra.ordinal

  // --- 1. QUIESCE (implicit: the committer mutex serializes all appends) --
  // --- 2. CHECKPOINT (idempotent; skipped when fresh) ----------------------
  const ckpt = await runtime.checkpoint()

  let reCuts = 0
  for (let attempt = 0; attempt <= MAX_RECUTS; attempt++) {
    await runtime.tailer.catchUp()
    const cur = runtime.tailer.currentEra

    if (cur.ordinal > startOrdinal) {
      // Someone rotated under us while we were checkpointing / re-cutting:
      // our (un-referenced) attempt orphans; adopt the rotation that won.
      await repairWalk(gw, client, databaseId)
      await runtime.refreshManifest()
      await ensureCheckpointCoversEraBase(runtime)
      return {
        fromOrdinal: startOrdinal,
        toOrdinal: cur.ordinal,
        eraId: cur.id,
        eraPath: cur.path,
        reCuts,
        adopted: true,
        repaired: false,
      }
    }

    const head = runtime.tailer.head
    const nextOrdinal = cur.ordinal + 1
    const nextEraId = `${pad6(nextOrdinal)}-${sortableId()}`
    const nextPath = `/era/${nextEraId}`
    const baseLsnText = formatLsn(head.lsn)

    // --- 3. REGISTER the attempt FIRST, then PUT era N+1 -----------------
    await gw.registerEraAttempt(databaseId, {
      ordinal: nextOrdinal,
      eraId: nextEraId,
      path: nextPath,
    })
    const oFrame: OFrame = {
      type: 'O',
      header: {
        v: 1,
        eraId: nextEraId,
        expectedOffset: INITIAL_OFFSET_TOKEN,
        ordinal: nextOrdinal,
        prevEraId: cur.id,
        prevEraUrl: cur.path,
        baseOffset: INITIAL_OFFSET_TOKEN,
        baseLsn: baseLsnText,
        snapEnd: baseLsnText,
        checkpointRef: ckpt.checkpointRef,
      },
    }
    const created = await client.createStream(nextPath, {
      body: encodeAppend([oFrame]),
    })
    const nextBaseOffset = created.nextOffset

    // --- 4. K frame: part of step 2 ---------------------------------------
    // --- 5. SEAL era N (CAS append+close; never close-only) --------------
    const res = await runtime.committer.sealEra(
      (expectedOffset) => {
        const sFrame: SFrame = {
          type: 'S',
          header: {
            v: 1,
            eraId: cur.id,
            expectedOffset,
            ordinal: cur.ordinal,
            finalOffset: expectedOffset,
            finalLsn: baseLsnText,
            nextEraUrl: nextPath,
            nextEraId,
          },
        }
        return [sFrame]
      },
      // If the head moved past the position the O frame was cut against,
      // the seal must NOT land (its finalLsn would break the O/S mirror).
      { ifHeadOffset: head.offset },
    )

    if (res.result === 'seq-conflict') {
      // A commit raced in — a legitimate era-N commit. Catch up (absorb
      // it), RE-CUT from step 3 with a fresh URL and updated baseLsn. The
      // stale attempt row + stream orphan (GC sweeps them after grace).
      reCuts++
      await runtime.tailer.catchUp()
      continue
    }

    if (res.result === 'closed') {
      // Another rotator won. Catch up: the tailer hops via THEIR S/O chain
      // (verifying the mirror; throws WedgedEraError / EraChainError when
      // their next era is invalid). Our attempt orphans.
      await runtime.tailer.catchUp()
      const adoptedEra = runtime.tailer.currentEra
      if (adoptedEra.ordinal <= cur.ordinal) {
        throw new Error(
          `seal of era ${cur.id} reported closed but the tailer did not ` +
            `hop (still at ordinal ${adoptedEra.ordinal}) — wedged?`,
        )
      }
      await repairWalk(gw, client, databaseId) // complete THEIR manifest
      await runtime.refreshManifest()
      await ensureCheckpointCoversEraBase(runtime)
      return {
        fromOrdinal: startOrdinal,
        toOrdinal: adoptedEra.ordinal,
        eraId: adoptedEra.id,
        eraPath: adoptedEra.path,
        reCuts,
        adopted: true,
        repaired: false,
      }
    }

    // --- sealed: 6. MANIFEST ---------------------------------------------
    const sealHeader: SFrameHeader = {
      v: 1,
      eraId: cur.id,
      expectedOffset: res.offset,
      ordinal: cur.ordinal,
      finalOffset: res.offset,
      finalLsn: baseLsnText,
      nextEraUrl: nextPath,
      nextEraId,
    }
    await completeManifest(gw, databaseId, sealHeader, {
      ordinal: nextOrdinal,
      eraId: nextEraId,
      path: nextPath,
      baseOffset: nextBaseOffset,
      baseLsn: baseLsnText,
    })
    // Hop the runtime's own tailer through the seal so the committer
    // continues in era N+1 without a reconnect.
    await runtime.tailer.catchUp()
    await runtime.refreshManifest()
    await ensureCheckpointCoversEraBase(runtime)
    return {
      fromOrdinal: startOrdinal,
      toOrdinal: nextOrdinal,
      eraId: nextEraId,
      eraPath: nextPath,
      reCuts,
      adopted: false,
      repaired: false,
    }
  }

  throw new Error(
    `rotation of ${databaseId} exhausted ${MAX_RECUTS} seal re-cuts — the ` +
      `stream is under continuous append pressure`,
  )
}
