// M7 W3 — the lazy attach recipe (fixed decision 5): materialize the eager
// skeleton, boot a WorkerCell over LazyCellFS at the checkpoint's snapEnd
// (zero relation bytes moved, zero-boot-WAL assert unchanged), then advance
// to the stream head via the EXISTING M5 live-apply pipeline running
// worker-side — redo's base-page reads fault through the FS on demand and
// `pgl_set_wal_position` makes the position canonical (NO sync-slice
// publish on this path). Any live-apply gap throws
// `LazyAttachFallbackError` so the caller recycles to the materialize path.
//
// The v3 eager set already ships the REAL pg_wal checkpoint segment, so the
// skeleton is a cleanly-shutdown datadir at snapEnd and no minting is
// needed. The M0-2 mint recipe (datadir.ts) is kept for the fresh-lineage
// case where the segment is absent but a template segment exists.

import { cpSync, existsSync, mkdirSync, readdirSync } from 'node:fs'
import { join } from 'node:path'
import {
  mintCheckpointStruct,
  mintSegment,
  readControl,
  walFacts,
  writeSynthesizedControl,
  writeWalRange,
  SHUTDOWN_CKPT_ALIGNED,
} from '../datadir'
import { lsnToSegment, walSegmentName } from '../lsn'
import type { LazyFileSpec } from '../lazy-fs'
import { WorkerCell } from './worker-cell'
import type { WorkerCellLazyOpts, WorkerCellOpenOpts } from './worker-cell'

/** Thrown when the lazy attach cannot complete (live-apply gate rejected
 *  or aborted mid-batch): the caller MUST recycle to the existing
 *  materialize path. The work dir has been torn down. */
export class LazyAttachFallbackError extends Error {
  constructor(public readonly reason: string) {
    super(`lazy attach fell back: ${reason}`)
    this.name = 'LazyAttachFallbackError'
  }
}

export interface LazyAttachSlice {
  baseLsn: bigint
  endLsn: bigint
  bytes: Uint8Array
}

export interface LazyAttachOpts {
  /** The eager skeleton (extractDatadirV3 lazySkip:true output). Copied. */
  skeletonDir: string
  /** The manifest's lazy relation-file list. */
  lazyFiles: LazyFileSpec[]
  /** Fresh per-cell work dir (created; owned by the returned cell). */
  workDir: string
  /** The checkpoint's snapEnd — the skeleton's clean position. */
  snapEnd: bigint
  /** Contiguous stream slices (snapEnd .. head], oldest first (may be
   *  empty — already at head). */
  slices?: LazyAttachSlice[]
  /** Host-side chunk reader (chunk cache -> gateway ranged read). */
  readChunk: WorkerCellLazyOpts['readChunk']
  chunkBytes?: number
  workerUrl?: WorkerCellOpenOpts['workerUrl']
  commitGate?: boolean
  dataSabBytes?: number
}

export interface LazyAttachResult {
  cell: WorkerCell
  /** The stream head the cell now sits at (capture cursor == this). */
  headLsn: bigint
}

/**
 * Ensure the checkpoint-record segment exists in `dir`'s pg_wal; mint it
 * (M0-2 recipe, first runtime use) only when absent — the fresh-lineage
 * case. Needs SOME template segment for the WAL long-header facts.
 */
function ensureCheckpointSegment(dir: string): void {
  const control = readControl(dir)
  const { segno } = lsnToSegment(control.checkPoint)
  const segName = walSegmentName(segno)
  const pgWal = join(dir, 'pg_wal')
  if (existsSync(join(pgWal, segName))) return // v3 eager set ships it
  const anySeg = readdirSync(pgWal).some((f) => /^[0-9A-F]{24}$/.test(f))
  if (!anySeg) {
    throw new Error(
      `lazyAttach: pg_wal has no checkpoint segment ${segName} and no ` +
        `template segment to mint from — checkpoint object is not attachable`,
    )
  }
  console.log(
    `[pglite-cell] lazyAttach: minting checkpoint segment ${segName} ` +
      `(fresh-lineage path — the eager set did not ship it)`,
  )
  const H = control.checkPoint
  const cp = mintCheckpointStruct(control.copy, H)
  mintSegment(pgWal, H, H - BigInt(SHUTDOWN_CKPT_ALIGNED), cp, walFacts(dir))
  writeSynthesizedControl(dir, H, cp)
}

/**
 * The lazy attach: skeleton copy -> (mint if fresh-lineage) -> WorkerCell
 * over LazyCellFS at snapEnd -> live-apply the tail to head. Throws
 * `LazyAttachFallbackError` when the tail is not live-appliable (cell and
 * work dir already torn down); other errors propagate raw.
 */
export async function lazyAttach(
  opts: LazyAttachOpts,
): Promise<LazyAttachResult> {
  const slices = opts.slices ?? []
  // Validate the chain BEFORE any expensive work.
  let expect = opts.snapEnd
  for (const s of slices) {
    if (s.baseLsn !== expect) {
      throw new Error(
        `lazyAttach: slice base ${s.baseLsn} != expected ${expect} — ` +
          `slices must chain contiguously from snapEnd`,
      )
    }
    expect = s.endLsn
  }
  const headLsn = expect

  mkdirSync(opts.workDir, { recursive: true })
  cpSync(opts.skeletonDir, opts.workDir, { recursive: true })
  ensureCheckpointSegment(opts.workDir)

  const cell = await WorkerCell.open(opts.workDir, {
    expectedHeadLsn: opts.snapEnd,
    commitGate: opts.commitGate,
    workerUrl: opts.workerUrl,
    dataSabBytes: opts.dataSabBytes,
    lazy: {
      files: opts.lazyFiles,
      chunkBytes: opts.chunkBytes,
      readChunk: opts.readChunk,
    },
  })

  if (slices.length === 0) return { cell, headLsn }

  try {
    // pg_wal files are plain local files in the skeleton — write the tail
    // host-side, then classify + apply worker-side (redo base-page reads
    // fault through LazyCellFS on demand).
    await cell.flushWal()
    for (const s of slices) {
      writeWalRange(opts.workDir, s.baseLsn, s.bytes)
    }
    const res = await cell.applyLiveTail(opts.snapEnd, headLsn)
    if (!res.applied) {
      await cell._closeDb().catch(() => undefined)
      await cell.terminate().catch(() => undefined)
      throw new LazyAttachFallbackError(
        `live-apply gate rejected: ${res.reason ?? 'unknown'}`,
      )
    }
    // Canonical position: the cell publishes from the head (write cells)
    // or tracks it exactly (read cells) — no sync slice on this path.
    cell.advanceTo(headLsn)
    await cell.settled()
    return { cell, headLsn }
  } catch (err) {
    if (err instanceof LazyAttachFallbackError) throw err
    await cell.terminate().catch(() => undefined)
    throw new LazyAttachFallbackError(
      `live-apply failed mid-batch: ${err instanceof Error ? err.message : String(err)}`,
    )
  }
}
