// Materialize = lay slices into pg_wal + flip pg_control to
// DB_IN_PRODUCTION + boot a throwaway PGlite (crash recovery replays the
// slices) + clean close (M0-1 R2). The clean close writes GENUINE
// end-of-recovery + shutdown-checkpoint WAL `(lastSliceEnd .. C'+120]`,
// which the caller MUST publish as a `sync` slice before committing from
// the new head (M1_PLAN attach algorithm, step 4 — never skipped).

import { cpSync } from 'node:fs'
import { PGlite } from '@electric-sql/pglite'
import {
  readControl,
  forceCrashState,
  readWalRange,
  writeWalRange,
} from './datadir'
import { shutdownCheckpointEnd } from './lsn'
import { SliceChainError } from './errors'

/** The minimal slice shape materialize consumes (TailSlice-compatible). */
export interface MaterializeSlice {
  baseLsn: bigint
  endLsn: bigint
  bytes: Uint8Array
}

/** Minimal constructor surface of PGlite used for the throwaway boot. */
export type ThrowawayPGliteCtor = new (dataDir: string) => {
  query(sql: string): Promise<unknown>
  close(): Promise<void>
}

export interface MaterializeOpts {
  /** Datadir to materialize into (a hydrated checkpoint copy). */
  baseDir: string
  /** Contiguous W slices to lay, oldest first (may be empty). */
  slices: MaterializeSlice[]
  /** Override the PGlite constructor (tests / alternative builds). */
  PGliteCtor?: ThrowawayPGliteCtor
}

export interface MaterializeResult {
  /**
   * The genuine boot WAL `(lastSliceEnd .. C'+120]` produced by the
   * throwaway recovery boot + clean close. Null iff `slices` was empty
   * (the dir was already clean — nothing to replay, nothing new written).
   */
  syncSlice: { baseLsn: bigint; endLsn: bigint; bytes: Uint8Array } | null
  /** The datadir's new WAL head: shutdown-checkpoint record end (C'+120). */
  headLsn: bigint
}

/**
 * Bring `baseDir` to the stream head by replaying `slices` through ordinary
 * crash recovery. Validates slice-to-slice chaining (each slice's baseLsn
 * must equal the previous slice's endLsn — whether the FIRST slice chains
 * from the checkpoint's snapEnd is the caller's concern). With no slices
 * the dir is already clean and the head is read straight from pg_control.
 */
export async function materializeAtHead(
  opts: MaterializeOpts,
): Promise<MaterializeResult> {
  const { baseDir, slices } = opts

  if (slices.length === 0) {
    const control = readControl(baseDir)
    return {
      syncSlice: null,
      headLsn: shutdownCheckpointEnd(control.checkPoint),
    }
  }

  for (let i = 1; i < slices.length; i++) {
    if (slices[i].baseLsn !== slices[i - 1].endLsn) {
      throw new SliceChainError(
        `slice ${i} baseLsn ${slices[i].baseLsn} != previous endLsn ` +
          `${slices[i - 1].endLsn} — slices must chain contiguously`,
      )
    }
  }

  for (const slice of slices) {
    writeWalRange(baseDir, slice.baseLsn, slice.bytes)
  }
  forceCrashState(baseDir)

  // Throwaway boot: crash recovery replays the transplanted slices; the
  // clean close writes the genuine shutdown checkpoint.
  const Ctor: ThrowawayPGliteCtor = opts.PGliteCtor ?? PGlite
  const db = new Ctor(baseDir)
  await db.query('select 1')
  await db.close()

  const control = readControl(baseDir)
  const headLsn = shutdownCheckpointEnd(control.checkPoint)
  const lastSliceEnd = slices[slices.length - 1].endLsn
  if (headLsn < lastSliceEnd) {
    throw new SliceChainError(
      `materialize: shutdown checkpoint end ${headLsn} is below the last ` +
        `slice end ${lastSliceEnd} — recovery did not replay to the head`,
    )
  }

  return {
    syncSlice: {
      baseLsn: lastSliceEnd,
      endLsn: headLsn,
      bytes: readWalRange(baseDir, lastSliceEnd, headLsn),
    },
    headLsn,
  }
}

/** Copy a checkpoint datadir into a working directory (recursive). */
export function hydrateDatadir(checkpointDir: string, workDir: string): void {
  cpSync(checkpointDir, workDir, { recursive: true })
}
