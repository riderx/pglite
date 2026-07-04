// One cell = one PGlite instance plain-opened on a materialized datadir,
// plus the capture cursor. The cursor is the end LSN of the last bytes this
// cell published (or attached at); every captured slice is exactly
// `(cursor, insertLsn]` — contiguous, unfiltered, no gaps ever (M1_PLAN
// "The one invariant that makes slices compose").

import { PGlite } from '@electric-sql/pglite'
import { parseLsn } from './lsn'
import { readControl, readWalRange } from './datadir'
import { shutdownCheckpointEnd } from './lsn'
import { ConfigPinError, ZeroBootWalError } from './errors'

/** A captured WAL byte range `(baseLsn, endLsn]`, ready to commit. */
export interface CapturedSlice {
  baseLsn: bigint
  endLsn: bigint
  bytes: Uint8Array
}

export interface CellOpenOpts {
  /**
   * The stream head LSN this datadir was materialized to. A plain open of
   * the cleanly-closed dir must leave the insert LSN exactly here (zero
   * boot WAL — M0 finding 1).
   */
  expectedHeadLsn: bigint
}

/** §9 configuration pins asserted at every open. */
const CONFIG_PINS: { name: string; expected: string }[] = [
  { name: 'wal_level', expected: 'replica' },
  { name: 'full_page_writes', expected: 'on' },
  { name: 'data_checksums', expected: 'off' },
]

/**
 * A solo cell: plain-opens a materialized datadir (no recovery, no
 * synthesis, zero boot WAL), asserts the §9 config pins and the
 * zero-boot-WAL invariant, and manages the capture cursor across
 * `captureSlice` / `confirmPublished` / `closeClean`.
 */
export class Cell {
  private cursor: bigint

  private constructor(
    public readonly dir: string,
    private readonly pg: PGlite,
    cursor: bigint,
  ) {
    this.cursor = cursor
  }

  /** The underlying PGlite instance (SQL execution passthrough). */
  get db(): PGlite {
    return this.pg
  }

  /** The capture cursor: end LSN of the last published bytes. */
  get captureCursor(): bigint {
    return this.cursor
  }

  /**
   * Plain-open `dir` and verify it is safe to capture from:
   *
   * - the §9 config pins hold (`wal_level=replica`, `full_page_writes=on`,
   *   `data_checksums=off`) — else `ConfigPinError` listing every mismatch;
   * - the boot wrote zero WAL: `pg_current_wal_insert_lsn()` equals
   *   `expectedHeadLsn` — else `ZeroBootWalError` (actual vs expected).
   *
   * On success the capture cursor starts at `expectedHeadLsn`.
   */
  static async open(dir: string, opts: CellOpenOpts): Promise<Cell> {
    const pg = new PGlite(dir)
    try {
      const mismatches: { name: string; expected: string; actual: string }[] =
        []
      for (const pin of CONFIG_PINS) {
        const rows = (
          await pg.query<Record<string, string>>(`show ${pin.name}`)
        ).rows
        const actual = rows[0][pin.name]
        if (actual !== pin.expected) {
          mismatches.push({ name: pin.name, expected: pin.expected, actual })
        }
      }
      if (mismatches.length > 0) throw new ConfigPinError(mismatches)

      const insertLsn = await currentInsertLsn(pg)
      if (insertLsn !== opts.expectedHeadLsn) {
        throw new ZeroBootWalError(opts.expectedHeadLsn, insertLsn)
      }
      return new Cell(dir, pg, opts.expectedHeadLsn)
    } catch (err) {
      await pg.close().catch(() => undefined)
      throw err
    }
  }

  /** The current WAL insert LSN (a commit-time bookmark). */
  async bookmark(): Promise<bigint> {
    return currentInsertLsn(this.pg)
  }

  /**
   * Capture `(cursor, insertLsn]` from the local pg_wal. Returns null iff
   * the insert LSN still equals the cursor (read-only work generates no
   * slice). The cursor does NOT move — call `confirmPublished` only after
   * the slice's append landed.
   */
  async captureSlice(): Promise<CapturedSlice | null> {
    const end = await this.bookmark()
    if (end === this.cursor) return null
    return {
      baseLsn: this.cursor,
      endLsn: end,
      bytes: readWalRange(this.dir, this.cursor, end),
    }
  }

  /**
   * Advance the capture cursor to `endLsn`. Call ONLY after the slice
   * ending there landed on the stream (a landed CAS append).
   */
  confirmPublished(endLsn: bigint): void {
    this.cursor = endLsn
  }

  /**
   * Detach: clean-close the instance (writes session-teardown WAL + a real
   * shutdown checkpoint) and return the final `(cursor .. checkPoint+120]`
   * bytes as the detach slice for the caller to publish (kind `sync`).
   * Null if the cursor already sits at the shutdown record's end.
   */
  async closeClean(): Promise<{ detachSlice: CapturedSlice | null }> {
    await this.pg.close()
    const control = readControl(this.dir)
    const end = shutdownCheckpointEnd(control.checkPoint)
    if (end === this.cursor) return { detachSlice: null }
    return {
      detachSlice: {
        baseLsn: this.cursor,
        endLsn: end,
        bytes: readWalRange(this.dir, this.cursor, end),
      },
    }
  }
}

async function currentInsertLsn(pg: PGlite): Promise<bigint> {
  const rows = (
    await pg.query<{ lsn: string }>(
      `select pg_current_wal_insert_lsn()::text as lsn`,
    )
  ).rows
  return parseLsn(rows[0].lsn)
}
