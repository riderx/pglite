// BaseDirManager — the per-(host, database) datadir farm behind the M1
// "advance = recycle-with-materialize" scope cut. It owns:
//
//   - the CANONICAL base: a datadir whose clean shutdown position IS a
//     position the stream itself reached (the hydrated checkpoint's snapEnd,
//     or the end of a sync slice THIS manager published and landed). Zero
//     local divergence — its pg_control checkpoint anchor sits at a stream
//     LSN, so stream slices lay cleanly on a copy of it;
//   - the READ base: the newest locally-materialized dir (read-attach, M1c
//     amendment): stream slices replayed, NOTHING published. Its own
//     recovery/shutdown boot records extend past the stream head, so its
//     clean position DIVERGES from the stream — publishing from any copy of
//     it is mechanically forbidden.
//
// LOAD-BEARING DEVIATION from the naive "copy the current base + lay the
// missing slices" advance: a DIVERGED (read-attached) base can never be
// advanced incrementally. Its pg_control checkpoint anchor — the record
// crash recovery must start from — lives INSIDE the divergent LSN range
// `(streamLsn, localHeadLsn]`, and the next stream slice starts exactly at
// `streamLsn` (contiguity), so laying it overwrites the anchor and the
// materialize boot PANICs ("could not locate a valid checkpoint record";
// verified empirically, see tests/base-dir.test.ts). Every advance therefore
// re-materializes from the newest CANONICAL dir, whose anchor sits at
// `canonical.lsn - 120` — strictly below every slice it will ever lay. The
// cost: read-heavy hosts replay the tail since the last canonical position
// on each advance instead of just the delta — acceptable under the M1
// recycle-with-materialize scope cut, revisited at M3 (live tail apply).

import { cpSync, mkdirSync, renameSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import { materializeAtHead } from '@electric-sql/pglite-cell'
import type {
  CommitResult,
  CommitSliceInput,
  TailSlice,
} from '@electric-sql/pglite-cell'
import { CaptureCursorError } from '@electric-sql/pglite-cell'
import { AdvanceRaceError } from './errors'

/** The tailer surface BaseDirManager consumes (EraTailer satisfies it). */
export interface TailView {
  readonly head: { offset: string; lsn: bigint }
  slicesSince(lsn: bigint): TailSlice[]
  catchUp(): Promise<number>
}

/** The committer surface the canonical ensure consumes. */
export interface SyncPublisher {
  commitSlice(input: CommitSliceInput): Promise<CommitResult>
}

/** A cell datadir handed to a session (a private copy of a base). */
export interface CellDirLease {
  dir: string
  base: {
    /** The stream-head LSN this cell logically serves. */
    lsn: bigint
    /** The stream offset paired with `lsn`. */
    offset: string
    /**
     * The dir's own clean insert position — what `Cell.open` must expect.
     * Equals `lsn` for canonical (write-attach) leases; exceeds it by the
     * local recovery/shutdown records for read-attach leases. A read cell's
     * capture cursor therefore tracks LOCAL position, and ANY nonempty
     * capture on it is the write-upgrade trigger — never a publish.
     */
    localHeadLsn: bigint
  }
  canonical: boolean
}

export interface BaseDirManagerOpts {
  /** Directory all bases / stagings / cell dirs live under (owned). */
  root: string
  /** The hydrated checkpoint datadir (already extracted under `root`). */
  canonicalDir: string
  /** The checkpoint's snapEnd — the canonical dir's clean position. */
  canonicalLsn: bigint
  /** The stream offset paired with `canonicalLsn`. */
  canonicalOffset: string
  /** Bound on the canonical-ensure publish-race loop (default 10). */
  maxEnsureAttempts?: number
}

interface CanonicalState {
  dir: string
  lsn: bigint
  offset: string
}

interface ReadState {
  dir: string
  streamLsn: bigint
  streamOffset: string
  localHeadLsn: bigint
}

/**
 * Small jittered delay between canonical-ensure attempts after a lost CAS
 * (M4): under genuine cross-host contention the materialize+publish loop
 * is slower than a foreign host's commit cadence — hammering re-attempts
 * back-to-back starves; a jitter lets the ensure slot between foreign
 * appends instead of always racing them from behind.
 */
function ensureBackoff(): Promise<void> {
  return new Promise((r) => setTimeout(r, 5 + Math.floor(Math.random() * 45)))
}

export class BaseDirManager {
  private readonly root: string
  private readonly maxEnsureAttempts: number
  private canonicalState: CanonicalState
  private readState: ReadState | null = null
  private gen = 0
  private chain: Promise<unknown> = Promise.resolve()
  private destroyed = false

  constructor(opts: BaseDirManagerOpts) {
    this.root = opts.root
    this.maxEnsureAttempts = opts.maxEnsureAttempts ?? 10
    mkdirSync(this.root, { recursive: true })
    this.canonicalState = {
      dir: opts.canonicalDir,
      lsn: opts.canonicalLsn,
      offset: opts.canonicalOffset,
    }
  }

  /** The canonical base position (a stream position, zero divergence). */
  get canonical(): { lsn: bigint; offset: string } {
    return { lsn: this.canonicalState.lsn, offset: this.canonicalState.offset }
  }

  /**
   * The canonical base DIRECTORY (M1e checkpoint worker input): a datadir
   * clean at a genuine stream position, its pg_control checkpoint anchor at
   * `canonical.lsn - 120`. The checkpoint worker packs a copy of this dir.
   * The paired `canonical.offset` is the sync-slice append's `nextOffset`
   * (or the hydrated checkpoint's streamOffset) — the `streamOffset` a
   * joiner must tail from. Only valid while the manager is active.
   */
  get canonicalDir(): string {
    return this.canonicalState.dir
  }

  /** Serialize every dir-mutating operation behind one promise chain. */
  private run<T>(fn: () => Promise<T>): Promise<T> {
    const p = this.chain.then(fn)
    this.chain = p.then(
      () => undefined,
      () => undefined,
    )
    return p
  }

  private newDir(prefix: string): string {
    return join(
      this.root,
      `${prefix}-${++this.gen}-${randomUUID().slice(0, 8)}`,
    )
  }

  /**
   * Bring the READ base to the tailer's head WITHOUT publishing anything
   * (read-attach, M1c amendment): materialize a staging copy of the
   * canonical base, then swap it in as the read base (rename dance — a
   * failed materialize leaves every existing base untouched). Returns the
   * stream position served and the dir's own (diverged) clean position.
   */
  ensureAtHeadLocal(
    tailer: TailView,
  ): Promise<{ baseLsn: bigint; localHeadLsn: bigint }> {
    return this.run(async () => {
      const head = tailer.head
      // Canonical already at head: the canonical dir IS the freshest read
      // base (zero divergence — read cells opened from it sit exactly at the
      // stream head).
      if (this.canonicalState.lsn >= head.lsn) {
        return {
          baseLsn: this.canonicalState.lsn,
          localHeadLsn: this.canonicalState.lsn,
        }
      }
      // Reuse the current read base if it already serves this head.
      if (this.readState && this.readState.streamLsn >= head.lsn) {
        return {
          baseLsn: this.readState.streamLsn,
          localHeadLsn: this.readState.localHeadLsn,
        }
      }
      const staging = this.newDir('staging')
      let mat: Awaited<ReturnType<typeof materializeAtHead>>
      try {
        cpSync(this.canonicalState.dir, staging, { recursive: true })
        mat = await materializeAtHead({
          baseDir: staging,
          slices: tailer.slicesSince(this.canonicalState.lsn),
        })
      } catch (err) {
        rmSync(staging, { recursive: true, force: true })
        throw err
      }
      // Swap: promote staging to the read base, drop the old one. The local
      // divergence (head.lsn, mat.headLsn] is recorded, NEVER published.
      const promoted = this.newDir('read')
      renameSync(staging, promoted)
      const old = this.readState
      this.readState = {
        dir: promoted,
        streamLsn: head.lsn,
        streamOffset: head.offset,
        localHeadLsn: mat.headLsn,
      }
      if (old) rmSync(old.dir, { recursive: true, force: true })
      return { baseLsn: head.lsn, localHeadLsn: mat.headLsn }
    })
  }

  /**
   * Bring the CANONICAL base to the stream head (write-attach): loop —
   * catch up; if the canonical position already IS the head, done;
   * otherwise materialize a staging copy and publish its sync slice (kind
   * `sync`). A `CaptureCursorError` or `landed: false` means someone
   * appended between catch-up and publish — retry (bounded), then throw
   * `AdvanceRaceError`. After a landed publish the staging swaps in as the
   * canonical base: base at head AND local head == stream head.
   */
  ensureAtHeadCanonical(
    tailer: TailView,
    committer: SyncPublisher,
  ): Promise<{ lsn: bigint; offset: string }> {
    return this.run(async () => {
      for (let attempt = 0; attempt < this.maxEnsureAttempts; attempt++) {
        await tailer.catchUp()
        const head = tailer.head
        if (this.canonicalState.lsn === head.lsn) {
          // Canonical at head: the last materialize's sync slice was
          // published (or nothing ever landed past us), so stream and local
          // positions coincide.
          return this.canonical
        }
        const staging = this.newDir('staging')
        let mat: Awaited<ReturnType<typeof materializeAtHead>>
        try {
          cpSync(this.canonicalState.dir, staging, { recursive: true })
          mat = await materializeAtHead({
            baseDir: staging,
            slices: tailer.slicesSince(this.canonicalState.lsn),
          })
        } catch (err) {
          rmSync(staging, { recursive: true, force: true })
          throw err
        }
        if (mat.syncSlice === null) {
          // Defensive: no slices to lay means the loop condition above
          // should have returned — treat as at-head.
          rmSync(staging, { recursive: true, force: true })
          return this.canonical
        }
        let res: CommitResult
        try {
          res = await committer.commitSlice({
            commitId: randomUUID(),
            kind: 'sync',
            baseLsn: mat.syncSlice.baseLsn,
            endLsn: mat.syncSlice.endLsn,
            bytes: mat.syncSlice.bytes,
          })
        } catch (err) {
          rmSync(staging, { recursive: true, force: true })
          if (err instanceof CaptureCursorError) {
            await ensureBackoff() // stream moved: retry
            continue
          }
          throw err
        }
        if (!res.landed) {
          rmSync(staging, { recursive: true, force: true })
          await ensureBackoff() // lost the CAS: someone appended; retry
          continue
        }
        // Landed: promote staging to the canonical base.
        const promoted = this.newDir('canonical')
        renameSync(staging, promoted)
        const old = this.canonicalState
        this.canonicalState = {
          dir: promoted,
          lsn: mat.headLsn,
          offset: res.nextOffset,
        }
        rmSync(old.dir, { recursive: true, force: true })
        return this.canonical
      }
      throw new AdvanceRaceError(this.maxEnsureAttempts)
    })
  }

  /**
   * Copy the appropriate base into a fresh per-cell dir. `write` requires a
   * preceding `ensureAtHeadCanonical` (the lease is the canonical base);
   * `read` serves whichever base is freshest. For read leases `base.lsn`
   * and `base.localHeadLsn` differ by the local divergence; for write
   * leases they are equal.
   */
  takeCellDir(mode: 'read' | 'write'): Promise<CellDirLease> {
    return this.run(async () => {
      const cellDir = this.newDir('cell')
      if (
        mode === 'write' ||
        this.readState === null ||
        this.readState.streamLsn <= this.canonicalState.lsn
      ) {
        cpSync(this.canonicalState.dir, cellDir, { recursive: true })
        return {
          dir: cellDir,
          base: {
            lsn: this.canonicalState.lsn,
            offset: this.canonicalState.offset,
            localHeadLsn: this.canonicalState.lsn,
          },
          canonical: true,
        }
      }
      cpSync(this.readState.dir, cellDir, { recursive: true })
      return {
        dir: cellDir,
        base: {
          lsn: this.readState.streamLsn,
          offset: this.readState.streamOffset,
          localHeadLsn: this.readState.localHeadLsn,
        },
        canonical: false,
      }
    })
  }

  /** Delete a cell dir handed out by `takeCellDir`. */
  releaseCellDir(dir: string): void {
    rmSync(dir, { recursive: true, force: true })
  }

  /** Drop every dir this manager owns (hibernation). */
  destroy(): void {
    if (this.destroyed) return
    this.destroyed = true
    rmSync(this.root, { recursive: true, force: true })
  }
}
