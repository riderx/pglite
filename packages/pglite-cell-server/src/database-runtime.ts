// DatabaseRuntime — one per active database per host (§14.3 / §14.4): the
// manifest, one stream tailer, one commit sequencer (Committer), the shared
// base-dir manager, the host watermark gate (§7), host-local sequence floors
// (§5.3 M1 rollout), head-lease / gc-pin L frames (flowing, unenforced at
// M1), and hibernation/wake.

import { mkdirSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import {
  Committer,
  EraTailer,
  CaptureCursorError,
  parseLsn,
  formatLsn,
} from '@electric-sql/pglite-cell'
import type {
  Cell,
  CommitResult,
  CommitSliceInput,
  LFrame,
} from '@electric-sql/pglite-cell'
import { extractDatadir } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { BaseDirManager } from './base-dir'
import type { GatewayHandle } from './gateway'
import { HostSession } from './session'
import { checkpointDatabase } from './checkpoint'
import type { CheckpointReport } from './checkpoint'

/** Runtime tuning knobs (host-wide defaults, applied per database). */
export interface RuntimeOpts {
  /** Head-lease TTL (L{head} frames; advisory at M1). Default 30000. */
  leaseTtlMs?: number
  /** gc-pin TTL for tainted (pinned) sessions. Default 600000. */
  pinTtlMs?: number
  /** Re-execution budget for one-shot CAS losses (§3.3). Default 3. */
  maxRetries?: number
  /** Bound on write-attach publish-race loops. Default 10. */
  attachAttempts?: number
  /**
   * Hibernate the database after this many ms with every session idle.
   * Undefined (default) disables the timer — hibernation stays explicit.
   */
  hibernateAfterMs?: number
  /**
   * On hibernate, run a checkpoint FIRST when the tail since the last
   * checkpoint exceeds this many bytes. Default 0 = ALWAYS checkpoint on
   * hibernate — this makes wake cheap (zero W-slice replay) and bounds the
   * M1c ever-longer-tail cost. A negative value disables it.
   */
  checkpointOnHibernateBytes?: number
  /**
   * Auto-cadence (§2.4 dial): after each landed commit, if bytes since the
   * last checkpoint exceed this, fire-and-forget a checkpoint. Default 0 =
   * disabled (any nonzero value arms it).
   */
  checkpointEveryBytes?: number
}

export interface ResolvedRuntimeOpts {
  leaseTtlMs: number
  pinTtlMs: number
  maxRetries: number
  attachAttempts: number
  hibernateAfterMs: number | undefined
  checkpointOnHibernateBytes: number
  checkpointEveryBytes: number
}

export function resolveRuntimeOpts(
  opts: RuntimeOpts = {},
): ResolvedRuntimeOpts {
  return {
    leaseTtlMs: opts.leaseTtlMs ?? 30000,
    pinTtlMs: opts.pinTtlMs ?? 600000,
    maxRetries: opts.maxRetries ?? 3,
    attachAttempts: opts.attachAttempts ?? 10,
    hibernateAfterMs: opts.hibernateAfterMs,
    checkpointOnHibernateBytes: opts.checkpointOnHibernateBytes ?? 0,
    checkpointEveryBytes: opts.checkpointEveryBytes ?? 0,
  }
}

/** A host-local sequence floor (§5.3 M1: closes the abort-only hazard). */
export interface SequenceFloor {
  schema: string
  name: string
  /** Highest on-disk `last_value` observed after an aborted transaction. */
  value: bigint
  /** Always true at M1: a floor exists only after an observed draw. */
  isCalled: boolean
}

export type RuntimeState = 'cold' | 'active' | 'hibernated'

export interface DatabaseRuntimeInit {
  databaseId: string
  hostId: string
  gateway: GatewayHandle
  /** Host data root; this runtime owns `<dataRoot>/<databaseId>`. */
  dataRoot: string
  opts: ResolvedRuntimeOpts
}

export class DatabaseRuntime {
  readonly databaseId: string
  readonly hostId: string
  readonly opts: ResolvedRuntimeOpts

  private readonly _gateway: GatewayHandle
  private readonly root: string

  private _state: RuntimeState = 'cold'
  private _manifest: Manifest | null = null
  private _tailer: EraTailer | null = null
  private _committer: Committer | null = null
  private _baseDirs: BaseDirManager | null = null

  /**
   * The host watermark W (§7): the highest (offset, LSN) this host has
   * acked or applied. No statement begins on a cell whose base < W.lsn —
   * the session advances first. Survives hibernation (in memory).
   */
  watermark: { offset: string; lsn: bigint } = {
    offset: '',
    lsn: 0n,
  }

  /**
   * Host-local sequence floors, merged maxima from post-abort probes.
   * Survive hibernation in memory; lost on host restart (documented M1
   * limitation — native nextval clamps + G-frame leases land at M4).
   */
  readonly floors = new Map<string, SequenceFloor>()

  private readonly sessions = new Set<HostSession>()
  private lastLeaseAt = 0
  private hibernateTimer: ReturnType<typeof setTimeout> | null = null
  private activating: Promise<void> | null = null

  /**
   * The snapEnd LSN of the last checkpoint this runtime knows of (from the
   * manifest at activation, refreshed after every checkpoint it runs). The
   * "tail since the last checkpoint" is `tailer.head.lsn - lastCheckpointLsn`
   * — the byte proxy the checkpoint dials (hibernate / auto-cadence) test.
   */
  private lastCheckpointLsn = 0n
  /** Guards against concurrent / re-entrant checkpoint runs (in-flight). */
  private checkpointing: Promise<CheckpointReport> | null = null

  constructor(init: DatabaseRuntimeInit) {
    this.databaseId = init.databaseId
    this.hostId = init.hostId
    this._gateway = init.gateway
    this.opts = init.opts
    this.root = join(init.dataRoot, init.databaseId)
  }

  get state(): RuntimeState {
    return this._state
  }

  get manifest(): Manifest {
    if (!this._manifest) throw new Error('runtime not active')
    return this._manifest
  }

  get tailer(): EraTailer {
    if (!this._tailer) throw new Error('runtime not active')
    return this._tailer
  }

  get committer(): Committer {
    if (!this._committer) throw new Error('runtime not active')
    return this._committer
  }

  get baseDirs(): BaseDirManager {
    if (!this._baseDirs) throw new Error('runtime not active')
    return this._baseDirs
  }

  /** The gateway handle (M1e checkpoint worker consumes object/checkpoint). */
  get gateway(): GatewayHandle {
    return this._gateway
  }

  get sessionCount(): number {
    return this.sessions.size
  }

  /** Activate (or wake) if not already active. Safe to call concurrently. */
  async ensureActive(): Promise<void> {
    if (this._state === 'active') return
    if (!this.activating) {
      this.activating = this.activate().finally(() => {
        this.activating = null
      })
    }
    return this.activating
  }

  /**
   * Cold-start / wake path (M1_PLAN attach algorithm steps 1–2): manifest →
   * hydrate the checkpoint → tail the era → committer (§3.8 recovery inside
   * `Committer.create`) → head-lease L frame. Measured and logged.
   */
  private async activate(): Promise<void> {
    const t0 = Date.now()
    const manifest = await this._gateway.getManifest(this.databaseId)
    this._manifest = manifest

    const dirsRoot = join(this.root, 'dirs')
    rmSync(dirsRoot, { recursive: true, force: true })
    mkdirSync(dirsRoot, { recursive: true })
    const canonicalDir = join(dirsRoot, 'canonical-0')
    const ckptBytes = await this._gateway.getObject(manifest.checkpoint.ref)
    await extractDatadir(ckptBytes, canonicalDir)

    const client = this._gateway.streamClientFor(this.databaseId)
    const tailer = new EraTailer(client, {
      path: manifest.era.path,
      eraId: manifest.era.id,
      ordinal: manifest.era.ordinal,
      baseOffset: manifest.era.baseOffset,
      baseLsn: parseLsn(manifest.era.baseLsn),
    })
    await tailer.catchUp()

    const committer = await Committer.create({
      client,
      era: {
        path: manifest.era.path,
        id: manifest.era.id,
        ordinal: manifest.era.ordinal,
      },
      tailer,
      journalDir: join(this.root, 'journal'), // persists across hibernation
    })

    this._tailer = tailer
    this._committer = committer
    this._baseDirs = new BaseDirManager({
      root: dirsRoot,
      canonicalDir,
      canonicalLsn: parseLsn(manifest.checkpoint.snapEnd),
      canonicalOffset: manifest.checkpoint.streamOffset,
      maxEnsureAttempts: this.opts.attachAttempts,
    })

    // The watermark never decreases; on wake it resumes at least at the
    // tailer head (everything on the stream is "applied" by definition).
    this.raiseWatermark({ offset: tailer.head.offset, lsn: tailer.head.lsn })

    this.lastCheckpointLsn = parseLsn(manifest.checkpoint.snapEnd)
    this._state = 'active'
    await this.appendHeadLease()

    console.log(
      `[pglite-cell-server] db ${this.databaseId} active in ` +
        `${Date.now() - t0}ms (manifest -> hydrated checkpoint -> ` +
        `tail head ${formatLsn(tailer.head.lsn)})`,
    )
  }

  private raiseWatermark(pos: { offset: string; lsn: bigint }): void {
    if (pos.lsn > this.watermark.lsn) this.watermark = { ...pos }
  }

  /**
   * Append (or refresh) the head-lease L frame. Frames flow; nothing
   * enforces them at M1 — a persistent CAS loss is logged and ignored.
   */
  private async appendHeadLease(): Promise<void> {
    const committer = this.committer
    for (let i = 0; i < 3; i++) {
      const res = await committer.appendControl((expectedOffset) => {
        const frame: LFrame = {
          type: 'L',
          header: {
            v: 1,
            eraId: this.manifest.era.id,
            expectedOffset,
            kind: 'head',
            holder: this.hostId,
            epoch: committer.epoch,
            ttlMs: this.opts.leaseTtlMs,
          },
        }
        return [frame]
      })
      if (res.landed) {
        this.lastLeaseAt = Date.now()
        return
      }
      await this.tailer.catchUp()
    }

    console.log(
      `[pglite-cell-server] db ${this.databaseId}: head-lease append kept ` +
        `losing its CAS — continuing without (advisory at M1)`,
    )
  }

  /**
   * The host sequencer's one-shot commit path: refresh the head lease
   * lazily, then CAS the slice. `CaptureCursorError` (stale base — the
   * local expression of a lost race) maps to `{ landed: false }`; a landed
   * commit raises the watermark so every sibling session's next statement
   * sees it (§7 read-your-writes).
   */
  async commitFromSession(input: CommitSliceInput): Promise<CommitResult> {
    if (Date.now() - this.lastLeaseAt > this.opts.leaseTtlMs / 2) {
      await this.appendHeadLease()
    }
    let res: CommitResult
    try {
      res = await this.committer.commitSlice(input)
    } catch (err) {
      if (err instanceof CaptureCursorError) return { landed: false }
      throw err
    }
    if (res.landed) {
      this.raiseWatermark({ offset: res.nextOffset, lsn: input.endLsn })
      this.maybeAutoCheckpoint()
    }
    return res
  }

  /** Bytes of tail past the last known checkpoint (LSN delta as a proxy). */
  private tailBytesSinceCheckpoint(): bigint {
    const head = this._tailer?.head.lsn ?? this.lastCheckpointLsn
    const delta = head - this.lastCheckpointLsn
    return delta > 0n ? delta : 0n
  }

  /**
   * Auto-cadence (§2.4 dial): once the tail past the last checkpoint exceeds
   * `checkpointEveryBytes`, fire-and-forget a checkpoint. Guarded by the
   * in-flight flag so a slow checkpoint never overlaps another. Disabled
   * when the option is 0.
   */
  private maybeAutoCheckpoint(): void {
    const threshold = this.opts.checkpointEveryBytes
    if (threshold <= 0 || this.checkpointing !== null) return
    if (this.tailBytesSinceCheckpoint() < BigInt(threshold)) return
    void this.checkpoint().catch((err) => {
      console.log(
        `[pglite-cell-server] db ${this.databaseId}: auto-checkpoint ` +
          `failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    })
  }

  /**
   * Build a checkpoint for this database (M1e). Serialized by the in-flight
   * guard — concurrent callers share the same run. On success the local
   * last-checkpoint LSN advances so subsequent cadence / hibernate decisions
   * measure the tail past the NEW checkpoint. Idempotent runs (`skipped`)
   * still refresh the LSN.
   */
  async checkpoint(): Promise<CheckpointReport> {
    if (this.checkpointing !== null) return this.checkpointing
    const run = (async (): Promise<CheckpointReport> => {
      await this.ensureActive()
      const report = await checkpointDatabase(this)
      this.lastCheckpointLsn = parseLsn(report.snapEnd)
      return report
    })()
    this.checkpointing = run
    try {
      return await run
    } finally {
      this.checkpointing = null
    }
  }

  /**
   * Probe `pg_sequences` on a cell and merge maxima into the floor map.
   * Called after any exec that ended with an ABORTED transaction (thrown
   * error, or trailing ROLLBACK) — the only case where a drawn `nextval`
   * can be observed without any slice ever publishing it (§5.3 rule 7).
   * `last_value IS NOT NULL` ⇔ the sequence has been called (the view
   * surfaces `pg_sequence_last_value()`, null before the first draw), so
   * `isCalled` is always true here.
   */
  async probeFloors(cell: Cell): Promise<void> {
    const rows = (
      await cell.db.query<{ s: string; n: string; v: string }>(
        `select schemaname as s, sequencename as n, last_value::text as v
           from pg_sequences where last_value is not null`,
      )
    ).rows
    for (const row of rows) {
      const key = `${row.s}.${row.n}`
      const value = BigInt(row.v)
      const existing = this.floors.get(key)
      if (!existing || value > existing.value) {
        this.floors.set(key, {
          schema: row.s,
          name: row.n,
          value,
          isCalled: true,
        })
      }
    }
  }

  /**
   * Apply the floor map to a freshly WRITE-attached cell and publish the
   * resulting slice immediately (kind `floors`, then `confirmPublished`)
   * BEFORE the session runs anything — read-only sessions stay empty-slice.
   * Returns the landed position, `null` if no floor needed applying, or
   * `{ lost: true }` when the publish lost its CAS (caller re-attaches).
   *
   * Floors are applied ONLY on write-attach: `setval` writes WAL, and a
   * read-attached cell can never publish — a read cell that upgrades gets
   * floors at upgrade time, and observed-but-unpublished draws stay covered
   * by the in-memory floor map until some write-attach publishes them.
   */
  async applyFloors(
    cell: Cell,
  ): Promise<{ lost: boolean; pos: { offset: string; lsn: bigint } | null }> {
    let applied = false
    for (const floor of this.floors.values()) {
      const cur = (
        await cell.db.query<{ v: string | null }>(
          `select last_value::text as v from pg_sequences
            where schemaname = $1 and sequencename = $2`,
          [floor.schema, floor.name],
        )
      ).rows
      if (cur.length === 0) continue // sequence absent in this state
      const current = cur[0].v === null ? null : BigInt(cur[0].v)
      if (current !== null && current >= floor.value) continue
      await cell.db.query(
        `select setval(
           (quote_ident($1) || '.' || quote_ident($2))::regclass,
           $3::bigint, $4::boolean)`,
        [floor.schema, floor.name, floor.value.toString(), floor.isCalled],
      )
      applied = true
    }
    if (!applied) return { lost: false, pos: null }
    const slice = await cell.captureSlice()
    if (slice === null) return { lost: false, pos: null }
    const res = await this.commitFromSession({
      commitId: randomUUID(),
      kind: 'floors',
      baseLsn: slice.baseLsn,
      endLsn: slice.endLsn,
      bytes: slice.bytes,
    })
    if (!res.landed) return { lost: true, pos: null }
    cell.confirmPublished(slice.endLsn)
    return { lost: false, pos: { offset: res.nextOffset, lsn: slice.endLsn } }
  }

  /**
   * Append the gc-pin L frame for a newly tainted (pinned) session (§3.3).
   * Advisory at M1 (nothing enforces it); persistent CAS losses are logged.
   */
  async appendGcPin(
    sessionId: string,
    base: { offset: string; lsn: bigint },
  ): Promise<void> {
    const committer = this.committer
    for (let i = 0; i < 3; i++) {
      const res = await committer.appendControl((expectedOffset) => {
        const frame: LFrame = {
          type: 'L',
          header: {
            v: 1,
            eraId: this.manifest.era.id,
            expectedOffset,
            kind: 'gc-pin',
            holder: sessionId,
            epoch: committer.epoch,
            ttlMs: this.opts.pinTtlMs,
            base: { offset: base.offset, lsn: formatLsn(base.lsn) },
          },
        }
        return [frame]
      })
      if (res.landed) return
      await this.tailer.catchUp()
    }

    console.log(
      `[pglite-cell-server] db ${this.databaseId}: gc-pin append for ` +
        `${sessionId} kept losing its CAS — pin tracked host-side only`,
    )
  }

  /** Create and register a session (read-attach by default; lazy attach). */
  connectSession(): HostSession {
    const session = new HostSession(this)
    this.sessions.add(session)
    return session
  }

  removeSession(session: HostSession): void {
    this.sessions.delete(session)
  }

  /**
   * Hibernate: for each idle (not-in-txn, untainted) session cell, publish
   * the detach slice if it is canonical write-attached (clean close writes
   * session-teardown WAL + a real shutdown checkpoint — the stream tail
   * then ends in a shutdown record); read-attached cells just close
   * (nothing publishable). Mid-transaction or tainted sessions lose
   * unreplayable state and are fatally reset. Then drop every dir except
   * the commit journal (producer identity persists). Wake = fresh
   * activation.
   */
  async hibernate(): Promise<void> {
    if (this._state !== 'active') return
    this.clearHibernateTimer()

    // Checkpoint FIRST when the tail past the last checkpoint exceeds the
    // threshold (default 0 = always). This is what makes wake cheap (zero
    // W-slice replay past the checkpoint) and bounds the M1c ever-longer-tail
    // cost. A negative threshold disables it. Best-effort: a failed
    // checkpoint still hibernates (wake replays the tail as before).
    const hibBytes = this.opts.checkpointOnHibernateBytes
    if (hibBytes >= 0 && this.tailBytesSinceCheckpoint() >= BigInt(hibBytes)) {
      try {
        await this.checkpoint()
      } catch (err) {
        console.log(
          `[pglite-cell-server] db ${this.databaseId}: hibernate ` +
            `checkpoint failed (continuing): ` +
            `${err instanceof Error ? err.message : String(err)}`,
        )
      }
    }

    for (const session of [...this.sessions]) {
      await session._hibernateCell()
    }
    this._baseDirs?.destroy()
    this._baseDirs = null
    this._tailer = null
    this._committer = null
    this._manifest = null
    this._state = 'hibernated'

    console.log(`[pglite-cell-server] db ${this.databaseId} hibernated`)
  }

  /** Arm/reset the idle-hibernation timer (no-op unless configured). */
  touch(): void {
    if (this.opts.hibernateAfterMs === undefined) return
    this.clearHibernateTimer()
    this.hibernateTimer = setTimeout(() => {
      const allIdle = [...this.sessions].every((s) => s.isIdle())
      if (this._state === 'active' && allIdle) {
        void this.hibernate()
      } else {
        this.touch()
      }
    }, this.opts.hibernateAfterMs)
    this.hibernateTimer.unref?.()
  }

  private clearHibernateTimer(): void {
    if (this.hibernateTimer) {
      clearTimeout(this.hibernateTimer)
      this.hibernateTimer = null
    }
  }
}
