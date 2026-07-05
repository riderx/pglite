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
  GFrame,
  LFrame,
  NFrame,
} from '@electric-sql/pglite-cell'
import { extractDatadir } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { BaseDirManager } from './base-dir'
import type { GatewayHandle } from './gateway'
import { HostSession } from './session'
import { checkpointDatabase } from './checkpoint'
import type { CheckpointReport } from './checkpoint'
import { rotateDatabase } from './rotation'
import type { RotationReport } from './rotation'
import { Janitor, resolveJanitorOpts, janitorEnabled } from './janitor'
import type { JanitorOpts, ResolvedJanitorOpts } from './janitor'

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
   * last checkpoint exceed this, fire-and-forget a checkpoint. Undefined
   * (default) = use the database's `checkpoint_every_bytes` manifest dial;
   * an explicit value (including 0 = disabled) overrides the dial.
   */
  checkpointEveryBytes?: number
  /**
   * Era rotation trigger (M2 dial): after a checkpoint, if the era's byte
   * length since its base exceeds this, fire-and-forget a rotation.
   * Undefined (default) = use the database's `rotate_every_bytes` manifest
   * dial; an explicit value (including 0 = never) overrides it.
   */
  rotateEveryBytes?: number
  /**
   * Sequence-grant range size (§5.3; natively enforced by the M5a
   * `nextval_internal` lease clamp). Default 4096. Tests use tiny sizes
   * to exercise the clamp/renew path.
   */
  sequenceGrantSize?: bigint
  /**
   * M5e commit gate (§3.6): defer the ON COMMIT DELETE ROWS truncate
   * past the CAS verdict on every cell this runtime opens. Default true
   * (off = vanilla commit sequence, pre-M5e contract).
   */
  commitGate?: boolean
  /**
   * Background maintenance dials (M6 janitor, §6.4): per-active-database
   * vacuum / freeze-age / GC scheduling. All OFF by default.
   */
  janitor?: JanitorOpts
  /**
   * Advisory-lock policy (M6, §4.6). `pg_advisory_*` locks are cell-local:
   * two hosts' locks do NOT exclude each other, so cross-cell mutual
   * exclusion is not provided.
   *
   * - 'local-warn' (default): the FIRST advisory-lock use per session
   *   injects a synthesized WARNING (01000) naming the cell-local scope
   *   before the statement's output; the statement still runs.
   * - 'error': any advisory-lock statement is rejected with 0A000 and NOT
   *   executed.
   */
  advisoryLocks?: 'local-warn' | 'error'
}

export interface ResolvedRuntimeOpts {
  leaseTtlMs: number
  pinTtlMs: number
  maxRetries: number
  attachAttempts: number
  hibernateAfterMs: number | undefined
  checkpointOnHibernateBytes: number
  /** Undefined = defer to the manifest dial. */
  checkpointEveryBytes: number | undefined
  /** Undefined = defer to the manifest dial. */
  rotateEveryBytes: number | undefined
  sequenceGrantSize: bigint
  commitGate: boolean
  janitor: ResolvedJanitorOpts
  advisoryLocks: 'local-warn' | 'error'
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
    checkpointEveryBytes: opts.checkpointEveryBytes,
    rotateEveryBytes: opts.rotateEveryBytes,
    sequenceGrantSize: opts.sequenceGrantSize ?? 4096n,
    commitGate: opts.commitGate ?? true,
    janitor: resolveJanitorOpts(opts.janitor),
    advisoryLocks: opts.advisoryLocks ?? 'local-warn',
  }
}
/** Bound on grant-take CAS retries (each loss re-reads the high-water). */
const GRANT_CAS_ATTEMPTS = 10

/** One live sequence grant this host incarnation holds (M4, §5.3): the
 *  host may draw values in `(start, end]`. Burned (never resumed) on any
 *  incarnation end — hibernate, recycle, crash. */
export interface SequenceGrant {
  schema: string
  name: string
  /** `schema.name` — the G-frame `seqName`. */
  seqName: string
  /** The sequence relation OID (stable across cells of one database) —
   *  the key the native lease clamp is registered under (M5a). */
  oid: number
  start: bigint
  end: bigint
}

/** The head-lease view for tests/operators (M4, §3.2). */
export interface LeaseState {
  /** Holder of the last L{head} frame seen, null before any. */
  holder: string | null
  epoch: number
  ttlMs: number
  /** True while the lease is within TTL of its local observation time. */
  fresh: boolean
  /** True iff this host holds a fresh head lease. */
  held: boolean
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

/** One notification delivered off the tailer (M3, §10.2): stream order ==
 *  commit order, globally — the committing session's own connection hears
 *  it via this same path. */
export interface DeliveredNotification {
  channel: string
  payload: string
  commitId: string
  commitLsn: string
  /** Stream offset of the append group the N frame rode in. */
  offset: string
}

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

  /**
   * Live sequence grants held by THIS runtime incarnation (M4, §5.3),
   * keyed `schema.name`. Cleared (burned) on hibernate — a fresh
   * incarnation must take a fresh grant at/above the replayed high-water,
   * never resuming a predecessor's residual range (§5.3 rule 7).
   */
  readonly grants = new Map<string, SequenceGrant>()

  private readonly sessions = new Set<HostSession>()

  /**
   * The LISTEN registry (M3, §10.2 step 1): the union of channels listened
   * across this runtime's sessions, refcounted. Cells auto-LISTEN this
   * union so `onNotification` harvests fire for any channel with ≥1 local
   * listener; `listenVersion` bumps on every union change so open cells
   * delta-apply between units.
   */
  private readonly listenRefs = new Map<string, number>()
  listenVersion = 0
  /** N-frame delivery subscribers (proxy connections, programmatic hooks). */
  private readonly notifSubs = new Set<(n: DeliveredNotification) => void>()
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
  /** Guards against concurrent / re-entrant rotations (in-flight). */
  private rotating: Promise<RotationReport> | null = null
  /** True while hibernate() runs (suppresses the auto-rotate trigger). */
  private hibernating = false
  /** The current era's base LSN (rotation trigger measures from here). */
  private eraBaseLsn = 0n
  /** Control-plane pin ids this host holds (mirror of L{gc-pin} frames). */
  private readonly pinIds = new Set<string>()
  /** The background maintenance janitor (M6 §6.4); null when disabled. */
  private _janitor: Janitor | null = null

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

  /** The background maintenance janitor (M6), or null when disabled/inactive. */
  get janitor(): Janitor | null {
    return this._janitor
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

    // Incarnation burn (M4, §5.3 rule 7): every grant on the era chain is
    // either LIVE (a foreign incarnation's — we must not draw in it) or
    // BURNED (a dead incarnation's residual — never resumed, ours
    // included). Either way this fresh incarnation may only draw ABOVE
    // every replayed grant's end, so floor each granted sequence to its
    // high-water before any session draws. Gaps allowed; duplicates not.
    for (const [seqName, hw] of tailer.grantHighWaters()) {
      const dot = seqName.indexOf('.')
      const schema = seqName.slice(0, dot)
      const name = seqName.slice(dot + 1)
      const existing = this.floors.get(seqName)
      if (!existing || hw > existing.value) {
        this.floors.set(seqName, { schema, name, value: hw, isCalled: true })
      }
    }

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

    // Uniform notification delivery (M3, §10.2 step 4): EVERY N frame this
    // runtime observes — own commits via the committer's local advance,
    // foreign ones via catch-up — fans out in stream order.
    tailer.onNotificationFrame = (header, offset) => {
      const n: DeliveredNotification = {
        channel: header.channel,
        payload: header.payload,
        commitId: header.commitId,
        commitLsn: header.commitLsn,
        offset,
      }
      for (const sub of [...this.notifSubs]) {
        try {
          sub(n)
        } catch {
          // Delivery is fire-and-forget (vanilla semantics): a subscriber
          // failure never poisons the tailer dispatch path.
        }
      }
    }

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
    this.eraBaseLsn = parseLsn(manifest.era.baseLsn)
    this._state = 'active'
    await this.appendHeadLease()

    // Background maintenance (M6 janitor, §6.4): armed only when a dial is
    // set. A fresh janitor per activation — its timers are cleared on
    // hibernate; the wake builds a new one.
    if (janitorEnabled(this.opts.janitor)) {
      this._janitor = new Janitor(this, this.opts.janitor)
      this._janitor.start()
    }

    console.log(
      `[pglite-cell-server] db ${this.databaseId} active in ` +
        `${Date.now() - t0}ms (manifest -> hydrated checkpoint -> ` +
        `tail head ${formatLsn(tailer.head.lsn)})`,
    )
  }

  private raiseWatermark(pos: { offset: string; lsn: bigint }): void {
    if (pos.lsn > this.watermark.lsn) this.watermark = { ...pos }
  }

  /** The live L{head} view: last frame seen + local-observation freshness
   *  (L headers carry no timestamp — freshness is judged from when THIS
   *  tailer dispatched the frame). */
  private headLeaseView(): {
    holder: string | null
    epoch: number
    ttlMs: number
    fresh: boolean
    foreign: boolean
  } {
    const lease = this._tailer?.leases.head
    const at = this._tailer?.leaseSeenAt.head ?? 0
    if (!lease) {
      return {
        holder: null,
        epoch: 0,
        ttlMs: this.opts.leaseTtlMs,
        fresh: false,
        foreign: false,
      }
    }
    return {
      holder: lease.holder,
      epoch: lease.epoch,
      ttlMs: lease.ttlMs,
      fresh: Date.now() - at < lease.ttlMs,
      foreign: lease.holder !== this.hostId,
    }
  }

  /** The head-lease state (M4 test/introspection surface). */
  leaseState(): LeaseState {
    const v = this.headLeaseView()
    return {
      holder: v.holder,
      epoch: v.epoch,
      ttlMs: v.ttlMs,
      fresh: v.fresh,
      held: v.fresh && !v.foreign && v.holder !== null,
    }
  }

  /**
   * Append (claim/refresh) the head-lease L frame (M4 §3.2 semantics; the
   * lease still has ZERO correctness weight):
   *
   * - a FRESH foreign lease means another host holds — do not claim; a
   *   demoted holder lands here too and thereby stops refreshing;
   * - an expired/absent lease is claimable by anyone (migration): the
   *   claim epoch is `max(committer epoch, last lease epoch + 1)` so the
   *   L{head} epoch chain stays strictly monotone across claims;
   * - a refresh of our own lease keeps `max(committer epoch, lease epoch)`.
   *
   * A CAS loss re-reads the tail; if the loss revealed a fresh foreign
   * lease, adopt it (stop). Persistent losses are logged and ignored.
   */
  private async appendHeadLease(): Promise<void> {
    const committer = this.committer
    for (let i = 0; i < 3; i++) {
      const v = this.headLeaseView()
      if (v.fresh && v.foreign) return // another host holds: adopt, no claim
      const epoch =
        v.holder !== null && !v.foreign
          ? Math.max(committer.epoch, v.epoch) // refresh our own lease
          : Math.max(committer.epoch, v.epoch + 1) // claim / migrate
      const res = await committer.appendControl((expectedOffset) => {
        const frame: LFrame = {
          type: 'L',
          header: {
            v: 1,
            // The tailer's CURRENT era, not the (possibly stale) manifest —
            // the era may have rotated since activation (M2).
            eraId: this.tailer.currentEra.id,
            expectedOffset,
            kind: 'head',
            holder: this.hostId,
            epoch,
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
        `losing its CAS — continuing without (advisory)`,
    )
  }

  /**
   * Lease-aware backoff after a CAS loss (M4, §3.2): when a FRESH foreign
   * head lease exists, the holder is pipelining — delay this host's retry
   * (25–100ms jittered) instead of hammering the CAS. No lease / stale
   * lease / own lease ⇒ no delay (the optimistic path proceeds).
   */
  private async leaseAwareBackoff(): Promise<void> {
    const v = this.headLeaseView()
    if (v.fresh && v.foreign) {
      const delay = 25 + Math.floor(Math.random() * 75)
      await new Promise((r) => setTimeout(r, delay))
    }
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
      if (err instanceof CaptureCursorError) {
        await this.leaseAwareBackoff()
        return { landed: false }
      }
      throw err
    }
    if (res.landed) {
      this.raiseWatermark({ offset: res.nextOffset, lsn: input.endLsn })
      this.maybeAutoCheckpoint()
    } else {
      await this.leaseAwareBackoff()
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
   * The effective checkpoint-cadence threshold: the host option when set,
   * else the database's `checkpoint_every_bytes` manifest dial (M2).
   */
  private effectiveCheckpointEveryBytes(): number {
    if (this.opts.checkpointEveryBytes !== undefined) {
      return this.opts.checkpointEveryBytes
    }
    return this._manifest
      ? Number(this._manifest.dials.checkpointEveryBytes)
      : 0
  }

  /**
   * The effective rotation threshold: the host option when set, else the
   * database's `rotate_every_bytes` manifest dial (M2; 0 = never).
   */
  private effectiveRotateEveryBytes(): number {
    if (this.opts.rotateEveryBytes !== undefined) {
      return this.opts.rotateEveryBytes
    }
    return this._manifest ? Number(this._manifest.dials.rotateEveryBytes) : 0
  }

  /**
   * Auto-cadence (§2.4 dial): once the tail past the last checkpoint exceeds
   * the effective threshold, fire-and-forget a checkpoint. Guarded by the
   * in-flight flag so a slow checkpoint never overlaps another. Disabled
   * when the effective threshold is 0.
   */
  private maybeAutoCheckpoint(): void {
    const threshold = this.effectiveCheckpointEveryBytes()
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
      // Rotation dial (M2): after a checkpoint, rotate when the era has
      // grown past `rotate_every_bytes`. No-op while a rotation is already
      // in flight (including the one that ran THIS checkpoint).
      this.maybeAutoRotate()
    }
  }

  /**
   * Fire-and-forget rotation trigger (M2 dial): after a checkpoint, if the
   * era's byte length since its base (LSN delta as a proxy) exceeds the
   * effective `rotate_every_bytes`, rotate. Guarded by the in-flight flag.
   */
  private maybeAutoRotate(): void {
    const threshold = this.effectiveRotateEveryBytes()
    if (threshold <= 0 || this.rotating !== null || this.hibernating) return
    if (this._state !== 'active' || this._tailer === null) return
    const eraBytes = this._tailer.head.lsn - this.eraBaseLsn
    if (eraBytes < BigInt(threshold)) return
    void this.rotate().catch((err) => {
      console.log(
        `[pglite-cell-server] db ${this.databaseId}: auto-rotate failed: ` +
          `${err instanceof Error ? err.message : String(err)}`,
      )
    })
  }

  /**
   * Rotate this database's era (M2, §6.1 steps 0–6). Serialized by the
   * in-flight guard — concurrent callers share the same run. Idempotent /
   * re-entrant: a re-run after any crash completes the pending transition.
   */
  async rotate(): Promise<RotationReport> {
    if (this.rotating !== null) return this.rotating
    const run = (async (): Promise<RotationReport> => {
      await this.ensureActive()
      return rotateDatabase(this)
    })()
    this.rotating = run
    try {
      return await run
    } finally {
      this.rotating = null
    }
  }

  /**
   * Re-read the manifest (rotation finished — the era moved). Refreshes the
   * era-base LSN the rotation dial measures from and the latest-checkpoint
   * LSN. The tailer/committer are NOT rebuilt: they already hopped in place.
   */
  async refreshManifest(): Promise<void> {
    const manifest = await this._gateway.getManifest(this.databaseId)
    this._manifest = manifest
    this.eraBaseLsn = parseLsn(manifest.era.baseLsn)
    const ckptLsn = parseLsn(manifest.checkpoint.snapEnd)
    if (ckptLsn > this.lastCheckpointLsn) this.lastCheckpointLsn = ckptLsn
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
    const rows = await this.querySequences(cell)
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
    // Grant maintenance rides the same probe (M4, §5.3): an abort-observed
    // draw is exactly the evidence a grant must cover.
    await this.ensureGrants(rows)
    this.applyLeases(cell)
  }

  /**
   * Grant maintenance probe for a cell whose transaction COMMITTED and
   * landed (M4, §5.3): first `nextval` evidence takes a grant; ≥50%
   * consumption renews it. Unlike `probeFloors` this does not raise the
   * floor map from observed values (committed draws are published — the
   * stream itself covers them).
   */
  async probeGrants(cell: Cell): Promise<void> {
    await this.ensureGrants(await this.querySequences(cell))
    this.applyLeases(cell)
  }

  /**
   * Register this runtime's live grants as native leases on a cell (M5a,
   * §5.3 rule 1): every granted sequence's `nextval` clamps to the grant
   * end. Applied on every cell open (after floors) and re-applied after
   * every grant take/renewal. `resetCaches` additionally flushes the
   * backend-local SeqTable cache (§5.3 rule 3) — cell-open only: mid-
   * session it would wipe `currval` state, and cached prepaid values are
   * always within the lease that admitted them.
   */
  applyLeases(cell: Cell, opts: { resetCaches?: boolean } = {}): void {
    for (const g of this.grants.values()) {
      cell.setSequenceLease(g.oid, g.end)
    }
    if (opts.resetCaches) cell.resetSequenceCaches()
  }

  /**
   * Reactive grant renewal on a native `sequence lease exhausted` error
   * (M5a): the standard probe/renew ran already (the abort's floors probe),
   * but one grant of headroom may not cover the unit — a re-executed batch
   * redraws EVERYTHING it drew before dying. Escalate: chain up to
   * `2^escalation - 1` additional grants beyond the renewal so each retry
   * doubles the headroom, then re-register the leases on the cell.
   */
  async extendGrantsForRetry(cell: Cell, escalation: number): Promise<void> {
    const rows = await this.querySequences(cell)
    await this.ensureGrants(rows)
    const extra = (1 << escalation) - 1
    for (const row of rows) {
      const key = `${row.s}.${row.n}`
      let g = this.grants.get(key)
      if (!g) continue
      for (let i = 0; i < extra; i++) {
        const fresh = await this.takeGrant(row.s, row.n, row.o, g.end)
        if (fresh === null) break
        g =
          fresh.start === g.end
            ? { ...fresh, start: g.start } // contiguous own extension
            : fresh
        this.grants.set(key, g)
      }
    }
    this.applyLeases(cell)
  }

  private async querySequences(
    cell: Cell,
  ): Promise<{ s: string; n: string; v: string; o: number }[]> {
    return (
      await cell.db.query<{ s: string; n: string; v: string; o: number }>(
        `select p.schemaname as s, p.sequencename as n,
                p.last_value::text as v, c.oid::int4 as o
           from pg_sequences p
           join pg_namespace ns on ns.nspname = p.schemaname
           join pg_class c on c.relnamespace = ns.oid
                          and c.relname = p.sequencename
          where p.last_value is not null`,
      )
    ).rows
  }

  /**
   * The M4 §5.3 grant state machine, probe-driven. For each sequence with
   * an observed draw:
   *
   * - no grant yet (first evidence) ⇒ take one at
   *   `start = max(observed, floor, replayed high-water)`;
   * - ≥50% of the current grant consumed ⇒ renew (next disjoint range at
   *   the high-water; a contiguous own-extension keeps cells drawing
   *   naturally, a jump re-floors future attaches to the new start);
   * - observed PAST the grant end ⇒ a FOREIGN published draw replayed
   *   into a fresh cell advanced the page (own draws are natively lease-
   *   clamped since M5a): logged, and a fresh grant is taken from the
   *   observed value.
   */
  private async ensureGrants(
    rows: { s: string; n: string; v: string; o: number }[],
  ): Promise<void> {
    for (const row of rows) {
      const key = `${row.s}.${row.n}`
      const observed = BigInt(row.v)
      const g = this.grants.get(key)
      if (g) {
        if (observed > g.end) {
          // Own draws are natively clamped to the lease since M5a, but
          // under multi-host traffic the on-disk value routinely reflects
          // FOREIGN published draws replayed into fresh cells — those land
          // here: take a fresh grant from the observed value so the grant
          // chain stays ahead of every published draw.
          console.log(
            `[pglite-cell-server] db ${this.databaseId}: sequence ${key} ` +
              `observed at ${observed}, past this host's grant end ` +
              `${g.end} (foreign replay advance); taking a fresh grant ` +
              `from the observed value`,
          )
        } else if (observed < g.start + (g.end - g.start) / 2n) {
          continue // under 50% consumed: nothing to do
        }
      }
      const floor = this.floors.get(key)?.value ?? 0n
      const minStart = g
        ? observed > g.end
          ? observed
          : g.end
        : observed > floor
          ? observed
          : floor
      const fresh = await this.takeGrant(row.s, row.n, row.o, minStart)
      if (fresh === null) continue // logged; retried on the next probe
      if (g && fresh.start === g.end && observed <= g.end) {
        // Contiguous own extension: the live cells keep drawing naturally
        // across the old end — no re-floor, no gap.
        this.grants.set(key, { ...fresh, start: g.start })
      } else {
        // Fresh grant or a jump over foreign ranges: future attaches must
        // draw inside the new range.
        this.grants.set(key, fresh)
        const existing = this.floors.get(key)
        if (!existing || fresh.start > existing.value) {
          this.floors.set(key, {
            schema: row.s,
            name: row.n,
            value: fresh.start,
            isCalled: true,
          })
        }
      }
    }
  }

  /**
   * CAS-append one G frame for `(schema, name)` (M4, §5.3). The range is
   * computed INSIDE the append callback — `start = max(minStart, replayed
   * high-water)` at the moment of the attempt — so a CAS loss to a foreign
   * grant re-reads and takes the next disjoint range. Bounded; a
   * persistent loss is logged and reported null (retried next probe).
   */
  private async takeGrant(
    schema: string,
    name: string,
    oid: number,
    minStart: bigint,
  ): Promise<SequenceGrant | null> {
    const seqName = `${schema}.${name}`
    const committer = this.committer
    for (let i = 0; i < GRANT_CAS_ATTEMPTS; i++) {
      let start = 0n
      let end = 0n
      const res = await committer.appendControl((expectedOffset) => {
        const hw = this.tailer.grantHighWater(seqName)
        start = minStart > hw ? minStart : hw
        end = start + this.opts.sequenceGrantSize
        const frame: GFrame = {
          type: 'G',
          header: {
            v: 1,
            eraId: this.tailer.currentEra.id,
            expectedOffset,
            kind: 'sequence',
            seqName,
            start: start.toString(),
            end: end.toString(),
            grantee: this.hostId,
            granteeEpoch: committer.epoch,
          },
        }
        return [frame]
      })
      if (res.landed) return { schema, name, seqName, oid, start, end }
      await this.tailer.catchUp() // another host granted first: re-read
    }
    console.log(
      `[pglite-cell-server] db ${this.databaseId}: sequence-grant append ` +
        `for ${seqName} kept losing its CAS after ${GRANT_CAS_ATTEMPTS} ` +
        `attempts — will retry on the next probe`,
    )
    return null
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
    // Mirror the pin into the control-plane `pins` table (§6.4: the stream
    // stays the in-band truth, the table is the queryable index GC honors).
    // Best-effort: a mirror failure never blocks the session.
    const pinId = randomUUID()
    try {
      await this._gateway.upsertPin(this.databaseId, {
        id: pinId,
        kind: 'gc-pin',
        holder: sessionId,
        pinnedOffset: base.offset,
        pinnedLsn: formatLsn(base.lsn),
        expiresAt: new Date(Date.now() + this.opts.pinTtlMs),
      })
      this.pinIds.add(pinId)
    } catch (err) {
      console.log(
        `[pglite-cell-server] db ${this.databaseId}: control-plane pin ` +
          `mirror failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    }

    const committer = this.committer
    for (let i = 0; i < 3; i++) {
      const res = await committer.appendControl((expectedOffset) => {
        const frame: LFrame = {
          type: 'L',
          header: {
            v: 1,
            eraId: this.tailer.currentEra.id,
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
    if (this.sessions.delete(session)) {
      for (const channel of session.listenChannels) {
        this.releaseListen(channel)
      }
    }
  }

  // ----- M3: LISTEN registry + notification fanout (§10.2) -----

  /** Channels with ≥1 listening session on this host (the cell union). */
  get listenUnion(): string[] {
    return [...this.listenRefs.keys()].sort()
  }

  /** Refcount a channel into the union (bumps `listenVersion` when new). */
  acquireListen(channel: string): void {
    const n = this.listenRefs.get(channel) ?? 0
    this.listenRefs.set(channel, n + 1)
    if (n === 0) this.listenVersion++
  }

  /** Drop one reference (bumps `listenVersion` when the channel empties). */
  releaseListen(channel: string): void {
    const n = this.listenRefs.get(channel)
    if (n === undefined) return
    if (n <= 1) {
      this.listenRefs.delete(channel)
      this.listenVersion++
    } else {
      this.listenRefs.set(channel, n - 1)
    }
  }

  /**
   * Subscribe to tailer-driven notification delivery (M3, §10.2): `cb`
   * fires for EVERY N frame in stream order; callers filter by their own
   * listen set. Returns the unsubscribe function. Survives hibernation
   * (the wake re-wires a fresh tailer to the same subscriber set).
   */
  subscribeNotifications(cb: (n: DeliveredNotification) => void): () => void {
    this.notifSubs.add(cb)
    return () => this.notifSubs.delete(cb)
  }

  /**
   * `linearizable` freshness (§7): confirm the true stream head with a
   * catch-up round-trip past the observed tail, then raise the watermark
   * so the ordinary gate advances the session's cell. The only mode
   * guaranteed to see a commit acked via ANOTHER host an instant ago.
   */
  async linearizableSync(): Promise<void> {
    await this.tailer.catchUp()
    this.raiseWatermark({
      offset: this.tailer.head.offset,
      lsn: this.tailer.head.lsn,
    })
  }

  /**
   * Block until this host's tailer has ingested the stream up to `lsn`
   * (M4 cross-host session tokens, §7): a client that committed on host A
   * carries the commit LSN as a session token; host B waits for it, the
   * watermark rises, and the ordinary `session` gate serves read-your-
   * writes. Polls catch-up (bounded by `timeoutMs`).
   */
  async waitForLsn(lsn: bigint, timeoutMs = 30_000): Promise<void> {
    await this.ensureActive()
    const t0 = Date.now()
    for (;;) {
      await this.tailer.catchUp()
      if (this.tailer.head.lsn >= lsn) break
      if (Date.now() - t0 > timeoutMs) {
        throw new Error(
          `waitForLsn: head ${formatLsn(this.tailer.head.lsn)} still below ` +
            `${formatLsn(lsn)} after ${timeoutMs}ms`,
        )
      }
      await new Promise((r) => setTimeout(r, 25))
    }
    this.raiseWatermark({
      offset: this.tailer.head.offset,
      lsn: this.tailer.head.lsn,
    })
  }

  /**
   * Publish harvested notifications from a transaction that committed
   * with an EMPTY capture (a pure `NOTIFY` writes no WAL — M3 leftover,
   * fixed at M4): CAS-append the N frames ALONE, `commitLsn` = the head
   * LSN at append time. The tailer fanout then delivers them everywhere,
   * this host included. Bounded retries; a persistent loss is logged
   * (delivery is fire-and-forget, matching vanilla's weak guarantees).
   */
  async publishNotificationOnlyCommit(
    notifications: { channel: string; payload: string }[],
  ): Promise<void> {
    if (notifications.length === 0) return
    const committer = this.committer
    const commitId = randomUUID()
    for (let i = 0; i < 3; i++) {
      const res = await committer.appendControl((expectedOffset) =>
        notifications.map(
          (n): NFrame => ({
            type: 'N',
            header: {
              v: 1,
              eraId: this.tailer.currentEra.id,
              expectedOffset,
              commitId,
              channel: n.channel,
              payload: n.payload,
              commitLsn: formatLsn(this.tailer.head.lsn),
            },
          }),
        ),
      )
      if (res.landed) return
      await this.tailer.catchUp()
    }
    console.log(
      `[pglite-cell-server] db ${this.databaseId}: NOTIFY-only append kept ` +
        `losing its CAS — ${notifications.length} notification(s) dropped`,
    )
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
    this.hibernating = true

    // Run-once janitor hooks (freeze check + opportunistic GC), then stop the
    // timers — the maintenance loop must not outlive the incarnation. The
    // hook runs while the runtime is still active (it needs a session).
    if (this._janitor) {
      try {
        await this._janitor.onHibernate()
      } catch {
        // Best-effort: maintenance must never block hibernation.
      }
      this._janitor.stop()
      this._janitor = null
    }

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

    // Release this host's control-plane pins: hibernation fatally reset any
    // pinned (tainted) session, so its pins protect nothing anymore. The
    // in-band L{gc-pin} frames expire by TTL; the table mirror is dropped
    // eagerly. Best-effort.
    for (const pinId of [...this.pinIds]) {
      try {
        await this._gateway.deletePin(pinId)
      } catch {
        // TTL expiry sweeps it eventually.
      }
      this.pinIds.delete(pinId)
    }

    this._baseDirs?.destroy()
    this._baseDirs = null
    this._tailer = null
    this._committer = null
    this._manifest = null
    // Incarnation burn (M4, §5.3 rule 7): the residual grant ranges die
    // with this incarnation — the wake takes fresh grants at/above the
    // replayed high-water (activate() floors every granted sequence).
    this.grants.clear()
    this._state = 'hibernated'
    this.hibernating = false

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
