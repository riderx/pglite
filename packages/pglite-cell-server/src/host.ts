// CellHost — the multi-tenant cell host (§14.3): one DatabaseRuntime per
// active database, created lazily on first connect. The M1d session proxy
// mounts on top of `connect()`; at M1c the session API is programmatic.

import { mkdirSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { gatewayHandle } from './gateway'
import type { GatewayHandle, GatewayTarget } from './gateway'
import { DatabaseRuntime, resolveRuntimeOpts } from './database-runtime'
import type { ResolvedRuntimeOpts, RuntimeOpts } from './database-runtime'
import type { HostSession } from './session'
import type { CheckpointReport } from './checkpoint'
import type { RotationReport } from './rotation'
import { graduateDatabase } from './graduation'
import type { GraduationResult } from './graduation'

export interface CellHostOpts {
  /** The gateway: an in-process GatewayCore or `{ url }` for HTTP. */
  gateway: GatewayTarget
  /** Root directory for this host's disposable state (bases, journals). */
  dataRoot: string
  /** Stable host identity (head-lease holder). Random when omitted. */
  hostId?: string
  opts?: RuntimeOpts
}

export class CellHost {
  readonly hostId: string
  private readonly gateway: GatewayHandle
  private readonly dataRoot: string
  private readonly opts: ResolvedRuntimeOpts
  private readonly runtimes = new Map<string, DatabaseRuntime>()

  constructor(opts: CellHostOpts) {
    this.gateway = gatewayHandle(opts.gateway)
    this.dataRoot = opts.dataRoot
    this.hostId = opts.hostId ?? `host-${randomUUID().slice(0, 8)}`
    this.opts = resolveRuntimeOpts(opts.opts)
    mkdirSync(this.dataRoot, { recursive: true })
  }

  /**
   * Connect a session to a database (by name or id). The runtime is
   * created lazily on first connect and woken if hibernated; sessions
   * start read-attached and upgrade on their first write.
   */
  async connect(dbNameOrId: string): Promise<HostSession> {
    const databaseId = await this.gateway.resolveDatabaseId(dbNameOrId)
    let runtime = this.runtimes.get(databaseId)
    if (!runtime) {
      runtime = new DatabaseRuntime({
        databaseId,
        hostId: this.hostId,
        gateway: this.gateway,
        dataRoot: this.dataRoot,
        opts: this.opts,
      })
      this.runtimes.set(databaseId, runtime)
    }
    await runtime.ensureActive()
    runtime.touch()
    return runtime.connectSession()
  }

  /** Hibernate a database (detach slices published; dirs dropped). */
  async hibernateDatabase(dbNameOrId: string): Promise<void> {
    const databaseId = await this.gateway.resolveDatabaseId(dbNameOrId)
    await this.runtimes.get(databaseId)?.hibernate()
  }

  /**
   * Build a checkpoint for a database (M1e worker): materialize the shared
   * base to a genuine stream position, pack + upload it, CAS a K frame, and
   * register the control-plane row so future wakes hydrate it. The runtime
   * is created + activated lazily if not already resident. Idempotent.
   */
  async checkpointDatabase(dbNameOrId: string): Promise<CheckpointReport> {
    const databaseId = await this.gateway.resolveDatabaseId(dbNameOrId)
    let runtime = this.runtimes.get(databaseId)
    if (!runtime) {
      runtime = new DatabaseRuntime({
        databaseId,
        hostId: this.hostId,
        gateway: this.gateway,
        dataRoot: this.dataRoot,
        opts: this.opts,
      })
      this.runtimes.set(databaseId, runtime)
    }
    await runtime.ensureActive()
    return runtime.checkpoint()
  }

  /**
   * Rotate a database's era (M2, §6.1 steps 0–6): checkpoint, cut era N+1
   * at a unique per-attempt URL, seal era N with the terminal S frame, and
   * record the transition in the control plane. Idempotent / re-entrant —
   * a re-run after any crash completes the pending transition. The runtime
   * is created + activated lazily if not already resident.
   */
  async rotateDatabase(dbNameOrId: string): Promise<RotationReport> {
    const databaseId = await this.gateway.resolveDatabaseId(dbNameOrId)
    let runtime = this.runtimes.get(databaseId)
    if (!runtime) {
      runtime = new DatabaseRuntime({
        databaseId,
        hostId: this.hostId,
        gateway: this.gateway,
        dataRoot: this.dataRoot,
        opts: this.opts,
      })
      this.runtimes.set(databaseId, runtime)
    }
    await runtime.ensureActive()
    return runtime.rotate()
  }

  /**
   * Graduate a database (M6, §15): produce a logical pg_dump export at a
   * linearizable-fresh head plus a manifest snapshot pinning the exported
   * stream position. The documented migration path OUT of the fleet (OQ7:
   * physical graduation to stock Postgres is closed; logical is THE path).
   * The runtime is created + activated lazily if not already resident.
   */
  async graduateDatabase(dbNameOrId: string): Promise<GraduationResult> {
    const databaseId = await this.gateway.resolveDatabaseId(dbNameOrId)
    let runtime = this.runtimes.get(databaseId)
    if (!runtime) {
      runtime = new DatabaseRuntime({
        databaseId,
        hostId: this.hostId,
        gateway: this.gateway,
        dataRoot: this.dataRoot,
        opts: this.opts,
      })
      this.runtimes.set(databaseId, runtime)
    }
    await runtime.ensureActive()
    return graduateDatabase(runtime)
  }

  /**
   * The resident runtime for a database id, if any (test hook + operational
   * introspection). Does NOT create or activate — returns undefined when no
   * runtime is resident.
   */
  runtimeFor(databaseId: string): DatabaseRuntime | undefined {
    return this.runtimes.get(databaseId)
  }

  /** Databases with an ACTIVE runtime on this host. */
  listActive(): { databaseId: string; sessions: number }[] {
    return [...this.runtimes.values()]
      .filter((r) => r.state === 'active')
      .map((r) => ({ databaseId: r.databaseId, sessions: r.sessionCount }))
  }

  /** Hibernate every active database (scale-to-zero shutdown). */
  async shutdown(): Promise<void> {
    for (const runtime of this.runtimes.values()) {
      await runtime.hibernate()
    }
  }
}
