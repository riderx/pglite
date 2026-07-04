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
