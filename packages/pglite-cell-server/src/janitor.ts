// Janitor — per-active-database background maintenance driven by the host
// (M6, design §6.4). Three timers, ALL OFF by default:
//
//   - vacuumIntervalMs  → `VACUUM (ANALYZE)` through an ordinary internal
//                         session (its commit rides the normal CAS path and
//                         appears in the stream as an ordinary W frame — the
//                         commit IS the "last vacuum" marker). We track the
//                         last run host-locally and skip if another vacuum
//                         (this host's) ran within the interval; a duplicate
//                         vacuum across hosts is harmless (§6.4: vacuum is
//                         just another CAS transaction).
//   - freezeMaxAge      → poll `age(datfrozenxid)`; once past the threshold,
//                         run `VACUUM (FREEZE)` to pull the frozen horizon
//                         forward (wraparound defence).
//   - gcIntervalMs      → call the gateway GC (`runGc`) for this database.
//
// The janitor also exposes a run-once `onHibernate()` hook the runtime fires
// on the hibernate path (a freeze check + an opportunistic GC when enabled),
// so a scale-to-zero database still gets its horizon and storage tended.
//
// Every operation is best-effort: a maintenance failure is logged and never
// propagated — it must never break the database it is tending. Timers are
// unref'd (they never keep the process alive) and cleared on stop.

import type { DatabaseRuntime } from './database-runtime'

/** Janitor dials (host-wide defaults, applied per active database). */
export interface JanitorOpts {
  /**
   * Run `VACUUM (ANALYZE)` on this cadence (ms). A vacuum is skipped when
   * this host ran one within the interval. Undefined/0 = disabled.
   */
  vacuumIntervalMs?: number
  /**
   * When `age(datfrozenxid)` exceeds this many transactions, run
   * `VACUUM (FREEZE)`. Checked on the vacuum cadence (or `freezeCheckMs` if
   * vacuum is disabled) and once on hibernate. Undefined/0 = disabled.
   */
  freezeMaxAge?: number
  /**
   * Freeze-age poll cadence (ms) used when the vacuum timer is disabled but
   * freeze monitoring is on. Default 60000. Ignored when vacuum is enabled
   * (the vacuum timer carries the freeze check).
   */
  freezeCheckMs?: number
  /** Run the gateway GC on this cadence (ms). Undefined/0 = disabled. */
  gcIntervalMs?: number
}

export interface ResolvedJanitorOpts {
  vacuumIntervalMs: number
  freezeMaxAge: number
  freezeCheckMs: number
  gcIntervalMs: number
}

export function resolveJanitorOpts(
  opts: JanitorOpts = {},
): ResolvedJanitorOpts {
  return {
    vacuumIntervalMs: opts.vacuumIntervalMs ?? 0,
    freezeMaxAge: opts.freezeMaxAge ?? 0,
    freezeCheckMs: opts.freezeCheckMs ?? 60000,
    gcIntervalMs: opts.gcIntervalMs ?? 0,
  }
}

/** True iff any janitor timer would arm (avoids spinning up an idle loop). */
export function janitorEnabled(o: ResolvedJanitorOpts): boolean {
  return o.vacuumIntervalMs > 0 || o.freezeMaxAge > 0 || o.gcIntervalMs > 0
}

/** Per-run counters (TEST HOOK / operational introspection). */
export interface JanitorStats {
  vacuums: number
  freezes: number
  gcRuns: number
  /** Age reported by the last freeze check (`age(datfrozenxid)`), -1 if none. */
  lastFrozenAge: number
}

type Timer = ReturnType<typeof setInterval>

/**
 * One janitor per active DatabaseRuntime. `start()` arms the timers; the
 * runtime calls `stop()` on hibernate/shutdown. All work runs through the
 * shared runtime (an ordinary internal session for SQL, the gateway for GC).
 */
export class Janitor {
  readonly stats: JanitorStats = {
    vacuums: 0,
    freezes: 0,
    gcRuns: 0,
    lastFrozenAge: -1,
  }

  /** Freeze-age threshold override (TEST HOOK): forces a freeze run when the
   *  real age can never be aged in a test. When set, the freeze check treats
   *  the observed age as this value for the threshold comparison. */
  _forceAgeForTest: number | null = null

  private readonly opts: ResolvedJanitorOpts
  private vacuumTimer: Timer | null = null
  private freezeTimer: Timer | null = null
  private gcTimer: Timer | null = null
  private lastVacuumAt = 0
  private stopped = false
  /** Guards against overlapping SQL maintenance runs (one at a time). */
  private busy = false

  constructor(
    private readonly runtime: DatabaseRuntime,
    opts: ResolvedJanitorOpts,
  ) {
    this.opts = opts
  }

  /** Arm the enabled timers. Idempotent (a second call is a no-op). */
  start(): void {
    if (this.stopped) return
    if (this.opts.vacuumIntervalMs > 0 && this.vacuumTimer === null) {
      this.vacuumTimer = setInterval(() => {
        void this.tick('vacuum')
      }, this.opts.vacuumIntervalMs)
      this.vacuumTimer.unref?.()
    } else if (this.opts.freezeMaxAge > 0 && this.freezeTimer === null) {
      // Freeze monitoring without a vacuum timer: poll on its own cadence.
      this.freezeTimer = setInterval(() => {
        void this.tick('freeze')
      }, this.opts.freezeCheckMs)
      this.freezeTimer.unref?.()
    }
    if (this.opts.gcIntervalMs > 0 && this.gcTimer === null) {
      this.gcTimer = setInterval(() => {
        void this.runGc()
      }, this.opts.gcIntervalMs)
      this.gcTimer.unref?.()
    }
  }

  /** Clear every timer. Safe to call repeatedly; idempotent after stop. */
  stop(): void {
    this.stopped = true
    if (this.vacuumTimer) clearInterval(this.vacuumTimer)
    if (this.freezeTimer) clearInterval(this.freezeTimer)
    if (this.gcTimer) clearInterval(this.gcTimer)
    this.vacuumTimer = null
    this.freezeTimer = null
    this.gcTimer = null
  }

  /**
   * Hibernate hook: a run-once freeze check (+ vacuum-freeze when past the
   * threshold) and an opportunistic GC when GC scheduling is enabled — so a
   * scale-to-zero database still gets its frozen horizon and storage tended.
   * Best-effort and quick; the hibernate path awaits it.
   */
  async onHibernate(): Promise<void> {
    if (this.opts.freezeMaxAge > 0) await this.maybeFreeze()
    if (this.opts.gcIntervalMs > 0) await this.runGc()
  }

  /**
   * Force a real vacuum now (TEST HOOK). Unlike the background tick this
   * ignores the interval guard and does not silently skip when a background
   * tick holds `busy` — it waits the in-flight tick out (bounded), then runs
   * a genuine vacuum. A "run now" hook that no-ops under a fast auto-interval
   * (whose slow-under-load vacuum keeps `busy` set) is useless for tests.
   */
  async runVacuumNow(): Promise<void> {
    for (let i = 0; this.busy && i < 2000; i++) {
      await new Promise((r) => setTimeout(r, 5))
    }
    if (this.stopped || this.runtime.state !== 'active') return
    this.busy = true
    try {
      await this.maybeVacuum({ force: true })
    } finally {
      this.busy = false
    }
  }

  /** Force a single freeze check now (TEST HOOK). */
  async runFreezeCheckNow(): Promise<void> {
    await this.maybeFreeze()
  }

  private async tick(kind: 'vacuum' | 'freeze'): Promise<void> {
    if (this.stopped || this.busy || this.runtime.state !== 'active') return
    this.busy = true
    try {
      if (kind === 'vacuum') await this.maybeVacuum()
      if (this.opts.freezeMaxAge > 0) await this.maybeFreeze()
    } catch (err) {
      this.log(`maintenance tick failed: ${msg(err)}`)
    } finally {
      this.busy = false
    }
  }

  /**
   * Run `VACUUM (ANALYZE)` unless this host ran one within the interval. The
   * commit rides the normal CAS path (an ordinary W frame), so no separate
   * marker is needed — a duplicate vacuum across hosts is harmless.
   */
  private async maybeVacuum({
    force = false,
  }: { force?: boolean } = {}): Promise<void> {
    const now = Date.now()
    if (!force && now - this.lastVacuumAt < this.opts.vacuumIntervalMs) return
    this.lastVacuumAt = now
    const session = this.runtime.connectSession()
    try {
      // VACUUM cannot run inside a transaction block; a lone simple statement
      // through the ordinary session executor commits via the CAS sequencer.
      await session.exec('vacuum (analyze)')
      this.stats.vacuums++
    } finally {
      await session.close().catch(() => undefined)
    }
  }

  /**
   * Poll `age(datfrozenxid)`; when past `freezeMaxAge`, run `VACUUM (FREEZE)`.
   * The threshold comparison honours the `_forceAgeForTest` override (real
   * xids cannot realistically be aged in a test — the override lets a test
   * exercise the freeze branch against a mocked age).
   */
  private async maybeFreeze(): Promise<void> {
    if (this.opts.freezeMaxAge <= 0) return
    if (this.runtime.state !== 'active') return
    const session = this.runtime.connectSession()
    try {
      const r = await session.exec(
        `select age(datfrozenxid)::int8 as age
           from pg_database where datname = current_catalog`,
      )
      const realAge = Number(r.rows[0]?.age ?? 0)
      this.stats.lastFrozenAge = realAge
      const effective = this._forceAgeForTest ?? realAge
      if (effective < this.opts.freezeMaxAge) return
      await session.exec('vacuum (freeze)')
      this.stats.freezes++
      this.log(
        `datfrozenxid age ${effective} past freezeMaxAge ` +
          `${this.opts.freezeMaxAge}: ran VACUUM (FREEZE)`,
      )
    } finally {
      await session.close().catch(() => undefined)
    }
  }

  private async runGc(): Promise<void> {
    if (this.stopped || this.runtime.state !== 'active') return
    try {
      await this.runtime.gateway.runGc(this.runtime.databaseId)
      this.stats.gcRuns++
    } catch (err) {
      this.log(`GC run failed: ${msg(err)}`)
    }
  }

  private log(text: string): void {
    console.log(
      `[pglite-cell-server] janitor db ${this.runtime.databaseId}: ${text}`,
    )
  }
}

function msg(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}
