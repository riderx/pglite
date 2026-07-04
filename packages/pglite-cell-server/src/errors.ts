// Typed errors for the §3.7 contract as surfaced by the host's programmatic
// session API. The M1d proxy maps these onto wire-protocol ErrorResponses
// (SerializationConflictError → a synthesized 40001; FatalSessionResetError →
// an ERROR naming the cause followed by connection termination).

/**
 * The single client-visible failure mode for every cross-cell conflict
 * (§4.0): a lost commit race that cannot be transparently re-executed. The
 * transaction was rolled back cleanly — nothing committed — and blind retry
 * of the whole transaction is the correct response. Carries SQLSTATE 40001
 * (`serialization_failure`) so ecosystem retry middleware applies unchanged.
 */
export class SerializationConflictError extends Error {
  /** SQLSTATE, for drivers/middleware that branch on `.code`. */
  readonly code = '40001'

  constructor(detail: string) {
    super(
      `could not serialize access due to concurrent update ` +
        `(SQLSTATE 40001): ${detail}; retry the transaction`,
    )
    this.name = 'SerializationConflictError'
  }
}

/**
 * A session holding unreplayable local state (temp tables, holdable cursors,
 * session advisory locks) lost a commit race. The forced recycle destroys
 * exactly the state re-execution would need, so continuing would silently
 * lie — the session is terminated instead (§3.3 fatal session reset; vanilla
 * precedent: crash recovery closes connections).
 */
export class FatalSessionResetError extends Error {
  constructor(public readonly taints: string[]) {
    super(
      `session reset: a conflicting commit landed and this session's local ` +
        `state (${taints.join(', ')}) cannot survive the recycle — ` +
        `reconnect and retry`,
    )
    this.name = 'FatalSessionResetError'
  }
}

/**
 * A tainted (pinned) session outlived its gc-pin TTL. Its pinned base can no
 * longer be protected from GC, so the session gets the same fatal reset a
 * backend crash would produce (§3.3 pinned-session rules).
 */
export class SessionPinnedExpiredError extends Error {
  constructor(public readonly pinTtlMs: number) {
    super(
      `session reset: this session was pinned at a stale base for longer ` +
        `than the gc-pin TTL (${pinTtlMs}ms) — reconnect and retry`,
    )
    this.name = 'SessionPinnedExpiredError'
  }
}

/**
 * A write attempted on a session in `pinned` freshness mode (§7). Pinned
 * sessions serve a fixed base and never advance; their transactions can
 * never publish. SQLSTATE 0A000 (`feature_not_supported`) — chosen over
 * 55000 because the write is categorically unsupported in this mode, not
 * a transient object-state problem; the message names the fix.
 */
export class PinnedWriteError extends Error {
  readonly code = '0A000'

  constructor() {
    super(
      `cannot execute a write in pinned freshness mode: this session ` +
        `serves a fixed historical base and never advances — ` +
        `SET pglite.freshness = 'session' to write`,
    )
    this.name = 'PinnedWriteError'
  }
}

/** Any use of a session that is closed, reset, or destroyed by hibernation. */
export class SessionClosedError extends Error {
  constructor(reason: string) {
    super(`session is closed: ${reason}`)
    this.name = 'SessionClosedError'
  }
}

/**
 * The write-attach (canonical-ensure) loop lost its publish race more times
 * than the retry budget allows — the stream is under heavy foreign append
 * pressure. Surfaced instead of livelocking.
 */
export class AdvanceRaceError extends Error {
  constructor(public readonly attempts: number) {
    super(
      `could not reach a canonical base position after ${attempts} ` +
        `attempts — every sync-slice publish lost its CAS race`,
    )
    this.name = 'AdvanceRaceError'
  }
}
