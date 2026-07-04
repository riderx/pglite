// Typed errors shared across the commit engine. Every failure mode a caller
// is expected to branch on gets its own class (instanceof-friendly), so the
// host/proxy layers above can map them onto the §3.7 contract without string
// matching.

/**
 * A frame or frame group violated the stream protocol: broken slice
 * contiguity, a sliceHash mismatch, an `O` frame outside group zero, an
 * eraId mismatch, or a malformed control frame. The tailer that threw is no
 * longer trustworthy and must be rebuilt from a known-good boundary.
 */
export class ProtocolError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ProtocolError'
  }
}

/**
 * The capture-cursor invariant was violated: a slice submitted for commit
 * does not start at the tailer's current head LSN (M1_PLAN "The one
 * invariant that makes slices compose"). The caller must catch up + rebase
 * (recycle-with-materialize at M1) and re-execute.
 */
export class CaptureCursorError extends Error {
  constructor(
    public readonly sliceBaseLsn: bigint,
    public readonly headLsn: bigint,
  ) {
    super(
      `slice baseLsn ${sliceBaseLsn} != stream head LSN ${headLsn} — ` +
        `capture-cursor invariant violated; rebase and re-execute`,
    )
    this.name = 'CaptureCursorError'
  }
}

/**
 * The era stream is closed and rotation retries are exhausted: the committer
 * hopped the maximum number of eras for one commit and every landing target
 * was closed too. The caller should rebuild from a fresh tail.
 */
export class EraClosedError extends Error {
  constructor(public readonly nextOffset: string) {
    super(
      `era stream is closed (tail ${nextOffset}) and rotation retries are exhausted`,
    )
    this.name = 'EraClosedError'
  }
}

/**
 * The S→O era chain is broken: the next era's O frame does not mirror the
 * sealed era's terminal S (`O.prevEraId === S.eraId && O.eraId ===
 * S.nextEraId && O.baseLsn === S.finalLsn`), the next era does not open
 * with an O frame, or the chain's LSN continuity does not match the
 * tailer's head. The chain is corrupt — do not follow it.
 */
export class EraChainError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'EraChainError'
  }
}

/**
 * An era stream is closed WITHOUT a terminal S frame (§2.6 sealed-detection
 * rule): the era is wedged. Writers must stop; repair is the rotator's job
 * (design §6.1 step 0), not the tailer's.
 */
export class WedgedEraError extends Error {
  constructor(
    public readonly eraId: string,
    public readonly tailOffset: string,
  ) {
    super(
      `era ${eraId} is closed at ${tailOffset} without a terminal S frame — ` +
        `wedged; writers stop, repair belongs to the rotator`,
    )
    this.name = 'WedgedEraError'
  }
}

/**
 * The server rejected an append because a newer incarnation of this
 * producer has fenced us (Producer-Epoch too old). This committer must not
 * write again; a fresh Committer.create() picks a higher epoch.
 */
export class FencedError extends Error {
  constructor(
    public readonly usedEpoch: number,
    public readonly currentEpoch: number,
  ) {
    super(
      `producer epoch ${usedEpoch} is fenced (server has epoch ${currentEpoch}); ` +
        `a newer incarnation exists`,
    )
    this.name = 'FencedError'
  }
}

/** The server reported a producer sequence gap — internal invariant broken. */
export class ProducerGapError extends Error {
  constructor(
    public readonly expectedSeq: number,
    public readonly receivedSeq: number,
  ) {
    super(
      `producer seq gap: server expected ${expectedSeq}, we sent ${receivedSeq}`,
    )
    this.name = 'ProducerGapError'
  }
}

/** One or more §9 configuration pins do not hold on an opened cell. */
export class ConfigPinError extends Error {
  constructor(
    public readonly mismatches: {
      name: string
      expected: string
      actual: string
    }[],
  ) {
    super(
      'config pins violated: ' +
        mismatches
          .map((m) => `${m.name}=${m.actual} (expected ${m.expected})`)
          .join(', '),
    )
    this.name = 'ConfigPinError'
  }
}

/**
 * The zero-boot-WAL invariant failed: a plain open of a cleanly-closed
 * datadir must leave the insert LSN exactly at the expected stream head
 * (M0 finding 1, requires --no-data-checksums at initdb).
 */
export class ZeroBootWalError extends Error {
  constructor(
    public readonly expectedLsn: bigint,
    public readonly actualLsn: bigint,
  ) {
    super(
      `zero-boot-WAL invariant failed: insert LSN after plain open is ` +
        `${actualLsn} but expected head is ${expectedLsn} ` +
        `(boot wrote ${actualLsn - expectedLsn} bytes of WAL)`,
    )
    this.name = 'ZeroBootWalError'
  }
}

/** Slices handed to materializeAtHead do not chain end-to-base. */
export class SliceChainError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'SliceChainError'
  }
}
