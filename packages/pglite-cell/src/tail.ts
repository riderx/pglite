// Era tail reader: follows an era CHAIN through the position-checked reader
// (W4), verifying slice contiguity (the capture-cursor invariant, checked on
// the read side) and slice hashes, and accumulating ordered W slices plus
// control-frame state (K checkpoints, L leases).
//
// M2: the tailer is multi-era. A terminal S frame triggers an era hop — the
// next era's O frame is fetched at the initial token and verified against
// the S (the O/S mirror), then tailing continues in the new era with LSN
// contiguity carried across the boundary via `O.baseLsn`. A closed era
// WITHOUT a terminal S is wedged (§2.6) and raises `WedgedEraError`.
//
// Fork tolerance (§2.5): a forked era stream's copied prefix carries the
// PARENT's eraIds. Frame-level eraId equality is therefore advisory for
// W/K/L and reserved control frames — foreign eraIds are tolerated and
// counted (`foreignEraFrames`) while position (W4) and W LSN chaining stay
// strict. The terminal S/O chain checks remain strict on their own fields.

import { createHash } from 'node:crypto'
import type { DsStreamClient } from './stream-client'
import { INITIAL_OFFSET_TOKEN, PositionCheckedReader } from './frames'
import type {
  AppendGroup,
  Frame,
  GenericFrame,
  GFrameHeader,
  KFrameHeader,
  LFrameHeader,
  NFrameHeader,
  OFrameHeader,
  SFrameHeader,
} from './frames'
import { parseLsn } from './lsn'
import { EraChainError, ProtocolError, WedgedEraError } from './errors'

/** One verified W slice pulled off the tail, LSNs parsed to bigints. */
export interface TailSlice {
  baseLsn: bigint
  endLsn: bigint
  kind: 'commit' | 'sync' | 'floors'
  commitId: string
  bytes: Uint8Array
}

/** The era the tailer is currently reading (moves forward on each hop). */
export interface CurrentEra {
  id: string
  ordinal: number
  /** Stream path of the era, relative to the client's base URL. */
  path: string
}

export interface EraTailerOpts {
  /** Stream path of the era, relative to the client's base URL. */
  path: string
  eraId: string
  /** Era ordinal (for W3 CAS tokens minted by the committer). */
  ordinal: number
  /** Boundary token to start reading from (the era base for full tails). */
  baseOffset: string
  /** WAL head LSN at `baseOffset` (the era's snapEnd for full tails). */
  baseLsn: bigint
}

function sha256Hex(bytes: Uint8Array): string {
  return 'sha256:' + createHash('sha256').update(bytes).digest('hex')
}

/**
 * Reads an era chain from a known (offset, LSN) pair and maintains the
 * tailer view: `head` (offset + LSN), ordered verified `slices`, the last
 * seen K frame and L leases. Frames are position-checked (W4) and W slices
 * are contiguity- and hash-verified before acceptance; any violation throws
 * a `ProtocolError` and the tailer must be rebuilt.
 */
export class EraTailer {
  private era: CurrentEra
  private reader: PositionCheckedReader
  private headOffset: string
  private headLsn: bigint
  private _closed = false
  /** Catch-up serialization chain (M4 hardening): runs never overlap, and
   *  every caller gets a run that STARTS at or after its call (freshness —
   *  sharing an in-flight run would let a CAS loser catch up to a tail
   *  older than the append it just lost to). */
  private catchUpChain: Promise<unknown> = Promise.resolve()
  /** Terminal S seen in the current era, pending an era hop. */
  private pendingSeal: SFrameHeader | null = null

  /** Ordered, verified W slices (oldest first). */
  readonly slices: TailSlice[] = []
  /** The last K (checkpoint) frame header seen, if any. */
  latestCheckpoint: KFrameHeader | null = null
  /** The last L (lease) frame header seen, per lease kind. */
  readonly leases: Partial<Record<'head' | 'gc-pin', LFrameHeader>> = {}
  /**
   * Wall-clock (Date.now) at which the corresponding `leases[kind]` frame
   * was DISPATCHED by this tailer (M4 lease-aware backoff: L headers carry
   * no timestamp — freshness is judged from local observation time).
   */
  readonly leaseSeenAt: Partial<Record<'head' | 'gc-pin', number>> = {}
  /** The era-open frame of the CURRENT era, if its origin was read. */
  eraOpen: OFrameHeader | null = null
  /** Raw reserved control frames (F/X) — recorded, not interpreted. */
  readonly controlFrames: GenericFrame[] = []
  /**
   * Every G (sequence grant) frame replayed off the era chain, in stream
   * order (M4, §5.3). Survives era hops — the map is never cleared; a
   * fresh era's zero-width re-assert grants seed joiners who tail only
   * the new era.
   */
  readonly grants: GFrameHeader[] = []
  /** Per-sequence grant high-water: max(end) over every replayed G. */
  private readonly grantHw = new Map<string, bigint>()
  /** Ordered N (notification) frame headers seen, with their group offset. */
  readonly notifications: { header: NFrameHeader; offset: string }[] = []
  /**
   * Subscriber hook for N frames (M3, §10.2): invoked for every N frame
   * as it is dispatched — via catch-up, live poll, or a local advance —
   * in stream order, which is commit order, globally.
   */
  onNotificationFrame: ((header: NFrameHeader, offset: string) => void) | null =
    null
  /**
   * Frames whose header eraId differs from the current era's — the fork
   * copied-prefix case (§2.5). Tolerated (position + LSN chain stay strict)
   * and counted here.
   */
  foreignEraFrames = 0

  constructor(
    private readonly client: DsStreamClient,
    opts: EraTailerOpts,
  ) {
    this.era = { id: opts.eraId, ordinal: opts.ordinal, path: opts.path }
    this.reader = new PositionCheckedReader(opts.baseOffset)
    this.headOffset = opts.baseOffset
    this.headLsn = opts.baseLsn
  }

  /** The era the tailer is currently reading (advances on era hops). */
  get currentEra(): CurrentEra {
    return { ...this.era }
  }

  /** Stream path of the current era (relative to the client base). */
  get path(): string {
    return this.era.path
  }

  /** Id of the current era. */
  get eraId(): string {
    return this.era.id
  }

  /** Ordinal of the current era (for W3 CAS tokens). */
  get ordinal(): number {
    return this.era.ordinal
  }

  /** The tailer's current head: stream offset + WAL LSN, tracked as a pair. */
  get head(): { offset: string; lsn: bigint } {
    return { offset: this.headOffset, lsn: this.headLsn }
  }

  /** True once the current era stream reported closed (reset on era hops). */
  get closed(): boolean {
    return this._closed
  }

  /**
   * Catch-up read loop: GET from the current boundary until the server
   * reports up-to-date, feeding the position-checked reader and dispatching
   * every validated append group. Follows era hops: a terminal S frame
   * chains into the next era via its O frame (O/S mirror verified — throws
   * `EraChainError` on violation); a closed era without a terminal S throws
   * `WedgedEraError`. Returns the number of new W slices.
   *
   * Concurrency (M4 hardening): catch-ups are SERIALIZED — concurrent
   * calls queue behind the in-flight run (each caller's run starts at or
   * after its call) — and a response fetched against a boundary that
   * `advanceLocal` moved mid-read is DISCARDED and re-read (the committer
   * advances the tailer locally after its own appends; feeding a
   * stale-boundary response would poison the reader).
   */
  catchUp(): Promise<number> {
    const p = this.catchUpChain.then(() => this.catchUpInner())
    this.catchUpChain = p.then(
      () => undefined,
      () => undefined,
    )
    return p
  }

  private async catchUpInner(): Promise<number> {
    const before = this.slices.length
    for (;;) {
      const reader = this.reader
      const res = await this.client.read(this.era.path, {
        offset: reader.boundary,
      })
      if (this.reader !== reader) continue // advanceLocal moved us: re-read
      this.ingest(res.bytes, res.nextOffset)
      const atTail = res.upToDate || res.bytes.length === 0
      if (this.pendingSeal) {
        // Sealed era: hop, then keep reading from the new era's boundary.
        await this.hop(this.pendingSeal)
        continue
      }
      if (res.closed && atTail) {
        // Closed without a terminal S ⇒ wedged (§2.6 sealed-detection rule).
        this._closed = true
        throw new WedgedEraError(this.era.id, this.reader.boundary)
      }
      if (atTail) break
    }
    return this.slices.length - before
  }

  /**
   * One live read from the current boundary (`long-poll`): waits up to the
   * server's long-poll window for new data. Handles the era hop: a poll
   * that returns the terminal S (closed stream) chains into the next era.
   * Returns the number of new W slices dispatched (0 on a 204 timeout).
   */
  async pollOnce(opts: { live: 'long-poll' }): Promise<number> {
    const before = this.slices.length
    const reader = this.reader
    const res = await this.client.read(this.era.path, {
      offset: reader.boundary,
      live: opts.live,
    })
    if (res.status === 204) return 0 // long-poll timeout, nothing new
    if (this.reader !== reader) return 0 // advanceLocal moved us: stale
    this.ingest(res.bytes, res.nextOffset)
    let hopped = false
    while (this.pendingSeal) {
      await this.hop(this.pendingSeal)
      hopped = true
    }
    if (!hopped && res.closed && res.upToDate) {
      // Closed without a terminal S ⇒ wedged (§2.6 sealed-detection rule).
      this._closed = true
      throw new WedgedEraError(this.era.id, this.reader.boundary)
    }
    return this.slices.length - before
  }

  /** Verified slices whose baseLsn is at or past `lsn` (oldest first). */
  slicesSince(lsn: bigint): TailSlice[] {
    return this.slices.filter((s) => s.baseLsn >= lsn)
  }

  /** Every replayed grant for one sequence, in stream order (M4, §5.3). */
  grantsFor(seqName: string): GFrameHeader[] {
    return this.grants.filter((g) => g.seqName === seqName)
  }

  /** The grant high-water for one sequence: max(end), 0n before any G. */
  grantHighWater(seqName: string): bigint {
    return this.grantHw.get(seqName) ?? 0n
  }

  /** Snapshot of every sequence's grant high-water (rotation re-assert). */
  grantHighWaters(): Map<string, bigint> {
    return new Map(this.grantHw)
  }

  /**
   * Advance the tailer past an append this host just landed itself, without
   * re-downloading its own bytes: dispatches the frames through the same
   * verification path (so the tailer view stays consistent) and moves the
   * read boundary to the append's `nextOffset`.
   */
  advanceLocal(frames: Frame[], nextOffset: string): void {
    this.dispatchGroup({ offset: this.headOffset, frames })
    this.reader = new PositionCheckedReader(nextOffset)
    this.headOffset = nextOffset
  }

  /**
   * Follow the S→O chain into the next era: read the next era stream at
   * the INITIAL token through a fresh PositionCheckedReader, require its
   * first frame to be an O that mirrors the seal (`O.prevEraId === S.eraId
   * && O.eraId === S.nextEraId && O.baseLsn === S.finalLsn`, and
   * `O.baseLsn` equal to the tailer's head LSN), then switch the tailer to
   * the new era and dispatch everything already appended past the O.
   */
  private async hop(seal: SFrameHeader): Promise<void> {
    const path = seal.nextEraUrl
    const res = await this.client.read(path, { offset: INITIAL_OFFSET_TOKEN })
    const reader = new PositionCheckedReader(INITIAL_OFFSET_TOKEN)
    const groups = reader.feed(res.bytes)
    const first = groups.next()
    const oFrame = first.done ? null : first.value.frames[0]
    if (!oFrame || oFrame.type !== 'O') {
      throw new EraChainError(
        `next era ${seal.nextEraId} at ${path} does not open with an O frame`,
      )
    }
    const o = oFrame.header
    if (
      o.prevEraId !== seal.eraId ||
      o.eraId !== seal.nextEraId ||
      o.baseLsn !== seal.finalLsn
    ) {
      throw new EraChainError(
        `O/S mirror violation hopping ${seal.eraId} → ${seal.nextEraId}: ` +
          `O{prevEraId:${o.prevEraId}, eraId:${o.eraId}, baseLsn:${o.baseLsn}} ` +
          `vs S{eraId:${seal.eraId}, nextEraId:${seal.nextEraId}, ` +
          `finalLsn:${seal.finalLsn}}`,
      )
    }
    if (parseLsn(o.baseLsn) !== this.headLsn) {
      throw new EraChainError(
        `era hop LSN discontinuity: O.baseLsn ${o.baseLsn} but tailer head ` +
          `LSN is ${this.headLsn}`,
      )
    }

    // Switch eras, then replay the initial read through the normal dispatch
    // path (first group is the O; anything already appended follows).
    this.pendingSeal = null
    this._closed = false
    this.era = { id: o.eraId, ordinal: o.ordinal, path }
    this.reader = reader
    this.dispatchGroup(first.value)
    for (const group of groups) this.dispatchGroup(group)
    if (res.nextOffset !== '') this.reader.expectBoundary(res.nextOffset)
    this.headOffset = this.reader.boundary

    // The new era may itself be closed already: sealed (its own terminal S
    // set pendingSeal — the caller's loop hops again) or wedged.
    if (res.closed && !this.pendingSeal) {
      this._closed = true
      throw new WedgedEraError(this.era.id, this.reader.boundary)
    }
  }

  private ingest(bytes: Uint8Array, serverNextOffset: string): void {
    if (bytes.length > 0) {
      for (const group of this.reader.feed(bytes)) {
        this.dispatchGroup(group)
      }
    }
    if (serverNextOffset !== '') this.reader.expectBoundary(serverNextOffset)
    this.headOffset = this.reader.boundary
  }

  private dispatchGroup(group: AppendGroup): void {
    for (const frame of group.frames) {
      if (frame.type === 'S') {
        // Terminal seal: strict on its own fields (never fork-relaxed).
        if (frame.header.eraId !== this.era.id) {
          throw new EraChainError(
            `terminal S at ${group.offset} carries eraId ` +
              `${frame.header.eraId}, expected ${this.era.id}`,
          )
        }
        this.pendingSeal = frame.header
        continue
      }
      if (frame.header.eraId !== this.era.id) {
        // Fork copied-prefix tolerance (§2.5): count, do not reject —
        // position (W4) and W LSN chaining below remain strict.
        this.foreignEraFrames += 1
      }
      switch (frame.type) {
        case 'W': {
          const baseLsn = parseLsn(frame.header.baseLsn)
          const endLsn = parseLsn(frame.header.endLsn)
          if (baseLsn !== this.headLsn) {
            throw new ProtocolError(
              `W slice ${frame.header.commitId} at ${group.offset} has ` +
                `baseLsn ${frame.header.baseLsn} but the stream head LSN is ` +
                `${this.headLsn} — contiguity broken`,
            )
          }
          const hash = sha256Hex(frame.wal)
          if (hash !== frame.header.sliceHash) {
            throw new ProtocolError(
              `W slice ${frame.header.commitId} sliceHash mismatch: header ` +
                `${frame.header.sliceHash}, computed ${hash}`,
            )
          }
          this.slices.push({
            baseLsn,
            endLsn,
            kind: frame.header.kind,
            commitId: frame.header.commitId,
            bytes: frame.wal,
          })
          this.headLsn = endLsn
          break
        }
        case 'K':
          this.latestCheckpoint = frame.header
          break
        case 'L':
          this.leases[frame.header.kind] = frame.header
          this.leaseSeenAt[frame.header.kind] = Date.now()
          break
        case 'G': {
          this.grants.push(frame.header)
          const end = BigInt(frame.header.end)
          const hw = this.grantHw.get(frame.header.seqName) ?? 0n
          if (end > hw) this.grantHw.set(frame.header.seqName, end)
          break
        }
        case 'O':
          if (group.offset !== INITIAL_OFFSET_TOKEN) {
            throw new ProtocolError(
              `O frame at ${group.offset} — era-open frames are only valid ` +
                `at group zero (${INITIAL_OFFSET_TOKEN})`,
            )
          }
          this.eraOpen = frame.header
          break
        case 'N':
          this.notifications.push({
            header: frame.header,
            offset: group.offset,
          })
          this.onNotificationFrame?.(frame.header, group.offset)
          break
        case '0':
          break // recovery fence: deliberate no-op
        default:
          // F/X: reserved — record raw, do not interpret.
          this.controlFrames.push(frame)
      }
    }
  }
}
