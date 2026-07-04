// Era tail reader: follows one era stream through the position-checked
// reader (W4), verifying slice contiguity (the capture-cursor invariant,
// checked on the read side) and slice hashes, and accumulating ordered W
// slices plus control-frame state (K checkpoints, L leases).

import { createHash } from 'node:crypto'
import type { DsStreamClient } from './stream-client'
import { INITIAL_OFFSET_TOKEN, PositionCheckedReader } from './frames'
import type {
  AppendGroup,
  Frame,
  GenericFrame,
  KFrameHeader,
  LFrameHeader,
  OFrameHeader,
} from './frames'
import { parseLsn } from './lsn'
import { ProtocolError } from './errors'

/** One verified W slice pulled off the tail, LSNs parsed to bigints. */
export interface TailSlice {
  baseLsn: bigint
  endLsn: bigint
  kind: 'commit' | 'sync' | 'floors'
  commitId: string
  bytes: Uint8Array
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
 * Reads an era stream from a known (offset, LSN) pair and maintains the
 * tailer view: `head` (offset + LSN), ordered verified `slices`, the last
 * seen K frame and L leases. Frames are position-checked (W4) and W slices
 * are contiguity- and hash-verified before acceptance; any violation throws
 * a `ProtocolError` and the tailer must be rebuilt.
 */
export class EraTailer {
  readonly path: string
  readonly eraId: string
  readonly ordinal: number

  private reader: PositionCheckedReader
  private headOffset: string
  private headLsn: bigint
  private _closed = false

  /** Ordered, verified W slices (oldest first). */
  readonly slices: TailSlice[] = []
  /** The last K (checkpoint) frame header seen, if any. */
  latestCheckpoint: KFrameHeader | null = null
  /** The last L (lease) frame header seen, per lease kind. */
  readonly leases: Partial<Record<'head' | 'gc-pin', LFrameHeader>> = {}
  /** The era-open frame, if the tail started at the stream origin. */
  eraOpen: OFrameHeader | null = null
  /** Raw reserved control frames (S/G/N/F/X) — recorded, not interpreted. */
  readonly controlFrames: GenericFrame[] = []

  constructor(
    private readonly client: DsStreamClient,
    opts: EraTailerOpts,
  ) {
    this.path = opts.path
    this.eraId = opts.eraId
    this.ordinal = opts.ordinal
    this.reader = new PositionCheckedReader(opts.baseOffset)
    this.headOffset = opts.baseOffset
    this.headLsn = opts.baseLsn
  }

  /** The tailer's current head: stream offset + WAL LSN, tracked as a pair. */
  get head(): { offset: string; lsn: bigint } {
    return { offset: this.headOffset, lsn: this.headLsn }
  }

  /** True once the era stream reported closed. */
  get closed(): boolean {
    return this._closed
  }

  /**
   * Catch-up read loop: GET from the current boundary until the server
   * reports up-to-date, feeding the position-checked reader and dispatching
   * every validated append group. Returns the number of new W slices.
   */
  async catchUp(): Promise<number> {
    const before = this.slices.length
    for (;;) {
      const res = await this.client.read(this.path, {
        offset: this.reader.boundary,
      })
      this.ingest(res.bytes, res.nextOffset)
      if (res.closed) this._closed = true
      if (res.upToDate || res.bytes.length === 0) break
    }
    return this.slices.length - before
  }

  /**
   * One live read from the current boundary (`long-poll`): waits up to the
   * server's long-poll window for new data. Returns the number of new W
   * slices dispatched (0 on a 204 timeout).
   */
  async pollOnce(opts: { live: 'long-poll' }): Promise<number> {
    const before = this.slices.length
    const res = await this.client.read(this.path, {
      offset: this.reader.boundary,
      live: opts.live,
    })
    if (res.status === 204) return 0 // long-poll timeout, nothing new
    this.ingest(res.bytes, res.nextOffset)
    if (res.closed) this._closed = true
    return this.slices.length - before
  }

  /** Verified slices whose baseLsn is at or past `lsn` (oldest first). */
  slicesSince(lsn: bigint): TailSlice[] {
    return this.slices.filter((s) => s.baseLsn >= lsn)
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
      if (frame.header.eraId !== this.eraId) {
        throw new ProtocolError(
          `frame at ${group.offset} carries eraId ${frame.header.eraId}, ` +
            `expected ${this.eraId}`,
        )
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
          break
        case 'O':
          if (group.offset !== INITIAL_OFFSET_TOKEN) {
            throw new ProtocolError(
              `O frame at ${group.offset} — era-open frames are only valid ` +
                `at group zero (${INITIAL_OFFSET_TOKEN})`,
            )
          }
          this.eraOpen = frame.header
          break
        case '0':
          break // recovery fence: deliberate no-op
        default:
          // S/G/N/F/X: reserved at M1 — record raw, do not interpret.
          this.controlFrames.push(frame)
      }
    }
  }
}
