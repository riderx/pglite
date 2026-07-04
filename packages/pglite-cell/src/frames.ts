// Frame codec v1 (M1_PLAN.md "Frame codec v1") plus the offset-token algebra
// and the position-checked reader (W4).
//
// Wire layout:
//   frame  := type(1 byte) | payloadLen(u32 BE) | payload
//   'W'    := hdrLen(u16 BE) | headerJson(utf8) | walBytes
//   others := headerJson(utf8)
//
// One CAS append = one or more whole frames, all carrying the same
// `expectedOffset` (== the tail token the writer observed). Byte-mode reads
// return concatenated payloads with the server's per-message boundaries
// stripped, but boundaries are reconstructible from the frame lengths, so the
// reader re-derives them and validates that each append group sat exactly at
// the position it claims (§2.6 metadata-rollback defense).

// ---------------------------------------------------------------------------
// Frame types & headers
// ---------------------------------------------------------------------------

export type FrameType =
  | 'W'
  | 'O'
  | 'S'
  | 'K'
  | 'L'
  | 'G'
  | 'N'
  | 'F'
  | 'X'
  | '0'

export interface BaseHeader {
  v: 1
  eraId: string
  expectedOffset: string
}

export interface WFrameHeader extends BaseHeader {
  commitId: string
  kind: 'commit' | 'sync' | 'floors'
  baseLsn: string
  endLsn: string
  sliceHash: string
}

export interface WFrame {
  type: 'W'
  header: WFrameHeader
  wal: Uint8Array
}

/**
 * Era-open frame. Rides in the creating PUT body. `expectedOffset` is the
 * initial token (group zero) — the O header intentionally omits it in the
 * plan's field list, but every header carries `{v, eraId, expectedOffset}`,
 * so we keep it here for the reader's grouping to work uniformly.
 */
export interface OFrameHeader extends BaseHeader {
  ordinal: number
  prevEraId: string | null
  prevEraUrl: string | null
  baseOffset: string
  baseLsn: string
  snapEnd: string
  checkpointRef: string
}

export interface OFrame {
  type: 'O'
  header: OFrameHeader
}

export interface KFrameHeader extends BaseHeader {
  lsn: string
  snapEnd: string
  checkpointRef: string
  sha256: string
}

export interface KFrame {
  type: 'K'
  header: KFrameHeader
}

export interface LFrameHeader extends BaseHeader {
  kind: 'head' | 'gc-pin'
  holder: string
  epoch: number
  ttlMs: number
  base?: { offset: string; lsn: string }
}

export interface LFrame {
  type: 'L'
  header: LFrameHeader
}

export interface FenceFrame {
  type: '0'
  header: BaseHeader
}

/**
 * Era-seal frame (M2 rotation, §6.1 step 5). Rides the sealing
 * `appendAndClose` CAS append; presence of a valid terminal S means sealed
 * regardless of the stream's closed bit (§2.6). `nextEraUrl` is a stream
 * PATH relative to the same client base (leading slash, M1 convention).
 * `ordinal` is the sealed era's ordinal (the next era's is `ordinal + 1`,
 * mirrored authoritatively in the next era's O frame).
 */
export interface SFrameHeader extends BaseHeader {
  ordinal: number
  finalOffset: string
  finalLsn: string
  nextEraUrl: string
  nextEraId: string
}

export interface SFrame {
  type: 'S'
  header: SFrameHeader
}

/**
 * Notification frame (M3, §10.2): one NOTIFY harvested at commit time,
 * riding the SAME CAS append as its commit's W frame — notifications exist
 * in the stream iff the commit does, atomically. One frame per
 * notification, ordered. `commitLsn` is the commit's endLsn (formatted).
 */
export interface NFrameHeader extends BaseHeader {
  commitId: string
  channel: string
  payload: string
  commitLsn: string
}

export interface NFrame {
  type: 'N'
  header: NFrameHeader
}

/**
 * Reserved control frames — codec support + tests only, not produced yet
 * (G: M4, F: M2b, X: M5). Their payload is an opaque JSON header
 * extending the common base. (N graduated to a typed frame at M3.)
 */
export interface GenericFrame {
  type: 'G' | 'F' | 'X'
  header: BaseHeader & Record<string, unknown>
}

export type Frame =
  | WFrame
  | OFrame
  | SFrame
  | KFrame
  | LFrame
  | FenceFrame
  | NFrame
  | GenericFrame

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

const te = new TextEncoder()
const td = new TextDecoder()

function concat(parts: Uint8Array[]): Uint8Array {
  let total = 0
  for (const p of parts) total += p.length
  const out = new Uint8Array(total)
  let off = 0
  for (const p of parts) {
    out.set(p, off)
    off += p.length
  }
  return out
}

function u32be(n: number): Uint8Array {
  const b = new Uint8Array(4)
  new DataView(b.buffer).setUint32(0, n, false)
  return b
}

function u16be(n: number): Uint8Array {
  const b = new Uint8Array(2)
  new DataView(b.buffer).setUint16(0, n, false)
  return b
}

/** Encode a single frame to its `type | payloadLen | payload` byte form. */
export function encodeFrame(frame: Frame): Uint8Array {
  const typeByte = te.encode(frame.type)
  let payload: Uint8Array
  if (frame.type === 'W') {
    const headerJson = te.encode(JSON.stringify(frame.header))
    payload = concat([u16be(headerJson.length), headerJson, frame.wal])
  } else {
    payload = te.encode(JSON.stringify(frame.header))
  }
  return concat([typeByte, u32be(payload.length), payload])
}

/**
 * Encode a batch of frames into one append body. Every frame of a single
 * append MUST carry the same `expectedOffset`; this is asserted here.
 */
export function encodeAppend(frames: Frame[]): Uint8Array {
  if (frames.length === 0) throw new Error('encodeAppend: empty frame list')
  const expected = frames[0].header.expectedOffset
  for (const f of frames) {
    if (f.header.expectedOffset !== expected) {
      throw new Error(
        `encodeAppend: mixed expectedOffset in one append (${expected} vs ${f.header.expectedOffset})`,
      )
    }
  }
  return concat(frames.map(encodeFrame))
}

// ---------------------------------------------------------------------------
// Decoding (low level)
// ---------------------------------------------------------------------------

/**
 * Decode the frame beginning at `pos` in `buf`. Returns the frame and the
 * position just past it, or `null` if `buf` does not yet contain the whole
 * frame (partial — caller should buffer more bytes).
 */
export function decodeFrame(
  buf: Uint8Array,
  pos: number,
): { frame: Frame; next: number } | null {
  if (pos + 5 > buf.length) return null // need type(1) + payloadLen(4)
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
  const type = String.fromCharCode(buf[pos]) as FrameType
  const payloadLen = view.getUint32(pos + 1, false)
  const payloadStart = pos + 5
  const payloadEnd = payloadStart + payloadLen
  if (payloadEnd > buf.length) return null // partial payload

  let frame: Frame
  if (type === 'W') {
    if (payloadStart + 2 > payloadEnd)
      throw new Error('W frame: truncated hdrLen')
    const hdrLen = view.getUint16(payloadStart, false)
    const headerStart = payloadStart + 2
    const headerEnd = headerStart + hdrLen
    if (headerEnd > payloadEnd) throw new Error('W frame: truncated header')
    const header = JSON.parse(
      td.decode(buf.subarray(headerStart, headerEnd)),
    ) as WFrameHeader
    const wal = buf.slice(headerEnd, payloadEnd)
    frame = { type: 'W', header, wal }
  } else {
    const header = JSON.parse(td.decode(buf.subarray(payloadStart, payloadEnd)))
    frame = { type, header } as Frame
  }
  return { frame, next: payloadEnd }
}

// ---------------------------------------------------------------------------
// Offset tokens
// ---------------------------------------------------------------------------

/** The stream's initial read position (empty stream). */
export const INITIAL_OFFSET_TOKEN = '0000000000000000_0000000000000000'

/**
 * Per-append server overhead in bytes. The Durable Streams server advances
 * byteOffset by `5 + payloadLength` for each append (4-byte length prefix +
 * 1-byte record separator — store.ts FRAME_OVERHEAD). Verified live.
 */
export const APPEND_OVERHEAD = 5

/** Parse an opaque offset token into its two fixed-width halves. */
export function parseOffsetToken(tok: string): {
  readSeq: bigint
  byteOffset: bigint
} {
  const [r, b] = tok.split('_')
  return { readSeq: BigInt(r), byteOffset: BigInt(b) }
}

/** Render an offset token from its halves (16-digit zero-padded each). */
export function formatOffsetToken(readSeq: bigint, byteOffset: bigint): string {
  return `${readSeq.toString().padStart(16, '0')}_${byteOffset
    .toString()
    .padStart(16, '0')}`
}

/**
 * The boundary token after an append of `appendPayloadLen` bytes made at
 * `tok`. NOTE: the real Durable Streams server (0.3.7) keeps readSeq FIXED and
 * only advances byteOffset by `5 + payloadLen` — it does NOT increment readSeq
 * (verified live; contradicts the M1_PLAN "(R+1)_(B+5+P)" prose). We match the
 * server so computed boundaries equal the server's Stream-Next-Offset.
 */
export function nextBoundary(tok: string, appendPayloadLen: number): string {
  const { readSeq, byteOffset } = parseOffsetToken(tok)
  return formatOffsetToken(
    readSeq,
    byteOffset + BigInt(APPEND_OVERHEAD) + BigInt(appendPayloadLen),
  )
}

/**
 * CAS token (W3): `pad10(eraOrdinal) + "," + offsetToken`. Fixed-width so the
 * server's byte-wise lexicographic `<=` comparison of Stream-Seq is monotone.
 */
export function casToken(eraOrdinal: number, offsetToken: string): string {
  return String(eraOrdinal).padStart(10, '0') + ',' + offsetToken
}

// ---------------------------------------------------------------------------
// Position-checked reader (W4)
// ---------------------------------------------------------------------------

/** Thrown when an append group does not sit at the position it claims. */
export class FramePositionError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'FramePositionError'
  }
}

/** An append group: the frames of one CAS append plus its start offset. */
export interface AppendGroup {
  offset: string
  frames: Frame[]
}

/**
 * Reconstructs append boundaries from a concatenated byte-mode read and
 * validates each append group's position (W4). Constructed at a known boundary
 * token; `feed()` buffers bytes and yields validated groups. On the first
 * mis-positioned group the reader poisons itself and throws — callers stop.
 */
export class PositionCheckedReader {
  private buf: Uint8Array = new Uint8Array(0)
  private currentBoundary: string
  private _poisoned = false

  constructor(startBoundary: string = INITIAL_OFFSET_TOKEN) {
    this.currentBoundary = startBoundary
  }

  /** The next boundary token the reader expects an append to start at. */
  get boundary(): string {
    return this.currentBoundary
  }

  /** True once a mis-positioned group has been seen; no further reads. */
  get poisoned(): boolean {
    return this._poisoned
  }

  /**
   * Buffer `bytes` and yield every complete, validated append group. A group
   * is the maximal run of consecutive frames sharing one `expectedOffset`; it
   * is valid iff that value equals the reader's current boundary. The boundary
   * then advances by the group's total encoded byte length via `nextBoundary`.
   *
   * CONTRACT: each `feed()` must receive a whole byte-mode response body (the
   * server always returns whole appends), or at minimum never split a
   * multi-frame append exactly at an interior frame boundary — a run that
   * ends exactly at the buffer end is treated as complete, so such a split
   * would emit a partial group and poison the reader when the rest arrives.
   */
  *feed(bytes: Uint8Array): Generator<AppendGroup> {
    if (this._poisoned) {
      throw new FramePositionError('reader is poisoned; refusing to read')
    }
    this.buf = this.buf.length === 0 ? bytes : concat([this.buf, bytes])

    let consumed = 0
    for (;;) {
      const group = this.tryNextGroup(consumed)
      if (!group) break // partial / no more complete groups
      // validate position
      const claimed = group.frames[0].header.expectedOffset
      if (claimed !== this.currentBoundary) {
        this._poisoned = true
        throw new FramePositionError(
          `frame group at buffer pos ${consumed} claims expectedOffset ${claimed} but reader is at ${this.currentBoundary}`,
        )
      }
      const startOffset = this.currentBoundary
      const encodedLen = group.end - consumed
      this.currentBoundary = nextBoundary(this.currentBoundary, encodedLen)
      consumed = group.end
      yield { offset: startOffset, frames: group.frames }
    }
    // retain the unconsumed tail (partial frame carried to next feed)
    this.buf = this.buf.slice(consumed)
  }

  /**
   * Parse the maximal run of consecutive frames sharing one expectedOffset
   * starting at `start`. Returns null if there is not yet a complete group
   * (either no complete frame, or the run has not been terminated by a frame
   * with a different expectedOffset / end of buffer with a possible partial).
   *
   * We only return a group once we can prove it is complete: either the next
   * frame after the run decodes with a different expectedOffset (proving the
   * run ended), or... we cannot prove completeness from a lone trailing run
   * because the next feed might extend it. To keep grouping deterministic we
   * treat each individual frame as decodable and greedily extend while the
   * NEXT frame is fully present; if the next frame is only partially present
   * we still emit the run we have as long as at least one frame is complete
   * AND the buffered bytes reach exactly the run's end (no dangling partial of
   * a same-offset continuation). See note below.
   */
  private tryNextGroup(start: number): { frames: Frame[]; end: number } | null {
    const first = decodeFrame(this.buf, start)
    if (!first) return null
    const expected = first.frame.header.expectedOffset
    const frames: Frame[] = [first.frame]
    let end = first.next

    for (;;) {
      const nxt = decodeFrame(this.buf, end)
      if (!nxt) {
        // Not enough bytes for another whole frame. If we are exactly at the
        // buffer end, the run is complete (nothing dangling). Otherwise a
        // partial frame follows — but it might belong to THIS group (same
        // expectedOffset). We cannot yet tell, so we must wait for more bytes.
        if (end === this.buf.length) return { frames, end }
        return null
      }
      if (nxt.frame.header.expectedOffset !== expected) {
        // The run is terminated by a different-offset frame — group complete.
        return { frames, end }
      }
      frames.push(nxt.frame)
      end = nxt.next
    }
  }

  /**
   * Cross-check the reader's current boundary against the server's reported
   * `Stream-Next-Offset` after consuming a response. Mismatch ⇒ poison+throw.
   */
  expectBoundary(serverNextOffset: string): void {
    if (this.currentBoundary !== serverNextOffset) {
      this._poisoned = true
      throw new FramePositionError(
        `reader boundary ${this.currentBoundary} != server next offset ${serverNextOffset}`,
      )
    }
  }
}
