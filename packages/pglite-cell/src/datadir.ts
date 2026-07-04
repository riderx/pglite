// Datadir engine — a faithful port of the proven code in
// experiments/m0-wal-roundtrip/{harness,attach}.mjs, as pure functions over a
// datadir path. Node fs (sync APIs) throughout.
//
// pg_control layout (wasm32 build, MAXALIGN 8):
//   sysid(8) pg_control_version(4) catalog_version_no(4) state(4) pad(4)
//   time(8)                                                    -> offset 32
//   checkPoint(8)                                              -> offset 40
//   checkPointCopy (CheckPoint, 88 bytes): redo(8) tli(4) prevTli(4)
//     fullPageWrites(1)+pad + wal_level etc (to +24), nextXid(8) nextOid(4)
//     nextMulti(4) nextMultiOffset(4) ...

import {
  readFileSync,
  writeFileSync,
  readdirSync,
  existsSync,
  openSync,
  readSync,
  writeSync,
  closeSync,
} from 'node:fs'
import { join } from 'node:path'
import { crc32c, crcInit, crcFeed, crcFin } from './crc32c'
import { WAL_SEG_SIZE, WAL_BLOCK_SIZE, walSegmentName } from './lsn'

const STATE_OFF = 16 // sysid(8) + pg_control_version(4) + catalog_version_no(4)
const CKPT_OFF = 32
const COPY_OFF = 40
const COPY_LEN = 88

/** DBState values relevant to attach/detach. */
export const DB_STATE = {
  SHUTDOWNED: 1,
  IN_PRODUCTION: 6,
} as const

const XLP_LONG = 0x0002
const REC_LEN = 114 // 24 hdr + 2 short-data hdr + 88 CheckPoint
const REC_ALIGNED = 120

export interface ControlInfo {
  buf: Buffer
  crcOff: number
  sysid: bigint
  state: number
  checkPoint: bigint
  /** 88-byte checkPointCopy struct, detached copy. */
  copy: Buffer
  nextXid: bigint
  nextOid: number
  nextMulti: number
  nextMultiOffset: number
}

export interface WalFacts {
  magic: number
  sysid: bigint
  segSize: number
  blcksz: number
}

/**
 * Read and parse `global/pg_control`. The CRC offset is discovered by scan
 * (the last uint32 at aligned offset k whose value equals crc32c(buf[0..k))),
 * exactly as the harnesses do — layout-robust across builds.
 */
export function readControl(dir: string): ControlInfo {
  const buf = readFileSync(join(dir, 'global', 'pg_control'))
  let crcOff = -1
  for (let k = 200; k <= 600; k += 4) {
    if (crc32c(buf, k) === buf.readUInt32LE(k)) {
      crcOff = k
      break
    }
  }
  if (crcOff < 0)
    throw new Error(
      'pg_control CRC offset not found — layout assumption broken',
    )
  return {
    buf,
    crcOff,
    sysid: buf.readBigUInt64LE(0),
    state: buf.readUInt32LE(STATE_OFF),
    checkPoint: buf.readBigUInt64LE(CKPT_OFF),
    copy: Buffer.from(buf.subarray(COPY_OFF, COPY_OFF + COPY_LEN)),
    nextXid: buf.readBigUInt64LE(COPY_OFF + 24),
    nextOid: buf.readUInt32LE(COPY_OFF + 32),
    nextMulti: buf.readUInt32LE(COPY_OFF + 36),
    nextMultiOffset: buf.readUInt32LE(COPY_OFF + 40),
  }
}

/**
 * Flip pg_control state to DB_IN_PRODUCTION (6) and rewrite the CRC, so a plain
 * boot runs crash recovery over transplanted WAL (harness.mjs forceCrashState).
 */
export function forceCrashState(dir: string): void {
  const c = readControl(dir)
  c.buf.writeUInt32LE(DB_STATE.IN_PRODUCTION, STATE_OFF)
  c.buf.writeUInt32LE(crc32c(c.buf, c.crcOff), c.crcOff)
  writeFileSync(join(dir, 'global', 'pg_control'), c.buf)
}

/**
 * Read the raw byte range [startLsn, endLsn) from the datadir's pg_wal segment
 * files. Throws if a required segment is missing.
 */
export function readWalRange(
  dir: string,
  startLsn: bigint,
  endLsn: bigint,
): Buffer {
  if (endLsn < startLsn) throw new Error('readWalRange: endLsn < startLsn')
  const out = Buffer.alloc(Number(endLsn - startLsn))
  const seg = BigInt(WAL_SEG_SIZE)
  let pos = startLsn
  let outOff = 0
  while (pos < endLsn) {
    const segno = Number(pos / seg)
    const off = Number(pos % seg)
    const take = Number(
      endLsn - pos < seg - BigInt(off) ? endLsn - pos : seg - BigInt(off),
    )
    const name = walSegmentName(segno)
    const src = join(dir, 'pg_wal', name)
    if (!existsSync(src))
      throw new Error(`readWalRange: WAL segment ${name} missing`)
    const fd = openSync(src, 'r')
    try {
      readSync(fd, out, outOff, take, off)
    } finally {
      closeSync(fd)
    }
    outOff += take
    pos += BigInt(take)
  }
  return out
}

/**
 * Lay `bytes` into the datadir's pg_wal starting at `startLsn`, spanning
 * segments as needed and creating fresh zero-filled 16MB segments where
 * absent (generalization of harness.mjs transplantRange to a byte source).
 */
export function writeWalRange(
  dir: string,
  startLsn: bigint,
  bytes: Uint8Array,
): void {
  const seg = BigInt(WAL_SEG_SIZE)
  const end = startLsn + BigInt(bytes.length)
  let pos = startLsn
  let srcOff = 0
  while (pos < end) {
    const segno = Number(pos / seg)
    const off = Number(pos % seg)
    const take = Number(
      end - pos < seg - BigInt(off) ? end - pos : seg - BigInt(off),
    )
    const name = walSegmentName(segno)
    const dst = join(dir, 'pg_wal', name)
    if (!existsSync(dst)) writeFileSync(dst, Buffer.alloc(WAL_SEG_SIZE))
    const chunk = Buffer.from(bytes.buffer, bytes.byteOffset + srcOff, take)
    const fd = openSync(dst, 'r+')
    try {
      writeSync(fd, chunk, 0, take, off)
    } finally {
      closeSync(fd)
    }
    srcOff += take
    pos += BigInt(take)
  }
}

/**
 * Read magic/sysid/blcksz/segSize from a template WAL segment's long header
 * (attach.mjs walFacts).
 */
export function walFacts(dir: string): WalFacts {
  const segs = readdirSync(join(dir, 'pg_wal'))
    .filter((f) => /^[0-9A-F]{24}$/.test(f))
    .sort()
  if (segs.length === 0) throw new Error('walFacts: no WAL segments found')
  const b = readFileSync(join(dir, 'pg_wal', segs[0]))
  return {
    magic: b.readUInt16LE(0),
    sysid: b.readBigUInt64LE(24),
    segSize: b.readUInt32LE(32),
    blcksz: b.readUInt32LE(36),
  }
}

/**
 * Build the CheckPoint struct for head H from a template copy: identical
 * identity/limit fields; redo := H; TLIs := 1; oldestActiveXid := Invalid.
 * ONE struct feeds both pg_control.checkPointCopy and the minted record
 * payload (§6.5 one-struct rule). (attach.mjs mintCheckpointStruct)
 */
export function mintCheckpointStruct(templateCopy: Buffer, H: bigint): Buffer {
  const cp = Buffer.from(templateCopy)
  cp.writeBigUInt64LE(H, 0) // redo = own record start (shutdown checkpoint)
  cp.writeUInt32LE(1, 8) // ThisTimeLineID
  cp.writeUInt32LE(1, 12) // PrevTimeLineID
  cp.writeUInt32LE(0, 80) // oldestActiveXid = Invalid
  return cp
}

/**
 * Mint one zeroed 16MB segment containing only the shutdown-checkpoint record
 * at LSN H (record start), with valid page headers (attach.mjs mintSegment).
 */
export function mintSegment(
  pgWalDir: string,
  H: bigint,
  prevLsn: bigint,
  cpStruct: Buffer,
  facts: WalFacts,
): void {
  const segno = Number(H / BigInt(WAL_SEG_SIZE))
  const segStart = BigInt(segno) * BigInt(WAL_SEG_SIZE)
  const off = Number(H - segStart)
  const buf = Buffer.alloc(WAL_SEG_SIZE)

  // page 0: long header (always validated, even for mid-segment reads)
  buf.writeUInt16LE(facts.magic, 0)
  buf.writeUInt16LE(XLP_LONG, 2)
  buf.writeUInt32LE(1, 4) // tli
  buf.writeBigUInt64LE(segStart, 8) // pageaddr
  buf.writeUInt32LE(0, 16) // rem_len
  buf.writeBigUInt64LE(facts.sysid, 24)
  buf.writeUInt32LE(facts.segSize, 32)
  buf.writeUInt32LE(facts.blcksz, 36)

  // short header on the record's page, if it is not page 0
  const page = Math.floor(off / WAL_BLOCK_SIZE)
  if (page > 0) {
    const p = page * WAL_BLOCK_SIZE
    buf.writeUInt16LE(facts.magic, p)
    buf.writeUInt16LE(0, p + 2)
    buf.writeUInt32LE(1, p + 4)
    buf.writeBigUInt64LE(segStart + BigInt(p), p + 8)
    buf.writeUInt32LE(0, p + 16)
  }
  if (off % WAL_BLOCK_SIZE < (page === 0 ? 40 : 24))
    throw new Error('H inside page header')
  if (Math.floor((off + REC_LEN - 1) / WAL_BLOCK_SIZE) !== page)
    throw new Error('record would cross page boundary')

  // XLogRecord header (24) + short data header (2) + CheckPoint (88)
  const rec = Buffer.alloc(REC_LEN)
  rec.writeUInt32LE(REC_LEN, 0) // xl_tot_len
  rec.writeUInt32LE(0, 4) // xl_xid
  rec.writeBigUInt64LE(prevLsn, 8) // xl_prev (must be < H)
  rec.writeUInt8(0x00, 16) // xl_info = XLOG_CHECKPOINT_SHUTDOWN
  rec.writeUInt8(0, 17) // xl_rmid = RM_XLOG_ID
  rec.writeUInt8(255, 24) // XLR_BLOCK_ID_DATA_SHORT
  rec.writeUInt8(COPY_LEN, 25) // data length = 88
  cpStruct.copy(rec, 26)
  // CRC: payload (after 24-byte header) first, then header bytes [0,20)
  let c = crcInit()
  c = crcFeed(c, rec, 24, REC_LEN)
  c = crcFeed(c, rec, 0, 20)
  rec.writeUInt32LE(crcFin(c), 20)

  rec.copy(buf, off)
  writeFileSync(join(pgWalDir, walSegmentName(segno)), buf)
}

/**
 * Write a synthesized pg_control claiming clean shutdown at head H, with
 * checkPoint = H and checkPointCopy = cpStruct (attach.mjs
 * writeSynthesizedControl). CRC recomputed over the payload region.
 */
export function writeSynthesizedControl(
  dir: string,
  H: bigint,
  cpStruct: Buffer,
): void {
  const c = readControl(dir)
  c.buf.writeUInt32LE(DB_STATE.SHUTDOWNED, STATE_OFF) // DB_SHUTDOWNED
  c.buf.writeBigUInt64LE(H, CKPT_OFF) // checkPoint = record start
  cpStruct.copy(c.buf, COPY_OFF) // checkPointCopy = same struct
  c.buf.writeUInt32LE(crc32c(c.buf, c.crcOff), c.crcOff)
  writeFileSync(join(dir, 'global', 'pg_control'), c.buf)
}

/** Aligned length of a shutdown-checkpoint record; new WAL lands at H + this. */
export const SHUTDOWN_CKPT_ALIGNED = REC_ALIGNED
