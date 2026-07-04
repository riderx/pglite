// LSN (log sequence number) helpers and WAL segment geometry.
//
// Ported from experiments/m0-wal-roundtrip/{harness,attach}.mjs. LSNs are
// 64-bit values rendered in Postgres text form as "<hi-hex>/<lo-hex>" with
// uppercase hex digits and no zero padding (matches pg_lsn output).

/** 16 MiB WAL segments (PGlite / the M0 experiments run 16MB segments). */
export const WAL_SEG_SIZE = 16 * 1024 * 1024

/** WAL page (block) size — 8 KiB. */
export const WAL_BLOCK_SIZE = 8192

/**
 * Length of a shutdown-checkpoint WAL record: 24-byte XLogRecord header +
 * 2-byte short-data header + 88-byte CheckPoint struct (attach.mjs REC_LEN).
 */
export const SHUTDOWN_CKPT_REC_LEN = 114

/**
 * MAXALIGN(8)-rounded length of the shutdown-checkpoint record. The next WAL
 * record after a clean shutdown lands at checkPoint + this (attach.mjs
 * REC_ALIGNED); i.e. C' + 120 == B'.
 */
export const SHUTDOWN_CKPT_REC_ALIGNED = 120

/** Number of 16MB segments per 4-byte "log id" (0x1_0000_0000 / 16MB = 256). */
const SEG_PER_ID = Number(0x100000000n / BigInt(WAL_SEG_SIZE))

/** Parse a pg-text LSN string ("1A/2B3C4D") into a bigint. */
export function parseLsn(s: string): bigint {
  const [hi, lo] = s.split('/')
  return (BigInt('0x' + hi) << 32n) | BigInt('0x' + lo)
}

/** Format a bigint LSN as a pg-text string with uppercase hex (no padding). */
export function formatLsn(n: bigint): string {
  const hi = (n >> 32n).toString(16).toUpperCase()
  const lo = (n & 0xffffffffn).toString(16).toUpperCase()
  return `${hi}/${lo}`
}

/**
 * WAL segment file name for a given segment number and timeline.
 * 24 uppercase hex chars: tli(8) + logid(8) + segInLog(8), with
 * segPerId = 256 for 16MB segments (harness.mjs segFileName).
 */
export function walSegmentName(segno: number, tli = 1): string {
  const h = (n: number) => n.toString(16).toUpperCase().padStart(8, '0')
  return h(tli) + h(Math.floor(segno / SEG_PER_ID)) + h(segno % SEG_PER_ID)
}

/** Map an LSN to its containing segment number and in-segment byte offset. */
export function lsnToSegment(lsn: bigint): { segno: number; offset: number } {
  const seg = BigInt(WAL_SEG_SIZE)
  return {
    segno: Number(lsn / seg),
    offset: Number(lsn % seg),
  }
}
