// crc32c (Castagnoli, reflected, poly 0x82F63B78) — the CRC Postgres uses for
// pg_control and WAL records. Incremental API ported verbatim from
// experiments/m0-wal-roundtrip/attach.mjs.

const T: Uint32Array = (() => {
  const t = new Uint32Array(256)
  for (let n = 0; n < 256; n++) {
    let c = n
    for (let k = 0; k < 8; k++) c = c & 1 ? 0x82f63b78 ^ (c >>> 1) : c >>> 1
    t[n] = c >>> 0
  }
  return t
})()

/** Start an incremental crc32c computation. */
export const crcInit = (): number => 0xffffffff

/** Feed bytes [start, end) of `buf` into the running crc. */
export const crcFeed = (
  c: number,
  buf: Uint8Array,
  start = 0,
  end = buf.length,
): number => {
  for (let i = start; i < end; i++) c = T[(c ^ buf[i]) & 0xff] ^ (c >>> 8)
  return c >>> 0
}

/** Finalize a running crc into the output value. */
export const crcFin = (c: number): number => (c ^ 0xffffffff) >>> 0

/** One-shot crc32c over the first `len` bytes of `buf`. */
export const crc32c = (buf: Uint8Array, len = buf.length): number =>
  crcFin(crcFeed(crcInit(), buf, 0, len))
