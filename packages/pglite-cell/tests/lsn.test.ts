// Shutdown-checkpoint record footprint (M3 finding): a 120-byte record
// that straddles an 8 KiB WAL page boundary carries the continuation page
// header (24 bytes; 40 at a segment boundary) inside its LSN footprint.

import { describe, it, expect } from 'vitest'
import {
  WAL_BLOCK_SIZE,
  WAL_SEG_SIZE,
  shutdownCheckpointEnd,
  shutdownCheckpointStart,
} from '../src/lsn'

const PAGE = BigInt(WAL_BLOCK_SIZE)
const SEG = BigInt(WAL_SEG_SIZE)

describe('shutdownCheckpointEnd / shutdownCheckpointStart', () => {
  it('plain record (no boundary): +120', () => {
    const start = 0x14cd000n + 0x100n
    expect(shutdownCheckpointEnd(start)).toBe(start + 120n)
  })

  it('record ending exactly at a page boundary: +120, no continuation', () => {
    const start = 5n * PAGE - 120n
    expect(shutdownCheckpointEnd(start)).toBe(5n * PAGE)
  })

  it('record straddling a page boundary: +120 +24 (short header)', () => {
    // The M3 repro: checkPoint 0/14CDFA8, page boundary 0/14CE000 inside
    // the record, actual end 0/14CE038 (not 0/14CE020).
    const start = 0x14cdfa8n
    expect(shutdownCheckpointEnd(start)).toBe(0x14ce038n)
  })

  it('record straddling a segment boundary: +120 +40 (long header)', () => {
    const start = 2n * SEG - 60n
    expect(shutdownCheckpointEnd(start)).toBe(2n * SEG + 60n + 40n)
  })

  it('start/end round-trip across boundary offsets', () => {
    for (let k = 24n; k <= PAGE - 1n; k += 7n) {
      const start = 3n * PAGE + k
      const end = shutdownCheckpointEnd(start)
      expect(shutdownCheckpointStart(end)).toBe(start)
    }
    // Segment-boundary crossing round-trip.
    const segStart = 4n * SEG - 100n
    expect(shutdownCheckpointStart(shutdownCheckpointEnd(segStart))).toBe(
      segStart,
    )
  })
})
