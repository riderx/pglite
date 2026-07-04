import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync, mkdirSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { PGlite } from '@electric-sql/pglite'
import { crc32c } from '../src/crc32c'
import {
  walSegmentName,
  lsnToSegment,
  parseLsn,
  formatLsn,
  WAL_SEG_SIZE,
} from '../src/lsn'
import {
  readControl,
  forceCrashState,
  readWalRange,
  writeWalRange,
  DB_STATE,
} from '../src/datadir'

function tmp(prefix: string): string {
  return mkdtempSync(join(tmpdir(), prefix))
}

describe('crc32c known vectors', () => {
  it('crc32c("123456789") === 0xE3069283', () => {
    const buf = new TextEncoder().encode('123456789')
    expect(crc32c(buf) >>> 0).toBe(0xe3069283)
  })

  it('crc32c of empty === 0', () => {
    expect(crc32c(new Uint8Array(0))).toBe(0)
  })

  it('crc32c of a single zero byte', () => {
    // known Castagnoli value for one 0x00 byte
    expect(crc32c(new Uint8Array([0])) >>> 0).toBe(0x527d5351)
  })
})

describe('LSN / WAL segment math', () => {
  it('parseLsn / formatLsn round-trip (uppercase hex)', () => {
    expect(parseLsn('0/0')).toBe(0n)
    expect(formatLsn(0n)).toBe('0/0')
    expect(parseLsn('1/1A2B3C4D')).toBe((1n << 32n) | 0x1a2b3c4dn)
    expect(formatLsn((1n << 32n) | 0x1a2b3c4dn)).toBe('1/1A2B3C4D')
  })

  it('walSegmentName: 24 uppercase hex chars, segPerId=256', () => {
    expect(walSegmentName(0)).toBe('000000010000000000000000')
    // segno 256 => logid 1, seg 0
    expect(walSegmentName(256)).toBe('000000010000000100000000')
    // segno 257 => logid 1, seg 1
    expect(walSegmentName(257)).toBe('000000010000000100000001')
    expect(walSegmentName(0, 5)).toBe('000000050000000000000000')
  })

  it('lsnToSegment maps LSN to segment + offset', () => {
    expect(lsnToSegment(0n)).toEqual({ segno: 0, offset: 0 })
    expect(lsnToSegment(BigInt(WAL_SEG_SIZE))).toEqual({ segno: 1, offset: 0 })
    expect(lsnToSegment(BigInt(WAL_SEG_SIZE) + 42n)).toEqual({
      segno: 1,
      offset: 42,
    })
  })
})

describe('writeWalRange / readWalRange round-trip across a segment boundary', () => {
  it('lays and reads bytes spanning two segments', () => {
    const dir = tmp('pgcell-wal-')
    try {
      mkdirSync(join(dir, 'pg_wal'), { recursive: true })
      // start 100 bytes before the seg boundary, write 200 bytes -> spans segs 0 and 1
      const start = BigInt(WAL_SEG_SIZE) - 100n
      const bytes = new Uint8Array(200)
      for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 13 + 7) & 0xff
      writeWalRange(dir, start, bytes)

      // both segments should now exist
      const seg0 = join(dir, 'pg_wal', walSegmentName(0))
      const seg1 = join(dir, 'pg_wal', walSegmentName(1))
      expect(readFileSync(seg0).length).toBe(WAL_SEG_SIZE)
      expect(readFileSync(seg1).length).toBe(WAL_SEG_SIZE)

      const got = readWalRange(dir, start, start + BigInt(bytes.length))
      expect([...got]).toEqual([...bytes])

      // boundary bytes land correctly: last byte of seg0 and first of seg1
      const s0 = readFileSync(seg0)
      const s1 = readFileSync(seg1)
      expect(s0[WAL_SEG_SIZE - 1]).toBe(bytes[99])
      expect(s1[0]).toBe(bytes[100])
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  it('readWalRange throws on a missing segment', () => {
    const dir = tmp('pgcell-wal-missing-')
    try {
      mkdirSync(join(dir, 'pg_wal'), { recursive: true })
      expect(() => readWalRange(dir, 0n, 10n)).toThrow(/missing/)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })
})

describe('real PGlite datadir: control read / crash-state / recovery', () => {
  it('clean close ⇒ SHUTDOWNED; forceCrashState ⇒ IN_PRODUCTION; reopen recovers data', async () => {
    const dir = tmp('pgcell-db-')
    try {
      {
        const db = new PGlite(dir, {
          initDbStartParams: ['--no-data-checksums'],
        })
        await db.exec(`
          create table t (id int primary key, v text);
          insert into t values (1, 'one'), (2, 'two');
        `)
        await db.close()
      }
      const ctl = readControl(dir)
      expect(ctl.state).toBe(DB_STATE.SHUTDOWNED)
      expect(ctl.nextXid > 0n).toBe(true)
      expect(ctl.nextOid).toBeGreaterThanOrEqual(16384)

      forceCrashState(dir)
      expect(readControl(dir).state).toBe(DB_STATE.IN_PRODUCTION)

      // reopen: boot runs crash recovery over the datadir's own WAL
      const db2 = new PGlite(dir)
      const rows = (await db2.query(`select id, v from t order by id`)).rows
      expect(rows).toEqual([
        { id: 1, v: 'one' },
        { id: 2, v: 'two' },
      ])
      // recovered cluster shuts down cleanly again
      await db2.close()
      expect(readControl(dir).state).toBe(DB_STATE.SHUTDOWNED)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  it('live capture: read the WAL range written by an insert (nonzero bytes)', async () => {
    const dir = tmp('pgcell-live-')
    try {
      const db = new PGlite(dir, { initDbStartParams: ['--no-data-checksums'] })
      await db.exec(`create table cap (id serial primary key, body text)`)

      const lsn = async () =>
        parseLsn(
          (
            (await db.query(
              `select pg_current_wal_insert_lsn()::text as l`,
            )) as { rows: { l: string }[] }
          ).rows[0].l,
        )

      const before = await lsn()
      await db.exec(
        `insert into cap (body) select 'row-' || g from generate_series(1, 200) g`,
      )
      const after = await lsn()
      expect(after > before).toBe(true)

      // WAL bytes are visible on the host fs immediately (no flush step).
      const slice = readWalRange(dir, before, after)
      expect(slice.length).toBe(Number(after - before))
      // the captured slice is not all zeros
      expect(slice.some((b) => b !== 0)).toBe(true)

      await db.close()
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })
})
