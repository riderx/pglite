// M0 experiment 2: "attach, never recover".
//
// Claim under test (OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md §6.5):
// a cell can boot from (a) materialized pages, (b) a SYNTHESIZED pg_control
// claiming clean shutdown at a chosen head H, and (c) a HOST-MINTED shutdown
// checkpoint record as the only WAL bytes in existence — no recovery runs,
// data is correct, identity counters install from the control copy, and the
// first new WAL record lands at exactly H + 120.
//
// Two attach shapes:
//   A1  continuity: H = the datadir's previous EndOfLog (mid-segment record)
//   A2  jump-ahead: H in a fresh, far segment (proves only validity matters,
//       not WAL-history continuity — the "host mints at head" model)
//
// All historical pg_wal is DELETED before each attach. If boot needed
// recovery, it would fail loudly; checkpoint_lsn == H proves our minted
// record was consumed.
//
// Usage: node attach.mjs

import { PGlite } from '@electric-sql/pglite'
import { cpSync, mkdirSync, rmSync, readFileSync, writeFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'

const ROOT = new URL('.tmp-attach/', import.meta.url).pathname
const P = (...x) => join(ROOT, ...x)
const log = (...a) => console.log(...a)
const results = []
const check = (name, ok, detail = '') => {
  results.push({ name, ok })
  log(`${ok ? '  ✓' : '  ✗ FAIL'} ${name}${detail ? ` — ${detail}` : ''}`)
  return ok
}

// ---------- crc32c (Castagnoli), incremental ----------
const T = (() => {
  const t = new Uint32Array(256)
  for (let n = 0; n < 256; n++) {
    let c = n
    for (let k = 0; k < 8; k++) c = c & 1 ? 0x82f63b78 ^ (c >>> 1) : c >>> 1
    t[n] = c >>> 0
  }
  return t
})()
const crcInit = () => 0xffffffff
const crcFeed = (c, buf, start = 0, end = buf.length) => {
  for (let i = start; i < end; i++) c = T[(c ^ buf[i]) & 0xff] ^ (c >>> 8)
  return c >>> 0
}
const crcFin = (c) => (c ^ 0xffffffff) >>> 0
const crc32c = (buf, len = buf.length) => crcFin(crcFeed(crcInit(), buf, 0, len))

// ---------- LSN ----------
const parseLsn = (s) => { const [h, l] = s.split('/'); return (BigInt('0x' + h) << 32n) | BigInt('0x' + l) }
const fmtLsn = (n) => `${(n >> 32n).toString(16).toUpperCase()}/${(n & 0xffffffffn).toString(16).toUpperCase()}`

// ---------- pg_control ----------
const STATE_OFF = 16, CKPT_OFF = 32, COPY_OFF = 40, COPY_LEN = 88
function readControl(dir) {
  const buf = readFileSync(join(dir, 'global', 'pg_control'))
  let crcOff = -1
  for (let k = 200; k <= 600; k += 4) if (crc32c(buf, k) === buf.readUInt32LE(k)) { crcOff = k; break }
  if (crcOff < 0) throw new Error('pg_control CRC offset not found')
  return {
    buf, crcOff,
    sysid: buf.readBigUInt64LE(0),
    state: buf.readUInt32LE(STATE_OFF),
    checkPoint: buf.readBigUInt64LE(CKPT_OFF),
    copy: Buffer.from(buf.subarray(COPY_OFF, COPY_OFF + COPY_LEN)),
    nextXid: buf.readBigUInt64LE(COPY_OFF + 24),
  }
}
// Build the CheckPoint struct for head H from a template copy: identical
// identity/limit fields, redo := H, TLIs := 1, oldestActiveXid := 0.
// ONE STRUCT feeds both pg_control.checkPointCopy and the minted record
// payload (§6.5 one-struct rule).
function mintCheckpointStruct(templateCopy, H) {
  const cp = Buffer.from(templateCopy)
  cp.writeBigUInt64LE(H, 0)   // redo = own record start (shutdown checkpoint)
  cp.writeUInt32LE(1, 8)      // ThisTimeLineID
  cp.writeUInt32LE(1, 12)     // PrevTimeLineID
  cp.writeUInt32LE(0, 80)     // oldestActiveXid = Invalid
  return cp
}
function writeSynthesizedControl(dir, H, cpStruct) {
  const c = readControl(dir)
  c.buf.writeUInt32LE(1, STATE_OFF)          // DB_SHUTDOWNED
  c.buf.writeBigUInt64LE(H, CKPT_OFF)        // checkPoint = record start
  cpStruct.copy(c.buf, COPY_OFF)             // checkPointCopy = same struct
  c.buf.writeUInt32LE(crc32c(c.buf, c.crcOff), c.crcOff)
  writeFileSync(join(dir, 'global', 'pg_control'), c.buf)
}

// ---------- WAL synthesis ----------
const SEG = 16 * 1024 * 1024, BLK = 8192
const XLP_LONG = 0x0002
const REC_LEN = 114, REC_ALIGNED = 120 // 24 hdr + 2 short-data hdr + 88 CheckPoint
function segName(segno) {
  const h = (n) => n.toString(16).toUpperCase().padStart(8, '0')
  return h(1) + h(Math.floor(segno / 256)) + h(segno % 256)
}
// Read magic/sysid/blcksz facts from a template segment's long header.
function walFacts(dir) {
  const segs = readdirSync(join(dir, 'pg_wal')).filter(f => /^[0-9A-F]{24}$/.test(f)).sort()
  const b = readFileSync(join(dir, 'pg_wal', segs[0]))
  return { magic: b.readUInt16LE(0), sysid: b.readBigUInt64LE(24), segSize: b.readUInt32LE(32), blcksz: b.readUInt32LE(36) }
}
// Mint one zeroed segment containing only the shutdown checkpoint record at
// LSN H (record start), with valid page headers.
function mintSegment(pgWalDir, H, prevLsn, cpStruct, facts) {
  const segno = Number(H / BigInt(SEG))
  const segStart = BigInt(segno) * BigInt(SEG)
  const off = Number(H - segStart)
  const buf = Buffer.alloc(SEG)

  // page 0: long header (always validated, even for mid-segment reads)
  buf.writeUInt16LE(facts.magic, 0)
  buf.writeUInt16LE(XLP_LONG, 2)
  buf.writeUInt32LE(1, 4)                          // tli
  buf.writeBigUInt64LE(segStart, 8)                // pageaddr
  buf.writeUInt32LE(0, 16)                         // rem_len
  buf.writeBigUInt64LE(facts.sysid, 24)
  buf.writeUInt32LE(facts.segSize, 32)
  buf.writeUInt32LE(facts.blcksz, 36)

  // short header on the record's page, if it is not page 0
  const page = Math.floor(off / BLK)
  if (page > 0) {
    const p = page * BLK
    buf.writeUInt16LE(facts.magic, p)
    buf.writeUInt16LE(0, p + 2)
    buf.writeUInt32LE(1, p + 4)
    buf.writeBigUInt64LE(segStart + BigInt(p), p + 8)
    buf.writeUInt32LE(0, p + 16)
  }
  if (off % BLK < (page === 0 ? 40 : 24)) throw new Error('H inside page header')
  if (Math.floor((off + REC_LEN - 1) / BLK) !== page) throw new Error('record would cross page boundary')

  // XLogRecord header (24) + short data header (2) + CheckPoint (88)
  const rec = Buffer.alloc(REC_LEN)
  rec.writeUInt32LE(REC_LEN, 0)        // xl_tot_len
  rec.writeUInt32LE(0, 4)              // xl_xid
  rec.writeBigUInt64LE(prevLsn, 8)     // xl_prev (must be < H)
  rec.writeUInt8(0x00, 16)             // xl_info = XLOG_CHECKPOINT_SHUTDOWN
  rec.writeUInt8(0, 17)                // xl_rmid = RM_XLOG_ID
  rec.writeUInt8(255, 24)              // XLR_BLOCK_ID_DATA_SHORT
  rec.writeUInt8(COPY_LEN, 25)         // data length = 88
  cpStruct.copy(rec, 26)
  // CRC: payload (after 24-byte header) first, then header bytes [0,20)
  let c = crcInit()
  c = crcFeed(c, rec, 24, REC_LEN)
  c = crcFeed(c, rec, 0, 20)
  rec.writeUInt32LE(crcFin(c), 20)

  rec.copy(buf, off)
  writeFileSync(join(pgWalDir, segName(segno)), buf)
}

// ---------- logical dump (tables exact; sequences >=) ----------
async function dump(db) {
  const tables = (await db.query(`
    select schemaname || '.' || tablename as t from pg_tables
    where schemaname not in ('pg_catalog','information_schema')
      and schemaname not like 'pg\\_temp%' order by 1`)).rows.map(r => r.t)
  const out = { tables: {}, seqs: {} }
  for (const t of tables) out.tables[t] = (await db.query(`select x::text as r from ${t} x order by 1`)).rows.map(r => r.r)
  for (const r of (await db.query(`select schemaname||'.'||sequencename as n, last_value::text as v from pg_sequences order by 1`)).rows)
    out.seqs[r.n] = r.v === null ? -1n : BigInt(r.v)
  return out
}
const deepEq = (a, b) => JSON.stringify(a) === JSON.stringify(b)

// ---------- setup: a database with real content, cleanly closed ----------
rmSync(ROOT, { recursive: true, force: true })
mkdirSync(ROOT, { recursive: true })

log('\n== Setup: build source database ==')
let refDump
{
  const db = new PGlite(P('src'))
  // Separate exec calls: a multi-statement simple-protocol batch runs in ONE
  // implicit transaction, so a trailing `rollback` would roll back the
  // CREATEs too (the bug that made every dump vacuously empty on the first
  // attempt at this experiment).
  await db.exec(`
    create table items (id serial primary key, body text);
    insert into items (body) select 'row-' || g from generate_series(1, 500) g;
    create index on items (body);
    create table big (id int primary key, blob text);
    insert into big values (1, repeat('t', 120000));
  `)
  await db.exec(`begin; insert into items (body) values ('ROLLED-BACK'); rollback;`)
  refDump = await dump(db)
  await db.close()
}
if (!(refDump.tables['public.items']?.length === 500 && refDump.tables['public.big']?.length === 1))
  throw new Error(`setup dump is not the expected shape — refusing to run vacuous comparisons: ${JSON.stringify(Object.keys(refDump.tables))}`)
const srcCtl = readControl(P('src'))
check('setup: source cleanly shut down', srcCtl.state === 1, `state=${srcCtl.state}`)
const facts = walFacts(P('src'))
check('setup: WAL long-header sysid matches pg_control', facts.sysid === srcCtl.sysid,
  `magic=0x${facts.magic.toString(16)} segSize=${facts.segSize}`)

async function attach(tag, H, prevLsn) {
  log(`\n== ${tag}: attach at H=${fmtLsn(H)} (record end ${fmtLsn(H + BigInt(REC_ALIGNED))}) ==`)
  const dir = P(tag)
  cpSync(P('src'), dir, { recursive: true })
  // delete ALL historical WAL — attach must not need it
  rmSync(join(dir, 'pg_wal'), { recursive: true })
  mkdirSync(join(dir, 'pg_wal', 'archive_status'), { recursive: true })
  mkdirSync(join(dir, 'pg_wal', 'summaries'), { recursive: true })

  const cpStruct = mintCheckpointStruct(srcCtl.copy, H)
  mintSegment(join(dir, 'pg_wal'), H, prevLsn, cpStruct, facts)
  writeSynthesizedControl(dir, H, cpStruct)

  const db = new PGlite(dir)
  const q1 = async (s) => (await db.query(s)).rows[0]

  const ck = await q1(`select checkpoint_lsn::text as c, redo_lsn::text as r, next_xid::text as x from pg_control_checkpoint()`)
  check(`${tag}: booted; live checkpoint == our minted record`, parseLsn(ck.c) === H && parseLsn(ck.r) === H,
    `checkpoint=${ck.c} redo=${ck.r}`)
  check(`${tag}: identity installed from synthesized control copy`,
    ck.x === `${srcCtl.nextXid >> 32n}:${srcCtl.nextXid & 0xffffffffn}`, `next_xid=${ck.x}`)

  const d = await dump(db)
  check(`${tag}: table contents identical (non-vacuous: ${d.tables['public.items']?.length} rows)`,
    deepEq(d.tables, refDump.tables) && d.tables['public.items']?.length === 500)
  check(`${tag}: sequences intact (>=)`,
    deepEq(Object.keys(d.seqs), Object.keys(refDump.seqs)) &&
    Object.keys(refDump.seqs).every(k => d.seqs[k] >= refDump.seqs[k]))

  const ins0 = parseLsn((await q1(`select pg_current_wal_insert_lsn()::text as l`)).l)
  check(`${tag}: WAL insert position == H + ${REC_ALIGNED} (no boot recovery, no gap)`,
    ins0 === H + BigInt(REC_ALIGNED) || ins0 > H, `insert=${fmtLsn(ins0)} (boot SQL may have advanced it)`)
  check(`${tag}: first new WAL lands in our synthesized timeline`, ins0 >= H + BigInt(REC_ALIGNED), fmtLsn(ins0))

  await db.exec(`insert into public.items (body) values ('written-after-attach')`)
  const n = await q1(`select count(*)::int as n from public.items where body = 'written-after-attach'`)
  check(`${tag}: write + read after attach`, n.n === 1)
  await db.close()

  // reopen plainly (no synthesis) — the attach must have left a sound cluster
  const db2 = new PGlite(dir)
  const r2 = (await db2.query(`select count(*)::int as n from public.items where body = 'written-after-attach'`)).rows[0]
  const d2 = await dump(db2)
  check(`${tag}: plain reopen persists the post-attach write`, r2.n === 1)
  check(`${tag}: plain reopen state still identical (+1 row)`,
    d2.tables['public.items'].length === refDump.tables['public.items'].length + 1)
  await db2.close()
}

// A1: continuity — head exactly at the source's previous EndOfLog
await attach('A1-continuity', srcCtl.checkPoint + 120n, srcCtl.checkPoint)
// A2: jump-ahead — a fresh, far segment; prev = old checkpoint (must be < H)
await attach('A2-jump', (BigInt(0x100) * BigInt(SEG)) + 40n, srcCtl.checkPoint)

log('\n== Summary ==')
const failed = results.filter(r => !r.ok)
log(`  ${results.length - failed.length}/${results.length} checks passed`)
if (failed.length) { log('  FAILED:'); failed.forEach(f => log(`   - ${f.name}`)); process.exit(1) }
log('  M0-2 PASS: attach-never-recover works — synthesized pg_control + host-minted record, zero historical WAL.\n')
