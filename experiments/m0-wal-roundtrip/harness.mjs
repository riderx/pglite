// M0 experiment 1: WAL round-trip proof.
//
// Claim under test (OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md §15 M0):
// contiguous WAL byte slices captured from one PGlite instance, laid into a
// copy of an earlier checkpoint snapshot, replay via ordinary crash recovery
// to a logically identical database — including aborted transactions,
// savepoints, temp-only commits, sequences, TOAST, DDL, and multixacts —
// and identity counters (nextXid/nextMulti) chain exactly (§5.1).
//
// Scenarios:
//   R1  whole-file transplant  (baseline sanity)
//   R2  slice reassembly       (the actual slice-transplant claim)
//   R3  slice prefix           (time travel: replay stops at a bookmark)
//
// Usage: node harness.mjs

import { PGlite } from '@electric-sql/pglite'
import { cpSync, mkdirSync, rmSync, readFileSync, writeFileSync, readdirSync, existsSync, openSync, readSync, writeSync, closeSync, statSync } from 'node:fs'
import { join } from 'node:path'

const ROOT = new URL('.tmp/', import.meta.url).pathname
const P = (...x) => join(ROOT, ...x)
const log = (...a) => console.log(...a)
const results = []
const check = (name, ok, detail = '') => {
  results.push({ name, ok, detail })
  log(`${ok ? '  ✓' : '  ✗ FAIL'} ${name}${detail ? ` — ${detail}` : ''}`)
  return ok
}

// ---------- crc32c (Castagnoli, reflected, poly 0x82F63B78) ----------
const CRC_TABLE = (() => {
  const t = new Uint32Array(256)
  for (let n = 0; n < 256; n++) {
    let c = n
    for (let k = 0; k < 8; k++) c = c & 1 ? 0x82f63b78 ^ (c >>> 1) : c >>> 1
    t[n] = c >>> 0
  }
  return t
})()
const crc32c = (buf, len = buf.length) => {
  let c = 0xffffffff
  for (let i = 0; i < len; i++) c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8)
  return (c ^ 0xffffffff) >>> 0
}

// ---------- LSN helpers ----------
const parseLsn = (s) => {
  const [hi, lo] = s.split('/')
  return (BigInt('0x' + hi) << 32n) | BigInt('0x' + lo)
}
const fmtLsn = (n) => `${(n >> 32n).toString(16).toUpperCase()}/${(n & 0xffffffffn).toString(16).toUpperCase()}`

// ---------- pg_control surgery ----------
// DBState: 0 STARTUP, 1 SHUTDOWNED, 2 SHUTDOWNED_IN_RECOVERY, 3 SHUTDOWNING,
//          4 IN_CRASH_RECOVERY, 5 IN_ARCHIVE_RECOVERY, 6 IN_PRODUCTION
const STATE_OFF = 16 // sysid(8) + pg_control_version(4) + catalog_version_no(4)
function readControl(dir) {
  const buf = readFileSync(join(dir, 'global', 'pg_control'))
  // discover offsetof(crc) empirically: the last uint32 c at aligned offset k
  // with crc32c(buf[0..k)) == c. Scan a generous window.
  let crcOff = -1
  for (let k = 200; k <= 600; k += 4) {
    if (crc32c(buf, k) === buf.readUInt32LE(k)) { crcOff = k; break }
  }
  if (crcOff < 0) throw new Error('pg_control CRC offset not found — layout assumption broken')
  // ControlFileData: sysid(8) ver(4) cat(4) state(4) pad(4) time(8) → 32,
  // checkPoint(8) → 40, then checkPointCopy: redo(8) tli(4) prevTli(4)
  // fullPageWrites(1)+pad/wal_level(7) → +24, nextXid(8) nextOid(4)
  // nextMulti(4) nextMultiOffset(4). (wasm32 build, MAXALIGN 8.)
  const cp = 40
  return {
    buf, crcOff,
    state: buf.readUInt32LE(STATE_OFF),
    checkPoint: buf.readBigUInt64LE(32),
    nextXid: buf.readBigUInt64LE(cp + 24),
    nextOid: buf.readUInt32LE(cp + 32),
    nextMulti: buf.readUInt32LE(cp + 36),
    nextMultiOffset: buf.readUInt32LE(cp + 40),
  }
}
const fmtXid8 = (n) => `${n >> 32n}:${n & 0xffffffffn}`
function forceCrashState(dir) {
  const c = readControl(dir)
  c.buf.writeUInt32LE(6, STATE_OFF) // DB_IN_PRODUCTION → boot runs crash recovery
  c.buf.writeUInt32LE(crc32c(c.buf, c.crcOff), c.crcOff)
  writeFileSync(join(dir, 'global', 'pg_control'), c.buf)
}

// ---------- WAL segment mapping ----------
const TLI = 1
function segFileName(segno, segPerId) {
  const h = (n) => n.toString(16).toUpperCase().padStart(8, '0')
  return h(TLI) + h(Math.floor(segno / segPerId)) + h(segno % segPerId)
}
// copy byte range [a,b) of the WAL address space from srcDir/pg_wal into dstDir/pg_wal
function transplantRange(srcDir, dstDir, a, b, segSize) {
  const segSizeN = BigInt(segSize)
  const segPerId = Number(0x100000000n / segSizeN)
  let pos = a
  while (pos < b) {
    const segno = Number(pos / segSizeN)
    const off = Number(pos % segSizeN)
    const take = Number((b - pos) < (segSizeN - BigInt(off)) ? (b - pos) : (segSizeN - BigInt(off)))
    const name = segFileName(segno, segPerId)
    const src = join(srcDir, 'pg_wal', name)
    const dst = join(dstDir, 'pg_wal', name)
    if (!existsSync(dst)) writeFileSync(dst, Buffer.alloc(segSize)) // fresh zero segment
    const chunk = Buffer.alloc(take)
    const sfd = openSync(src, 'r')
    readSync(sfd, chunk, 0, take, off)
    closeSync(sfd)
    const dfd = openSync(dst, 'r+')
    writeSync(dfd, chunk, 0, take, off)
    closeSync(dfd)
    pos += BigInt(take)
  }
}

// ---------- logical dump ----------
// Tables compare exactly. Sequences compare by the vanilla crash contract:
// same set of sequences, replica last_value >= primary (crash recovery jumps
// to the logged-ahead value — gaps are the documented semantics). Temp
// schemas are session-local by definition and excluded.
async function logicalDump(db) {
  const tables = (await db.query(`
    select schemaname || '.' || tablename as t from pg_tables
    where schemaname not in ('pg_catalog','information_schema')
      and schemaname not like 'pg\\_temp%' order by 1`)).rows.map(r => r.t)
  const out = { tables: {}, seqs: {} }
  for (const t of tables) {
    out.tables[t] = (await db.query(`select x::text as r from ${t} x order by 1`)).rows.map(r => r.r)
  }
  for (const r of (await db.query(`
    select schemaname || '.' || sequencename as n, last_value::text as v
    from pg_sequences order by 1`)).rows) {
    out.seqs[r.n] = r.v === null ? -1n : BigInt(r.v)
  }
  return out
}
function compareDump(tag, got, expect) {
  const tOk = deepEq(got.tables, expect.tables)
  check(`${tag}: table contents identical`, tOk, tOk ? '' : `DIFF — see .tmp/${tag}.diff.json`)
  if (!tOk) writeFileSync(P(`${tag}.diff.json`),
    JSON.stringify({ expect: expect.tables, got: got.tables }, null, 2))
  const names = deepEq(Object.keys(got.seqs), Object.keys(expect.seqs))
  const ge = names && Object.keys(expect.seqs).every(k => got.seqs[k] >= expect.seqs[k])
  check(`${tag}: sequences obey the crash contract (same set, replica >= primary)`, names && ge,
    Object.entries(got.seqs).map(([k, v]) => `${k}:${expect.seqs[k]}→${v}`).join(' '))
}
async function counters(db) {
  const r = (await db.query(`
    select next_xid::text as next_xid, next_oid::text as next_oid,
           next_multixact_id::text as next_multi, next_multi_offset::text as next_moff,
           checkpoint_lsn::text as ckpt, redo_lsn::text as redo
    from pg_control_checkpoint()`)).rows[0]
  return r
}
const deepEq = (a, b) => JSON.stringify(a) === JSON.stringify(b)

// ---------- phases ----------
rmSync(ROOT, { recursive: true, force: true })
mkdirSync(ROOT, { recursive: true })

log('\n== Phase A: base schema, clean shutdown, snapshot ==')
{
  const db = new PGlite(P('primary'))
  await db.exec(`
    create table parent (id int primary key, note text);
    create table child  (id serial primary key, pid int references parent(id), body text);
    create table events (id bigserial primary key, body text);
    create sequence loose_seq;
    insert into parent values (1,'one'),(2,'two');
  `)
  await db.close()
  const st = readControl(P('primary'))
  check('A: clean shutdown leaves pg_control DB_SHUTDOWNED', st.state === 1, `state=${st.state}`)
  cpSync(P('primary'), P('snapshot'), { recursive: true })
  log(`  snapshot checkpoint @ ${fmtLsn(st.checkPoint)}`)
}

log('\n== Phase B: workload with LSN bookmarks ==')
const marks = []
let dumpAtPrefix, dumpFull, segSize, base, snapEnd
{
  const db = new PGlite(P('primary'))
  const q1 = async (sql) => (await db.query(sql)).rows[0]
  const lsn = async () => parseLsn((await q1('select pg_current_wal_insert_lsn()::text as l')).l)

  const wl = (await q1('show wal_level')).wal_level
  check('B: wal_level is replica', wl === 'replica', wl)
  const fpw = (await q1('show full_page_writes')).full_page_writes
  check('B: full_page_writes on', fpw === 'on', fpw)
  segSize = 16 * 1024 * 1024
  const ss = (await q1('show wal_segment_size')).wal_segment_size
  check('B: wal_segment_size 16MB', ss === '16MB', ss)

  base = await lsn()
  const snapCkpt = readControl(P('snapshot')).checkPoint
  snapEnd = snapCkpt + 120n // shutdown checkpoint record end = prior EndOfLog
  const bootWal = base - snapEnd
  // FINDING, not failure: published PGlite writes WAL during reopen (its
  // bootstrap SQL). The design's slices are contiguous from the attach
  // point, so boot writes simply ride in the first slice; the harness
  // anchors slice zero at snapEnd accordingly. (§6.5's GUC-mirror pin is
  // about *avoiding* PARAMETER_CHANGE boot records; this is bigger and
  // needs identifying before M1.)
  log(`  finding: reopen wrote ${bootWal} bytes of WAL before first txn (snapEnd=${fmtLsn(snapEnd)} base=${fmtLsn(base)})`)

  const step = async (name, fn) => { await fn(); marks.push({ name, lsn: await lsn() }) }

  await step('plain insert', () => db.exec(`insert into events (body) values ('e1'),('e2')`))
  await step('aborted txn with writes', () =>
    db.exec(`begin; insert into events (body) values ('ROLLED-BACK'); rollback`))
  await step('savepoint partial rollback', () =>
    db.exec(`begin;
      insert into events (body) values ('kept-a');
      savepoint s; insert into events (body) values ('DISCARDED'); rollback to s;
      insert into events (body) values ('kept-c'); commit`))
  await step('temp-only commit (forced commit record)', () =>
    db.exec(`begin; create temp table tt (x int); insert into tt values (1); commit`))
  await step('abort-only nextval', () =>
    db.exec(`begin; select nextval('loose_seq'); rollback`))
  await step('sequences + serial', () =>
    db.exec(`insert into child (pid, body) values (1,'c1'),(2,'c2')`))
  await step('multixact candidate (subxact FOR SHARE + update)', () =>
    db.exec(`begin;
      savepoint s1; select * from parent where id = 1 for share;
      savepoint s2; update parent set note = 'one!' where id = 1;
      commit`))
  await step('toast (100 KB value)', () =>
    db.exec(`insert into events (body) values (repeat('x', 100000) || 'END')`))

  // prefix bookmark: everything above replays in R3; everything below must not
  dumpAtPrefix = await logicalDump(db)

  await step('DDL in the slice', () =>
    db.exec(`create table late (id int primary key, v text);
             create index on events (id desc);
             insert into late values (7,'seven')`))
  await step('post-DDL dml', () =>
    db.exec(`update events set body = 'e1-updated' where body = 'e1';
             delete from events where body = 'e2'`))

  dumpFull = await logicalDump(db)
  await db.close()
}
const prefixMark = marks[7] // through 'toast'
const endMark = marks[marks.length - 1]
log(`  ${marks.length} steps, base=${fmtLsn(base)} prefix=${fmtLsn(prefixMark.lsn)} end=${fmtLsn(endMark.lsn)}`)

// Reference counters parsed straight from the primary's pg_control — a
// reopen would contaminate (PGlite boot writes WAL and assigns xids). The
// shutdown checkpoint also tells us the true end of durable WAL: session
// cleanup (temp-table drops) runs a final transaction AFTER the last
// bookmark, and slices must cover everything durably written.
const primCtl = readControl(P('primary'))
const walEnd = primCtl.checkPoint // start of the shutdown checkpoint record
const primCounters = {
  next_xid: fmtXid8(primCtl.nextXid),
  next_oid: String(primCtl.nextOid),
  next_multi: String(primCtl.nextMulti),
}
check('B: parsed pg_control counters plausible',
  primCtl.nextXid >> 32n === 0n && (primCtl.nextXid & 0xffffffffn) > 700n && primCtl.nextOid >= 16384,
  `nextXid=${primCounters.next_xid} nextOid=${primCounters.next_oid} nextMulti=${primCounters.next_multi}`)
check('B: workload minted a multixact', primCtl.nextMulti > 1, `next_multixact_id=${primCounters.next_multi}`)
log(`  durable WAL end (pre-shutdown-checkpoint) = ${fmtLsn(walEnd)}; ` +
  `${walEnd - endMark.lsn} bytes of session-cleanup WAL after the last bookmark`)

async function bootAndCompare(tag, dir, expectDump, { compareCounters = false } = {}) {
  const db = new PGlite(dir)
  const dump = await logicalDump(db)
  compareDump(tag, dump, expectDump)
  if (compareCounters) {
    const c = await counters(db)
    check(`${tag}: nextXid chains exactly`, c.next_xid === primCounters.next_xid,
      `${c.next_xid} vs ${primCounters.next_xid}`)
    check(`${tag}: nextMultiXactId chains exactly`, c.next_multi === primCounters.next_multi,
      `${c.next_multi} vs ${primCounters.next_multi}`)
    check(`${tag}: nextOid >= primary (prefetch jump allowed)`,
      BigInt(c.next_oid) >= BigInt(primCounters.next_oid), `${c.next_oid} vs ${primCounters.next_oid}`)
  }
  await db.close()
}

log('\n== R1: whole-file transplant ==')
{
  cpSync(P('snapshot'), P('r1'), { recursive: true })
  rmSync(P('r1', 'pg_wal'), { recursive: true })
  cpSync(P('primary', 'pg_wal'), P('r1', 'pg_wal'), { recursive: true })
  forceCrashState(P('r1'))
  await bootAndCompare('R1', P('r1'), dumpFull, { compareCounters: true })
}

log('\n== R2: slice reassembly (the real claim) ==')
{
  cpSync(P('snapshot'), P('r2'), { recursive: true })
  // transplant each bookmarked slice individually, in order — the design's
  // commit-slice model, not a bulk copy. Slice zero is (snapEnd..mark0]:
  // contiguous from the prior EndOfLog, so boot-time WAL rides along.
  let cursor = snapEnd
  for (const m of marks) {
    transplantRange(P('primary'), P('r2'), cursor, m.lsn, segSize)
    cursor = m.lsn
  }
  // final slice: last bookmark → end of durable WAL (session-cleanup txn)
  transplantRange(P('primary'), P('r2'), cursor, walEnd, segSize)
  forceCrashState(P('r2'))
  await bootAndCompare('R2', P('r2'), dumpFull, { compareCounters: true })
}

log('\n== R3: slice prefix → time travel to a bookmark ==')
{
  cpSync(P('snapshot'), P('r3'), { recursive: true })
  transplantRange(P('primary'), P('r3'), snapEnd, prefixMark.lsn, segSize)
  forceCrashState(P('r3'))
  await bootAndCompare('R3', P('r3'), dumpAtPrefix)
}

log('\n== Summary ==')
const failed = results.filter(r => !r.ok)
log(`  ${results.length - failed.length}/${results.length} checks passed`)
if (failed.length) { log('  FAILED:'); failed.forEach(f => log(`   - ${f.name} ${f.detail}`)); process.exit(1) }
log('  M0-1 PASS: WAL slices are transplantable; identity counters chain.\n')
