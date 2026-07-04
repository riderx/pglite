// M0 experiment 4: FPI / WAL-volume accounting vs page-image manifests.
//
// Decides on-ramp A vs B (§6.2's "FPI dial"): per workload, compare real
// delta-WAL bytes (stock full_page_writes, checkpoints re-arm FPIs) against
// the page-image-manifest model, where EVERY commit ships full 8 KiB images
// of every page it touched.
//
// Usage: node fpi-accounting.mjs

import { PGlite } from '@electric-sql/pglite'
import { pg_walinspect } from '@electric-sql/pglite/contrib/pg_walinspect'
import { rmSync } from 'node:fs'

const ROOT = new URL('.tmp-fpi/', import.meta.url).pathname
rmSync(ROOT, { recursive: true, force: true })

const db = new PGlite(ROOT, {
  initDbStartParams: ['--no-data-checksums'],
  extensions: { pg_walinspect },
})
const q = async (s) => (await db.query(s)).rows
const lsn = async () => (await q(`select pg_current_wal_insert_lsn()::text as l`))[0].l

await db.exec(`
  create extension pg_walinspect;
  create table hot (id int primary key, n int default 0, v text);
  insert into hot values (1, 0, 'hot-row');
  create table append_t (id serial primary key, v text);
  create table spread (id int primary key, v text) with (fillfactor = 10);
  insert into spread select g, md5(g::text) from generate_series(1, 100) g;
`)

async function workload(name, stmtFor, n, { checkpointAt } = {}) {
  await db.exec(`checkpoint`) // re-arm FPW baseline
  const marks = [await lsn()]
  for (let i = 1; i <= n; i++) {
    if (checkpointAt === i) await db.exec(`checkpoint`)
    await db.exec(stmtFor(i))
    marks.push(await lsn())
  }
  // real WAL bytes + FPI count over the whole range
  const [tot] = await q(`
    select sum(record_length)::int as wal, count(*)::int as recs,
           sum((fpi_length > 0)::int)::int as fpis
    from pg_get_wal_records_info('${marks[0]}', '${marks[n]}')`)
  // page-image model: per txn, distinct touched (rel,block) x 8192
  let pageImageBytes = 0
  for (let i = 0; i < n; i++) {
    const [r] = await q(`
      select count(distinct br)::int as pages from (
        select unnest(string_to_array(block_ref, 'blkref')) as br
        from pg_get_wal_records_info('${marks[i]}', '${marks[i + 1]}')
        where block_ref is not null and block_ref <> ''
      ) x where br <> ''`)
    pageImageBytes += r.pages * 8192
  }
  return { name, n, wal: tot.wal, fpis: tot.fpis, pageImage: pageImageBytes,
           ratio: (pageImageBytes / tot.wal).toFixed(1) }
}

const rows = []
rows.push(await workload('hot-row update ×100', () =>
  `update hot set n = n + 1 where id = 1`, 100))
rows.push(await workload('append insert ×100', (i) =>
  `insert into append_t (v) values ('row-${i}')`, 100))
rows.push(await workload('spread update (own page each) ×100', (i) =>
  `update spread set v = v || 'x' where id = ${i}`, 100))
rows.push(await workload('hot-row update ×100, checkpoint mid-run', () =>
  `update hot set n = n + 1 where id = 1`, 100, { checkpointAt: 50 }))

console.log('\n== M0-4: WAL bytes vs page-image-manifest bytes ==')
console.log('  workload'.padEnd(44) + 'WAL'.padStart(9) + 'FPIs'.padStart(6) +
  'page-img'.padStart(10) + 'ratio'.padStart(7))
for (const r of rows) {
  console.log(`  ${r.name.padEnd(42)}${String(r.wal).padStart(9)}${String(r.fpis).padStart(6)}${String(r.pageImage).padStart(10)}${(r.ratio + '×').padStart(7)}`)
}
await db.close()
