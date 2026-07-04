import { PGlite } from '@electric-sql/pglite'
import { pg_walinspect } from '@electric-sql/pglite/contrib/pg_walinspect'
import { rmSync } from 'node:fs'
const dir = new URL('.tmp-inspect/', import.meta.url).pathname
rmSync(dir, { recursive: true, force: true })
let db = new PGlite(dir)
await db.exec(`create table t (x int); insert into t values (1)`)
await db.close()
db = new PGlite(dir, { extensions: { pg_walinspect } })
const q = async (s) => (await db.query(s)).rows
const [{ ckpt }] = await q(`select checkpoint_lsn::text as ckpt from pg_control_checkpoint()`)
const [{ base }] = await q(`select pg_current_wal_insert_lsn()::text as base`)
console.log(`checkpoint=${ckpt} base-after-boot=${base}`)
await db.exec(`create extension if not exists pg_walinspect`)
const recs = await q(`
  select resource_manager as rm, record_type as type, count(*)::int as n,
         sum(main_data_length + fpi_length + 24)::int as approx_bytes
  from pg_get_wal_records_info('${ckpt}', '${base}')
  group by 1, 2 order by approx_bytes desc`)
console.table(recs)
const [{ total }] = await q(`
  select count(*)::int as total from pg_get_wal_records_info('${ckpt}', '${base}')`)
console.log('total records in boot range:', total)
// which relations do the heap records touch?
const rels = await q(`
  select block_ref, count(*)::int as n
  from pg_get_wal_records_info('${ckpt}', '${base}')
  where resource_manager in ('Heap','Heap2','Btree')
  group by 1 order by n desc limit 12`)
console.table(rels)
await db.close()
