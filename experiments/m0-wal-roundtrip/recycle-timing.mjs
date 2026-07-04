// M0 experiment 3: cell recycle timing.
//
// Measures the costs the design's "recycle is cheap" claims rest on (§3.4,
// OQ5): fresh initdb, plain reopen (the recycle+reattach path), and close —
// on a small and a deliberately fattened datadir, to show reopen cost does
// not scale with database size. NodeFS numbers on this machine; the lazy
// VFS changes the constants, not the shape.
//
// Usage: node recycle-timing.mjs

import { PGlite } from '@electric-sql/pglite'
import { mkdirSync, rmSync } from 'node:fs'
import { join } from 'node:path'

const ROOT = new URL('.tmp-timing/', import.meta.url).pathname
rmSync(ROOT, { recursive: true, force: true })
mkdirSync(ROOT, { recursive: true })
const OPTS = { initDbStartParams: ['--no-data-checksums'] }
const ms = (t) => `${t.toFixed(0)} ms`
const median = (xs) => xs.sort((a, b) => a - b)[Math.floor(xs.length / 2)]

async function timeOpen(dir, opts = {}) {
  const t0 = performance.now()
  const db = new PGlite(dir, opts)
  await db.query('select 1')
  const t = performance.now() - t0
  return { db, t }
}

const results = []

// fresh initdb (small)
{
  const { db, t } = await timeOpen(join(ROOT, 'small'), OPTS)
  results.push(['fresh initdb + ready', ms(t)])
  await db.exec(`create table t (id serial primary key, v text);
                 insert into t (v) select 'x' from generate_series(1, 100)`)
  const t0 = performance.now()
  await db.close()
  results.push(['clean close (small)', ms(performance.now() - t0)])
}

// reopen small ×5
{
  const times = []
  for (let i = 0; i < 5; i++) {
    const { db, t } = await timeOpen(join(ROOT, 'small'))
    times.push(t)
    await db.close()
  }
  results.push(['reopen small (median of 5)', ms(median(times))])
}

// fatten a second dir to ~40 MB and reopen
{
  const { db } = await timeOpen(join(ROOT, 'large'), OPTS)
  await db.exec(`create table big (id serial primary key, v text)`)
  for (let i = 0; i < 20; i++) {
    await db.exec(
      `insert into big (v) select md5(g::text) || md5((g+1)::text) || md5((g+2)::text)
       from generate_series(1, 20000) g`
    )
  }
  const [{ sz }] = (await db.query(
    `select pg_size_pretty(pg_database_size(current_database())) as sz`
  )).rows
  const t0 = performance.now()
  await db.close()
  results.push([`clean close (large, ${sz})`, ms(performance.now() - t0)])

  const times = []
  for (let i = 0; i < 5; i++) {
    const { db: d2, t } = await timeOpen(join(ROOT, 'large'))
    times.push(t)
    await d2.close()
  }
  results.push([`reopen large (${sz}, median of 5)`, ms(median(times))])
}

console.log('\n== M0-3: recycle timing ==')
for (const [k, v] of results) console.log(`  ${k.padEnd(38)} ${v}`)
