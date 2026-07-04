// demo-scale-to-zero — the M1e narrated demo (run: `pnpm demo`).
//
// Stands up the whole vertical slice on a real TCP port — an embedded
// GatewayCore (object store + control plane + Durable Streams), a CellHost,
// and a CellProxyServer speaking the Postgres wire protocol — then drives a
// real `pg` client through the scale-to-zero story with timings:
//
//   create database -> connect -> DDL + 1000 inserts -> read-your-writes on a
//   second connection -> interactive txn conflict (40001) -> hibernate
//   (checkpoint + detach; stream/checkpoint sizes) -> reconnect (wake from
//   checkpoint; wake-to-first-row ms) -> data intact -> summary table.
//
// Everything runs in a throwaway temp dir, cleaned up at the end.

import { mkdtempSync, rmSync, statSync, readdirSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import pg from 'pg'
import { GatewayCore } from '@electric-sql/pglite-gateway'
import { CellHost, CellProxyServer } from '../src/index'

const { Client } = pg

function ms(t0: number): string {
  return `${Date.now() - t0}ms`
}

/** Total size in bytes of everything under `dir` (recursive). */
function dirBytes(dir: string): number {
  let total = 0
  const walk = (d: string): void => {
    for (const name of readdirSync(d)) {
      const p = join(d, name)
      const st = statSync(p)
      if (st.isDirectory()) walk(p)
      else total += st.size
    }
  }
  try {
    walk(dir)
  } catch {
    /* dir gone */
  }
  return total
}

function human(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`
  return `${(bytes / 1024).toFixed(1)} KB`
}

function log(step: string, detail = ''): void {
  const pad = step.padEnd(28)
  console.log(`  ${pad}${detail}`)
}

function rule(): void {
  console.log('  ' + '─'.repeat(56))
}

async function newClient(port: number): Promise<pg.Client> {
  const client = new Client({
    host: '127.0.0.1',
    port,
    database: 'demo',
    user: 'postgres',
  })
  await client.connect()
  return client
}

async function main(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-demo-'))
  const gwRoot = join(root, 'gw')
  const core = new GatewayCore({ dataRoot: gwRoot })
  await core.start()
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host'),
    hostId: 'demo-host',
    // Default checkpointOnHibernateBytes = 0 (always checkpoint on hibernate)
    // is exactly the scale-to-zero behavior this demo showcases.
  })
  const proxy = new CellProxyServer({ host, port: 0, defaultDatabase: 'demo' })
  const port = await proxy.start()

  const timings: Record<string, string> = {}
  let streamKb = ''
  let checkpointKb = ''

  console.log('')
  console.log('  PGlite optimistic physical replication — scale-to-zero demo')
  console.log(`  proxy listening on 127.0.0.1:${port}`)
  rule()

  try {
    // 1) Create the database (initdb -> settling boot -> checkpoint 0 -> era).
    let t = Date.now()
    const manifest = await core.createDatabase('demo')
    timings['create database'] = ms(t)
    log(
      'create database',
      `${timings['create database']} (era ${manifest.era.id})`,
    )
    const dbId = manifest.databaseId

    // 2) Connect a real pg client over TCP.
    t = Date.now()
    const a = await newClient(port)
    timings['connect (cold)'] = ms(t)
    log('connect (cold attach)', timings['connect (cold)'])

    // 3) DDL + 1000 inserts.
    t = Date.now()
    await a.query(
      `create table items (id serial primary key, sku text, qty int)`,
    )
    await a.query('begin')
    for (let i = 0; i < 1000; i++) {
      await a.query(`insert into items (sku, qty) values ($1, $2)`, [
        `sku-${i}`,
        i,
      ])
    }
    await a.query('commit')
    timings['create table + 1000 rows'] = ms(t)
    log('create table + 1000 rows', timings['create table + 1000 rows'])

    // LISTEN/NOTIFY is M3 — noted, skipped.
    log('LISTEN/NOTIFY', 'skipped (M3)')

    // 4) Second connection reads its own writes (watermark gate, §7).
    t = Date.now()
    const b = await newClient(port)
    const count = await b.query(`select count(*)::int as n from items`)
    timings['read-your-writes (conn 2)'] = ms(t)
    log(
      'read-your-writes (conn 2)',
      `${timings['read-your-writes (conn 2)']} — sees ${count.rows[0].n} rows`,
    )

    // 5) Interactive txn conflict: A holds a txn open, B commits underneath,
    //    A's COMMIT loses the race and gets 40001 (session survives).
    await a.query(`insert into items (sku, qty) values ('seed', 0)`) // sticky write
    await a.query('begin')
    await a.query(`insert into items (sku, qty) values ('a-txn', 1)`)
    await b.query(`insert into items (sku, qty) values ('b-wins', 2)`)
    let conflictCode = '(none)'
    try {
      await a.query('commit')
    } catch (err) {
      conflictCode = (err as { code?: string }).code ?? '(unknown)'
    }
    log('interactive txn conflict', `COMMIT -> SQLSTATE ${conflictCode}`)
    // A survives; verify it is usable and re-run its work as a one-shot.
    await a.query(`insert into items (sku, qty) values ('a-retry', 1)`)

    const beforeHibernate = (
      await b.query(`select count(*)::int as n from items`)
    ).rows[0].n

    // 6) Hibernate: checkpoint (default = always) + detach; scale to zero.
    t = Date.now()
    await a.end()
    await b.end()
    await host.hibernateDatabase('demo')
    timings['hibernate (ckpt + detach)'] = ms(t)
    // Sizes AFTER hibernation: the checkpoint object bounds wake cost; the
    // stream holds the (now short) tail past the checkpoint.
    const latest = await core.latestCheckpoint(dbId)
    checkpointKb = human((await core.getObject(latest!.objectRef)).length)
    streamKb = human(dirBytes(join(gwRoot, 'streams')))
    log(
      'hibernate (ckpt + detach)',
      `${timings['hibernate (ckpt + detach)']} — checkpoint ${checkpointKb}, stream ${streamKb}`,
    )

    // 7) Reconnect: wake from the checkpoint; measure wake-to-first-row.
    t = Date.now()
    const c = await newClient(port)
    const firstRow = await c.query(`select count(*)::int as n from items`)
    timings['wake-to-first-row'] = ms(t)
    log(
      'wake-to-first-row',
      `${timings['wake-to-first-row']} — ${firstRow.rows[0].n} rows intact`,
    )

    // 8) Data intact assertion.
    if (firstRow.rows[0].n !== beforeHibernate) {
      throw new Error(
        `data loss: ${firstRow.rows[0].n} rows after wake, expected ${beforeHibernate}`,
      )
    }
    const sample = await c.query(`select sku, qty from items where id = 1`)
    log(
      'data intact',
      `row 1 = ${JSON.stringify(sample.rows[0])} (${beforeHibernate} rows total)`,
    )
    await c.end()

    // Summary table.
    rule()
    console.log('  summary')
    rule()
    for (const [k, v] of Object.entries(timings)) {
      console.log(`  ${k.padEnd(28)}${v}`)
    }
    console.log(`  ${'checkpoint object'.padEnd(28)}${checkpointKb}`)
    console.log(`  ${'stream tail (post-ckpt)'.padEnd(28)}${streamKb}`)
    console.log(
      `  ${'interactive conflict'.padEnd(28)}SQLSTATE ${conflictCode}`,
    )
    rule()
    console.log('  scale-to-zero round trip complete — data intact.')
    console.log('')
  } finally {
    await proxy.stop().catch(() => undefined)
    await host.shutdown().catch(() => undefined)
    await core.stop().catch(() => undefined)
    rmSync(root, { recursive: true, force: true })
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
