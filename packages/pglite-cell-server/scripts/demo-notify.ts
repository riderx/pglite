// demo-notify — the M3 §10.2 headline demo (run: `pnpm demo:notify`).
//
// Cluster-wide LISTEN/NOTIFY in commit order, on a database that scales to
// zero. Three real `pg` connections against the wire proxy:
//
//   conn A (writer):    INSERT INTO orders ...   -- trigger pg_notify
//   conn B (listener):  LISTEN orders            -- separate connection/cell
//   conn C (listener):  LISTEN orders            -- separate connection/cell
//
// plus a RAW stream tail — the activity-feed/GUI path — printing the same
// events with zero Postgres connections. Every listener (the committing
// connection included) hears the same global order, because delivery is
// driven by N frames in the WAL stream: stream order == commit order.
//
// Side-by-side note: stock Postgres rejects LISTEN on hot standbys entirely
// ("cannot execute LISTEN during recovery") and logical replication does
// not carry notifications either. Here the stream IS the message bus.
//
// Everything runs in a throwaway temp dir, cleaned up at the end.

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import pg from 'pg'
import { GatewayCore } from '@electric-sql/pglite-gateway'
import { EraTailer, parseLsn } from '@electric-sql/pglite-cell'
import { CellHost, CellProxyServer } from '../src/index'

const { Client } = pg

async function main(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-demo-notify-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host'),
    hostId: 'demo-host',
  })
  const proxy = new CellProxyServer({ host, port: 0 })
  const port = await proxy.start()
  console.log(`\n== M3 NOTIFY demo: proxy listening on 127.0.0.1:${port} ==\n`)

  const connect = async (name: string): Promise<pg.Client> => {
    const c = new Client({
      host: '127.0.0.1',
      port,
      database: 'appdb',
      user: name,
    })
    c.on('error', () => undefined)
    await c.connect()
    return c
  }

  const a = await connect('conn-a')
  const b = await connect('conn-b')
  const c = await connect('conn-c')

  // Schema + the classic zero-infrastructure change feed: a trigger that
  // pg_notify's every insert.
  await a.query(`create table orders (id serial primary key, item text)`)
  await a.query(
    `create function notify_orders() returns trigger as $$
       begin
         perform pg_notify('orders', NEW.id || ':' || NEW.item);
         return NEW;
       end $$ language plpgsql`,
  )
  await a.query(
    `create trigger orders_notify after insert on orders
       for each row execute function notify_orders()`,
  )

  // B and C LISTEN on their own connections (their own cells). A listens
  // too — the committing connection hears its own notification via the
  // same tailer-driven path, in the same global order.
  let seq = 0
  const hear = (who: string) => (n: pg.Notification) => {
    console.log(
      `  ${who} heard #${++seq} [${n.channel}] ${JSON.stringify(n.payload)}`,
    )
  }
  a.on('notification', hear('conn A (writer) '))
  b.on('notification', hear('conn B (listener)'))
  c.on('notification', hear('conn C (listener)'))
  await a.query(`listen orders`)
  await b.query(`listen orders`)
  await c.query(`listen orders`)

  console.log(`-- conn A inserts three orders (trigger fires pg_notify) --`)
  await a.query(`insert into orders (item) values ('espresso')`)
  await a.query(`insert into orders (item) values ('flat white')`)
  await a.query(`insert into orders (item) values ('cortado')`)
  await new Promise((r) => setTimeout(r, 500))

  // The GUI path: tail the RAW stream — no Postgres connection at all —
  // and print the same events as an activity feed with commit-order
  // sequence numbers (the N frames' stream offsets).
  console.log(`\n-- raw stream activity feed (zero pg connections) --`)
  const tailer = new EraTailer(core.streamClientFor(manifest.databaseId), {
    path: manifest.era.path,
    eraId: manifest.era.id,
    ordinal: manifest.era.ordinal,
    baseOffset: manifest.era.baseOffset,
    baseLsn: parseLsn(manifest.era.baseLsn),
  })
  let feedSeq = 0
  tailer.onNotificationFrame = (header, offset) => {
    console.log(
      `  feed #${++feedSeq} [${header.channel}] ${JSON.stringify(
        header.payload,
      )}  (commit ${header.commitLsn} @ ${offset})`,
    )
  }
  await tailer.catchUp()

  console.log(
    `\n${feedSeq} notifications in the stream, ${seq} deliveries heard ` +
      `across 3 connections — same order everywhere.\n` +
      `(vanilla Postgres: 'ERROR: cannot execute LISTEN during recovery' ` +
      `on any standby)\n`,
  )

  await a.end()
  await b.end()
  await c.end()
  // Let the proxy's async session closes (detach-slice publishes) settle
  // before hibernating, so the shutdown checkpoint sees a quiet stream.
  await new Promise((r) => setTimeout(r, 500))
  await proxy.stop()
  await host.shutdown()
  await core.stop()
  rmSync(root, { recursive: true, force: true })
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
