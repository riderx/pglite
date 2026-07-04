// M3 NOTIFY exit tests (§10.2): cluster-wide LISTEN/NOTIFY in commit order
// over real `pg` clients — host LISTEN aggregation, notification harvest at
// commit, N frames riding the winning CAS POST (same append group as the W
// frame), uniform tailer-driven delivery ('A' stripping + synthesis), and
// the raw-stream activity feed.
//
// Each test builds its own GatewayCore + CellHost + proxy (many PGlite
// boots — generous timeouts; the suite is serialized in vitest.config.ts).

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Client } from 'pg'
import type { Notification } from 'pg'
import {
  EraTailer,
  PositionCheckedReader,
  parseLsn,
} from '@electric-sql/pglite-cell'
import type { AppendGroup, NFrameHeader } from '@electric-sql/pglite-cell'
import { GatewayCore } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'
import { CellProxyServer } from '../src/proxy/server'
import type { HostSession, UnitObservation } from '../src/session'

const TEST_TIMEOUT = 240_000

interface Ctx {
  root: string
  core: GatewayCore
  host: CellHost
  proxy: CellProxyServer
  port: number
  manifest: Manifest
  dbId: string
  sessions: HostSession[]
  clients: Client[]
  teardown: () => Promise<void>
}

async function setup(): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-notify-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const host = new CellHost({
    gateway: core,
    dataRoot: join(root, 'host'),
    hostId: 'h1',
  })
  const sessions: HostSession[] = []
  const proxy = new CellProxyServer({
    host,
    port: 0,
    onSession: (s) => sessions.push(s),
  })
  const port = await proxy.start()
  const clients: Client[] = []
  return {
    root,
    core,
    host,
    proxy,
    port,
    manifest,
    dbId: manifest.databaseId,
    sessions,
    clients,
    teardown: async () => {
      for (const c of clients) {
        await c.end().catch(() => undefined)
      }
      await proxy.stop().catch(() => undefined)
      await host.shutdown().catch(() => undefined)
      await core.stop()
      rmSync(root, { recursive: true, force: true })
    },
  }
}

async function connect(ctx: Ctx): Promise<Client> {
  const client = new Client({
    host: '127.0.0.1',
    port: ctx.port,
    database: 'appdb',
    user: 'postgres',
  })
  client.on('error', () => undefined)
  await client.connect()
  ctx.clients.push(client)
  return client
}

/** Collect `notification` events from a pg client. */
function tapNotifications(client: Client): Notification[] {
  const seen: Notification[] = []
  client.on('notification', (n) => seen.push(n))
  return seen
}

/** Wait until `cond` holds (or time out — assertions then report). */
async function waitFor(cond: () => boolean, ms = 5000): Promise<void> {
  const t0 = Date.now()
  while (!cond() && Date.now() - t0 < ms) {
    await new Promise((r) => setTimeout(r, 25))
  }
}

/** The `orders` table + a trigger firing pg_notify('orders', NEW.v). */
async function createOrdersTrigger(c: Client): Promise<void> {
  await c.query(`create table orders (id serial primary key, v text)`)
  await c.query(
    `create function notify_orders() returns trigger as $$
       begin
         perform pg_notify('orders', NEW.v);
         return NEW;
       end $$ language plpgsql`,
  )
  await c.query(
    `create trigger orders_notify after insert on orders
       for each row execute function notify_orders()`,
  )
}

/** Every validated append group of the era stream, oldest first. */
async function streamGroups(ctx: Ctx): Promise<AppendGroup[]> {
  const client = ctx.core.streamClientFor(ctx.dbId)
  const reader = new PositionCheckedReader(ctx.manifest.era.baseOffset)
  const groups: AppendGroup[] = []
  for (;;) {
    const res = await client.read(ctx.manifest.era.path, {
      offset: reader.boundary,
    })
    if (res.bytes.length > 0) {
      for (const g of reader.feed(res.bytes)) groups.push(g)
    }
    if (res.upToDate || res.bytes.length === 0) break
  }
  return groups
}

function newTailer(ctx: Ctx): EraTailer {
  return new EraTailer(ctx.core.streamClientFor(ctx.dbId), {
    path: ctx.manifest.era.path,
    eraId: ctx.manifest.era.id,
    ordinal: ctx.manifest.era.ordinal,
    baseOffset: ctx.manifest.era.baseOffset,
    baseLsn: parseLsn(ctx.manifest.era.baseLsn),
  })
}

/** The stream tail offset (HEAD through the same client the host uses). */
async function streamHead(ctx: Ctx): Promise<string> {
  const head = await ctx.core
    .streamClientFor(ctx.dbId)
    .head(ctx.manifest.era.path)
  return head.nextOffset
}

describe('cluster-wide LISTEN/NOTIFY (M3 §10.2)', () => {
  it(
    '1. NOTIFY core: trigger notify on A is heard by B and by A itself (uniform order); the N frame rides the SAME append group as its W frame; LISTEN writes no WAL / never CAS-es',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await createOrdersTrigger(s)

        const a = await connect(ctx)
        const b = await connect(ctx)
        const aSeen = tapNotifications(a)
        const bSeen = tapNotifications(b)

        // LISTEN is read-only: the stream head must not move (this is the
        // LISTEN-writes-no-WAL probe — an empty capture ⇒ no CAS, ever).
        const head0 = await streamHead(ctx)
        await a.query(`listen orders`)
        await b.query(`listen orders`)
        expect(await streamHead(ctx)).toBe(head0)

        await a.query(`insert into orders (v) values ('o-1')`)
        await waitFor(() => bSeen.length >= 1 && aSeen.length >= 1)

        // B hears exactly one notification with the payload; the COMMITTING
        // connection A hears its own via the same tailer-driven path (and
        // never twice — raw 'A' output is stripped).
        expect(bSeen.map((n) => [n.channel, n.payload])).toEqual([
          ['orders', 'o-1'],
        ])
        expect(aSeen.map((n) => [n.channel, n.payload])).toEqual([
          ['orders', 'o-1'],
        ])

        // Stream shape: exactly ONE N frame, adjacent to its commit's W
        // frame in ONE append group (same expectedOffset — atomic by
        // construction).
        const groups = await streamGroups(ctx)
        const nGroups = groups.filter((g) =>
          g.frames.some((f) => f.type === 'N'),
        )
        expect(nGroups.length).toBe(1)
        const g = nGroups[0]
        const types = g.frames.map((f) => f.type)
        expect(types).toEqual(['W', 'N'])
        const n = g.frames[1]
        if (n.type !== 'N') throw new Error('unreachable')
        expect(n.header.channel).toBe('orders')
        expect(n.header.payload).toBe('o-1')
        const w = g.frames[0]
        if (w.type !== 'W') throw new Error('unreachable')
        expect(n.header.commitId).toBe(w.header.commitId)
        expect(n.header.commitLsn).toBe(w.header.endLsn)
        expect(n.header.expectedOffset).toBe(w.header.expectedOffset)
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '2. exactly-once under race: an insert that loses its first CAS re-executes — exactly ONE N frame in the stream, listeners hear exactly one',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await createOrdersTrigger(s)
        const listener = tapNotifications(s)
        await s.query(`listen orders`)

        const a = await connect(ctx)
        const b = await connect(ctx)
        await b.query(`select count(*) from orders`) // prime B's read cell
        await a.query(`insert into orders (v) values ('a-1')`) // stales B

        const events: UnitObservation[] = []
        ctx.sessions[2]._unitObserver = (ev) => events.push(ev)
        await b.query(`insert into orders (v) values ('b-1')`)

        // The unit executed TWICE (discarded first attempt + re-execution):
        // the losing attempt's harvested notification died with it.
        const attempts = events.filter((e) => e.phase === 'attempt')
        expect(attempts.length).toBe(2)

        await waitFor(() => listener.length >= 2)
        expect(listener.map((n) => n.payload)).toEqual(['a-1', 'b-1'])

        // Exactly one N frame per commit in the stream.
        const groups = await streamGroups(ctx)
        const nHeaders = groups
          .flatMap((g) => g.frames)
          .filter((f) => f.type === 'N')
          .map((f) => f.header as NFrameHeader)
        expect(nHeaders.map((h) => h.payload)).toEqual(['a-1', 'b-1'])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '3. order + activity feed: two alternating writers; a listener receives in stream commit order; a RAW tailer subscriber (zero pg connections) sees the same feed',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await createOrdersTrigger(s)
        const listener = tapNotifications(s)
        await s.query(`listen orders`)

        const a = await connect(ctx)
        const b = await connect(ctx)
        for (let i = 0; i < 3; i++) {
          await a.query(`insert into orders (v) values ('a-${i}')`)
          await b.query(`insert into orders (v) values ('b-${i}')`)
        }
        await waitFor(() => listener.length >= 6)

        // The GUI path: a raw stream tail with no Postgres connection at
        // all — the subscriber callback fires per N frame in stream order.
        const tailer = newTailer(ctx)
        const feed: { payload: string; offset: string }[] = []
        tailer.onNotificationFrame = (header, offset) =>
          feed.push({ payload: header.payload, offset })
        await tailer.catchUp()

        const expected = ['a-0', 'b-0', 'a-1', 'b-1', 'a-2', 'b-2']
        expect(feed.map((f) => f.payload)).toEqual(expected)
        // The pg listener's received order == stream commit order == the
        // tailer's W slice order.
        expect(listener.map((n) => n.payload)).toEqual(expected)
        const commitSlices = tailer.slices.filter((sl) => sl.kind === 'commit')
        // Every notified commit appears in the same relative order as its
        // N frame (offsets are monotone by construction of the feed).
        expect(commitSlices.length).toBeGreaterThanOrEqual(6)
        expect(tailer.notifications.map((n) => n.header.payload)).toEqual(
          expected,
        )
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '4. multi-channel + UNLISTEN: B stops receiving after UNLISTEN; C on another channel is unaffected',
    async () => {
      const ctx = await setup()
      try {
        const s = await connect(ctx)
        await s.query(`create table evts (id serial primary key, v text)`)

        const b = await connect(ctx)
        const c = await connect(ctx)
        const bSeen = tapNotifications(b)
        const cSeen = tapNotifications(c)
        await b.query(`listen ch1`)
        await c.query(`listen ch2`)

        const a = await connect(ctx)
        // A write in the same unit gives the txn a nonempty slice, so the
        // notifications ride N frames (a pure NOTIFY writes no WAL — an
        // empty slice never CASes; documented M3 shape).
        await a.query(
          `insert into evts (v) values ('e1'); select pg_notify('ch1', 'x1'); select pg_notify('ch2', 'y1');`,
        )
        await waitFor(() => bSeen.length >= 1 && cSeen.length >= 1)
        expect(bSeen.map((n) => [n.channel, n.payload])).toEqual([
          ['ch1', 'x1'],
        ])
        expect(cSeen.map((n) => [n.channel, n.payload])).toEqual([
          ['ch2', 'y1'],
        ])

        await b.query(`unlisten ch1`)
        await a.query(
          `insert into evts (v) values ('e2'); select pg_notify('ch1', 'x2'); select pg_notify('ch2', 'y2');`,
        )
        await waitFor(() => cSeen.length >= 2)
        await new Promise((r) => setTimeout(r, 300))

        // B heard nothing more; C heard exactly its channel, in order.
        expect(bSeen.map((n) => n.payload)).toEqual(['x1'])
        expect(cSeen.map((n) => [n.channel, n.payload])).toEqual([
          ['ch2', 'y1'],
          ['ch2', 'y2'],
        ])
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    '5. NOTIFY-only distribution (M4 fix of the M3 gap): a pure NOTIFY txn (empty capture) lands N frames ALONE and is heard by another connection AND by a second CellHost subscriber',
    async () => {
      const ctx = await setup()
      try {
        const b = await connect(ctx)
        const bSeen = tapNotifications(b)
        await b.query(`listen bare`)

        // A second CellHost on the same gateway, subscribing at the
        // runtime level (zero pg connections on that host).
        const host2 = new CellHost({
          gateway: ctx.core,
          dataRoot: join(ctx.root, 'host2'),
          hostId: 'h2',
        })
        const s2 = await host2.connect('appdb')
        const rt2 = host2.runtimeFor(ctx.dbId)!
        const h2Seen: { channel: string; payload: string }[] = []
        rt2.subscribeNotifications((n) =>
          h2Seen.push({ channel: n.channel, payload: n.payload }),
        )

        // Pure NOTIFY: writes no WAL, capture is EMPTY — pre-M4 this was
        // never distributed. Now the N frames CAS-append alone.
        const a = await connect(ctx)
        await a.query(`notify bare, 'hello'`)

        await waitFor(() => bSeen.length >= 1)
        expect(bSeen.map((n) => [n.channel, n.payload])).toEqual([
          ['bare', 'hello'],
        ])

        // Host 2 hears it off ITS tailer once it catches up.
        await rt2.linearizableSync()
        expect(h2Seen).toEqual([{ channel: 'bare', payload: 'hello' }])

        // Stream shape: exactly ONE N frame total (exactly-once). NOTE
        // (M4 finding, contradicting the M3 "pure NOTIFY writes no WAL"
        // comment): a NOTIFY-only transaction DOES publish a W frame in
        // practice — the commit writes a commit record — so the N rides
        // the normal commitSlice path here ([W, N]); the N-frames-ALONE
        // fallback below covers the genuinely-empty-capture case.
        const groups = await streamGroups(ctx)
        const nFrames = groups
          .flatMap((g) => g.frames)
          .filter((f) => f.type === 'N')
        expect(nFrames.length).toBe(1)

        // The N-frames-alone mechanism itself (empty capture + pending
        // notifications): drive it directly and assert a W-less group
        // that still fans out through the tailer everywhere.
        const rt1 = ctx.host.runtimeFor(ctx.dbId)!
        await rt1.publishNotificationOnlyCommit([
          { channel: 'bare', payload: 'alone' },
        ])
        await waitFor(() => bSeen.length >= 2)
        expect(bSeen[1].payload).toBe('alone')
        await rt2.linearizableSync()
        expect(h2Seen[1]).toEqual({ channel: 'bare', payload: 'alone' })
        const groups2 = await streamGroups(ctx)
        const aloneGroups = groups2.filter((g) =>
          g.frames.some((f) => f.type === 'N' && f.header.payload === 'alone'),
        )
        expect(aloneGroups.length).toBe(1)
        expect(aloneGroups[0].frames.map((f) => f.type)).toEqual(['N'])

        await s2.close()
        await host2.shutdown()
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
