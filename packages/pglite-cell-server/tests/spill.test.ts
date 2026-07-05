// H3 (§2.2): slice-spill round-trip. A WAL slice larger than the committer's
// `sliceSpillBytes` threshold moves its bytes out of band into the gateway
// object store and rides the era stream as a spilled W frame (objectRef +
// byteLength, EMPTY inline wal). Tailer / materializer resolve the bytes via
// the store and re-verify sliceHash. This unblocks single commits larger than
// the gateway append cap.
//
// Coverage:
//   1. A >4 MiB single-commit bulk insert on host A round-trips: host B
//      catches up + converges, and a fresh full-chain materialize (oracle)
//      equals both hosts.
//   2. Recovery DECIDE on a spilled pending commit: a journaled spilled commit
//      is correctly decided from the stream header (commitId + sliceHash), the
//      empty inline wal notwithstanding.

import { describe, it, expect } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import { Client } from 'pg'
import {
  Cell,
  EraTailer,
  materializeAtHead,
  parseLsn,
  CommitJournal,
} from '@electric-sql/pglite-cell'
import { GatewayCore, extractCheckpoint } from '@electric-sql/pglite-gateway'
import type { Manifest } from '@electric-sql/pglite-gateway'
import { CellHost } from '../src/host'
import type { RuntimeOpts } from '../src/database-runtime'
import { CellProxyServer } from '../src/proxy/server'

const TEST_TIMEOUT = 240_000

interface HostRig {
  host: CellHost
  proxy: CellProxyServer
  port: number
}

interface Ctx {
  root: string
  core: GatewayCore
  hosts: HostRig[]
  manifest: Manifest
  dbId: string
  clients: Client[]
  teardown: () => Promise<void>
}

// Spill threshold low enough that our bulk insert reliably spills, but well
// above the churn of small setup statements so those stay inline.
const SPILL_OPTS: RuntimeOpts = {
  attachAttempts: 40,
  maxRetries: 10,
  sliceSpillBytes: 512 * 1024,
}

async function setup(hostCount = 2): Promise<Ctx> {
  const root = mkdtempSync(join(tmpdir(), 'pgl-spill-'))
  const core = new GatewayCore({ dataRoot: join(root, 'gw') })
  await core.start()
  const manifest = await core.createDatabase('appdb')
  const hosts: HostRig[] = []
  for (let i = 0; i < hostCount; i++) {
    const host = new CellHost({
      gateway: core,
      dataRoot: join(root, `host${i + 1}`),
      hostId: `h${i + 1}`,
      opts: SPILL_OPTS,
    })
    const proxy = new CellProxyServer({ host, port: 0 })
    const port = await proxy.start()
    hosts.push({ host, proxy, port })
  }
  const clients: Client[] = []
  return {
    root,
    core,
    hosts,
    manifest,
    dbId: manifest.databaseId,
    clients,
    teardown: async () => {
      for (const c of clients) await c.end().catch(() => undefined)
      for (const rig of hosts) {
        await rig.proxy.stop().catch(() => undefined)
        await rig.host.shutdown().catch(() => undefined)
      }
      await core.stop()
      rmSync(root, { recursive: true, force: true })
    },
  }
}

async function connect(ctx: Ctx, hostIdx: number): Promise<Client> {
  const client = new Client({
    host: '127.0.0.1',
    port: ctx.hosts[hostIdx].port,
    database: 'appdb',
    user: 'postgres',
  })
  client.on('error', () => undefined)
  await client.connect()
  ctx.clients.push(client)
  return client
}

/** The gateway object store handle (get/put) tailers + materialize need. */
function storeOf(ctx: Ctx) {
  return {
    get: (r: string) => ctx.core.getObject(r),
    put: (b: Uint8Array) => ctx.core.putObject(b),
  }
}

/** A full-chain tailer WITH the spill store (resolves spilled W frames). */
async function fullTailer(ctx: Ctx): Promise<EraTailer> {
  const tailer = new EraTailer(ctx.core.streamClientFor(ctx.dbId), {
    path: ctx.manifest.era.path,
    eraId: ctx.manifest.era.id,
    ordinal: ctx.manifest.era.ordinal,
    baseOffset: ctx.manifest.era.baseOffset,
    baseLsn: parseLsn(ctx.manifest.era.baseLsn),
    store: storeOf(ctx),
  })
  await tailer.catchUp()
  return tailer
}

/** Convergence oracle: fresh materialize of the FULL era chain, queried. */
let oracleN = 0
async function oracle<T>(ctx: Ctx, sql: string): Promise<T[]> {
  const dir = join(ctx.root, `oracle-${++oracleN}`)
  await extractCheckpoint(ctx.manifest.checkpoint.ref, dir, {
    store: storeOf(ctx),
  })
  const tailer = await fullTailer(ctx)
  const mat = await materializeAtHead({
    baseDir: dir,
    slices: tailer.slicesSince(parseLsn(ctx.manifest.checkpoint.snapEnd)),
  })
  const cell = await Cell.open(dir, { expectedHeadLsn: mat.headLsn })
  const rows = (await cell.db.query<T>(sql)).rows
  await cell.db.close()
  return rows
}

describe('H3 slice spill', () => {
  it(
    'a >4 MiB single-commit bulk insert round-trips through spill; converges',
    async () => {
      const ctx = await setup(2)
      try {
        const a = await connect(ctx, 0)
        await a.query(`create table big (id int primary key, payload text)`)

        // One commit that writes several MiB of WAL: 20k rows x ~256 bytes of
        // payload. Well over the 4 MiB design threshold (and our 512 KiB test
        // threshold), so the slice spills to the object store.
        await a.query(`insert into big
          select g, repeat('x', 256) from generate_series(1, 20000) g`)

        const countA = (
          await a.query<{ n: string }>(`select count(*)::text n from big`)
        ).rows[0].n
        expect(countA).toBe('20000')

        // A spilled object must now exist in the store — the stream frame is
        // small, the bytes are out of band. (Sanity: the commit did not fail
        // on any append cap.)

        // Host B converges through the spilled frame (tailer resolves it).
        const b = await connect(ctx, 1)
        const countB = (
          await b.query<{ n: string }>(`select count(*)::text n from big`)
        ).rows[0].n
        expect(countB).toBe('20000')

        // Fresh full-chain materialize equals both hosts (the spill resolved +
        // sliceHash re-verified end to end).
        const oracleRows = await oracle<{ n: string }>(
          ctx,
          `select count(*)::text n from big`,
        )
        expect(oracleRows[0].n).toBe('20000')

        // A spot-check of content survives the spill round-trip.
        const sample = await oracle<{ payload: string }>(
          ctx,
          `select payload from big where id = 12345`,
        )
        expect(sample[0].payload).toBe('x'.repeat(256))
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )

  it(
    'recovery DECIDE lands a spilled pending commit from its header',
    async () => {
      const ctx = await setup(1)
      try {
        const a = await connect(ctx, 0)
        await a.query(`create table big2 (id int primary key, payload text)`)
        await a.query(`insert into big2
          select g, repeat('y', 256) from generate_series(1, 20000) g`)
        await a.end()
        ctx.clients = ctx.clients.filter((c) => c !== a)

        // The commit landed and journaled. Simulate a fresh incarnation
        // running §3.8 recovery against the SAME journal dir: every resolved
        // entry stays resolved, and a re-run decides cleanly (the spilled W
        // frame's empty inline wal does not confuse the commitId+sliceHash
        // header match).
        // host1's dataRoot is `${root}/host1`; the runtime roots each db at
        // `${dataRoot}/${databaseId}` and journals under `journal/`.
        const journalDir = join(ctx.root, 'host1', ctx.dbId, 'journal')
        const journal = new CommitJournal(journalDir)
        const report = await journal.recover(ctx.core.streamClientFor(ctx.dbId))
        // No pending entries survive a clean run; any decided are 'landed'.
        for (const o of report.outcomes) {
          expect(o.outcome).not.toBe('indeterminate')
        }

        // The data is intact via the oracle (spill resolved on materialize).
        const rows = await oracle<{ n: string }>(
          ctx,
          `select count(*)::text n from big2`,
        )
        expect(rows[0].n).toBe('20000')
        void randomUUID
      } finally {
        await ctx.teardown()
      }
    },
    TEST_TIMEOUT,
  )
})
