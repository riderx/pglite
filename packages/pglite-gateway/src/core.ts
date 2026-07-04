// GatewayCore — the embeddable deployment of the §14.5 gateway. It owns:
//   - a content-addressed object store (checkpoints, spilled slices),
//   - the control plane (an embedded PGlite catalog, §14.6),
//   - an embedded Durable Streams test server (the dev DS backend).
//
// It is authoritative-state-free ITSELF: everything durable lives in the three
// components above. `createDatabase` productizes the commit-engine test's
// era-0 fixture: initdb -> settling boot (M1a finding) -> checkpoint 0 ->
// era-1 PUT with the O frame -> control-plane rows -> manifest.

import { mkdirSync, rmSync, mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomBytes } from 'node:crypto'
import { DurableStreamTestServer } from '@durable-streams/server'
import { PGlite } from '@electric-sql/pglite'
import {
  DsStreamClient,
  encodeAppend,
  INITIAL_OFFSET_TOKEN,
  formatLsn,
  readControl,
  SHUTDOWN_CKPT_ALIGNED,
} from '@electric-sql/pglite-cell'
import type { OFrame } from '@electric-sql/pglite-cell'
import { FsObjectStore } from './object-store'
import { packDatadir } from './checkpoint-object'
import { ControlPlane } from './control-plane'

/**
 * The manifest a client needs to attach: which era stream to tail (path
 * RELATIVE to the per-database stream mount, so clients join it onto their
 * stream base URL) and which checkpoint to hydrate.
 */
export interface Manifest {
  databaseId: string
  name: string
  era: {
    id: string
    ordinal: number
    /**
     * Path relative to the per-database stream mount, with a LEADING slash
     * (e.g. "/era/000001-xxxx"), so a DsStreamClient built with base
     * `<gatewayUrl>/v1/db/<id>/stream` resolves it by raw concatenation
     * (pglite-cell's client convention).
     */
    path: string
    baseOffset: string
    /** pg_lsn text. */
    baseLsn: string
  }
  checkpoint: {
    ref: string
    /** pg_lsn text of the checkpoint (C0). */
    lsn: string
    /** pg_lsn text of the snapshot end (the attach point). */
    snapEnd: string
    streamOffset: string
  }
}

export interface GatewayCoreOpts {
  /** Root directory for all backing state (streams, objects, control plane). */
  dataRoot: string
}

/**
 * A tiny sortable id (ULID-shaped, no dependency): 10 hex chars of the
 * millisecond timestamp + 12 hex chars of randomness, uppercased. Monotone by
 * time, unique per attempt — the era-URL uniqueness the design's PUT-per-attempt
 * rule (§2.4) needs.
 */
function sortableId(): string {
  const ts = Date.now().toString(16).padStart(10, '0')
  const rand = randomBytes(6).toString('hex')
  return (ts + rand).toUpperCase()
}

/**
 * The embedded gateway deployment. Call `start()` before use, `stop()` after.
 * Holds NO authoritative mutable state of its own (§14.5): the object store,
 * control plane, and DS server hold everything, and two GatewayCores over the
 * same backing dirs are interchangeable.
 */
export class GatewayCore {
  private readonly dataRoot: string
  private _store: FsObjectStore | null = null
  private _controlPlane: ControlPlane | null = null
  private _ds: DurableStreamTestServer | null = null
  private _dsUrl: string | null = null

  constructor(opts: GatewayCoreOpts) {
    this.dataRoot = opts.dataRoot
  }

  /** The embedded Durable Streams base URL (valid after `start()`). */
  get dsUrl(): string {
    if (this._dsUrl === null) throw new Error('GatewayCore not started')
    return this._dsUrl
  }

  private get store(): FsObjectStore {
    if (!this._store) throw new Error('GatewayCore not started')
    return this._store
  }

  private get controlPlane(): ControlPlane {
    if (!this._controlPlane) throw new Error('GatewayCore not started')
    return this._controlPlane
  }

  /** Start the object store, control plane, and embedded DS server. */
  async start(): Promise<void> {
    mkdirSync(this.dataRoot, { recursive: true })
    this._store = new FsObjectStore(join(this.dataRoot, 'objects'))
    this._controlPlane = await ControlPlane.create(
      join(this.dataRoot, 'control-plane'),
    )
    const streamsDir = join(this.dataRoot, 'streams')
    mkdirSync(streamsDir, { recursive: true })
    this._ds = new DurableStreamTestServer({
      port: 0,
      dataDir: streamsDir,
      longPollTimeout: 1000,
    })
    this._dsUrl = await this._ds.start()
  }

  /** Stop the embedded DS server and close the control plane. */
  async stop(): Promise<void> {
    await this._ds?.stop()
    await this._controlPlane?.close()
    this._ds = null
    this._dsUrl = null
    this._controlPlane = null
    this._store = null
  }

  /** Per-database stream mount prefix (the DS path prefix). */
  private streamMountPath(databaseId: string): string {
    return `/pgl/${databaseId}`
  }

  /**
   * A DsStreamClient whose baseUrl targets the embedded DS server scoped to
   * this database's mount. Manifest era paths (relative, e.g.
   * "era/000001-xxxx") join onto this base directly. This is the in-process
   * attach shape (Supabase-lite embedding).
   */
  streamClientFor(databaseId: string): DsStreamClient {
    return new DsStreamClient(this.dsUrl + this.streamMountPath(databaseId))
  }

  /**
   * Create a database end to end (M1_PLAN createDatabase recipe):
   *   1. initdb a scratch PGlite with --no-data-checksums; close;
   *   2. settling boot (open + select 1 + close) — the M1a finding;
   *   3. readControl => C0; snapEnd = C0 + 120;
   *   4. pack the settled datadir => object store => checkpointRef;
   *   5. PUT-create the era-1 stream with the O frame as the body;
   *   6. control-plane rows (databases, eras, checkpoints);
   *   7. return the manifest.
   */
  async createDatabase(name: string): Promise<Manifest> {
    const databaseId = await this.controlPlane.createDatabase(name)

    const scratch = mkdtempSync(join(tmpdir(), 'pgl-gw-initdb-'))
    let snapEnd: bigint
    let checkpointC0: bigint
    let checkpointRef: string
    try {
      // (1) initdb (no data checksums) + close.
      const initdb = new PGlite(scratch, {
        initDbStartParams: ['--no-data-checksums'],
      })
      await initdb.exec(`select 1`)
      await initdb.close()

      // (2) settling boot: the FIRST reopen after initdb writes ~8 KB of
      // bootstrap WAL even with checksums off; reopens 2+ are WAL-silent, so a
      // checkpoint object must be cut from a dir >= 1 reopen past initdb or its
      // snapEnd is not a valid zero-boot-WAL attach point (M1a finding).
      const settle = new PGlite(scratch)
      await settle.query(`select 1`)
      await settle.close()

      // (3) checkpoint 0 coordinates.
      checkpointC0 = readControl(scratch).checkPoint
      snapEnd = checkpointC0 + BigInt(SHUTDOWN_CKPT_ALIGNED)

      // (4) pack + store the checkpoint object.
      const packed = await packDatadir(scratch)
      checkpointRef = (await this.store.put(packed)).ref
    } finally {
      rmSync(scratch, { recursive: true, force: true })
    }

    // (5) era-1 stream: unique per-attempt URL; O frame rides in the PUT body.
    // The path is relative to the per-database stream mount and carries a
    // LEADING slash so a DsStreamClient (which raw-concatenates base + path,
    // matching pglite-cell's convention) resolves it against its base URL.
    const ordinal = 1
    const eraId = `${String(ordinal).padStart(6, '0')}-${sortableId()}`
    const streamPath = `/era/${eraId}`
    const relPath = streamPath
    const snapEndLsn = formatLsn(snapEnd)
    const oFrame: OFrame = {
      type: 'O',
      header: {
        v: 1,
        eraId,
        expectedOffset: INITIAL_OFFSET_TOKEN,
        ordinal,
        prevEraId: null,
        prevEraUrl: null,
        baseOffset: INITIAL_OFFSET_TOKEN,
        baseLsn: snapEndLsn,
        snapEnd: snapEndLsn,
        checkpointRef,
      },
    }
    const client = this.streamClientFor(databaseId)
    const created = await client.createStream(streamPath, {
      body: encodeAppend([oFrame]),
    })
    const eraBaseOffset = created.nextOffset

    // (6) control-plane rows.
    await this.controlPlane.addEra({
      databaseId,
      ordinal,
      eraId,
      path: relPath,
      baseOffset: eraBaseOffset,
      baseLsn: snapEndLsn,
    })
    await this.controlPlane.registerCheckpoint({
      databaseId,
      lsn: formatLsn(checkpointC0),
      snapEnd: snapEndLsn,
      streamOffset: eraBaseOffset,
      objectRef: checkpointRef,
    })

    // (7) manifest.
    return this.buildManifest(databaseId, name, {
      eraId,
      ordinal,
      relPath,
      eraBaseOffset,
      snapEndLsn,
      checkpointRef,
      checkpointLsn: formatLsn(checkpointC0),
      streamOffset: eraBaseOffset,
    })
  }

  /**
   * The latest (highest-LSN) checkpoint row for a database, or null. Used by
   * the M1e checkpoint worker to decide idempotency (skip when a checkpoint
   * already exists at the current canonical position). Additive accessor
   * over the control plane's `latestCheckpoint`.
   */
  async latestCheckpoint(databaseId: string): Promise<{
    lsn: string
    snapEnd: string
    streamOffset: string
    objectRef: string
  } | null> {
    return this.controlPlane.latestCheckpoint(databaseId)
  }

  /** Read a database's current manifest from the control plane. */
  async getManifest(databaseId: string): Promise<Manifest> {
    const db = await this.controlPlane.getDatabaseById(databaseId)
    if (!db) throw new Error(`database not found: ${databaseId}`)
    const era = await this.controlPlane.currentEra(databaseId)
    if (!era) throw new Error(`database has no era: ${databaseId}`)
    const checkpoint = await this.controlPlane.latestCheckpoint(databaseId)
    if (!checkpoint)
      throw new Error(`database has no checkpoint: ${databaseId}`)
    return {
      databaseId,
      name: db.name,
      era: {
        id: era.eraId,
        ordinal: era.ordinal,
        path: era.path,
        baseOffset: era.baseOffset,
        baseLsn: era.baseLsn,
      },
      checkpoint: {
        ref: checkpoint.objectRef,
        lsn: checkpoint.lsn,
        snapEnd: checkpoint.snapEnd,
        streamOffset: checkpoint.streamOffset,
      },
    }
  }

  private buildManifest(
    databaseId: string,
    name: string,
    parts: {
      eraId: string
      ordinal: number
      relPath: string
      eraBaseOffset: string
      snapEndLsn: string
      checkpointRef: string
      checkpointLsn: string
      streamOffset: string
    },
  ): Manifest {
    return {
      databaseId,
      name,
      era: {
        id: parts.eraId,
        ordinal: parts.ordinal,
        path: parts.relPath,
        baseOffset: parts.eraBaseOffset,
        baseLsn: parts.snapEndLsn,
      },
      checkpoint: {
        ref: parts.checkpointRef,
        lsn: parts.checkpointLsn,
        snapEnd: parts.snapEndLsn,
        streamOffset: parts.streamOffset,
      },
    }
  }

  /**
   * Register a checkpoint produced by a checkpoint worker (M1e). Guarded on
   * (database, lsn). Lifecycle-rate; never in the commit path.
   */
  async registerCheckpoint(
    databaseId: string,
    input: {
      lsn: string
      snapEnd: string
      streamOffset: string
      objectRef: string
    },
  ): Promise<void> {
    await this.controlPlane.registerCheckpoint({ databaseId, ...input })
  }

  async listDatabases(): Promise<
    { id: string; name: string; status: string }[]
  > {
    return this.controlPlane.listDatabases()
  }

  /** Fetch an object by content-address ref. */
  async getObject(ref: string): Promise<Uint8Array> {
    return this.store.get(ref)
  }

  /** Store bytes; returns the content-address ref (idempotent). */
  async putObject(bytes: Uint8Array): Promise<{ ref: string }> {
    return this.store.put(bytes)
  }

  /** Whether an object is present. */
  async hasObject(ref: string): Promise<boolean> {
    return this.store.has(ref)
  }

  /**
   * Proxy target for the HTTP stream endpoint: the absolute DS-server URL a
   * per-database stream request `rest` (path + optional query) maps onto —
   * `<dsUrl>/pgl/<id>/<rest>`.
   */
  streamUpstreamUrl(databaseId: string, rest: string): string {
    const clean = rest.replace(/^\//, '')
    return this.dsUrl + this.streamMountPath(databaseId) + '/' + clean
  }
}
