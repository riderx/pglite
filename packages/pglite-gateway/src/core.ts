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
  encodeFrame,
  casToken,
  INITIAL_OFFSET_TOKEN,
  formatLsn,
  readControl,
  SHUTDOWN_CKPT_ALIGNED,
} from '@electric-sql/pglite-cell'
import type { OFrame, Frame } from '@electric-sql/pglite-cell'
import { FsObjectStore } from './object-store'
import { packDatadir, packDatadirV3 } from './checkpoint-object'
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
  /** Per-database lifecycle dials (§ M2_PLAN), surfaced for clients/rotators. */
  dials: {
    currentEraOrdinal: number
    /** Bytes as decimal strings (bigint columns; 0 = disabled). */
    checkpointEveryBytes: string
    rotateEveryBytes: string
    gcGraceMs: string
  }
}

export interface GatewayCoreOpts {
  /** Root directory for all backing state (streams, objects, control plane). */
  dataRoot: string
  /**
   * Checkpoint object format `createDatabase` writes: 2 = gzip'd tar (default,
   * M2), 3 = per-file content-addressed + manifest (M7 lazy VFS). The flip to
   * default 3 is a W3/W4 call; createDatabase/registerCheckpoint store the
   * resulting ref opaquely either way.
   */
  checkpointFormat?: 1 | 2 | 3
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
  private readonly checkpointFormat: 1 | 2 | 3
  private _store: FsObjectStore | null = null
  private _controlPlane: ControlPlane | null = null
  private _ds: DurableStreamTestServer | null = null
  private _dsUrl: string | null = null

  constructor(opts: GatewayCoreOpts) {
    this.dataRoot = opts.dataRoot
    // Default to v2 (the conservative, M6-proven checkpoint format). v3
    // (per-file content-addressed + manifest) is the lazy-worker bundle:
    // opt in with `checkpointFormat: 3` alongside `cellMode: 'auto' |
    // 'lazy-worker'`. v3 is fully built + tested (GC is v3-aware) but is not
    // the default until the rotation-quiesce fix lets lazy-worker be the
    // default cell mode (see database-runtime cellMode note). All formats
    // remain explicitly selectable and readable.
    this.checkpointFormat = opts.checkpointFormat ?? 2
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

      // (4) pack + store the checkpoint object. v3 uploads per-file objects +
      // a manifest itself; v1/v2 upload the single archive blob. The ref is
      // stored opaquely in the control plane either way.
      if (this.checkpointFormat === 3) {
        checkpointRef = (await packDatadirV3(scratch, this.store)).manifestRef
      } else {
        const packed = await packDatadir(scratch, {
          format: this.checkpointFormat,
        })
        checkpointRef = (await this.store.put(packed)).ref
      }
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
    return this.getManifest(databaseId)
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

  /**
   * Operator stats for a database's LATEST checkpoint. When that checkpoint's
   * ref resolves to a v3 manifest, reports the eager/lazy byte split and file
   * count (summed from the manifest by kind) — the numbers the console's
   * wake-byte counter needs (eager bytes = what a cold wake moves). When the
   * latest checkpoint is a v1/v2 archive (or there is none), the fields are
   * omitted (undefined) and only the shape is returned.
   */
  async dbStats(databaseId: string): Promise<{
    latestCheckpoint: {
      eagerBytes?: number
      lazyBytes?: number
      fileCount?: number
    }
  }> {
    const latest = await this.controlPlane.latestCheckpoint(databaseId)
    if (!latest) return { latestCheckpoint: {} }
    const { readCheckpointManifest } = await import('./checkpoint-object')
    let manifest
    try {
      manifest = await readCheckpointManifest(
        latest.objectRef,
        this.objectGetStore,
      )
    } catch {
      // v1/v2 archive blob, or object absent — no per-kind stats available.
      return { latestCheckpoint: {} }
    }
    let eagerBytes = 0
    let lazyBytes = 0
    for (const f of manifest.files) {
      if (f.kind === 'lazy') lazyBytes += f.size
      else eagerBytes += f.size
    }
    return {
      latestCheckpoint: {
        eagerBytes,
        lazyBytes,
        fileCount: manifest.files.length,
      },
    }
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
      dials: {
        currentEraOrdinal: db.currentEraOrdinal,
        checkpointEveryBytes: db.checkpointEveryBytes,
        rotateEveryBytes: db.rotateEveryBytes,
        gcGraceMs: db.gcGraceMs,
      },
    }
  }

  /** Update any subset of a database's dials. */
  async setDials(
    databaseId: string,
    dials: import('./control-plane').Dials,
  ): Promise<void> {
    await this.controlPlane.setDials(databaseId, dials)
  }

  // --- Era rotation primitives (thin pass-throughs for the cell-server
  // rotator; steps 0/3/5/6). Kept stateless — the rotator drives them. ----

  async registerEraAttempt(
    databaseId: string,
    input: { ordinal: number; eraId: string; path: string },
  ): Promise<void> {
    await this.controlPlane.registerEraAttempt({ databaseId, ...input })
  }

  async promoteEraAttempt(databaseId: string, eraId: string): Promise<void> {
    await this.controlPlane.promoteEraAttempt(databaseId, eraId)
  }

  async sealEra(
    databaseId: string,
    ordinal: number,
    seal: { finalOffset: string; finalLsn: string; nextOrdinal: number },
  ): Promise<void> {
    await this.controlPlane.sealEra(databaseId, ordinal, seal)
  }

  async advanceCurrentEra(
    databaseId: string,
    from: number,
    to: number,
  ): Promise<boolean> {
    return this.controlPlane.advanceCurrentEra(databaseId, from, to)
  }

  /**
   * Insert the era-N row (rotation step 6). Thin pass-through onto the control
   * plane's guarded (database, ordinal) insert.
   */
  async addEra(
    databaseId: string,
    input: {
      ordinal: number
      eraId: string
      path: string
      baseOffset: string
      baseLsn: string
    },
  ): Promise<void> {
    await this.controlPlane.addEra({ databaseId, ...input })
  }

  /** Read an era row by (database, ordinal), or null (rotation repair-walk). */
  async eraByOrdinal(
    databaseId: string,
    ordinal: number,
  ): Promise<import('./control-plane').EraRow | null> {
    return this.controlPlane.eraByOrdinal(databaseId, ordinal)
  }

  // --- Pins (control-plane mirror of L{gc-pin} frames; §6.4). Thin
  // pass-throughs; the rotator/GC mirror pins here. -----------------------

  async upsertPin(
    databaseId: string,
    pin: {
      id: string
      kind: string
      holder: string
      pinnedOffset: string
      pinnedLsn: string
      expiresAt: Date | string
    },
  ): Promise<void> {
    await this.controlPlane.upsertPin({ databaseId, ...pin })
  }

  async deletePin(pinId: string): Promise<void> {
    await this.controlPlane.deletePin(pinId)
  }

  /** Delete expired pins; returns how many were swept (TTL sweep). */
  async expirePins(): Promise<number> {
    return this.controlPlane.expirePins()
  }

  /** Live (unexpired) pins for a database. */
  async livePins(
    databaseId: string,
  ): Promise<import('./control-plane').PinRow[]> {
    return this.controlPlane.livePins(databaseId)
  }

  /** The control plane (GC and rotator internals reach through this). */
  get catalog(): ControlPlane {
    return this.controlPlane
  }

  /**
   * A DsStreamClient scoped to a database's mount (exposed for GC's stream
   * deletion — same base as `streamClientFor`). Alias kept explicit for intent.
   */
  streamClientForGc(databaseId: string): DsStreamClient {
    return this.streamClientFor(databaseId)
  }

  /** Run garbage collection (see GcExecutor). Optionally scoped to one db. */
  async runGc(databaseId?: string): Promise<import('./gc').GcReport> {
    const { GcExecutor } = await import('./gc')
    return new GcExecutor(this).run(databaseId)
  }

  /**
   * Fork `parentId` into a new database `name` at the parent's LATEST
   * checkpoint position (M2 restriction — arbitrary-LSN forks arrive with M3).
   * The fork point is that checkpoint's `(snapEnd, streamOffset)`.
   *
   * Steps (M2_PLAN "Forks"):
   *  1. Control-plane txn: child databases row + lineage edge + child era row
   *     (keeps the PARENT's current ordinal, gets a NEW era_id) + child
   *     checkpoint row pointing at the PARENT's checkpoint OBJECT (shared,
   *     content-addressed).
   *  2. Stream-fork PUT of the parent era to the child path at `fork_offset`
   *     (Stream-Forked-From / Stream-Fork-Offset — VERIFIED against the DS
   *     server: offset is a range-checked token, prefix is shared by
   *     reference, reads continue from the fork point).
   *  3. CAS-append an `F` frame to the PARENT era (in-band announcement + GC
   *     pin signal), via a plain seq-token append at the parent's head.
   */
  async forkDatabase(parentId: string, name: string): Promise<Manifest> {
    const parent = await this.controlPlane.getDatabaseById(parentId)
    if (!parent) throw new Error(`parent database not found: ${parentId}`)
    const parentEra = await this.controlPlane.currentEra(parentId)
    if (!parentEra) throw new Error(`parent has no era: ${parentId}`)
    const parentCkpt = await this.controlPlane.latestCheckpoint(parentId)
    if (!parentCkpt) throw new Error(`parent has no checkpoint: ${parentId}`)

    // Fork point = parent's latest checkpoint position.
    const forkOffset = parentCkpt.streamOffset
    const forkLsn = parentCkpt.snapEnd
    const ordinal = parentEra.ordinal

    // (1) Control-plane rows. The child era keeps the parent's ordinal (so W3
    // offset tokens stay monotone across the shared prefix) but a fresh era_id
    // names the child's own writes.
    const childId = await this.controlPlane.createDatabase(name)
    const childEraId = `${String(ordinal).padStart(6, '0')}-${sortableId()}`
    const childStreamPath = `/era/${childEraId}`
    await this.controlPlane.insertLineage({
      childId,
      parentId,
      forkLsn,
      forkOffset,
    })
    await this.controlPlane.addEra({
      databaseId: childId,
      ordinal,
      eraId: childEraId,
      path: childStreamPath,
      baseOffset: forkOffset,
      baseLsn: forkLsn,
    })
    await this.controlPlane.registerCheckpoint({
      databaseId: childId,
      lsn: parentCkpt.lsn,
      snapEnd: parentCkpt.snapEnd,
      streamOffset: forkOffset,
      objectRef: parentCkpt.objectRef, // SHARED content-addressed object
    })

    // (2) Stream-fork PUT: the child era stream is a fork of the parent era at
    // `forkOffset`. `Stream-Forked-From` is the parent era's path AS THE DS
    // SERVER KNOWS IT (its full mount-qualified path), not the child-relative
    // path. The DS server shares the prefix by reference and continues the
    // fork's offset space from the fork point.
    const parentDsPath = this.streamMountPath(parentId) + parentEra.path
    const childUrl =
      this.dsUrl + this.streamMountPath(childId) + childStreamPath
    const putRes = await fetch(childUrl, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/octet-stream',
        'Stream-Forked-From': parentDsPath,
        'Stream-Fork-Offset': forkOffset,
      },
    })
    if (!putRes.ok && putRes.status !== 200) {
      const body = await putRes.text().catch(() => '')
      throw new Error(
        `fork PUT failed: HTTP ${putRes.status} ${body} (parent=${parentDsPath} offset=${forkOffset})`,
      )
    }

    // (3) CAS-append an F frame to the PARENT era announcing the fork. The
    // gateway has no committer, so we do a plain seq-token append at the
    // parent's current head, retrying a bounded number of times on seq
    // conflict (a concurrent commit moved the head).
    await this.appendForkFrame(
      parentId,
      parentEra.eraId,
      parentEra.path,
      ordinal,
      {
        childDatabaseId: childId,
        forkOffset,
        forkLsn,
      },
    )

    return this.getManifest(childId)
  }

  /**
   * CAS-append an `F` frame to a parent era at its current head. Bounded retry
   * on seq-conflict (a commit raced in and moved the head). Uses a seq token
   * built from the era ordinal + observed head, matching the W3 CAS convention.
   */
  private async appendForkFrame(
    parentId: string,
    parentEraId: string,
    parentEraPath: string,
    parentOrdinal: number,
    info: { childDatabaseId: string; forkOffset: string; forkLsn: string },
  ): Promise<void> {
    const client = this.streamClientFor(parentId)
    const maxAttempts = 8
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const head = await client.head(parentEraPath)
      const fFrame: Frame = {
        type: 'F',
        header: {
          v: 1,
          eraId: parentEraId,
          expectedOffset: head.nextOffset,
          childDatabaseId: info.childDatabaseId,
          forkOffset: info.forkOffset,
          forkLsn: info.forkLsn,
        },
      }
      const body = encodeFrame(fFrame)
      const res = await client.append(parentEraPath, body, {
        seq: casToken(parentOrdinal, head.nextOffset),
        expectedOffset: head.nextOffset,
      })
      if (res.kind === 'ok') return
      if (res.kind === 'seq-conflict') continue // head moved; re-read and retry
      throw new Error(
        `fork F-frame append rejected: ${res.kind} (parent era ${parentEraId})`,
      )
    }
    throw new Error(
      `fork F-frame append exhausted retries on parent era ${parentEraId}`,
    )
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

  /**
   * An `ObjectGetStore` handle over the object store (fetch-by-ref only). GC
   * uses this to resolve v3 checkpoint manifests when expanding the live-object
   * set; also the read side any manifest-aware consumer needs.
   */
  get objectGetStore(): import('./checkpoint-object').ObjectGetStore {
    return { get: (ref: string) => this.store.get(ref) }
  }

  /**
   * Ranged object read (`length` bytes from `offset`) — the lazy VFS host
   * cache's per-chunk fetch (§decision 3). Positional fd read, no full load.
   */
  async getObjectRange(
    ref: string,
    offset: number,
    length: number,
  ): Promise<Uint8Array> {
    return this.store.getRange(ref, offset, length)
  }

  /** Store bytes; returns the content-address ref (idempotent). */
  async putObject(bytes: Uint8Array): Promise<{ ref: string }> {
    return this.store.put(bytes)
  }

  /** Delete an object (GC of unreferenced checkpoint objects). */
  async deleteObject(ref: string): Promise<boolean> {
    return this.store.delete(ref)
  }

  /** List all stored object refs. */
  async listObjects(): Promise<string[]> {
    return this.store.list()
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
