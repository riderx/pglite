// The host's view of the gateway (§14.5): manifest reads, checkpoint-object
// fetches, and per-database stream clients. Two shapes, one interface — an
// in-process GatewayCore (the Supabase-lite / test embedding) or an HTTP
// gateway URL (the fleet shape). Cell hosts speak ONLY to the gateway; the
// DS server and object store stay behind it.

import { DsStreamClient } from '@electric-sql/pglite-cell'
import type { GatewayCore, Manifest } from '@electric-sql/pglite-gateway'

/** Constructor target: an embedded core or a remote gateway base URL. */
export type GatewayTarget = GatewayCore | { url: string }

/** A checkpoint control-plane row (M1e). */
export interface CheckpointInfo {
  lsn: string
  snapEnd: string
  streamOffset: string
  objectRef: string
}

/** An era control-plane row as the rotator consumes it (M2). */
export interface EraInfo {
  ordinal: number
  eraId: string
  path: string
  baseOffset: string
  baseLsn: string
  sealed: boolean
  sealedFinalOffset: string | null
  sealedFinalLsn: string | null
  nextEraOrdinal: number | null
}

/** Input for registering an era attempt (rotation step 3, pre-PUT). */
export interface EraAttemptInput {
  ordinal: number
  eraId: string
  path: string
}

/** Input for inserting an era row (rotation step 6). */
export interface AddEraInfo {
  ordinal: number
  eraId: string
  path: string
  baseOffset: string
  baseLsn: string
}

/** Terminal-S coordinates recorded on the sealed era row (step 6). */
export interface SealEraRowInput {
  finalOffset: string
  finalLsn: string
  nextOrdinal: number
}

/** A control-plane pin row (the queryable mirror of L{gc-pin} frames). */
export interface PinInput {
  id: string
  kind: string
  holder: string
  pinnedOffset: string
  pinnedLsn: string
  expiresAt: Date
}

/** The narrow gateway surface the host consumes. */
export interface GatewayHandle {
  /** Resolve a database name OR id to its id (names are control-plane rows). */
  resolveDatabaseId(nameOrId: string): Promise<string>
  getManifest(databaseId: string): Promise<Manifest>
  getObject(ref: string): Promise<Uint8Array>
  /** Store bytes; returns the content-address ref (M1e checkpoint object). */
  putObject(bytes: Uint8Array): Promise<{ ref: string }>
  /** The latest (highest-LSN) checkpoint row for a database, or null. */
  latestCheckpoint(databaseId: string): Promise<CheckpointInfo | null>
  /** Register a checkpoint (control-plane row; guarded on (database, lsn)). */
  registerCheckpoint(databaseId: string, input: CheckpointInfo): Promise<void>
  /**
   * A DsStreamClient whose base URL is the database's stream mount, so
   * manifest era paths (leading slash, M1b finding) join by concatenation.
   */
  streamClientFor(databaseId: string): DsStreamClient

  // --- Era rotation primitives (M2c rotator, §6.1 steps 0/3/6) ----------
  /** Register an era attempt BEFORE the stream PUT (GC orphan registry). */
  registerEraAttempt(databaseId: string, input: EraAttemptInput): Promise<void>
  /** Mark an era attempt promoted (it manifested as a real era). */
  promoteEraAttempt(databaseId: string, eraId: string): Promise<void>
  /** Record the terminal-S coordinates on the sealed era's row. */
  sealEraRow(
    databaseId: string,
    ordinal: number,
    seal: SealEraRowInput,
  ): Promise<void>
  /** Guarded current_era_ordinal advance; false ⇒ someone else advanced. */
  advanceCurrentEra(
    databaseId: string,
    from: number,
    to: number,
  ): Promise<boolean>
  /** Insert the era N+1 row (guarded on (database, ordinal)). */
  addEra(databaseId: string, input: AddEraInfo): Promise<void>
  /** Read an era row by ordinal, or null. */
  eraByOrdinal(databaseId: string, ordinal: number): Promise<EraInfo | null>

  // --- Pins (control-plane mirror of L{gc-pin} frames; §6.4) ------------
  upsertPin(databaseId: string, pin: PinInput): Promise<void>
  deletePin(pinId: string): Promise<void>

  // --- GC (§6.4; the M6 janitor schedules it) ---------------------------
  /** Run the gateway GC sweeps scoped to one database. */
  runGc(databaseId: string): Promise<void>
}

class InProcessGatewayHandle implements GatewayHandle {
  constructor(private readonly core: GatewayCore) {}

  async resolveDatabaseId(nameOrId: string): Promise<string> {
    const dbs = await this.core.listDatabases()
    const hit =
      dbs.find((d) => d.id === nameOrId) ?? dbs.find((d) => d.name === nameOrId)
    if (!hit) throw new Error(`database not found: ${nameOrId}`)
    return hit.id
  }

  getManifest(databaseId: string): Promise<Manifest> {
    return this.core.getManifest(databaseId)
  }

  getObject(ref: string): Promise<Uint8Array> {
    return this.core.getObject(ref)
  }

  putObject(bytes: Uint8Array): Promise<{ ref: string }> {
    return this.core.putObject(bytes)
  }

  latestCheckpoint(databaseId: string): Promise<CheckpointInfo | null> {
    return this.core.latestCheckpoint(databaseId)
  }

  registerCheckpoint(databaseId: string, input: CheckpointInfo): Promise<void> {
    return this.core.registerCheckpoint(databaseId, input)
  }

  streamClientFor(databaseId: string): DsStreamClient {
    return this.core.streamClientFor(databaseId)
  }

  registerEraAttempt(databaseId: string, input: EraAttemptInput) {
    return this.core.registerEraAttempt(databaseId, input)
  }

  promoteEraAttempt(databaseId: string, eraId: string) {
    return this.core.promoteEraAttempt(databaseId, eraId)
  }

  sealEraRow(databaseId: string, ordinal: number, seal: SealEraRowInput) {
    return this.core.sealEra(databaseId, ordinal, seal)
  }

  advanceCurrentEra(databaseId: string, from: number, to: number) {
    return this.core.advanceCurrentEra(databaseId, from, to)
  }

  addEra(databaseId: string, input: AddEraInfo): Promise<void> {
    return this.core.catalog.addEra({ databaseId, ...input })
  }

  async eraByOrdinal(
    databaseId: string,
    ordinal: number,
  ): Promise<EraInfo | null> {
    const row = await this.core.catalog.eraByOrdinal(databaseId, ordinal)
    if (!row) return null
    return {
      ordinal: row.ordinal,
      eraId: row.eraId,
      path: row.path,
      baseOffset: row.baseOffset,
      baseLsn: row.baseLsn,
      sealed: row.sealed,
      sealedFinalOffset: row.sealedFinalOffset,
      sealedFinalLsn: row.sealedFinalLsn,
      nextEraOrdinal: row.nextEraOrdinal,
    }
  }

  upsertPin(databaseId: string, pin: PinInput): Promise<void> {
    return this.core.catalog.upsertPin({ databaseId, ...pin })
  }

  deletePin(pinId: string): Promise<void> {
    return this.core.catalog.deletePin(pinId)
  }

  async runGc(databaseId: string): Promise<void> {
    await this.core.runGc(databaseId)
  }
}

class HttpGatewayHandle implements GatewayHandle {
  private readonly base: string

  constructor(url: string) {
    this.base = url.replace(/\/$/, '')
  }

  private async json<T>(path: string): Promise<T> {
    const res = await fetch(this.base + path)
    if (!res.ok) {
      throw new Error(
        `gateway ${path} -> HTTP ${res.status}: ${await res.text()}`,
      )
    }
    return (await res.json()) as T
  }

  async resolveDatabaseId(nameOrId: string): Promise<string> {
    const dbs = await this.json<{ id: string; name: string }[]>('/v1/db')
    const hit =
      dbs.find((d) => d.id === nameOrId) ?? dbs.find((d) => d.name === nameOrId)
    if (!hit) throw new Error(`database not found: ${nameOrId}`)
    return hit.id
  }

  getManifest(databaseId: string): Promise<Manifest> {
    return this.json<Manifest>(`/v1/db/${databaseId}/manifest`)
  }

  async getObject(ref: string): Promise<Uint8Array> {
    const res = await fetch(`${this.base}/v1/objects/${ref}`)
    if (!res.ok) {
      throw new Error(
        `gateway object ${ref} -> HTTP ${res.status}: ${await res.text()}`,
      )
    }
    return new Uint8Array(await res.arrayBuffer())
  }

  async putObject(bytes: Uint8Array): Promise<{ ref: string }> {
    const res = await fetch(`${this.base}/v1/objects`, {
      method: 'PUT',
      body: bytes,
    })
    if (!res.ok) {
      throw new Error(`gateway putObject -> HTTP ${res.status}`)
    }
    return (await res.json()) as { ref: string }
  }

  async latestCheckpoint(databaseId: string): Promise<CheckpointInfo | null> {
    const res = await fetch(
      `${this.base}/v1/db/${databaseId}/checkpoint/latest`,
    )
    if (res.status === 404) return null
    if (!res.ok) {
      throw new Error(`gateway latestCheckpoint -> HTTP ${res.status}`)
    }
    return (await res.json()) as CheckpointInfo
  }

  async registerCheckpoint(
    databaseId: string,
    input: CheckpointInfo,
  ): Promise<void> {
    const res = await fetch(`${this.base}/v1/db/${databaseId}/checkpoint`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    })
    if (!res.ok) {
      throw new Error(`gateway registerCheckpoint -> HTTP ${res.status}`)
    }
  }

  streamClientFor(databaseId: string): DsStreamClient {
    return new DsStreamClient(`${this.base}/v1/db/${databaseId}/stream`)
  }

  private async post(path: string, body: unknown): Promise<Response> {
    const res = await fetch(this.base + path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    if (!res.ok) {
      throw new Error(
        `gateway ${path} -> HTTP ${res.status}: ${await res.text()}`,
      )
    }
    return res
  }

  async registerEraAttempt(
    databaseId: string,
    input: EraAttemptInput,
  ): Promise<void> {
    await this.post(`/v1/db/${databaseId}/era/attempt`, input)
  }

  async promoteEraAttempt(databaseId: string, eraId: string): Promise<void> {
    await this.post(`/v1/db/${databaseId}/era/promote`, { eraId })
  }

  async sealEraRow(
    databaseId: string,
    ordinal: number,
    seal: SealEraRowInput,
  ): Promise<void> {
    await this.post(`/v1/db/${databaseId}/era/seal`, { ordinal, ...seal })
  }

  async advanceCurrentEra(
    databaseId: string,
    from: number,
    to: number,
  ): Promise<boolean> {
    const res = await this.post(`/v1/db/${databaseId}/era/advance`, {
      from,
      to,
    })
    return ((await res.json()) as { advanced: boolean }).advanced
  }

  // The M2 gateway HTTP surface exposes no era-row insert/read or pin
  // routes yet — rotation over an HTTP gateway therefore cannot complete
  // step 6 remotely. Loud failure (never silent divergence); the pin mirror
  // degrades to a no-op (the stream's L{gc-pin} frames remain the in-band
  // truth, the table is only the queryable index).
  addEra(): Promise<void> {
    return Promise.reject(
      new Error(
        'GatewayHandle(http).addEra: the gateway HTTP API exposes no era-row ' +
          'insert route yet — era rotation requires an in-process GatewayCore ' +
          '(M2c limitation)',
      ),
    )
  }

  eraByOrdinal(): Promise<EraInfo | null> {
    return Promise.reject(
      new Error(
        'GatewayHandle(http).eraByOrdinal: the gateway HTTP API exposes no ' +
          'era-row read route yet (M2c limitation)',
      ),
    )
  }

  async upsertPin(): Promise<void> {
    // No pin routes over HTTP yet: advisory index only, no-op (M2c limitation).
  }

  async deletePin(): Promise<void> {
    // No pin routes over HTTP yet: advisory index only, no-op (M2c limitation).
  }

  async runGc(databaseId: string): Promise<void> {
    await this.post(`/v1/db/${databaseId}/gc`, {})
  }
}

/** Wrap a GatewayTarget in the uniform handle. */
export function gatewayHandle(target: GatewayTarget): GatewayHandle {
  if ('url' in target) return new HttpGatewayHandle(target.url)
  return new InProcessGatewayHandle(target)
}
