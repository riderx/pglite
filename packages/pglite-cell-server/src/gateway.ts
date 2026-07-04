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
}

/** Wrap a GatewayTarget in the uniform handle. */
export function gatewayHandle(target: GatewayTarget): GatewayHandle {
  if ('url' in target) return new HttpGatewayHandle(target.url)
  return new InProcessGatewayHandle(target)
}
