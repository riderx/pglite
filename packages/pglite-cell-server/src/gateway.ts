// The host's view of the gateway (§14.5): manifest reads, checkpoint-object
// fetches, and per-database stream clients. Two shapes, one interface — an
// in-process GatewayCore (the Supabase-lite / test embedding) or an HTTP
// gateway URL (the fleet shape). Cell hosts speak ONLY to the gateway; the
// DS server and object store stay behind it.

import { DsStreamClient } from '@electric-sql/pglite-cell'
import type { GatewayCore, Manifest } from '@electric-sql/pglite-gateway'

/** Constructor target: an embedded core or a remote gateway base URL. */
export type GatewayTarget = GatewayCore | { url: string }

/** The narrow gateway surface the host consumes. */
export interface GatewayHandle {
  /** Resolve a database name OR id to its id (names are control-plane rows). */
  resolveDatabaseId(nameOrId: string): Promise<string>
  getManifest(databaseId: string): Promise<Manifest>
  getObject(ref: string): Promise<Uint8Array>
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

  streamClientFor(databaseId: string): DsStreamClient {
    return new DsStreamClient(`${this.base}/v1/db/${databaseId}/stream`)
  }
}

/** Wrap a GatewayTarget in the uniform handle. */
export function gatewayHandle(target: GatewayTarget): GatewayHandle {
  if ('url' in target) return new HttpGatewayHandle(target.url)
  return new InProcessGatewayHandle(target)
}
