// GatewayServer — the HTTP surface over a GatewayCore (§14.5 dev/fleet shape;
// the salvaged pageserver was a Hono app, §13.1). It is STATELESS: it holds
// only the shared GatewayCore handle. Two GatewayServers over one GatewayCore
// are indistinguishable — the statelessness invariant, tested directly.
//
// The load-bearing endpoint is the stream proxy: it forwards verbatim to the
// embedded DS server so a DsStreamClient pointed at
// `<gatewayUrl>/v1/db/<id>/stream` speaks CAS commits through the gateway
// UNCHANGED. The gateway validates frames and enforces size caps, but NEVER
// touches CAS position (that lives in the DS server's append lock, §14.5).

import { Hono } from 'hono'
import { serve } from '@hono/node-server'
import type { ServerType } from '@hono/node-server'
import { decodeFrame, parseLsn } from '@electric-sql/pglite-cell'
import type { GatewayCore } from './core'

/** Max append body the gateway forwards (32 MiB, matches the frame cap). */
export const MAX_APPEND_BYTES = 32 * 1024 * 1024

/**
 * Request headers forwarded to the DS server verbatim on stream proxy. Prefix
 * matches (Stream-*, Producer-*) plus a fixed allowlist. Casing-insensitive.
 */
function forwardedRequestHeaders(headers: Headers): Record<string, string> {
  const out: Record<string, string> = {}
  headers.forEach((value, key) => {
    const k = key.toLowerCase()
    if (
      k.startsWith('stream-') ||
      k.startsWith('producer-') ||
      k === 'content-type' ||
      k === 'accept-encoding'
    ) {
      out[key] = value
    }
  })
  return out
}

/**
 * Response headers forwarded back from the DS server verbatim. Prefix matches
 * (Stream-*, Producer-*) plus content-type / etag. Casing-insensitive.
 */
function forwardedResponseHeaders(headers: Headers): Record<string, string> {
  const out: Record<string, string> = {}
  headers.forEach((value, key) => {
    const k = key.toLowerCase()
    if (
      k.startsWith('stream-') ||
      k.startsWith('producer-') ||
      k === 'content-type' ||
      k === 'etag'
    ) {
      out[key] = value
    }
  })
  return out
}

/**
 * Validate an append body as a well-formed frame sequence (§14.5 frame
 * validation): decodes as whole frames end to end, total <= 32 MiB, and every
 * W frame's LSNs parse with baseLsn < endLsn and every frame carries a nonempty
 * eraId. Returns a reason string on failure, or null when the body is valid.
 *
 * NEVER checks CAS position — that is the DS server's job (§14.5).
 */
export function validateAppendBody(body: Uint8Array): string | null {
  if (body.length > MAX_APPEND_BYTES) {
    return `append body ${body.length} bytes exceeds ${MAX_APPEND_BYTES} cap`
  }
  if (body.length === 0) return null // empty body handled by the DS server (400/close)

  let pos = 0
  let count = 0
  while (pos < body.length) {
    let decoded: ReturnType<typeof decodeFrame>
    try {
      decoded = decodeFrame(body, pos)
    } catch (err) {
      return `frame ${count} malformed: ${err instanceof Error ? err.message : String(err)}`
    }
    if (!decoded) {
      return `frame ${count} truncated at byte ${pos}`
    }
    const { frame, next } = decoded
    const eraId = (frame.header as { eraId?: unknown }).eraId
    if (typeof eraId !== 'string' || eraId.length === 0) {
      return `frame ${count} has empty eraId`
    }
    if (frame.type === 'W') {
      let baseLsn: bigint
      let endLsn: bigint
      try {
        baseLsn = parseLsn(frame.header.baseLsn)
        endLsn = parseLsn(frame.header.endLsn)
      } catch {
        return `frame ${count} has unparseable W LSNs`
      }
      if (!(baseLsn < endLsn)) {
        return `frame ${count} W baseLsn ${frame.header.baseLsn} not < endLsn ${frame.header.endLsn}`
      }
    }
    pos = next
    count++
  }
  return null
}

/** Read the full request body into a single Uint8Array. */
async function readBody(req: Request): Promise<Uint8Array> {
  const buf = await req.arrayBuffer()
  return new Uint8Array(buf)
}

export interface GatewayServerOpts {
  core: GatewayCore
  /**
   * Static bearer token hook (M1: off by default in dev). TODO: wire real
   * capability-token verification (§11.2, control-plane auth §14.6).
   */
  // authToken?: string
}

/**
 * HTTP front for a GatewayCore. Build the app with `app`, or run a Node server
 * with `listen()`.
 */
export class GatewayServer {
  private readonly core: GatewayCore
  private server: ServerType | null = null
  private boundUrl: string | null = null

  readonly app: Hono

  constructor(opts: GatewayServerOpts) {
    this.core = opts.core
    this.app = this.buildApp()
  }

  private buildApp(): Hono {
    const app = new Hono()
    const core = this.core

    // --- Databases -------------------------------------------------------
    app.post('/v1/db', async (c) => {
      const body = (await c.req.json().catch(() => ({}))) as { name?: string }
      if (!body.name) {
        return c.text('missing "name"', 400)
      }
      const manifest = await core.createDatabase(body.name)
      return c.json(manifest, 201)
    })

    app.get('/v1/db', async (c) => {
      return c.json(await core.listDatabases())
    })

    app.get('/v1/db/:id/manifest', async (c) => {
      try {
        return c.json(await core.getManifest(c.req.param('id')))
      } catch (err) {
        return c.text(err instanceof Error ? err.message : String(err), 404)
      }
    })

    // --- Forks (M2) ------------------------------------------------------
    app.post('/v1/db/:id/fork', async (c) => {
      const body = (await c.req.json().catch(() => ({}))) as { name?: string }
      if (!body.name) return c.text('missing "name"', 400)
      try {
        const manifest = await core.forkDatabase(c.req.param('id'), body.name)
        return c.json(manifest, 201)
      } catch (err) {
        return c.text(err instanceof Error ? err.message : String(err), 400)
      }
    })

    // --- Dials (M2) ------------------------------------------------------
    app.patch('/v1/db/:id/dials', async (c) => {
      const body = (await c.req.json().catch(() => ({}))) as {
        checkpointEveryBytes?: string | number
        rotateEveryBytes?: string | number
        gcGraceMs?: string | number
      }
      await core.setDials(c.req.param('id'), body)
      return c.json((await core.getManifest(c.req.param('id'))).dials)
    })

    // --- GC (M2) ---------------------------------------------------------
    app.post('/v1/gc', async (c) => {
      return c.json(await core.runGc())
    })
    app.post('/v1/db/:id/gc', async (c) => {
      return c.json(await core.runGc(c.req.param('id')))
    })

    // --- Era rotation primitives (M2 — the cell-server rotator calls these;
    // thin 1:1 mappings onto the control plane) ---------------------------
    app.post('/v1/db/:id/era/attempt', async (c) => {
      const body = (await c.req.json()) as {
        ordinal: number
        eraId: string
        path: string
      }
      await core.registerEraAttempt(c.req.param('id'), body)
      return c.body(null, 204)
    })
    app.post('/v1/db/:id/era/promote', async (c) => {
      const body = (await c.req.json()) as { eraId: string }
      await core.promoteEraAttempt(c.req.param('id'), body.eraId)
      return c.body(null, 204)
    })
    app.post('/v1/db/:id/era/seal', async (c) => {
      const body = (await c.req.json()) as {
        ordinal: number
        finalOffset: string
        finalLsn: string
        nextOrdinal: number
      }
      await core.sealEra(c.req.param('id'), body.ordinal, body)
      return c.body(null, 204)
    })
    app.post('/v1/db/:id/era/advance', async (c) => {
      const body = (await c.req.json()) as { from: number; to: number }
      const advanced = await core.advanceCurrentEra(
        c.req.param('id'),
        body.from,
        body.to,
      )
      return c.json({ advanced })
    })

    // --- Checkpoints (M1e worker) ----------------------------------------
    app.get('/v1/db/:id/checkpoint/latest', async (c) => {
      const row = await core.latestCheckpoint(c.req.param('id'))
      if (row === null) return c.text('no checkpoint', 404)
      return c.json(row)
    })

    app.post('/v1/db/:id/checkpoint', async (c) => {
      const body = (await c.req.json()) as {
        lsn: string
        snapEnd: string
        streamOffset: string
        objectRef: string
      }
      await core.registerCheckpoint(c.req.param('id'), body)
      return c.body(null, 204)
    })

    // --- Objects (content-addressed, immutable) --------------------------
    app.get('/v1/objects/:ref', async (c) => {
      const ref = c.req.param('ref')
      try {
        const bytes = await core.getObject(ref)
        c.header('Content-Type', 'application/octet-stream')
        c.header('Cache-Control', 'public, max-age=31536000, immutable')
        return c.body(toArrayBufferView(bytes))
      } catch {
        return c.text(`object not found: ${ref}`, 404)
      }
    })

    app.put('/v1/objects', async (c) => {
      const bytes = await readBody(c.req.raw)
      const { ref } = await core.putObject(bytes)
      return c.json({ ref }, 201)
    })

    // --- Stream proxy (the load-bearing part) ----------------------------
    // ALL methods on /v1/db/:id/stream/* forward verbatim to the embedded DS
    // server at /pgl/<id>/<rest>: method, query string, body bytes, and
    // Stream-*/Producer-*/Content-Type/Accept-Encoding request headers.
    app.all('/v1/db/:id/stream/*', async (c) => {
      const id = c.req.param('id')
      const rest = extractStreamRest(c.req.path, id)
      const method = c.req.method

      const requestHeaders = forwardedRequestHeaders(c.req.raw.headers)

      let body: Uint8Array | undefined
      if (method !== 'GET' && method !== 'HEAD') {
        body = await readBody(c.req.raw)
      }

      // Frame validation runs on appends only (POST with a nonempty body).
      if (method === 'POST' && body && body.length > 0) {
        const reason = validateAppendBody(body)
        if (reason !== null) {
          return c.text(reason, 422)
        }
      }

      // Preserve the query string (offset=, live=, etc).
      const qs = queryString(c.req.url)
      const upstream = core.streamUpstreamUrl(id, rest) + qs

      const res = await fetch(upstream, {
        method,
        headers: requestHeaders,
        body: body as BodyInit | undefined,
      })

      const responseHeaders = forwardedResponseHeaders(res.headers)
      const outHeaders = new Headers(responseHeaders)
      // HEAD/204 carry no body.
      if (method === 'HEAD' || res.status === 204) {
        return new Response(null, { status: res.status, headers: outHeaders })
      }
      const outBytes = new Uint8Array(await res.arrayBuffer())
      return new Response(toArrayBufferView(outBytes), {
        status: res.status,
        headers: outHeaders,
      })
    })

    return app
  }

  /** Start a Node HTTP server on `port` (0 = ephemeral). Returns the URL. */
  async listen(port = 0): Promise<string> {
    this.boundUrl = await new Promise<string>((resolve) => {
      this.server = serve({ fetch: this.app.fetch, port }, (info) => {
        const host = info.address === '::' ? '127.0.0.1' : info.address
        resolve(`http://${host}:${info.port}`)
      })
    })
    return this.boundUrl
  }

  /** The bound URL (valid after `listen()`). */
  get url(): string {
    if (!this.boundUrl) throw new Error('GatewayServer not listening')
    return this.boundUrl
  }

  /** Stop the Node HTTP server. */
  async close(): Promise<void> {
    if (!this.server) return
    const server = this.server
    await new Promise<void>((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()))
    })
    this.server = null
    this.boundUrl = null
  }
}

/** Extract the `<rest>` path after `/v1/db/<id>/stream/`. */
function extractStreamRest(path: string, id: string): string {
  const prefix = `/v1/db/${id}/stream/`
  const idx = path.indexOf(prefix)
  if (idx < 0) return ''
  return path.slice(idx + prefix.length)
}

/** The query string (including leading `?`) of a URL, or '' if none. */
function queryString(url: string): string {
  const idx = url.indexOf('?')
  return idx < 0 ? '' : url.slice(idx)
}

/**
 * A same-length ArrayBuffer view of `bytes` (Hono/undici want a BodyInit
 * backed by a plain ArrayBuffer, not a possibly-shared Buffer pool slice).
 */
function toArrayBufferView(bytes: Uint8Array): ArrayBuffer {
  const ab = new ArrayBuffer(bytes.byteLength)
  new Uint8Array(ab).set(bytes)
  return ab
}
