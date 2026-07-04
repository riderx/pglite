// Hybrid Durable Streams client.
//
// Where the official `@durable-streams/client` (0.2.6) exposes offsets and
// typed results we delegate to it (head, create-with-body, append+close,
// delete). The pieces it cannot express are kept as purpose-built fetch paths:
//
//   - CAS append: the official `append()` returns void, throws untyped errors,
//     and sends only Stream-Seq (no producer headers on the same POST); its
//     `IdempotentProducer` batches and auto-manages epochs, fighting our CAS +
//     §3.8 manual fencing. We need one POST that carries Stream-Seq AND
//     producer headers AND returns a typed AppendResult. See `casAppend` — it
//     is the shape of a planned upstream addition to @durable-streams/client
//     (typed AppendResult + Stream-Seq + producer headers on one POST), to be
//     upstreamed alongside the strict Stream-Expected-Offset extension.
//   - read: our reader needs the raw response bytes, the exact HTTP status
//     (200 vs 204), and `Accept-Encoding: identity` (so gzip never perturbs the
//     byte offsets the position-checked reader reconstructs). The official
//     `stream()` read session wraps reads in retry/backoff and does not set
//     identity encoding, so we issue the GET directly.
//
// ALL header/param names come from the client's exported constants — zero
// hand-written header strings.

import {
  DurableStream,
  DurableStreamError,
  FetchError,
  STREAM_SEQ_HEADER,
  STREAM_OFFSET_HEADER,
  STREAM_UP_TO_DATE_HEADER,
  STREAM_CLOSED_HEADER,
  PRODUCER_ID_HEADER,
  PRODUCER_EPOCH_HEADER,
  PRODUCER_SEQ_HEADER,
  PRODUCER_EXPECTED_SEQ_HEADER,
  PRODUCER_RECEIVED_SEQ_HEADER,
  OFFSET_QUERY_PARAM,
  LIVE_QUERY_PARAM,
} from '@durable-streams/client'

/** Advisory strict-position header (ignored by the 0.3.7 server; §W1 posture). */
const STREAM_EXPECTED_OFFSET_HEADER = 'Stream-Expected-Offset'

/** Fork-create headers (server 0.3.7 protocol; the official client 0.2.6
 *  exports no constants for them, so they are kept local until upstreamed). */
const STREAM_FORKED_FROM_HEADER = 'Stream-Forked-From'
const STREAM_FORK_OFFSET_HEADER = 'Stream-Fork-Offset'

const OCTET = 'application/octet-stream'

export type FetchImpl = typeof fetch

/** PUT create conflicted with an existing stream of different config (409). */
export class StreamConfigConflictError extends Error {
  constructor(public readonly body: string) {
    super(`stream config conflict: ${body}`)
    this.name = 'StreamConfigConflictError'
  }
}

/** Any unexpected status from the server. */
export class StreamHttpError extends Error {
  constructor(
    public readonly status: number,
    public readonly body: string,
  ) {
    super(`durable-streams HTTP ${status}: ${body}`)
    this.name = 'StreamHttpError'
  }
}

export interface CreateResult {
  created: boolean
  nextOffset: string
}

export interface ProducerTuple {
  id: string
  epoch: number
  seq: number
}

export interface AppendOpts {
  seq?: string
  expectedOffset?: string
  producer?: ProducerTuple
  close?: boolean
}

export type AppendResult =
  /** Landed. `closed` is true when this very append also closed the stream. */
  | { kind: 'ok'; nextOffset: string; deduped: boolean; closed?: boolean }
  | { kind: 'seq-conflict' }
  /** Rejected: the stream was already closed (409 + Stream-Closed). */
  | { kind: 'closed'; nextOffset: string }
  | { kind: 'stale-epoch'; currentEpoch: number }
  | { kind: 'producer-gap'; expectedSeq: number; receivedSeq: number }

export interface ReadResult {
  bytes: Uint8Array
  nextOffset: string
  upToDate: boolean
  closed: boolean
  status: number
}

export interface HeadResult {
  nextOffset: string
  closed: boolean
}

export class DsStreamClient {
  constructor(
    private readonly baseUrl: string,
    private readonly fetchImpl: FetchImpl = fetch,
  ) {}

  private url(path: string): string {
    return this.baseUrl.replace(/\/$/, '') + path
  }

  private handle(path: string): DurableStream {
    return new DurableStream({
      url: this.url(path),
      fetch: this.fetchImpl,
      // Byte mode — the handle defaults this on create()/close() bodies, and
      // the server rejects bodied POSTs without a Content-Type (400).
      contentType: OCTET,
      // Our writes are explicit single CAS POSTs; never let the handle batch.
      batching: false,
      // Node context — silence the browser-HTTP warning.
      warnOnHttp: false,
    })
  }

  /**
   * PUT-create a stream (via the official client). The optional `body` is
   * stored as the first append (the O-frame ride-along). Matching re-PUT ⇒
   * created:false; config mismatch (409) ⇒ StreamConfigConflictError. The
   * official create() does not surface the tail offset, so we HEAD for it.
   */
  async createStream(
    path: string,
    opts: { body?: Uint8Array; contentType?: string } = {},
  ): Promise<CreateResult> {
    const handle = this.handle(path)
    // HEAD first to learn whether this is a fresh create (201) or an identical
    // re-PUT (200) — the official create() swallows the status distinction. A
    // config mismatch still throws below.
    const preExisting = (await handle.head()).exists
    try {
      await handle.create({
        contentType: opts.contentType ?? OCTET,
        body: opts.body,
      })
    } catch (err) {
      // A 409 here means the existing stream has a different configuration (an
      // identical re-PUT returns 200 and does not throw). The official client's
      // backoff wrapper surfaces non-retryable 4xx as FetchError BEFORE its own
      // handleErrorResponse maps them to DurableStreamError, so accept both.
      if (
        (err instanceof FetchError && err.status === 409) ||
        (err instanceof DurableStreamError && err.code === 'CONFLICT_EXISTS')
      ) {
        throw new StreamConfigConflictError(
          err instanceof Error ? err.message : String(err),
        )
      }
      throw err
    }
    const head = await handle.head()
    return {
      created: !preExisting,
      nextOffset: head.exists ? (head.offset ?? '') : '',
    }
  }

  /**
   * PUT-create `newPath` as a FORK of `sourcePath` at `atOffset` (§2.5 era
   * forks: the child stream inherits the parent's bytes up to the fork
   * point; its own offsets continue from `atOffset`). Purpose-built raw
   * fetch: the official client's create() has no fork-header support in
   * 0.2.6. `atOffset` must be a boundary token of the source stream; the
   * server rejects offsets past the source tail (400) and missing sources
   * (404) — both surface as StreamHttpError.
   */
  async forkStream(
    sourcePath: string,
    newPath: string,
    atOffset: string,
  ): Promise<CreateResult> {
    const res = await this.fetchImpl(this.url(newPath), {
      method: 'PUT',
      headers: {
        'Content-Type': OCTET,
        [STREAM_FORKED_FROM_HEADER]: sourcePath,
        [STREAM_FORK_OFFSET_HEADER]: atOffset,
      },
    })
    if (res.status !== 200 && res.status !== 201) {
      throw new StreamHttpError(res.status, await res.text())
    }
    const head = await this.head(newPath)
    return { created: res.status === 201, nextOffset: head.nextOffset }
  }

  /**
   * CAS append (purpose-built — see file header). Sends Stream-Seq when `seq`
   * is given, and ALWAYS ALSO sends Stream-Expected-Offset when
   * `expectedOffset` is given (dual-header posture; advisory on 0.3.7).
   * Producer headers enable server-side dedup.
   */
  async append(
    path: string,
    body: Uint8Array,
    opts: AppendOpts = {},
  ): Promise<AppendResult> {
    return this.casAppend(path, body, opts)
  }

  /**
   * The shape of a planned upstream addition to @durable-streams/client: one
   * POST carrying Stream-Seq + producer headers, returning a typed
   * discriminated AppendResult instead of void/throw. Kept local until
   * upstreamed alongside the strict Stream-Expected-Offset extension.
   */
  private async casAppend(
    path: string,
    body: Uint8Array,
    opts: AppendOpts,
  ): Promise<AppendResult> {
    const headers: Record<string, string> = { 'Content-Type': OCTET }
    if (opts.seq !== undefined) headers[STREAM_SEQ_HEADER] = opts.seq
    if (opts.expectedOffset !== undefined)
      headers[STREAM_EXPECTED_OFFSET_HEADER] = opts.expectedOffset
    if (opts.producer) {
      headers[PRODUCER_ID_HEADER] = opts.producer.id
      headers[PRODUCER_EPOCH_HEADER] = String(opts.producer.epoch)
      headers[PRODUCER_SEQ_HEADER] = String(opts.producer.seq)
    }
    if (opts.close) headers[STREAM_CLOSED_HEADER] = 'true'

    const res = await this.fetchImpl(this.url(path), {
      method: 'POST',
      headers,
      body: body as BodyInit,
    })
    const hasProducer = opts.producer !== undefined

    // Success: 200 (producer, fresh) or 204 (no producer, fresh; OR producer
    // dedup). A dedup 204 with producer headers carries no Stream-Next-Offset,
    // so we HEAD to learn the tail (§3.8 needs the landed offset).
    if (res.status === 200 || res.status === 204) {
      const closed = res.headers.get(STREAM_CLOSED_HEADER) === 'true'
      let nextOffset = res.headers.get(STREAM_OFFSET_HEADER) ?? ''
      const deduped = hasProducer && res.status === 204
      if (deduped && nextOffset === '') {
        nextOffset = (await this.head(path)).nextOffset
      }
      // A 2xx with Stream-Closed: true is a SUCCESSFUL append+close (ours) —
      // not to be confused with the 409 'closed' rejection below.
      return { kind: 'ok', nextOffset, deduped, closed }
    }

    const text = await res.text()

    if (res.status === 409) {
      if (res.headers.get(STREAM_CLOSED_HEADER) === 'true') {
        return {
          kind: 'closed',
          nextOffset: res.headers.get(STREAM_OFFSET_HEADER) ?? '',
        }
      }
      const expected = res.headers.get(PRODUCER_EXPECTED_SEQ_HEADER)
      if (expected !== null) {
        return {
          kind: 'producer-gap',
          expectedSeq: Number(expected),
          receivedSeq: Number(
            res.headers.get(PRODUCER_RECEIVED_SEQ_HEADER) ?? '0',
          ),
        }
      }
      // Sequence conflict (body "Sequence conflict", no headers).
      return { kind: 'seq-conflict' }
    }

    if (res.status === 403) {
      const epoch = res.headers.get(PRODUCER_EPOCH_HEADER)
      if (epoch !== null)
        return { kind: 'stale-epoch', currentEpoch: Number(epoch) }
    }

    throw new StreamHttpError(res.status, text)
  }

  /**
   * Read from `offset` (`-1` = start, `now` = tail, or a boundary token).
   * `live: 'long-poll'` waits for new data (204 on timeout ⇒ empty bytes).
   * Purpose-built (see file header): needs raw bytes, exact status, and
   * Accept-Encoding: identity.
   */
  async read(
    path: string,
    opts: { offset: string; live?: 'long-poll' },
  ): Promise<ReadResult> {
    const qs = new URLSearchParams({ [OFFSET_QUERY_PARAM]: opts.offset })
    if (opts.live) qs.set(LIVE_QUERY_PARAM, opts.live)
    const res = await this.fetchImpl(this.url(path) + '?' + qs.toString(), {
      method: 'GET',
      headers: { 'Accept-Encoding': 'identity' },
    })
    if (res.status !== 200 && res.status !== 204) {
      throw new StreamHttpError(res.status, await res.text())
    }
    const bytes =
      res.status === 204
        ? new Uint8Array(0)
        : new Uint8Array(await res.arrayBuffer())
    return {
      bytes,
      nextOffset: res.headers.get(STREAM_OFFSET_HEADER) ?? '',
      upToDate: res.headers.get(STREAM_UP_TO_DATE_HEADER) === 'true',
      closed: res.headers.get(STREAM_CLOSED_HEADER) === 'true',
      status: res.status,
    }
  }

  /** HEAD: current tail offset + closed flag (via the official client). */
  async head(path: string): Promise<HeadResult> {
    const result = await this.handle(path).head()
    if (!result.exists) {
      throw new StreamHttpError(404, 'Stream not found')
    }
    return { nextOffset: result.offset ?? '', closed: result.streamClosed }
  }

  /**
   * Append `body` atomically and close the stream. Deliberately NOT the
   * official client's close(): that path carries no Stream-Seq, i.e. an
   * un-CAS'd sealing append — the exact W1 violation the design's "never
   * seal with close-only" rule (§2.4) exists to prevent. Era seals must be
   * able to LOSE a race with a landing commit, so the seal rides the same
   * CAS append as everything else.
   */
  async appendAndClose(
    path: string,
    body: Uint8Array,
    opts: AppendOpts = {},
  ): Promise<AppendResult> {
    return this.append(path, body, { ...opts, close: true })
  }

  /** DELETE the stream (via the official client). */
  async deleteStream(path: string): Promise<void> {
    await this.handle(path).delete()
  }
}
