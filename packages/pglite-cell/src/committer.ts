// The solo/host commit sequencer. Owns the capture-cursor invariant on the
// write side, serializes every append through one promise-chain mutex (W1:
// every era-stream append is CAS'd — commits, syncs, leases, fences), and
// journals each commit before its POST so §3.8 recovery can decide its
// outcome after a crash.

import { createHash, randomUUID } from 'node:crypto'
import type { DsStreamClient } from './stream-client'
import { StreamHttpError } from './stream-client'
import { casToken, encodeAppend } from './frames'
import type { Frame, WFrame } from './frames'
import { formatLsn } from './lsn'
import { CommitJournal } from './journal'
import type { RecoveryReport } from './journal'
import type { EraTailer } from './tail'
import {
  CaptureCursorError,
  EraClosedError,
  FencedError,
  ProducerGapError,
} from './errors'

/** Era coordinates the committer appends into. */
export interface CommitterEra {
  /** Stream path relative to the client's base URL. */
  path: string
  /** Era id (must match the O frame / every frame header). */
  id: string
  /** Era ordinal for W3 CAS tokens. */
  ordinal: number
}

export interface CommitterOpts {
  client: DsStreamClient
  era: CommitterEra
  tailer: EraTailer
  /** Journal directory for this (host, database) pair. */
  journalDir: string
}

/** A captured WAL slice submitted for commit. */
export interface CommitSliceInput {
  commitId: string
  kind: 'commit' | 'sync' | 'floors'
  baseLsn: bigint
  endLsn: bigint
  bytes: Uint8Array
}

/** Outcome of a commit attempt: landed at `offset`, or definitively lost. */
export type CommitResult =
  | { landed: true; offset: string; nextOffset: string }
  | { landed: false }

/** Result of a control-frame append (`appendControl`). */
export type ControlAppendResult =
  | { landed: true; offset: string; nextOffset: string }
  | { landed: false }

/** Total POST attempts for one commit on network failure (W2: same bytes,
 *  same producer tuple — the server dedups replays of the winning POST). */
const MAX_POST_ATTEMPTS = 3

function sha256Hex(bytes: Uint8Array): string {
  return 'sha256:' + createHash('sha256').update(bytes).digest('hex')
}

/**
 * The commit sequencer for one era. Create with `Committer.create()`, which
 * FIRST runs §3.8 journal recovery for any commits left pending by a prior
 * incarnation, then claims a producer epoch strictly above every epoch that
 * incarnation (or its recovery fences) used.
 */
export class Committer {
  /** The §3.8 recovery report produced during `create()`. */
  readonly recovery: RecoveryReport
  readonly journal: CommitJournal
  readonly producerId: string
  readonly epoch: number

  private seq = 0
  private chain: Promise<unknown> = Promise.resolve()

  private constructor(
    private readonly client: DsStreamClient,
    private readonly era: CommitterEra,
    private readonly tailer: EraTailer,
    journal: CommitJournal,
    producerId: string,
    epoch: number,
    recovery: RecoveryReport,
  ) {
    this.journal = journal
    this.producerId = producerId
    this.epoch = epoch
    this.recovery = recovery
  }

  /**
   * Load (or mint) the producer identity, run §3.8 recovery for pending
   * journal entries, then start a new producer incarnation at
   * `epoch = max(persisted epoch + 1, recovery epoch floor)`, seq 0. The
   * chosen epoch is persisted to `meta.json` before any append.
   */
  static async create(opts: CommitterOpts): Promise<Committer> {
    const journal = new CommitJournal(opts.journalDir)
    const meta = journal.readMeta() ?? { producerId: randomUUID(), epoch: 0 }

    // Recovery first: fences the previous incarnation and decides pending
    // commits from stream bytes. It may raise meta.json's epoch.
    const recovery = await journal.recover(opts.client)
    const persisted = journal.readMeta() ?? meta
    const epoch = Math.max(persisted.epoch + 1, recovery.epochFloor)
    journal.writeMeta({ producerId: meta.producerId, epoch })

    return new Committer(
      opts.client,
      opts.era,
      opts.tailer,
      journal,
      meta.producerId,
      epoch,
      recovery,
    )
  }

  /** Serialize `fn` behind every other append this committer has issued. */
  private run<T>(fn: () => Promise<T>): Promise<T> {
    const p = this.chain.then(fn)
    this.chain = p.then(
      () => undefined,
      () => undefined,
    )
    return p
  }

  /**
   * Commit one captured slice. Under the mutex:
   *
   * - asserts the capture-cursor invariant `baseLsn === tailer.head.lsn`
   *   (throws `CaptureCursorError` — the caller must rebase + re-execute);
   * - journals the commit (fsync) BEFORE the POST (§3.8);
   * - CAS-appends the W frame with the dual headers (Stream-Seq W3 token +
   *   Stream-Expected-Offset) and the producer tuple;
   * - `ok` ⇒ advances the tailer locally (no re-download of our own bytes),
   *   resolves the journal entry, returns `{ landed: true }`;
   * - `seq-conflict` ⇒ a definitive reject: resolves the journal entry and
   *   returns `{ landed: false }` (the caller rebases; NEVER re-POST these
   *   slice bytes — re-execute with a new commitId);
   * - network errors ⇒ retries the SAME bytes with the SAME producer tuple
   *   up to 3 attempts (W2 — the server dedups), then rethrows leaving the
   *   journal entry pending for the next incarnation's recovery;
   * - `closed` / `stale-epoch` / `producer-gap` ⇒ typed errors.
   */
  commitSlice(input: CommitSliceInput): Promise<CommitResult> {
    return this.run(async () => {
      const head = this.tailer.head
      if (input.baseLsn !== head.lsn) {
        throw new CaptureCursorError(input.baseLsn, head.lsn)
      }
      const expectedOffset = head.offset
      const sliceHash = sha256Hex(input.bytes)
      const frame: WFrame = {
        type: 'W',
        header: {
          v: 1,
          eraId: this.era.id,
          expectedOffset,
          commitId: input.commitId,
          kind: input.kind,
          baseLsn: formatLsn(input.baseLsn),
          endLsn: formatLsn(input.endLsn),
          sliceHash,
        },
        wal: input.bytes,
      }
      const body = encodeAppend([frame])
      const seqToken = casToken(this.era.ordinal, expectedOffset)
      const producer = { id: this.producerId, epoch: this.epoch, seq: this.seq }

      this.journal.record({
        commitId: input.commitId,
        eraId: this.era.id,
        eraPath: this.era.path,
        eraOrdinal: this.era.ordinal,
        expectedOffset,
        casToken: seqToken,
        baseLsn: formatLsn(input.baseLsn),
        endLsn: formatLsn(input.endLsn),
        sliceHash,
        producerId: producer.id,
        producerEpoch: producer.epoch,
        producerSeq: producer.seq,
        fenceEpoch: producer.epoch,
      })

      const res = await this.postWithRetry(body, {
        seq: seqToken,
        expectedOffset,
        producer,
      })

      switch (res.kind) {
        case 'ok': {
          if (res.deduped) {
            // Our append landed on an earlier attempt (network retry deduped).
            // The recovered nextOffset is the CURRENT tail — a foreign append
            // may sit between ours and it, so advancing locally would skip
            // frames. Re-download from the pre-append boundary instead.
            await this.tailer.catchUp()
          } else {
            this.tailer.advanceLocal([frame], res.nextOffset)
          }
          this.seq += 1
          this.journal.resolve(input.commitId)
          return {
            landed: true,
            offset: expectedOffset,
            nextOffset: this.tailer.head.offset,
          }
        }
        case 'seq-conflict':
          // A 409 is a definitive reject: nothing mutated server-side, the
          // producer seq was not consumed. Journal entry resolved.
          this.journal.resolve(input.commitId)
          return { landed: false }
        case 'closed':
          this.journal.resolve(input.commitId)
          throw new EraClosedError(res.nextOffset)
        case 'stale-epoch':
          this.journal.resolve(input.commitId)
          throw new FencedError(this.epoch, res.currentEpoch)
        case 'producer-gap':
          this.journal.resolve(input.commitId)
          throw new ProducerGapError(res.expectedSeq, res.receivedSeq)
      }
    })
  }

  /**
   * CAS-append control frames (L leases, K checkpoints, '0' fences) under
   * the same mutex + seq discipline as commits (W1), without a journal
   * entry. `build` receives the append position so headers can carry it
   * (W4); every returned frame must use exactly that `expectedOffset`.
   * `seq-conflict` ⇒ `{ landed: false }` (re-observe the tail and retry).
   */
  appendControl(
    build: (expectedOffset: string) => Frame[],
  ): Promise<ControlAppendResult> {
    return this.run(async () => {
      const expectedOffset = this.tailer.head.offset
      const frames = build(expectedOffset)
      const body = encodeAppend(frames)
      if (frames.some((f) => f.header.expectedOffset !== expectedOffset)) {
        throw new Error(
          'appendControl: built frames must carry the provided expectedOffset',
        )
      }
      const res = await this.postWithRetry(body, {
        seq: casToken(this.era.ordinal, expectedOffset),
        expectedOffset,
        producer: { id: this.producerId, epoch: this.epoch, seq: this.seq },
      })
      switch (res.kind) {
        case 'ok':
          if (res.deduped) {
            // Same skip hazard as commitSlice: re-download, never advance
            // locally past a tail we did not observe frame-by-frame.
            await this.tailer.catchUp()
          } else {
            this.tailer.advanceLocal(frames, res.nextOffset)
          }
          this.seq += 1
          return {
            landed: true,
            offset: expectedOffset,
            nextOffset: this.tailer.head.offset,
          }
        case 'seq-conflict':
          return { landed: false }
        case 'closed':
          throw new EraClosedError(res.nextOffset)
        case 'stale-epoch':
          throw new FencedError(this.epoch, res.currentEpoch)
        case 'producer-gap':
          throw new ProducerGapError(res.expectedSeq, res.receivedSeq)
      }
    })
  }

  /**
   * POST with up-to-3 network-failure retries carrying byte-identical body
   * and producer tuple (W2): if an earlier attempt actually landed, the
   * server dedups the replay to a success. HTTP-level rejects (typed
   * AppendResult kinds, StreamHttpError) are never retried.
   */
  private async postWithRetry(
    body: Uint8Array,
    opts: {
      seq: string
      expectedOffset: string
      producer: { id: string; epoch: number; seq: number }
    },
  ) {
    let lastErr: unknown
    for (let attempt = 1; attempt <= MAX_POST_ATTEMPTS; attempt++) {
      try {
        return await this.client.append(this.era.path, body, opts)
      } catch (err) {
        if (err instanceof StreamHttpError) throw err
        lastErr = err // network-level failure: outcome unknown, retry W2
      }
    }
    throw lastErr
  }
}
