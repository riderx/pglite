// The solo/host commit sequencer. Owns the capture-cursor invariant on the
// write side, serializes every append through one promise-chain mutex (W1:
// every era-stream append is CAS'd — commits, syncs, leases, fences), and
// journals each commit before its POST so §3.8 recovery can decide its
// outcome after a crash.
//
// M2: the committer is era-aware. All era coordinates (path, id, W3 token
// ordinal) come from the TAILER's current era at append time, not from
// construction-time config. A `closed` append result is no longer terminal:
// the committer catches up (which hops the era via the S/O chain) and — if
// the slice still sits at the new head — RE-CASes the same WAL bytes into
// the new era as a NEW append (new frame headers, new journal entry, same
// commitId; W1/W2: never a byte-identical retry of the old one). Bounded at
// two hops per commit, then `EraClosedError`.

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

/** Era coordinates the committer starts from (kept for construction-time
 *  bookkeeping; live coordinates always come from the tailer's current era). */
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

/** Maximum era hops one append may follow on `closed` results before the
 *  committer gives up with EraClosedError. */
const MAX_ROTATION_HOPS = 2

function sha256Hex(bytes: Uint8Array): string {
  return 'sha256:' + createHash('sha256').update(bytes).digest('hex')
}

/**
 * The commit sequencer for one era chain. Create with `Committer.create()`,
 * which FIRST runs §3.8 journal recovery for any commits left pending by a
 * prior incarnation, then claims a producer epoch strictly above every epoch
 * that incarnation (or its recovery fences) used.
 */
export class Committer {
  /** The §3.8 recovery report produced during `create()`. */
  readonly recovery: RecoveryReport
  readonly journal: CommitJournal
  readonly producerId: string
  readonly epoch: number

  /**
   * Producer seq PER ERA STREAM: the server keeps producer state per
   * stream and requires a producer's first append on a stream to carry
   * seq 0 (verified against 0.3.7 validateProducer), so the counter cannot
   * be global across era hops.
   */
  private readonly seqByPath = new Map<string, number>()
  private chain: Promise<unknown> = Promise.resolve()

  private constructor(
    private readonly client: DsStreamClient,
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
   * - `closed` ⇒ the era rotated under us: resolve the entry
   *   (lost-to-rotation), catch up (hops via the S/O chain), and if the
   *   slice still sits at the new head, RE-CAS the same WAL bytes into the
   *   new era (fresh frame + journal entry, same commitId); otherwise
   *   `{ landed: false }`. Bounded at 2 hops, then `EraClosedError`;
   * - network errors ⇒ retries the SAME bytes with the SAME producer tuple
   *   up to 3 attempts (W2 — the server dedups), then rethrows leaving the
   *   journal entry pending for the next incarnation's recovery;
   * - `stale-epoch` / `producer-gap` ⇒ typed errors.
   */
  commitSlice(input: CommitSliceInput): Promise<CommitResult> {
    return this.run(async () => {
      if (input.baseLsn !== this.tailer.head.lsn) {
        throw new CaptureCursorError(input.baseLsn, this.tailer.head.lsn)
      }
      return this.postSlice(input, MAX_ROTATION_HOPS)
    })
  }

  /** One CAS attempt of `input` into the tailer's CURRENT era, following up
   *  to `hopsLeft` era rotations on `closed` results. Mutex held by caller. */
  private async postSlice(
    input: CommitSliceInput,
    hopsLeft: number,
  ): Promise<CommitResult> {
    const era = this.tailer.currentEra
    const expectedOffset = this.tailer.head.offset
    const sliceHash = sha256Hex(input.bytes)
    const frame: WFrame = {
      type: 'W',
      header: {
        v: 1,
        eraId: era.id,
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
    const seqToken = casToken(era.ordinal, expectedOffset)
    const producer = {
      id: this.producerId,
      epoch: this.epoch,
      seq: this.seqByPath.get(era.path) ?? 0,
    }

    this.journal.record({
      commitId: input.commitId,
      eraId: era.id,
      eraPath: era.path,
      eraOrdinal: era.ordinal,
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

    const res = await this.postWithRetry(era.path, body, {
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
        this.seqByPath.set(era.path, producer.seq + 1)
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
      case 'closed': {
        // The era rotated under us. The 409 was a definitive reject, so the
        // journal entry resolves as lost-to-rotation; the re-CAS below is a
        // NEW append with a FRESH entry against the new era (never a
        // byte-identical retry — W1/W2).
        this.journal.resolve(input.commitId)
        if (hopsLeft <= 0) throw new EraClosedError(res.nextOffset)
        await this.tailer.catchUp() // hops via the S/O chain (or throws)
        if (input.baseLsn !== this.tailer.head.lsn) {
          // Another commit landed ahead of us in the new era: ordinary
          // lost race — the caller rebases and re-executes.
          return { landed: false }
        }
        return this.postSlice(input, hopsLeft - 1)
      }
      case 'stale-epoch':
        this.journal.resolve(input.commitId)
        throw new FencedError(this.epoch, res.currentEpoch)
      case 'producer-gap':
        this.journal.resolve(input.commitId)
        throw new ProducerGapError(res.expectedSeq, res.receivedSeq)
    }
  }

  /**
   * CAS-append control frames (L leases, K checkpoints, '0' fences) under
   * the same mutex + seq discipline as commits (W1), without a journal
   * entry. `build` receives the append position so headers can carry it
   * (W4); every returned frame must use exactly that `expectedOffset`.
   * `seq-conflict` ⇒ `{ landed: false }` (re-observe the tail and retry).
   * `closed` ⇒ catch up (era hop) and rebuild via `build` at the new
   * position, bounded at 2 hops.
   */
  appendControl(
    build: (expectedOffset: string) => Frame[],
  ): Promise<ControlAppendResult> {
    return this.run(async () => this.postControl(build, MAX_ROTATION_HOPS))
  }

  private async postControl(
    build: (expectedOffset: string) => Frame[],
    hopsLeft: number,
  ): Promise<ControlAppendResult> {
    const era = this.tailer.currentEra
    const expectedOffset = this.tailer.head.offset
    const frames = build(expectedOffset)
    const body = encodeAppend(frames)
    if (frames.some((f) => f.header.expectedOffset !== expectedOffset)) {
      throw new Error(
        'appendControl: built frames must carry the provided expectedOffset',
      )
    }
    const producer = {
      id: this.producerId,
      epoch: this.epoch,
      seq: this.seqByPath.get(era.path) ?? 0,
    }
    const res = await this.postWithRetry(era.path, body, {
      seq: casToken(era.ordinal, expectedOffset),
      expectedOffset,
      producer,
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
        this.seqByPath.set(era.path, producer.seq + 1)
        return {
          landed: true,
          offset: expectedOffset,
          nextOffset: this.tailer.head.offset,
        }
      case 'seq-conflict':
        return { landed: false }
      case 'closed':
        if (hopsLeft <= 0) throw new EraClosedError(res.nextOffset)
        await this.tailer.catchUp() // hops via the S/O chain (or throws)
        // `build` re-derives headers at the new position (new era's frames).
        return this.postControl(build, hopsLeft - 1)
      case 'stale-epoch':
        throw new FencedError(this.epoch, res.currentEpoch)
      case 'producer-gap':
        throw new ProducerGapError(res.expectedSeq, res.receivedSeq)
    }
  }

  /**
   * POST with up-to-3 network-failure retries carrying byte-identical body
   * and producer tuple (W2): if an earlier attempt actually landed, the
   * server dedups the replay to a success. HTTP-level rejects (typed
   * AppendResult kinds, StreamHttpError) are never retried.
   */
  private async postWithRetry(
    path: string,
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
        return await this.client.append(path, body, opts)
      } catch (err) {
        if (err instanceof StreamHttpError) throw err
        lastErr = err // network-level failure: outcome unknown, retry W2
      }
    }
    throw lastErr
  }
}
