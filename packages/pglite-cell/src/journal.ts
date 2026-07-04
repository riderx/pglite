// Pending-commit journal + §3.8 fence-then-read recovery.
//
// One journal directory per (host, database). Each pending commit is a
// `<commitId>.json` file written atomically (tmp → fsync → rename → fsync
// dir) BEFORE the commit's POST, and deleted (`resolve`) once the append's
// outcome is known in-process. `meta.json` holds the durable producer
// identity `{ producerId, epoch }` for the committer, fsync'd on change.
//
// After a crash between POST and ack, `recover()` decides each pending
// commit's outcome exactly — landed / lost / indeterminate — from stream
// bytes at immutable positions, never from producer state (§3.8, W1–W4).

import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeSync,
} from 'node:fs'
import { join } from 'node:path'
import type { DsStreamClient } from './stream-client'
import { StreamHttpError } from './stream-client'
import { casToken, encodeAppend, PositionCheckedReader } from './frames'
import type { FenceFrame } from './frames'
import { ProtocolError } from './errors'

/**
 * One pending commit, journaled (fsync'd) before its POST (§3.8). All LSNs
 * are pg-text strings; `eraOrdinal` is carried so recovery can rebuild the
 * era-qualified CAS token (W3) for the fence append.
 */
export interface JournalEntry {
  commitId: string
  eraId: string
  /** Stream path of the era, relative to the client's base URL. */
  eraPath: string
  eraOrdinal: number
  /** The tail offset the writer observed == the append's position. */
  expectedOffset: string
  /** W3 era-qualified CAS token sent as Stream-Seq. */
  casToken: string
  baseLsn: string
  endLsn: string
  sliceHash: string
  producerId: string
  producerEpoch: number
  producerSeq: number
  /** Monotonic per recovery attempt; starts at producerEpoch. */
  fenceEpoch: number
}

/** Durable producer identity for the committer using this journal dir. */
export interface JournalMeta {
  producerId: string
  epoch: number
}

/** Per-commit outcome of §3.8 recovery. */
export interface CommitOutcome {
  commitId: string
  outcome: 'landed' | 'lost' | 'indeterminate'
  /** True if a newer incarnation had already fenced this producer (403). */
  selfFenced: boolean
  /** The fence epoch this recovery attempt used for the entry. */
  fenceEpoch: number
}

/** Result of `CommitJournal.recover()`. */
export interface RecoveryReport {
  outcomes: CommitOutcome[]
  /**
   * The minimum producer epoch the next incarnation may write with (strictly
   * above every epoch used or observed during recovery, so its first append
   * at seq 0 can never collide with a fence tuple's dedup window). Zero when
   * nothing was pending.
   */
  epochFloor: number
}

const META_FILE = 'meta.json'

/** Write `data` to `dir/name` atomically: tmp → fsync → rename → fsync dir. */
function writeFileDurable(dir: string, name: string, data: string): void {
  const tmp = join(dir, `${name}.tmp`)
  const fd = openSync(tmp, 'w')
  try {
    writeSync(fd, data)
    fsyncSync(fd)
  } finally {
    closeSync(fd)
  }
  renameSync(tmp, join(dir, name))
  const dirFd = openSync(dir, 'r')
  try {
    fsyncSync(dirFd)
  } finally {
    closeSync(dirFd)
  }
}

/**
 * The pending-commit journal for one (host, database) pair, plus the §3.8
 * `recover()` algorithm run against the journaled era streams.
 */
export class CommitJournal {
  constructor(public readonly dir: string) {
    mkdirSync(dir, { recursive: true })
  }

  /** Read `meta.json`, or null if this journal has no producer identity yet. */
  readMeta(): JournalMeta | null {
    const p = join(this.dir, META_FILE)
    if (!existsSync(p)) return null
    return JSON.parse(readFileSync(p, 'utf8')) as JournalMeta
  }

  /** Durably write `meta.json` (tmp → fsync → rename → fsync dir). */
  writeMeta(meta: JournalMeta): void {
    writeFileDurable(this.dir, META_FILE, JSON.stringify(meta))
  }

  /** Durably record a pending commit BEFORE its POST (§3.8). */
  record(entry: JournalEntry): void {
    writeFileDurable(this.dir, `${entry.commitId}.json`, JSON.stringify(entry))
  }

  /** Delete a pending entry once its outcome is known. Idempotent. */
  resolve(commitId: string): void {
    const p = join(this.dir, `${commitId}.json`)
    if (existsSync(p)) unlinkSync(p)
  }

  /** All pending entries, ordered by (producerEpoch, producerSeq, commitId). */
  listPending(): JournalEntry[] {
    const entries = readdirSync(this.dir)
      .filter((f) => f.endsWith('.json') && f !== META_FILE)
      .map(
        (f) =>
          JSON.parse(readFileSync(join(this.dir, f), 'utf8')) as JournalEntry,
      )
    entries.sort(
      (a, b) =>
        a.producerEpoch - b.producerEpoch ||
        a.producerSeq - b.producerSeq ||
        a.commitId.localeCompare(b.commitId),
    )
    return entries
  }

  /**
   * §3.8 fence-then-read recovery. For each pending entry:
   *
   * 1. bump `fenceEpoch := max(fenceEpoch, epoch) + 1` (monotonic per
   *    attempt, and across the entries of one recovery pass so no two
   *    fences share a producer tuple), fsync the journal entry;
   * 2. HEAD the era → tail T (+closed); closed ⇒ skip to decide;
   * 3. FENCE: CAS-append a `'0'` frame `{v:1, eraId, expectedOffset: T}`
   *    with producer `(producerId, fenceEpoch, seq 0)` and Stream-Seq
   *    `casToken(eraOrdinal, T)` (W1: fences are CAS'd too). seq-conflict ⇒
   *    re-HEAD + retry; closed ⇒ decide; stale-epoch ⇒ a newer incarnation
   *    already fenced — mark self-fenced and decide;
   * 4. DECIDE from stream bytes at the journaled `expectedOffset`: the
   *    FIRST group's first frame is a `W` with matching commitId AND
   *    sliceHash ⇒ LANDED; any other frame or empty+closed ⇒ LOST (never
   *    re-POST old slice bytes); stream 404/gone ⇒ INDETERMINATE.
   *
   * Landed/lost entries are resolved (deleted); indeterminate entries stay
   * pending. `meta.json`'s epoch is raised to the highest epoch used or
   * observed, and the report's `epochFloor` is one above it.
   */
  async recover(client: DsStreamClient): Promise<RecoveryReport> {
    const pending = this.listPending()
    if (pending.length === 0) return { outcomes: [], epochFloor: 0 }

    const outcomes: CommitOutcome[] = []
    let maxEpoch = 0

    for (const entry of pending) {
      // Step 1: monotonic fence-epoch bump, fsync'd before any network I/O.
      entry.fenceEpoch =
        Math.max(entry.fenceEpoch, entry.producerEpoch, maxEpoch) + 1
      this.record(entry)
      maxEpoch = Math.max(maxEpoch, entry.fenceEpoch)

      let selfFenced = false
      let gone = false

      // Step 2: HEAD the journaled era.
      let tail: string | null = null
      let closed = false
      try {
        const head = await client.head(entry.eraPath)
        tail = head.nextOffset
        closed = head.closed
      } catch (err) {
        if (err instanceof StreamHttpError && err.status === 404) gone = true
        else throw err
      }

      // Step 3: FENCE (skipped if the era is closed or gone — closure
      // fences everything, and a gone era is decided as indeterminate).
      if (!gone && !closed && tail !== null) {
        for (;;) {
          const fence: FenceFrame = {
            type: '0',
            header: { v: 1, eraId: entry.eraId, expectedOffset: tail },
          }
          const res = await client.append(
            entry.eraPath,
            encodeAppend([fence]),
            {
              seq: casToken(entry.eraOrdinal, tail),
              expectedOffset: tail,
              producer: {
                id: entry.producerId,
                epoch: entry.fenceEpoch,
                seq: 0,
              },
            },
          )
          if (res.kind === 'ok') break
          if (res.kind === 'seq-conflict') {
            tail = (await client.head(entry.eraPath)).nextOffset
            continue
          }
          if (res.kind === 'closed') break
          if (res.kind === 'stale-epoch') {
            selfFenced = true
            maxEpoch = Math.max(maxEpoch, res.currentEpoch)
            break
          }
          throw new ProtocolError(
            `recovery fence for ${entry.commitId}: unexpected append result ` +
              `'${res.kind}'`,
          )
        }
      }

      // Step 4: DECIDE from bytes at the immutable journaled position.
      let outcome: CommitOutcome['outcome']
      if (gone) {
        outcome = 'indeterminate'
      } else {
        try {
          const read = await client.read(entry.eraPath, {
            offset: entry.expectedOffset,
          })
          if (read.bytes.length === 0) {
            // Nothing at the position. Closed ⇒ definitively lost. If we
            // were self-fenced the old-epoch in-flight POST can never be
            // accepted either ⇒ lost. Otherwise (unreachable when our own
            // fence landed past this offset) stay honest: indeterminate.
            outcome =
              read.closed || selfFenced || closed ? 'lost' : 'indeterminate'
          } else {
            const reader = new PositionCheckedReader(entry.expectedOffset)
            const first = reader.feed(read.bytes).next()
            if (first.done) {
              outcome = 'indeterminate' // partial garbage — cannot decide
            } else {
              const frame = first.value.frames[0]
              outcome =
                frame.type === 'W' &&
                frame.header.commitId === entry.commitId &&
                frame.header.sliceHash === entry.sliceHash
                  ? 'landed'
                  : 'lost'
            }
          }
        } catch (err) {
          if (err instanceof StreamHttpError && err.status === 404) {
            outcome = 'indeterminate'
          } else {
            throw err
          }
        }
      }

      if (outcome !== 'indeterminate') this.resolve(entry.commitId)
      outcomes.push({
        commitId: entry.commitId,
        outcome,
        selfFenced,
        fenceEpoch: entry.fenceEpoch,
      })
    }

    // Persist the epoch high-water mark so future incarnations (even ones
    // that skip recovery because nothing is pending) start above it.
    const meta = this.readMeta()
    if (meta) {
      if (maxEpoch > meta.epoch) this.writeMeta({ ...meta, epoch: maxEpoch })
    } else {
      this.writeMeta({ producerId: pending[0].producerId, epoch: maxEpoch })
    }

    return { outcomes, epochFloor: maxEpoch + 1 }
  }
}
