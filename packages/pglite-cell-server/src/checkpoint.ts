// checkpointDatabase — the M1e checkpoint worker (§6.1 / §2.4). Any active
// runtime can build a checkpoint idempotently: bring the shared base to a
// genuine stream position (write-attach, publishing the sync slice), pack
// that datadir into a content-addressed object, CAS-append a K frame at the
// era head, and register a control-plane checkpoint row so `getManifest`
// serves it as the LATEST checkpoint. Joiners then hydrate this object and
// tail from its recorded streamOffset — bounding the M1c "ever-longer tail
// replay" cost (§2.4: checkpoint cadence is the central dial).
//
// Mechanics (all idempotent, all reused from existing machinery):
//   - ensureAtHeadCanonical materializes + publishes the landed sync slice ⇒
//     the canonical dir is clean at the stream head. Its clean shutdown
//     position IS the head (snapEnd); its pg_control checkpoint record sits
//     at C = snapEnd - 120 (SHUTDOWN_CKPT_REC_ALIGNED). The sync-slice
//     append's nextOffset is the streamOffset a joiner tails from.
//   - packDatadir(canonicalDir) -> gateway.putObject ⇒ checkpointRef; the
//     object ref already encodes the sha256, so we reuse it (no re-hash).
//   - CAS-append K {lsn: C, snapEnd, checkpointRef, sha256}. A seq-conflict
//     ⇒ catch up + retry (bounded). Commits landing between the sync slice
//     and the K frame are FINE — their baseLsn >= snapEnd, so joiners replay
//     them on top of the checkpoint.
//   - registerCheckpoint ⇒ control-plane row; getManifest serves it latest.
//
// Idempotent: a second run with no intervening writes finds the canonical
// dir already at head AND a checkpoint row already at C ⇒ `{ skipped: true }`.

import { formatLsn, parseLsn } from '@electric-sql/pglite-cell'
import type { KFrame } from '@electric-sql/pglite-cell'
import { shutdownCheckpointStart } from '@electric-sql/pglite-cell'
import { packDatadir } from '@electric-sql/pglite-gateway'
import type { DatabaseRuntime } from './database-runtime'
import { AdvanceRaceError } from './errors'

export interface CheckpointReport {
  /** True when nothing changed since the last checkpoint (no-op run). */
  skipped: boolean
  /** pg_lsn text of the checkpoint record C (= snapEnd - 120). */
  lsn: string
  /** pg_lsn text of the snapshot end (the attach point == stream head). */
  snapEnd: string
  /** The stream offset a joiner tails from (the sync slice's nextOffset). */
  streamOffset: string
  /** Content-address ref of the packed datadir object. */
  checkpointRef: string
  /** Size of the packed checkpoint object in bytes (0 when skipped). */
  objectBytes: number
}

/**
 * Build (or no-op) a checkpoint for an active runtime. Safe to run from any
 * host with the runtime active; racing workers are resolved by the K-frame
 * CAS + the content-addressed object store + the (database, lsn)-guarded
 * checkpoint row (all idempotent). Bounded retry loop on sync/K CAS races.
 */
export async function checkpointDatabase(
  runtime: DatabaseRuntime,
): Promise<CheckpointReport> {
  await runtime.ensureActive()

  // Cross-host K-frame ping-pong (M4): two hosts checkpointing the same
  // database alternate losses — each K loss re-runs the canonical ensure,
  // whose fresh SYNC slice moves the head and defeats the OTHER host's
  // in-flight K. Double the solo bound and jitter the retries (below) so
  // one side wins quickly instead of strict alternation to exhaustion.
  const maxAttempts = runtime.opts.attachAttempts * 2
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    // (1) Bring the shared base to a genuine stream position (write-attach):
    // materialize + publish the landed sync slice ⇒ canonical dir clean at
    // head. `canonical.offset` is that append's nextOffset (the streamOffset
    // a joiner tails from); the dir's clean shutdown position is `snapEnd`.
    const canonical = await runtime.baseDirs.ensureAtHeadCanonical(
      runtime.tailer,
      runtime.committer,
    )
    const snapEnd = canonical.lsn
    const streamOffset = canonical.offset
    const checkpointC = shutdownCheckpointStart(snapEnd)
    const lsnText = formatLsn(checkpointC)
    const snapEndText = formatLsn(snapEnd)

    // Idempotency: canonical already at head AND a checkpoint row exists at C
    // ⇒ nothing changed since the last checkpoint. (One era per db, and
    // checkpoint LSNs are monotone, so the latest row at C means C's row.)
    const latest = await runtime.gateway.latestCheckpoint(runtime.databaseId)
    if (
      latest !== null &&
      parseLsn(latest.lsn) === checkpointC &&
      canonical.lsn === runtime.tailer.head.lsn
    ) {
      return {
        skipped: true,
        lsn: lsnText,
        snapEnd: snapEndText,
        streamOffset: latest.streamOffset,
        checkpointRef: latest.objectRef,
        objectBytes: 0,
      }
    }

    // (2) Pack the canonical dir -> content-addressed object. The ref
    // encodes the sha256; reuse it rather than re-hashing.
    const packed = await packDatadir(runtime.baseDirs.canonicalDir)
    const { ref: checkpointRef } = await runtime.gateway.putObject(packed)
    const sha256 = checkpointRef // sha256:<hex>

    // (3) CAS-append the K frame at the era head. A seq-conflict means a
    // commit (or another worker) landed between the sync slice and here —
    // catch up and retry the whole loop (the canonical position may need to
    // re-advance; commits since snapEnd replay on top of the checkpoint).
    const res = await runtime.committer.appendControl((expectedOffset) => {
      const frame: KFrame = {
        type: 'K',
        header: {
          v: 1,
          // The tailer's CURRENT era at build time: appendControl re-invokes
          // this callback after an era hop (M2), and the manifest's era may
          // be stale after a rotation.
          eraId: runtime.tailer.currentEra.id,
          expectedOffset,
          lsn: lsnText,
          snapEnd: snapEndText,
          checkpointRef,
          sha256,
        },
      }
      return [frame]
    })
    if (!res.landed) {
      await runtime.tailer.catchUp()
      // Jittered stagger before re-advancing (see maxAttempts note).
      await new Promise((r) => setTimeout(r, 10 + Math.random() * 90))
      continue // lost the K-frame CAS: re-advance + retry
    }

    // (4) Control-plane row: getManifest now serves this as the latest
    // checkpoint (guarded on (database, lsn); re-put safe).
    await runtime.gateway.registerCheckpoint(runtime.databaseId, {
      lsn: lsnText,
      snapEnd: snapEndText,
      streamOffset,
      objectRef: checkpointRef,
    })

    // Refresh the runtime's latest-checkpoint view. appendControl already
    // advanced the tailer locally past our own K frame; this confirms
    // against the server before returning (awaited — a dangling catch-up
    // racing a server teardown would surface as an unhandled rejection).
    await runtime.tailer.catchUp()

    return {
      skipped: false,
      lsn: lsnText,
      snapEnd: snapEndText,
      streamOffset,
      checkpointRef,
      objectBytes: packed.length,
    }
  }
  throw new AdvanceRaceError(maxAttempts)
}
