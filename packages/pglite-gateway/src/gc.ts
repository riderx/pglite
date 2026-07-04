// GcExecutor (§6.4, execution) — the gateway-side garbage collector. Four
// sweeps, each honoring `gc_grace_ms` and every live pin (fork lineage +
// explicit `pins` rows):
//
//   1. Orphan era attempts   — era_attempts registered but never promoted,
//                              older than grace → DELETE stream, drop row.
//   2. Sealed-era streams     — a sealed era whose final_lsn is covered by a
//                              checkpoint on the SAME database, no live child
//                              fork inside its range, no covering pin → DELETE.
//   3. Checkpoint rows        — keep the latest per db, plus any covered by a
//                              pin or referenced by a child's checkpoint row;
//                              prune the rest.
//   4. Unreferenced objects   — checkpoint objects no row references anymore
//                              (content-addressed: shared across databases).
//
// CRITICAL DS-server fact (verified against durable-streams server/store):
// DELETE of a stream that HAS forks does NOT refuse — the server soft-deletes
// it and blocks path reuse (409 "active forks") but the DELETE call itself
// returns 204. The parent's data is only hard-deleted when the LAST fork is
// removed (refCount → 0 cascade). Therefore the CONTROL PLANE lineage/pins are
// the authoritative protection: GC must NOT delete a parent era stream while a
// live child fork references it — the DS server will not save us. We enforce
// that here (sweep 2's lineage join), so we never issue the DELETE at all.

import { parseLsn } from '@electric-sql/pglite-cell'
import type { GatewayCore } from './core'
import type { EraRow, LineageRow, PinRow } from './control-plane'

/** Per-run GC report. `kept` tallies why survivors were spared, by reason. */
export interface GcReport {
  deletedStreams: number
  deletedCheckpoints: number
  deletedObjects: number
  kept: Record<string, number>
}

/** Offset tokens are fixed-width, so byte-wise string compare is numeric. */
function offsetInRange(offset: string, lo: string, hi: string): boolean {
  return offset >= lo && offset <= hi
}

export class GcExecutor {
  constructor(private readonly core: GatewayCore) {}

  /** Run all four sweeps. Scope to `databaseId` when given, else all databases. */
  async run(databaseId?: string): Promise<GcReport> {
    const report: GcReport = {
      deletedStreams: 0,
      deletedCheckpoints: 0,
      deletedObjects: 0,
      kept: {},
    }
    const keep = (reason: string): void => {
      report.kept[reason] = (report.kept[reason] ?? 0) + 1
    }

    const cp = this.core.catalog
    // TTL sweep first so expired pins stop protecting anything this run.
    await cp.expirePins()

    const dbs = databaseId
      ? [await cp.getDatabaseById(databaseId)].filter(
          (d): d is NonNullable<typeof d> => d !== null,
        )
      : await cp.listDatabases()

    for (const db of dbs) {
      const graceMs = Number(db.gcGraceMs)

      // --- Sweep 1: orphan era attempts --------------------------------
      // listOrphanAttempts is global; filter to this db.
      const orphans = (await cp.listOrphanAttempts(graceMs)).filter(
        (a) => a.databaseId === db.id,
      )
      for (const att of orphans) {
        const client = this.core.streamClientForGc(db.id)
        try {
          await client.deleteStream(att.path)
          report.deletedStreams++
        } catch {
          // Stream may never have been PUT (attempt registered pre-PUT, then
          // the rotator crashed) — deleting a nonexistent stream is fine.
        }
        await cp.deleteEraAttempt(db.id, att.eraId)
      }

      // --- Sweep 2: sealed-era streams ---------------------------------
      const eras = await cp.erasOf(db.id)
      const checkpoints = await cp.checkpointsOf(db.id)
      const children = await cp.childrenOf(db.id)
      const pins = await cp.livePins(db.id)

      // Highest checkpoint snapEnd (attach coverage) on THIS database.
      let maxSnapEnd = -1n
      for (const c of checkpoints) {
        const s = parseLsn(c.snapEnd)
        if (s > maxSnapEnd) maxSnapEnd = s
      }

      for (const era of eras) {
        if (!era.sealed || era.sealedFinalLsn === null) continue
        const finalLsn = parseLsn(era.sealedFinalLsn)
        // Covered by a checkpoint? (a checkpoint at/after the era's final LSN
        // means a joiner never needs to tail this era).
        if (!(maxSnapEnd >= finalLsn)) {
          keep('sealed-era:no-covering-checkpoint')
          continue
        }
        if (this.eraPinnedByFork(era, children)) {
          keep('sealed-era:child-fork')
          continue
        }
        if (this.eraPinnedByPin(era, pins)) {
          keep('sealed-era:pin')
          continue
        }
        const client = this.core.streamClientForGc(db.id)
        try {
          await client.deleteStream(era.path)
          report.deletedStreams++
        } catch {
          keep('sealed-era:delete-failed')
        }
      }

      // --- Sweep 3: checkpoint rows ------------------------------------
      // Keep the latest (highest LSN); keep any covered by a pin or referenced
      // by a child's checkpoint row; prune the rest.
      if (checkpoints.length > 0) {
        // `checkpointsOf` is ascending; the last is the latest.
        const latest = checkpoints[checkpoints.length - 1]
        const childRefObjects = new Set<string>()
        for (const child of children) {
          for (const cc of await cp.checkpointsOf(child.childId)) {
            childRefObjects.add(cc.objectRef)
          }
        }
        for (const c of checkpoints) {
          if (c.lsn === latest.lsn) {
            keep('checkpoint:latest')
            continue
          }
          if (this.checkpointPinned(c.snapEnd, pins)) {
            keep('checkpoint:pin')
            continue
          }
          if (childRefObjects.has(c.objectRef)) {
            keep('checkpoint:child-referenced')
            continue
          }
          await cp.deleteCheckpoint(db.id, c.lsn)
          report.deletedCheckpoints++
        }
      }
    }

    // --- Sweep 4: unreferenced objects (global; content-addressed) ------
    // An object survives while ANY checkpoint row (any database — a fork shares
    // its parent's object) still references it.
    const referenced = new Set(await cp.allReferencedObjects())
    for (const ref of await this.core.listObjects()) {
      if (referenced.has(ref)) continue
      if (await this.core.deleteObject(ref)) report.deletedObjects++
    }

    return report
  }

  /** True iff a live child fork's fork point lies within this era's range. */
  private eraPinnedByFork(era: EraRow, children: LineageRow[]): boolean {
    const hi = era.sealedFinalOffset ?? era.baseOffset
    for (const child of children) {
      if (offsetInRange(child.forkOffset, era.baseOffset, hi)) return true
    }
    return false
  }

  /** True iff a live pin's offset lies within this era's range. */
  private eraPinnedByPin(era: EraRow, pins: PinRow[]): boolean {
    const hi = era.sealedFinalOffset ?? era.baseOffset
    for (const pin of pins) {
      if (offsetInRange(pin.pinnedOffset, era.baseOffset, hi)) return true
    }
    return false
  }

  /** True iff a live pin's LSN is at/before a checkpoint's snapEnd. */
  private checkpointPinned(snapEnd: string, pins: PinRow[]): boolean {
    const s = parseLsn(snapEnd)
    for (const pin of pins) {
      if (parseLsn(pin.pinnedLsn) <= s) return true
    }
    return false
  }
}
