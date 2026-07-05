// Graduation (M6, design §15 / OQ7) — the documented migration path OUT of
// the cell fleet: a LOGICAL export (pg_dump) of a database at a
// linearizable-fresh head, plus a manifest snapshot pinning exactly which
// stream position the dump reflects.
//
// OQ7 (answered NO, definitive — M6_PLAN): a PGlite (wasm32) datadir cannot
// boot under stock native 64-bit Postgres (the USE_FLOAT8_BYVAL pg_control
// gate). Physical graduation is therefore closed; a logical dump/restore is
// THE path. The dump restores into any Postgres — a plain PGlite, or stock
// native Postgres — via `exec(sql)`.
//
// Mechanics (all reused machinery, no shared-state mutation):
//   1. linearizableSync() on the runtime — catch the TRUE stream head past
//      any observed tail, so the export reflects every commit acked anywhere
//      up to now (§7 linearizable freshness).
//   2. Read the manifest checkpoint object, extract it to a scratch datadir,
//      and materialize the W slices (checkpoint.snapEnd .. head] on top via
//      ordinary crash recovery (the same hydrate+tail path activate() and
//      the checkpoint worker use). This yields a clean datadir AT HEAD.
//   3. Open a plain, throwaway PGlite on that datadir and run
//      `@electric-sql/pglite-tools` pgDump against it — the tools API accepts
//      an EXISTING PGlite instance (`pgDump({ pg })`), so no bespoke driving
//      is needed. --inserts is applied by pgDump, so the SQL restores by
//      `exec()`.
//   4. Return { sql, manifestSnapshot } — the snapshot records databaseId,
//      the head LSN/offset the dump reflects, the checkpoint ref it built on,
//      and the era ordinal.
//
// PGDUMP INTEGRATION PATH (reported at M6 close): pgDump is driven against a
// FRESH PGlite opened on a materialized-at-head scratch datadir — NOT a live
// cell. pgDump issues `DEALLOCATE ALL` and mutates search_path on its target
// connection (documented in its README), and it needs a stable, quiescent
// instance; a throwaway materialized datadir gives exactly that without
// perturbing any serving cell or its capture cursor.

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { File as NodeFile } from 'node:buffer'
import { PGlite } from '@electric-sql/pglite'
import { pgDump } from '@electric-sql/pglite-tools/pg_dump'
import {
  formatLsn,
  materializeAtHead,
  parseLsn,
} from '@electric-sql/pglite-cell'
import { extractDatadir } from '@electric-sql/pglite-gateway'
import type { DatabaseRuntime } from './database-runtime'

// `@electric-sql/pglite-tools` pgDump returns its result as a `File`, which
// is a browser/Node-20 global. On Node 18 the constructor is only exported
// from `node:buffer` — surface it globally so pgDump works there too.
const g = globalThis as unknown as { File?: unknown }
if (typeof g.File === 'undefined') g.File = NodeFile

/** The stream position + provenance a graduation dump reflects. */
export interface GraduationManifestSnapshot {
  databaseId: string
  /** pg_lsn text of the head the dump reflects. */
  headLsn: string
  /** Stream offset of that head. */
  headOffset: string
  /** Content-address ref of the checkpoint object the export built on. */
  checkpointRef: string
  /** Era ordinal current at export time. */
  eraOrdinal: number
}

export interface GraduationResult {
  /** The pg_dump SQL (restorable into any Postgres via `exec(sql)`). */
  sql: string
  manifestSnapshot: GraduationManifestSnapshot
}

/**
 * Produce a logical export of `runtime`'s database at a linearizable-fresh
 * head. The runtime must be (or is made) active. Does NOT mutate the shared
 * base or publish anything — it materializes a private scratch datadir.
 */
export async function graduateDatabase(
  runtime: DatabaseRuntime,
): Promise<GraduationResult> {
  await runtime.ensureActive()

  // Linearizable-fresh: confirm the TRUE head (catch-up past any observed
  // tail) so the export reflects every commit acked anywhere up to now.
  await runtime.linearizableSync()

  const manifest = runtime.manifest
  const tailer = runtime.tailer
  const head = tailer.head
  const checkpointRef = manifest.checkpoint.ref
  const snapEnd = parseLsn(manifest.checkpoint.snapEnd)

  const scratchRoot = mkdtempSync(join(tmpdir(), 'pgl-graduate-'))
  const dataDir = join(scratchRoot, 'datadir')
  let pg: PGlite | null = null
  try {
    // Hydrate the checkpoint datadir, then materialize the era tail on top
    // (crash recovery replays the W slices; the clean close brings the dir
    // to a genuine head). Slices whose baseLsn >= snapEnd chain from the
    // checkpoint (materialize validates the contiguity).
    const ckptBytes = await runtime.gateway.getObject(checkpointRef)
    await extractDatadir(ckptBytes, dataDir)
    const slices = tailer.slicesSince(snapEnd)
    await materializeAtHead({ baseDir: dataDir, slices })

    // Open a plain throwaway PGlite on the materialized datadir and dump it.
    pg = new PGlite(dataDir)
    await pg.query('select 1') // force boot / recovery to settle
    const file = await pgDump({ pg })
    const sql = await file.text()

    return {
      sql,
      manifestSnapshot: {
        databaseId: runtime.databaseId,
        headLsn: formatLsn(head.lsn),
        headOffset: head.offset,
        checkpointRef,
        eraOrdinal: manifest.era.ordinal,
      },
    }
  } finally {
    if (pg) await pg.close().catch(() => undefined)
    rmSync(scratchRoot, { recursive: true, force: true })
  }
}
