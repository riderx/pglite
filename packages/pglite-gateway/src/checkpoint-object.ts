// Checkpoint datadir <-> object bytes.
//
// A checkpoint object is the recovery state of a datadir (§6.1). M1 (v1) shipped
// the ENTIRE datadir including all of pg_wal as a plain (uncompressed) tar — a
// ~99 MB object for a trivial database, dominated by pristine 16 MB WAL
// segments that attach never reads.
//
// v2 (M2_PLAN "Checkpoint slimming — the 99 MB fix"):
//   - Exclude every `pg_wal/<segment>` file EXCEPT the one containing the
//     checkpoint record (derived from readControl().checkPoint via
//     lsnToSegment/walSegmentName). Attach reads exactly the two pages holding
//     that record; older/newer segments are never read (M0-2), and
//     writeWalRange recreates future segments zero-filled on demand.
//   - Keep pg_wal SUBDIRECTORIES (archive_status, summaries) and any non-segment
//     files there.
//   - gzip the tar (node:zlib, level 6).
//
// extractDatadir sniffs the magic bytes (gzip 1f 8b vs a POSIX tar) and handles
// both, so v1 objects still extract (backward compat).

import { create as tarCreate, extract as tarExtract } from 'tar'
import { gzipSync, gunzipSync } from 'node:zlib'
import {
  mkdtempSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
  rmSync,
  statSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  readControl,
  lsnToSegment,
  walSegmentName,
} from '@electric-sql/pglite-cell'

/** A WAL segment file name is 24 uppercase hex chars (tli+logid+seg). */
const WAL_SEG_RE = /^[0-9A-F]{24}$/

/** Recursively list all file/dir entries under `dir`, sorted, dir-relative. */
function listEntriesSorted(dir: string): string[] {
  const out: string[] = []
  const walk = (rel: string): void => {
    const abs = rel === '' ? dir : join(dir, rel)
    const names = readdirSync(abs).sort()
    for (const name of names) {
      const childRel = rel === '' ? name : `${rel}/${name}`
      const st = statSync(join(dir, childRel))
      if (st.isDirectory()) {
        out.push(childRel + '/')
        walk(childRel)
      } else {
        out.push(childRel)
      }
    }
  }
  walk('')
  return out
}

/**
 * The WAL segment file name (24 hex) that holds the checkpoint record for
 * `dir`, so packDatadir can keep exactly that one and drop the rest.
 */
function checkpointSegmentName(dir: string): string {
  const { segno } = lsnToSegment(readControl(dir).checkPoint)
  return walSegmentName(segno)
}

/**
 * Pack the datadir at `dir` into gzip'd tar bytes (v2). Entries are packed in
 * sorted order for run-to-run stability. All `pg_wal/<segment>` files are
 * excluded except the one holding the checkpoint record; pg_wal subdirectories
 * and non-segment files are kept.
 */
export async function packDatadir(dir: string): Promise<Uint8Array> {
  const keepSeg = checkpointSegmentName(dir)
  const scratch = mkdtempSync(join(tmpdir(), 'pgl-ckpt-pack-'))
  const tarPath = join(scratch, 'checkpoint.tar')
  try {
    const entries = listEntriesSorted(dir).filter((rel) => {
      // Only filter direct pg_wal segment files: `pg_wal/<24-hex>`.
      const m = /^pg_wal\/([^/]+)$/.exec(rel)
      if (!m) return true
      if (!WAL_SEG_RE.test(m[1])) return true // keep non-segment files
      return m[1] === keepSeg // drop every segment but the checkpoint's
    })
    await tarCreate(
      {
        file: tarPath,
        cwd: dir,
        portable: true,
        noMtime: true,
      },
      entries,
    )
    // gzip level 6 (node:zlib default) — no new dependency.
    return gzipSync(readFileSync(tarPath), { level: 6 })
  } finally {
    rmSync(scratch, { recursive: true, force: true })
  }
}

/** True iff `bytes` starts with the gzip magic (1f 8b). */
function isGzip(bytes: Uint8Array): boolean {
  return bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b
}

/**
 * Extract checkpoint `bytes` into `destDir` (created if absent). Transparently
 * handles both v2 (gzip) and v1 (plain tar) by sniffing the magic bytes.
 */
export async function extractDatadir(
  bytes: Uint8Array,
  destDir: string,
): Promise<void> {
  mkdirSync(destDir, { recursive: true })
  const tar = isGzip(bytes) ? gunzipSync(bytes) : bytes
  const scratch = mkdtempSync(join(tmpdir(), 'pgl-ckpt-extract-'))
  const tarPath = join(scratch, 'checkpoint.tar')
  try {
    writeFileSync(tarPath, tar)
    await tarExtract({
      file: tarPath,
      cwd: destDir,
      preservePaths: false,
    })
  } finally {
    rmSync(scratch, { recursive: true, force: true })
  }
}
