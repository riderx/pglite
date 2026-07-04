// Checkpoint datadir <-> tar bytes. A checkpoint object is the full recovery
// state of a datadir (§6.1) packed as a tar archive. M1 ships the entire
// datadir including pg_wal (the M0-1 rule: the shutdown-checkpoint record must
// travel with the checkpoint). Determinism is best-effort: entries are sorted
// so the same tree packs the same way run-to-run; byte-reproducibility across
// machines is explicitly out of scope at M1 (M1_PLAN checkpoint-object note).

import { create as tarCreate, extract as tarExtract } from 'tar'
import {
  mkdtempSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

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
 * Pack the datadir at `dir` into tar bytes. Entries are packed in sorted
 * order for run-to-run stability. Nothing is excluded (pg_wal ships too).
 */
export async function packDatadir(dir: string): Promise<Uint8Array> {
  const scratch = mkdtempSync(join(tmpdir(), 'pgl-ckpt-pack-'))
  const tarPath = join(scratch, 'checkpoint.tar')
  try {
    const entries = listEntriesSorted(dir)
    await tarCreate(
      {
        file: tarPath,
        cwd: dir,
        // Sorted entries + no compression: deterministic-ish, simple to verify.
        portable: true,
        noMtime: true,
      },
      entries,
    )
    return readFileSync(tarPath)
  } finally {
    rmSync(scratch, { recursive: true, force: true })
  }
}

/**
 * Extract checkpoint `bytes` into `destDir` (created if absent). Restores the
 * full datadir tree written by `packDatadir`.
 */
export async function extractDatadir(
  bytes: Uint8Array,
  destDir: string,
): Promise<void> {
  mkdirSync(destDir, { recursive: true })
  const scratch = mkdtempSync(join(tmpdir(), 'pgl-ckpt-extract-'))
  const tarPath = join(scratch, 'checkpoint.tar')
  try {
    // tar.extract wants a real file; write the bytes out first.
    const { writeFileSync } = await import('node:fs')
    writeFileSync(tarPath, bytes)
    await tarExtract({
      file: tarPath,
      cwd: destDir,
      preservePaths: false,
    })
  } finally {
    rmSync(scratch, { recursive: true, force: true })
  }
}
