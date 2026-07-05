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
import { join, dirname } from 'node:path'
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
 * The v2/v3 pg_wal pruning predicate: given the checkpoint segment name, decide
 * whether a datadir-relative entry survives. Drops every direct
 * `pg_wal/<segment>` file except the checkpoint's; keeps pg_wal subdirectories,
 * non-segment pg_wal files, and everything outside pg_wal.
 */
function keepEntry(rel: string, keepSeg: string): boolean {
  // Only filter direct pg_wal segment files: `pg_wal/<24-hex>`.
  const m = /^pg_wal\/([^/]+)$/.exec(rel)
  if (!m) return true
  if (!WAL_SEG_RE.test(m[1])) return true // keep non-segment files
  return m[1] === keepSeg // drop every segment but the checkpoint's
}

/**
 * Options for `packDatadir`. `format` selects the checkpoint object format:
 * 1 = plain tar (legacy), 2 = gzip'd tar (default), 3 = per-file
 * content-addressed + manifest. Format 3 requires a `store` handle (each file
 * and the manifest are uploaded as content-addressed objects); `packDatadir`
 * returns the manifest ref bytes for symmetry, but callers that want the v3
 * stats should call `packDatadirV3` directly.
 */
export interface PackDatadirOpts {
  format?: 1 | 2 | 3
  /** Required when `format === 3`: the object store to upload files into. */
  store?: ObjectPutStore
}

/**
 * Pack the datadir at `dir`. Default (format 2 or 1) returns the checkpoint
 * object bytes directly. Format 3 uploads each file + the manifest to `store`
 * and returns the manifest ref encoded as UTF-8 bytes (`sha256:...`); prefer
 * `packDatadirV3` when you need the file/byte stats.
 */
export async function packDatadir(
  dir: string,
  opts: PackDatadirOpts = {},
): Promise<Uint8Array> {
  const format = opts.format ?? 2
  if (format === 3) {
    if (!opts.store) {
      throw new Error('packDatadir(format:3) requires a store handle')
    }
    const { manifestRef } = await packDatadirV3(dir, opts.store)
    return new TextEncoder().encode(manifestRef)
  }
  const keepSeg = checkpointSegmentName(dir)
  const scratch = mkdtempSync(join(tmpdir(), 'pgl-ckpt-pack-'))
  const tarPath = join(scratch, 'checkpoint.tar')
  try {
    const entries = listEntriesSorted(dir).filter((rel) =>
      keepEntry(rel, keepSeg),
    )
    if (format === 1) {
      await tarCreate(
        { file: tarPath, cwd: dir, portable: true, noMtime: true },
        entries,
      )
      return readFileSync(tarPath)
    }
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

// --- v3: per-file content-addressed + manifest -------------------------------
//
// v3 (M7 fixed decision 3) uploads EACH datadir file as its own
// content-addressed object and describes the set with a small JSON manifest
// (itself content-addressed, same `sha256:` namespace). Unchanged relation
// files dedupe across checkpoints for free — this is the incremental-checkpoint
// win and the substrate for the lazy VFS (W3): eager files hydrate a skeleton
// dir at attach; lazy relation forks fault in per-chunk via ranged reads.
//
// Classification (as implemented):
//   LAZY  = a file whose datadir-relative path is `base/<db>/<name>` or
//           `global/<name>` where <name> is a RELATION FORK filename:
//             <relfilenode>            main fork
//             <relfilenode>_fsm        free-space map
//             <relfilenode>_vm         visibility map
//             <relfilenode>_init       unlogged-init fork
//           with <relfilenode> a run of digits and an OPTIONAL `.<segno>`
//           segment suffix (a relation > 1 GB spills to `<node>.1`, `.2`…).
//           Regex: /^[0-9]+(_(fsm|vm|init))?(\.[0-9]+)?$/  applied to the
//           basename, gated on the dir being `base/<db>` or `global`.
//   EAGER = everything else: pg_control, SLRUs (pg_xact/pg_multixact/…),
//           pg_filenode.map, pg_internal.init (NON-numeric basename → eager),
//           PG_VERSION, all conf files, and the single retained pg_wal
//           checkpoint segment (v2 pruning reused verbatim).
//
// Note `_fsm` IS classified lazy here (the plan left it open) — it is a
// per-relation fork keyed by relfilenode exactly like `_vm`, and the redo path
// faults it like any other relation fork.

/** Minimal store surface v3 needs: idempotent content-addressed put. */
export interface ObjectPutStore {
  put(bytes: Uint8Array): Promise<{ ref: string }>
}

/** Minimal store surface v3 extract/read needs: fetch by ref. */
export interface ObjectGetStore {
  get(ref: string): Promise<Uint8Array>
}

export type CheckpointFileKind = 'eager' | 'lazy'

/** One entry in a v3 manifest: a datadir file and the object holding its bytes. */
export interface CheckpointFileEntry {
  /** Datadir-relative path with `/` separators (e.g. `base/5/1259_vm`). */
  path: string
  size: number
  /** Content-address of this file's bytes (`sha256:...`). */
  ref: string
  kind: CheckpointFileKind
}

/** The v3 checkpoint manifest (uploaded content-addressed; its ref is the checkpoint ref). */
export interface CheckpointManifestV3 {
  v: 3
  files: CheckpointFileEntry[]
  /**
   * Datadir-relative paths of directories that must exist on restore. Includes
   * EMPTY directories (pg_notify, pg_wal/archive_status, pg_tblspc, …) which a
   * per-file restore would otherwise drop — PGlite's recovery requires them.
   * Non-empty dirs are recreated implicitly by their files, but are listed here
   * too for completeness (mkdir is idempotent).
   */
  dirs: string[]
}

/** Basename of a relation fork file (main/fsm/vm/init, optional `.N` segment). */
const RELATION_FORK_RE = /^[0-9]+(_(fsm|vm|init))?(\.[0-9]+)?$/

/**
 * Classify a datadir-relative path as eager or lazy per the v3 rules above.
 * Lazy iff it lives directly under `base/<db>/` or `global/` AND its basename
 * is a relation fork filename; eager otherwise.
 */
export function classifyDatadirFile(rel: string): CheckpointFileKind {
  const parts = rel.split('/')
  const base = parts[parts.length - 1]
  if (!RELATION_FORK_RE.test(base)) return 'eager'
  // `global/<name>` — exactly two segments.
  if (parts.length === 2 && parts[0] === 'global') return 'lazy'
  // `base/<db>/<name>` — exactly three segments, numeric <db>.
  if (parts.length === 3 && parts[0] === 'base' && /^[0-9]+$/.test(parts[1])) {
    return 'lazy'
  }
  return 'eager'
}

/**
 * Pack the datadir at `dir` into v3 form: upload each surviving file as its own
 * content-addressed object (idempotent — unchanged files dedupe across
 * checkpoints), then upload the JSON manifest content-addressed and return its
 * ref. pg_wal pruning matches v2 (only the checkpoint segment survives).
 */
export async function packDatadirV3(
  dir: string,
  store: ObjectPutStore,
): Promise<{
  manifestRef: string
  files: CheckpointFileEntry[]
  eagerBytes: number
  lazyBytes: number
}> {
  const keepSeg = checkpointSegmentName(dir)
  const entries = listEntriesSorted(dir)
  // Directories are marked with a trailing '/'; keep them (empty ones matter).
  const dirs = entries
    .filter((rel) => rel.endsWith('/'))
    .map((rel) => rel.slice(0, -1))
  // Files, pg_wal-pruned.
  const rels = entries.filter(
    (rel) => !rel.endsWith('/') && keepEntry(rel, keepSeg),
  )
  const files: CheckpointFileEntry[] = []
  let eagerBytes = 0
  let lazyBytes = 0
  for (const rel of rels) {
    const bytes = readFileSync(join(dir, rel))
    const { ref } = await store.put(bytes)
    const kind = classifyDatadirFile(rel)
    files.push({ path: rel, size: bytes.length, ref, kind })
    if (kind === 'lazy') lazyBytes += bytes.length
    else eagerBytes += bytes.length
  }
  const manifest: CheckpointManifestV3 = { v: 3, files, dirs }
  const manifestBytes = new TextEncoder().encode(JSON.stringify(manifest))
  const { ref: manifestRef } = await store.put(manifestBytes)
  return { manifestRef, files, eagerBytes, lazyBytes }
}

/** True iff `ref` is a well-formed `sha256:<64 hex>` object ref. */
export function isObjectRef(ref: string): boolean {
  return /^sha256:[0-9a-f]{64}$/.test(ref)
}

/** Read + parse a v3 manifest object by ref. Throws if it is not a v3 manifest. */
export async function readCheckpointManifest(
  ref: string,
  store: ObjectGetStore,
): Promise<CheckpointManifestV3> {
  const bytes = await store.get(ref)
  return parseManifest(bytes)
}

/** Parse manifest bytes, validating the `v:3` shape. */
function parseManifest(bytes: Uint8Array): CheckpointManifestV3 {
  let obj: unknown
  try {
    obj = JSON.parse(new TextDecoder().decode(bytes))
  } catch (err) {
    throw new Error(
      `not a v3 checkpoint manifest (invalid JSON): ${err instanceof Error ? err.message : String(err)}`,
    )
  }
  if (
    typeof obj !== 'object' ||
    obj === null ||
    (obj as { v?: unknown }).v !== 3 ||
    !Array.isArray((obj as { files?: unknown }).files)
  ) {
    throw new Error('not a v3 checkpoint manifest (missing v:3 / files[])')
  }
  const m = obj as CheckpointManifestV3
  if (!Array.isArray(m.dirs)) m.dirs = [] // tolerate older/dir-less manifests
  return m
}

/**
 * True iff the checkpoint at `ref` is v3 (per-file content-addressed +
 * manifest) — the W4 lazy-worker capability probe. Resolves the object via
 * `store` and sniffs its format (v3 JSON manifest vs v1/v2 archive). A fetch
 * failure returns false (treat as non-lazy-capable rather than throwing).
 */
export async function checkpointIsV3(
  ref: string,
  store: ObjectGetStore,
): Promise<boolean> {
  try {
    return isV3Manifest(await store.get(ref))
  } catch {
    return false
  }
}

/** True iff `bytes` is a JSON v3 manifest body (cheap sniff before full parse). */
function isV3Manifest(bytes: Uint8Array): boolean {
  // Skip leading whitespace; a manifest is a JSON object starting with `{`.
  let i = 0
  while (i < bytes.length && (bytes[i] === 0x20 || bytes[i] === 0x0a)) i++
  if (bytes[i] !== 0x7b /* { */) return false
  try {
    parseManifest(bytes)
    return true
  } catch {
    return false
  }
}

/**
 * Restore a v3 checkpoint identified by `manifestRef` into `destDir`.
 *
 * Full restore (default): every file — eager and lazy — is fetched from `store`
 * and written to its datadir path. When `lazySkip` is true, ONLY eager files
 * are materialized (the W3 lazy-attach skeleton) and the lazy file list is
 * returned so the caller can fault those forks in on demand; the eager skeleton
 * is NOT bootable on its own (that is W3's job).
 */
export async function extractDatadirV3(
  manifestRef: string,
  store: ObjectGetStore,
  destDir: string,
  opts: { lazySkip?: boolean } = {},
): Promise<{ lazyFiles: CheckpointFileEntry[] }> {
  const manifest = await readCheckpointManifest(manifestRef, store)
  mkdirSync(destDir, { recursive: true })
  // Recreate every directory first (empty ones — pg_notify, archive_status, …
  // — would otherwise be dropped and PGlite recovery fails).
  for (const d of manifest.dirs) {
    mkdirSync(join(destDir, d), { recursive: true })
  }
  const lazyFiles: CheckpointFileEntry[] = []
  for (const entry of manifest.files) {
    if (entry.kind === 'lazy') {
      lazyFiles.push(entry)
      if (opts.lazySkip) continue
    }
    const abs = join(destDir, entry.path)
    mkdirSync(dirname(abs), { recursive: true })
    writeFileSync(abs, await store.get(entry.ref))
  }
  return { lazyFiles }
}

/**
 * The single format-agnostic restore entry point the cell-server keeps calling.
 * Accepts either a checkpoint ref (resolved via `store`) OR raw object bytes,
 * and sniffs the format: a v3 JSON manifest ⇒ v3 (needs `store`); gzip magic ⇒
 * v2; plain tar ⇒ v1. Returns the lazy file list for v3 (empty for v1/v2).
 */
export async function extractCheckpoint(
  refOrBytes: string | Uint8Array,
  destDir: string,
  opts: {
    store?: ObjectGetStore & Partial<ObjectPutStore>
    lazySkip?: boolean
  } = {},
): Promise<{ lazyFiles: CheckpointFileEntry[] }> {
  // A ref string: could be a v3 manifest ref or a v1/v2 archive object ref.
  if (typeof refOrBytes === 'string') {
    if (!opts.store) {
      throw new Error('extractCheckpoint(ref) requires a store handle')
    }
    const bytes = await opts.store.get(refOrBytes)
    if (isV3Manifest(bytes)) {
      return extractDatadirV3(refOrBytes, opts.store, destDir, {
        lazySkip: opts.lazySkip,
      })
    }
    await extractDatadir(bytes, destDir)
    return { lazyFiles: [] }
  }
  // Raw bytes: v1/v2 archive (a v3 manifest is only ever addressed by ref, but
  // sniff anyway so a manifest body passed inline still routes to v3).
  if (isV3Manifest(refOrBytes)) {
    if (!opts.store || !opts.store.put) {
      throw new Error(
        'extractCheckpoint(v3 manifest bytes) requires a put-capable store',
      )
    }
    const { ref } = await opts.store.put(refOrBytes)
    return extractDatadirV3(ref, opts.store, destDir, {
      lazySkip: opts.lazySkip,
    })
  }
  await extractDatadir(refOrBytes, destDir)
  return { lazyFiles: [] }
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
