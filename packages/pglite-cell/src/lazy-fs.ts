// M7 W3 — `LazyCellFS`: the worker-side custom PGlite filesystem (fixed
// decision 4). The datadir is a REAL local directory (the materialized
// skeleton: every eager file + every dir, extracted from a v3 checkpoint
// manifest with `lazySkip`), and every LAZY relation file is backed by a
// sparse local file of its manifest size plus a written/faulted chunk
// bitmap. Reads of a lazy chunk resolve: local overlay (already faulted or
// written) → synchronous fault via the SAB bridge (OP_FAULT_READ) → the
// host answers from its chunk cache / gateway ranged read. Writes ALWAYS
// land locally: copy-on-write at chunk granularity (a partial write to a
// non-present chunk faults it in first, then writes).
//
// Shape ported with provenance from the salvage reference
// `codex/durable-vfs-plan`:`packages/pglite-durable-vfs/src/replica/
// lazy-replica-fs.ts` (a BaseFilesystem whose files live on real disk and
// whose reads ensure remote ranges first). Differences: the remote index
// here is the fixed v3 manifest file list (not a commit index), the fault
// unit is a 256 KiB chunk (not an 8 KiB page), and everything is writable
// locally (cells own their overlay; publishing goes via WAL slices, never
// via files).

import * as fs from 'node:fs'
import * as path from 'node:path'
import {
  BaseFilesystem,
  ERRNO_CODES,
  type FsStats,
} from '@electric-sql/pglite/basefs'

/** Fault granularity (M7 fixed decision 3): 256 KiB aligned chunks. */
export const LAZY_CHUNK_BYTES = 256 * 1024

/** One lazy file as listed by the v3 checkpoint manifest. */
export interface LazyFileSpec {
  /** Datadir-relative path (`base/5/1259`, `global/1260_vm`, ...). */
  path: string
  /** Manifest size in bytes (seeds initial nblocks via stat). */
  size: number
  /** Content-address of the file's bytes (`sha256:...`). */
  ref: string
}

/**
 * Synchronous chunk fault: return the bytes of `length` at
 * `chunkIdx * chunkBytes` of lazy file `fileIdx` (index into the spec list
 * handed to the constructor). MUST be synchronous — it runs inside a WASM
 * filesystem callback (the worker blocks on the SAB bridge underneath).
 */
export type ChunkFaultFn = (
  fileIdx: number,
  chunkIdx: number,
  length: number,
) => Uint8Array

export interface LazyCellFsStats {
  /** Chunk faults that went to the host (bridge round-trips). */
  chunkFaults: number
  /** Bytes moved by those faults. */
  bytesFaulted: number
  /** Lazy-chunk reads served from the local overlay (no bridge). */
  overlayHits: number
}

export interface LazyCellFSOptions {
  lazyFiles: LazyFileSpec[]
  fault: ChunkFaultFn
  chunkBytes?: number
  debug?: boolean
}

interface LazyEntry {
  idx: number
  ref: string
  /** Current lazy extent: chunks below this size may still fault; local
   *  truncation lowers it (never raised — extension bytes are local). */
  size: number
  /** Total chunks of the ORIGINAL manifest size (bitmap length). */
  chunks: number
  present: Uint8Array
}

type FsError = Error & { code: number }

function fsError(code: number, message: string): FsError {
  return Object.assign(new Error(message), { code })
}

function withFsErrors<T>(operation: () => T): T {
  try {
    return operation()
  } catch (error) {
    if (error instanceof Error) {
      const code = (error as Error & { code?: number | string }).code
      if (typeof code === 'number') throw error
      if (typeof code === 'string' && code in ERRNO_CODES) {
        throw fsError(
          ERRNO_CODES[code as keyof typeof ERRNO_CODES],
          error.message,
        )
      }
      throw fsError(ERRNO_CODES.EINVAL, error.message)
    }
    throw fsError(ERRNO_CODES.EINVAL, String(error))
  }
}

function nodeStatsToFsStats(stats: fs.Stats): FsStats {
  return {
    dev: stats.dev,
    ino: stats.ino,
    mode: stats.mode,
    nlink: stats.nlink,
    uid: stats.uid,
    gid: stats.gid,
    rdev: stats.rdev,
    size: stats.size,
    blksize: stats.blksize,
    blocks: stats.blocks,
    atime: stats.atimeMs,
    mtime: stats.mtimeMs,
    ctime: stats.ctimeMs,
  }
}

/** Normalize an emscripten-mount path (`/base/5/1259`, possibly with a
 *  leading `undefined` or duplicate slashes) to `/a/b/c` form. */
export function normalizeVfsPath(inputPath: string): string {
  const parts: string[] = []
  for (const part of inputPath.replace(/\\/g, '/').split('/')) {
    if (part === '' || part === '.' || part === 'undefined') continue
    if (part === '..') {
      parts.pop()
      continue
    }
    parts.push(part)
  }
  return `/${parts.join('/')}`
}

export class LazyCellFS extends BaseFilesystem {
  readonly rootDir: string
  readonly chunkBytes: number
  private readonly fault: ChunkFaultFn
  /** normalized path (`/base/5/1259`) -> lazy entry. */
  private readonly lazy = new Map<string, LazyEntry>()
  private readonly fdPaths = new Map<number, string>()
  private readonly _stats: LazyCellFsStats = {
    chunkFaults: 0,
    bytesFaulted: 0,
    overlayHits: 0,
  }

  constructor(rootDir: string, options: LazyCellFSOptions) {
    super(rootDir, { debug: options.debug })
    this.rootDir = path.resolve(rootDir)
    this.chunkBytes = options.chunkBytes ?? LAZY_CHUNK_BYTES
    this.fault = options.fault
    fs.mkdirSync(this.rootDir, { recursive: true })
    options.lazyFiles.forEach((spec, idx) => {
      const norm = normalizeVfsPath(spec.path)
      const chunks = Math.max(1, Math.ceil(spec.size / this.chunkBytes))
      this.lazy.set(norm, {
        idx,
        ref: spec.ref,
        size: spec.size,
        chunks,
        present: new Uint8Array(chunks),
      })
      // Sparse placeholder at the manifest size: stat/readdir/nblocks all
      // work without moving a byte; only content reads fault.
      const abs = this.resolvePath(norm)
      fs.mkdirSync(path.dirname(abs), { recursive: true })
      const fd = fs.openSync(abs, 'a')
      try {
        if (fs.fstatSync(fd).size < spec.size) fs.ftruncateSync(fd, spec.size)
      } finally {
        fs.closeSync(fd)
      }
    })
  }

  stats(): LazyCellFsStats {
    return { ...this._stats }
  }

  // -------------------------------------------------------------------
  // Lazy resolution
  // -------------------------------------------------------------------

  private entryFor(normPath: string): LazyEntry | undefined {
    return this.lazy.get(normPath)
  }

  /** Fault every non-present chunk overlapping [position, position+length)
   *  into the local backing file. */
  private ensureChunks(
    entry: LazyEntry,
    normPath: string,
    position: number,
    length: number,
  ): void {
    if (length <= 0 || position >= entry.size) return
    const endPos = Math.min(position + length, entry.size)
    const first = Math.floor(position / this.chunkBytes)
    const last = Math.floor((endPos - 1) / this.chunkBytes)
    for (let idx = first; idx <= last && idx < entry.chunks; idx++) {
      if (entry.present[idx] === 1) {
        this._stats.overlayHits++
        continue
      }
      this.faultChunk(entry, normPath, idx)
    }
  }

  private faultChunk(entry: LazyEntry, normPath: string, idx: number): void {
    const off = idx * this.chunkBytes
    const want = Math.min(this.chunkBytes, entry.size - off)
    if (want <= 0) {
      entry.present[idx] = 1
      return
    }
    const bytes = this.fault(entry.idx, idx, want)
    if (bytes.byteLength !== want) {
      throw fsError(
        ERRNO_CODES.EINVAL,
        `lazy fault ${normPath} chunk ${idx}: got ${bytes.byteLength} bytes, wanted ${want}`,
      )
    }
    const abs = this.resolvePath(normPath)
    const fd = fs.openSync(abs, 'r+')
    try {
      fs.writeSync(fd, bytes, 0, want, off)
    } finally {
      fs.closeSync(fd)
    }
    entry.present[idx] = 1
    this._stats.chunkFaults++
    this._stats.bytesFaulted += want
  }

  /** Copy-on-write at chunk granularity: before a write lands, fault any
   *  overlapped chunk the write does not fully cover; mark all overlapped
   *  chunks present (their authoritative bytes are now local). */
  private ensureWritable(
    entry: LazyEntry,
    normPath: string,
    position: number,
    length: number,
  ): void {
    if (length <= 0) return
    const first = Math.floor(position / this.chunkBytes)
    const last = Math.floor((position + length - 1) / this.chunkBytes)
    for (let idx = first; idx <= last; idx++) {
      if (idx >= entry.chunks || entry.present[idx] === 1) continue
      const chunkStart = idx * this.chunkBytes
      const chunkEnd = Math.min(chunkStart + this.chunkBytes, entry.size)
      const covered = position <= chunkStart && position + length >= chunkEnd
      if (!covered) this.faultChunk(entry, normPath, idx)
      entry.present[idx] = 1
    }
  }

  /** External-writer hook (worker-side live-apply): make [offset,
   *  offset+length) of `absOrRelPath` safely writable by direct node-fs
   *  writes — fault-then-mark exactly like an FS write. */
  prepareExternalWrite(filePath: string, offset: number, length: number): void {
    const entry = this.entryFor(this.normalizeExternal(filePath))
    if (entry === undefined) return
    this.ensureWritable(entry, this.normalizeExternal(filePath), offset, length)
  }

  /** External-writer hook: the file was removed outside the FS — drop the
   *  lazy entry so nothing ever resurrects checkpoint bytes for it. */
  noteExternalRemove(filePath: string): void {
    this.lazy.delete(this.normalizeExternal(filePath))
  }

  private normalizeExternal(filePath: string): string {
    const abs = path.resolve(filePath)
    if (abs.startsWith(this.rootDir)) {
      return normalizeVfsPath(abs.slice(this.rootDir.length))
    }
    return normalizeVfsPath(filePath)
  }

  // -------------------------------------------------------------------
  // BaseFilesystem API (passthrough to the backing dir + lazy hooks)
  // -------------------------------------------------------------------

  chmod(filePath: string, mode: number): void {
    withFsErrors(() => fs.chmodSync(this.resolvePath(filePath), mode))
  }

  close(fd: number): void {
    // Idempotence guard: worker threads share the PROCESS fd table, so a
    // double close here could close an fd number since reused by another
    // thread (observed as EBADF/corruption in sibling cells). Only close
    // fds this FS still tracks.
    if (!this.fdPaths.has(fd)) return
    this.fdPaths.delete(fd)
    withFsErrors(() => fs.closeSync(fd))
  }

  fstat(fd: number): FsStats {
    return nodeStatsToFsStats(withFsErrors(() => fs.fstatSync(fd)))
  }

  lstat(filePath: string): FsStats {
    return nodeStatsToFsStats(
      withFsErrors(() => fs.lstatSync(this.resolvePath(filePath))),
    )
  }

  mkdir(
    dirPath: string,
    options: { recursive?: boolean; mode?: number } = {},
  ): void {
    withFsErrors(() => fs.mkdirSync(this.resolvePath(dirPath), options))
  }

  open(filePath: string, flags?: string, mode = 0o666): number {
    const norm = normalizeVfsPath(filePath)
    const fd = withFsErrors(() =>
      fs.openSync(this.resolvePath(norm), flags ?? 'r+', mode),
    )
    this.fdPaths.set(fd, norm)
    return fd
  }

  readdir(dirPath: string): string[] {
    return withFsErrors(() => fs.readdirSync(this.resolvePath(dirPath)))
  }

  read(
    fd: number,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): number {
    const norm = this.fdPaths.get(fd)
    if (norm !== undefined) {
      const entry = this.entryFor(norm)
      if (entry !== undefined) this.ensureChunks(entry, norm, position, length)
    }
    return withFsErrors(() => fs.readSync(fd, buffer, offset, length, position))
  }

  rename(oldPath: string, newPath: string): void {
    const oldNorm = normalizeVfsPath(oldPath)
    const newNorm = normalizeVfsPath(newPath)
    withFsErrors(() => {
      fs.mkdirSync(path.dirname(this.resolvePath(newNorm)), {
        recursive: true,
      })
      fs.renameSync(this.resolvePath(oldNorm), this.resolvePath(newNorm))
    })
    const entry = this.lazy.get(oldNorm)
    if (entry !== undefined) {
      this.lazy.delete(oldNorm)
      this.lazy.set(newNorm, entry)
    } else {
      this.lazy.delete(newNorm)
    }
    for (const [fd, fdPath] of this.fdPaths) {
      if (fdPath === oldNorm) this.fdPaths.set(fd, newNorm)
    }
  }

  rmdir(dirPath: string): void {
    withFsErrors(() => fs.rmdirSync(this.resolvePath(dirPath)))
  }

  truncate(filePath: string, len = 0): void {
    const norm = normalizeVfsPath(filePath)
    const entry = this.entryFor(norm)
    if (entry !== undefined && len < entry.size) {
      // Shrink the lazy extent: bytes at/after `len` are gone; a later
      // extension is local-only by definition (a fault must never re-grow
      // the file with stale checkpoint bytes).
      entry.size = len
      const firstGone = Math.ceil(len / this.chunkBytes)
      for (let idx = firstGone; idx < entry.chunks; idx++) {
        entry.present[idx] = 1
      }
    }
    withFsErrors(() => fs.truncateSync(this.resolvePath(norm), len))
  }

  unlink(filePath: string): void {
    const norm = normalizeVfsPath(filePath)
    this.lazy.delete(norm)
    try {
      fs.unlinkSync(this.resolvePath(norm))
    } catch {
      // match base.ts node_ops.unlink tolerance
    }
  }

  utimes(filePath: string, atime: number, mtime: number): void {
    withFsErrors(() => fs.utimesSync(this.resolvePath(filePath), atime, mtime))
  }

  writeFile(
    filePath: string,
    data: string | Uint8Array,
    options: { encoding?: string; mode?: number; flag?: string } = {},
  ): void {
    const norm = normalizeVfsPath(filePath)
    // A whole-file write replaces any lazy content (mknod path writes '').
    this.lazy.delete(norm)
    const abs = this.resolvePath(norm)
    withFsErrors(() => {
      fs.mkdirSync(path.dirname(abs), { recursive: true })
      fs.writeFileSync(abs, data, {
        encoding: (options.encoding as BufferEncoding) ?? undefined,
        mode: options.mode,
        flag: options.flag,
      })
    })
  }

  write(
    fd: number,
    buffer: Uint8Array | ArrayBuffer,
    offset: number,
    length: number,
    position: number,
  ): number {
    const norm = this.fdPaths.get(fd)
    if (norm !== undefined) {
      const entry = this.entryFor(norm)
      if (entry !== undefined) {
        this.ensureWritable(entry, norm, position, length)
      }
    }
    const data =
      buffer instanceof Uint8Array
        ? buffer.subarray(offset, offset + length)
        : new Uint8Array(buffer, offset, length)
    return withFsErrors(() =>
      fs.writeSync(fd, data, 0, data.byteLength, position),
    )
  }

  async closeFs(): Promise<void> {
    const fds = [...this.fdPaths.keys()]
    this.fdPaths.clear()
    for (const fd of fds) {
      try {
        fs.closeSync(fd)
      } catch {
        // already closed
      }
    }
  }

  private resolvePath(filePath: string): string {
    const norm = normalizeVfsPath(filePath)
    const resolved = path.resolve(this.rootDir, norm.slice(1))
    const rel = path.relative(this.rootDir, resolved)
    if (rel.startsWith('..') || path.isAbsolute(rel)) {
      throw fsError(ERRNO_CODES.EINVAL, `path escapes VFS root: ${filePath}`)
    }
    return resolved
  }
}
