// M7 W3 — the host-side chunk cache (fixed decision 6): a disk-backed LRU
// of content-addressed 256 KiB chunks under `<dir>/<sha-hex>/<chunkIdx>`,
// byte-capped (default 1 GiB), shared per CellHost across every worker
// cell's OP_FAULT_READ traffic. Misses go to the injected fetcher (the
// gateway ranged read).

import {
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeSync,
} from 'node:fs'
import { join } from 'node:path'

export const DEFAULT_CHUNK_CACHE_BYTES = 1024 * 1024 * 1024 // 1 GiB

/** Ranged fetch of `length` bytes at `offset` of object `ref`. */
export type ChunkFetchFn = (
  ref: string,
  offset: number,
  length: number,
) => Promise<Uint8Array>

export interface ChunkCacheStats {
  hits: number
  misses: number
  /** Bytes fetched from the backing store (gateway) on misses. */
  fetchedBytes: number
  evictions: number
  /** Current resident bytes. */
  residentBytes: number
}

export interface ChunkCacheOpts {
  dir: string
  fetch: ChunkFetchFn
  /** Byte cap (LRU evicts past it). Default 1 GiB. */
  maxBytes?: number
}

/** `sha256:<hex>` -> a filesystem-safe directory name. */
function refDirName(ref: string): string {
  return ref.replace(/[^0-9a-zA-Z]/g, '_')
}

export class ChunkCache {
  private readonly dir: string
  private readonly fetch: ChunkFetchFn
  private readonly maxBytes: number
  /** Insertion-ordered LRU index: key -> byte size. */
  private readonly index = new Map<string, number>()
  private residentBytes = 0
  private readonly _stats = {
    hits: 0,
    misses: 0,
    fetchedBytes: 0,
    evictions: 0,
  }

  constructor(opts: ChunkCacheOpts) {
    this.dir = opts.dir
    this.fetch = opts.fetch
    this.maxBytes = opts.maxBytes ?? DEFAULT_CHUNK_CACHE_BYTES
    mkdirSync(this.dir, { recursive: true })
    // Rebuild the index from disk leftovers (order arbitrary — treated as
    // coldest-first).
    for (const refDir of readdirSync(this.dir)) {
      const abs = join(this.dir, refDir)
      let entries: string[]
      try {
        entries = readdirSync(abs)
      } catch {
        continue
      }
      for (const name of entries) {
        try {
          const size = statSync(join(abs, name)).size
          this.index.set(`${refDir}/${name}`, size)
          this.residentBytes += size
        } catch {
          // ignore unreadable leftovers
        }
      }
    }
    this.evictPastCap()
  }

  stats(): ChunkCacheStats {
    return { ...this._stats, residentBytes: this.residentBytes }
  }

  private pathFor(key: string): string {
    return join(this.dir, key)
  }

  /** Get chunk `chunkIdx` (`length` bytes at `offset`) of object `ref`. */
  async get(
    ref: string,
    chunkIdx: number,
    offset: number,
    length: number,
  ): Promise<Uint8Array> {
    const key = `${refDirName(ref)}/${chunkIdx}`
    if (this.index.has(key)) {
      const abs = this.pathFor(key)
      if (existsSync(abs)) {
        const bytes = readFileSync(abs)
        if (bytes.byteLength >= length) {
          // LRU touch.
          const size = this.index.get(key)!
          this.index.delete(key)
          this.index.set(key, size)
          this._stats.hits++
          return bytes.subarray(0, length)
        }
      }
      // Stale/short entry: drop and refetch.
      this.drop(key)
    }
    this._stats.misses++
    const bytes = await this.fetch(ref, offset, length)
    this._stats.fetchedBytes += bytes.byteLength
    const abs = this.pathFor(key)
    mkdirSync(join(this.dir, refDirName(ref)), { recursive: true })
    const fd = openSync(abs, 'w')
    try {
      writeSync(fd, bytes, 0, bytes.byteLength, 0)
    } finally {
      closeSync(fd)
    }
    this.index.set(key, bytes.byteLength)
    this.residentBytes += bytes.byteLength
    this.evictPastCap()
    return bytes
  }

  private drop(key: string): void {
    const size = this.index.get(key)
    if (size !== undefined) {
      this.index.delete(key)
      this.residentBytes -= size
    }
    rmSync(this.pathFor(key), { force: true })
  }

  private evictPastCap(): void {
    while (this.residentBytes > this.maxBytes && this.index.size > 0) {
      const oldest = this.index.keys().next().value as string
      this.drop(oldest)
      this._stats.evictions++
    }
  }
}
