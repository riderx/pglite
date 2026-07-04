// fs-backed content-addressed object store (§14.5: "serve checkpoint objects
// and spilled slices ... content-addressed objects make caching trivial").
//
// A ref is `sha256:<hex>`; the store is content-addressed, so `put` is
// idempotent — the same bytes always land at the same path, and a re-put is a
// no-op. Writes go to a temp file and are renamed into place (atomic on the
// same filesystem); the store verifies the hash on write.

import { createHash } from 'node:crypto'
import {
  mkdirSync,
  writeFileSync,
  readFileSync,
  readdirSync,
  renameSync,
  existsSync,
  rmSync,
} from 'node:fs'
import { join } from 'node:path'

/** Thrown by `get` when a ref is not present in the store. */
export class ObjectNotFoundError extends Error {
  constructor(public readonly ref: string) {
    super(`object not found: ${ref}`)
    this.name = 'ObjectNotFoundError'
  }
}

/** Thrown when verify-on-write finds the written bytes do not hash to `ref`. */
export class ObjectHashMismatchError extends Error {
  constructor(
    public readonly expected: string,
    public readonly actual: string,
  ) {
    super(`object hash mismatch: expected ${expected}, got ${actual}`)
    this.name = 'ObjectHashMismatchError'
  }
}

/** Compute the content-address ref (`sha256:<hex>`) for `bytes`. */
export function refFor(bytes: Uint8Array): string {
  return 'sha256:' + createHash('sha256').update(bytes).digest('hex')
}

/** Map a `sha256:<hex>` ref to its hex digest (the on-disk object name). */
function refToHex(ref: string): string {
  const [algo, hex] = ref.split(':')
  if (algo !== 'sha256' || !/^[0-9a-f]{64}$/.test(hex ?? '')) {
    throw new Error(`malformed object ref: ${ref}`)
  }
  return hex
}

/**
 * A local-filesystem content-addressed store. Objects live under
 * `<rootDir>/objects/<hex>`; temporary writes under `<rootDir>/tmp`.
 */
export class FsObjectStore {
  private readonly objectsDir: string
  private readonly tmpDir: string
  private tmpSeq = 0

  constructor(rootDir: string) {
    this.objectsDir = join(rootDir, 'objects')
    this.tmpDir = join(rootDir, 'tmp')
    mkdirSync(this.objectsDir, { recursive: true })
    mkdirSync(this.tmpDir, { recursive: true })
  }

  private pathFor(ref: string): string {
    return join(this.objectsDir, refToHex(ref))
  }

  /**
   * Store `bytes`; returns their content-address ref. Idempotent: if the
   * object already exists the write is skipped. Verifies the hash on write
   * (a `writeFileSync` + re-read + re-hash) before renaming into place.
   */
  async put(bytes: Uint8Array): Promise<{ ref: string }> {
    const ref = refFor(bytes)
    const dst = this.pathFor(ref)
    if (existsSync(dst)) return { ref }

    const tmp = join(
      this.tmpDir,
      `${process.pid}-${this.tmpSeq++}-${refToHex(ref)}`,
    )
    writeFileSync(tmp, bytes)
    try {
      // Verify-on-write: read the persisted bytes back and re-hash.
      const actual = refFor(readFileSync(tmp))
      if (actual !== ref) throw new ObjectHashMismatchError(ref, actual)
      renameSync(tmp, dst)
    } catch (err) {
      rmSync(tmp, { force: true })
      throw err
    }
    return { ref }
  }

  /** Read the object at `ref`, or throw `ObjectNotFoundError`. */
  async get(ref: string): Promise<Uint8Array> {
    const src = this.pathFor(ref)
    if (!existsSync(src)) throw new ObjectNotFoundError(ref)
    return readFileSync(src)
  }

  /** True iff an object with `ref` is present. */
  async has(ref: string): Promise<boolean> {
    return existsSync(this.pathFor(ref))
  }

  /**
   * Delete the object at `ref` (GC of unreferenced checkpoint objects).
   * Idempotent — a missing object is a no-op. Returns true iff a file was
   * removed.
   */
  async delete(ref: string): Promise<boolean> {
    const dst = this.pathFor(ref)
    if (!existsSync(dst)) return false
    rmSync(dst, { force: true })
    return true
  }

  /** List every stored object's ref (`sha256:<hex>`). */
  async list(): Promise<string[]> {
    if (!existsSync(this.objectsDir)) return []
    return readdirSync(this.objectsDir)
      .filter((n) => /^[0-9a-f]{64}$/.test(n))
      .map((hex) => 'sha256:' + hex)
  }
}
