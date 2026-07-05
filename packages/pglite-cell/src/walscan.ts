// WAL introspection over the native pgl_walscan exports (M5b, design
// §14.2): the per-record loop is C (xlogreader — battle-tested parsing),
// classification lands here as typed records. The scanner reads pg_wal
// segment FILES directly (independent of the instance's own insert/flush
// state), so externally transplanted slice bytes are scannable in place.

import type { PGlite } from '@electric-sql/pglite'

/** Block reference of one WAL record (walscan `blocks[]` entry). */
export interface WalBlockRef {
  /** [spcOid, dbOid, relNumber] */
  rel: [number, number, number]
  fork: number
  blk: number
  /** Record carries a full-page image for this block. */
  img: boolean
  /** Redo fully rebuilds this page without reading the old contents. */
  init: boolean
}

/** One classified WAL record. Eager-set kinds carry decoded payloads. */
export interface WalRecord {
  rmid: number
  info: number
  xid: number
  lsn: bigint
  end: bigint
  blocks: WalBlockRef[]
  /** Present for the §6.3 eager special-record set. */
  kind?:
    | 'commit'
    | 'abort'
    | 'invalidations'
    | 'heap_inplace'
    | 'multixact_create'
    | 'multixact_zero_off_page'
    | 'multixact_zero_mem_page'
    | 'multixact_truncate'
    | 'clog_zero_page'
    | 'clog_truncate'
    | 'seq_log'
    | 'nextoid'
    | 'checkpoint'
    | 'fpi'
    | 'parameter_change'
    | 'smgr_create'
    | 'smgr_truncate'
    | 'dbase_create'
    | 'dbase_drop'
  // commit/abort
  subxids?: number[]
  /** Relfilelocator drops: [spc, db, rel][] */
  drops?: [number, number, number][]
  nmsgs?: number
  /** Raw SharedInvalidationMessage array (hex) — fed to pgl_process_invals. */
  invals?: string
  dbId?: number
  tsId?: number
  relcacheInitFileInval?: boolean
  twophase_xid?: number
  // multixact_create
  mid?: number
  moff?: number
  nmembers?: number
  members?: [number, number][]
  members_raw?: string
  // zero pages
  pageno?: bigint
  // nextoid / checkpoint
  nextOid?: number
  nextXid?: bigint
  nextMulti?: number
  nextMultiOffset?: number
  redo?: bigint
  shutdown?: boolean
  // seq_log / smgr
  seqRel?: [number, number, number]
  rel?: [number, number, number]
  fork?: number
  blkno?: number
  flags?: number
}

export class WalScanError extends Error {
  constructor(message: string) {
    super(`walscan: ${message}`)
    this.name = 'WalScanError'
  }
}

const U64_KEYS = new Set(['lsn', 'end', 'pageno', 'nextXid', 'redo'])

function parseRecord(json: string): WalRecord {
  const raw = JSON.parse(json) as Record<string, unknown>
  if (typeof raw.error === 'string') throw new WalScanError(raw.error)
  for (const k of U64_KEYS) {
    if (typeof raw[k] === 'string') raw[k] = BigInt(raw[k] as string)
  }
  return raw as unknown as WalRecord
}

/**
 * Scan `[start, end)` of the instance's pg_wal, invoking `visit` per
 * classified record. `visit` may call `imageOf(blockId)` to restore the
 * full-page image of the CURRENT record's block (decompression + hole
 * handling native — RestoreBlockImage).
 */
export function walscan(
  pg: PGlite,
  start: bigint,
  end: bigint,
  visit: (
    rec: WalRecord,
    imageOf: (blockId: number) => Uint8Array | null,
  ) => void,
  tli = 1,
): void {
  const mod = pg.Module
  const rc = mod._pgl_walscan_begin(start, end, tli)
  if (rc !== 0) throw new WalScanError(`begin failed (${rc})`)
  const imgBuf = mod._malloc(8192)
  try {
    for (;;) {
      const ptr = mod._pgl_walscan_next()
      if (ptr === 0) break
      const rec = parseRecord(mod.UTF8ToString(ptr))
      const imageOf = (blockId: number): Uint8Array | null => {
        if (mod._pgl_walscan_block_image(blockId, imgBuf) !== 1) return null
        return mod.HEAPU8.slice(imgBuf, imgBuf + 8192)
      }
      visit(rec, imageOf)
    }
  } finally {
    mod._free(imgBuf)
    mod._pgl_walscan_end_scan()
  }
}

/** Collect every classified record of `[start, end)` (no images). */
export function walscanRange(
  pg: PGlite,
  start: bigint,
  end: bigint,
  tli = 1,
): WalRecord[] {
  const out: WalRecord[] = []
  walscan(pg, start, end, (rec) => out.push(rec), tli)
  return out
}
