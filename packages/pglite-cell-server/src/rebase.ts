// M5d transparent rebase (design §4.2/§4.4): harvest — validate — re-apply.
//
// FIXED DESIGN DECISIONS (M5_PLAN §M5d):
//
// - Harvest = walscan enumeration + local heap re-read. The session's own
//   local WAL range (B, localEnd] is enumerated with pgl_walscan (heap
//   insert/update/delete/multi-insert records now carry tuple offsets);
//   net-effect filtering — dropping aborted-subxact rows and collapsing
//   update chains to final versions — falls out of SAME-SESSION VISIBILITY:
//   the transaction committed locally before capture (M1 architecture), so
//   a post-commit read sees exactly the committed net effect. Tuple
//   payloads are fetched by ctid through ordinary same-session reads
//   (detoasting comes free). No WAL tuple extraction, ever (§12 class 2).
//
// - Re-apply = JS-level parameterized DML under
//   `session_replication_role = replica` in ONE fresh transaction at K:
//   ordinary triggers are suppressed (their B-time effects are already
//   harvested data) and volatile functions never re-run (values ride as
//   parameters). A 23505 during re-apply is a cross-cell conflict
//   surfacing late and maps to 40001 (§4.4).
//
// - Rows are addressed at K by their pre-B ctid: validation proves every
//   page the transaction read (which includes every page it modified) is
//   untouched by the winner tail, so pre-existing rows sit at exactly
//   their B-time ctids with their B-time contents.
//
// Validation (§4.2): pageLSN(K) <= B for every captured page via
// pgl_page_lsn (pinned-buffer BufferGetLSNAtomic — never executor paths),
// nblocks(K) == min captured probe per seqscanned rel, and the
// schema-epoch fence — winner-tail invalidation messages (all three
// carriers) intersected with the transaction's relation footprint.
// Sequence pages are excluded (§4.1); index-only scans are disabled in
// cells (§4.2 escape hatch), so VM-bit content checks are unnecessary.

import type { Cell, WalRecord } from '@electric-sql/pglite-cell'
import { walscanRange } from '@electric-sql/pglite-cell'

const XLOG_HEAP_OPMASK = 0x70
const OP_TRUNCATE = 0x30
const RM_HEAP_ID = 10

/** The page-LSN "missing page" sentinel (pgl_page_lsn). */
const MISSING_PAGE = 0xffffffffffffffffn
/** The nblocks "missing fork" sentinel (pgl_relation_nblocks). */
const MISSING_FORK = 0xffffffff

const FIRST_NORMAL_OID = 16384

/** Statement-text rebase-taint scan (§4.5 v1 documented approximation):
 *  observations of ctid/xmin/cmin/cmax/txid cannot survive re-placement
 *  at K. */
const TAINT_RE =
  /\bctid\b|\bxmin\b|\bcmin\b|\bcmax\b|txid_current|pg_current_xact_id/i

export function scanRebaseTaint(text: string): string | null {
  const m = TAINT_RE.exec(text)
  return m === null ? null : m[0].toLowerCase()
}

interface RelMeta {
  oid: number
  relfilenode: number
  nsp: string
  name: string
  kind: string
  persistence: string
}

interface ColMeta {
  name: string
  type: string
  generated: boolean
  /** GENERATED ALWAYS AS IDENTITY — re-insert needs OVERRIDING SYSTEM VALUE. */
  identityAlways: boolean
}

interface InsertOp {
  relKey: string
  values: (string | null)[]
}

interface UpdateOp {
  relKey: string
  oldCtid: string
  values: (string | null)[]
}

interface DeleteOp {
  relKey: string
  ctid: string
}

export interface RebasePlan {
  /** Ordered re-apply: deletes (reverse WAL order), updates, inserts. */
  deletes: DeleteOp[]
  updates: UpdateOp[]
  inserts: InsertOp[]
  /** setval targets: regclass text + harvested position. */
  seqSets: { regclass: string; lastValue: string; isCalled: boolean }[]
  /** rel metadata by relKey (`db/relfilenode`). */
  rels: Map<string, { meta: RelMeta; cols: ColMeta[] }>
  /** Relation-OID footprint for the schema-epoch fence (§4.3). */
  footprintOids: Set<number>
  /** Relfilenode footprint (smgr invals). */
  footprintRelfilenodes: Set<number>
  /** Relfilenodes of sequences (read-set page exclusion, §4.1). */
  seqRelfilenodes: Set<number>
}

export type HarvestResult =
  | { ok: true; plan: RebasePlan }
  | { ok: false; reason: string }

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

function qname(meta: RelMeta): string {
  return `${quoteIdent(meta.nsp)}.${quoteIdent(meta.name)}`
}

/**
 * Harvest the transaction's net-effect logical changes from its own local
 * WAL range `(baseLsn, endLsn]` plus same-session re-reads. Must run
 * BEFORE anything discards the local commit (reset/recycle) — payload
 * fetch depends on post-commit same-session visibility.
 */
export async function harvestRebasePlan(
  cell: Cell,
  baseLsn: bigint,
  endLsn: bigint,
  readSetRelfilenodes: Iterable<{ db: number; rel: number }>,
): Promise<HarvestResult> {
  let records: WalRecord[]
  try {
    records = walscanRange(cell.db, baseLsn, endLsn)
  } catch (err) {
    return { ok: false, reason: `own-WAL scan failed: ${String(err)}` }
  }

  // ---- enumerate heap DML (WAL order) ----
  interface HeapEvent {
    relKey: string
    rel: [number, number, number]
    op: 'insert' | 'update' | 'delete'
    ctid?: string // new tuple (insert/update) or target (delete)
    oldCtid?: string // update only
  }
  const events: HeapEvent[] = []
  const seqRelfilenodesSeen = new Set<number>()
  const relsSeen = new Map<string, [number, number, number]>()

  for (const rec of records) {
    if (rec.kind === 'seq_log' && rec.seqRel !== undefined) {
      seqRelfilenodesSeen.add(rec.seqRel[2])
      relsSeen.set(`${rec.seqRel[1]}/${rec.seqRel[2]}`, rec.seqRel)
      continue
    }
    if (
      rec.rmid === RM_HEAP_ID &&
      (rec.info & XLOG_HEAP_OPMASK) === OP_TRUNCATE
    ) {
      // TRUNCATE inside the transaction: relfilenode swap + catalog churn;
      // never rebaseable (also caught by the catalog-write check below —
      // this is the belt to that braces).
      return { ok: false, reason: 'TRUNCATE in transaction' }
    }
    const blk0 = rec.blocks[0]
    switch (rec.kind) {
      case 'heap_insert': {
        const key = `${blk0.rel[1]}/${blk0.rel[2]}`
        relsSeen.set(key, blk0.rel)
        events.push({
          relKey: key,
          rel: blk0.rel,
          op: 'insert',
          ctid: `(${blk0.blk},${rec.offnum})`,
        })
        break
      }
      case 'heap_multi_insert': {
        const key = `${blk0.rel[1]}/${blk0.rel[2]}`
        relsSeen.set(key, blk0.rel)
        for (const off of rec.offsets ?? []) {
          events.push({
            relKey: key,
            rel: blk0.rel,
            op: 'insert',
            ctid: `(${blk0.blk},${off})`,
          })
        }
        break
      }
      case 'heap_delete': {
        const key = `${blk0.rel[1]}/${blk0.rel[2]}`
        relsSeen.set(key, blk0.rel)
        events.push({
          relKey: key,
          rel: blk0.rel,
          op: 'delete',
          ctid: `(${blk0.blk},${rec.offnum})`,
        })
        break
      }
      case 'heap_update': {
        const oldBlk = rec.blocks[1] ?? blk0
        const key = `${blk0.rel[1]}/${blk0.rel[2]}`
        relsSeen.set(key, blk0.rel)
        events.push({
          relKey: key,
          rel: blk0.rel,
          op: 'update',
          ctid: `(${blk0.blk},${rec.newOffnum})`,
          oldCtid: `(${oldBlk.blk},${rec.oldOffnum})`,
        })
        break
      }
      default:
        break
    }
  }

  // ---- relation metadata (single catalog query) ----
  const wantRelfilenodes = new Set<number>()
  for (const rel of relsSeen.values()) wantRelfilenodes.add(rel[2])
  for (const r of readSetRelfilenodes) wantRelfilenodes.add(r.rel)

  const relRows =
    wantRelfilenodes.size === 0
      ? []
      : (
          await cell.db.query<{
            relfilenode: number
            oid: number
            nspname: string
            relname: string
            relkind: string
            relpersistence: string
          }>(
            `select c.relfilenode::int4 as relfilenode, c.oid::int4 as oid,
                    n.nspname, c.relname, c.relkind, c.relpersistence
             from pg_class c join pg_namespace n on n.oid = c.relnamespace
             where c.relfilenode = any($1::oid[])`,
            [[...wantRelfilenodes]],
          )
        ).rows

  const byRelfilenode = new Map<number, RelMeta>()
  for (const r of relRows) {
    byRelfilenode.set(r.relfilenode, {
      oid: r.oid,
      relfilenode: r.relfilenode,
      nsp: r.nspname,
      name: r.relname,
      kind: r.relkind,
      persistence: r.relpersistence,
    })
  }

  const footprintOids = new Set<number>()
  const footprintRelfilenodes = new Set<number>()
  const seqRelfilenodes = new Set<number>()
  for (const [rfn, meta] of byRelfilenode) {
    footprintOids.add(meta.oid)
    footprintRelfilenodes.add(rfn)
    if (meta.kind === 'S') seqRelfilenodes.add(rfn)
  }

  // ---- eligibility over the WRITTEN relations ----
  const rels = new Map<string, { meta: RelMeta; cols: ColMeta[] }>()
  const writtenKeys = new Set(events.map((e) => e.relKey))
  for (const key of writtenKeys) {
    const rel = relsSeen.get(key)!
    const meta = byRelfilenode.get(rel[2])
    if (meta === undefined) {
      return { ok: false, reason: `unknown written relation ${key}` }
    }
    if (meta.oid < FIRST_NORMAL_OID) {
      // Catalog heap write = DDL in the transaction: logical re-apply of
      // catalog rows is out of contract (§12 class 1 adjacent).
      return { ok: false, reason: `catalog write (${meta.nsp}.${meta.name})` }
    }
    if (meta.kind === 't') continue // TOAST: rides the parent detoasted
    if (meta.kind !== 'r') {
      return {
        ok: false,
        reason: `unsupported relkind '${meta.kind}' (${meta.name})`,
      }
    }
    if (meta.persistence !== 'p') {
      return { ok: false, reason: `non-permanent write (${meta.name})` }
    }
    rels.set(key, { meta, cols: [] })
  }

  // Column metadata (generated columns recompute at K; dropped excluded).
  for (const entry of rels.values()) {
    const cols = (
      await cell.db.query<{
        attname: string
        t: string
        gen: boolean
        ida: boolean
      }>(
        `select attname, format_type(atttypid, atttypmod) as t,
                attgenerated <> '' as gen, attidentity = 'a' as ida
         from pg_attribute
         where attrelid = $1 and attnum > 0 and not attisdropped
         order by attnum`,
        [entry.meta.oid],
      )
    ).rows
    entry.cols = cols.map((c) => ({
      name: c.attname,
      type: c.t,
      generated: c.gen,
      identityAlways: c.ida,
    }))
  }

  // ---- net-effect resolution by same-session visibility ----
  // Chain roots: ctid -> pre-B root ctid (null = inserted this txn).
  const deletes: DeleteOp[] = []
  const updates: UpdateOp[] = []
  const inserts: InsertOp[] = []

  for (const [relKey, entry] of rels) {
    const root = new Map<string, string | null>()
    const newCtids: string[] = []
    const preBRoots = new Set<string>()
    for (const ev of events) {
      if (ev.relKey !== relKey) continue
      if (ev.op === 'insert') {
        root.set(ev.ctid!, null)
        newCtids.push(ev.ctid!)
      } else if (ev.op === 'update') {
        const r = root.has(ev.oldCtid!) ? root.get(ev.oldCtid!)! : ev.oldCtid!
        root.set(ev.ctid!, r)
        newCtids.push(ev.ctid!)
        if (r !== null) preBRoots.add(r)
      } else {
        const r = root.has(ev.ctid!) ? root.get(ev.ctid!)! : ev.ctid!
        if (r !== null) preBRoots.add(r)
      }
    }

    const { meta, cols } = entry
    const fetchCols = cols.filter((c) => !c.generated)
    const selectList = fetchCols
      .map((c, i) => `${quoteIdent(c.name)}::text as v${i}`)
      .join(', ')

    // One visibility fetch over every candidate ctid (news + roots): the
    // committed net effect is exactly what this post-commit read returns.
    const candidates = [...new Set([...newCtids, ...preBRoots])]
    const visible = new Map<string, (string | null)[]>()
    if (candidates.length > 0) {
      const rows = (
        await cell.db.query<Record<string, string | null>>(
          `select ctid::text as __ctid${selectList ? ', ' + selectList : ''}
           from ${qname(meta)} where ctid = any($1::tid[])`,
          [candidates],
        )
      ).rows
      for (const row of rows) {
        visible.set(
          row.__ctid as string,
          fetchCols.map((_c, i) => row[`v${i}`] ?? null),
        )
      }
    }

    // Final version per pre-B root: the unique VISIBLE descendant.
    const finalOfRoot = new Map<string, string>()
    for (const c of newCtids) {
      if (!visible.has(c)) continue // aborted subxact / superseded version
      const r = root.get(c) ?? null
      if (r !== null) finalOfRoot.set(r, c)
    }

    for (const c of newCtids) {
      if (!visible.has(c)) continue
      if ((root.get(c) ?? null) === null) {
        inserts.push({ relKey, values: visible.get(c)! })
      }
    }
    for (const r of preBRoots) {
      const fin = finalOfRoot.get(r)
      if (fin !== undefined) {
        updates.push({ relKey, oldCtid: r, values: visible.get(fin)! })
      } else if (!visible.has(r)) {
        deletes.push({ relKey, ctid: r })
      }
      // else: root still visible — every op against it aborted; skip.
    }
  }
  deletes.reverse() // reverse WAL order

  // ---- sequences: harvested positions re-assert at K via setval ----
  const seqSets: RebasePlan['seqSets'] = []
  for (const rfn of seqRelfilenodesSeen) {
    const meta = byRelfilenode.get(rfn)
    if (meta === undefined || meta.kind !== 'S') continue
    const rows = (
      await cell.db.query<{
        last_value: string
        is_called: boolean
      }>(`select last_value, is_called from ${qname(meta)}`)
    ).rows
    if (rows.length === 1) {
      seqSets.push({
        regclass: qname(meta),
        lastValue: String(rows[0].last_value),
        isCalled: rows[0].is_called,
      })
    }
  }

  return {
    ok: true,
    plan: {
      deletes,
      updates,
      inserts,
      seqSets,
      rels,
      footprintOids,
      footprintRelfilenodes,
      seqRelfilenodes,
    },
  }
}

// ---- validation at K (§4.2) ----

export interface ReadSetForValidation {
  pins: { spc: number; db: number; rel: number; fork: number; blk: number }[]
  nblocks: {
    spc: number
    db: number
    rel: number
    fork: number
    nblocks: number
  }[]
  overflowed: boolean
}

export type ValidationResult = { ok: true } | { ok: false; reason: string }

/** SharedInvalidationMessage ids (sinval.h). */
const SI_CATALOG = -55
const SI_RELCACHE = -56
const SI_SMGR = -57
const SI_RELMAP = -58

/**
 * Validate the harvested read set against the local state at K (the cell
 * has been advanced past the winner tail). `winnerRecords` are the
 * classified records of `(B, K]` — the three inval carriers feed the
 * schema-epoch fence (§4.3).
 */
export function validateAtK(
  cell: Cell,
  baseLsn: bigint,
  readSet: ReadSetForValidation,
  plan: RebasePlan,
  winnerRecords: WalRecord[],
): ValidationResult {
  if (readSet.overflowed) {
    return { ok: false, reason: 'read-set ring overflow' }
  }

  // Structural fence FIRST (order matters — see the missing-page rule
  // below): winner-tail relation drops (commit/abort record drops[]) and
  // smgr truncations against ANY relation in the footprint fail loudly.
  // smgr sinval messages do NOT ride commit records (they are sent
  // immediately in vanilla PG), so truncation is fenced off the WAL
  // records themselves.
  for (const rec of winnerRecords) {
    if (rec.kind === 'commit' || rec.kind === 'abort') {
      for (const d of rec.drops ?? []) {
        if (plan.footprintRelfilenodes.has(d[2])) {
          return {
            ok: false,
            reason: `winner dropped relfilenode ${d[2]} in the footprint`,
          }
        }
      }
    } else if (
      rec.kind === 'smgr_truncate' &&
      rec.rel !== undefined &&
      plan.footprintRelfilenodes.has(rec.rel[2])
    ) {
      return {
        ok: false,
        reason: `winner truncated relfilenode ${rec.rel[2]} in the footprint`,
      }
    }
  }

  // Page LSNs: pageLSN(K) <= B, sequence pages excluded (§4.1/§5.3).
  // A page MISSING at K is IGNORED: the ring also records pins on pages
  // the transaction itself CREATED (its own relation extension, undone by
  // the reset) — those legitimately do not exist at K. A page that
  // existed at B can only be missing at K through a winner truncation or
  // drop, and both were fenced above; winner extensions of scanned rels
  // are the nblocks rule's job.
  const seenPages = new Set<string>()
  for (const p of readSet.pins) {
    if (plan.seqRelfilenodes.has(p.rel)) continue
    const key = `${p.spc}/${p.db}/${p.rel}/${p.fork}/${p.blk}`
    if (seenPages.has(key)) continue
    seenPages.add(key)
    const lsn = cell.pageLsn(p.spc, p.db, p.rel, p.fork, p.blk)
    if (lsn === MISSING_PAGE) continue
    if (lsn > baseLsn) {
      return {
        ok: false,
        reason: `page ${key} modified past base (lsn ${lsn} > ${baseLsn})`,
      }
    }
  }

  // nblocks: min captured probe per (rel, fork) must equal nblocks(K) —
  // later probes may include the transaction's OWN extensions (undone by
  // the reset); a winner extension makes nblocks(K) exceed the minimum.
  const minProbe = new Map<
    string,
    { p: (typeof readSet.nblocks)[0]; n: number }
  >()
  for (const p of readSet.nblocks) {
    if (plan.seqRelfilenodes.has(p.rel)) continue
    const key = `${p.spc}/${p.db}/${p.rel}/${p.fork}`
    const cur = minProbe.get(key)
    if (cur === undefined || p.nblocks < cur.n)
      minProbe.set(key, { p, n: p.nblocks })
  }
  for (const [key, { p, n }] of minProbe) {
    const now = cell.relationNblocks(p.spc, p.db, p.rel, p.fork)
    if (now === MISSING_FORK) {
      return { ok: false, reason: `missing fork ${key}` }
    }
    if (now > n) {
      // Grown past the smallest size ANY probe saw: a winner extension no
      // pinned page can witness — the §4.1 verified false negative.
      return {
        ok: false,
        reason: `nblocks grown for ${key} (${n} at B, ${now} at K)`,
      }
    }
    // now < n is the transaction's OWN growth inflating later probes
    // (undone at K): winner shrink is impossible outside truncate/drop —
    // both fenced above — and a winner extension hiding under own growth
    // lands on block numbers the scan pinned, so the pageLSN rule sees it.
  }

  // Schema-epoch fence (§4.3): winner-tail invals ∩ relation footprint.
  for (const rec of winnerRecords) {
    if (
      rec.kind !== 'commit' &&
      rec.kind !== 'invalidations' &&
      rec.kind !== 'heap_inplace'
    ) {
      continue
    }
    const nmsgs = rec.nmsgs ?? 0
    if (nmsgs <= 0 || rec.invals === undefined) continue
    const bytes = Buffer.from(rec.invals, 'hex')
    const msgSize = bytes.length / nmsgs
    for (let i = 0; i < nmsgs; i++) {
      const off = i * msgSize
      const id = bytes.readInt8(off)
      if (id === SI_RELCACHE) {
        const relId = bytes.readUInt32LE(off + 8)
        if (relId === 0 || plan.footprintOids.has(relId)) {
          return {
            ok: false,
            reason: `schema-epoch fence: relcache inval on ${relId === 0 ? 'ALL relations' : `rel ${relId}`}`,
          }
        }
      } else if (id === SI_SMGR) {
        const relNumber = bytes.readUInt32LE(off + 12)
        if (plan.footprintRelfilenodes.has(relNumber)) {
          return {
            ok: false,
            reason: `schema-epoch fence: smgr inval on relfilenode ${relNumber}`,
          }
        }
      } else if (id === SI_CATALOG || id === SI_RELMAP) {
        return {
          ok: false,
          reason: `schema-epoch fence: ${id === SI_CATALOG ? 'catalog-wide' : 'relmap'} inval`,
        }
      }
      // catcache (id >= 0) and snapshot invals: no table-shape signal that
      // is not also carried by a relcache inval; re-apply replays harvested
      // DATA, never re-executes SQL, so function/proc changes are inert.
    }
  }

  return { ok: true }
}

// ---- re-apply at K (§4.4) ----

export class ReapplyConflictError extends Error {
  constructor(
    message: string,
    public readonly unique: boolean,
  ) {
    super(message)
    this.name = 'ReapplyConflictError'
  }
}

/**
 * Re-apply the harvested net effect in ONE fresh transaction at K under
 * `session_replication_role = replica` (ordinary + RI triggers suppressed
 * — their B-time effects are already harvested data; uniqueness is
 * index-level and still enforced). Throws ReapplyConflictError on ANY
 * failure after rolling back — the caller maps it to 40001 (unique=true
 * marks the §4.4 late-surfacing 23505 case).
 *
 * Pre-existing rows are addressed by their B-time ctids — sound because
 * validation proved their pages untouched since B. Affected-row counts
 * are asserted: a 0-row UPDATE/DELETE means the addressing premise broke
 * and the transaction must not land.
 */
export async function reapplyPlan(cell: Cell, plan: RebasePlan): Promise<void> {
  const db = cell.db
  await db.exec("begin; set local session_replication_role = 'replica'")
  try {
    for (const d of plan.deletes) {
      const { meta } = plan.rels.get(d.relKey)!
      const res = await db.query(
        `delete from ${qname(meta)} where ctid = $1::tid`,
        [d.ctid],
      )
      if ((res.affectedRows ?? 0) !== 1) {
        throw new ReapplyConflictError(
          `re-apply DELETE matched ${res.affectedRows ?? 0} rows at ${d.ctid}`,
          false,
        )
      }
    }
    for (const u of plan.updates) {
      const { meta, cols } = plan.rels.get(u.relKey)!
      const fetchCols = cols.filter((c) => !c.generated)
      const sets = fetchCols
        .map((c, i) => `${quoteIdent(c.name)} = $${i + 2}::${c.type}`)
        .join(', ')
      const res = await db.query(
        `update ${qname(meta)} set ${sets} where ctid = $1::tid`,
        [u.oldCtid, ...u.values],
      )
      if ((res.affectedRows ?? 0) !== 1) {
        throw new ReapplyConflictError(
          `re-apply UPDATE matched ${res.affectedRows ?? 0} rows at ${u.oldCtid}`,
          false,
        )
      }
    }
    for (const ins of plan.inserts) {
      const { meta, cols } = plan.rels.get(ins.relKey)!
      const fetchCols = cols.filter((c) => !c.generated)
      const names = fetchCols.map((c) => quoteIdent(c.name)).join(', ')
      const params = fetchCols.map((c, i) => `$${i + 1}::${c.type}`).join(', ')
      const overriding = fetchCols.some((c) => c.identityAlways)
        ? ' overriding system value'
        : ''
      await db.query(
        `insert into ${qname(meta)} (${names})${overriding} values (${params})`,
        ins.values,
      )
    }
    for (const s of plan.seqSets) {
      // The quoted qualified name is valid regclass input text.
      await db.query(
        `select pg_catalog.setval($1::regclass, $2::bigint, $3::boolean)`,
        [s.regclass, s.lastValue, s.isCalled],
      )
    }
    await db.exec('commit')
  } catch (err) {
    await db.exec('rollback').catch(() => undefined)
    if (err instanceof ReapplyConflictError) throw err
    const code = (err as { code?: string }).code
    throw new ReapplyConflictError(
      `re-apply failed: ${String((err as Error).message ?? err)}`,
      code === '23505' ||
        /duplicate key value violates unique constraint/.test(String(err)),
    )
  }
}
