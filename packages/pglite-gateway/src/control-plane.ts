// The control plane (§14.6): a boring embedded Postgres (PGlite) that is
// authoritative for TOPOLOGY and LIFECYCLE only — which databases exist, their
// eras, and their checkpoints — NEVER for database contents (the stream is the
// truth for that) and NEVER in the commit path. All methods here are
// lifecycle-rate (create / rotate / checkpoint publish); a commit never touches
// this class.
//
// Schema v0 (M1_PLAN "control-plane.ts"): databases / eras / checkpoints.
// Schema v1 (M2_PLAN "control-plane v1"): dials on `databases`, era sealing
// columns + a separate `era_attempts` registry (registered BEFORE the stream
// PUT so GC can sweep orphans — the DS server has no stream listing), forks
// (`lineage`), and GC pins (`pins`). All additions are applied idempotently at
// init via CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.

import { PGlite } from '@electric-sql/pglite'

const SCHEMA_V0 = `
create table if not exists databases (
  id uuid primary key,
  name text unique not null,
  status text not null default 'active',
  created_at timestamptz default now()
);
create table if not exists eras (
  database_id uuid references databases(id),
  ordinal int not null,
  era_id text not null,
  path text not null,
  base_offset text not null,
  base_lsn pg_lsn not null,
  sealed bool not null default false,
  primary key (database_id, ordinal)
);
create table if not exists checkpoints (
  database_id uuid references databases(id),
  lsn pg_lsn not null,
  snap_end pg_lsn not null,
  stream_offset text not null,
  object_ref text not null,
  created_at timestamptz default now(),
  primary key (database_id, lsn)
);
`

// Additive v1 migration. Every statement is IF NOT EXISTS / ADD COLUMN IF NOT
// EXISTS so re-running it over a v0 or v1 datadir is a no-op.
const SCHEMA_V1 = `
alter table databases add column if not exists current_era_ordinal int not null default 1;
alter table databases add column if not exists checkpoint_every_bytes bigint not null default 0;
alter table databases add column if not exists rotate_every_bytes bigint not null default 0;
alter table databases add column if not exists gc_grace_ms bigint not null default 300000;

alter table eras add column if not exists sealed_final_offset text;
alter table eras add column if not exists sealed_final_lsn pg_lsn;
alter table eras add column if not exists next_era_ordinal int;

create table if not exists era_attempts (
  database_id uuid references databases(id),
  ordinal int not null,
  era_id text not null,
  path text not null,
  promoted boolean not null default false,
  created_at timestamptz default now(),
  primary key (database_id, era_id)
);

create table if not exists lineage (
  child_id uuid primary key references databases(id),
  parent_id uuid references databases(id),
  fork_lsn pg_lsn not null,
  fork_offset text not null,
  created_at timestamptz default now()
);

create table if not exists pins (
  id uuid primary key,
  database_id uuid references databases(id),
  kind text not null,
  holder text not null,
  pinned_offset text not null,
  pinned_lsn pg_lsn not null,
  expires_at timestamptz not null
);
`

export interface DatabaseRow {
  id: string
  name: string
  status: string
  currentEraOrdinal: number
  checkpointEveryBytes: string
  rotateEveryBytes: string
  gcGraceMs: string
}

/** The three per-database lifecycle dials (§ M2_PLAN "Dials"). */
export interface Dials {
  checkpointEveryBytes?: bigint | number | string
  rotateEveryBytes?: bigint | number | string
  gcGraceMs?: bigint | number | string
}

export interface EraAttemptRow {
  databaseId: string
  ordinal: number
  eraId: string
  path: string
  promoted: boolean
}

export interface LineageRow {
  childId: string
  parentId: string
  forkLsn: string
  forkOffset: string
}

export interface PinRow {
  id: string
  databaseId: string
  kind: string
  holder: string
  pinnedOffset: string
  pinnedLsn: string
  expiresAt: string
}

export interface EraRow {
  databaseId: string
  ordinal: number
  eraId: string
  path: string
  baseOffset: string
  baseLsn: string
  sealed: boolean
  sealedFinalOffset: string | null
  sealedFinalLsn: string | null
  nextEraOrdinal: number | null
}

export interface CheckpointRow {
  databaseId: string
  lsn: string
  snapEnd: string
  streamOffset: string
  objectRef: string
}

export interface AddEraInput {
  databaseId: string
  ordinal: number
  eraId: string
  path: string
  baseOffset: string
  /** pg_lsn text form, e.g. "0/1A2B3C". */
  baseLsn: string
  sealed?: boolean
}

export interface RegisterCheckpointInput {
  databaseId: string
  /** pg_lsn text of the checkpoint (C0). */
  lsn: string
  /** pg_lsn text of the snapshot end (attach point). */
  snapEnd: string
  /** Stream offset token recorded for the checkpoint. */
  streamOffset: string
  /** Content-address ref of the checkpoint object. */
  objectRef: string
}

interface RawDatabaseRow {
  id: string
  name: string
  status: string
  current_era_ordinal: number
  checkpoint_every_bytes: string
  rotate_every_bytes: string
  gc_grace_ms: string
}

const DB_SELECT = `select id, name, status, current_era_ordinal,
  checkpoint_every_bytes::text as checkpoint_every_bytes,
  rotate_every_bytes::text as rotate_every_bytes,
  gc_grace_ms::text as gc_grace_ms
  from databases`

function mapDatabaseRow(r: RawDatabaseRow): DatabaseRow {
  return {
    id: r.id,
    name: r.name,
    status: r.status,
    currentEraOrdinal: r.current_era_ordinal,
    checkpointEveryBytes: r.checkpoint_every_bytes,
    rotateEveryBytes: r.rotate_every_bytes,
    gcGraceMs: r.gc_grace_ms,
  }
}

interface RawEraRow {
  database_id: string
  ordinal: number
  era_id: string
  path: string
  base_offset: string
  base_lsn: string
  sealed: boolean
  sealed_final_offset: string | null
  sealed_final_lsn: string | null
  next_era_ordinal: number | null
}

const ERA_SELECT = `select database_id, ordinal, era_id, path, base_offset,
  base_lsn, sealed, sealed_final_offset, sealed_final_lsn, next_era_ordinal
  from eras`

function mapEraRow(r: RawEraRow): EraRow {
  return {
    databaseId: r.database_id,
    ordinal: r.ordinal,
    eraId: r.era_id,
    path: r.path,
    baseOffset: r.base_offset,
    baseLsn: r.base_lsn,
    sealed: r.sealed,
    sealedFinalOffset: r.sealed_final_offset,
    sealedFinalLsn: r.sealed_final_lsn,
    nextEraOrdinal: r.next_era_ordinal,
  }
}

/**
 * Topology/lifecycle catalog over a PGlite instance. Pass an existing PGlite
 * (own datadir under the gateway data root, or in-memory for tests) via
 * `create`. All writes are guarded and idempotent-friendly where the plan
 * calls for it. NEVER used in the commit path.
 */
export class ControlPlane {
  private constructor(private readonly db: PGlite) {}

  /** Open a control plane at `dataDir` (or in-memory when omitted). */
  static async create(dataDir?: string): Promise<ControlPlane> {
    const db = dataDir ? new PGlite(dataDir) : new PGlite()
    await db.exec(SCHEMA_V0)
    await db.exec(SCHEMA_V1)
    return new ControlPlane(db)
  }

  /** Wrap an already-open PGlite (used when the gateway owns the instance). */
  static async fromPGlite(db: PGlite): Promise<ControlPlane> {
    await db.exec(SCHEMA_V0)
    await db.exec(SCHEMA_V1)
    return new ControlPlane(db)
  }

  /** Set a database's status (e.g. 'deleted' — drops it from lineage pins). */
  async setDatabaseStatus(id: string, status: string): Promise<void> {
    await this.db.query(`update databases set status = $2 where id = $1`, [
      id,
      status,
    ])
  }

  /** Insert a database row; returns the generated id. Name is unique. */
  async createDatabase(name: string): Promise<string> {
    const res = await this.db.query<{ id: string }>(
      `insert into databases (id, name) values (gen_random_uuid(), $1) returning id`,
      [name],
    )
    return res.rows[0].id
  }

  async getDatabaseById(id: string): Promise<DatabaseRow | null> {
    const res = await this.db.query<RawDatabaseRow>(
      `${DB_SELECT} where id = $1`,
      [id],
    )
    return res.rows[0] ? mapDatabaseRow(res.rows[0]) : null
  }

  async getDatabaseByName(name: string): Promise<DatabaseRow | null> {
    const res = await this.db.query<RawDatabaseRow>(
      `${DB_SELECT} where name = $1`,
      [name],
    )
    return res.rows[0] ? mapDatabaseRow(res.rows[0]) : null
  }

  async listDatabases(): Promise<DatabaseRow[]> {
    const res = await this.db.query<RawDatabaseRow>(
      `${DB_SELECT} order by created_at`,
    )
    return res.rows.map(mapDatabaseRow)
  }

  /** Set any subset of the three dials (§ M2_PLAN). No-op subsets are fine. */
  async setDials(databaseId: string, dials: Dials): Promise<void> {
    const sets: string[] = []
    const params: unknown[] = [databaseId]
    const add = (col: string, val: bigint | number | string | undefined) => {
      if (val === undefined) return
      params.push(String(val))
      sets.push(`${col} = $${params.length}`)
    }
    add('checkpoint_every_bytes', dials.checkpointEveryBytes)
    add('rotate_every_bytes', dials.rotateEveryBytes)
    add('gc_grace_ms', dials.gcGraceMs)
    if (sets.length === 0) return
    await this.db.query(
      `update databases set ${sets.join(', ')} where id = $1`,
      params,
    )
  }

  /** Insert an era row (guarded on the (database, ordinal) primary key). */
  async addEra(input: AddEraInput): Promise<void> {
    await this.db.query(
      `insert into eras
         (database_id, ordinal, era_id, path, base_offset, base_lsn, sealed)
       values ($1, $2, $3, $4, $5, $6, $7)`,
      [
        input.databaseId,
        input.ordinal,
        input.eraId,
        input.path,
        input.baseOffset,
        input.baseLsn,
        input.sealed ?? false,
      ],
    )
  }

  /** The current (highest-ordinal) era for a database, or null. */
  async currentEra(databaseId: string): Promise<EraRow | null> {
    const res = await this.db.query<RawEraRow>(
      `${ERA_SELECT} where database_id = $1 order by ordinal desc limit 1`,
      [databaseId],
    )
    return res.rows[0] ? mapEraRow(res.rows[0]) : null
  }

  /** All eras for a database, ascending by ordinal. */
  async erasOf(databaseId: string): Promise<EraRow[]> {
    const res = await this.db.query<RawEraRow>(
      `${ERA_SELECT} where database_id = $1 order by ordinal`,
      [databaseId],
    )
    return res.rows.map(mapEraRow)
  }

  /** A specific era by (database, ordinal), or null. */
  async eraByOrdinal(
    databaseId: string,
    ordinal: number,
  ): Promise<EraRow | null> {
    const res = await this.db.query<RawEraRow>(
      `${ERA_SELECT} where database_id = $1 and ordinal = $2`,
      [databaseId, ordinal],
    )
    return res.rows[0] ? mapEraRow(res.rows[0]) : null
  }

  // --- Era rotation primitives (steps 0/3/5/6) --------------------------

  /**
   * Register an era attempt BEFORE its stream PUT (§6.1 step 3). GC's orphan
   * sweep reads this registry — the DS server has no stream listing, so an
   * un-promoted attempt is the only trace of a stream that may have been PUT
   * but never manifested. Guarded on (database, era_id); idempotent re-register.
   */
  async registerEraAttempt(input: {
    databaseId: string
    ordinal: number
    eraId: string
    path: string
  }): Promise<void> {
    await this.db.query(
      `insert into era_attempts (database_id, ordinal, era_id, path)
       values ($1, $2, $3, $4)
       on conflict (database_id, era_id) do nothing`,
      [input.databaseId, input.ordinal, input.eraId, input.path],
    )
  }

  /** Mark an era attempt promoted (step 6 — it manifested as a real era). */
  async promoteEraAttempt(databaseId: string, eraId: string): Promise<void> {
    await this.db.query(
      `update era_attempts set promoted = true
         where database_id = $1 and era_id = $2`,
      [databaseId, eraId],
    )
  }

  /**
   * Seal era `ordinal` (§6.1 step 6): record its terminal S-frame coordinates
   * and the ordinal of the era that succeeds it. Idempotent — re-sealing with
   * the same values is benign.
   */
  async sealEra(
    databaseId: string,
    ordinal: number,
    seal: { finalOffset: string; finalLsn: string; nextOrdinal: number },
  ): Promise<void> {
    await this.db.query(
      `update eras set sealed = true, sealed_final_offset = $3,
         sealed_final_lsn = $4, next_era_ordinal = $5
         where database_id = $1 and ordinal = $2`,
      [databaseId, ordinal, seal.finalOffset, seal.finalLsn, seal.nextOrdinal],
    )
  }

  /**
   * The step-0/6 guarded advance of `current_era_ordinal` from `from` to `to`.
   * Returns true iff exactly this call performed the transition (the row was
   * still at `from`); false means someone else already advanced it (re-read and
   * continue — the repair-walk primitive).
   */
  async advanceCurrentEra(
    databaseId: string,
    from: number,
    to: number,
  ): Promise<boolean> {
    const res = await this.db.query(
      `update databases set current_era_ordinal = $3
         where id = $1 and current_era_ordinal = $2`,
      [databaseId, from, to],
    )
    return (res.affectedRows ?? 0) > 0
  }

  /** Era attempts registered but never promoted, older than `olderThanMs`. */
  async listOrphanAttempts(olderThanMs: number): Promise<EraAttemptRow[]> {
    const res = await this.db.query<{
      database_id: string
      ordinal: number
      era_id: string
      path: string
      promoted: boolean
    }>(
      `select database_id, ordinal, era_id, path, promoted from era_attempts
         where promoted = false
           and created_at < now() - ($1::bigint * interval '1 millisecond')`,
      [String(olderThanMs)],
    )
    return res.rows.map((r) => ({
      databaseId: r.database_id,
      ordinal: r.ordinal,
      eraId: r.era_id,
      path: r.path,
      promoted: r.promoted,
    }))
  }

  /** Drop an era_attempts row (after its orphan stream is swept). */
  async deleteEraAttempt(databaseId: string, eraId: string): Promise<void> {
    await this.db.query(
      `delete from era_attempts where database_id = $1 and era_id = $2`,
      [databaseId, eraId],
    )
  }

  // --- Lineage (forks) --------------------------------------------------

  /** Insert a lineage edge (child -> parent, with the fork point). */
  async insertLineage(input: {
    childId: string
    parentId: string
    forkLsn: string
    forkOffset: string
  }): Promise<void> {
    await this.db.query(
      `insert into lineage (child_id, parent_id, fork_lsn, fork_offset)
       values ($1, $2, $3, $4)`,
      [input.childId, input.parentId, input.forkLsn, input.forkOffset],
    )
  }

  /** Live children forked off `parentId` (their fork points pin the parent). */
  async childrenOf(parentId: string): Promise<LineageRow[]> {
    const res = await this.db.query<{
      child_id: string
      parent_id: string
      fork_lsn: string
      fork_offset: string
    }>(
      `select l.child_id, l.parent_id, l.fork_lsn, l.fork_offset
         from lineage l join databases d on d.id = l.child_id
         where l.parent_id = $1 and d.status <> 'deleted'`,
      [parentId],
    )
    return res.rows.map((r) => ({
      childId: r.child_id,
      parentId: r.parent_id,
      forkLsn: r.fork_lsn,
      forkOffset: r.fork_offset,
    }))
  }

  // --- Pins -------------------------------------------------------------

  /** Create or refresh a pin (guarded on id). */
  async upsertPin(input: {
    id: string
    databaseId: string
    kind: string
    holder: string
    pinnedOffset: string
    pinnedLsn: string
    expiresAt: Date | string
  }): Promise<void> {
    const expires =
      input.expiresAt instanceof Date
        ? input.expiresAt.toISOString()
        : input.expiresAt
    await this.db.query(
      `insert into pins (id, database_id, kind, holder, pinned_offset,
         pinned_lsn, expires_at)
       values ($1, $2, $3, $4, $5, $6, $7)
       on conflict (id) do update set
         holder = excluded.holder, pinned_offset = excluded.pinned_offset,
         pinned_lsn = excluded.pinned_lsn, expires_at = excluded.expires_at`,
      [
        input.id,
        input.databaseId,
        input.kind,
        input.holder,
        input.pinnedOffset,
        input.pinnedLsn,
        expires,
      ],
    )
  }

  async deletePin(id: string): Promise<void> {
    await this.db.query(`delete from pins where id = $1`, [id])
  }

  /** Delete expired pins; returns how many were swept. */
  async expirePins(): Promise<number> {
    const res = await this.db.query(
      `delete from pins where expires_at <= now()`,
    )
    return res.affectedRows ?? 0
  }

  /** Live (unexpired) pins for a database. */
  async livePins(databaseId: string): Promise<PinRow[]> {
    const res = await this.db.query<{
      id: string
      database_id: string
      kind: string
      holder: string
      pinned_offset: string
      pinned_lsn: string
      expires_at: string
    }>(
      `select id, database_id, kind, holder, pinned_offset,
         pinned_lsn::text as pinned_lsn, expires_at::text as expires_at
         from pins where database_id = $1 and expires_at > now()`,
      [databaseId],
    )
    return res.rows.map((r) => ({
      id: r.id,
      databaseId: r.database_id,
      kind: r.kind,
      holder: r.holder,
      pinnedOffset: r.pinned_offset,
      pinnedLsn: r.pinned_lsn,
      expiresAt: r.expires_at,
    }))
  }

  // --- Checkpoints (GC helpers) -----------------------------------------

  /** All checkpoint rows for a database, ascending by LSN. */
  async checkpointsOf(databaseId: string): Promise<CheckpointRow[]> {
    const res = await this.db.query<{
      database_id: string
      lsn: string
      snap_end: string
      stream_offset: string
      object_ref: string
    }>(
      `select database_id, lsn, snap_end, stream_offset, object_ref
         from checkpoints where database_id = $1 order by lsn`,
      [databaseId],
    )
    return res.rows.map((r) => ({
      databaseId: r.database_id,
      lsn: r.lsn,
      snapEnd: r.snap_end,
      streamOffset: r.stream_offset,
      objectRef: r.object_ref,
    }))
  }

  /** Delete a single checkpoint row by (database, lsn). */
  async deleteCheckpoint(databaseId: string, lsn: string): Promise<void> {
    await this.db.query(
      `delete from checkpoints where database_id = $1 and lsn = $2`,
      [databaseId, lsn],
    )
  }

  /** True iff ANY checkpoint row (across all databases) references `objectRef`. */
  async objectReferenced(objectRef: string): Promise<boolean> {
    const res = await this.db.query<{ n: number }>(
      `select count(*)::int as n from checkpoints where object_ref = $1`,
      [objectRef],
    )
    return (res.rows[0]?.n ?? 0) > 0
  }

  /** Every distinct checkpoint object_ref still referenced by a row. */
  async allReferencedObjects(): Promise<string[]> {
    const res = await this.db.query<{ object_ref: string }>(
      `select distinct object_ref from checkpoints`,
    )
    return res.rows.map((r) => r.object_ref)
  }

  /**
   * Register a checkpoint (guarded insert on (database, lsn)). Idempotent: a
   * repeat registration of the same (database, lsn) is a silent no-op — the
   * object is content-addressed, so re-publishing the same LSN is benign.
   */
  async registerCheckpoint(input: RegisterCheckpointInput): Promise<void> {
    await this.db.query(
      `insert into checkpoints
         (database_id, lsn, snap_end, stream_offset, object_ref)
       values ($1, $2, $3, $4, $5)
       on conflict (database_id, lsn) do nothing`,
      [
        input.databaseId,
        input.lsn,
        input.snapEnd,
        input.streamOffset,
        input.objectRef,
      ],
    )
  }

  /** The latest checkpoint (highest LSN) for a database, or null. */
  async latestCheckpoint(databaseId: string): Promise<CheckpointRow | null> {
    const res = await this.db.query<{
      database_id: string
      lsn: string
      snap_end: string
      stream_offset: string
      object_ref: string
    }>(
      `select database_id, lsn, snap_end, stream_offset, object_ref
         from checkpoints where database_id = $1
         order by lsn desc limit 1`,
      [databaseId],
    )
    const r = res.rows[0]
    if (!r) return null
    return {
      databaseId: r.database_id,
      lsn: r.lsn,
      snapEnd: r.snap_end,
      streamOffset: r.stream_offset,
      objectRef: r.object_ref,
    }
  }

  /** Close the underlying PGlite. */
  async close(): Promise<void> {
    await this.db.close()
  }
}
