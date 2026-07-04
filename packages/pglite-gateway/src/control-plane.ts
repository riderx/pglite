// The control plane (§14.6): a boring embedded Postgres (PGlite) that is
// authoritative for TOPOLOGY and LIFECYCLE only — which databases exist, their
// eras, and their checkpoints — NEVER for database contents (the stream is the
// truth for that) and NEVER in the commit path. All methods here are
// lifecycle-rate (create / rotate / checkpoint publish); a commit never touches
// this class.
//
// Schema v0 (M1_PLAN "control-plane.ts"): databases / eras / checkpoints.

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

export interface DatabaseRow {
  id: string
  name: string
  status: string
}

export interface EraRow {
  databaseId: string
  ordinal: number
  eraId: string
  path: string
  baseOffset: string
  baseLsn: string
  sealed: boolean
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
    return new ControlPlane(db)
  }

  /** Wrap an already-open PGlite (used when the gateway owns the instance). */
  static async fromPGlite(db: PGlite): Promise<ControlPlane> {
    await db.exec(SCHEMA_V0)
    return new ControlPlane(db)
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
    const res = await this.db.query<DatabaseRow>(
      `select id, name, status from databases where id = $1`,
      [id],
    )
    return res.rows[0] ?? null
  }

  async getDatabaseByName(name: string): Promise<DatabaseRow | null> {
    const res = await this.db.query<DatabaseRow>(
      `select id, name, status from databases where name = $1`,
      [name],
    )
    return res.rows[0] ?? null
  }

  async listDatabases(): Promise<DatabaseRow[]> {
    const res = await this.db.query<DatabaseRow>(
      `select id, name, status from databases order by created_at`,
    )
    return res.rows
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
    const res = await this.db.query<{
      database_id: string
      ordinal: number
      era_id: string
      path: string
      base_offset: string
      base_lsn: string
      sealed: boolean
    }>(
      `select database_id, ordinal, era_id, path, base_offset, base_lsn, sealed
         from eras where database_id = $1 order by ordinal desc limit 1`,
      [databaseId],
    )
    const r = res.rows[0]
    if (!r) return null
    return {
      databaseId: r.database_id,
      ordinal: r.ordinal,
      eraId: r.era_id,
      path: r.path,
      baseOffset: r.base_offset,
      baseLsn: r.base_lsn,
      sealed: r.sealed,
    }
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
