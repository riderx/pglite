# @electric-sql/pglite-gateway

The **storage/stream gateway** (§14.5): a thin, stateless data-plane server
that fronts both the object store and the Durable-Streams service. Cell hosts
and cells speak only to it; it holds the real storage/stream credentials and
enforces tenant capabilities. It is also the local **control plane** at M1 (a
real Postgres — PGlite itself in dev), authoritative for topology and lifecycle
but **never** for database contents.

> Status: **M1**. Private, unpublished, APIs unstable and expected to change.

For the full design see
[`OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md`](../../OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md)
(§14.5–§14.6) and [`M1_PLAN.md`](../../M1_PLAN.md).

## Three deployments, one API surface

One binary, three deployments — the same API surface in all three is what makes
sandbox → fleet promotion an upload, not a migration:

```text
embedded    in-process inside an app sandbox, e.g. Supabase-lite (fs, no network)
dev         single node, fs backend, embedded DS test server   <- shipped at M1
fleet       horizontal pool in front of object storage + a DS service
```

Only **dev/fs** is shipped at M1: an embedded `GatewayCore` (object store +
control-plane PGlite + embedded Durable-Streams test server).

## The statelessness invariant

The gateway holds **zero** authoritative state; everything in it is disposable
cache, reconstructible from the stream, the object store, or the control plane.
Verbatim (§14.5):

```text
MAY hold (disposable, TTL'd; loss costs latency only):
  object read-through cache · manifest/era-pointer cache ·
  token-verification keys · approximate metering counters

MUST NEVER hold (loss or divergence would cost correctness):
  append/CAS serialization state · leases or locks · commit journals ·
  connection/session state (that is the session proxy's job, §3.5, and it
  lives on the cell host) · anything two gateway instances could disagree
  about
```

Any request can hit any instance; `kill -9` of any gateway at any moment
changes latency and nothing else. **CAS enforcement cannot live in the gateway**
— only the stream server's per-stream critical section can decide the tail. A
gateway-local "fast-path" of the CAS check is the canonical statefulness creep
to reject in review.

## The stream-proxy acceptance criterion

The load-bearing endpoint is the **verbatim stream proxy**: an unmodified
`DsStreamClient` (from `pglite-cell`) CAS-commits through the gateway exactly as
if it were talking to the DS server directly. All methods on
`/v1/db/<id>/stream/*` forward verbatim to the embedded DS server — method,
query string, body bytes, and every `Stream-*` / `Producer-*` header pass
through untouched. On append (POST with a nonempty body) the gateway
**validates-and-forwards** only: well-formed frames, size caps, `W.baseLsn`
sanity, and the advisory `Stream-Expected-Offset` header (dual-header posture,
§2.3). It never touches the CAS position — that stays in the DS server's append
lock.

## createDatabase recipe

`GatewayCore.createDatabase(name)` builds an era-0 database end to end
(M1_PLAN recipe):

1. `initdb` a scratch PGlite with `--no-data-checksums`; close.
2. **settling boot** (open + `select 1` + close) — the M1a finding: the first
   reopen after initdb writes ~8 KB of bootstrap WAL, so a checkpoint object
   must be cut from a dir ≥ 1 reopen past initdb or its `snapEnd` is not a valid
   zero-boot-WAL attach point.
3. `readControl` ⇒ C0; `snapEnd = C0 + 120`.
4. pack the settled datadir ⇒ object store ⇒ `checkpointRef`.
5. PUT-create the era-1 stream with the `O` frame as the body.
6. write control-plane rows (`databases`, `eras`, `checkpoints`).
7. return the `Manifest`.

## Control-plane schema (v0)

Authoritative for topology and lifecycle only — never data (the stream is the
truth for every database's contents):

```sql
databases   (id uuid pk, name unique, status, created_at)
eras        (database_id, ordinal, era_id, path, base_offset,
             base_lsn pg_lsn, sealed, pk (database_id, ordinal))
checkpoints (database_id, lsn pg_lsn, snap_end pg_lsn, stream_offset,
             object_ref, created_at, pk (database_id, lsn))
```

The commit path is cell → gateway → stream — **never** the control plane;
control-plane writes are lifecycle-rate (create, checkpoint), never commit-rate.

## HTTP endpoints (`GatewayServer`, dev/fleet)

| Method | Path                           | Purpose                                         |
| ------ | ------------------------------ | ----------------------------------------------- |
| POST   | `/v1/db`                       | create a database (the recipe above)            |
| GET    | `/v1/db`                       | list databases                                  |
| GET    | `/v1/db/:id/manifest`          | current manifest (era pointer + checkpoint)     |
| GET    | `/v1/db/:id/checkpoint/latest` | latest checkpoint row                           |
| POST   | `/v1/db/:id/checkpoint`        | register a checkpoint                           |
| ALL    | `/v1/db/:id/stream/*`          | verbatim CAS/read proxy to the DS server        |
| GET    | `/v1/objects/:ref`             | fetch a content-addressed object (read-through) |
| PUT    | `/v1/objects`                  | store a content-addressed object                |

The `embedded` deployment (Supabase-lite) skips the HTTP surface and calls
`GatewayCore` in-process.

## Module map

`GatewayCore` (`core.ts`) · `ControlPlane` (`control-plane.ts`) ·
`FsObjectStore` (`object-store.ts`) · checkpoint pack/extract
(`checkpoint-object.ts`, incl. `extractDatadir`) · `GatewayServer`
(`http.ts`). Public API re-exported from `src/index.ts`.
