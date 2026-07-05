# M6 implementation plan — productization

Companion to design doc §15 M6. Four pieces; the console ships first
(parallel with the native wave), the rest land after M5 frees the
cell-server package.

## Console (demo GUI) — in pglite-gateway, zero dependencies

A single-file HTML+JS app served by the gateway (`GET /console`, static
string route on the existing Hono app — no build step, no npm deps, no
lockfile changes; plain fetch + EventSource-free long-poll against the
existing HTTP API):

- database list (name, status, era ordinal, latest checkpoint LSN, dials)
  with create + fork buttons (`POST /v1/db`, `POST /v1/db/:id/fork`);
- per-database detail: connection string for the proxy (host/port shown
  from a `GET /v1/console-info` helper the cell-server registers…
  M6 v0: a static config blob the operator passes to GatewayServer),
  era history, checkpoints, pins, GC button;
- **live activity feed**: tail the era stream through the existing
  stream proxy (`GET /v1/db/:id/stream/<era>?offset=…&live=long-poll`)
  and decode frames IN THE BROWSER (a ~80-line JS mirror of the frame
  codec — W/K/L/G/N/S/O rendered as typed feed rows; N frames rendered
  as notification toasts) — the §10.2 "GUI is just another stream
  listener" demo, no Postgres connection anywhere;
- counters panel: bytes moved on wake / checkpoint sizes / era length
  (derivable from manifest + HEAD offsets) — the §6.5 footprint story
  ("this database woke by moving X KB").

## Janitor automation (cell-server)

- Periodic per-active-database maintenance driven by the host:
  scheduled `VACUUM (no FULL)` through an ordinary session at a
  configurable cadence (commits ride the normal CAS path — §6.4), with
  freeze-age monitoring (`datfrozenxid` age query → forced vacuum
  freeze past a threshold);
- GC scheduling: `runGc` on an interval + after rotation/hibernate;
- both off by default, dials on the host options + per-db columns.

## Advisory-lock policy (§4.6 interim → M6 decision)

Decision for M6: **loud-local** (the design's documented interim,
hardened): first advisory-lock use in a session raises a WARNING naming
the cell-local scope (proxy-injected NoticeResponse), session-scoped
advisory locks keep tainting (existing), and a host option
`advisoryLocks: 'local-warn' | 'error'` lets operators pick the strict
mode (0A000 on any advisory call). The stream-level lock service stays
future work (needs product signal first).

## Graduation tooling

- `graduateDatabase(db)`: logical export via `pglite-tools`' pg_dump on
  a linearizable-fresh cell → SQL artifact + manifest snapshot (the
  documented migration path);
- **OQ7 experiment — RUN, ANSWER: NO (definitive).** A cleanly-closed
  PGlite 18.3 (wasm32) datadir booted under stock native `postgres:18`
  fails at the pg_control compatibility gate: *"initialized without
  USE_FLOAT8_BYVAL but the server was compiled with USE_FLOAT8_BYVAL"*.
  Inherent to 32-bit wasm vs 64-bit native Datum passing — not a
  configuration issue, and no hosted target runs 32-bit Postgres.
  **Physical graduation to stock Postgres is closed; logical
  dump/restore is THE graduation path** (as the design already treats
  as primary). Design doc §12 "ecosystem risks" and OQ7 updated at M6
  close.

## Exit criteria

Console lists/creates/forks databases and shows the live frame feed
with zero Postgres connections; janitor keeps a written-to database's
freeze age bounded and its storage GC'd across a soak; advisory policy
enforced per option with tests; graduation produces a restorable dump
and the OQ7 result is recorded.
