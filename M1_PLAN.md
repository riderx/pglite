# M1 implementation plan — single-host vertical slice

Working companion to `OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md` (the design
doc governs; this file pins the concrete formats, algorithms, and scope cuts
for M1 and is disposable once M1 closes). Everything here is buildable on
**published `@electric-sql/pglite` 0.5.4 with zero native changes** — every
load-bearing move below was proven by an M0 experiment or the live-WAL probe.

## Verified mechanics M1 stands on

1. **Live slice capture needs no flush step.** WAL bytes are visible on the
   real filesystem immediately after a committed statement on a NodeFS
   datadir, and bytes past the insert LSN are zero (probe: scratchpad
   `live-wal-visibility.mjs`). Capture = read `pg_wal` segment bytes between
   LSN bookmarks. (M0-1 proved the transplanted bytes replay.)
2. **Materialize = lay slices + flip `pg_control` to `DB_IN_PRODUCTION` +
   boot + clean close** (M0-1 R2, 18/18).
3. **Plain reopen of a cleanly-closed datadir writes zero WAL** with
   `--no-data-checksums` — insert LSN after open == shutdown-checkpoint
   record end (M0 finding 1). So "attach" after materialize is a plain open.
4. **Synthesized-control attach** (M0-2, 20/20) is in the library for M3's
   lazy path and tests; the M1 runtime does not need it (see attach
   algorithm below).
5. Reopen ~60–80 ms size-independent (M0-3); real WAL bytes as payload
   (M0-4).

## The one invariant that makes slices compose

> **Each cell keeps a capture cursor = the end LSN of the last bytes it
> published (or attached at). Every published slice is exactly
> `(cursor, newEnd]` — contiguous, unfiltered, no gaps ever.** Stray WAL
> from detection queries, boot, or session cleanup rides in the next slice
> (M0-1 slice zero precedent). A slice that would start anywhere other than
> the stream head LSN is a bug, checked at append time
> (`W.baseLsn == streamHeadLsn`).

## Attach / advance / detach algorithm (M1 runtime)

Cold start or advance-to-head for database D on host H:

```text
1. hydrate: latest checkpoint object → base datadir (host cache, per db)
2. tail: read era stream from the checkpoint's recorded offset; collect W
   slices (contiguous from the checkpoint's snapEnd)
3. if tail has W slices newer than the base dir: materialize —
   lay slices into pg_wal, force DB_IN_PRODUCTION, boot throwaway PGlite,
   clean close. Local WAL now holds GENUINE records (B .. B']: end-of-
   recovery + shutdown checkpoint, C' + 120 == B'.
4. publish the sync slice: CAS-append W {baseLsn:B, endLsn:B', kind:"sync"}
   with those genuine bytes. (Required for contiguity: the next commit's
   base is B'. Never skipped. Tiny — a few hundred bytes.)
   On CAS loss: someone advanced the stream; goto 2.
5. cell datadir = copy of base dir; PLAIN OPEN (clean boot, no recovery,
   no synthesis, zero boot WAL). Assert insert LSN == B' == stream head.
   Assert config pins (§9): wal_level=replica, full_page_writes=on,
   data_checksums=off.
```

- Tail empty (fresh checkpoint): skip 3–4; plain open; head = snapEnd.
- **M1a finding: the FIRST reopen after initdb writes ~8 KB of bootstrap
  WAL even with checksums off; reopens 2+ are WAL-silent.** (Refines M0
  finding 1, which measured a datadir already one boot past initdb.)
  Consequence: `createDatabase` = initdb → close → **settling boot**
  (open + `select 1` + close) → cut checkpoint 0. Any checkpoint object
  must be cut from a datadir ≥ 1 reopen past initdb or its snapEnd is not
  a valid zero-boot-WAL attach point.
- **Detach / hibernate**: clean close writes session-teardown WAL + a real
  shutdown checkpoint; publish `(cursor .. checkPoint+120]` as the detach
  sync slice. The stream tail then ends in a shutdown record — the next
  attach's step 3 is a no-op if nothing else landed.
- The §6.1 "host-minted checkpoint record" is satisfied at M1 by publishing
  *genuine* records produced by a host-driven clean shutdown — single-host,
  sequencer-serialized, so no cross-host mint determinism is needed yet
  (that's M4; the scratch-mint recipe stays proven in attach.mjs / its port).
- Commit-time: bookmark E = insert LSN after txn end; slice `(cursor, E]`;
  empty ⇒ read-only ⇒ no CAS; else CAS append via the host sequencer. Win ⇒
  cursor = E, ack. Loss ⇒ §3.7 contract (recycle + re-execute / 40001 /
  fatal reset).

## Resolved environment facts (verified against source, 2026-07-04)

Durable Streams (`@durable-streams/server` **0.3.7**, published; embed via
`new DurableStreamTestServer({ port: 0, dataDir })`, `start()/stop()`;
omit `dataDir` for the in-memory store):

- `Stream-Seq` is compared as a **string with JS `<=` (byte-wise
  lexicographic), scoped per-stream** (`store.ts:794-799`). Tokens must be
  fixed-width; the W3 token `pad10(eraOrdinal) + "," + offset` satisfies
  this (offsets are themselves fixed-width, below). Seq conflict ⇒ `409`,
  body `"Sequence conflict"`, **no headers** — re-HEAD to learn the tail.
- Offsets are opaque fixed-width tokens `<16-digit readSeq>_<16-digit
  byteOffset>`; each append advances byteOffset by `5 + payloadLength`;
  **readSeq never changes per stream** (M1a live-verified; the plan
  originally said readSeq+1 — wrong). Only server-handed boundary tokens
  are valid read positions. Initial: `0000000000000000_0000000000000000`.
- Producer-dedup `204` carries **no** `Stream-Next-Offset` — the client
  recovers the tail with a follow-up HEAD (implemented in
  `pglite-cell/src/stream-client.ts`).
- The official `@durable-streams/client` is a runtime dep: constants +
  `head/create(body)/close(body)/delete` delegated; CAS append and raw
  reads stay purpose-built (typed conflicts + offset math; upstream
  candidate at M4).
- Append success: `204` (no producer headers) / `200` (with) +
  `Stream-Next-Offset` (the appended message's end offset). Producer
  headers `Producer-Id/-Epoch/-Seq`: validated **before** the seq check
  (dedup-to-204 wins over 409 — exactly §3.8's need); dedup matches the
  producer tuple only (**body hash is NOT checked — W2 byte-identical
  retries are entirely our discipline**); stale epoch ⇒ `403` +
  current epoch echoed in `Producer-Epoch`; new epoch must start at seq 0;
  seq gap ⇒ `409` + `Producer-Expected-Seq`/`Producer-Received-Seq`.
- `POST` with `Stream-Closed: true` + body = atomic append+close; append
  to closed ⇒ `409` + `Stream-Closed: true` + `Stream-Next-Offset`.
- `PUT` with body stores the body as the first append (the `O` frame
  ride-along); PUT idempotency compares config only (silently discards a
  matching re-PUT's body — the §2.4 unique-URL rationale, verified);
  create with `Content-Type: application/octet-stream` for byte mode and
  keep using it on every append (mismatch ⇒ 409). No TTL by default.
- Reads: `GET ?offset=<-1|now|token>[&live=long-poll]`; byte-mode body =
  **raw concatenated payloads, boundaries stripped**; `Stream-Next-Offset`
  on every response; at/past tail ⇒ `200` empty; long-poll timeout ⇒
  `204`. `HEAD` ⇒ tail offset + `Stream-Closed`. Errors are text/plain.
- Empty-body appends are rejected (`400`) unless closing — fences are
  fine (a `'0'` frame is a nonempty body).
- The published `@durable-streams/client` hides offsets (`append()`
  returns void) — we write a minimal fetch client instead, reusing only
  the header-name constants.

Monorepo: pnpm 9.7.0, Node ≥ 20, tsup + vitest (^1.3.1) + eslint-flat +
prettier (no semi, single quote) + attw; new packages under `packages/*`
are auto-wired into CI (`pnpm -r stylecheck`, `--filter "...^pglite"`
test/typecheck) once they depend on `@electric-sql/pglite` at
`workspace:*` — the workspace package IS 0.5.4 with dist + WASM present,
byte-equivalent to the published pin the M0 experiments used. Mark the
three packages `"private": true` until first release. Copy
package.json/tsconfig/tsup/vitest shapes from `packages/pglite-socket`
(its vitest config also shows the serialize-tests pattern for
fixed-port TCP suites).

PGlite (workspace 0.5.4): `execProtocolRaw`/`execProtocolRawStream`
(wrap calls in `db.runExclusive` yourself), `isInTransaction()`,
`close()` runs a genuine Postgres shutdown (shutdown checkpoint) via
atexit, NodeFS is a synchronous passthrough (WAL bytes visible on the
host fs immediately; PGlite runs `-F` so no platter fsync — same-host
visibility is what we need). `@electric-sql/pg-protocol` parses
backend messages only (`Parser` → `readyForQuery.status`,
`commandComplete`, `DatabaseError.code`); it can NOT serialize backend
messages — the proxy hand-rolls ErrorResponse bytes (`'E'` + len +
`{S,V,C,M}` cstrings + NUL, then `ReadyForQuery`).

## Frame codec v1

```text
frame   := type(1 byte) | payloadLen(u32 BE) | payload
'W'     := hdrLen(u16 BE) | headerJson | walBytes
others  := headerJson (utf8 JSON)
```

Every header carries `{v:1, eraId, expectedOffset}` (W4: readers void frames
whose expectedOffset ≠ the append position they occupy; one append = one or
more frames, all carrying the append's position; the first must sit exactly
there). LSNs are pg-text strings (`"0/1A2B3C"`); hashes `sha256:<hex>`.

- `W`: `{v, eraId, expectedOffset, commitId, kind: "commit"|"sync"|"floors",
  baseLsn, endLsn, sliceHash}` + raw WAL bytes. `w` (object-ref spill): M2.
- `O` (era open, in creating PUT body): `{v, eraId, ordinal, prevEraId:null,
  prevEraUrl:null, baseOffset, baseLsn, snapEnd, checkpointRef}`
- `K` (checkpoint): `{v, eraId, expectedOffset, lsn, snapEnd, checkpointRef,
  sha256}`
- `L` (lease): `{v, eraId, expectedOffset, kind:"head"|"gc-pin", holder,
  epoch, ttlMs, base?:{offset,lsn}}`
- `0` (fence): `{v, eraId, expectedOffset}`
- `S`/`G`/`N`/`F`/`X`: codec support + tests only; not produced at M1
  (S: M2 rotation; G: M4; N: M3; F: M2; X: M5).

CAS token (W3): `pad10(eraOrdinal) + "," + offset`. Producer tuple per §2.3;
retries byte-identical against the journaled URL (W2); **every** append
CAS'd (W1). Pending journal + §3.8 fence-then-read recovery from the first
commit: journal file per commitId, fsync'd before POST, resolved from stream
bytes at immutable positions.

**Position-checked reader (W4), exact algorithm.** One CAS append = one or
more whole frames, all carrying the same `expectedOffset` = the tail token
the writer observed (== the position the append starts at). Byte-mode reads
return concatenated payloads with boundaries stripped, but boundaries are
reconstructible: starting from a known boundary token `R_B` (16-digit
readSeq `R`, 16-digit byteOffset `B`), parse frames, group consecutive
frames sharing one `expectedOffset` value, let `P` = total encoded byte
length of the group; the group is valid iff its `expectedOffset == R_B`;
the next boundary is `R_(B+5+P)` — **`readSeq` stays fixed per stream in
the shipped server (store.ts:1366-1372, verified live at M1a); only
byteOffset advances, by exactly 5 + payload per append**. Cross-check the
final computed
boundary against the response's `Stream-Next-Offset`; mismatch or a
mis-positioned group ⇒ **void the frame(s) and stop** (the §2.6
metadata-rollback defense). The `O` frame (PUT body) sits at the initial
token and is group zero.

## Scope cuts (documented, deliberate — each lands at its owning milestone)

- **One era per database.** Era-shaped everything (URLs
  `…/era/<6-digit-ordinal>-<ulid>`, O frames, era-qualified tokens; NOTE:
  manifest `era.path` carries a LEADING slash — `DsStreamClient` joins by
  string concatenation, M1b finding), but no
  rotation: K frames land mid-era; joiners hydrate the latest checkpoint and
  tail from its recorded offset. Rotation state machine = M2 (§6.1).
- **Advance = recycle-with-materialize**, host base-dir shared per database;
  live tail apply = M3. Cell datadir = copy of the host base dir (lazy VFS =
  M3).
- **Sequence cursor is host-local floors**: committed `nextval` needs
  nothing (XLOG_SEQ_LOG rides the slice); the abort-only hazard is closed by
  reading `pg_sequences.last_value` from a cell after any aborted txn and,
  at next attach, applying `setval` floors and publishing them immediately
  as a `floors` slice before serving (so read-only sessions stay
  empty-slice). Native nextval clamp + G-frame leases = M4.
- **Head lease & gc-pin frames flow but nothing enforces them** (single
  host): sequencer appends/refreshes `L{head}`; tainted pinned sessions get
  `L{gc-pin}` + host-side max-TTL ⇒ fatal reset. Cross-host meaning = M4.
- Session GUC replay on conflict-recycle: replay tracked `SET`s; prepared
  statements documented as lost on conflict (pooler-grade replay later).
- Auth: static bearer token, off by default in dev.

## Packages (§14.7)

`packages/pglite-cell` — library, single-cell-complete:
- `frames.ts` codec + position-checking reader; `lsn.ts`, `crc32c.ts`
- `stream-client.ts` minimal fetch client for the DS protocol via the
  gateway: create-with-body, CAS append (dual headers `Stream-Seq` +
  `Stream-Expected-Offset`, producer headers), catch-up read, long-poll,
  HEAD
- `journal.ts` pending-commit journal + §3.8 `recover()`
- `datadir.ts` port of harness.mjs/attach.mjs: readControl/forceCrashState/
  transplantRange/segment math/mint (mint kept for M3/tests)
- `tail.ts` era tail reader → ordered slices + control-frame state
- `cell.ts` one PGlite instance: attach(plain-open), bookmarks, capture,
  execProtocolRaw passthrough, taint flags, recycle, detach
- `committer.ts` solo sequencer: serialize appends, own the capture-cursor
  invariant, re-execute loop for one-shots
- `gateway-api.ts` the `GatewayApi` TS interface (implemented in-process by
  pglite-gateway core and over HTTP by `HttpGatewayClient`)

`packages/pglite-gateway` — service + embeddable core:
- fs-backed content-addressed object store (sha256); embedded DS TS server
  (dev deployment); stream proxy endpoints (validate + forward, stateless);
  manifest read API; `createDatabase` (initdb → clean close → settling boot
  → checkpoint 0 → era-1 PUT with O frame → control-plane rows); control
  plane = embedded
  PGlite with `databases/eras/checkpoints` tables (§14.6 schema v0)
- statelessness: only disposable caches; the §14.5 MAY/MUST-NEVER contract
  as a lint-style test (no module-level mutable authoritative state)

`packages/pglite-cell-server` — multi-tenant host:
- host: per-db base-dir manager + materializer, one tailer per db, commit
  sequencer (head lease), watermark gate W (no statement on a cell whose
  base < W; base never decreases), sequence floors, hibernation/wake,
  gc-pins
- proxy: TCP wire-protocol server (pglite-socket salvage), cell-per-
  connection, response buffering until CAS resolution, read-only fast
  flush, re-execute for one-shots, 40001 ErrorResponse synthesis at COMMIT
  for interactive losses, taint detection (temp schema / holdable cursors /
  advisory locks via one post-txn catalog probe), fatal session reset

## Build order

- **M1a** cell core: frames, stream client, journal+recovery, datadir
  engine, tail, committer. Exit test: two solo cells racing through one raw
  DS stream — loser re-executes, dumps converge, journal recovery decides
  landed/lost correctly after simulated crashes.
- **M1b** gateway v0 + control plane. Exit: createDatabase → attach via
  gateway (HTTP and in-process) → commits → cold re-attach from checkpoint 0
  + tail.
- **M1c** cell host. Exit: two connections' cells on one host — sequencer
  serializes, watermark gives read-your-writes, abort-only sequence floor
  survives recycle, hibernate/wake round-trips.
- **M1d** proxy v0. Exit: real `pg` clients over TCP — §3.7 contract table
  enforced row by row; client-observation kill-tests (zero bytes before CAS
  resolution at every kill point).
- **M1e** checkpoint worker (materialize → tar → upload → K frame →
  control-plane row) + wake-from-checkpoint + scale-to-zero demo script.
- **M1f** E2E: convergence oracle v0 (replay full stream via materializer,
  pg_dump-diff vs every live cell), contract tests, README(s), design-doc
  M1 status update.

## Test matrix (the §16 subset M1 must pass)

- convergence oracle v0 (randomized two-cell workload, abort-heavy)
- §3.7 contract: one-shot re-execute; temp-table session fatal reset;
  interactive 40001 at COMMIT; DDL one-shot re-execute; read-only never
  CAS'd
- client-observation: kill CAS at each pipeline step ⇒ client saw zero
  bytes (one-shot) / only pre-COMMIT rows (interactive)
- §3.8: crash between POST and ack at each step ⇒ landed/lost decided from
  stream bytes; fence carries CAS token
- capture-cursor: detection-noise and boot-noise ride the next slice;
  `W.baseLsn == head` asserted at every append
- sequence floors: abort-only nextval → recycle → no duplicate draw
- watermark: commit on conn A, immediate read on conn B — never stale
- scale-to-zero: create → write → idle → hibernate (detach slice) → wake
  from checkpoint+tail → data intact, ~cold-start budget logged
```
