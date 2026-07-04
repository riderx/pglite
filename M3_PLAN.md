# M3 implementation plan — followers, freshness, NOTIFY headline

> **STATUS: M3 (JS subset) CLOSED (2026-07-04).** Shipped per the
> re-sequencing note below; live apply remains with the M5 native wave.
> Findings: PGlite's execProtocolRawStream bypasses onNotification — the
> proxy's 'A'-strip walk IS the harvest; LISTEN verified WAL-silent; a
> latent head-computation bug (shutdown record straddling an 8 KiB WAL
> page ⇒ `checkPoint+120` under-counts) found and fixed in every
> materialize path; **pure-NOTIFY (no-write) transactions produce an
> empty slice and therefore don't distribute — fixed in M4a via an
> N-only CAS control append when notifications are pending on an empty
> capture.**

Companion to design doc §7, §8.1, §10.2 and §15 M3. One structural
decision up front, made when the goal became "complete M2–M6":

> **Re-sequencing.** M3's *incremental live apply* (page-version index,
> eager special-record set applied to a LIVE cell, lifting the
> tainted-session pinning) has no JS implementation path — applying clog
> bits, invalidations, and buffer discards to a running Postgres requires
> the §14.2 native hooks (xlogreader wrapper, single-page redo,
> `PgliteDropRelationBuffersRange`, cache-flush exports). The
> `postgres-pglite` docker build (`electricsql/pglite-builder`) is
> available in this environment, so that work is FEASIBLE — but it shares
> its entire toolchain, submodule discipline (§14.8), and risk profile
> with M5's native work. Therefore: **M3 ships the JS-complete subset now
> (NOTIFY headline, freshness modes, DDL propagation, N-frame protocol);
> live apply moves into the native wave, executed together with M5 after
> M4.** Every M3 capability that users can see ships at M3; what moves is
> an internal mechanism upgrade (recycle-advance → in-place advance).

## NOTIFY — cluster-wide LISTEN/NOTIFY in commit order (§10.2, headline)

Mechanics (JS-only, no native hooks — the design's "cell harvests its
pending notification list" is realized via host LISTEN aggregation):

1. **LISTEN registry.** The proxy classifies `LISTEN x` / `UNLISTEN x` /
   `UNLISTEN *` simple-protocol statements per session (forwarded to the
   cell as normal; also recorded). The DatabaseRuntime keeps the union of
   listened channels across its sessions.
2. **Cell auto-LISTEN.** Every write-capable cell LISTENs the union
   (applied at cell open like SET replay; delta-applied when the union
   changes). PGlite then delivers `NotificationResponse` for any commit
   on that cell touching those channels — harvested via `onNotification`
   during the unit execution, AFTER local commit, BEFORE our CAS (§10.2
   timing: local commit precedes capture, so pending notifications are in
   hand when the W frame is built).
3. **N frames ride the winning POST** (same CAS append as the W frame —
   atomic by construction; lost attempts emit nothing; a re-execution
   emits its own). Header: `{ v, eraId, expectedOffset, commitId,
   channel, payload, commitLsn }`, one frame per notification, ordered.
4. **Uniform delivery via the tailer.** ALL client-facing delivery is
   driven by N frames arriving through the database tailer, in stream
   order == commit order, globally — including the committing session's
   own connection. The proxy therefore STRIPS `NotificationResponse`
   ('A') messages from buffered unit output (a light type+length walk;
   pg-protocol parses, the splitter slices) so nothing is delivered
   twice and every listener — committer included — hears the same global
   order. The proxy synthesizes 'A' messages to attached clients whose
   sessions LISTEN the channel.
5. Known M3 limits: delivery to live listeners only (offset replay for
   reconnecting listeners is the reserved M6 question); channels only
   fan out to cells while ≥1 session on the host LISTENs them
   (host-local union; the M4 fleet propagates the union via the already-
   flowing L/G-style frames or simply relies on every host's own tailer —
   cross-host listeners hear everything because N frames are in the
   stream, which IS the cross-host bus: only the *auto-LISTEN of writer
   cells* is host-local, and the committing host always has the writer).

**The demo** (`scripts/demo-notify.ts`): three `pg` connections — A
writes (trigger `pg_notify`), B and C LISTEN on different connections
(different cells), both receive in commit order; plus a raw stream tail
printing the same events as an activity feed with zero Postgres
connections. Side-by-side note: stock Postgres rejects LISTEN on hot
standbys entirely.

## Freshness modes (§7)

Product-visible session setting, intercepted by the proxy as
`SET pglite.freshness = '<mode>'` (not forwarded to the cell):

- `session` (default) — the existing watermark gate; unchanged.
- `linearizable` — before each idle-state unit: stream HEAD + tailer
  catch-up past the observed tail, THEN the watermark gate. The only mode
  guaranteed to see another host's just-acked commit (single-host today;
  the check is real regardless).
- `local` — skip the gate entirely; serve at the cell's current base.
- `pinned <lsn>` — read-attach at a fixed base; never advances; write
  attempts ⇒ error (documented; forks/time-travel tooling).
- `bounded-stale <ms>` — gate only when the base is older than Δ
  (wall-clock of last advance).

## DDL propagation (§8.1, recycle-based at M3)

Tests pinning the semantics (mechanism upgrade comes with live apply):
DDL on conn A (one-shot CREATE/ALTER + post-DDL DML); conn B (separate
cell, read-attached) sees the new schema + rows on its next statement via
the watermark advance; a raced `ALTER TABLE` one-shot re-executes; the
convergence oracle stays clean. `CREATE DATABASE`/tablespaces stay out of
scope (single-database streams at M3; WAL_LOG pin noted for later).

## Also in this wave

- `unlogged table` policy (§9): proxy/session detects `CREATE UNLOGGED
  TABLE` (statement classification) ⇒ loud ERROR (the tested policy).
- Read-replica soak: N read sessions at `local`/`session` against one
  writer, byte-count assertion that read advances append nothing.

## Deferred to the native wave (with M5)

Lazy tail apply + page-version index; eager special-record set; live
invalidation (three carriers); in-place sibling advance (lifts tainted
pinning + the diverged-base re-materialize cost); no-restart DDL claim.

## Exit criteria

The NOTIFY demo runs (three connections + stream feed, global order);
freshness modes behave per contract under test (incl. linearizable
catch-up and pinned rejection of writes); DDL propagation suite green;
all prior suites green.
