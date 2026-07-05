# Hardening wave — audit gaps, decisions FIXED (executable by any model)

From the 2026-07-05 design-vs-shipped audit. Four fix groups + tests + CI.
Each item's design decision is final; implement as written. Gate: full
corpus green (core 269 / cell 80 / gateway 51 / cell-server 77+M6).

## H1 — response-size safety (§3.5 ladder, v1 rungs)

- Proxy/unit buffering: in-memory up to `bufferMemoryMax` (default 8 MiB)
  → spool to a per-connection temp file up to `bufferSpoolMax` (default
  256 MiB) → beyond: abort the unit with 40001 + HINT "response exceeded
  proxy buffer; use a READ ONLY transaction or cursor" (the lease-probe
  rungs 3–4 are SPEC'D in §3.5 but deferred — document inline; do NOT
  fake them).
- **Declared read-only streaming** (the true fix): on `BEGIN READ ONLY`,
  `START TRANSACTION READ ONLY`, or session `default_transaction_read_only
  = on` (text classifier), stream output to the client incrementally with
  NO buffering; at txn end assert the capture is empty (nonempty ⇒
  protocol-bug error, loud). Route `COPY ... TO STDOUT` the same way when
  the txn is read-only.
- §16 client-observation additions: kill-at-every-step repeated in the
  spooled regime; a 100 MB read-only SELECT streams under constant memory
  (assert RSS delta bound loosely).

## H2 — read-cell WAL suppression (native, tiny)

Submodule (§14.8 ritual): GUC `pglite.suppress_read_wal` (default off) +
ONE guard hunk at the top of `heap_page_prune_opt` (return when set).
Hint bits stay in-memory-only (checksums off ⇒ no WAL) — no further
hunks. JS: set the GUC on read-attached cells at open; drop the
write-upgrade path's "stray WAL" tolerance note. Test: a hot-page read
loop on a read cell captures nothing with the GUC on.

## H3 — `w` spill frames (§2.2)

Slices > `sliceSpillBytes` (default 4 MiB): committer uploads the WAL
bytes via the gateway object store and appends a `w` frame `{ ...same
header fields..., objectRef, byteLength }` (no inline bytes). Tailer +
materializer resolve `w` via object fetch (verify sliceHash after).
Gateway append-validation accepts `w` (well-formed, no size cap issue).
Journal/recovery: DECIDE matches on commitId+sliceHash exactly as for
`W`. Test: a >4 MiB single-commit bulk insert round-trips through spill;
convergence oracle green; 32 MiB proxy cap no longer constrains commits.

## H4 — feature policing (§9 table rows)

Statement-text classifiers (consistent with existing ones, documented
simple-protocol approximation): `PREPARE TRANSACTION` ⇒ 0A000 "two-phase
commit is not supported"; `CREATE DATABASE` / `CREATE TABLESPACE` ⇒
0A000 (single-database streams); `ALTER SEQUENCE ... RESTART` ⇒ 0A000 on
leased sequences; user `setval(...)` ⇒ ALLOWED but post-txn the host
re-probes and re-grants strictly above the set value + republishes
floors (keeps §5.3 rule 5 honest without breaking migrations that
setval). Tests for each; §3.7/§9 README rows updated.

## H5 — missing §16 tests

- `SKIP LOCKED` double-claim: two hosts' workers claim the same job row ⇒
  exactly one wins, loser gets 40001 at commit (document queue guidance).
- RETURNING-heavy ORM abuse: 200 inserts with RETURNING under two-host
  contention ⇒ ids observed by clients are exactly the committed ids.
- Migration-tool flow under advisory policy ('local-warn'): acquire
  advisory lock, run DDL batch, release — succeeds with the warning.
- FK-heavy multixact minting under savepoints across two cells ⇒ oracle +
  pg_amcheck-style sanity (dump equality suffices).

## H6 — CI

- `scripts/native-budget-check.mjs`: counts `#ifdef __PGLITE__` hunks in
  existing PG files on the submodule branch vs an allowlist file
  (`native-hunk-budget.json`), fails on growth without an allowlist
  change; asserts submodule gitlink is remote-reachable and its branch
  name matches the superproject branch (§14.8); asserts
  `src/include/pglite.h`'s export surface changes only alongside doc
  changes. Wire into build_and_test.yml as one job step; also ensure the
  three new packages' suites actually run in CI (they auto-join via the
  `...^pglite` filter — verify the fleet/rebase suites' runtime is
  CI-tolerable and mark the slowest files with a CI-skip env guard if
  >10 min, documented).

## H7 — test-speed optimizations (Sam, 2026-07-05)

The suites are integration-real by design (no mocks; every scenario
boots genuine cells/gateways/streams) — keep that. Cut the harness
waste instead:

1. **File-level parallelism.** The `maxWorkers: 1, fileParallelism:
   false` vitest config was inherited from pglite-socket's fixed-port
   pattern, but nearly every test binds port 0. Audit each test file
   for hidden shared state (fixed ports, shared scratch paths, global
   env like PGL_* flags); enable `fileParallelism: true` with a
   sensible `maxWorkers` (e.g. 4) in pglite-cell / pglite-gateway /
   pglite-cell-server; keep any genuinely port-fixed or
   resource-hungry files serialized via `describe.sequential` or a
   `*.serial.test.ts` naming convention + config match. Expected win:
   2–4×.
2. **Template-datadir fixture.** A shared test helper (pglite-cell
   `tests/` util or a small `src/testing.ts` export): lazily create ONE
   settled datadir per (initdb-params) variant per vitest worker
   (initdb + settling boot + clean close, ~3 s once), then every test
   clones it with `cpSync` (~50 ms). Sweep all suites' inline initdb
   fixtures onto it. This is ALSO the production
   checkpoint-template-hydration seed (M0-3's finding: initdb 1.4 s vs
   reopen 80 ms): follow-up = gateway `createDatabase` clones a cached
   template datadir instead of running initdb per database — implement
   if time allows, else leave the helper + a TODO wired for it.
3. **Timeout hygiene.** Grep for fixed sleeps/awaits in tests; replace
   with condition polling at 10–25 ms; drop long-poll fixture timeouts
   to 250 ms where the test isn't measuring long-poll itself; lease-TTL
   tests keep their real clocks (correctness) but run in the parallel
   pool so they overlap other files.
4. **Scratch-dir hygiene.** All mkdtemp scratch under one per-run root
   so failures don't strand tens of MB; verify no test writes into the
   repo tree.
5. Record before/after wall-clock per package in the final report
   (target: full three-package corpus under ~8 min on this machine;
   CI markers from H6 then become mostly unnecessary — revisit H6's
   skip-guards after measuring).

Order: H2 first (needs a docker rebuild — batch with any other native
work in flight), then H1/H3/H4 (JS, parallel-safe across packages),
then H5/H6/H7 (H7 last so it measures the finished suites; it may also
run FIRST for its own wave if wall-clock is hurting wave turnaround —
implementer's call, report which).
