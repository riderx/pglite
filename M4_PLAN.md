# M4 implementation plan — multi-host fleet + upstreaming

Companion to design doc §15 M4, §5.3, §3.2, §14.5 and OQ1. Three waves.

## M4a — cross-host protocol hardening (pglite-cell / pglite-cell-server)

- **Real cross-host CAS contention**: two+ `CellHost`s over one gateway;
  the full §3.7 contract exercised across hosts (losses now come from
  genuinely foreign appends, not same-host siblings). Watermark stays
  host-local; cross-host read-your-writes = commit-LSN session tokens +
  `linearizable` (already implemented; deepen the suite: lagged second
  host, token round-trip through a client reconnecting to the other
  host).
- **Head-lease semantics (§3.2), JS enforcement**: the holder pipelines;
  a non-holder host attempts optimistically (allowed by design — the
  lease has zero correctness weight) but applies lease-aware backoff
  after a loss (read the live L frame; if a fresh lease exists elsewhere,
  delay/batch retries instead of hammering); lease refresh by the holder;
  **migration**: on TTL expiry any host claims via a CAS'd L frame with a
  higher epoch; a stale holder's next refresh loses the CAS and it
  demotes. No cross-host write proxying at M4 (the optimistic path IS the
  correctness story; proxying is a latency optimization deferred).
- **Sequence grants (§5.3), G frames + incarnation burn — JS subset**:
  `G { kind:'sequence', seqName, start, end, grantee: hostId,
  granteeEpoch }` CAS-appended; grant state = replay of the era chain
  (tailer records G frames). Host draws a grant per (db, sequence) on
  first use, asserts the floor via `setval` (published as a 'floors'
  slice — existing machinery), monitors consumption via the existing
  post-txn probes, takes the next grant at 50% consumption, and **burns
  the remainder on any incarnation end** (recycle/hibernate/crash —
  epoch-bound grants; a new incarnation must take a fresh grant).
  HONESTY CLAUSE: without the native `nextval_internal` clamp (§14.2, a
  tiny hunk arriving with the native wave), nothing hard-stops a cell's
  32-ahead prelog from crossing its grant end; grants are sized 4096 with
  renewal at 50% so the window is unreachable under the abuse suite
  (hot sequence hammered from two hosts, zero duplicates required) — the
  clamp turns this from tested-safe into enforced-safe.
- **Convergence oracle, randomized multi-host**: N hosts × M sessions ×
  randomized abort-heavy workload → full-stream materialize → pg_dump
  diff vs every host's view + identity-chain assertions (§5.1). Runs as
  a normal (slow) vitest file with bounded iterations.

## M4b — durable-streams upstreaming (the parked OQ1 work, now due)

In `~/Code/durable-streams`, branch `optimistic-physical-replication`
(paired name; push the branch, do NOT touch main). Revive the parked
store-layer WIP (local commit 32955c81) and complete:

1. **Strict `Stream-Expected-Offset`** (§2.3 extension): enforced
   append-iff-tail inside the per-stream append lock in BOTH the
   in-memory and file stores; `Stream-Next-Offset` on the 409 so losers
   skip the extra HEAD; wired through server.ts; spec'd in PROTOCOL.md;
   per-stream `Stream-Seq` scope documented while there.
2. **Conformance tests** (their CLAUDE.md mandates YAML conformance
   suites over unit tests) + changeset.
3. **Client**: typed CAS-append API — one POST carrying
   Stream-Seq/Stream-Expected-Offset + producer headers, returning
   `{ nextOffset }` | typed conflict (the shape pglite-cell's `casAppend`
   already defines), fork-header constants, and offset-returning append.
   Changeset; no breaking changes.

pglite-cell already sends both headers (dual-header posture) — nothing
to change there until these ship to npm; a follow-up swaps `casAppend`'s
internals for the official API.

## M4c — gateway fleet mode completion

- The M2c gap: era-row/pin HTTP routes so rotation works through the
  HTTP gateway mode; then the fleet test — N `GatewayServer` instances
  (shared object store dir + control plane + DS server), random
  per-request routing, kill instances during commit/rotation/wake flows
  (§16 statelessness chaos suite, now with rotation).
- Fleet-mode config surface on `CellHost` ({ url } gateway handles get
  the same capabilities as in-process).

## Exit criteria

Two hosts, one database: contended writes converge with exactly-once
semantics and zero duplicate sequence values under the abuse suite;
lease migrates on expiry with epoch fencing; linearizable reads see the
other host's acks; rotation + wake work through randomly-routed HTTP
gateways with instance kills; the durable-streams branch carries the
strict extension + client API with green conformance tests, pushed.
