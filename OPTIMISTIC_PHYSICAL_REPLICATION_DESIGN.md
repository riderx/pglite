# Optimistic Physical Replication for PGlite

Routerless, scale-to-zero, forkable, Postgres-compatible databases built from
isolated PGlite/WASM compute cells, object-storage checkpoints, and a
per-database Durable Stream that carries canonical physical WAL.

This is a **serialized multi-writer** design. There is no primary, no
promotion, no failover machinery: any cell can execute and propose commits,
and the stream's compare-and-append admits exactly one proposal at a time.
Writers race; the log serializes; committed history is always a single
linear WAL and nothing is ever merged or conflict-resolved after the fact.

Because the stream is also the coordination and notification bus, the system
delivers **cluster-wide `LISTEN`/`NOTIFY` in commit order** (§10.2) — a
capability stock Postgres replication does not have in any form, and one of
the headline demos.

## Status

Exploratory design, consolidating:

- the single-writer page-image timeline design and implementation in
  `PGLITE_DURABLE_VFS_DESIGN.md` / `packages/pglite-durable-vfs`;
- the multi-writer "storage intent" draft (routerless serverless Postgres via
  optimistic physical replication);
- a source-verified research pass over Postgres (master and the PG 18.3
  `postgres-pglite` fork) and the Durable Streams protocol and reference
  servers, including adversarial verification of the load-bearing claims.

Where this document states a Postgres internals fact, it has been checked
against source. Line references cite the vanilla master checkout unless noted;
the fork is PG 18.3 and exact lines drift, but every cited mechanism was
confirmed to exist in both.

Pinned reference revisions for all internals claims: vanilla Postgres master
@ `4c1a27e53a5` (19devel, 2026-02-25 checkout); PGlite fork
`electric-sql/postgres-pglite` branch `codex/durable-vfs-postgres` @
`582010dc8d` (REL_18_3+23); Durable Streams repo @ `82f9963a` (2026-06-03).
Re-verify line references against these before implementation.

## Design principles

1. **WAL is canonical. Pages are derived.** A commit exists iff its WAL bytes
   are in the database's stream, and the stream head admits one commit at a
   time — many writers may race, but committed history is always a single
   linear WAL; nothing is ever merged. Checkpoints, page images, and indexes
   are deterministic functions of the stream that any worker can rebuild.
2. **Pages load lazily, everywhere.** No component ever eagerly materializes
   a data directory. A cell opens with zero relation pages resident and
   fetches only what its queries touch; followers apply commits as metadata,
   not bytes; every cache layer is evictable at will because durability lives
   in the stream and object storage. Memory, IO, and cold-start time are
   proportional to the **working set a query actually touches — never to
   database size**. This is the entire footprint story (§6.5). The one
   deliberate exception is the checkpoint worker, which *streams* full state
   to object storage.
3. **Compute is disposable.** Cells start, serve, fail, and are recycled. No
   cell ever holds unique state a client depends on after commit ack.
4. **The stream is the coordinator.** Allocation grants, leases, checkpoints,
   era rotation, and notification fanout are ordered control frames in the
   same stream as the WAL, serialized by the same compare-and-append. There is
   no allocator service, no lock manager, no router state. The platform
   services preserve this: the gateway is **stateless by hard invariant**
   (§14.5) and the control plane is never in the commit path (§14.6). If a
   feature needs shared mutable state, it goes in the stream — or it does
   not exist.
5. **Leases are the steady state; optimism is the safety net.** A cell holding
   the head lease never loses a commit race and delivers vanilla Postgres
   semantics. Optimistic CAS commits are what make the lease safe to lose —
   failover, bursts, cross-sandbox writes — not the common case.
6. **Fail loud, never silent.** Every known gap in transparent conflict
   handling degrades to a retryable serialization failure — SQLSTATE `40001`,
   defined in §4.0 — never to silently wrong data. The four known
   silent-corruption classes (§12) gate every release.
7. **Graduate large databases.** This is a substrate for the long tail of
   AI-built apps. Databases that outgrow it migrate to hosted Postgres.

## 1. High-level architecture

The write model in one line: **any cell can write; the stream serializes;
committed history is one linear WAL.**

```text
                       ┌────────────────────────────────────┐
                       │ Object storage                     │
                       │  - manifest (per database)         │
                       │  - checkpoint objects (pages+aux)  │
                       │  - spilled large WAL slices        │
                       └───────────────▲────────────────────┘
                                       │ lazy hydration / checkpoint upload
                                       │
┌────────────────────┐        ┌────────┴─────────┐        ┌────────────────────┐
│ PGlite/WASM cell   │  CAS   │ Durable Stream   │  CAS   │ PGlite/WASM cell   │
│ (writer + reader)  │◄──────►│ per db timeline: │◄──────►│ (writer + reader)  │
│ local MVCC         │ append │ ONE serial order │ append │ local MVCC         │
│ dirty overlay VFS  │ + tail │ WAL + control    │ + tail │ dirty overlay VFS  │
└─────────▲──────────┘        └──────────────────┘        └──────────▲─────────┘
          │                                                          │
   any server, via ordinary load balancer; no routing state anywhere │
          └──────────────────────────────────────────────────────────┘
```

**There is no primary — both cells above are the same kind of thing.**
Every cell executes writes locally and speculatively against its view of the
head, then proposes them via compare-and-append; the stream admits exactly
one proposal at a time. Execution is concurrent, **commit is serialized**,
and a losing proposer rebases or re-executes (§3.3, §4) — never merges.
"Writer" and "follower" are momentary roles, not node types: a tailing cell
becomes a writer by appending, and the head lease (§3.2) is a contention
optimization layered on top, not an ownership mechanism.

Each database (branch/timeline) consists of:

- one **era chain** of Durable Streams carrying framed WAL and control records;
- a **manifest**: latest checkpoint ref, current era stream URL + start
  offset, era history, fork lineage, allocator high-water marks — held as
  rows in the control-plane Postgres (§14.6), cached by gateways;
- **checkpoint objects**: immutable snapshots of the full recovery state (§6.1);
- zero or more active **compute cells**, each an isolated WASM instance with a
  capability-scoped host API (§11).

Two platform services complete the picture: a thin, stateless
**storage/stream gateway** that fronts object storage and the stream service
and enforces tenant capabilities (§14.5), and a **control-plane Postgres** —
one boring real Postgres indexing the existence, lineage, eras, and auth of
every database on the platform (§14.6). Neither is ever in the commit path.

An idle database is manifest + stream + checkpoints. No compute. Activation is:
read manifest → open cell attached at head → serve. **No page bytes move at
boot** — pages materialize on first touch (§6.5), so activation cost and cell
memory scale with the queries actually run, not with database size.

## 2. The stream: framing and wire protocol

### 2.1 Why in-band framing is mandatory

The Durable Streams spec guarantees message-boundary preservation **only for
JSON mode**; byte streams are explicitly "framing left to clients", and the
spec reserves the right for servers to chunk reads at arbitrary byte positions
(PROTOCOL.md §5.6). Both shipped servers (TS reference, caddy-plugin) happen to
store appends as whole frames and mint offsets only at append boundaries — but
a catch-up GET returns **many appends concatenated with the server-side framing
stripped**, so a reader cannot recover per-append boundaries from the body.
Offsets are opaque resume cursors, never delimiters.

Therefore the stream payload is a self-delimiting frame protocol, in the style
of the Postgres wire protocol:

```text
frame := type(1 byte) | length(4 bytes, BE, payload length) | payload
```

One CAS append = one or more whole frames. Readers buffer and split; partial
frames at a read boundary are handled by ordinary buffering.

**Two positions, never conflated.** A **stream offset** is an opaque,
server-minted cursor: it advances on *every* append, control frames
included, and its format is implementation-specific — treat it strictly as
a token. A **WAL LSN** is a byte position in the physical timeline: it
advances only when `W`/`w` frames land. CAS guards and resume cursors use
offsets; page validation, checkpoints, forks, and GC horizons use LSNs.
Every commit therefore tracks a pair — attached at `(offset O, LSN B)` —
and no code may ever derive one from the other.

### 2.2 Frame types

```text
'W'  WAL slice        { commitId, baseLsn, endLsn, flags, walBytes }
'w'  WAL slice ref    { commitId, baseLsn, endLsn, objectRef, sha256, byteLength }
'G'  grant            { kind: sequence (others reserved — see §5.1), range, grantee }
'L'  lease            { kind: head|gc-pin|ddl, holder, epoch, ttl, pinnedLsn? }
'K'  checkpoint       { lsn, manifestRef, sha256 }
'N'  notify sidecar   { channel, payload, commitLsn }
'S'  seal / era-step  { nextEraUrl, finalLsn }
'F'  fork marker      { childDatabaseId, forkLsn }
'X'  schema epoch     { epoch, reason }   (optional; derivable from invals)
```

Rules:

- `W`/`w` frames carry contiguous physical WAL. Slices above a size threshold
  (default well under caddy's 64 MB per-message cap, which surfaces as an
  unmapped 500, not 413) spill to object storage and ride as `w` pointers.
- Control frames serialize with commits because they go through the same CAS
  append. Allocator and lease state is therefore a deterministic replay of the
  stream — a joiner reconstructs it from the tail it is already reading.
- `N` frames are appended atomically with their commit's `W` frame (same POST),
  giving cross-cell NOTIFY fanout in commit order (§10.2).
- `commitId` is a cell-generated UUID journaled locally **before** the
  append attempt, together with `{era URL, expected offset O, baseLsn,
  endLsn, slice hash}` (pending-commit journal, salvaged from the old
  branch). Producer headers alone cannot answer "did my commit land?" after
  a crash — the restarted incarnation no longer holds the byte-identical
  body a dedup retry would need. Recovery is exact and bounded, not a scan:
  (1) **bump the producer epoch** — this fences any in-flight zombie append
  from the dead incarnation, making the next step conclusive; (2) read the
  journaled era at offset O; (3) our `commitId` there ⇒ landed; anything
  else or nothing ⇒ lost (base-exactness means offset O could only ever
  hold our proposal or a rival's). If the journaled era has already been
  GC'd (recovery delayed past retention), the outcome is surfaced as
  **indeterminate** — the same honest state a vanilla client is in after
  losing its connection mid-`COMMIT`.

### 2.3 Compare-and-append

Two layers on every commit POST:

- **`Stream-Seq` = the tail offset the writer observed.** Both shipped servers
  scope `Stream-Seq` per stream and enforce strictly-increasing inside the
  per-stream append lock, so this is a working cooperative CAS today: the
  winner advances `lastSeq`; any loser that observed the same tail sends
  `seq <= lastSeq` and gets `409 Conflict`.
- **`Producer-Id`/`Producer-Epoch`/`Producer-Seq`** for retry dedup and zombie
  fencing. Producer validation runs first and dedups retries to 204 before the
  seq check — exactly the right composition for "did my commit land?" recovery
  after a crash between append and ack.

Caveats to engineer around:

- Cooperative, not enforced: the server checks monotonicity, not
  tail-equality. A strict `Stream-Expected-Offset: <offset>` extension
  (append iff tail == X, 409 carries `Stream-Next-Offset`) is a ~30–40 line
  change to the TS server (append path already holds the lock with the tail in
  hand) and fits the spec's additive-header extension rules. Do this early;
  it also removes the extra HEAD round-trip losers pay today (seq-conflict
  409s carry no next-offset header).
- Spec permits per-writer `Stream-Seq` scope; the production deployment must
  document per-stream scope before the commit protocol relies on it.
- Never set a TTL on database streams: sliding TTL deletes the stream on
  idleness and path re-creation restarts offsets at zero — a stale manifest
  offset then reads silent garbage. Delete explicitly via lifecycle.
- Tail with long-poll or catch-up reads, never SSE (SSE base64-encodes binary,
  +33%).

### 2.4 Eras: bounded streams, stepped by checkpoints

Era rotation is load-bearing, not hygiene: both shipped servers return the
**entire remainder of a stream in one response, buffered in server memory**.
Unbounded streams mean unbounded joins.

Rotation protocol:

1. A checkpoint completes at LSN `C` (§6.1) and its `K` frame is appended.
2. The rotating worker seals era `N`: `POST` with body = `S` frame
   (`nextEraUrl`, `finalLsn`), `Stream-Closed: true`, `Stream-Seq` guard, and
   producer headers. Verified semantics: append+close is atomic under the
   per-stream lock and closure is terminal — nothing can land after the seal.
   Two sharp edges: the close-*only* path (empty body) skips `Stream-Seq`
   validation entirely, so always seal **with** a body; and append+close is
   not itself tail-conditional, so the `Stream-Seq`/expected-offset guard on
   the sealing POST is what makes rotation race-safe.
3. Era `N+1` is a **fresh stream** (offsets restart; manifest carries the
   mapping), not a stream-fork: fork-chain reads recurse the whole chain per
   GET, so a long-lived database would pay per-era recursion forever. Fresh
   streams also release old eras for deletion once no fork or pin references
   them.
4. The manifest is CAS-updated to point at era `N+1` — a guarded
   transactional update in the control plane (§14.6).

**The central operational dial:** checkpoint cadence = era length = worst-case
join tail = worst-case rebase distance. One knob governs recovery time,
storage amplification, and commit-latency tails. Expose it per-database.

New joiners never read from the start: manifest → checkpoint (lazy) → tail
era from recorded offset. Joiners sanity-check the manifest offset against
`HEAD Stream-Next-Offset` (reads at/past the tail return 200-empty, not an
error, so a lost tail is otherwise undetectable).

### 2.5 Database forks

Forking a database at LSN `H` uses the native stream fork primitive
(`PUT` + `Stream-Forked-From` + `Stream-Fork-Offset`), which is implemented in
all stores with refcounting, soft-delete, and cascading GC:

- fork manifest references the parent checkpoint lineage and parent eras up to
  `H`; reads resolve through the layered lookup (fork-local first, then parent
  up to `H`);
- the fork gets its own era chain from `H`; producer and `Stream-Seq` state are
  not inherited (verified: forks are writer-state-fresh — writers re-bootstrap
  with an epoch bump);
- allocator high-water marks at `H` are recorded in the fork manifest; the
  fork's allocator namespace is independent thereafter (sibling forks may
  reuse future XIDs/sequence values — they are independent databases);
- GC is fork-aware: parent checkpoints, eras, and WAL ranges are pinned while
  any descendant references them (reverse references in the manifest).

Fork = one manifest write + one stream-fork PUT. No page copies. Long-lived
forks should compact onto their own checkpoint + fresh era to unpin parents.

### 2.6 Transport durability is a first-class dependency

A CAS ack is the commit point of every database on the platform, so the
stream deployment's durability class is part of this design's correctness
envelope, not an ops detail. Required of the production stream service:

- append acked ⇒ payload fsync-durable at the stated replication factor.
  The in-repo TS and caddy servers are **single-disk fsync — development
  only**; the production target's replication story must be stated, tested,
  and treated as a launch gate;
- per-stream append serialization with the `Stream-Seq`/expected-offset
  check inside the same critical section as the write (both shipped servers
  already do this);
- offsets stable across server restart and failover.

Required of the object store: immutable checkpoint objects with
read-after-write visibility — nothing more. The manifest, which needs an
atomic conditional update to make checkpoint publication and era rotation
race-safe (§6.1), lives in the control plane (§14.6) where that guard is an
ordinary transaction rather than an object-store conditional PUT.

Tenant data at rest — stream bytes and checkpoint objects — rides the
deployment's storage-layer encryption; per-tenant key scoping is a tier
feature (§11.3), not assumed by the base design.

## 3. Commit protocol

### 3.1 Cell lifecycle and the happy path

```text
1.  request → any server → start/reuse cell for (database, era)
2.  cell reads manifest and attaches at (stream offset O, WAL head B)
    through the host's page-version index (the tail is applied virtually,
    §6.2/§6.5) — no page bytes move at boot; pages materialize lazily on
    first touch; asserts config pins (§9)
3.  SQL executes locally under native MVCC against the physical image at B
4.  dirty pages go to a private overlay; WAL accumulates in the local
    pg_wal (visible to the VFS as ordinary file writes)
5.  at commit: slice WAL (B .. B'] — ALL bytes, unfiltered (§5.3 rule 4:
    aborted-transaction records must stay in)
6.  CAS append: W frame {commitId, baseLsn=B, endLsn=B'} (+ N frames),
    expected stream offset = O — the CAS token is the OFFSET, never an LSN
    (control frames advance offsets without advancing WAL, §2.1)
7.  win → new head (O', B'); ack client; overlay becomes clean local cache
8.  lose → §3.3 / §7
```

The client ack strictly follows CAS success. `synchronous_commit=off` is
accepted but inert: acking before the CAS resolves would convert a lost race
into acknowledged data loss (verified: the vanilla fast path acks before WAL
flush; the analogous shortcut here is unsound).

Read-only requests skip the CAS entirely — but note reads are not WAL-silent
in vanilla Postgres (§7.6): follower-mode cells suppress pruning/hint-FPI so
read traffic generates no slice.

### 3.2 The head lease: steady state

The **head lease** is claimed via an `L` frame (TTL + holder + epoch,
fenced by `Producer-Epoch`). The holder is the host's commit sequencer — a
lone cell is just the degenerate case (§14.4). While held and unexpired:

- the router-free load balancer still sends requests anywhere, but any cell
  can see from the tail who holds the lease and proxy writes to the holder
  (or the holder simply wins every CAS because nobody else is appending);
- the holder pipelines commits without contention — vanilla semantics,
  vanilla latency minus one stream round-trip per commit;
- interactive transactions on the holder never rebase;
- DDL transactions implicitly require the lease (§8.2).

Lease loss (crash, TTL expiry, network partition) needs no recovery protocol:
the next writer just CAS-appends. Stale-holder appends lose the CAS or are
fenced by epoch. The lease is a latency/contention optimization with zero
correctness weight — that is what the optimistic layer buys.

### 3.3 One-shot transactions on race loss: re-execute

For single-request transactions (autocommit statements, whole-transaction
batches — the dominant shape behind PostgREST-style APIs), nothing has been
acked when the CAS fails, so **any serial re-execution is valid**:

```text
lose CAS → discard overlay + speculative WAL → advance to new head K
        → re-execute the SQL → new slice (K .. K'] → CAS again
        → bounded retries, then 40001
```

No intent capture, no read-set machinery. Different `now()`/`random()`/serial
values on re-execution are fine — the client observed nothing. This is the
MVP conflict path and remains the fallback forever.

Two preconditions make "the client observed nothing" true, and both are
**M1 work, not polish**: the session proxy buffers all protocol output until
CAS resolution (§3.5), and the session must hold no unreplayable local
state. A session that has created temp tables (or other cell-local durable
state) cannot be transparently re-executed after a recycle — the recycle
destroys exactly the state re-execution would need, and worse, silently: the
re-run would read an empty temp table and commit wrong results. Such
sessions carry a **session-state taint** — and because the loss forces a
recycle that destroys exactly that state, a bare `40001` with the
connection kept open would leave the client silently continuing against a
session whose temp tables, held cursors, and advisory locks have vanished.
So tainted loss is a **fatal session reset**: an `ERROR` naming the cause,
then connection termination (vanilla precedent — crash recovery closes
connections; every driver and pool handles reconnect). Tainted sessions are
also excluded from advance-by-reattach (§15 M1): they pin at their base
until they commit or end. The in-place reset (M5) lifts all of this.

### 3.4 Reset-to-head

Discarding speculative state and re-aligning to head `K`:

- **MVP: cell recycle.** Drop the instance, re-open from checkpoint + tail —
  the identical code path as cold start, exercised constantly. Behind the
  connection-holding proxy (§3.5) this is invisible to clients. Acceptable at
  low contention, which cell affinity makes the norm.
- **Later: in-place reset.** Building blocks already in the fork:
  `PgliteDropRelationBuffersRange` / `PgliteFindAndDropRelationBuffersRange`
  for buffer discard, plus `__PGLITE__` hunks in `clog.c`, `subtrans.c`,
  `transam.c`, `multixact.c` where the counter-rewind audit starts. A correct
  in-place reset must also: rewind XID/OID/nextOid state to K's values, apply
  the winner tail's invalidation messages in order (or do a full relcache/
  syscache/plancache flush — the simple safe default), honor
  `relcacheInitFileInval`, and call `ResetSequenceCaches()` (§5.3, mandatory —
  relfilenumber-keyed invalidation does not catch replayed foreign sequence
  records).

### 3.5 The session proxy

A thin wire-protocol layer (evolved `pglite-socket`, which already tracks
per-connection transaction affinity) owns client TCP sessions and re-attaches
them to cells between transactions. It converts cell recycling — CAS-loss
reset, scale-to-zero wake, lease migration, DDL-barrier restarts — into
non-events. Session state replay (GUCs, prepared statements) is standard
pooler territory; temp table contents survive only in-place resets, not
recycles (documented). Connection topology behind the proxy is configurable —
one cell per connection (default) or many connections multiplexed onto one
cell (§14.4).

The proxy's second load-bearing job (M1, not polish): **response
buffering**. pglite-socket today forwards raw protocol bytes as PGlite emits
them; that is incompatible with transparent retry. For one-shot
transactions, every byte — `DataRow`s, `CommandComplete`,
`NotificationResponse`, errors — is held until the transaction's CAS
resolves; on loss, the buffer is discarded and the re-execution's output is
sent instead. End-of-transaction with an empty WAL slice ⇒ read-only ⇒
flush immediately, no CAS. Oversized results never break the invariant:
past the in-memory cap the buffer **spools to local disk**, and past the
spool cap the host **acquires the head lease before flushing** — a leased
commit cannot lose, so streaming becomes safe. Mutating output (`RETURNING`
rows, `CommandComplete`) is never sent ahead of an unresolved CAS, at any
size. Interactive transactions stream mid-transaction results by design;
only the `COMMIT` response is held (§4).

### 3.6 Commit-sequence placement audit

Vanilla `CommitTransaction` runs irreversible steps **before** the commit
record: deferred triggers fire → holdable portals materialize →
`ON COMMIT DELETE ROWS` truncates temp tables → LO cleanup → NOTIFY pre-commit
→ `RecordTransactionCommit` (xact.c:2275–2378). With a commit point that can
fail (CAS), each step needs an explicit before/after-CAS placement decision.
Worst instance (verified): the temp-table truncate is physical and pre-CAS —
a lost race would destroy the client's staging data in an aborted
transaction. It has no WAL footprint and moves after CAS success — **when
the commit gate ships (M5)**. Until then no reorder exists, and the hazard
is neutralized by the session-state taint instead: a temp-table session
that loses CAS gets the fatal session reset of §3.3, never a silent
continuation against truncated staging data. NOTIFY delivery already sits
correctly after commit.

### 3.7 The MVP SQL contract

Transparent conflict handling arrives in stages; what each stage *supports*
is stated, enforced, and tested — never implied. At M1–M4 (one-shot optimism
+ leases):

| Shape | On CAS loss |
| --- | --- |
| One-shot statements / whole-transaction batches, no session-local state | transparent re-execute (§3.3) |
| Sessions holding temp tables, `WITH HOLD` cursors, session advisory locks | fatal session reset — error + connection close (taint, §3.3) |
| Interactive transactions | `40001` at `COMMIT` (from M1) until rebase lands (M5) |
| DDL | never races — serialized via the head lease (§8.2) |
| Read-only (empty slice) | never conflicts; response flushes at txn end |

On leased cells — the steady state — none of these degradations trigger,
because losses require an actual cross-host race. The contract is enforced
mechanically (taint bits route the failure) and each row is pinned by a §16
contract test. One structural grace note: cells have no filesystem or
network access, so the vanilla hazard of "trigger with external side
effects ran before the commit failed" is impossible here by construction —
in-database side effects are the only kind, and those retry cleanly.

## 4. Interactive transactions: transparent rebase

### 4.0 The one failure mode: SQLSTATE `40001`

`40001` is PostgreSQL's standard SQLSTATE for **`serialization_failure`**
(SQL-standard class 40, "transaction rollback"). Vanilla Postgres raises it
under `SERIALIZABLE` and `REPEATABLE READ` when concurrent transactions
cannot be ordered consistently ("could not serialize access due to concurrent
update / due to read/write dependencies among transactions"). Its contract is
precise and well known to drivers, ORMs, and retry middleware:

- the transaction was rolled back cleanly — **nothing committed**, no partial
  effects;
- the failure is transient, not a bug in the SQL — **retrying the whole
  transaction is the correct and sufficient response**;
- it is the only error class for which blind client-side retry is safe by
  definition.

This design adopts `40001` as the **single client-visible failure mode for
every cross-cell conflict**: a lost commit race that cannot be transparently
rebased, a read-set validation failure, a schema-epoch fence trip, a tainted
transaction losing a race, a retry/rebase budget being exhausted. Surfaced as:

```text
ERROR:  could not serialize access due to concurrent update
SQLSTATE: 40001
DETAIL:  transaction conflicted with a concurrent commit on this database
HINT:  retry the transaction
```

We reuse the standard code rather than inventing one so that existing retry
behavior in the ecosystem (e.g. ORM serialization-retry policies) applies
unchanged. The caveat, examined in §4.6 and §12: apps written for READ
COMMITTED have never needed to handle `40001` and mostly don't — which is why
lease-holder affinity (§3.2) keeps it rare rather than merely documented.

Goal: clients never implement retry loops. Interactive transactions
(`BEGIN … reads … think … writes … COMMIT`) that lose a race are either
**transparently re-applied** or fail with `40001` — the same contract as
vanilla `SERIALIZABLE`, and rare in practice because leased cells never race.

This ships as a ladder: (v1) any race → `40001` (loud, trivial, correct);
(v2) rebase with validation, below. All mechanisms are source-verified; the
original naive formulations were adversarially refuted and the repaired rules
are what follows.

### 4.1 Read-set capture

- **One hook at `PinBufferForBlock`** (bufmgr.c:1210): sees every physical
  page read across all paths, including PG18 `read_stream`/AIO. (Hooking
  `ReadBufferExtended` misses streaming reads; harvesting SSI's predicate
  locks is unsound here — predicate.c exempts all catalogs, opts read-only
  transactions out entirely in single-user mode, collapses precision at
  default thresholds, and its designated-conflict-page tags are not
  physically-read pages for hash/GIN.)
- Record the page set only; no per-page LSN capture is needed (see 4.2).
- Additionally capture **nblocks at scan time** for every relation scanned
  without full index assistance (seqscan, BRIN, lossy bitmap): a seqscan's
  page set is frozen from `RelationGetNumberOfBlocks` — an smgr lseek, not a
  page read — so relation-extension inserts by a winner touch no page the
  loser read. Verified false negative without this.
- Exclusions: FSM forks (not WAL-logged), temp/local buffers (skip
  `BufferIsLocal`, as `MarkBufferDirtyHint` does), and **sequence pages**
  (§5.3 — every cross-cell nextval bumps them; including them guarantees
  spurious 40001 on any shared serial column).

### 4.2 Validation at rebase

After applying the winner tail `(B, K]` (B = the LSN the transaction executed
against):

```text
for every captured page:      pageLSN(K) <= B          else 40001
for every seqscanned rel:     nblocks(K) == nblocks(B) else 40001
for every IOS-skipped block:  VM bit still set at K    else 40001
missing / truncated page:                              40001
schema epoch unchanged (4.3):                          else 40001
```

Two verified rules embedded there:

- **Compare against B, not against per-page captured LSNs.** The loser's own
  speculative WAL shares the LSN space past B; pages it dirtied (its own
  writes, prune/hint side effects) carry captured LSNs above B that can mask
  winner modifications. `pageLSN(K) <= B` is the sound form.
- **Index-only scans need VM-bit *content* checks**, not LSNs:
  `visibilitymap_clear` dirties the VM page **without** `PageSetLSN`
  (visibilitymap.c — contrast the set path, which logs and stamps), and heap
  redo clears VM bits the same way. A winner DELETE seen only through a VM
  bit is LSN-invisible. Checking the bit is sufficient: any tuple change on a
  skipped block must clear it. (Alternative: `enable_indexonlyscan=off` in
  cells.)

Validation itself must not perturb the evidence: read LSNs via
`BufferGetLSNAtomic` on pinned buffers, never through executor access paths —
plain scans trigger opportunistic pruning and hint-bit setting, so a naive
validation walk would dirty the very pages it compares (and interleave stray
WAL into the fresh slice).

### 4.3 The catalog blind spot and the schema-epoch fence

Catalog state is consumed through syscache/relcache/plancache **without
touching bufmgr**, so page validation cannot see a winner's DDL — the loser
would re-apply mis-shaped tuples with zero signal. This is silent-corruption
class #1 and the fence is mandatory from the first rebase release:

- while applying `(B, K]`, collect invalidation messages from **all three
  carriers** (verified): commit records (`xl_xact_invals`), standalone
  `XLOG_INVALIDATIONS` (no-XID transactions), and `XLOG_HEAP_INPLACE`
  (vacuum's inplace pg_class updates carry invals too);
- intersect with the rebasing transaction's relation footprint (its opened
  relations / local lock list); any overlap → `40001`;
- after any reset, process invals like a hot standby (or flush caches
  wholesale) so cached plans revalidate before the next statement.

Invals are only generated at `wal_level >= replica` — one of the hard config
pins (§9). RLS policy changes are covered for free (policy DDL emits relcache
invals on the table; policy *data* reads are ordinary validated reads).

### 4.4 Logical re-apply

If validation passes, the transaction's effects are re-applied at K. Four
independent verified findings force the same conclusion: **the write intent is
logical tuples driven through normal heap/index/TOAST routines — never
verbatim WAL-slice or page-image replay**:

- the local slice contains tuples from subtransactions aborted by
  `ROLLBACK TO` (no undo in WAL) — harvest must net-effect filter against
  local clog/subtrans and collapse update chains;
- local XIDs alias the winner's committed XID space at K — replaying them
  corrupts visibility; re-apply restamps everything with **one fresh XID
  allocated at K**, carrying no subxact structure;
- TOAST pointers embed chunk OIDs uniqueness-checked at B that can collide at
  K — capture parent tuples **detoasted** and let `toast_insert_or_update`
  re-chunk at K;
- uniqueness must be re-checked against K — re-apply goes through real index
  AM insertion (`_bt_check_unique` etc.); a violation there is a cross-cell
  conflict surfacing late and maps to `40001` (not 23505, preserving the
  single-failure-mode contract).

Deferred constraints: the after-trigger queue addresses rows by ctid
(`ate_ctid1/2`) and refetches at fire time; re-apply emits an old→new ctid map
and rewrites the pending event list before commit-time recheck. Lock-only WAL
(`XLOG_HEAP_LOCK`) is dropped — locks are dead at commit and fresh WAL defines
the canonical bytes.

**Sequences under rebase — the reason RETURNING survives:** `nextval` is
*never re-called* during re-apply; drawn values are frozen in the captured
images. The lease (§5.3) guarantees no other cell used them. Client-observed
serial ids are stable by construction. (One-shot re-execution may change them;
nothing was observed.)

Volatile functions are the design's quiet strength over any logical-retry
scheme: `random()`, `gen_random_uuid()`, `now()` were evaluated once at B;
images are replayed, nothing re-evaluates, and validation certifies that
"executed at B" remains a legal serialization at K.

### 4.5 Taint bits: loud degradation for unrebaseable observations

Some observed values cannot survive re-placement at K, and no mechanism can
preserve them. Track cheap per-transaction taint; if tainted, a lost CAS
downgrades to `40001` instead of rebase:

- result sets projecting `ctid`, `xmin`, `cmin`, `cmax`;
- calls to `txid_current()` / `pg_current_xact_id()`.

Concrete casualty otherwise (verified): EF Core/Npgsql's default optimistic
concurrency token is `xmin` — post-rebase it silently mismatches and the app
sees phantom conflicts or lost updates. Agent-generated code copies this
pattern; the taint bit converts it to the documented failure mode.

### 4.6 Contract changes to document loudly

- **`SELECT FOR UPDATE` / `SKIP LOCKED` are not cross-cell mutexes.** Locks
  live in the local cell only. Two cells' `SKIP LOCKED` workers can claim the
  same job and the loser gets `40001` at commit where vanilla READ COMMITTED
  would block and `EvalPlanQual`-recheck. Mitigation is architectural: queue
  tables want lease-holder affinity. Documented, not fixable inside WAL.
- **Advisory locks have no cross-cell meaning** — no WAL artifact, nothing to
  validate; migration tools (Prisma, golang-migrate, Rails) use them as
  mutexes. Options: route `pg_advisory_*` to a stream-level lock service
  (control frames), or error in multi-cell mode. Never silently local.
- **Delivered cross-cell isolation is snapshot-at-B, commit-at-K** —
  REPEATABLE-READ-shaped. Page validation cannot see phantoms on never-read
  pages; do not claim serializability. Per-cell, semantics are vanilla.

### 4.7 Bounding the loop

Think-time transactions hold B for unbounded wall-clock; the tail `(B, K]`
grows with peer activity, and the loop (apply, validate, re-apply, CAS) can
lose again. Bound it: max attempts, max rebase distance (bytes of tail), max
commit latency — beyond any bound, `40001`. The head lease keeps the common
case at zero iterations.

## 5. Identity and allocators

### 5.1 XIDs and multixacts: chained state, no allocator

Earlier drafts granted per-cell XID ranges (and, after external review,
multixact id+offset ranges). **Neither allocator exists in this design** —
both counters are ordinary replayed state, made consistent by two rules
already enforced elsewhere:

1. **a slice lands only on its exact base** (offset CAS, §3.1), so every
   committed slice's xids and mxids chain from precisely the
   `nextXid`/`nextMulti` state its readers replay to;
2. **losers never publish** — they discard and re-execute (verbatim replay
   is banned, §4.4), so speculative identities never reach the stream; the
   M5 rebase restamps with fresh identities allocated at K.

Edge cases, checked against source:

- aborted transactions ride inside the winner's *contiguous* slice, and
  recovery advances `nextXid` past every xid it sees in records
  (`AdvanceNextFullTransactionIdPastXid`) — burn is consistent;
- a transaction that wrote only to temp tables still emits a commit/abort
  record (an assigned xid forces one via `markXidCommitted`), so no xid is
  ever consumed invisibly to the stream;
- multixact creation is WAL-logged before use (`XLOG_MULTIXACT_CREATE_ID`),
  so mxids and member offsets chain identically; `pg_multixact/` ships in
  checkpoints (§6.1) and replay extends it;
- wraparound: freezing is ordinary vacuum work (§6.4); `datfrozenxid`
  advancement flows through normal WAL.

The aliasing hazard the grants defended against exists only in the
verbatim-replay world this design bans. The one identity that *does* need
coordination is sequences — the only counter a client can observe
**without a commit** (§5.3). The convergence oracle (§16) pins this claim
mechanically: stream replay into vanilla Postgres must yield clean clog,
multixact, and `pg_amcheck` state under randomized, abort-heavy,
multi-cell workloads.

### 5.2 OIDs and relfilenumbers

Allocated by DDL and uniqueness-checked at execution time against local
state. DDL holds the head lease (§8.2), so checked-at-base equals
checked-at-head; one-shot DDL that loses a race re-executes and
re-allocates. TOAST chunk OIDs and other `GetNewOidWithIndex` allocations
self-heal by construction — the retry loop re-checks against whatever state
the cell executes on — and `nextOid` regression after a discarded slice is
harmless for the same reason it is after a vanilla crash: uniqueness is
enforced by index lookup, not by the counter. If rebaseable DDL is ever
wanted, `G` frames extend naturally; nothing needs them today.

### 5.3 Sequences

Verified mechanics that drive the design (sequence.c, sequence_xlog.c; fork
PG 18.3 byte-identical):

- `nextval` pre-logs **`SEQ_LOG_VALS`(=32) + CACHE values ahead** in one
  `XLOG_SEQ_LOG` record containing the whole sequence tuple with `last_value`
  pre-advanced; consuming prepaid values afterwards writes **no WAL** (on-page
  `log_cnt` decrement only);
- replay **rebuilds the entire page from the record** (`REGBUF_WILL_INIT`,
  fresh `PageInit`, memcpy) — strict last-write-wins in log order, no
  max-merge;
- the record is **non-transactional**: the xid on it exists only to force
  commit-time WAL flush; redo never consults commit status, and aborted
  transactions' advancements persist (vanilla "nextval is never rolled back");
- the backend-local `SeqTable` cache is invalidated **only** on relfilenumber
  change; a page-LSN-after-checkpoint guard forces a fresh record on first
  nextval after each checkpoint.

**Design: per-cell leased ranges, granted as setval-shaped `G` frames through
the CAS.** Adversarially verified to hold, conditional on all of:

1. **Clamp in `nextval_internal`**: patch `maxv` to the lease end. Allocation
   authority is the lease, never the page. The existing maxvalue logic then
   also caps the 32-ahead pre-log at the lease boundary (more frequent records
   near lease end — fine).
2. **The page is not the cursor.** After any reset-to-head the page holds
   whichever record replayed last — typically a neighbour cell's value, and
   (LWW quirk) possibly *below* the newest grant if an older slice landed
   later. The cell re-asserts its local cursor from lease state after every
   reset, and the rebase merge rule for sequence pages writes
   `max(stream watermark, own cursor)`, never the raw local value.
3. **`ResetSequenceCaches()` on every reset-to-head** — otherwise the fast
   path silently serves values from the discarded speculative timeline
   (relfilenumber-keyed invalidation cannot catch replayed foreign records).
   Note the currval seam: this wipes `last_used_seq`, so rebase bookkeeping
   must repopulate currval state or a post-rebase `currval()` errors.
4. **Slices carry sequence records unconditionally** — filtering to committed
   work drops non-transactional advancement and the same cell re-issues values
   its client already observed.
5. **User `setval()`/`ALTER SEQUENCE RESTART` escape leases** (bounds-checked
   only against catalog min/max): intercept as lease-epoch bumps through the
   stream (all outstanding leases invalidated) or forbid mid-lease. `ALTER
   SEQUENCE` rewrites the relfilenumber and is transactional — route it
   through the DDL/lease path.
6. **Sequence pages are excluded from read-set validation** (§4.1) — nextval
   is a fetch-and-add, not a read.
7. **Grants are per cell incarnation and burn on any reset.** The forcing
   case is abort-only consumption: `BEGIN; SELECT nextval(...); ROLLBACK;`
   exposes a value to the client with no commit and therefore no CAS append
   — nothing durable records consumption. The rule that keeps observed
   values safe without durable consumption tracking: a grant is bound to the
   cell's producer epoch; on recycle, reset, crash, or hibernation the
   incarnation's remaining range is **burned**, and a new incarnation must
   take a fresh grant — it may never resume a predecessor's range. Observed
   ⇒ inside a range no other incarnation will ever draw from ⇒ re-issue is
   impossible. That is the vanilla crash guarantee (jump forward, gaps
   allowed), achieved by disjointness instead of by logging consumption.
   Keep ranges small (64–256) so burn under hibernation churn stays cheap;
   values consumed by aborted transactions in a *surviving* cell are
   published by rule 4 the next time that cell commits.

**Leases are mandatory for Postgres sequence semantics — not noise
reduction.** Without them, duplicate draws surface as unique violations
*only where a unique index happens to exist*: a plain bigserial-shaped
column silently receives duplicated "unique" values, `SELECT
last_value`/`pg_sequences` visibility diverges from vanilla after
abort-only draws, and the never-reissue guarantee breaks at the first
recycle. Rollout matches topology: at M1 the host cell manager coordinates
a per-database sequence cursor across its sibling cells host-locally (no
stream frames needed — this closes the abort-only duplicate two same-host
connections could otherwise observe); M4's cross-host lease frames extend
the same discipline to the fleet; M5's RETURNING stability under rebase
(§4.4) depends on them outright. `uuidv7` guidance for generated schemas
stands — as advice, not as the correctness story.

## 6. Storage: checkpoints, lazy followers, GC

### 6.1 Checkpoint objects

A checkpoint is the complete recovery state at a REDO point — verified list,
all mandatory:

```text
relation page files (all forks except FSM optional-rebuildable)
pg_xact/           (clog — REQUIRED; tail replay only extends it)
pg_multixact/
pg_commit_ts/      (if enabled)
pg_filenode.map    (global + per-database relmapper files)
pg_twophase/
unlogged-relation init forks
pg_control         (coherent CheckPoint struct: nextXid, nextOid,
                    nextMulti/Offset, oldest* horizons, TLI, fullPageWrites)
```

The checkpoint record itself must be **real, CRC-valid bytes in the stream**
with a correct back-pointer and a reachable REDO record — recovery validates
all of it; it cannot be fabricated at an arbitrary LSN. MVP protocol: the
host sequencer quiesces the database (it holds appends — with multiple
cells per host there is no single "producing cell"), one cell runs a
genuine shutdown-style checkpoint, the worker snapshots the datadir into
the object, appends the `K` frame. Fuzzy
(non-quiesced) checkpoints come later and must drive the backup-recovery path
(`backupStartPoint`/`minRecoveryPoint`) so consistency is not declared before
the snapshot end.

Because checkpoints are deterministic functions of the stream, **any worker
can build one, idempotently**; racing workers are resolved by the manifest
CAS. There is no nominated physicaliser — this is the core operational
advantage of physical over logical replication for this system.

The fork already compiles out automatic XLOG-consumption checkpoints under
`__PGLITE__` — consistent with externally-orchestrated checkpointing.

Checkpoint publication and era rotation follow a fixed state machine — this
is the normative order (§2.4 is the narrative view); every step is
idempotent and any worker can resume after a crash at any point:

```text
1. quiesce + local checkpoint       redo: rerun — nothing published yet
2. upload checkpoint objects        redo: content-addressed, re-put is safe
3. create era N+1 stream (PUT)      redo: idempotent PUT (200 on match);
                                    created before the seal so the pointer
                                    target always exists
4. append K frame to era N          redo: producer dedup, or tail scan
5. seal era N (S frame + close,     redo: guarded seal — losing the seal
   Stream-Seq guard, with body)     race means another worker rotated;
                                    abandon this attempt
6. CAS manifest → {checkpoint,      redo: conditional update; loser
   era N+1}                         re-reads and reconciles
```

Joiner tolerance rules make partial progress harmless: a `K` frame with no
seal ⇒ keep tailing era N; an era N+1 that exists but is unreferenced ⇒
ignore it until the manifest or an `S` frame says otherwise; a sealed era ⇒
follow the `S` pointer even if the manifest lags behind. The manifest in
step 6 is control-plane rows (§14.6); the guard is `UPDATE … WHERE era = N`.

### 6.2 The follower invariant (corrected)

The naive claim "every touched page arrives first as an FPI in the tail, so
followers never need base pages" is **false** — refuted with in-tree
counterexamples that occur on every vacuum: `XLOG_HEAP2_VISIBLE` registers the
heap buffer `REGBUF_NO_IMAGE` (no checksums ⇒ no FPI ever, and redo mutates
the page without stamping its LSN); prune/freeze VM-only updates do the same;
hash-index NO_CHANGE registrations require pre-existing pages; and
`XLOG_SMGR_TRUNCATE` redo mutates FSM/VM boundary pages that have **no
per-page records at all**.

The invariant that survives verification, and that the design builds on:

> **Checkpoint-at-REDO is a mandatory lazy page source. The tail never needs
> a page version *newer* than REDO.** (The FPI-on-first-touch guarantee is
> real and race-free for normally-registered buffers.)

Follower model, per block:

```text
page-version index:  block → (checkpoint base ref, [records since REDO])
materialize on read: fetch base (skippable iff first record carries an FPI
                     or is REGBUF_WILL_INIT), apply records in order
NO_IMAGE records:    flagged base-required
```

Tail apply is therefore still **O(metadata) lazy** for page state — index
updates plus cache eviction — with base fetches deferred to first read. This
is exactly the shape of the existing `pglite-durable-vfs` tailer + page-version
index, generalized from full page images to (base, deltas).

### 6.3 Eager processing: what lazy apply must not defer

Enumerated (verified) record set a tail applier handles specially, eagerly:

- **Commit/abort records**: clog bits (side effect with no dedicated record —
  skip this and every tuple looks in-progress forever), `nextXid`,
  relfilelocator drops (`DropRelationFiles`), and **invalidation messages** —
  from all three carriers (§4.3) — applied to live local backends exactly as
  hot standby does (`ProcessCommittedInvalidationMessages`). This is what
  makes **DDL work on live followers without restart** (§8.1).
- **File/metadata effects** to the VFS layer: SMGR create/truncate (including
  synthesizing the un-logged FSM/VM boundary mutations), RELMAP updates,
  DBASE create/drop, TBLSPC, SLRU zero/truncate pages.
- **XLOG-rmgr control**: NEXTOID, checkpoints, `PARAMETER_CHANGE`,
  `FPW_CHANGE`, standalone FPIs.
- Buffer invalidation for pages the running follower has in shared_buffers —
  the fork's `PgliteDropRelationBuffersRange` hook, driven by the block refs
  in applied records.

### 6.4 Vacuum and GC: no global xmin

Vacuum is an ordinary transaction on an ordinary cell (janitor role or the
lease holder): its prune/freeze WAL flows through the CAS like any commit.
The classic distributed hazard — vacuum removing tuples a remote snapshot can
still see — does not exist: a reader pinned at LSN `H` reads **page versions
≤ H** from the timeline; storage versioning protects old snapshots, not
vacuum horizons.

The constraint reappears in tractable form as the **storage GC horizon**:

```text
gc_horizon = min(pinned LSNs advertised by live cell leases, retention floor)
```

Checkpoints, eras, and page versions at or above the horizon are kept; a cell
that outlives its lease TTL and reads past the horizon gets a clean error and
re-pins at a newer LSN. Fork refcounts pin parent history independently
(§2.5). No coordination beyond lease frames the stream already carries.

### 6.5 The lazy read path, end to end

The system-wide invariant (design principle 2), stated once, precisely:

> **Bytes move only when (a) a query touches a page, (b) a commit publishes a
> slice, or (c) a checkpoint worker streams state to object storage.** Nothing
> else — not cold start, not tail apply, not fork creation, not era rotation —
> transfers or materializes page content.

A page read resolves through this chain, returning at the first hit:

```text
Postgres shared_buffers        in-WASM; deliberately small (§14.3)
  → cell dirty overlay         private, uncommitted local writes
  → host materialized cache    (database, block, LSN) — shared by all of the
                               database's cells and forks (§14.3)
  → host base cache            content-addressed, immutable — shared
                               fleet-wide across tenants (§14.3)
  → object storage + stream    fetch checkpoint base, apply records since
                               REDO (single-page redo, §14.2), then populate
                               every layer above
```

The synchronous-VFS contract holds because a cache miss parks the compute
worker on the SAB/Atomics bridge while the fetch worker fills WASM memory
directly — the machinery already built in `pglite-durable-vfs`.

What each lifecycle event actually transfers:

- **cold start / wake**: manifest + `pg_control`-scale metadata — kilobytes.
  The cell attaches at head via the page-version index (a synthesized
  clean-at-`H` control view, with `nextXid`/`nextOid` and clog state coming
  from the tailer's eager commit-record processing, §6.3); it does not run
  eager redo. First-query latency pays only for that query's working set;
- **tail apply (followers and idle siblings)**: index updates, clog bits,
  invalidation messages — metadata and SLRU bits, kilobytes, never pages;
- **fork**: one manifest write + one stream-fork PUT — zero page bytes;
- **eviction**: any layer, any time, zero correctness impact — everything
  below durability is cache. Host memory pressure is answered by dropping
  pages, worst case re-fetching them.

This is what the ~80 MB active-cell figure assumes and what makes
scale-to-zero economics real: a cell's footprint is its working set, a cold
database's footprint is zero, and a host's footprint is one shared cache
rather than N private copies. Laziness is a **tested invariant, not an
optimization** — the byte-count regression suite (§16) asserts it stays true.

## 7. Follower and freshness model

- Freshness is **product-visible API, not an implementation detail**. Modes:
  - `linearizable` — confirm the true head with a stream `HEAD` round-trip
    and catch up past it before serving. The only mode guaranteed to see a
    commit acknowledged via *another host* an instant ago;
  - `session` (default) — read-your-writes via commit-LSN tokens
    (`waitForLsn`); a cell advances between transactions;
  - `local` — serve at the host tailer's head, zero round-trips; may lag
    other hosts' acked commits by the tailing latency (an earlier draft
    over-claimed this as "fresh" — it is not linearizable);
  - `bounded-stale(Δ)` and `pinned(LSN)` — explicit staleness; forks,
    tests, time travel.
- The tailer never advances the visible LSN under an open snapshot; the
  query/apply gate from `pglite-durable-vfs` carries over unchanged.
- `waitForLsn` gives read-your-writes across cells; the commit response
  carries the commit LSN as a session token.
- **Reads generate WAL in vanilla Postgres** — opportunistic pruning fires
  from scan paths, hint-bit FPIs when checksums/`wal_log_hints` are on,
  `XLOG_HEAP_LOCK` from lock-then-abort. Untreated this (a) gives "read-only"
  cells nonempty slices and (b) makes hot pages fail rebase validation
  constantly. Cells in follower/read mode therefore run with pruning and
  hint-FPI suppressed (hot standby precedent: recovery never prunes), and
  checksums/`wal_log_hints` stay off (§9). A tuple-level revalidation slow
  path behind the page-LSN fast path is the pressure-relief valve if
  false-conflict rates still bite.

## 8. DDL

### 8.1 On followers: just works

Hot standby is the existence proof, and the mechanism ports directly: catalog
changes arrive as ordinary heap WAL; invalidation messages arrive on the three
carriers and are applied to live backends; relmapper/smgr/database effects are
the special records of §6.3. Two hard rules (verified):

- force `CREATE DATABASE ... STRATEGY=WAL_LOG` — the FILE_COPY strategy
  replays by *copying local directories*, which do not exist in an
  object-storage world;
- unlogged tables are forbidden (or documented as per-cell ephemera): their
  main forks are never in WAL, are reset on every recovery, and every cell
  recycle is a recovery.

No replica restarts for DDL. The restart fallback from the earlier design
remains only as a belt-and-braces path for unrecognized records.

### 8.2 On writers: DDL takes the head lease

One-shot DDL through the optimistic path is sound (losers re-execute). But
DDL inside *rebaseable* interactive transactions hits verified walls:
checked-at-B allocators (OIDs, relfilenumbers, TOAST value OIDs) collide at K,
and `CREATE INDEX` et al. perform `heap_inplace_update` — non-transactional
catalog mutations that cannot be modeled as re-appliable images.

Design call: **DDL transactions implicitly acquire the head lease.**
Pessimistic for DDL, optimistic for DML. Consequences:

- DDL "just works" with no user-visible restrictions — it briefly serializes,
  which is what migrations need anyway (an `ALTER TABLE` rewrite touches every
  page and would be maximally abort-prone under optimism);
- checked-at-execution allocation is checked-at-head — no OID leasing needed;
- rebasing DML that crosses a committed DDL is fenced by the schema epoch →
  `40001` — vanilla-adjacent, since that DDL would have lock-blocked the DML;
- `CREATE TEMP TABLE` allocates catalog OIDs too — it rides the same rule or
  a small OID range grant if lease acquisition for temp DDL proves annoying.

Schema epochs are derivable (any DDL-bearing commit bumps the epoch; `X`
frames are an optional convenience for joiners).

## 9. Configuration pins and feature policy

Asserted at cell startup, non-negotiable for correctness:

```text
wal_level = replica          minimal legally skips WAL for bulk-loaded
                             relations (storage.c WAL-skip paths) AND emits no
                             invalidation/standby records — silent data loss
                             and a blind catalog fence. Fork default is
                             already replica; add the assert.
full_page_writes = on        the §6.2 invariant depends on it
data_checksums = off         kills XLOG_FPI_FOR_HINT read-noise; page
wal_log_hints = off          integrity comes from object hashes; accept
                             byte-level (not logical) replica divergence
synchronous ack semantics    COMMIT ack strictly after CAS win; the
                             synchronous_commit GUC is accepted but inert
stream TTL: none             explicit lifecycle deletion only
CREATE DATABASE: WAL_LOG     FILE_COPY replays via local copydir
```

Feature policy (enforced, each with its loud failure mode):

| Feature | Policy |
| --- | --- |
| Unlogged tables | forbid in multi-cell mode (or document as per-cell ephemera) |
| Advisory locks | stream lock service, or error in multi-cell mode |
| `SKIP LOCKED`/`FOR UPDATE` cross-cell | works, but contention → `40001`; route queues to lease holder |
| ctid/xmin/txid observation | rebase-taint → `40001` on race |
| `setval` on leased sequences | lease-epoch bump or error |
| Prepared transactions (2PC) | out of scope initially |
| SERIALIZABLE | per-cell yes; cross-cell not claimed |
| Native extensions | trusted build set only (per WASM isolation tiers) |

## 10. Cross-cell services on the stream

### 10.1 Coordination frames

Everything that would traditionally need a coordinator is an ordered control
frame through the same CAS (§2.2): sequence grants (the only granted
identity — xids, mxids, and OIDs are chained state needing none, §5.1–§5.2),
head/GC/DDL leases, checkpoint markers, era steps, fork markers. State is
reconstructed by replaying the tail — the joiner already reads it.

### 10.2 Cluster-wide LISTEN/NOTIFY — headline capability

**What stock Postgres cannot do, in any configuration:** `LISTEN`/`NOTIFY`
is unavailable on hot standbys (notifications are not WAL-logged and are
never delivered to physical replicas), and logical replication does not carry
notifications either. A client listening on a vanilla read replica hears
nothing, ever. Here, cross-cell NOTIFY falls out of the architecture nearly
for free — and becomes a differentiating feature rather than a limitation.

**Mechanics.** Vanilla notification semantics are already rebase-correct on
the committing cell (verified): payloads are queued transaction-locally,
staged at `PreCommit_Notify`, delivered only after commit, discarded on
abort — and payload contents computed from reads are covered by read-set
validation. The one missing piece is distribution, because the notify queue
is explicitly non-WAL. The fix:

- at commit time, the cell harvests its pending notification list and encodes
  it as `N` frames **in the same CAS POST as the commit's `W` frame** —
  notifications exist in the stream iff the commit does, atomically;
- because `N` frames are constructed at the final (winning) CAS attempt, a
  rebased transaction emits its notifications exactly once — lost attempts
  never reach the stream;
- the committing cell delivers to its local listeners post-commit exactly as
  vanilla does; every follower's tailer delivers `N` frames to its local
  listeners (and the session proxy to its attached clients) **in stream
  order, which is commit order, globally**;
- delivery semantics for live listeners match vanilla (fire-and-forget to
  currently-connected sessions). Because notifications are durable frames
  with offsets, offset-replay for reconnecting listeners is available as an
  extension vanilla could never offer (open question #6: replay window).

**Why it matters for the product.** `NOTIFY` from triggers is the classic
zero-infrastructure change feed (`pg_notify('orders', row_to_json(NEW))`),
and it is exactly the pattern Supabase Realtime is built around. In this
system that pattern works across every replica and every sandbox with no
extra moving parts — the WAL stream is already the message bus.

**The demo.**

```text
psql A ──► cell 1 (writer):    INSERT INTO orders ... ;  -- trigger pg_notify
psql B ──► cell 2 (follower):  LISTEN orders;   -- receives, in commit order
psql C ──► cell 3 (follower):  LISTEN orders;   -- receives the same
GUI    ──► tails the stream directly: live activity feed for the database,
           no connection to any cell at all
```

Write anywhere, hear it everywhere, ordered — on a database that was cold
storage thirty seconds ago and costs nothing when idle. This should be shown
side by side with vanilla Postgres, where the same `LISTEN` on a standby is
simply rejected.

## 11. Isolation and runtime

The runtime is **Node/V8**: PGlite is an Emscripten build whose JS glue —
including this design's entire VFS layer — is load-bearing, and V8 shares a
compiled `WebAssembly.Module` across instances natively. The isolation story
is built on one observation that changes the threat model:

> **Tenants never supply JavaScript — only SQL and data.** All JS in the
> process is ours. An attacker's starting position is *inside* the WASM
> sandbox, having first exploited a Postgres memory-safety bug via SQL; from
> there they control only their own linear memory and whatever the import
> object exposes.

### 11.1 The import object is the security boundary

Each cell is instantiated with an explicitly constructed, **deny-by-default
import object** — the capability bundle realized in Emscripten terms:

- our `BaseFilesystem` VFS is the filesystem surface; stock Emscripten
  NODEFS/NODERAWFS (ambient filesystem access) is never linked;
- no exec/environ-shaped imports (single-process build needs none);
- the host API (page read, tail read, CAS append, grant requests,
  time/random, metrics) is injected per-tenant and can construct no storage
  paths, stream URLs, or credentials;
- extensions are WASM side modules from the trusted build set only — no
  tenant-supplied native or JS code, ever.

Fuzzing effort concentrates here: hostile arguments (paths, fds, lengths,
pointers) from a hypothetically compromised instance against the import
surface. WASM linear memory is bounds-checked per instance, so a Postgres RCE
corrupts at worst the tenant's own database image unless the import surface
also fails.

### 11.2 Cell = worker thread; process = blast radius

**Per cell (`worker_threads`, one worker per active cell):** hard WASM memory
cap via `WebAssembly.Memory` `maximum` (growth failure surfaces as ordinary
Postgres OOM); JS-heap caps via worker `resourceLimits` (breach kills the
worker, not the process); CPU fairness from the OS scheduler (workers are
real threads — a spinning loop burns one core, not the fleet); runaway
termination via `statement_timeout` first and a watchdog +
`worker.terminate()` second (V8 interrupts WASM loops); traps/aborts are
contained to the worker. Termination is cheap **by architecture**: cells are
disposable and recovery is cold-start-from-stream behind the session proxy —
the reset-to-head machinery doubles as security teardown.

**Per process (pools of M workers):** the process is the unit of damage.
cgroups (`cpu.max`, `memory.max`), Node permission model with fs scoped to
the process's cache dirs, `--disallow-code-generation-from-strings`,
inspector off. Two gaps Node does not close, and their mitigations:

- **no network egress permission** — enforce at the infra layer (cell hosts
  reach only stream/storage endpoints) and, more importantly, scope
  credentials: a process holds short-lived tokens (gateway-minted, §14.5)
  for exactly its currently assigned tenants, so full process compromise
  leaks co-residents only, never the fleet;
- **no per-worker syscall filter** (seccomp is process-granular) — same
  answer: partition by process, keep M modest.

**Named residual risks:** V8 WASM-engine escapes (a real, regularly patched
CVE category — patch cadence + process recycling) and Spectre-class side
channels between co-resident tenants (needs an in-WASM gadget plus timers —
note the SAB/Atomics build hands WASM a usable timer, so hostile and
sensitive tenants should not share a process). Both are what the tier ladder
is for.

### 11.3 Tier ladder (same code at every rung)

```text
default    worker-per-cell in pooled Node processes (free-tier economics)
dedicated  one tenant per Node process: own cgroups, permissions, creds
microVM    the same Node cell host inside Firecracker (regulated/enterprise)
future     WASI/Wasmtime tier: fuel preemption, pooling allocator, .cwasm
           cross-process module cache, no JS in the TCB. Blockers today: the
           Emscripten glue is load-bearing (the VFS is TypeScript) and
           extension side-module loading. Node now; WASI as escalation, not
           prerequisite. (Seed exists: the pglite-wasi POC.)
```

### 11.4 Sharing the compiled module

In-process: compile once, `postMessage` the `Module` to workers — V8 shares
the underlying code; each worker gets its own `Memory` and its own import
object. Across processes: `WebAssembly.Module` does not serialize in Node;
processes are long-lived so per-process compilation (Liftoff baseline,
background tier-up) amortizes to noise — and if it ever matters, that is
another point for the Wasmtime tier. Density note: per-cell memory dominates
packing (~80 MB active); the SAB shared-memory artifact's fixed 256 MB heap
is right for the fetch bridge, wrong for fleet density — keep growable-with-
maximum as the fleet build and fixed-SAB as the bridge build.

## 12. Risk register

**Silent-corruption classes — gate every release; convert to loud before
optimizing anything:**

1. Catalog blind spot: rebase without the schema-epoch fence re-applies
   mis-shaped tuples with no error (§4.3).
2. Verbatim slice replay: XID aliasing against the winner's clog +
   aborted-subxact tuples + TOAST chunk collisions (§4.4). The doc bans the
   words "replay the captured slice at K" — re-apply is logical, always.
3. Deferred-trigger recheck against stale ctids after re-apply (§4.4).
4. Cross-transaction ctid/xmin reuse by clients (§4.5) — taint bit required.

**Contract risks:** commit-time `40001` in READ COMMITTED apps (mitigated by
lease affinity — steady state is single-writer); `SKIP LOCKED` queues;
advisory-lock migrations; xmin-token ORMs.

**Performance risks:** false-conflict storms from read-generated WAL (§7);
interactive rebase distance under peer load (§4.7 bounds); one CAS round-trip
of write latency by construction (accepted); full-tail reads bounded only by
era rotation (§2.4).

**Ecosystem risks:** physical format ties a timeline to a PG major —
upgrades are materialize + logical dump/restore (also the graduation path);
production Durable Streams durability (repo servers are single-disk fsync;
the deployment target's replication story must be verified).

**Architectural drift risks:** statefulness creep in the gateway. The §14.5
invariant exists to be cited in review: any proposed feature needing state
two gateway instances must agree on goes in the stream, the control plane,
or nowhere. The gateway touches everything (auth, storage, streams,
manifest cache), which makes it the natural landing spot for exactly the
coordination state this design exists to eliminate.

## 13. Relationship to existing code

| Existing (`packages/pglite-durable-vfs`, fork) | Role here |
| --- | --- |
| `TrackingNodeFS` + dirty tracker | dirty overlay + slice/overlay bookkeeping |
| `LazyPrimaryFS` / `LazyReplicaFS` + page cache | cell VFS: lazy checkpoint hydration, overlay |
| Page-image commit manifests | interim stream payload option; superseded by WAL slices (a page-image commit ≡ all-FPI WAL) |
| Pageserver (objects, atomic promotion) | checkpoint object store + materializer |
| Tailer + apply journal + page-version index | follower applier (§6.2–6.3), extended from images to (base, deltas) + special records |
| Native buffer invalidation (`PgliteDropRelationBuffersRange`) | live-follower invalidation + reset-to-head |
| Native WAL LSN exports | slice boundary capture |
| SAB fetch bridge, shared-memory build | remote page faults for cells |
| `pglite-socket` | session proxy seed (§3.5); its `QueryQueueManager` is the multiplexer mode (§14.4) |
| `__PGLITE__` hunks in clog/subtrans/transam/multixact | in-place reset counter-rewind audit anchors |
| Durable Streams client/server | transport; plus the ~30-line `Stream-Expected-Offset` extension |

### 13.1 Salvage plan from `codex/durable-vfs-plan`

The prior implementation lives on two pushed reference branches:
`codex/durable-vfs-plan` on `electric-sql/pglite` (the
`packages/pglite-durable-vfs` package plus core-PGlite changes, 26 commits)
and `codex/durable-vfs-postgres` on `electric-sql/postgres-pglite` (native
hooks and the shared-memory build, REL_18_3+23). Both stay frozen as
reference and as the working single-writer page-image demo. This project
builds on a fresh branch from `main` and harvests from them deliberately.

Note on mechanics: the old branch's history is 26 interleaved "phase"
commits — the reusable pieces are not isolated commits, so **extraction is
copy-based, not cherry-pick-based**. Record provenance in commit messages
(`extracted from codex/durable-vfs-plan @ <sha>`).

**Tier 1 — lift early, as standalone PRs (independently useful, shrink the
eventual review diff):**

- the `FilesystemQueryHooks` / `aroundQuery` mechanism in core PGlite
  (`packages/pglite/src/base.ts`, `src/fs/base.ts`), together with
  `tests/filesystem-query-hooks.test.ts`;
- the shared-memory WASM build variant and the SAB/`Atomics` bridge
  (`pglite-shared` artifacts, control block, fetch worker) — needed by the
  lazy read path (§6.5) and later the commit gate (§14.2);
- the memory benchmark scripts (`scripts/pglite-memory-explore.ts`,
  `worker-replica-memory.ts`) — they back the footprint claims (§6.5, §16).

**Tier 2 — copy and reshape into the new packages:**

- `LazyPrimaryFS` / `LazyReplicaFS`, the page cache, and the sync/SAB page
  resolvers → the cell VFS (§6.5 read chain);
- dirty tracker + path classifier → overlay bookkeeping and slice-boundary
  classification;
- tailer, apply journal, and query gate → the follower applier's *shape*
  survives (journal idempotency, apply/query gating, offset resume);
  internals change from manifest events to frames + WAL records (§2, §6.2);
- the pageserver's Hono app, disk store, and object store → the serving
  skeleton of the storage/stream gateway (§14.5) — only its commit-promotion
  protocol is superseded;
- the native fork branch's exports — WAL-LSN getters,
  `PgliteDropRelationBuffersRange` / `FindAndDrop…Range`, the `__PGLITE__`
  hunks in clog/subtrans/transam/multixact — are re-landed on a fresh fork
  branch as new-files-plus-tiny-hunks under the §14.1 patch budget (the old
  fork branch predates that discipline);
- **port the tests' invariants even where implementations are rewritten** —
  the crash-window, idempotent-replay, and dirty-generation tests encode
  hard-won thinking that transfers directly to §16's suites.

**Tier 3 — leave behind (superseded by stream-as-truth):**

- the pageserver's atomic commit-promotion protocol, staging dirs, and JSONL
  page/file indexes (checkpoint objects + the stream replace them, §6.1);
- `CommitManifest` / commit-event types and the manifest-driven tailer;
- the VFS-managed timeline LSN (real WAL LSNs replace it);
- path-derived `InvalidationEntry` mapping (WAL block refs and commit-record
  inval payloads replace it, §6.3).

One caution from the earlier misadventure: the extension repos inside the
fork carry local-only `codex/pglite-extension-build-state` branches holding
generated build artifacts (`.defs.txt`/`.undef.txt`/`.so`). Those are not
salvage — never pin submodules to generated-artifact commits; regenerate at
build time (and consider `ignore = dirty` in the fork's `.gitmodules`).

## 14. Implementation strategy

### 14.1 The patch budget

Fork maintenance cost is measured in **hunks touching existing Postgres
files** — that is what must be re-reasoned on every major-version rebase.
Track the count in CI and keep it small. The interception ladder, preferred
first:

1. **JS host layer** — orchestration, IO, protocol. The VFS already lives
   here; so do frames, CAS, leases, tailing, checkpointing, the proxy.
2. **The PGlite libc layer** — syscall-shaped interception (file IO, fsync
   boundaries, clock/random) without touching Postgres source at all.
3. **New C files linked into the build** — mechanisms that need Postgres
   internal APIs or hot-loop performance, exported to JS where useful.
4. **Hunks in existing Postgres files** — last resort: 1–5 line call-site
   hooks, `__PGLITE__`-guarded, vanilla behavior when unconfigured.

The fork already follows this shape (`PgliteDropRelationBuffersRange` and
friends as added functions; small `__PGLITE__` hunks in
clog/subtrans/transam/multixact). Second principle: **reuse Postgres
machinery through exports rather than reimplementing it** — xlogreader for
record parsing, `rm_redo` for single-page materialization, the index AMs for
re-apply. Battle-tested code, and it tracks version changes for free.

### 14.2 Layer placement

Rule of thumb: **per-record / per-page / per-tuple loops in C; per-commit /
per-frame orchestration in JS.** (Node-native crypto counts as C — hashing
from JS is fine.)

| Mechanism | Layer | Patch shape |
| --- | --- | --- |
| Frames, CAS client, leases/grants, manifest, era rotation, checkpoint worker, tailer orchestration, session proxy | JS | none |
| WAL slice capture | JS via VFS (`pg_wal` reads) + existing native LSN exports | none |
| Durability boundary / fsync interception | libc / Emscripten FS | none |
| WAL record iteration + classification for the applier | C, new file wrapping xlogreader; emits block refs + special-record callbacks to JS | new file |
| Page materialization at LSN (base + records) | C, new single-page redo entry point over `rm_redo` (walredo pattern) | new file |
| Synthesized clean-at-H control view (lazy boot, §6.5) | JS: VFS serves a constructed `pg_control` from tailer-tracked state | none |
| Sequence lease clamp + cursor re-assert | C new file + few-line hook in `nextval_internal` (`ResetSequenceCaches` already exists) | tiny hunk |
| Read-set capture | one hook at `PinBufferForBlock` appending to a ring buffer; JS harvests at commit | tiny hunk + new file |
| Prune/hint-FPI suppression for follower reads | GUC-gated guards at the few call sites | tiny hunks |
| Invalidation apply / cache flush on reset | C new file extending the existing `Pglite*` exports | new file |
| Logical harvest + re-apply (M5) | C new files over heapam/index-AM/TOAST APIs | new files |
| Commit gate (block inside COMMIT awaiting CAS) | C hook → synchronous JS import parked on the SAB/Atomics bridge | the one deep hunk — deferred to M5 |

Note the ordering luck: **the MVP needs no commit-path patch — under the
M1 contract (§3.7).** For one-shot commits the CAS runs in JS after the
query returns and before the client ack; the irreversible pre-commit steps
of §3.6 are neutralized not by a gate but by the contract (session-state
taint → fatal reset) plus proxy output buffering (§3.5). Pulling the
commit gate earlier would not help: without M5's in-place reset, a CAS
failure inside `COMMIT` still ends in a recycle and loses the same session
state — the gate only becomes useful together with the machinery it gates.
It arrives at M5, parked on the SAB/Atomics bridge already built for page
faults.

### 14.3 The cell host: shared pages, one tailer

Cells on a host must never fetch or materialize the same bytes twice:

- **content-addressed immutable cache** (sha256 → bytes) for checkpoint
  objects and base pages, shared across all cells, databases, and tenants —
  identical template pages (every fresh initdb looks alike) dedupe
  fleet-wide;
- **per-database materialized page-version cache** ((block, LSN) → bytes):
  physical replay is deterministic, so materialized versions are valid for
  every cell of that database and for forks below the fork point — a direct
  payoff of choosing physical over logical replication;
- **one stream tailer per (database, host)**: fans frames out to resident
  cells, maintains the shared page-version index, makes `local`-mode reads
  free, and shortens `linearizable` catch-up — but the tailer's head can
  lag commits acked via other hosts, which is exactly why `linearizable`
  still performs a stream `HEAD` confirmation (§7);
- per-cell memory holds only the dirty overlay and Postgres shared_buffers;
  cells run with small shared_buffers because the host cache is warm.

Side-channel note: a cross-tenant content-addressed cache is in principle a
cache-hit timing oracle; paranoid tiers (§11.3) scope dedup per-tenant.

### 14.4 Connection topology: cell-per-connection first

Configurable per server/database; two modes:

**`cell-per-connection` (default).** Every wire connection gets its own
PGlite cell. Sessions are exactly vanilla Postgres — own backend, own temp
tables, own GUCs, real interactive transactions — and concurrent connections
get real write concurrency *today*: **the optimistic commit protocol is the
multi-connection story until multi-session PGlite lands.** Cross-connection
visibility is the freshness model (§7): `session` by default (a cell
advances between transactions, so two connections of one app read each
other's committed writes via commit-LSN tokens), `linearizable` on request.
Same-host cells serialize their appends through a **host-local commit
sequencer** — the head lease is naturally host-scoped — so co-resident
connections never burn CAS round-trips racing each other; the stream CAS
guards cross-host races only.

Cost, honestly: ~80 MB per *connection*, not per database. Offsets: the
shared host cache (small per-cell shared_buffers), proxy-driven hibernation
of idle connections' cells (recycle, resurrect on next statement;
transaction-idle connections pin their cell), and the multiplexer below.

**`multiplexer`.** Many connections onto one cell via the existing
pglite-socket queue (per-handler transaction affinity, one statement at a
time). Right for read-heavy and many-idle-connection workloads. When
multi-session PGlite lands, this flips back to being the natural default —
one cell hosting real concurrent sessions, topology shifting from
cells-per-connection to cells-per-database.

### 14.5 The storage/stream gateway

A thin, stateless data-plane server that fronts both storage and streams.
Cell hosts speak only to it; it holds the real credentials. One binary,
three deployments — and the same API surface in all of them is what makes
sandbox → fleet promotion an upload, not a migration:

```text
embedded    in-process inside a Supalite sandbox (fs backend, no network)
dev         single node, fs backend, embedded DS test server
fleet       horizontal pool in front of object storage + DS service
```

Responsibilities:

- **serve checkpoint objects and spilled slices** — read-through cache;
  content-addressed objects make caching trivial and CDN-friendly; backend
  pluggable (local fs / object storage);
- **proxy durable-stream operations** (CAS appends, catch-up, long-poll).
  The DS server remains the append serializer; the gateway adds tenant
  authorization and frame validation (well-formed frames, size caps, sanity
  of `W.baseLsn`), and **validates-and-forwards** the strict
  `Stream-Expected-Offset` header — enforcement lands in the DS server's
  append lock (§2.3), never in the gateway (see the statelessness
  invariant below for why this is forced, not chosen);
- **mint and validate tenant-scoped capability tokens** (backed by
  control-plane auth, §14.6) — the concrete enforcement point for §11.2's
  credential scoping: cells and hosts never see raw storage or stream
  credentials.

**Statelessness is a hard invariant, not a preference — this is essential
to the design.** The gateway holds zero authoritative state; everything in
it is disposable cache, reconstructible from the stream, the object store,
or the control plane:

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

Consequences — which are the point:

- any request can hit any gateway instance: the routerless property (§1)
  extends through the platform layer;
- scaling is "add instances"; deploys are invisible; `kill -9` of any
  gateway at any moment changes latency and nothing else (§16 tests
  exactly this);
- **CAS enforcement cannot live in the gateway** — two instances can each
  believe they know the tail; only the stream server's per-stream critical
  section can decide. The gateway validates and forwards; the ~30-line
  strict check (§2.3) lands in the DS server where the append lock already
  is. A gateway-local "fast-path" of this check is the canonical
  statefulness creep to reject in review;
- the general test for any proposed gateway feature: if it needs state two
  instances must agree on, it belongs in the stream (control frames), the
  control plane, or nowhere.

Salvage note: the old branch's pageserver — Hono app, disk store, object
store — is the serving skeleton for exactly this component; only its
commit-promotion protocol stays dead (§13.1).

### 14.6 The control plane: a real Postgres

The platform's catalog is an ordinary, boring Postgres database — in dev it
can be PGlite itself, same SQL. It is authoritative for **topology and
lifecycle, never data**: the stream stays the truth for every database's
contents; the control plane records which databases exist and where their
streams and checkpoints are.

Schema, roughly:

```text
databases     tenant, name, status (idle|active|deleted), timestamps
lineage       parent database, fork LSN            -- GC refcounts = joins
eras          database → current era URL + offset; sealed era history
checkpoints   database → LSN → checkpoint object manifest ref
auth          API keys, token grants the gateway enforces
usage         metering rollups
```

This *upgrades* two mechanisms the doc previously did with weaker tools:

- **the manifest becomes rows, and manifest CAS becomes a transaction.**
  Rotation step 6 (§6.1) is a guarded `UPDATE … WHERE era = N`; fork
  creation is one transaction inserting the child row and pinning the
  parent. The object store consequently only needs immutable objects with
  read-after-write visibility — §2.6's conditional-update requirement moves
  here, onto a tool actually built for it;
- **fork-aware GC becomes SQL** — reverse-reference refcounting over
  `lineage` and `checkpoints` instead of refcount files.

Failure-independence rules (strict, testable):

- the commit path is cell → gateway → stream — **never** the control plane;
- control-plane writes are lifecycle-rate (create, fork, rotate, idle/wake
  transitions), never commit-rate — one modest Postgres indexes millions of
  databases;
- active databases keep serving through a control-plane outage (gateways
  cache era pointers and manifests with short TTLs); an outage degrades
  create/fork/rotate and cold wakes only;
- deliberate non-goal: hosting the control plane on this substrate itself —
  the recovery story must have no circular dependency.

The M6 GUI is simply a control-plane client with a stream tailer attached.

## 15. Milestones

Each is independently demoable; the conflict path starts trivial and hardens.

- **M0 — proofs.** WAL round-trip: run at pinned config, slice WAL between
  LSNs via the VFS, replay into a second cell from a checkpoint, assert
  identical dumps. Strict CAS extension in the TS server. Cell recycle timing.
  FPI/WAL-volume accounting vs page-image manifests.
- **M1 — single-host vertical slice.** Framed era streams; quiesced
  checkpoint objects + manifest; cold start via the synthesized clean-at-H
  control view (§6.5) — attach, never recover; head lease; one-shot CAS
  with re-execute-on-loss; **interactive transactions get `40001`-on-loss
  from day one** (sibling races exist the moment two connections do);
  session advance between transactions by cheap reattach — tainted
  sessions pin instead, a documented degradation until M3's live advance;
  session proxy v0 with cell-per-connection topology (§14.4), response
  buffering/spool and session-state taints enforcing the §3.7 contract —
  the "client observed nothing" property is M1, not polish; host cell
  manager: shared content-addressed page cache, one tailer per database,
  host-local commit sequencer, **host-local sequence cursor** (§5.3,
  §14.3); storage/stream gateway v0 (fs backend, embedded DS server) and
  control-plane schema v0 (§14.5–§14.6; PGlite as the dev control plane).
  Scale-to-zero works here.
- **M2 — storage lifecycle.** Era rotation via guarded seal, with the host
  sequencer as the quiesce point (§6.1); fork manifests over stream forks;
  GC horizon from lease pins + fork refcounts **and GC execution** — era
  deletion, checkpoint pruning, page-version trimming; the checkpoint
  cadence dial exposed per database (§2.4).
- **M3 — followers & live apply.** Lazy tail apply (page-version index,
  base-required flags), eager special-record set, live invalidation incl.
  all three inval carriers; **sibling-cell advance upgraded from reattach
  to incremental live apply**, lifting M1's tainted-session pinning; read
  replicas with freshness modes; DDL-on-followers; **NOTIFY sidecar frames
  with cross-cell LISTEN delivery — the headline demo (§10.2)**.
- **M4 — multi-host fleet.** Real cross-host CAS contention; sequence
  lease frames + incarnation-burn rules (§5.3); lease migration between
  hosts; gateway fleet mode; `linearizable` freshness verified against
  multi-host tailer lag (§7); convergence oracle in CI including the §5.1
  identity-chaining assertions.
- **M5 — interactive transparent rebase.** Read-set hook + repaired
  validation rules; schema-epoch fence; logical re-apply, inserts first,
  then update/delete with version preconditions; deferred-trigger ctid
  remap; **commit gate + in-place reset**; commit-sequence placement
  reorder (§3.6); session-state taint lift (§3.3).
- **M6 — productization.** DDL-lease polish; advisory-lock service or
  errors; **janitor automation** (vacuum/freeze cadence, GC scheduling);
  graduation tooling (logical export; physical materialize-and-start
  experiment); demo GUI: list databases, fork button, connect via psql
  through the proxy, a live per-database activity feed driven by the NOTIFY
  stream (§10.2) — the GUI itself is just another stream listener — and
  per-cell lazy-load/cache-hit counters (§6.5) so the footprint story is
  visible on screen: "this 2 GB database woke up by moving 40 KB".

## 16. Test strategy

- **The convergence oracle (build in week one):** replay a database's stream
  through vanilla Postgres recovery (it is real WAL) and `pg_dump`-diff
  against every cell's dump. Runs in CI on randomized multi-cell workloads.
  Extended with the §5.1 identity-chaining assertions: clean clog and
  multixact state plus a passing `pg_amcheck` under abort-heavy,
  savepoint-heavy, multi-cell runs — the no-allocator claim is tested, not
  argued.
- **Deterministic cluster simulator:** N in-process cells + in-memory Durable
  Streams server; scripted interleavings of CAS races, lease expiries, era
  rotations, crashes at every step of the commit pipeline (the pending/journal
  crash windows enumerate the matrix).
- **Rebase soundness suite:** targeted tests for every verified failure mode —
  VM-bit/IOS deletes, relation-extension phantoms, own-WAL LSN masking,
  catalog-change fencing (three inval carriers), aborted-subxact filtering,
  TOAST re-chunking, deferred-trigger ctid remap, sequence reset rules
  (foreign LWW page, stale SeqTable cache, aborted-txn records) — each
  asserting loud failure where transparency is impossible.
- **History checking:** elle/Jepsen-style analysis of multi-cell histories
  against the claimed model (per-cell vanilla; cross-cell snapshot-at-B
  commit-at-K; one-shot serial).
- **Contract tests:** `SKIP LOCKED` double-claim → `40001`; xmin-taint;
  advisory-lock policy; unlogged-table rejection; config-pin asserts; every
  row of the §3.7 MVP SQL contract table.
- **Client-observation property:** kill the CAS at every step of the commit
  pipeline and assert a one-shot client received zero bytes before
  resolution at every buffer regime (in-memory, disk-spooled,
  lease-escalated — §3.5), that read-only transactions flush without CAS,
  and that tainted sessions get the fatal session reset (§3.3), never a
  bare `40001` on a session whose state is gone — the §3.7 contract
  enforced mechanically, not by review.
- **Abuse/load suite (early, not late):** hot single sequence hammered from
  N cells; `RETURNING`-heavy ORM traffic; migration tools under the
  advisory-lock policy; `SKIP LOCKED` queue workers on two cells; FK-heavy
  schemas minting multixacts under savepoints; temp-table-heavy sessions
  exercising the taint path; incarnation-burn churn under aggressive
  hibernation.
- **Gateway statelessness (chaos) suite:** ≥2 gateway instances with
  per-request random routing; `kill -9` one instance at every step of the
  commit, catch-up, rotation, and wake flows; assert client-visible
  behavior is byte-identical and the only measurable effect is latency; no
  test may be able to distinguish which instance served which request. Any
  future gateway feature that breaks this suite is rejected by §14.5, not
  negotiated.
- **NOTIFY suite:** atomicity (notification in stream iff commit is);
  exactly-once under rebase (lost CAS attempts emit nothing); global commit
  order across three listening cells; vanilla parity for local semantics
  (post-commit delivery, discard on abort, `pg_notify` from triggers);
  proxy-attached client delivery; GUI/stream-listener path.
- **Postgres regression subset** on a single leased cell (should be near-clean
  — the point of physical fidelity).
- **Laziness regression suite (byte-count assertions, §6.5):** cold start
  moves O(metadata) bytes and zero relation pages; a point query fetches
  exactly its touched pages and bases, a second identical query fetches
  nothing; tail apply on a follower transfers no page content; fork creation
  transfers no page content; eviction under memory pressure recovers with
  correct results. These counters also surface in the demo GUI — laziness is
  part of the pitch, so it must be visible and un-regressable.
- **Performance rigs:** false-conflict rate on hot tables with/without prune
  suppression; commit latency vs slice size; join time vs era length;
  checkpoint cadence sweep; working-set-vs-database-size memory curves
  (footprint must stay flat as data grows).

## 17. Open questions

1. Strict `Stream-Expected-Offset` upstreamed vs deployment-specific? (Small
   either way; cooperative Stream-Seq suffices for trusted cells.)
2. Fuzzy checkpoints: how soon do quiesced shutdown-style checkpoints become
   the bottleneck for hot databases, forcing the backup-path implementation?
3. Read-set false-conflict rates in practice: is page-LSN + prune-suppression
   enough, or is the tuple-level slow path needed early?
4. Sequence lease sizing and the `currval` repopulation seam — validate
   against real ORM traffic.
5. In-place reset vs recycle: at what contention level does recycle latency
   (behind the proxy) actually hurt?
6. NOTIFY sidecar delivery guarantees for offline listeners (replay window =
   era? separate notify stream?).
7. Physical graduation: how close is a materialized checkpoint + tail to a
   `pg_upgrade`-able datadir for stock Postgres of the same major?
8. Import-surface hardening: enumerate exactly which Emscripten syscalls
   survive into the pinned import object, and the fuzz plan for the VFS
   boundary; when (if ever) the WASI tier graduates from POC.
9. Multi-tenant packing: cells per host, shared-module memory ceilings, warm
   pool sizing against the 80 MB active footprint.
10. Where does the schema-epoch live — derived only, or also an `X` frame for
    cheap joiner access?
11. Control-plane schema versioning and the multi-region story (the catalog
    is read-mostly — replicas suffice for reads, but era-rotation guards
    want a single writer region per database).
12. Gateway manifest-cache TTLs: the wake path reads the manifest, so TTL
    trades cold-wake latency against control-plane-outage tolerance —
    measure before choosing defaults.
