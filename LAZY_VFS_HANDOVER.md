# Handover: make lazy-worker the default cell mode (the rotation quiesce fix)

You are picking up a large, mature project. **One specific, well-diagnosed
problem stands between the current state and shipping the headline feature.**
This document gives you everything: the exact defect, the exact fix, the
exact files and lines, the test changes, the verification protocol, and the
traps. Read it fully before writing code. Do not re-architect; the fix is
localized and specified.

---

## 0. Orientation (read once)

**What the project is.** PGlite (WASM Postgres) turned into a scale-to-zero,
multi-master, serverless database. Real Postgres WAL bytes are the canonical
payload on a per-database append-only "Durable Stream"; writers race and a
compare-and-append (CAS) at the stream head serializes commits into one
linear WAL. Milestones M0–M6 are complete and pushed; an audit-added lazy-VFS
capstone (M7) and a hardening wave are also landed. **The design doc is
`OPTIMISTIC_PHYSICAL_REPLICATION_DESIGN.md`** (authoritative). Milestone plans
are `M1_PLAN.md`…`M6_PLAN.md`, `M7_LAZY_VFS_PLAN.md`, `HARDENING_PLAN.md`.

**Repos / branches.**
- Superproject: `/Users/samwillis/Code/pglite`, branch
  `optimistic-physical-replication` (HEAD ~`bf7f072e`). Pushed to
  `electric-sql/pglite`.
- Native submodule: `/Users/samwillis/Code/pglite/postgres-pglite`, paired
  branch `optimistic-physical-replication` (HEAD `4d9a5b43c1`). Pushed. **YOU
  DO NOT NEED TO TOUCH THE SUBMODULE FOR THIS TASK — it is pure TypeScript.**
  The WASM is already built and in `packages/pglite/dist`; no docker rebuild
  is required.
- Durable Streams: `/Users/samwillis/Code/durable-streams`, branch
  `optimistic-physical-replication` (pushed). Not needed for this task.

**The three packages you will touch (all TypeScript):**
- `packages/pglite-cell` — the cell library: frame codec, DS CAS client,
  `Committer` (the commit sequencer — **this is where the fix's core lives**),
  `EraTailer`, `WorkerCell` (lazy-worker cell), datadir/materialize/live-apply.
- `packages/pglite-cell-server` — the host: `DatabaseRuntime`, rotation
  (`src/rotation.ts` — **the other half of the fix**), session/proxy, janitor.
- `packages/pglite-gateway` — object store, control plane (Postgres), stream
  proxy, checkpoint v3 format. (Only relevant here for default settings.)

**Environment / tooling.**
- Node 18.20.5 in this shell (engines want ≥20; it warns, works). pnpm 9.7.0.
- Per-package scripts: `pnpm build` (tsup), `pnpm typecheck` (tsc),
  `pnpm test` / `npx vitest run`, `pnpm stylecheck` (eslint + prettier, run
  by the husky pre-commit hook repo-wide as `pnpm -r stylecheck` with
  `--write`).
- **Vitest is file-parallel** (H7): `maxWorkers` 3–4. The full cell-server
  suite is slow (~13–25 min). Run individual files while iterating; run the
  full suite only for the final gate. `TEST_TIMEOUT` in the test files is
  generous.
- Prettier: no semicolons, single quotes, 2-space. ESLint `--max-warnings 0`.

**Commit discipline (IMPORTANT).** The husky pre-commit hook runs
`prettier --write` across the repo, which can leave working-tree changes
*after* a commit (reconcile them with a follow-up `chore: prettier` commit;
check `git status` after every commit). Commit messages end with a
`Co-Authored-By:` trailer — match the existing `git log` style. This fix is
JS-only, so **no submodule/gitlink dance is needed** (that discipline, §14.8,
only applies when `postgres-pglite` changes).

---

## 1. The problem, precisely

**Symptom.** `packages/pglite-cell-server/tests/rotation.test.ts` test **#6**
("mid-commit rotation at the proxy level: a pg client keeps inserting while
the era rotates") **fails when the default cell mode is `lazy-worker`.** It
passes in `nodefs` mode. Because of this, the M7-W4 change that made
lazy-worker the *default* was reverted: today the defaults are
`cellMode: 'nodefs'` (in `packages/pglite-cell-server/src/database-runtime.ts`,
~line 189) and `checkpointFormat: 2` (in `packages/pglite-gateway/src/core.ts`,
~line 114). Lazy-worker + v3 are fully built and TESTED as an opt-in
(`cellMode: 'auto' | 'lazy-worker'` + `checkpointFormat: 3`; the §16 lazy
suite `tests/lazy.test.ts` and `tests/watchdog.test.ts` exercise them and are
green). **Your job: implement the quiesce fix, then flip both defaults back,
and prove test #6 (and the whole corpus) green in the default (now
lazy-worker) mode.**

**Root cause — era rotation's seal race.** Rotating era N → N+1 is a
multi-step sequence in `packages/pglite-cell-server/src/rotation.ts`
(`rotateDatabase`, the `for (let attempt = 0; attempt <= MAX_RECUTS; …)` loop,
~lines 376–520):

1. checkpoint (once, before the loop).
2. capture `head = runtime.tailer.head` (~line 404).
3. `registerEraAttempt` + `client.createStream(nextPath, { body: O frame })`
   — **a gateway/network round-trip** (~lines 405–427). The O frame records
   `baseLsn = head.lsn`.
4. `runtime.committer.sealEra(build, { ifHeadOffset: head.offset })`
   (~line 431). The S frame records `finalLsn = head.lsn`.

There is a **hard invariant, the O/S mirror**: `finalLsn(era N) ==
baseLsn(era N+1)` — the sealed era must end exactly where the new era begins
(readers hop N→N+1 at that LSN; a gap or overlap corrupts the read path).

`sealEra` (in `packages/pglite-cell/src/committer.ts`, ~lines 472–533)
enforces this: it reads the *current* head and, if `opts.ifHeadOffset` (the
head captured back at step 2) no longer equals it, returns
`{ result: 'seq-conflict' }` **without appending** — because a commit landed
in the window between step 2 and step 4, moving the head, so sealing at the
old head would break the mirror. On `seq-conflict` the rotator **re-cuts**
(catch up, fresh era attempt at a new URL against the new head, try again),
bounded by `MAX_RECUTS = 5` (`rotation.ts` ~line 42), then it throws
`"rotation … exhausted N seal re-cuts"`.

**Why lazy-worker breaks it.** The window in step 3 (the gateway PUT) is where
a client `INSERT` slips in. In `nodefs` mode the window is short enough that
against test #6's 12 rate-limited inserts the rotator wins within 5 re-cuts.
In `lazy-worker` mode every operation carries worker round-trip + page-fault
latency, widening the window so a client insert lands during *every* re-cut;
the 5-budget is exhausted and rotation throws.

**Severity: liveness, not safety.** When rotation exhausts the budget it
throws and the database keeps serving era N — every committed row is intact,
no reader ever sees a torn/gapped log. It is a starvation/progress failure
(a busy database can't rotate), which is why it was a defensible temporary
revert — but it must be fixed to ship lazy-worker as the default.

---

## 2. The fix, precisely

**Core idea — a true quiesce.** The rotator currently releases the committer's
serialization mutex between steps 2–4, so same-host commits interleave. Hold
the mutex across the entire `{read head → PUT era N+1 → seal era N}` critical
section so **no same-host commit can move the head inside the mirror window.**
Then single-host rotation seals on the first try.

**CRITICAL NUANCE — same-host only; cross-host must still re-cut.** The
committer's mutex only serializes *this host's* appends. A commit from a
**different host** (a different `Committer` on a different `CellHost`) lands on
the Durable Streams server regardless of our mutex, so our seal at the
captured head still gets a legitimate server-side `409 seq-conflict` → re-cut.
**This is correct and must be preserved.** Test #3 ("a commit races the seal")
injects its racing commit **from a second host** (`secondHost(ctx, 'h2')`,
`rotation.test.ts` ~lines 291–307) precisely to exercise the cross-host
re-cut + adopt path. Your fix must:
- eliminate **same-host** seal races (fixes #6), and
- leave **cross-host** seal races re-cutting exactly as today (keeps #3, #4).

### 2a. New committer method: `Committer.sealExclusive`

In `packages/pglite-cell/src/committer.ts`. The committer serializes via a
private promise-chain mutex `run<T>(fn)` (~lines 198–207): `this.chain =
this.chain.then(fn)`. `sealEra` (and every other append) runs inside one
`this.run(...)`. **Deadlock trap:** anything you call from *inside* a
`this.run` block must NOT itself call `this.run` (it would await the same
chain and hang). The rotation register + gateway PUT are NOT committer calls,
so they are safe to run inside the exclusive block.

Add a method that acquires the mutex once and runs the whole critical section,
reading the head *inside* the block:

```ts
/**
 * Rotation seal under a held mutex (§6.1 quiesce). Runs {read current head →
 * `critical(head)` → seal era N at that head} in ONE `this.run()` acquisition,
 * so no SAME-host append can move the head inside the O/S-mirror window
 * (finalLsn(N) == baseLsn(N+1)). Cross-host commits are unaffected and still
 * yield a server-side seq-conflict → the caller re-cuts. `critical` must do
 * the rotator's registerEraAttempt + O-frame PUT cut against `head`, and
 * return the S frames to seal era N at `head.offset`. `critical` MUST NOT call
 * back into this committer (deadlock).
 */
sealExclusive(
  critical: (head: { offset: string; lsn: bigint }) => Promise<Frame[]>,
): Promise<SealEraResult> {
  return this.run(async () => {
    const era = this.tailer.currentEra
    const head = { offset: this.tailer.head.offset, lsn: this.tailer.head.lsn }
    const frames = await critical(head) // register + gateway PUT happen here, mutex held
    if (frames.some((f) => f.header.expectedOffset !== head.offset)) {
      throw new Error('sealExclusive: built frames must carry the head offset')
    }
    const body = encodeAppend(frames)
    const producer = {
      id: this.producerId,
      epoch: this.epoch,
      seq: this.seqByPath.get(era.path) ?? 0,
    }
    const res = await this.postWithRetry(era.path, body, {
      seq: casToken(era.ordinal, head.offset),
      expectedOffset: head.offset,
      producer,
      close: true,
    })
    switch (res.kind) {
      case 'ok':
        if (res.deduped) {
          await this.tailer.catchUp()
        } else if (this.tailer.head.offset === head.offset) {
          this.tailer.advanceLocal(frames, res.nextOffset)
        }
        this.seqByPath.set(era.path, producer.seq + 1)
        return { result: 'sealed', offset: head.offset, nextOffset: res.nextOffset }
      case 'seq-conflict':
        return { result: 'seq-conflict' } // cross-host raced — caller re-cuts
      case 'closed':
        return { result: 'closed' }
      case 'stale-epoch':
        throw new FencedError(this.epoch, res.currentEpoch)
      case 'producer-gap':
        throw new ProducerGapError(res.expectedSeq, res.receivedSeq)
    }
  })
}
```

This is `sealEra`'s body with (a) the head read *inside* the block and (b) a
`critical(head)` callback inserted between the head read and the seal append.
Confirm against the real `sealEra` (~lines 472–533) that you mirror its
`ok`/`deduped`/`advanceLocal`/`seqByPath` handling exactly — copy it, do not
paraphrase. `SealEraResult`, `postWithRetry`, `casToken`, `encodeAppend`,
`FencedError`, `ProducerGapError`, `Frame` are all already imported/defined in
this file. **Keep `sealEra` as-is** (do not delete it — other code/tests may
still reference it; you can remove it later if truly unused, but not as part
of this change).

### 2b. Rewire the rotation loop

In `packages/pglite-cell-server/src/rotation.ts`, inside the `for (attempt…)`
loop. Today it does: `catchUp` → adopt-check → compute `nextOrdinal/nextEraId/
nextPath` → `registerEraAttempt` → `createStream(O frame)` (capturing
`nextBaseOffset = created.nextOffset`) → `sealEra(…, { ifHeadOffset: head.offset })`
→ branch on `res.result`.

Move steps 3–5 (register + PUT + seal) **into a single `sealExclusive`
call** whose `critical(head)` does the register + PUT (cutting the O frame's
`baseLsn`/`snapEnd` against the *passed* `head.lsn`, not a pre-captured one)
and returns the S frames (with `expectedOffset`/`finalOffset` = `head.offset`
and `finalLsn` = `head.lsn`). Capture `nextBaseOffset` via a closure variable
set inside `critical`. Keep the `catchUp` + adopt-check at the *top* of the
loop (outside the exclusive block — they do reads/hops). Keep everything after
the seal result (`completeManifest`, `refreshManifest`,
`ensureCheckpointCoversEraBase`, the return object, the `seq-conflict ⇒
reCuts++; catchUp; continue` branch, the `closed ⇒ adopt` branch) **unchanged**.

Sketch (adapt to the real code — preserve all existing variable names,
logging, `ensureCheckpointCoversEraBase`, and error handling):

```ts
const cur = runtime.tailer.currentEra
const nextOrdinal = cur.ordinal + 1
const nextEraId = `${pad6(nextOrdinal)}-${sortableId()}`
const nextPath = `/era/${nextEraId}`
let nextBaseOffset = ''

const res = await runtime.committer.sealExclusive(async (head) => {
  const baseLsnText = formatLsn(head.lsn)
  await gw.registerEraAttempt(databaseId, {
    ordinal: nextOrdinal, eraId: nextEraId, path: nextPath,
  })
  const oFrame: OFrame = {
    type: 'O',
    header: {
      v: 1, eraId: nextEraId, expectedOffset: INITIAL_OFFSET_TOKEN,
      ordinal: nextOrdinal, prevEraId: cur.id, prevEraUrl: cur.path,
      baseOffset: INITIAL_OFFSET_TOKEN, baseLsn: baseLsnText,
      snapEnd: baseLsnText, checkpointRef: ckpt.checkpointRef,
    },
  }
  const created = await client.createStream(nextPath, { body: encodeAppend([oFrame]) })
  nextBaseOffset = created.nextOffset
  const sFrame: SFrame = {
    type: 'S',
    header: {
      v: 1, eraId: cur.id, expectedOffset: head.offset, ordinal: cur.ordinal,
      finalOffset: head.offset, finalLsn: baseLsnText,
      nextEraUrl: nextPath, nextEraId,
    },
  }
  return [sFrame]
})

if (res.result === 'seq-conflict') { reCuts++; await runtime.tailer.catchUp(); continue }
// … existing 'closed' ⇒ adopt branch, then 'sealed' ⇒ completeManifest(… nextBaseOffset …) …
```

**Note on the checkpoint/base relationship.** Because `head` is now read
*inside* the exclusive block, a commit that landed between the (pre-loop)
checkpoint and the seal is included in the sealed era, so the O frame's
`baseLsn` can be > `ckpt.snapEnd`. That is already handled: joiners read era
N's tail from the checkpoint's `snapEnd` up to `finalLsn` then hop, and
`ensureCheckpointCoversEraBase(runtime)` (already called after a successful
rotation) cuts a fresh checkpoint at the era base for future joiners. **Verify
this still holds** with a fresh-joiner attach test (rotation test #1 already
asserts "a fresh joiner attaches via checkpoint + era-2 tail only" — make sure
it stays green).

### 2c. Migrate the test race-injection points

`rotation.test.ts` has a helper `wrapSealEra(committer, wrap)` (~lines
108–128) that monkeypatches `committer.sealEra`. It is used by tests **#2, #3,
#4**. Since the rotator now calls `sealExclusive`, those wraps won't fire.
Provide a parallel `wrapSealExclusive(committer, wrap)` (same shape, patches
`sealExclusive`) and point #2/#3/#4 at it. Key detail for **#3**: its wrap runs
a **sibling-host** insert before calling `orig` — that models a cross-host
race, which `sealExclusive` deliberately does NOT prevent, so #3 must still see
`reCuts ≥ 1` and the raced commit landing in era 1 with `era1.finalLsn ==
era2.baseLsn`. Wrapping `sealExclusive` (whose `critical` runs the PUT, after
which the sibling's committed insert makes the server reject our seal) fires
the sibling insert in the right window → real 409 → re-cut. Confirm #3's
assertions still pass. (#2 forces a one-shot `seq-conflict` to test re-cut/GC;
#4 wraps both hosts for the concurrent-rotator adopt path — both just need the
wrap to target the new method.)

### 2d. Flip the defaults back

- `packages/pglite-cell-server/src/database-runtime.ts` (~line 189): change
  the resolved `cellMode` default from `'nodefs'` to `'auto'` (keep the
  `PGLITE_CELL_MODE` env override and the explicit-opt precedence). There is a
  large explanatory comment there describing exactly this revert — update it to
  say the quiesce landed and the default is `'auto'` again.
- `packages/pglite-gateway/src/core.ts` (~line 114): change
  `this.checkpointFormat = opts.checkpointFormat ?? 2` back to `?? 3`. Update
  the adjacent comment.
- Check the three gateway tests pinned to `checkpointFormat: 2`
  (`tests/gateway-e2e.test.ts`, `inprocess.test.ts`, `statelessness.test.ts`)
  and the `extractDatadir`-direct v1/v2 tests in
  `packages/pglite-gateway/tests/checkpoint-object.test.ts` — those pins are
  about exercising v1/v2 *extraction paths* and should STAY pinned (they test
  backward compat, not the default). Only remove a pin if it exists solely to
  dodge the default and the test is really about the default path.
- `tests/watchdog.test.ts` forces `cellMode: 'lazy-worker'` explicitly — leave
  it (harmless and correct regardless of the default).

---

## 3. Verification protocol (the definition of done)

This fix is **pure TypeScript — NO docker/WASM rebuild needed.** The WASM at
the current gitlink already has everything (the native lazy-attach/redo/reset
primitives from M5/M7 are built in).

Run, in order, fixing until green:

1. `cd packages/pglite-cell && npx tsc --noEmit && npx vitest run` → expect
   **89 passed** (+ any you add). `sealExclusive` unit coverage welcome.
2. `cd packages/pglite-gateway && npx tsc --noEmit && npx vitest run` → **76**.
3. `cd packages/pglite-cell-server && npx tsc --noEmit` then, while iterating,
   run the targeted files first:
   - `npx vitest run tests/rotation.test.ts` → **8/8** (especially #3, #4, #6)
     — run it **in isolation AND 3–5× consecutively**; rotation has shown
     load-sensitive flakiness (an `ECONNRESET` transient exists; a bounded
     retry was added to the stream-client read path — if you still see rare
     socket-hang flakes, that is pre-existing and separate from this fix, but
     #6 must now pass **deterministically**, not flakily).
   - `npx vitest run tests/lazy.test.ts` → the §16 byte-count suite stays green
     (it forces lazy-worker explicitly; the cold-start ≈ 3.28 MB / 50 MB-table,
     point-query = 2 chunk faults, repeat = 0 assertions must hold).
   - `npx vitest run tests/contention.test.ts` (the FK-multixact test is
     flaky under full-parallel load but passes solo — not your regression).
4. Final gate: `npx vitest run` (full cell-server suite, ~13–25 min, now in
   default = lazy-worker mode) → **110/110**. If a file other than the known
   flakes (`contention` FK-multixact, occasional rotation ECONNRESET) fails,
   it is in scope.
5. Style: `pnpm stylecheck` clean in each touched package (or
   `npx eslint ./src ./tests --fix && npx prettier --write ./src ./tests`).

**Commit** with the §14.8-free JS discipline: stage the touched files, commit
with a descriptive message + `Co-Authored-By:` trailer, then `git status`
again and make a `chore: prettier` reconcile commit if the husky hook left
working-tree changes. Push `optimistic-physical-replication`.

**Update docs when done:** `M7_LAZY_VFS_PLAN.md` §W4 (flip "default deferred"
→ "default `'auto'`, quiesce landed"); the `cellMode` comment in
`database-runtime.ts`; and add a one-line §15-M7 note in the design doc if you
like. Update the project memory file at
`/Users/samwillis/.claude/projects/-Users-samwillis/memory/pglite-multi-master-project.md`
(the last big paragraph tracks this exact item).

---

## 4. Traps and rules (do not skip)

1. **Same-host vs cross-host is the whole game.** The quiesce fixes same-host
   races only. If you ever find yourself trying to stop a *cross-host* commit
   from racing the seal, you have misunderstood — that path must keep
   re-cutting (tests #3/#4 prove it). The mutex is per-`Committer`; other hosts
   have their own.
2. **Deadlock trap.** `critical` runs inside `this.run`. It must only call
   gateway/`client`/`tailer`-read code, never a `Committer` method that itself
   calls `this.run` (`commitSlice`, `appendControl`, `sealEra`, another
   `sealExclusive`). The register + `createStream` PUT are safe.
3. **Do not weaken the `ifHeadOffset`/mirror guard as a "fix."** The guard is
   correct; the fix is to close the window, not to allow mirror-breaking seals.
4. **Do not touch `postgres-pglite` or rebuild WASM.** JS-only change.
5. **Husky reformats post-commit** — always re-check `git status` and reconcile.
6. **If you spawn subagents:** they frequently stop saying "a monitor will wake
   me" — nothing wakes a stopped agent. Resume them (SendMessage) with "no
   monitors; run in the FOREGROUND, chunked, long timeouts." Prefer running the
   slow suites yourself in the foreground with generous timeouts.
7. **Full cell-server suite is slow (10–25 min).** Don't chain short sleeps to
   poll; run it once as the final gate with a long timeout, iterate on single
   files.

---

## 5. If the quiesce proves insufficient (unlikely, but the fallback)

If, after the mutex hold, test #6 still flakes (it should not — same-host
inserts now strictly queue behind the exclusive block), the remaining cause
would be that `client.createStream` inside `critical` is *itself* observing a
head move because the tailer's `catchUp` runs off-mutex elsewhere. In that
case, additionally: (a) confirm no background `catchUp`/`pollOnce` advances the
tailer head concurrently with the exclusive block (the tailer's `catchUp` was
serialized in M4 — verify), and (b) as a belt-and-braces bound, raise
`MAX_RECUTS` and add jittered backoff between re-cuts. But treat that strictly
as secondary; the mutex hold is the real fix and should make #6 deterministic.

---

**Bottom line:** add `Committer.sealExclusive`, route rotation's
register+PUT+seal through it, migrate the three test injection points to the
new method, flip the two defaults, and gate the corpus in lazy-worker mode.
The lazy VFS then ships as the default — a cold start moving ~3 MB for a
50 MB database, which is the entire point of the design.
