import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ControlPlane } from '../src/control-plane'

const TEST_TIMEOUT = 60_000

let cp: ControlPlane

beforeEach(async () => {
  // In-memory PGlite control plane (fast; no datadir).
  cp = await ControlPlane.create()
})

afterEach(async () => {
  await cp.close()
})

describe('ControlPlane schema v0 CRUD', () => {
  it(
    'creates databases and looks them up by id and name',
    async () => {
      const id = await cp.createDatabase('mydb')
      expect(id).toMatch(/^[0-9a-f-]{36}$/)

      const byId = await cp.getDatabaseById(id)
      expect(byId).toMatchObject({ id, name: 'mydb', status: 'active' })

      const byName = await cp.getDatabaseByName('mydb')
      expect(byName?.id).toBe(id)

      expect(
        await cp.getDatabaseById('00000000-0000-0000-0000-000000000000'),
      ).toBeNull()
      expect(await cp.getDatabaseByName('nope')).toBeNull()
    },
    TEST_TIMEOUT,
  )

  it(
    'enforces unique database names',
    async () => {
      await cp.createDatabase('dup')
      await expect(cp.createDatabase('dup')).rejects.toThrow()
    },
    TEST_TIMEOUT,
  )

  it(
    'lists databases in creation order',
    async () => {
      await cp.createDatabase('a')
      await cp.createDatabase('b')
      await cp.createDatabase('c')
      const names = (await cp.listDatabases()).map((d) => d.name)
      expect(names).toEqual(['a', 'b', 'c'])
    },
    TEST_TIMEOUT,
  )

  it(
    'adds eras and returns the highest-ordinal one as current',
    async () => {
      const id = await cp.createDatabase('eras')
      expect(await cp.currentEra(id)).toBeNull()

      await cp.addEra({
        databaseId: id,
        ordinal: 1,
        eraId: '000001-AAAA',
        path: 'era/000001-AAAA',
        baseOffset: '0000000000000000_0000000000000010',
        baseLsn: '0/1000',
      })
      await cp.addEra({
        databaseId: id,
        ordinal: 2,
        eraId: '000002-BBBB',
        path: 'era/000002-BBBB',
        baseOffset: '0000000000000000_0000000000000020',
        baseLsn: '0/2000',
        sealed: false,
      })

      const cur = await cp.currentEra(id)
      expect(cur).toMatchObject({
        ordinal: 2,
        eraId: '000002-BBBB',
        path: 'era/000002-BBBB',
        baseOffset: '0000000000000000_0000000000000020',
        baseLsn: '0/2000',
        sealed: false,
      })
    },
    TEST_TIMEOUT,
  )

  it(
    'registers checkpoints and returns the highest-LSN one; guarded insert is idempotent',
    async () => {
      const id = await cp.createDatabase('ckpts')
      expect(await cp.latestCheckpoint(id)).toBeNull()

      await cp.registerCheckpoint({
        databaseId: id,
        lsn: '0/1000',
        snapEnd: '0/1078',
        streamOffset: '0000000000000000_0000000000000010',
        objectRef: 'sha256:aaa',
      })
      await cp.registerCheckpoint({
        databaseId: id,
        lsn: '0/5000',
        snapEnd: '0/5078',
        streamOffset: '0000000000000000_0000000000000090',
        objectRef: 'sha256:bbb',
      })

      // pg_lsn ordering, not text ordering: 0/5000 > 0/1000.
      const latest = await cp.latestCheckpoint(id)
      expect(latest).toMatchObject({
        lsn: '0/5000',
        snapEnd: '0/5078',
        objectRef: 'sha256:bbb',
      })

      // Re-registering the same (db, lsn) with a different ref is a no-op.
      await cp.registerCheckpoint({
        databaseId: id,
        lsn: '0/5000',
        snapEnd: '0/5078',
        streamOffset: '0000000000000000_0000000000000090',
        objectRef: 'sha256:SHOULD-NOT-OVERWRITE',
      })
      const stillLatest = await cp.latestCheckpoint(id)
      expect(stillLatest?.objectRef).toBe('sha256:bbb')
    },
    TEST_TIMEOUT,
  )
})

describe('ControlPlane schema v1 (M2)', () => {
  it(
    'databases carry default dials; setDials round-trips',
    async () => {
      const id = await cp.createDatabase('dials')
      const db0 = await cp.getDatabaseById(id)
      expect(db0).toMatchObject({
        currentEraOrdinal: 1,
        checkpointEveryBytes: '0',
        rotateEveryBytes: '0',
        gcGraceMs: '300000',
      })

      await cp.setDials(id, {
        checkpointEveryBytes: 1_000_000,
        rotateEveryBytes: 64n * 1024n * 1024n,
        gcGraceMs: '5000',
      })
      const db1 = await cp.getDatabaseById(id)
      expect(db1).toMatchObject({
        checkpointEveryBytes: '1000000',
        rotateEveryBytes: '67108864',
        gcGraceMs: '5000',
      })

      // Partial update leaves untouched dials alone.
      await cp.setDials(id, { gcGraceMs: 1 })
      const db2 = await cp.getDatabaseById(id)
      expect(db2?.checkpointEveryBytes).toBe('1000000')
      expect(db2?.gcGraceMs).toBe('1')
    },
    TEST_TIMEOUT,
  )

  it(
    'era attempt register/promote and seal/advance guarded-update semantics',
    async () => {
      const id = await cp.createDatabase('rot')
      await cp.addEra({
        databaseId: id,
        ordinal: 1,
        eraId: '000001-A',
        path: 'era/000001-A',
        baseOffset: '0000000000000000_0000000000000010',
        baseLsn: '0/1000',
      })

      // Register an attempt; it shows up as an orphan (grace 0) until promoted.
      await cp.registerEraAttempt({
        databaseId: id,
        ordinal: 2,
        eraId: '000002-B',
        path: 'era/000002-B',
      })
      let orphans = await cp.listOrphanAttempts(0)
      expect(orphans.map((o) => o.eraId)).toContain('000002-B')

      await cp.promoteEraAttempt(id, '000002-B')
      orphans = await cp.listOrphanAttempts(0)
      expect(orphans.map((o) => o.eraId)).not.toContain('000002-B')

      // Seal era 1 -> 2 and add era 2.
      await cp.sealEra(id, 1, {
        finalOffset: '0000000000000000_0000000000000050',
        finalLsn: '0/5000',
        nextOrdinal: 2,
      })
      const era1 = await cp.eraByOrdinal(id, 1)
      expect(era1).toMatchObject({
        sealed: true,
        sealedFinalOffset: '0000000000000000_0000000000000050',
        sealedFinalLsn: '0/5000',
        nextEraOrdinal: 2,
      })

      // advanceCurrentEra: guarded — true on right `from`, false on wrong.
      expect(await cp.advanceCurrentEra(id, 1, 2)).toBe(true)
      expect((await cp.getDatabaseById(id))?.currentEraOrdinal).toBe(2)
      // Repeat with the now-stale `from` returns false (someone else advanced).
      expect(await cp.advanceCurrentEra(id, 1, 2)).toBe(false)
    },
    TEST_TIMEOUT,
  )

  it(
    'lineage records fork edges and lists live children',
    async () => {
      const parent = await cp.createDatabase('lin-parent')
      const child = await cp.createDatabase('lin-child')
      await cp.insertLineage({
        childId: child,
        parentId: parent,
        forkLsn: '0/3000',
        forkOffset: '0000000000000000_0000000000000030',
      })
      const kids = await cp.childrenOf(parent)
      expect(kids).toHaveLength(1)
      expect(kids[0]).toMatchObject({
        childId: child,
        parentId: parent,
        forkLsn: '0/3000',
        forkOffset: '0000000000000000_0000000000000030',
      })
      expect(await cp.childrenOf(child)).toHaveLength(0)
    },
    TEST_TIMEOUT,
  )

  it(
    'pins: upsert, live listing, TTL expiry sweep',
    async () => {
      const id = await cp.createDatabase('pin-db')
      await cp.upsertPin({
        id: '11111111-1111-1111-1111-111111111111',
        databaseId: id,
        kind: 'gc-pin',
        holder: 'joiner-x',
        pinnedOffset: '0000000000000000_0000000000000010',
        pinnedLsn: '0/1000',
        expiresAt: new Date(Date.now() + 60_000),
      })
      expect(await cp.livePins(id)).toHaveLength(1)

      // An already-expired pin is not "live" and is swept by expirePins.
      await cp.upsertPin({
        id: '22222222-2222-2222-2222-222222222222',
        databaseId: id,
        kind: 'gc-pin',
        holder: 'joiner-y',
        pinnedOffset: '0000000000000000_0000000000000020',
        pinnedLsn: '0/2000',
        expiresAt: new Date(Date.now() - 1000),
      })
      expect(await cp.livePins(id)).toHaveLength(1) // still just the live one
      const swept = await cp.expirePins()
      expect(swept).toBe(1)

      await cp.deletePin('11111111-1111-1111-1111-111111111111')
      expect(await cp.livePins(id)).toHaveLength(0)
    },
    TEST_TIMEOUT,
  )
})
