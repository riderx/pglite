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
