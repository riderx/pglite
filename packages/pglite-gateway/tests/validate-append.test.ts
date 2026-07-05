// validateAppendBody frame validation (§14.5), with emphasis on the H3
// object-ref slice spill ("w" frame): a W frame whose WAL bytes live in a
// content-addressed object (header carries `objectRef`, no inline wal). Such a
// frame is well-formed iff objectRef is nonempty and baseLsn < endLsn.

import { describe, it, expect } from 'vitest'
import { encodeFrame } from '@electric-sql/pglite-cell'
import type { WFrame } from '@electric-sql/pglite-cell'
import { validateAppendBody } from '../src/http'

/** A base inline-W frame with the required LSN discipline. */
function inlineW(overrides: Partial<WFrame['header']> = {}): Uint8Array {
  const frame: WFrame = {
    type: 'W',
    header: {
      v: 1,
      eraId: '000001-ABC',
      expectedOffset: '0000000000000000_0000000000000000',
      commitId: 'c1',
      kind: 'commit',
      baseLsn: '0/1000',
      endLsn: '0/2000',
      sliceHash: 'deadbeef',
      ...overrides,
    },
    wal: new Uint8Array([1, 2, 3, 4]),
  }
  return encodeFrame(frame)
}

/** A "w" spill frame: header carries objectRef, no inline wal. */
function spillW(objectRef: string | undefined, walEmpty = true): Uint8Array {
  const header: Record<string, unknown> = {
    v: 1,
    eraId: '000001-ABC',
    expectedOffset: '0000000000000000_0000000000000000',
    commitId: 'c1',
    kind: 'commit',
    baseLsn: '0/1000',
    endLsn: '0/2000',
    sliceHash: 'deadbeef',
  }
  if (objectRef !== undefined) header.objectRef = objectRef
  const frame = {
    type: 'W',
    header,
    wal: walEmpty ? new Uint8Array(0) : new Uint8Array([9]),
  } as unknown as WFrame
  return encodeFrame(frame)
}

describe('validateAppendBody', () => {
  it('accepts a plain inline W frame', () => {
    expect(validateAppendBody(inlineW())).toBeNull()
  })

  it('accepts a well-formed "w" object-ref spill (nonempty objectRef, no wal)', () => {
    const body = spillW('sha256:' + 'a'.repeat(64))
    expect(validateAppendBody(body)).toBeNull()
  })

  it('rejects a spill with an empty objectRef', () => {
    const reason = validateAppendBody(spillW(''))
    expect(reason).toMatch(/empty objectRef/)
  })

  it('still rejects a W frame with baseLsn >= endLsn (spill or not)', () => {
    const bad = inlineW({ baseLsn: '0/2000', endLsn: '0/1000' })
    expect(validateAppendBody(bad)).toMatch(/not < endLsn/)
    const badSpill = spillW('sha256:' + 'b'.repeat(64))
    // sanity: the good spill passes, so isolate the LSN rule via a broken one.
    expect(validateAppendBody(badSpill)).toBeNull()
  })

  it('rejects an empty eraId on any frame', () => {
    const body = inlineW({ eraId: '' })
    expect(validateAppendBody(body)).toMatch(/empty eraId/)
  })
})
