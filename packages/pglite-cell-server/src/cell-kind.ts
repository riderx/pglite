// M7 W3 — the session-facing cell type: session/rebase/runtime code is
// mode-agnostic across the plain in-process `Cell` (NodeFS) and the
// worker-hosted `WorkerCell` (LazyCellFS or passthrough). The two expose
// the same member NAMES; WorkerCell's Module-touching members are async
// where Cell's are sync, so all shared call sites `await` uniformly (an
// awaited sync value is a no-op).

import type { Cell } from '@electric-sql/pglite-cell'
import type { WorkerCell } from '@electric-sql/pglite-cell/worker-cell'

export type SessionCell = Cell | WorkerCell

/**
 * The host-side attach mode dial (M7 W3; W4 flips the default).
 *
 * `'auto'` (the W4 default) picks `'lazy-worker'` when the database's latest
 * checkpoint is v3-capable AND the gateway handle supports ranged object
 * reads, else `'nodefs'` (v1/v2 lineages). An explicit `'nodefs'` /
 * `'lazy-worker'` overrides the auto decision.
 */
export type CellMode = 'nodefs' | 'lazy-worker' | 'auto'
