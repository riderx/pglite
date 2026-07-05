// Test bootstrap: run the TypeScript worker entry inside a Worker without a
// build step (vitest runs the host from src; the Worker thread needs its own
// loader). tsx is already a devDependency.
import { register } from 'tsx/esm/api'
register()
await import('../src/worker-cell/worker-entry.ts')
