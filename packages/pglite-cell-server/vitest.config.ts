import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    name: 'pglite-cell-server tests',
    globals: true,
    typecheck: { enabled: true },
    environment: 'node',
    testTimeout: 240000,
    hookTimeout: 240000,
    watch: false,
    dir: './tests',
    // H7: file-level parallelism. Every fixture builds its own
    // GatewayCore + CellHost + proxy on port 0 under a private mkdtemp
    // (audited: no fixed ports, no shared env/paths, no chdir), so files
    // run concurrently. The fixtures boot several real PGlite cells each and
    // are memory-hungry, so the pool is bounded to 3 (vs cell's 4). Within a
    // file, `it`s stay serial. Escape hatch for a file later proven
    // parallel-unsafe: a `*.serial.test.ts` pass. None needed today.
    fileParallelism: true,
    maxWorkers: 3,
    minWorkers: 1,
    maxConcurrency: 1,
  },
})
