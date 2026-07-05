import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    name: 'pglite-cell tests',
    globals: true,
    typecheck: { enabled: true },
    environment: 'node',
    testTimeout: 30000,
    watch: false,
    dir: './tests',
    // H7: file-level parallelism. Every test binds port 0 and mkdtemps its
    // own scratch (audited: no fixed ports, no shared env/paths, no chdir),
    // so files run concurrently across a bounded fork pool. Within a file,
    // `it`s stay serial (maxConcurrency: 1) — the integration fixtures boot
    // real cells and are memory-hungry, so overlapping FILES is the win, not
    // overlapping tests inside one. Escape hatch for any file later proven
    // parallel-unsafe: exclude it here and run it in a `*.serial.test.ts`
    // pass (see the `serial` config below). None needed today.
    fileParallelism: true,
    maxWorkers: 4,
    minWorkers: 1,
    maxConcurrency: 1,
  },
})
