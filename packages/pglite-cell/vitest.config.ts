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
    maxWorkers: 1,
    fileParallelism: false,
    maxConcurrency: 1,
  },
})
