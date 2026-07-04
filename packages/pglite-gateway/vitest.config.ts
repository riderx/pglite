import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    name: 'pglite-gateway tests',
    globals: true,
    typecheck: { enabled: true },
    environment: 'node',
    testTimeout: 120000,
    watch: false,
    dir: './tests',
    maxWorkers: 1,
    fileParallelism: false,
    maxConcurrency: 1,
  },
})
