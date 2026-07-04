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
    maxWorkers: 1,
    fileParallelism: false,
    maxConcurrency: 1,
  },
})
