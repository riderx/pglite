import { defineConfig } from 'tsup'

const entryPoints = [
  'src/index.ts',
  // M7 W1: the worker-cell entry + the worker artifact itself. WorkerCell
  // resolves `./worker-entry.js` (ESM) / `./worker-entry.cjs` (CJS) next to
  // its own bundle, so both must land in dist/worker-cell/ per format.
  'src/worker-cell/index.ts',
  'src/worker-cell/worker-entry.ts',
]

// Only entries with exports get dts (worker-entry is a side-effect script).
const dtsEntryPoints = ['src/index.ts', 'src/worker-cell/index.ts']

const minify = process.env.DEBUG === 'true' ? false : true

export default defineConfig([
  {
    entry: entryPoints,
    sourcemap: true,
    dts: {
      entry: dtsEntryPoints,
      resolve: true,
    },
    clean: true,
    minify: minify,
    shims: true,
    // No chunk splitting: worker-entry must be a standalone file the
    // Worker() constructor can load by path in both formats.
    splitting: false,
    format: ['esm', 'cjs'],
  },
])
