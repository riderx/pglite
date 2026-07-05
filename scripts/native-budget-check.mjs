#!/usr/bin/env node
// H6-lite native-hunk budget check.
//
// The `postgres-pglite` submodule carries a small, deliberately-bounded set of
// `#ifdef __PGLITE__`-guarded edits inside otherwise-vanilla PostgreSQL source
// files. Each guarded hunk is native C surface we must maintain, audit, and
// port across upstream rebases — so the count is a budget, not a free variable.
// This script:
//
//   1. Counts, per modified existing file, the `__PGLITE__`-guarded preprocessor
//      hunks (`#if / #ifdef / #ifndef / #elif` directives that mention
//      `__PGLITE__`), EXCLUDING the net-new PGlite subtrees (`src/backend/pglite/`,
//      `src/include/pglite/`, `pglite/`) and PGlite dev scaffolding
//      (`README-PGLITE*`, `build-pglite*`) — those are wholly-new files, not
//      guarded edits to existing PostgreSQL code.
//   2. Compares that census against the allowlist in `native-hunk-budget.json`
//      (a snapshot of current reality). Any new/removed guarded file, or any
//      file whose hunk count changed, is a VIOLATION.
//   3. Asserts the superproject's gitlink commit for the submodule actually
//      exists on the submodule's configured origin branch (so the pinned commit
//      is reachable/pushed, not a local-only SHA).
//
// Exits nonzero on any violation. This is a standalone script — it is NOT wired
// into CI here; see the one-line job step printed at the end.

import { execFileSync } from 'node:child_process'
import { readFileSync, existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), '..')
const SUBMODULE = 'postgres-pglite'
const SUBMODULE_DIR = join(REPO_ROOT, SUBMODULE)
const BUDGET_FILE = join(REPO_ROOT, 'native-hunk-budget.json')

// Paths that are wholly-new PGlite files, NOT guarded edits to existing files.
const EXCLUDE_RE =
  /^(src\/backend\/pglite\/|src\/include\/pglite\/|pglite\/|README-PGLITE|build-pglite)/

// A guarded hunk: a preprocessor conditional directive that references __PGLITE__.
const HUNK_RE = /^\s*#\s*(if|ifdef|ifndef|elif)([\s!].*)?__PGLITE__/

function git(args, cwd) {
  return execFileSync('git', args, { cwd, encoding: 'utf8' })
}

/** The census: { relPath: guardedHunkCount } over modified existing files. */
function census() {
  const files = git(['grep', '-lE', '__PGLITE__'], SUBMODULE_DIR)
    .trim()
    .split('\n')
    .filter((f) => f && !EXCLUDE_RE.test(f))
    .sort()
  const out = {}
  for (const f of files) {
    const abs = join(SUBMODULE_DIR, f)
    const count = readFileSync(abs, 'utf8')
      .split('\n')
      .filter((l) => HUNK_RE.test(l)).length
    if (count > 0) out[f] = count
  }
  return out
}

/** Diff the live census against the allowlist. Returns an array of violations. */
function diffBudget(actual, allow) {
  const violations = []
  const keys = new Set([...Object.keys(actual), ...Object.keys(allow)])
  for (const k of [...keys].sort()) {
    const a = actual[k]
    const b = allow[k]
    if (a === undefined) {
      violations.push(`REMOVED guarded file (was ${b}): ${k}`)
    } else if (b === undefined) {
      violations.push(`NEW guarded file (+${a}): ${k}`)
    } else if (a !== b) {
      violations.push(`hunk count changed ${b} -> ${a}: ${k}`)
    }
  }
  return violations
}

/** Assert the gitlink commit is on the submodule's origin branch. */
function checkGitlinkOnBranch() {
  // Configured submodule branch (fall back to the checked-out branch).
  let branch
  try {
    branch = git(
      ['config', '--file', '.gitmodules', `submodule.${SUBMODULE}.branch`],
      REPO_ROOT,
    ).trim()
  } catch {
    branch = ''
  }
  if (!branch) {
    branch = git(
      ['-C', SUBMODULE, 'rev-parse', '--abbrev-ref', 'HEAD'],
      REPO_ROOT,
    ).trim()
  }

  // The gitlink commit recorded in the superproject tree.
  const lsTree = git(['ls-tree', 'HEAD', SUBMODULE], REPO_ROOT).trim()
  const m = /^\d+ commit ([0-9a-f]{40})\t/.exec(lsTree)
  if (!m) {
    return [`could not read superproject gitlink commit for ${SUBMODULE}`]
  }
  const gitlink = m[1]

  const remoteBranches = git(
    ['-C', SUBMODULE, 'branch', '-r', '--contains', gitlink],
    REPO_ROOT,
  )
    .split('\n')
    .map((s) => s.trim().replace(/^\* /, ''))
    .filter(Boolean)

  const wanted = `origin/${branch}`
  if (!remoteBranches.includes(wanted)) {
    return [
      `gitlink commit ${gitlink.slice(0, 12)} is not on ${wanted} ` +
        `(contained by: ${remoteBranches.join(', ') || 'none'})`,
    ]
  }
  return []
}

function main() {
  if (!existsSync(SUBMODULE_DIR)) {
    console.error(`native-budget-check: submodule missing at ${SUBMODULE_DIR}`)
    process.exit(2)
  }

  const actual = census()
  const total = Object.values(actual).reduce((a, b) => a + b, 0)

  if (process.argv.includes('--snapshot')) {
    // Regenerate the allowlist from current reality (bootstrap / intentional bump).
    process.stdout.write(JSON.stringify(actual, null, 2) + '\n')
    return
  }

  if (!existsSync(BUDGET_FILE)) {
    console.error(
      `native-budget-check: missing ${BUDGET_FILE}; ` +
        `bootstrap it with: node scripts/native-budget-check.mjs --snapshot > native-hunk-budget.json`,
    )
    process.exit(2)
  }
  const allow = JSON.parse(readFileSync(BUDGET_FILE, 'utf8'))
  const allowTotal = Object.values(allow).reduce((a, b) => a + b, 0)

  const violations = [
    ...diffBudget(actual, allow),
    ...checkGitlinkOnBranch(),
  ]

  console.log(
    `native-budget-check: ${Object.keys(actual).length} guarded files, ` +
      `${total} guarded hunks (budget ${allowTotal}).`,
  )
  if (violations.length) {
    console.error(`\n${violations.length} violation(s):`)
    for (const v of violations) console.error(`  - ${v}`)
    console.error(
      `\nIf a change is intentional, re-snapshot the budget:\n` +
        `  node scripts/native-budget-check.mjs --snapshot > native-hunk-budget.json`,
    )
    process.exit(1)
  }
  console.log('OK: native hunk budget respected; gitlink on origin branch.')
}

main()
