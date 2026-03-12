# MView workloads and branch-build guide

This file is the handoff guide for TiDB Jepsen MView work. It explains which workload owns which problem, how branch builds enter Jepsen, and what a build manifest must contain so other threads can reproduce failures.

Use `doc/mview-review-checklist.md` when doing code review or design review on the MView Jepsen stack.

## Status

- `mv-stateful` is the semantic workload.
- `mv-lifecycle` is the lifecycle rebuild workload.
- `mv-autosched` is the background scheduling and long-stability workload.

## Workload split

### `mv-stateful`

Use `mv-stateful` when the question is: after an explicit fast synchronous refresh, does the materialized view equal the reference model?

Scope:

- explicit write ops on the base table
- explicit `refresh-row`
- explicit `refresh-agg`
- explicit `purge`
- immediate validation against base-table projections

Primary bugs it should catch:

- silent loss
- duplicate consume
- stale row projection
- stale aggregate projection
- delete residue
- refresh materializing an incorrect state

### `mv-lifecycle`

Use `mv-lifecycle` when the question is: after dropping and recreating row views, aggregate views, or the whole MLog + MV chain under live writes and faults, can explicit refresh rebuild the correct state from the current base table?

Scope:

- active writes continue throughout lifecycle phases
- explicit `drop-row-view` / `create-row-view`
- explicit `drop-agg-view` / `create-agg-view`
- explicit `drop-mlog` / `create-mlog` after both views are dropped
- explicit refresh validation after each recreate boundary

Primary bugs it should catch:

- recreate succeeding syntactically but leaving the wrong object set behind
- row view rebuild materializing a stale or incomplete projection
- aggregate view rebuild materializing a stale or incomplete projection
- full-chain rebuild missing writes that happened while the old objects were absent
- lifecycle phase order leaving an unusable chain after recovery

### `mv-autosched`

Use `mv-autosched` when the question is: after faults and after writes stop, do auto refresh and auto purge converge without manual rescue?

Scope:

- active phase keeps writing the base table
- row MV refresh is scheduled every 5 seconds
- agg MV refresh is scheduled every 7 seconds
- purge is scheduled every 11 seconds
- quiet phase performs snapshots only; it never issues manual refresh or manual purge

Current checker contract:

- quiet phase must produce at least one converged snapshot
- quiet phase must produce two consecutive equal snapshots before declaring stable
- if the MLog backing table is discoverable, the checker also reports whether purge visibly progressed
- if the MLog backing table is not discoverable in SQL metadata, purge evidence is reported as `unknown` rather than failing the whole test only because telemetry is unavailable

### `mv-autosched-time`

Use `mv-autosched-time` as the phase-2 skeleton for schedule-time anomalies.

Current checker contract:

- reuse `mv-autosched` data path and quiet-phase convergence checks
- emit an extra time-analysis artifact under `mv-autosched-time/`
- capture best-effort `SHOW CREATE` metadata for row refresh / agg refresh / log purge schedules; fallback `SHOW CREATE` results that omit `START WITH / NEXT` are warning-only unless native materialized-view DDL loses the clause
- require at least one real skew injection (`bump-clock` or `strobe-clock`) when `clock-skew` is requested
- require a successful `reset-clock` before the quiet-phase oracle is considered valid
- fail when post-reset convergence or post-reset stability exceeds the configured quiet-phase budgets
- fail when DB wall clock still diverges materially from the client after reset
- warn when quiet-phase churn is higher than expected, to flag possible duplicate scheduling / `next_time` anomalies

Current limitation:

- `clock-skew` is enabled for manual runs, but it is not yet part of the default gate suite
- the checker now uses best-effort `SHOW CREATE` metadata as a direct schedule signal, but it still does not read a first-class `next_time` system table from TiDB

## Branch-build input model

During feature development, Jepsen should consume a specific branch build directly instead of waiting for a public release package.

Inputs:

- `--tarball-url`: the main TiDB branch build package
- `--binary-urls`: optional component overrides, applied after the tarball is installed

Code pointers:

- CLI flags: `src/tidb/core.clj`
- install order: `src/tidb/db.clj`

Recommended rule:

- if you only have a local file, put it on a temporary HTTP file server first
- do not pass local filesystem paths directly as `--tarball-url` or `--binary-urls`

## Build manifest

A build manifest is the minimal reproducibility record that pins the exact build Jepsen consumed.

Why it exists:

- branch builds are ephemeral
- the same branch name may point to a different commit later
- binary overrides can make two runs with the same tarball behave differently
- without a manifest, a failure is hard to reproduce in a later thread

Minimum fields:

- `branch`: source branch name used to build the package
- `commit_sha`: exact git commit of the build
- `tarball_url`: URL passed to `--tarball-url`
- `binary_urls`: URLs passed to `--binary-urls`
- `feature_flags`: session or cluster flags required by the feature
- `build_time`: build timestamp in ISO 8601

Recommended extra fields:

- `workload`: `mv-stateful`, `mv-lifecycle`, or `mv-autosched`
- `nemesis`: nemesis selection used in the run
- `repro_command`: exact Jepsen command line
- `notes`: short free-form notes for branch-specific caveats

Example:

```json
{
  "branch": "feature/mview-stage1",
  "commit_sha": "0123456789abcdef0123456789abcdef01234567",
  "tarball_url": "https://files.example.com/tidb-feature-mview-stage1.tar.gz",
  "binary_urls": [
    "https://files.example.com/tidb-server-override.tar.gz"
  ],
  "feature_flags": {
    "tidb_enable_materialized_view": true
  },
  "build_time": "2026-03-11T10:20:30Z",
  "workload": "mv-stateful",
  "nemesis": "kill-db",
  "repro_command": "lein run test --workload mv-stateful --nemesis kill-db --time-limit 600 --test-count 1 --concurrency 2n --tarball-url https://files.example.com/tidb-feature-mview-stage1.tar.gz"
}
```

## Suggested execution order

1. `mv-stateful` with `none`
2. `mv-stateful` with `kill-db`
3. `mv-stateful` with `pause-db`
4. `mv-stateful` with `partition`
5. `mv-lifecycle` with `none`
6. `mv-lifecycle` with `kill-db`
7. `mv-lifecycle` with `pause-db`
8. `mv-lifecycle` with `partition`
9. `mv-autosched` with `none`
10. `mv-autosched` with `kill-db`
11. `mv-autosched` with `partition`

Equivalent helper commands:

```bash
scripts/mview_branch_validation.sh <tarball-url>
TIME_LIMIT=21600 scripts/mview_autosched_longrun.sh <tarball-url>
```

## Output checklist

Each run should retain at least:

- the build manifest
- Jepsen history
- checker summary
- first failing op
- final row diff
- final aggregate diff
- reproduction command

Auto-emitted artifacts:

- `build/manifest.edn`
- `build/manifest.json`
- workload-specific JSON mirrors for the main MV artifacts, so scripts do not have to parse EDN first
- `mv-stateful/final-row-check.edn`
- `mv-stateful/final-agg-check.edn`
- `mv-stateful/recent-refresh-purge.edn`
- `mv-stateful/first-failure.edn`
- `mv-lifecycle/final-row-check.edn`
- `mv-lifecycle/final-agg-check.edn`
- `mv-lifecycle/lifecycle-history.edn`
- `mv-lifecycle/recent-refresh-purge.edn`
- `mv-lifecycle/first-failure.edn`
- `mv-autosched/snapshots.edn` for `mv-autosched`
- `mv-autosched-time/analysis.edn`
- `mv-autosched-time/snapshots.edn`
- `mv-autosched-time/analysis.json` and `mv-autosched-time/snapshots.json`
- workload summaries under `*/summary.edn` and `*/summary.json`
- `scripts/mview_store_report.py <store-dir>` for a compact human/JSON handoff summary
- `scripts/mview_run_and_report.sh <workload> <tarball-url> [binary-urls]` to run one workload and materialize `mview-report.txt/json` under the resulting store directory
- `scripts/mview_suite_run_and_report.sh <suite> <tarball-url> [binary-urls]` to run `branch-validation` or `autosched-longrun` and aggregate all new store dirs into one suite report

Helper scripts:

- `scripts/mview_branch_validation.sh` runs the recommended branch gate matrix
- `scripts/mview_autosched_longrun.sh` runs the recommended autosched long-run matrix
- `scripts/mview_autosched_time.sh` runs the experimental phase-2 skeleton

## Next work

- harden `mv-autosched-time` from experimental analysis into a release-gate workload
- consider stricter purge evidence once the MLog backing table or scheduler telemetry is queryable in a stable way
