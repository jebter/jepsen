# MView workloads and branch-build guide

This file is the handoff guide for TiDB Jepsen MView work. It explains which workload owns which problem, how branch builds enter Jepsen, and what a build manifest must contain so other threads can reproduce failures.

Use `doc/mview-review-checklist.md` when doing code review or design review on the MView Jepsen stack.
Use `doc/mview-debug-runbook.md` when doing execution triage, branch-build debugging, or `test-plan` / `tcctl run` reproduction work.

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

Current checker contract:

- final row refresh and final aggregate refresh remain the pass/fail oracle
- active-phase row or aggregate failures are retained in the summary separately from final-phase checks
- intermediate row or aggregate failures are retained in the summary, even if later explicit refreshes recover
- the summary reports whether those failures recovered before the final oracle
- unresolved write ambiguity is still surfaced via `strict-valid?` and `first-unresolved-write`, but it does not fail an otherwise correct final refresh oracle by itself

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
- unresolved write ambiguity is still surfaced via `strict-valid?` and `first-unresolved-write`, but it does not fail an otherwise converged autosched run by itself

Execution baseline:

- treat `900` seconds as the default smoke and branch-validation baseline for `mv-autosched`
- shorter local runs are useful for SQL-surface or bridge triage, but they are not strong enough to decide whether the autosched baseline is gate-ready because purge convergence can legitimately need multiple scheduled cycles during quiet phase

### `mv-autosched-time`

Use `mv-autosched-time` as the phase-2 skeleton for schedule-time anomalies.

Current checker contract:

- reuse `mv-autosched` data path and quiet-phase convergence checks
- emit an extra time-analysis artifact under `mv-autosched-time/`
- capture runtime schedule metadata for row refresh / agg refresh / log purge from `mysql.tidb_mview_refresh_info` and `mysql.tidb_mlog_purge_info`; if those system tables are unavailable, fall back to best-effort `SHOW CREATE` / legacy timer metadata and downgrade missing schedule evidence to warnings
- require at least one real skew injection (`bump-clock` or `strobe-clock`) when `clock-skew` is requested
- require a successful `reset-clock` before the quiet-phase oracle is considered valid
- fail when post-reset convergence or post-reset stability exceeds the configured quiet-phase budgets
- only fail `:purge-not-progressing` when quiet-phase runtime metadata says log purge should already be due, or when telemetry cannot explain the stall; if `log-purge.next-time-ms` is still in the future, downgrade that signal to warning `:purge-delayed-by-future-next-time`
- fail when DB wall clock still diverges materially from the client after reset
- warn when quiet-phase churn is higher than expected, to flag possible duplicate scheduling / `next_time` anomalies

Current limitation:

- `clock-skew` is enabled for manual runs, but it is not yet part of the default gate suite
- `manual_only` is an intentional suite-policy state for this experimental workload, not a claim that every direct manual run is currently red
- the checker treats `mysql.tidb_mview_refresh_info` / `mysql.tidb_mlog_purge_info` as the primary schedule signal, but it still keeps compatibility fallbacks for older builds that only expose legacy timer metadata

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

## Suite layering and case catalog

The MView suite matrix now comes from `scripts/mview_case_catalog.json`. Shell wrappers and `run_jepsen.py` should read this catalog instead of maintaining separate hand-written MView case lists.

Each explicit catalog entry carries:

- `workload`
- `nemesis`
- `tier`
- `status`
- `time_limit`
- `reason`

Status meanings:

- `active`: included in the default full single-fault suite
- `manual_only`: runnable manually, but excluded from default suites
- `deferred`: reserved for future or intentionally excluded cases

Current eligibility split:

- `active`: `mv-stateful`, `mv-lifecycle`, and `mv-autosched` crossed with `none`, `kill-pd`, `kill-kv`, `kill-db`, `stop-pd`, `stop-kv`, `stop-db`, `pause-pd`, `pause-kv`, `pause-db`, `partition`, `shuffle-leader`, `shuffle-region`, and `random-merge`
- `manual_only`: `mv-autosched-time` with `clock-skew`
- `deferred`: all combination faults and any future single fault not yet assigned a default time limit in the catalog

Suite definitions:

- `branch-validation`: 11-case smoke suite for wiring, manifest, and report-chain regression checks
- `full-single-fault`: 42-case exhaustive single-fault matrix across the 3 stable workloads
- `autosched-longrun`: long-run `mv-autosched` stability suite

## Suggested execution order

For the practical debug loop, stop rules, artifact triage order, and `test-plan` handoff boundary, use `doc/mview-debug-runbook.md`.

1. Run `branch-validation` first as the fast smoke gate.
2. Run `full-single-fault` second to materialize the complete 42-case debug input.
3. Keep `autosched-longrun` separate from the single-fault matrix.
4. Keep `mv-autosched-time` on the manual path until it is promoted out of experimental status.

Within `full-single-fault`, the default batch order is:

1. `mv-stateful` with all 14 single faults
2. `mv-lifecycle` with all 14 single faults
3. `mv-autosched` with all 14 single faults

Within each workload batch, the nemesis order is:

1. `none`
2. `kill-*`
3. `stop-*`
4. `pause-*`
5. `partition`
6. `shuffle-leader`, `shuffle-region`, `random-merge`

## Debug batch flow

When consuming `full-single-fault`, debug by workload rather than by nemesis:

1. `mv-stateful`
2. `mv-lifecycle`
3. `mv-autosched`

Stopping rules:

- if a workload's `none` case fails, stop that workload batch immediately and fix the shared or workload-local baseline before retrying from `none`
- if two different nemeses in the same workload fail with the same infra signature before the workload checker emits artifacts, stop that workload batch and fix the shared layer first
- if the same infra signature reappears in a second workload, do not continue into the third workload until the shared layer is fixed

Each workload batch should produce:

- a `pass` case list
- a `fail` case list
- failure buckets grouped by signature
- shared issues that cut across workloads
- a clear continue/stop decision for the next batch

Equivalent helper commands:

```bash
scripts/mview_branch_validation.sh <tarball-url>
scripts/mview_full_single_fault.sh <tarball-url>
scripts/mview_suite_run_and_report.sh full-single-fault <tarball-url>
TIME_LIMIT=21600 scripts/mview_autosched_longrun.sh <tarball-url>
scripts/mview_autosched_time.sh <tarball-url>
```

To run a single workload batch from the catalog-driven suites, use `WORKLOAD_FILTER`:

```bash
WORKLOAD_FILTER=mv-stateful scripts/mview_full_single_fault.sh <tarball-url>
WORKLOAD_FILTER=mv-lifecycle scripts/mview_suite_run_and_report.sh full-single-fault <tarball-url>
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
- `mv-stateful/first-row-failure.edn`
- `mv-stateful/first-agg-failure.edn`
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
- `scripts/mview_full_single_fault.sh` runs the exhaustive 42-case single-fault suite
- `scripts/mview_autosched_longrun.sh` runs the recommended autosched long-run matrix
- `scripts/mview_autosched_time.sh` runs the experimental phase-2 skeleton
- `scripts/mview_suite_run_and_report.sh` accepts `branch-validation`, `full-single-fault`, and `autosched-longrun`

## Next work

- harden `mv-autosched-time` from experimental analysis into a release-gate workload
- consider stricter purge evidence once the MLog backing table or scheduler telemetry is queryable in a stable way
