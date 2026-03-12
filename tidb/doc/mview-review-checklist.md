# MView review checklist

This file captures the review workflow used for the TiDB Jepsen MView work so future threads can apply the same checks consistently instead of rediscovering the same classes of bugs.

For a shorter copy-paste version, use `doc/mview-review-template.md`.

## What this checklist is for

Use this checklist when reviewing any of the following:

- `mv-stateful`
- `mv-lifecycle`
- `mv-autosched`
- `mv-autosched-time`
- branch-build manifest / artifact plumbing
- report and suite aggregation helpers
- wrapper scripts that run Jepsen and summarize stores

The goal is not only to find code that fails immediately, but also to find:

- silent no-op paths
- false-positive oracles
- irreproducible failures
- report corruption
- shell / quoting breakage in branch-build workflows

## Review order

Review in this order because it tends to catch the highest-value issues first.

### 1. Contract vs implementation

Start from the declared contract, then compare it to the implementation.

Check:

- README claims vs actual CLI behavior
- workload guide wording vs checker severity
- branch-build input contract vs actual flags and defaults
- artifact promises vs what the checker really writes

Typical bugs found this way:

- doc says best-effort telemetry, code treats it as hard failure
- doc says workload is experimental, code accidentally puts it in default suites
- manifest promises reproducibility, command string is not shell-safe

### 2. Registration vs execution closure

For every new workload or helper, verify the whole path is wired.

Check:

- workload is present in `src/tidb/core.clj`
- if `test-all` should support it, it appears in the workload matrix source
- if `test-all` should not support it, the CLI rejects it clearly
- wrapper scripts invoke supported workloads only
- report scripts recognize the workload artifacts they claim to summarize

Typical bugs found this way:

- CLI accepts a workload but matrix generation produces zero runs
- report code ignores a workload and silently drops its artifacts
- helper scripts default to a nemesis the workload does not actually support

### 3. Fallback and telemetry paths

Search specifically for fallback logic.

Check:

- `try` / `catch` fallback chains
- alternate SQL statements in helper functions
- metadata readers that downgrade from native objects to generic objects
- logic that treats “metadata unavailable” the same as “product incorrect”

Typical bugs found this way:

- fallback `SHOW CREATE TABLE` / `SHOW CREATE VIEW` returns ordinary DDL and is mistaken for schedule corruption
- missing telemetry is turned into a hard product failure instead of a warning
- a fallback path succeeds but produces weaker semantics than the checker assumes

### 4. Oracle strictness

Check whether the checker is too weak or too strong.

Questions to ask:

- Does the checker catch the real bug class the workload claims to target?
- Can telemetry gaps create deterministic false failures?
- Can a workload pass while the claimed invariant is still broken?
- Are warnings and hard failures assigned at the right boundary?

Examples:

- `mv-stateful` should fail on wrong row or aggregate projection immediately after explicit refresh
- `mv-lifecycle` should fail when a create/drop phase reports the wrong artifact state or a rebuilt chain refreshes to the wrong projection
- `mv-autosched` should require convergence without manual rescue
- `mv-autosched-time` should treat best-effort metadata as supporting evidence unless a stronger source is available

### 5. Shell, URL, and JSON boundaries

Search for string concatenation around commands and manifests.

Check:

- shell command construction in Python and shell scripts
- quoting of branch names, timestamps, notes, and JSON feature flags
- URLs containing `&`, `?`, spaces, or quotes
- repro commands copied into manifests or reports

Typical bugs found this way:

- branch names containing `'` break the shell command
- JSON feature flags become malformed when pasted into a repro command
- report or wrapper scripts split comma-joined values incorrectly

### 6. Store layout and report aggregation

Treat report helpers like data pipelines, not just convenience scripts.

Check:

- store discovery only finds real run directories
- suite output directories are not re-consumed as raw stores
- aggregation rejects garbage inputs instead of folding them into valid-looking summaries
- summary counters cannot silently bucket invalid inputs under `None`

Typical bugs found this way:

- `store/suites/*` accidentally treated as raw Jepsen stores
- empty directories counted as valid runs
- report summaries drift from underlying artifacts because one layer ignores missing data

### 7. Reproducibility contract

Review the manifest and artifact contract as a first-class feature.

Required properties:

- build identity is pinned (`branch`, `commit_sha`, `build_time`)
- package identity is pinned (`tarball_url`, `binary_urls`)
- feature switches are pinned (`feature_flags`)
- execution shape is pinned (`workload`, `nemesis`, `time_limit`, `concurrency`)
- repro command is copy/paste-safe

Typical bugs found this way:

- dynamic arguments are missing from the manifest
- repro command renders invalid empty options
- command string is not shell-safe for real branch-build values

## Workload-specific checks

### `mv-stateful`

Review these invariants:

- every key has a single writer
- ambiguous writes are resolved by read-back before being marked indeterminate
- `refresh-row` validates row projection against the reference model
- `refresh-agg` validates aggregate projection against the reference model
- `purge` does not silently weaken later refresh checks
- final artifacts preserve first failure, final row diff, final aggregate diff, and recent refresh/purge history

### `mv-lifecycle`

Review these invariants:

- lifecycle management phases actually execute instead of being silently skipped
- create/drop phases verify artifact presence after the SQL returns
- row-view rebuild validates row projection against the reference model
- agg-view rebuild validates aggregate projection against the reference model
- full-chain rebuild drops views before the MLog and recreates a usable chain afterward
- final artifacts preserve first failure, lifecycle history, final row diff, final aggregate diff, and recent refresh/purge history

### `mv-autosched`

Review these invariants:

- active phase performs writes only
- quiet phase does snapshots only
- no manual refresh or purge is used as a hidden rescue path
- convergence requires at least one converged snapshot
- stability requires two consecutive equal converged snapshots
- purge evidence distinguishes `unknown` telemetry from real non-progress

### `mv-autosched-time`

Review these invariants:

- `clock-skew` is required only when explicitly requested
- at least one skew injection and one reset must be observed when the nemesis is requested
- post-reset convergence and stability budgets are enforced
- residual clock skew is measured after reset
- best-effort metadata remains warning-only unless a stronger source proves a real regression
- the checker does not claim to validate a first-class scheduler table it does not read

## Negative tests to try mentally or with a quick local check

These are small adversarial cases that catch many regressions quickly.

- branch name contains `'`
- `feature_flags` contains JSON with spaces and quotes
- tarball URL contains query parameters
- suite aggregator receives an empty directory
- suite aggregator receives a suite output directory instead of a raw store
- workload is accepted by CLI but absent from the test matrix source
- fallback metadata is present but weaker than native metadata

## Minimal validation after review changes

Run the smallest checks that validate the touched surface area.

Recommended commands:

```bash
cd tidb
LEIN_HOME=/tmp/.lein /tmp/lein test
python3 -m py_compile run_jepsen.py scripts/mview_report_common.py scripts/mview_store_report.py scripts/mview_suite_report.py
bash -n scripts/mview_common.sh scripts/mview_run_and_report.sh scripts/mview_suite_run_and_report.sh
git diff --check
```

Add targeted one-off checks when relevant, for example:

- generate a repro command with quotes and query parameters
- run report aggregation on a temporary empty directory
- run store discovery against a layout containing `store/suites/*`

## How to write review findings

A useful finding should answer three things:

- what concrete path is wrong
- why it is wrong under a realistic scenario
- what boundary should change

Preferred structure:

- scope the issue to one file and a tight line range
- name the real consequence (`runs nothing`, `false hard failure`, `invalid repro command`, `corrupt suite summary`)
- suggest the boundary change, not the exact patch, unless the fix is obvious

## Common bug patterns already seen in this project

- workload registered in `workloads` but absent from the `test-all` matrix source
- best-effort telemetry promoted to a hard oracle
- shell command values concatenated without quoting
- suite output directories re-consumed as raw stores
- invalid store directories silently counted in aggregated reports
- manifest repro commands that cannot be pasted back into a shell

Use these as search seeds before doing a broader pass.
