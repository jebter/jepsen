# MView debug runbook

This file is the execution and triage runbook for TiDB Jepsen MView work.

Use it when debugging:

- `mv-stateful`
- `mv-lifecycle`
- `mv-autosched`
- branch-build binary overrides
- catalog-driven suites such as `branch-validation` and `full-single-fault`
- later `test-plan` / `tcctl run` reproductions for the same workloads

Use `doc/mview-workloads.md` for workload ownership and suite composition. Use this file for the practical debug loop.

## Choose the execution layer first

### Prefer direct Jepsen runs when the suspected bug is in the workload stack

Use local Jepsen execution first when the failure may be in:

- workload SQL generation
- checker severity or artifact writing
- manifest plumbing
- branch-build tarball or binary override behavior
- shell wrapper or suite aggregation logic

Typical examples:

- a new build rejects a previously accepted `REFRESH MATERIALIZED VIEW` syntax
- `summary.edn` or `first-failure.edn` looks wrong
- a suite fails before `test-plan` adds any useful information

### Prefer `test-plan` / `tcctl run` when the workload baseline is already stable

Switch to `test-plan` after direct Jepsen validation proves the workload baseline is good and the remaining question is about:

- plan wiring
- remote one-shot reproduction
- TCMS visibility
- governed or shared execution flow
- remote image or binary propagation

Do not start with `tcctl run` when a direct `none` Jepsen run is still failing in the workload itself. That only adds an extra orchestration layer and slows triage.

## Gate standalone jars before remote `JAR_URL` runs

When a `test-plan` or one-shot reproduction depends on a standalone Jepsen jar uploaded as `JAR_URL`, validate that jar before uploading it or changing the plan.

Run:

```bash
cd tidb
bash scripts/check_jar_compat.sh /path/to/jepsen-standalone.jar
```

This check exists to catch the exact regression class we already hit in MView work:

- class files compiled above Java 8 bytecode level
- root classes that reference post-Java-8 sequenced collection APIs such as `java.util.SequencedCollection`

Stop and fix the jar if this check fails. Do not continue to `tcctl run` just to rediscover a startup crash on the remote JVM.

## Standard debug order

Use this order unless a user explicitly asks for something narrower.

1. Pin the build inputs.
2. Run the target workload with `nemesis=none`.
3. If `none` fails, stop that workload batch and fix the shared layer first.
4. Run one or two representative non-network faults such as `kill-db`, `kill-pd`, or `kill-kv`.
5. Run the workload's full 14-case single-fault batch only after the baseline and representative faults are stable.
6. Treat `partition` as valid only when the testbed really supports network fault injection.

Time-limit guardrails:

- use the catalog `time_limit` when deciding whether a workload baseline is really gate-ready
- `mv-stateful` and `mv-lifecycle` baseline smoke runs use `300` seconds
- `mv-autosched` baseline smoke runs use `900` seconds; shorter ad-hoc runs are triage-only because the checker needs enough quiet-phase time to observe scheduled refresh and purge convergence
- `mv-autosched-time` stays on the manual path even if a direct manual run passes, because promotion is a suite-policy decision rather than a one-off green-run signal

Examples:

```bash
lein run test --workload mv-stateful --nemesis none --time-limit 300 --test-count 1 --concurrency 2n --tarball-url <tarball-url> --binary-urls <binary-urls>
lein run test --workload mv-lifecycle --nemesis none --time-limit 300 --test-count 1 --concurrency 2n --tarball-url <tarball-url> --binary-urls <binary-urls>
lein run test --workload mv-autosched --nemesis none --time-limit 900 --test-count 1 --concurrency 2n --tarball-url <tarball-url> --binary-urls <binary-urls>
WORKLOAD_FILTER=mv-lifecycle scripts/mview_suite_run_and_report.sh full-single-fault <tarball-url> <binary-urls>
MAX_PARALLEL=3 WORKLOAD_FILTER=mv-lifecycle scripts/mview_parallel_suite_run_and_report.sh full-single-fault <tarball-url> <binary-urls>
```

Current default binary override pair for MView lines and debug runs:

- `tidb:https://fileserver.pingcap.net/download/builds/devbuild/10254/tidb-linux-amd64.tar.gz`
- `tikv:https://fileserver.pingcap.net/download/builds/hotfix/tikv/v8.5.4-20260316-c69cb9b/10004/tikv-patch-linux-amd64.tar.gz`

The repo wrappers, raw `lein run test`, and `run_jepsen.py` now default to this pair. Pass `--binary-urls` or `BINARY_URLS` explicitly only when you need to override it.

## Bridge-backed direct runs

When a direct Jepsen debug run uses a fresh testbed created by `scripts/mview_testbed_bridge.sh`, source the generated bridge env first. The bridge now exports `JEPSEN_NODES` and `JEPSEN_SSH_PRIVATE_KEY`, and `jepsen.cli` will use them automatically when raw CLI flags are omitted.

Rules:

- sourcing `bridge.env.sh` is enough for raw `lein run test` as long as you do not override nodes or SSH key with conflicting flags
- explicit `--nodes` and `--ssh-private-key` still override the bridge env and remain useful for debugging command construction
- the `scripts/mview_run_and_report.sh` and suite wrappers also pick up the bridge node and SSH env automatically
- `scripts/mview_parallel_suite_run_and_report.sh` creates a fresh bridge-backed testbed per case, so it is the safe path when you want bounded parallelism instead of multiple cases sharing one sourced bridge env
- for hosted automation that may reap background child processes after `create` returns, prefer `scripts/mview_testbed_bridge.sh exec ... -- <command...>` so the SQL tunnels stay alive in the same session as the Jepsen run

Example:

```bash
source /tmp/mview-testbed-XXXXXX/bridge.env.sh
export JEPSEN_BEST_EFFORT_NET=1
lein run test --workload mv-autosched --nemesis partition --time-limit 60 --test-count 1 --concurrency 10 --tarball-url <tarball-url> --binary-urls <binary-urls>
```

Automation-safe example:

```bash
scripts/mview_testbed_bridge.sh exec /tmp/mview-bridge-XXXXXX -- bash -lc '
  cd /path/to/jepsen/tidb
  export JEPSEN_BEST_EFFORT_NET=1
  scripts/mview_run_and_report.sh mv-lifecycle <tarball-url> <binary-urls>
'
```

For longer `bridge exec` reruns with many environment variables, prefer a tiny launcher script over one huge shell line. This keeps testbed create, SQL tunnels, Jepsen execution, and cleanup in one process, and it avoids shell quoting mistakes that can fail before the run even creates a testbed.

Pattern:

```bash
#!/usr/bin/env bash
set -euo pipefail

workdir=/tmp/mview-debug-<case>-$(date +%Y%m%dT%H%M%S)
run_tag=mv-autosched-time-<case>-$(date +%Y%m%dT%H%M%S)
tarball_url='<tarball-url>'
binary_urls='<binary-urls>'

cd /path/to/jepsen/tidb

scripts/mview_testbed_bridge.sh exec "$workdir" -- env \
  BUILD_BRANCH='<branch>' \
  BUILD_COMMIT_SHA='<commit>' \
  BUILD_NOTES='<notes>' \
  CONCURRENCY='5' \
  TIME_LIMIT='900' \
  TXN_MODE='optimistic' \
  MVIEW_RUN_TAG="$run_tag" \
  MVIEW_SUITE_OUTPUT_DIR="$workdir/suite-output" \
  MVIEW_RUNNER_LOG="$workdir/suite-output/runner.log" \
  MVIEW_STATUS_TSV="$workdir/suite-output/status.tsv" \
  MVIEW_CASE_LOG_DIR="$workdir/suite-output/case-logs" \
  bash -lc 'cd /path/to/jepsen/tidb && scripts/mview_run_and_report.sh mv-autosched-time "$1" "$2"' bash \
  "$tarball_url" \
  "$binary_urls"
```

Read it like this:

- keep the `workdir` unique per run so the created testbed and cleanup are unambiguous
- let `bridge exec` own the whole lifecycle so the SQL tunnels and cleanup stay tied to the same parent process
- pass build metadata and suite-output paths through `env` instead of cramming them into nested shell quoting
- use `status.tsv`, `runner.log`, and the case log under that `workdir` as the first live progress view while the run is still executing

## Always pin the build identity

Before debugging semantics, make sure the run records:

- `--tarball-url`
- `--binary-urls`
- workload
- nemesis
- time limit
- concurrency

The resulting store must include:

- `build/manifest.edn`
- `build/manifest.json`

If the manifest is missing or malformed, fix that first. Otherwise later failures are hard to reproduce across threads.

For remote one-shot or `test-plan` runs that also override `JAR_URL`, treat jar compatibility as part of the pinned build identity. A manifest alone is not enough if the uploaded standalone jar cannot start on the remote JVM.

## Artifact-first triage order

Read artifacts in this order before jumping into the full log:

1. `results.edn`
2. workload `summary.edn`
3. workload `first-failure.edn`
4. workload `recent-refresh-purge.edn`
5. workload-specific history such as `mv-lifecycle/lifecycle-history.edn`
6. `jepsen.log`

Use the summary to answer these questions first:

- Did the run finish?
- Is the workload `valid?`
- Was the first failure a syntax problem, a missing object, or a semantic mismatch?
- Did refresh or purge fail, or only the final comparison?
- Is the failure workload-local or shared across nemeses?

## SQL surface probe before changing assertions

When a new branch build or binary override starts rejecting previously valid SQL, probe the SQL surface directly before editing workload logic.

A reliable pattern is:

1. Port-forward a TiDB node SQL port.
2. Use a real MySQL client against the running cluster.
3. Try the exact statements the workload emits.
4. Record which variants succeed.
5. Update the workload fallback chain.
6. Re-run `nemesis=none`.

Example:

```bash
kubectl -n <ns> port-forward pod/node-0 34000:4000
mysql --protocol tcp -h127.0.0.1 -P34000 -uroot test -e 'REFRESH MATERIALIZED VIEW mv_stateful_row FAST'
mysql --protocol tcp -h127.0.0.1 -P34000 -uroot test -e 'REFRESH MATERIALIZED VIEW mv_stateful_row COMPLETE'
```

This is especially important for `mv-stateful` and `mv-lifecycle`, because both rely on explicit manual refresh and can fail the same way when refresh syntax changes.

## Network fault caveat

Some temporary testbeds do not grant the container the privileges needed for real network shaping.

Rules:

- non-network faults may still be useful with `JEPSEN_BEST_EFFORT_NET=true`
- `partition` is not trustworthy when the environment lacks `NET_ADMIN`
- current direct runs fail fast during setup with a `Real network fault injection is required ... lacks permissions for iptables-based partition faults` error when this capability is missing
- do not treat a `partition` failure from that environment as a product conclusion until the environment is verified

If the environment cannot inject real network faults:

- continue with `none`, `kill-*`, `stop-*`, `pause-*`, `shuffle-*`, and `random-merge` as appropriate
- defer `partition` to a capable testbed

## Fault capability requirements

Do not treat missing container capabilities as a TiDB or workload regression.

Rules:

- `clock-skew` requires `SYS_TIME`; otherwise `/opt/jepsen/bump-time` fails with `settimeofday: Operation not permitted`
- `partition` requires `NET_ADMIN`; otherwise iptables-based fault injection is not trustworthy
- `scripts/mview_common.sh` defaults `JEPSEN_BEST_EFFORT_NET=1`, so `none` and other non-network faults can still run when real network shaping is unavailable
- `tidb/jepsen-testbed.yaml` is the current bridge testbed template and requests both `SYS_TIME` and `NET_ADMIN` for direct debug runs

When a run finishes but bridge cleanup cannot confirm namespace state because of cluster RBAC:

- use `tcctl testbed get <testbed-name>` as the fallback verifier
- if `tcctl` reports `namespace <name> not found`, treat cleanup as confirmed even when `kubectl get namespace` is forbidden
- only keep it as an environment blocker when both kubectl verification and `tcctl testbed get` fail to confirm deletion

## Make suites observable

Do not let a long-running suite write its only status into a short-lived control pod filesystem.

A suite runner should expose:

- `status.tsv`
- `runner.log`
- one log per case
- copied result artifacts per case
- progress lines on container stdout

If the control pod completes and the only status files live under pod-local `/tmp`, postmortem recovery becomes much harder. Prefer writing progress to stdout and keeping case-level snapshots in a durable or easy-to-copy location.

## Move to `test-plan` only after the workload baseline is stable

`test-plan` is the next layer, not the first layer.

Use it after:

- `none` is stable for the target workload
- the relevant single-fault repro is stable locally
- the binary inputs are confirmed
- the failure signature is understood

Then use `test-plan` / `tcctl` for:

- remote one-shot verification
- governed execution records
- plan-level regression checks
- TCMS or artifact URL triage

## Triage an existing TCMS execution with `tcctl`

When a user hands you an existing TCMS execution ID, stay in the CLI path first.

Use this order:

1. Read the plan execution shell:

```bash
tcctl get raw '/api/v1/plan-executions/<plan-exec-id>'
tcctl get raw '/api/v1/plan-executions/<plan-exec-id>/plan'
tcctl get raw '/api/v1/plan-executions/<plan-exec-id>/triage'
```

2. If triage returns case execution IDs, inspect each case execution directly:

```bash
tcctl get raw '/api/v1/case-executions/<case-exec-id>'
```

3. Use the case execution JSON as the source of truth for:

- `case.id` and `case.name`
- `stepName`
- `status` and `output`
- `artifactURLs.main-logs`
- `artifactURLs.archive`

4. `tcctl get raw` is JSON-oriented. Plain-text artifact logs should be fetched from the returned artifact URL directly:

```bash
curl -fsSL '<main-log-url>' | tail -n 200
```

This still keeps the debug path TCMS-first and browser-free: `tcctl` identifies the exact execution, plan, case execution, and artifact endpoints; `curl` only reads the already-resolved log body.

5. If `main.log` only tells you the final anomaly shape, pull the archived analysis artifacts and inspect the structured snapshots:

```bash
curl -fL '<archive-url>' -o /tmp/<case-exec-id>.tgz
tar -tzf /tmp/<case-exec-id>.tgz | rg 'mv-autosched-time|manifest\\.(edn|json)'
tar -xOzf /tmp/<case-exec-id>.tgz '<path-to>/mv-autosched-time/snapshots.json' \
  | jq '[ .[] | {node: .["snapshot-node"], snapshot_at_ms: .["snapshot-at-ms"], db_client_offset_ms: .["db-client-offset-ms"]} ] | .[-10:]'
```

For `mv-autosched-time` with `clock-skew`, the fastest confirmation of a bad reset is:

- the quiet-phase snapshots begin after the final `reset-clock`
- one or more nodes still show roughly the same large `db-client-offset-ms` for the entire quiet window
- the offset remains large enough to explain follow-on anomalies such as no convergence, no stability, and no visible purge progress

When the shape is ambiguous, compare two different signals instead of relying on one:

- the final `:reset-clock` entry in `history.edn`, which reports post-reset host clock offsets from the clock nemesis path
- the first quiet-phase `mv-autosched-time/snapshots.json` entries, which report `db-client-offset-ms` from `CURRENT_TIMESTAMP(6)` on the SQL connection

If a node is still badly skewed in both places, the reset itself likely failed for that node.

If a node looks normal in `:clock-offsets` but still shows a large `db-client-offset-ms`, treat that as evidence that the SQL time source did not recover with the nemesis-visible host clock. In practice that usually means one of:

- the SQL path is not reading the same time source the clock nemesis just reset
- the connection is reaching a different skewed backend than the node label suggests
- the database process or its runtime kept a residual skew even after the host clock recovered

Before deciding that the node label is wrong, resolve what the node label actually points to:

```bash
tcctl get raw '/api/v1/plan-executions/<plan-exec-id>/plan' | jq '.testbed'
sed -n '116,136p' tidb/scripts/mview_testbed_bridge.sh
```

For the Jepsen MView flow in this repo, `tidb/scripts/mview_testbed_bridge.sh` builds node addresses as `node-<idx>.node-peer.<testbed>.svc.cluster.local`, and `tidb.sql/open` connects directly to that node label on port `4000`.

That matters for interpretation:

- a testbed spec can still expose `loadBalancer.containerPort = 4000`, but the Jepsen node label itself is a per-node peer DNS name rather than a single shared external service name
- this makes "all node labels secretly hit the same external LB backend" a weaker hypothesis than it first appears
- if two node labels still report the same skewed SQL wall clock while only one node remains skewed in the final host `:clock-offsets`, prioritize investigating the SQL time source, a local forward behind that node's `:4000`, or process-level residual skew

`8060108` is a useful concrete pattern to remember:

- `2026-03-24 08:49:24 GMT` `strobe-clock` still showed `node-4 ~= +93.836s` while `node-1 ~= -0.163s`
- `2026-03-24 08:50:18 GMT` the final `reset-clock` showed `node-4 ~= -0.092s` but `node-1 ~= -94.021s`
- `2026-03-24 08:50:28` through `08:51:05 GMT` quiet-phase `mv-autosched-time/snapshots.json` still showed both `node-1` and `node-4` at roughly `-93.924s`

That shape is stronger than a generic "reset failed" story:

- `node-1` really did fail to recover at the host clock layer
- `node-4` did recover at the host clock layer, but its SQL `CURRENT_TIMESTAMP(6)` still tracked the same skewed wall clock as `node-1`
- when the pre-reset skewed node and the post-reset skewed node are different, but the post-reset SQL time is identical on both labels, do not classify both nodes as host reset failures
- prefer hypotheses such as "SQL time source is not the nemesis-visible host clock" or "the SQL path for one node label is actually landing on another skewed backend"

If the branch already includes the newer snapshot instrumentation, use it before re-reading raw snapshots by hand:

```bash
tar -xOzf /tmp/<case-exec-id>.tgz '<path-to>/mv-autosched-time/analysis.json' \
  | jq '.["db-identity-summary"]'
```

The newer `mv-autosched-time` snapshots can include `db-identity`, for example:

- `connection-node`
- `connection-target`
- `connection-id`
- `hostname`
- `port`

And the analysis can summarize them as `db-identity-summary`.

Use those fields like this:

- if `snapshot-node = node-4...` but `db-identity.hostname` consistently points at a different backend identity than expected, suspect SQL routing or forwarding first
- if multiple `snapshot-node` values appear under the same `db-identity-summary.multi-node-backends` entry, treat that as direct evidence that different node labels reached the same SQL backend during quiet phase
- if the route target stays distinct but the backend identity is shared, focus on what sits behind `:4000` for that node
- if both route target and backend identity remain node-local, but SQL time still disagrees with the final host `:clock-offsets`, focus on the SQL time source or process-level residual skew rather than label routing

The checker may also emit a `:shared-db-backend` warning when multiple snapshot nodes report the same backend identity during quiet phase. Treat that warning as a routing/identity clue, not as a replacement for the existing hard anomalies.

When you have a fresh direct rerun with the newer `analysis.json`, use it to falsify the "shared backend" hypothesis before digging deeper into raw snapshots again:

```bash
jq '.["db-identity-summary"]' /path/to/mv-autosched-time/analysis.json
jq -r '
  . as $root
  | $root["snapshot-analysis-by-node"]
  | to_entries[]
  | [
      .key,
      ((($root["clock-summary"]["latest-offsets"][.key]) * 1000) | round),
      .value["last-snapshot"]["db-client-offset-ms"],
      (.value["last-snapshot"]["db-client-offset-ms"]
        - ((($root["clock-summary"]["latest-offsets"][.key]) * 1000) | round)),
      .value["quiet-window-ms"]
    ]
  | @tsv
' /path/to/mv-autosched-time/analysis.json
```

Read that pair of outputs like this:

- `distinct-backend-count = 5` together with `multi-node-backends = []` means each snapshot node stayed on its own SQL backend for the whole quiet window
- if the final host offsets are around a few hundred milliseconds and `db-client-offset-ms` is also around a few hundred milliseconds, the rerun did not reproduce the `8060108`-style `94s` host/SQL split
- if the host-vs-SQL delta stays around tens of milliseconds or low hundreds of milliseconds, treat it as ordinary measurement or transport skew, not as evidence of a bad final reset

The `2026-03-25` fresh rerun of `mv-autosched-time + clock-skew` is the reference example:

- `db-identity-summary.distinct-backend-count = 5`
- `db-identity-summary.multi-node-backends = []`
- final host offsets were roughly `193-201ms`
- final SQL `db-client-offset-ms` values were roughly `295-303ms`
- host-vs-SQL deltas stayed around `96-107ms`
- `anomalies = []`, `converged-snapshot-count = 35`, and the quiet window was about `39s`

That rerun matters because it weakens the old `8060108` "shared backend" theory. With fresh per-node backend identity and no large post-reset SQL skew, the old failure looks more like a run-specific time-source or residual-skew issue than a stable node-label routing bug.

When `mv-autosched-time` is invalid and the last snapshots already tell you `row-equal? = true` but `agg-equal? = false`, check the component liveness gap in `runtime-metadata.rows` before jumping to a routing explanation:

```bash
jq '
  .["snapshot-analysis-by-node"]
  | to_entries[0].value["last-snapshot"]["runtime-metadata"]["rows"]
' /path/to/mv-autosched-time/analysis.json
```

Read the three component rows together:

- `row-refresh.last-success-read-tso`
- `agg-refresh.last-success-read-tso`
- `log-purge.last-purged-tso`

Use the relative gap, not just the raw value:

- if `row-refresh` is current but `agg-refresh` and `log-purge` lag far behind, treat that as evidence that the aggregate refresh or purge scheduler is stale even if the metadata rows are still present
- if `row-refresh`, `agg-refresh`, and `log-purge` stay close together, but the snapshots still diverge, focus on snapshot contents or SQL routing instead
- if `agg-refresh` history shows obviously broken timing such as very large or negative `duration-ms`, treat that as a separate scheduler/runtime-history clue rather than a host-vs-SQL clock-skew clue

When reading `recent-history`, remember that the query is ordered by `job_id DESC`, not by `end_time_ms`. For the checker, the practical question is whether the newest history row for that component changes across quiet-phase snapshots.

Use that distinction like this:

- if `duration-ms` looks impossible but the newest `job-id`, `read-tso`, or `end-time-ms` still advances across snapshots, the clock reset may have dirtied timestamps without fully stalling that component
- if the newest `job-id` and the paired runtime row both stay frozen across every quiet-phase snapshot, treat it as real component non-progress even when older history rows show bizarre negative or very large durations
- if purge history rows advance but `log-row-count` never decreases, that still supports `:purge-not-progressing`; history churn alone is not enough

Also compare `next-time-ms` against the snapshot time itself:

- if a component's `next-time-ms` stays pinned to one absolute future timestamp while `snapshot-at-ms` keeps moving toward it, that component is effectively scheduled into the future instead of actively making progress during the quiet window
- if `row-refresh.next-time-ms` keeps stepping forward but `agg-refresh.next-time-ms` stays pinned, expect `row-refresh` to keep advancing while `agg-refresh` freezes
- if `log-purge.next-time-ms` advances and purge jobs keep changing, but `last-purged-tso` stays equal to the stalled `agg-refresh.last-success-read-tso`, treat purge non-progress as a downstream consequence of aggregate-refresh stall

The concrete comparison around `8060108` is useful:

- original `8060108`:
  `row-refresh` stayed ahead, `agg-refresh` lagged by about `1.4e9`, and `log-purge` lagged by about `7.0e10`
- passing rerun on `2026-03-25`:
  `agg-refresh` lag was only `248`, `log-purge` lag was only `629`
- failing rerun4 on `2026-03-25`:
  `agg-refresh` and `log-purge` both lagged `row-refresh` by about `1.31e8`
- failing rerun6c on `2026-03-25`:
  `agg-refresh` and `log-purge` both lagged `row-refresh` by about `1.35e9`

That comparison helps split failures into different buckets:

- the original `8060108` had both large residual SQL skew and strongly stale purge metadata
- rerun4 had no large residual SQL skew and no shared-backend evidence, but `agg-refresh` and `log-purge` still lagged `row-refresh`
- rerun6c again had no shared-backend evidence and no large residual SQL skew, but it added `:purge-not-progressing` on top of the rerun4-style stalled aggregate/purge state
- a clean passing rerun keeps all three components nearly aligned

The `2026-03-25` rerun6c is a useful reinforcement of that split:

- `anomalies = [:no-post-reset-convergence :no-post-reset-stability :purge-not-progressing]`
- `db-identity-summary.distinct-backend-count = 5`
- `db-identity-summary.multi-node-backends = []`
- final host offsets were roughly `-27ms` to `-30ms`
- final SQL `db-client-offset-ms` values were roughly `65-72ms`
- every node's last snapshot still had `row-equal? = true` and `agg-equal? = false`
- `recent-history` again showed broken runtime timing, for example `agg-refresh duration-ms = -39525` and `log-purge duration-ms = -128736 / 128752 / 128753`
- on node-0, the newest `agg-refresh` history row stayed fixed across all 7 quiet snapshots at `job-id = 465151441747312651`, while the newest purge history row advanced but `log-row-count` stayed flat at `8868`
- on node-0, `agg-refresh.next-time-ms` stayed pinned at `1774411940000` across all 7 quiet snapshots while `snapshot-at-ms` moved from `1774411836388` to `1774411875905`; the gap shrank from about `104s` to `64s`, but no new aggregate-refresh run arrived inside that window

That pinned-`next-time-ms` pattern also links rerun6c back to the older samples:

- rerun4 showed the same shape for `agg-refresh.next-time-ms`, with the future gap shrinking from about `105s` to `65s` while row refresh continued and purge only partially caught up
- original `8060108` was more severe: `row-refresh.next-time-ms`, `agg-refresh.next-time-ms`, and `log-purge.next-time-ms` were all pinned in the future during quiet phase, so every component stayed frozen, and the run also kept the separate residual-skew signature

That matters because rerun6c is still not a reproduction of the original `8060108` host/SQL `94s` split. It is better treated as another scheduler/runtime-history failure mode that can coexist with `:purge-not-progressing`, not as fresh evidence for a stable residual-skew or shared-backend theory.

## Smallest verified reproduction for the scheduler future-pin bug

For issue filing and handoff, treat this as the smallest **verified** reproduction so far:

- workload: `mv-autosched-time`
- nemesis: `clock-skew`
- scope: one fresh direct Jepsen run on one fresh bridge-managed testbed
- goal: reproduce the scheduler stall shape without depending on the original `8060108` residual-`94s` host/SQL split

The direct Jepsen path is still the simplest checker-backed repro. As of `2026-03-25`, there is also a verified lower-level SQL-driven repro, but only for the all-node bump/reset variant documented below; the earlier single-node `node-0 +90s / hold 20s / reset` SQL variant still did **not** reproduce the future-pin shape.

Run it like this from `tidb/`:

```bash
ts="$(date +%Y%m%dT%H%M%S)"
workdir="/tmp/mview-autosched-time-repro-$ts"
run_tag="mv-autosched-time-future-pin-$ts"
mkdir -p "$workdir" "$workdir/suite-output"

export TARBALL_URL='<tarball-url>'
export BINARY_URLS='<optional-binary-urls>'
export MVIEW_SUITE_OUTPUT_DIR="$workdir/suite-output"
export MVIEW_RUN_TAG="$run_tag"

./scripts/mview_testbed_bridge.sh exec "$workdir" ./jepsen-testbed.yaml -- \
  ./scripts/mview_autosched_time.sh "$TARBALL_URL" "$BINARY_URLS"
```

This keeps the repro path minimal while still obeying the fresh-testbed rule:

- one workload
- one nemesis
- one run tag
- one fresh testbed created and cleaned up by the bridge wrapper

After the run, locate the artifacts by run tag instead of guessing the store path:

```bash
analysis_json="$(find "$PWD/store" -type f -path "* run-tag $run_tag */mv-autosched-time/analysis.json" -print -quit)"
snapshots_json="$(dirname "$analysis_json")/snapshots.json"
```

The shortest first-pass verdict is:

```bash
jq '{
  anomalies: [.anomalies[]?.kind],
  distinct_backend_count: .["db-identity-summary"]["distinct-backend-count"],
  multi_node_backends: .["db-identity-summary"]["multi-node-backends"],
  latest_offsets: .["snapshot-offset-summary"]["latest-offsets"]
}' "$analysis_json"
```

For the scheduler-stall shape itself, inspect one node's quiet-phase snapshots:

```bash
printf 'snapshot_at_ms\trow_next\tagg_next\tpurge_next\trow_tso\tagg_tso\tpurge_tso\tlog_row_count\n'
jq -r '
  map(select(."snapshot-node" | contains("node-0.")))
  | .[]
  | . as $s
  | ($s."runtime-metadata".rows | map({(.component): .}) | add) as $rt
  | [
      $s."snapshot-at-ms",
      $rt["row-refresh"]."next-time-ms",
      $rt["agg-refresh"]."next-time-ms",
      $rt["log-purge"]."next-time-ms",
      $rt["row-refresh"]."last-success-read-tso",
      $rt["agg-refresh"]."last-success-read-tso",
      $rt["log-purge"]."last-purged-tso",
      $s."log-row-count"
    ] | @tsv
' "$snapshots_json"
```

Treat the run as reproducing the core product bug when all of these hold:

- anomalies include `:no-post-reset-convergence` and `:no-post-reset-stability`
- `distinct-backend-count` matches the node count and `multi-node-backends = []`, so this is not a shared-backend artifact
- `latest_offsets` are only in the tens or low hundreds of milliseconds, so this is not the original `8060108` residual-`94s` skew bucket
- on one node's snapshot table, `row-refresh.next-time-ms` keeps stepping forward while `agg-refresh.next-time-ms` stays pinned to one future timestamp
- `row-refresh.last-success-read-tso` keeps advancing while `agg-refresh.last-success-read-tso` stays frozen

If `log-purge.last-purged-tso` also stays equal to the stale aggregate TSO and `log-row-count` stays flat, that strengthens the same bug as a downstream purge symptom. `:purge-not-progressing` is helpful but not required to establish the core scheduler future-pin failure.

When choosing an external duplicate anchor, the nearest current open issue is `#66843` in `references/known_issues.md`: automatic refresh can stall after a DST fall-back time shift. Use it only as the closest scheduler time-boundary reference, not as an exact duplicate, because the trigger here is Jepsen `clock-skew` / reset rather than a DST transition.

Interpret `manual_only` carefully:

- `manual_only` means excluded from default suites, not a soft-pass or expected red state
- for `mv-autosched-time` with `clock-skew`, hard anomalies such as `:residual-clock-skew`, `:no-post-reset-convergence`, `:no-post-reset-stability`, and `:purge-not-progressing` are real failures under the current checker contract
- only downgrade the result to a suite-policy note when the failure reason is about promotion or catalog eligibility, not when the checker emits hard anomalies

Classify the failure before changing code:

- if the setup log fails before the case starts, treat it as setup, testbed, or plan wiring
- if setup succeeds and the case `main.log` ends in invalid workload analysis, treat it as workload, checker, or product behavior and debug the artifacts in the normal order
- if the failure signature matches a documented environment limitation such as missing `SYS_TIME` or `NET_ADMIN`, classify it as environment capability, not product semantics

## Verified SQL-level future-pin reproduction

The `2026-03-25` bridge-managed SQL repro below is now verified as a lower-level reproduction of the scheduler future-pin bug:

- fresh testbed only, created and cleaned up by `mview_testbed_bridge.sh`
- bootstrap with `mv-stateful + none`
- then apply MLog/MView schema manually over SQL
- then bump **all five nodes** forward by `120s`, hold for `40s`, and reset them back to host wall clock
- sample one node's runtime metadata over an `18 x 5s` post-reset window

Run it from `tidb/` like this:

```bash
ts="$(date +%Y%m%dT%H%M%S)"
workdir="/tmp/mview-sql-clock-repro-$ts"

./scripts/mview_testbed_bridge.sh exec "$workdir" ./jepsen-testbed.yaml -- \
  env \
    PRE_RESET_SAMPLES=3 \
    POST_RESET_SAMPLES=18 \
    SAMPLE_INTERVAL_SECONDS=5 \
    BUMP_SECONDS=120 \
    BUMP_HOLD_SECONDS=40 \
    SAMPLE_NODE_INDEX=0 \
    TARGET_NODE_INDEXES=0,1,2,3,4 \
    ./scripts/mview_sql_clock_repro.sh "$TARBALL_URL" "$BINARY_URLS"
```

The key artifact is:

```bash
summary_json="$workdir/sql-clock-repro/summary.json"
runtime_tsv="$workdir/sql-clock-repro/runtime.tsv"
history_tsv="$workdir/sql-clock-repro/history.tsv"
```

With the current script, treat either of these as a reproduced scheduler future-pin:

- `reproduced_agg_only_future_pin = true`
- `reproduced_all_components_future_pin = true`

The verified `2026-03-25` all-node run produced the stronger second shape:

- `reproduced_all_components_future_pin = true`
- `row_future_gap_start_ms = 120595`, `agg_future_gap_start_ms = 123595`, `purge_future_gap_start_ms = 118595`
- `row_future_gap_end_ms = 24467`, `agg_future_gap_end_ms = 27467`, `purge_future_gap_end_ms = 22467`
- all `post-reset` samples kept one fixed `row-refresh.next-time-ms`, one fixed `agg-refresh.next-time-ms`, and one fixed `log-purge.next-time-ms`
- all `post-reset` samples also kept one fixed `row_tso`, one fixed `agg_tso`, and one fixed `purge_tso`

Read that carefully:

- this is not just `agg-refresh` stalling while `row-refresh` advances
- it is closer to the more severe `8060108` shape, where all components are pinned into the future during the quiet window
- in this SQL repro, the pin lasted for the whole observed post-reset window and then later history resumed only after wall clock caught up to those pinned absolute `next_time` values

That last point matters for wording:

- call this a verified SQL-level reproduction of the **post-reset future-pin bug**
- do not overstate it as a permanent dead stall unless later samples prove the components still do not recover after wall clock reaches the pinned timestamp
- if you need the narrower `rerun4` / `rerun6c` shape where `row-refresh` keeps advancing but `agg-refresh` stays pinned, keep using the direct Jepsen `mv-autosched-time + clock-skew` path above

After the run, confirm cleanup with `tcctl`, not `kubectl`:

```bash
testbed="$(jq -r .name "$workdir/output")"
tcctl testbed get "$testbed"
```

Treat `not found` as successful cleanup for the fresh testbed created by that run.

## When a fresh direct rerun hangs after writing analysis artifacts

Sometimes the workload finishes, writes `history.edn`, `analysis.json`, and related artifacts, but the local wrapper still hangs in tail-end analysis or cleanup.

The common shape is:

- `status.tsv` still contains only the `start` row
- the case log already shows `Run complete, writing` and `Analyzing...`
- the store dir already contains `history.edn` and `mv-autosched-time/analysis.json`
- the semantic verdict is already recoverable from artifacts even if the wrapper has not yet printed its final pass/fail line
- later log-snarf lines stall on `jepsen.core downloading ...`
- one of the final log collection errors may be `:jepsen.control/download-failed` with `session is down`
- the fresh testbed still exists when you run `tcctl testbed get <testbed-name>`

Treat that as a cleanup or wrapper tail problem first. Decide the workload semantic result from `analysis.json`, not from the still-pending wrapper exit.

Use this order:

1. Preserve the evidence first: keep the workdir, case log, and written `analysis.json`.
2. Confirm the hang is tail-only by checking that the case log has reached `Analyzing...` and the store dir already contains the expected analysis artifacts.
3. Stop only the local processes tied to that run tag or testbed, such as the bridge wrapper, `lein`, and SQL port-forward loops.
4. Delete only that run's fresh testbed with `tcctl testbed delete <testbed-name>`.
5. Confirm cleanup with `tcctl testbed get <testbed-name>` and expect `namespace <name> not found`.

If `analysis.json` exists before wrapper exit:

- use `analysis.json` as the source of truth for `valid?`, anomalies, warnings, backend identity, and snapshot offsets
- do not overwrite that semantic conclusion just because the outer wrapper later records `failed exit_code=1`
- if `status.tsv` only gains the final `failed` row after you manually force cleanup, classify that exit code as wrapper/cleanup fallout unless the artifacts themselves show missing or incomplete analysis

If cleanup still fails, report all three items explicitly:

- the fresh testbed name
- the exact `tcctl testbed delete <testbed-name>` command
- the blocker that prevented deletion

Do not mix this bucket with the earlier setup timeout bucket. For example, if a direct rerun fails in `tidb.db install :sync` and times out at `tidb/src/tidb/db.clj:871` before the workload starts, classify that as a setup flake and do a new fresh-testbed rerun instead of reusing the old namespace.

## Failure buckets

When summarizing a run, put it into one of these buckets before editing code:

- environment or privilege limitation
- binary or image propagation
- SQL surface change
- workload or checker bug
- real product behavior mismatch
- plan wiring issue

If two nemeses fail with the same shared signature, stop the batch and fix the shared layer first.

## Minimal handoff after a debug cycle

Capture these fields in the handoff:

- workload
- nemesis
- tarball URL
- binary URLs
- store dir
- `valid?`
- first failure
- whether `none` passes
- whether the failure reproduces under `test-plan` or only under direct Jepsen
