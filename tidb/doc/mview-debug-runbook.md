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

## Standard debug order

Use this order unless a user explicitly asks for something narrower.

1. Pin the build inputs.
2. Run the target workload with `nemesis=none`.
3. If `none` fails, stop that workload batch and fix the shared layer first.
4. Run one or two representative non-network faults such as `kill-db`, `kill-pd`, or `kill-kv`.
5. Run the workload's full 14-case single-fault batch only after the baseline and representative faults are stable.
6. Treat `partition` as valid only when the testbed really supports network fault injection.

Examples:

```bash
lein run test --workload mv-stateful --nemesis none --time-limit 300 --test-count 1 --concurrency 2n --tarball-url <tarball-url> --binary-urls <binary-urls>
lein run test --workload mv-lifecycle --nemesis none --time-limit 300 --test-count 1 --concurrency 2n --tarball-url <tarball-url> --binary-urls <binary-urls>
WORKLOAD_FILTER=mv-lifecycle scripts/mview_suite_run_and_report.sh full-single-fault <tarball-url> <binary-urls>
```

## Bridge-backed direct runs

When a direct Jepsen debug run uses a fresh testbed created by `scripts/mview_testbed_bridge.sh`, source the generated bridge env first. The bridge now exports `JEPSEN_NODES` and `JEPSEN_SSH_PRIVATE_KEY`, and `jepsen.cli` will use them automatically when raw CLI flags are omitted.

Rules:

- sourcing `bridge.env.sh` is enough for raw `lein run test` as long as you do not override nodes or SSH key with conflicting flags
- explicit `--nodes` and `--ssh-private-key` still override the bridge env and remain useful for debugging command construction
- the `scripts/mview_run_and_report.sh` and suite wrappers also pick up the bridge node and SSH env automatically

Example:

```bash
source /tmp/mview-testbed-XXXXXX/bridge.env.sh
export JEPSEN_BEST_EFFORT_NET=1
lein run test --workload mv-autosched --nemesis partition --time-limit 60 --test-count 1 --concurrency 10 --tarball-url <tarball-url> --binary-urls <binary-urls>
```

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
