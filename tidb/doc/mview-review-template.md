# MView review template

Use this template when reviewing changes in the TiDB Jepsen MView stack. It is intentionally short so another thread can copy it directly and fill in findings.

## Scope

- Reviewed files:
- Review goal:
- Out of scope:

## 1. Contract vs implementation

Check:

- Does README / `doc/mview-workloads.md` match the actual behavior?
- Do checker severities match the documented contract?
- Does the build-manifest contract match the real emitted artifacts?

Notes:

- 

## 2. Registration and execution closure

Check:

- Is the workload registered in `src/tidb/core.clj`?
- If `test-all` should support it, is it present in the matrix source?
- If `test-all` should not support it, does the CLI reject it?
- Do wrapper scripts and report scripts recognize the workload end to end?

Notes:

- 

## 3. Fallback and telemetry boundaries

Check:

- Are fallback SQL paths weaker than the primary path?
- Can missing metadata be confused with a product correctness failure?
- Are best-effort signals warning-only unless a stronger source exists?

Notes:

- 

## 4. Oracle strength

Check:

- Can the workload pass while the claimed invariant is still broken?
- Can telemetry gaps create deterministic false failures?
- Are warnings vs hard failures assigned at the right boundary?

Notes:

- 

## 5. Reproducibility and shell boundaries

Check:

- Are branch-build fields fully pinned in the manifest?
- Is `repro_command` copy/paste-safe for quotes, spaces, JSON, and URLs?
- Do wrapper scripts safely pass tarball URLs, binary URLs, and feature flags?
- If a remote `JAR_URL` is involved, was `bash scripts/check_jar_compat.sh <standalone-jar>` run before upload?

Notes:

- 

## 6. Store and report pipeline

Check:

- Does store discovery return only raw Jepsen run directories?
- Do suite outputs stay out of raw-store aggregation?
- Does aggregation reject invalid inputs instead of folding them into counters?

Notes:

- 

## 7. Workload-specific prompts

### `mv-stateful`

Check:

- Does explicit refresh validate against the reference model immediately?
- Are ambiguous writes resolved by read-back before being left indeterminate?
- Are final row / agg diffs and recent refresh-purge history preserved?

Notes:

- 

### `mv-lifecycle`

Check:

- Do create/drop phases verify artifact presence after each lifecycle transition?
- Does the workload rebuild row views, aggregate views, and the full MLog chain in a deterministic order?
- Do rebuilt views validate against the reference model immediately after explicit refresh?
- Are lifecycle history, final row / agg diffs, and first failure preserved?

Notes:

- 

### `mv-autosched`

Check:

- Does quiet phase avoid manual refresh or purge rescue?
- Does convergence require at least one converged snapshot?
- Does stability require two consecutive equal converged snapshots?
- Does purge evidence distinguish `unknown` from real non-progress?

Notes:

- 

### `mv-autosched-time`

Check:

- Is `clock-skew` only required when requested?
- Are skew injection and reset both observed?
- Are post-reset convergence / stability budgets enforced?
- Is best-effort schedule metadata warning-only unless native MV metadata proves a regression?

Notes:

- 

## Negative cases to probe quickly

- branch name contains `'`
- feature flags contain JSON with quotes or spaces
- tarball URL contains query parameters
- aggregator receives an empty directory
- aggregator receives a suite output directory
- CLI accepts a workload that the matrix generator cannot expand
- fallback metadata is present but weaker than native metadata

## Minimal validation

```bash
cd tidb
LEIN_HOME=/tmp/.lein /tmp/lein test
python3 -m py_compile run_jepsen.py scripts/mview_report_common.py scripts/mview_store_report.py scripts/mview_suite_report.py
bash -n scripts/mview_common.sh scripts/mview_run_and_report.sh scripts/mview_suite_run_and_report.sh
git diff --check
bash scripts/check_jar_compat.sh <standalone-jar>
```

## Findings

### Finding N

- File:
- Risk:
- Why it breaks:
- Suggested boundary fix:
