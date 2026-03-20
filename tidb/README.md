# TiDB Jepsen Test

A Clojure library designed to test TiDB, a distributed NewSQL Database.

## What is being tested?

The tests run concurrent operations to some shared data from different
nodes in a TiDB cluster and checks that the operations preserve
the consistency properties defined in each test.  During the tests,
various combinations of nemeses can be added to interfere with the
database operations and exercise the database's consistency protocols.

## Running

To run a single test, try

```
lein run test --workload sets --nemesis kill --time-limit 60 --test-count 1 --concurrency 2n
```

For MV semantic validation on a branch build, try

```
lein run test --workload mv-stateful --nemesis none --time-limit 300 --test-count 1 --concurrency 2n --tarball-url <branch_build_url>
lein run test --workload mv-lifecycle --nemesis kill-db --time-limit 600 --test-count 1 --concurrency 2n --tarball-url <branch_build_url>
lein run test --workload mv-autosched --nemesis kill-db --time-limit 900 --test-count 1 --concurrency 2n --tarball-url <branch_build_url>
```

To run the full suite, use

```
lein run test-all
```

See `lein run test --help` and `lein run test-all --help` for options.

For the MV rollout plan, workload split, and build manifest contract, see `doc/mview-workloads.md`. For execution triage, branch-build SQL-surface probing, suite observability, and the boundary between direct Jepsen debugging and later `test-plan` / `tcctl run`, see `doc/mview-debug-runbook.md`. Each run now emits `build/manifest.edn` and `build/manifest.json`; `mv-stateful`, `mv-lifecycle`, `mv-autosched`, and `mv-autosched-time` also emit JSON mirrors beside their main EDN artifacts under their workload subdirectories. Use `scripts/mview_branch_validation.sh` for the branch gate suite, `scripts/mview_autosched_longrun.sh` for the long-run autosched suite, `scripts/mview_autosched_time.sh` for the experimental phase-2 skeleton, `scripts/mview_store_report.py <store-dir>` to summarize a finished run, and `scripts/mview_run_and_report.sh <workload> <tarball-url> [binary-urls]` to run one workload then emit a report automatically, and `scripts/mview_suite_run_and_report.sh <suite> <tarball-url> [binary-urls]` to run a suite plus aggregate all resulting stores.

#### Workloads

+ **append** Checks for dependency cycles in append/read transactions
+ **bank** concurrent transfers between rows of a shared table
+ **mv-stateful** explicit MV refresh/purge semantic checker over base + row/agg materialized views
+ **mv-lifecycle** drop/recreate lifecycle checker over row views, aggregate views, and the MLog chain
+ **mv-autosched** auto refresh/purge convergence checker over the same base + row/agg materialized views
+ **mv-autosched-time** experimental phase-2 workload for schedule-time anomalies under `clock-skew`
+ **bank-multitable** multi-table variant of the bank test
+ **long-fork** distinguishes between parallel snapshot isolation and standard SI
+ **monotonic** looks for contradictory orders over increment-only registers
+ **register** concurrent atomic updates to a shared register
+ **sequential** looks for serializsble yet non-sequential orders on independent registers
+ **set** concurrent unique appends to a single table
+ **set-cas** appends elements via compare-and-set to a single row
+ **table** checks for a race condition in table creation
+ **txn-cycle** looks for write-read dependency cycles over read-write registers

#### Nemeses

+ **none** no nemesis
+ **clock-skew** randomized clock skew and strobes
+ **kill** kills random processes
+ **kill-db** kill TiDB only
+ **kill-pd** kill PD only
+ **kill-kv** kill TiKV only
+ **partition** network partitions
+ **partition-half** n/2+1 splits
+ **partition-one** isolate single nodes
+ **partition-ring** each node can see separate, intersecting majorities
+ **pause** process pauses
+ **pause-pd** pause only PD
+ **pause-kv** pause only TiKV
+ **pause-db** pause only TiDB
+ **random-merge** merge partitions
+ **restart-kv-without-pd** restart KV nodes without PD available
+ **schedules** use debugging schedules in TiDB
+ **shuffle-leader** randomly reassign TiDB leaders
+ **shuffle-region** randomly reassign TiDB regions

#### Time Limit

Time to run test, usually 60, 180, ... seconds

#### Test Count

Times to run test, should >= 1

#### Concurrency

Number of threads. 2n means "twice the number of nodes", and is a good default.

## License

Copyright © 2017--2019 TiDB, Jepsen, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.


The shell wrappers share `scripts/mview_common.sh` for common build-arg parsing, test invocation, and report generation.
The Python report entrypoints share `scripts/mview_report_common.py` for store and suite aggregation.
