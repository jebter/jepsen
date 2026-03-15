#!/usr/bin/env python3

import argparse
import json
from pathlib import Path


CATALOG_PATH = Path(__file__).resolve().parent / "scripts" / "mview_case_catalog.json"
ALLOWED_STATUSES = {"active", "manual_only", "deferred"}
ALLOWED_TIERS = {"smoke", "full", "manual"}
WORKLOAD_ORDER = ["mv-stateful", "mv-lifecycle", "mv-autosched", "mv-autosched-time"]
FULL_SINGLE_FAULT_WORKLOADS = WORKLOAD_ORDER[:3]
SINGLE_FAULT_NEMESIS_ORDER = [
    "none",
    "kill-pd",
    "kill-kv",
    "kill-db",
    "stop-pd",
    "stop-kv",
    "stop-db",
    "pause-pd",
    "pause-kv",
    "pause-db",
    "partition",
    "shuffle-leader",
    "shuffle-region",
    "random-merge",
]
BRANCH_VALIDATION_ORDER = [
    ("mv-stateful", "none"),
    ("mv-stateful", "kill-db"),
    ("mv-stateful", "pause-db"),
    ("mv-stateful", "partition"),
    ("mv-lifecycle", "none"),
    ("mv-lifecycle", "kill-db"),
    ("mv-lifecycle", "pause-db"),
    ("mv-lifecycle", "partition"),
    ("mv-autosched", "none"),
    ("mv-autosched", "kill-db"),
    ("mv-autosched", "partition"),
]


def load_catalog(path=CATALOG_PATH):
    payload = json.loads(Path(path).read_text())
    cases = payload.get("cases")
    if not isinstance(cases, list):
        raise ValueError("catalog must contain a top-level cases list")
    validate_catalog(cases)
    return cases


def validate_catalog(cases):
    seen = set()
    for case in cases:
        required = {"workload", "nemesis", "tier", "status", "time_limit", "reason"}
        missing = sorted(required.difference(case))
        if missing:
            raise ValueError(f"case is missing keys: {missing}")
        pair = (case["workload"], case["nemesis"])
        if pair in seen:
            raise ValueError(f"duplicate case entry for {pair}")
        seen.add(pair)
        if case["status"] not in ALLOWED_STATUSES:
            raise ValueError(f"unsupported status for {pair}: {case['status']}")
        if case["tier"] not in ALLOWED_TIERS:
            raise ValueError(f"unsupported tier for {pair}: {case['tier']}")
        if not isinstance(case["time_limit"], int) or case["time_limit"] <= 0:
            raise ValueError(f"time_limit must be a positive integer for {pair}")
        if not isinstance(case["reason"], str) or not case["reason"].strip():
            raise ValueError(f"reason must be non-empty for {pair}")


def _workload_index(workload):
    return WORKLOAD_ORDER.index(workload)


def _nemesis_index(nemesis):
    return SINGLE_FAULT_NEMESIS_ORDER.index(nemesis)


def _case_lookup(cases):
    return {(case["workload"], case["nemesis"]): case for case in cases}


def _filter_cases_by_workload(cases, workload):
    if workload is None:
        return cases
    filtered = [case for case in cases if case["workload"] == workload]
    if not filtered:
        raise ValueError(f"workload {workload!r} is not present in the selected suite")
    return filtered


def cases_for_suite(suite, cases=None, workload=None):
    cases = load_catalog() if cases is None else cases
    lookup = _case_lookup(cases)
    if suite == "branch-validation":
        branch_cases = [lookup[pair] for pair in BRANCH_VALIDATION_ORDER]
        extras = [
            (case["workload"], case["nemesis"])
            for case in cases
            if case["status"] == "active" and case["tier"] == "smoke"
            and (case["workload"], case["nemesis"]) not in BRANCH_VALIDATION_ORDER
        ]
        if extras:
            raise ValueError(f"unexpected smoke cases outside branch-validation: {extras}")
        return _filter_cases_by_workload(branch_cases, workload)
    if suite == "full-single-fault":
        ordered = sorted(
            [
                case for case in cases
                if case["status"] == "active" and case["workload"] in FULL_SINGLE_FAULT_WORKLOADS
            ],
            key=lambda case: (_workload_index(case["workload"]), _nemesis_index(case["nemesis"])),
        )
        return _filter_cases_by_workload(ordered, workload)
    if suite == "manual-only":
        return _filter_cases_by_workload([
            case for case in cases
            if case["status"] == "manual_only"
        ], workload)
    raise ValueError(f"unsupported suite: {suite}")


def active_mview_workloads(cases=None):
    cases = cases_for_suite("full-single-fault", cases)
    return [
        workload for workload in FULL_SINGLE_FAULT_WORKLOADS
        if any(case["workload"] == workload for case in cases)
    ]


def active_mview_cases(cases=None):
    return cases_for_suite("full-single-fault", cases)


def manual_only_cases(cases=None):
    return cases_for_suite("manual-only", cases)


def explicit_workloads(cases=None):
    cases = load_catalog() if cases is None else cases
    ordered = []
    for workload in WORKLOAD_ORDER:
        if any(case["workload"] == workload for case in cases):
            ordered.append(workload)
    return ordered


def emit_suite_cases(suite, workload=None):
    for case in cases_for_suite(suite, workload=workload):
        print(f"{case['workload']}\t{case['nemesis']}\t{case['time_limit']}")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    suite_cases = subparsers.add_parser("suite-cases", help="emit suite cases as tab-separated workload, nemesis, time_limit")
    suite_cases.add_argument("suite", choices=["branch-validation", "full-single-fault", "manual-only"])
    suite_cases.add_argument("--workload", choices=WORKLOAD_ORDER, default=None)

    args = parser.parse_args()

    if args.command == "suite-cases":
        emit_suite_cases(args.suite, workload=args.workload)


if __name__ == "__main__":
    main()
