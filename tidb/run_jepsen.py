#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import shlex
import sys
import subprocess
from collections import OrderedDict

import mview_case_catalog

DEFAULT_BINARY_URLS = (
    "tidb:https://fileserver.pingcap.net/download/builds/hotfix/tidb/"
    "v8.5.4-20260409-967ae37/10274/tidb-patch-linux-amd64.tar.gz,"
    "tikv:https://fileserver.pingcap.net/download/builds/hotfix/tikv/"
    "v8.5.4-20260316-c69cb9b/10004/tikv-patch-linux-amd64.tar.gz,"
    "pd:https://fileserver.pingcap.net/download/builds/hotfix/pd/"
    "v8.5.4-20260409-39dcf2c/10276/pd-patch-linux-amd64.tar.gz"
)


def shell_quote(value):
    return shlex.quote(str(value))


def build_extra_args(binary_urls, build_branch, build_commit_sha, build_time, feature_flags, build_notes):
    parts = []
    if binary_urls:
        parts.append(" --binary-urls=" + shell_quote(binary_urls))
    if build_branch:
        parts.append(" --build-branch=" + shell_quote(build_branch))
    if build_commit_sha:
        parts.append(" --build-commit-sha=" + shell_quote(build_commit_sha))
    if build_time:
        parts.append(" --build-time=" + shell_quote(build_time))
    if feature_flags:
        parts.append(" --feature-flags=" + shell_quote(feature_flags))
    if build_notes:
        parts.append(" --build-notes=" + shell_quote(build_notes))
    return "".join(parts)


def docker_exec_bash(inner_command):
    cmd = ["docker", "exec", "jepsen-control", "bash", "-lc", inner_command]
    print(cmd)
    return subprocess.run(cmd, stdout=subprocess.PIPE)


def all_nemesis():
    process_faults = ["kill-pd", "kill-kv", "kill-db", "pause-pd", "pause-kv", "pause-db"]
    network_faults = ["partition"]
    schedule_faults = ["shuffle-leader", "shuffle-region", "random-merge"]
    # clock_faults = "clock-skew"

    nemesis = ["none"]
    nemesis.extend(process_faults)
    nemesis.extend(network_faults)
    nemesis.extend(schedule_faults)
    # for n in schedule_faults:
    #     for pf in process_faults:
    #         nemesis.append(n+","+pf)
    #     for nf in network_faults:
    #         nemesis.append(n+","+nf)
    # nemesis.append("kill-pd,kill-db,pause-pd,kill-kv,shuffle-leader,partition,"
    #               "shuffle-region,pause-kv,pause-db,random-merge")

    return nemesis


def non_mview_workload_options():
    return {
        "append": ["",
                   "--predicate-read=true",
                   "--read-lock=update --predicate-read=true",
                   "--read-lock=update --predicate-read=false"],
        # "bank": ["--update-in-place=true", "--update-in-place=false",
        #          "--read-lock=update --update-in-place=true",
        #          "--read-lock=update --update-in-place=false"],
        "bank-multitable": ["",
                            "--update-in-place=true",
                            "--read-lock=update --update-in-place=true",
                            "--read-lock=update --update-in-place=false"],
        # "long-fork": ["--use-index=true", "--use-index=false"],
        # "monotonic": ["--use-index=true", "--use-index=false"],
        "register": ["",
                     "--use-index=true",
                     "--read-lock=update --use-index=true",
                     "--read-lock=update --use-index=false"],
        "set-cas": ["", "--read-lock=update"],
        "set": [],
        # "sequential": [],
        "table": []
    }


def non_mview_workload_options_for_pessimistic_txn():
    return {
        "bank": ["--read-lock=update"],
        "bank-multitable": ["--read-lock=update --update-in-place=true",
                            "--read-lock=update --update-in-place=false"],
        "register": ["--read-lock=update --use-index=true",
                     "--read-lock=update --use-index=false"],
        "set-cas": ["--read-lock=update"],
        "append": ["--read-lock=update"]
    }


def workload_options_for_mixed_txn():
    # I'm not sure which tests can be passed, so use pessimistic transaction tests first.
    return workload_options_for_pessimistic_txn()


def mview_workload_options():
    return OrderedDict((workload, [""]) for workload in mview_case_catalog.active_mview_workloads())


def workload_options():
    workloads = OrderedDict(non_mview_workload_options())
    workloads.update(mview_workload_options())
    return workloads


def workload_options_for_pessimistic_txn():
    workloads = OrderedDict(non_mview_workload_options_for_pessimistic_txn())
    workloads.update(mview_workload_options())
    return workloads


def mview_cases_by_workload():
    cases = OrderedDict((workload, []) for workload in mview_case_catalog.active_mview_workloads())
    for case in mview_case_catalog.active_mview_cases():
        cases.setdefault(case["workload"], []).append(case)
    return cases


def build_test_command(workload, option, nemesis, time_limit, version, tarball, txn_mode, follower_c, extra_build):
    return (
        "lein run test --workload=" + shell_quote(workload) +
        " --time-limit=" + str(time_limit) +
        " --concurrency 2n" +
        " --auto-retry=default --auto-retry-limit=default" +
        " --version=" + shell_quote(version) +
        " --tarball-url=" + shell_quote(tarball) + extra_build +
        " --nemesis=" + shell_quote(nemesis) + " " + option +
        " --ssh-private-key /root/.ssh/id_rsa" +
        " --txn-mode=" + shell_quote(txn_mode) + follower_c
    )


def gen_tests(version, tarball, time_limit, txn_mode, follower_read, binary_urls="", build_branch="", build_commit_sha="", build_time="", feature_flags="", build_notes=""):
    nemesis = all_nemesis()

    workloads = workload_options()
    if txn_mode == "pessimistic":
        workloads = workload_options_for_pessimistic_txn()
    elif txn_mode == "mixed":
        workloads = workload_options_for_mixed_txn()

    follower_c = ""
    if follower_read:
        follower_c = " --follower-read=true"

    extra_build = build_extra_args(binary_urls, build_branch, build_commit_sha, build_time, feature_flags, build_notes)

    tests = []
    mview_cases = mview_cases_by_workload()
    for w in workloads:
        for option in workloads[w]:
            if w in mview_cases:
                for case in mview_cases[w]:
                    tests.append(build_test_command(
                        workload=w,
                        option=option,
                        nemesis=case["nemesis"],
                        time_limit=case["time_limit"],
                        version=version,
                        tarball=tarball,
                        txn_mode=txn_mode,
                        follower_c=follower_c,
                        extra_build=extra_build,
                    ))
            else:
                for ne in nemesis:
                    tests.append(build_test_command(
                        workload=w,
                        option=option,
                        nemesis=ne,
                        time_limit=time_limit,
                        version=version,
                        tarball=tarball,
                        txn_mode=txn_mode,
                        follower_c=follower_c,
                        extra_build=extra_build,
                    ))

    return tests


def sampling(selection, offset=0, limit=None):
    return selection[offset:(limit + offset if limit is not None else None)]


def run_tests(offset, limit, unique_id, file_server, version, tarball, time_limit, txn_mode, follower_read, binary_urls="", build_branch="", build_commit_sha="", build_time="", feature_flags="", build_notes=""):
    tests = gen_tests(version, tarball, time_limit, txn_mode, follower_read, binary_urls, build_branch, build_commit_sha, build_time, feature_flags, build_notes)
    to_run_tests = sampling(tests, offset, limit)
    # print to_run_tests
    for test in to_run_tests:
        inner_command = "cd /jepsen/tidb/ && timeout --preserve-status 1200 " + test + " > jepsen.log"

        max_retry = 3
        for i in range(max_retry):
            result = docker_exec_bash(inner_command)

            if result.returncode != 0:
                print(result.stderr)
                print(result.stdout)

                if i >= max_retry-1:
                    print("failed to exec jepsen test")
                    update_stores(offset, limit, unique_id, file_server)
                    sys.exit(1)

                print("retry...")
            else:
                break

    update_stores(offset, limit, unique_id, file_server)


def run_special_test(test, store_name, unique_id, file_server, version, tarball, time_limit, txn_mode, binary_urls="", build_branch="", build_commit_sha="", build_time="", feature_flags="", build_notes=""):
    extra_build = build_extra_args(binary_urls, build_branch, build_commit_sha, build_time, feature_flags, build_notes)
    test = (
        "lein run test " + test +
        " --version=" + shell_quote(version) +
        " --tarball-url=" + shell_quote(tarball) + extra_build +
        " --time-limit=" + str(time_limit) +
        " --txn-mode=" + shell_quote(txn_mode) +
        " --auto-retry=default --auto-retry-limit=default" +
        " --concurrency 2n --ssh-private-key /root/.ssh/id_rsa"
    )

    inner_command = "cd /jepsen/tidb/ && timeout --preserve-status 7200 " + test + " > jepsen.log"

    max_retry = 3
    for i in range(max_retry):
        result = docker_exec_bash(inner_command)

        if result.returncode != 0:
            print(result.stderr)
            print(result.stdout)

            if i >= max_retry-1:
                print("failed to exec jepsen test")
                update_special_store(store_name, unique_id, file_server)
                sys.exit(1)

            print("retry...")
        else:
            break

    update_special_store(store_name, unique_id, file_server)


def update_special_store(store_name, unique_id, file_server):
    store_name = store_name + ".tar.gz"
    filepath = "tests/pingcap/jepsen/" + str(unique_id) + "/" + store_name
    inner_command = (
        "cd /jepsen/tidb/ && tar -zcvf " + shell_quote(store_name) +
        " store && curl -F " + shell_quote(filepath + "=@" + store_name) +
        " " + shell_quote(file_server + "/upload")
    )
    result = docker_exec_bash(inner_command)

    if result.returncode != 0:
        print(result.stderr)
        print(result.stdout)
        sys.exit(1)

    print(file_server + "/download/tests/pingcap/jepsen/" + str(unique_id) + "/" + store_name)


def update_stores(offset, limit, unique_id, file_server):
    end = offset+limit
    store_name = "store-" + str(offset) + "-" + str(end) + ".tar.gz"
    filepath = "tests/pingcap/jepsen/" + str(unique_id) + "/" + store_name
    inner_command = (
        "cd /jepsen/tidb/ && tar -zcvf " + shell_quote(store_name) +
        " store && curl -F " + shell_quote(filepath + "=@" + store_name) +
        " " + shell_quote(file_server + "/upload")
    )
    result = docker_exec_bash(inner_command)

    if result.returncode != 0:
        print(result.stderr)
        print(result.stdout)
        sys.exit(1)

    print(file_server + "/download/tests/pingcap/jepsen/" + str(unique_id) + "/" + store_name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--return-count", type=bool, default=False, help="return the numbers of test")
    parser.add_argument("--offset", type=int, default=0, help="offset of tests to run")
    parser.add_argument("--limit", type=int, default=5, help="limit of tests to run")
    parser.add_argument("--unique-id", type=int, default=0, help="unique id")
    parser.add_argument("--file-server", type=str, default="http://fileserver.pingcap.net", help="file server")
    parser.add_argument("--version", type=str, default="latest", help="tidb version")
    parser.add_argument("--tarball", type=str,
                        default="http://172.16.30.25/download/builds/pingcap/release/tidb-latest-linux-amd64.tar.gz",
                        help="tidb tarball url")
    parser.add_argument("--time-limit", type=int, default=120, help="time limit for each jepsen test")
    parser.add_argument(
        "--binary-urls",
        type=str,
        default=DEFAULT_BINARY_URLS,
        help="comma separated binary override urls",
    )
    parser.add_argument("--build-branch", type=str, default="", help="source branch name for manifest")
    parser.add_argument("--build-commit-sha", type=str, default="", help="source commit sha for manifest")
    parser.add_argument("--build-time", type=str, default="", help="build time for manifest")
    parser.add_argument("--feature-flags", type=str, default="", help="feature flags JSON for manifest")
    parser.add_argument("--build-notes", type=str, default="", help="notes for manifest")
    parser.add_argument("--test", type=str, default="", help="special test to run")
    parser.add_argument("--store-name", type=str, default="", help="store name to store")
    parser.add_argument("--txn-mode", type=str, default="optimistic", choices=['optimistic', 'pessimistic', 'mixed'],
                        help="transaction mode to test")
    parser.add_argument("--follower-read", type=bool, default=False, help="whether to open follower read")

    args = parser.parse_args()

    if args.return_count:
        print(len(gen_tests(args.version, args.tarball, args.time_limit, args.txn_mode, args.follower_read, args.binary_urls, args.build_branch, args.build_commit_sha, args.build_time, args.feature_flags, args.build_notes)))
        sys.exit(0)

    if args.test:
        run_special_test(args.test, args.store_name, args.unique_id, args.file_server, args.version, args.tarball, args.time_limit, args.txn_mode, args.binary_urls, args.build_branch, args.build_commit_sha, args.build_time, args.feature_flags, args.build_notes)
        sys.exit(0)

    run_tests(args.offset, args.limit, args.unique_id, args.file_server, args.version, args.tarball, args.time_limit, args.txn_mode, args.follower_read, args.binary_urls, args.build_branch, args.build_commit_sha, args.build_time, args.feature_flags, args.build_notes)


if __name__ == "__main__":
    main()
