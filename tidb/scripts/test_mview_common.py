import os
import shlex
import signal
import subprocess
import tempfile
import textwrap
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
COMMON = ROOT / "scripts" / "mview_common.sh"
PARALLEL_RUNNER = ROOT / "scripts" / "mview_parallel_suite_run_and_report.sh"


def run_common_helper(cwd, command):
    proc = subprocess.run(
        ["bash", "-lc", f"source {shlex.quote(str(COMMON))}; {command}"],
        cwd=cwd,
        text=True,
        capture_output=True,
    )
    return proc


class MViewCommonShellHelpersTest(unittest.TestCase):
    def test_parallel_runner_inner_command_uses_tidb_root_once(self):
        script_text = PARALLEL_RUNNER.read_text(encoding="utf-8")
        self.assertIn(
            "printf -v command 'cd %q && scripts/mview_run_and_report.sh %q %q'",
            script_text,
        )
        self.assertNotIn(
            "printf -v command 'cd %q/tidb && scripts/mview_run_and_report.sh %q %q'",
            script_text,
        )
        self.assertIn(
            'CREATE_QUOTA_RETRY_LIMIT="${CREATE_QUOTA_RETRY_LIMIT:-90}"',
            script_text,
        )
        self.assertIn(
            'CREATE_QUOTA_RETRY_DELAY_SECONDS="${CREATE_QUOTA_RETRY_DELAY_SECONDS:-10}"',
            script_text,
        )
        self.assertIn(
            "export CREATE_QUOTA_RETRY_LIMIT CREATE_QUOTA_RETRY_DELAY_SECONDS",
            script_text,
        )

    def test_store_dirs_for_run_tag_matches_exact_suffix(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            matching_one = root / "store" / "TiDB nightly mv-stateful run-tag target nemesis none" / "20260322T010101"
            matching_two = root / "store" / "TiDB nightly mv-lifecycle run-tag target nemesis kill-db" / "20260322T020202"
            non_matching = root / "store" / "TiDB nightly mv-stateful run-tag target-extra nemesis pause-db" / "20260322T030303"
            suite_dir = root / "store" / "suites" / "branch-validation"
            matching_one.mkdir(parents=True)
            matching_two.mkdir(parents=True)
            non_matching.mkdir(parents=True)
            suite_dir.mkdir(parents=True)

            proc = run_common_helper(root, "mview_store_dirs_for_run_tag target")

            self.assertEqual(0, proc.returncode, proc.stderr)
            self.assertEqual(
                sorted([str(matching_one.resolve()), str(matching_two.resolve())]),
                [line for line in proc.stdout.splitlines() if line],
            )

    def test_collect_store_dirs_from_status_deduplicates_completed_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            status = root / "status.tsv"
            status.write_text(
                "timestamp\tevent\tworkload\tnemesis\ttime_limit\trun_tag\texit_code\tstore_dirs\tcase_log\ttarball_url\tbinary_urls\n"
                "2026-03-22T10:00:00+0800\tstart\tmv-stateful\tnone\t300\trun-a\t\t\t/tmp/a.log\thttp://example/a\t\n"
                "2026-03-22T10:05:00+0800\tpassed\tmv-stateful\tnone\t300\trun-a\t0\t/tmp/store-a,/tmp/store-b\t/tmp/a.log\thttp://example/a\t\n"
                "2026-03-22T10:10:00+0800\tfailed\tmv-lifecycle\tkill-db\t600\trun-b\t1\t/tmp/store-b,/tmp/store-c\t/tmp/b.log\thttp://example/b\t\n",
                encoding="utf-8",
            )

            proc = run_common_helper(root, f"mview_collect_store_dirs_from_status {shlex.quote(str(status))}")

            self.assertEqual(0, proc.returncode, proc.stderr)
            self.assertEqual(
                ["/tmp/store-a", "/tmp/store-b", "/tmp/store-c"],
                [line for line in proc.stdout.splitlines() if line],
            )

    def test_catalog_helper_honors_env_override(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fake_catalog = root / "fake_catalog.py"
            fake_catalog.write_text("#!/usr/bin/env python3\n", encoding="utf-8")

            proc = run_common_helper(
                root,
                f"MVIEW_CATALOG_HELPER={shlex.quote(str(fake_catalog))} mview_catalog_helper",
            )

            self.assertEqual(0, proc.returncode, proc.stderr)
            self.assertEqual(str(fake_catalog), proc.stdout.strip())


class MViewParallelRunnerAbortCleanupTest(unittest.TestCase):
    def test_sigint_runs_bridge_cleanup_for_active_workdirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            suite_output = root / "suite-output"
            bridge_log = root / "bridge.log"
            catalog = root / "catalog.py"
            bridge = root / "bridge.sh"

            catalog.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env python3
                    import sys

                    rows = [
                        ("mv-fake", "none", "300"),
                        ("mv-fake", "kill-db", "600"),
                    ]

                    workload = None
                    if "--workload" in sys.argv:
                        workload = sys.argv[sys.argv.index("--workload") + 1]

                    for row in rows:
                        if workload and row[0] != workload:
                            continue
                        print("\\t".join(row))
                    """
                ),
                encoding="utf-8",
            )
            bridge.write_text(
                textwrap.dedent(
                    f"""\
                    #!/usr/bin/env bash
                    set -euo pipefail

                    command="${{1:-}}"
                    shift || true

                    case "$command" in
                      exec)
                        workdir="$1"
                        shift || true
                        if [[ "${{1:-}}" == "--" ]]; then
                          shift
                        fi
                        mkdir -p "$workdir"
                        printf '{{"name":"stub-testbed","items":[{{"name":"node","details":{{"spec":{{"replicas":1}}}}}}]}}\\n' > "$workdir/output"
                        printf 'exec\\t%s\\n' "$workdir" >> {shlex.quote(str(bridge_log))}
                        trap 'exit 0' INT TERM
                        while true; do
                          sleep 1
                        done
                        ;;
                      cleanup)
                        workdir="$1"
                        printf 'cleanup\\t%s\\n' "$workdir" >> {shlex.quote(str(bridge_log))}
                        rm -f "$workdir/output"
                        ;;
                      *)
                        echo "unexpected bridge command: $command" >&2
                        exit 1
                        ;;
                    esac
                    """
                ),
                encoding="utf-8",
            )
            catalog.chmod(0o755)
            bridge.chmod(0o755)

            env = dict(**os.environ)
            env.update(
                {
                    "MVIEW_CATALOG_HELPER": str(catalog),
                    "MVIEW_TESTBED_BRIDGE_SCRIPT": str(bridge),
                    "SUITE_OUTPUT_DIR": str(suite_output),
                    "WORKLOAD_FILTER": "mv-fake",
                }
            )

            proc = subprocess.Popen(
                [
                    "bash",
                    str(PARALLEL_RUNNER),
                    "branch-validation",
                    "http://example.invalid/tidb.tar.gz",
                ],
                cwd=ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )

            try:
                self._wait_for(
                    lambda: bridge_log.exists()
                    and sum(
                        1
                        for line in bridge_log.read_text(encoding="utf-8").splitlines()
                        if line.startswith("exec\t")
                    )
                    == 2
                )

                os.killpg(proc.pid, signal.SIGINT)
                stdout, stderr = proc.communicate(timeout=15)
            except Exception:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
                raise

            self.assertEqual(130, proc.returncode, stdout + stderr)

            lines = bridge_log.read_text(encoding="utf-8").splitlines()
            exec_workdirs = [line.split("\t", 1)[1] for line in lines if line.startswith("exec\t")]
            cleanup_workdirs = [line.split("\t", 1)[1] for line in lines if line.startswith("cleanup\t")]

            self.assertEqual(sorted(exec_workdirs), sorted(cleanup_workdirs))
            for workdir in exec_workdirs:
                self.assertFalse(Path(workdir).exists(), workdir)

            runner_log = (suite_output / "runner.log").read_text(encoding="utf-8")
            self.assertIn("abort cleanup complete workdir=", runner_log)
            self.assertEqual(2, runner_log.count("abort cleanup complete workdir="))
            self.assertEqual(2, runner_log.count("case aborted workload=mv-fake"))

            status_rows = (suite_output / "status.tsv").read_text(encoding="utf-8").splitlines()
            self.assertEqual(2, sum(1 for row in status_rows if "\taborted\t" in row))

    @staticmethod
    def _wait_for(predicate, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(0.1)
        raise AssertionError("condition not met before timeout")


class MViewParallelRunnerCompletionTest(unittest.TestCase):
    def test_single_case_completion_handles_empty_arrays_under_nounset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            scripts_dir = root / "scripts"
            scripts_dir.mkdir(parents=True)

            (scripts_dir / "mview_common.sh").write_text(COMMON.read_text(encoding="utf-8"), encoding="utf-8")
            (scripts_dir / "mview_parallel_suite_run_and_report.sh").write_text(
                PARALLEL_RUNNER.read_text(encoding="utf-8"),
                encoding="utf-8",
            )

            catalog = scripts_dir / "catalog.py"
            bridge = scripts_dir / "bridge.sh"
            run_and_report = scripts_dir / "mview_run_and_report.sh"
            suite_report = scripts_dir / "mview_suite_report.py"
            store_report = scripts_dir / "mview_store_report.py"

            catalog.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env python3
                    print("mv-fake\tnone\t300")
                    """
                ),
                encoding="utf-8",
            )
            bridge.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail

                    command="${1:-}"
                    shift || true

                    case "$command" in
                      exec)
                        workdir="$1"
                        shift || true
                        if [[ "${1:-}" == "--" ]]; then
                          shift
                        fi
                        mkdir -p "$workdir"
                        printf '{"name":"stub-testbed","items":[{"name":"node","details":{"spec":{"replicas":1}}}]}\n' > "$workdir/output"
                        "$@"
                        ;;
                      cleanup)
                        workdir="$1"
                        rm -f "$workdir/output"
                        ;;
                      *)
                        echo "unexpected bridge command: $command" >&2
                        exit 1
                        ;;
                    esac
                    """
                ),
                encoding="utf-8",
            )
            run_and_report.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail

                    workload="${1:-}"
                    run_dir="store/Fake ${workload} run-tag ${MVIEW_RUN_TAG} nemesis ${NEMESIS:-none}/20260322T000000"
                    mkdir -p "$run_dir"
                    printf 'stub run complete for %s\n' "$MVIEW_RUN_TAG"
                    """
                ),
                encoding="utf-8",
            )
            suite_report.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env python3
                    import json
                    import sys

                    if "--json" in sys.argv:
                        print(json.dumps({"stores": sys.argv[sys.argv.index("--json") + 1 :]}))
                    else:
                        print("suite report ok")
                    """
                ),
                encoding="utf-8",
            )
            store_report.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env python3
                    import json
                    import sys

                    if "--json" in sys.argv:
                        print(json.dumps({"store": sys.argv[-1]}))
                    else:
                        print(f"store report ok: {sys.argv[-1]}")
                    """
                ),
                encoding="utf-8",
            )

            catalog.chmod(0o755)
            bridge.chmod(0o755)
            run_and_report.chmod(0o755)

            suite_output = root / "suite-output"
            env = dict(**os.environ)
            env.update(
                {
                    "MVIEW_CATALOG_HELPER": str(catalog),
                    "MVIEW_TESTBED_BRIDGE_SCRIPT": str(bridge),
                    "SUITE_OUTPUT_DIR": str(suite_output),
                    "WORKLOAD_FILTER": "mv-fake",
                    "REPORT_FORMAT": "text",
                }
            )

            proc = subprocess.run(
                [
                    "bash",
                    str(scripts_dir / "mview_parallel_suite_run_and_report.sh"),
                    "branch-validation",
                    "http://example.invalid/tidb.tar.gz",
                ],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
            self.assertIn("suite report ok", proc.stdout)

            runner_log = (suite_output / "runner.log").read_text(encoding="utf-8")
            self.assertIn("case passed workload=mv-fake", runner_log)
            self.assertIn("parallel suite end suite=branch-validation exit_code=0 new_store_dirs=1", runner_log)
            self.assertNotIn("unbound variable", proc.stdout + proc.stderr + runner_log)

            status_rows = (suite_output / "status.tsv").read_text(encoding="utf-8").splitlines()
            self.assertEqual(1, sum(1 for row in status_rows if "\tpassed\t" in row))


class MViewParallelRunnerStdinIsolationTest(unittest.TestCase):
    def test_background_cases_do_not_consume_catalog_stdin(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            scripts_dir = root / "scripts"
            scripts_dir.mkdir(parents=True)

            (scripts_dir / "mview_common.sh").write_text(COMMON.read_text(encoding="utf-8"), encoding="utf-8")
            (scripts_dir / "mview_parallel_suite_run_and_report.sh").write_text(
                PARALLEL_RUNNER.read_text(encoding="utf-8"),
                encoding="utf-8",
            )

            catalog = scripts_dir / "catalog.py"
            bridge = scripts_dir / "bridge.sh"
            run_and_report = scripts_dir / "mview_run_and_report.sh"
            suite_report = scripts_dir / "mview_suite_report.py"
            store_report = scripts_dir / "mview_store_report.py"

            catalog.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env python3
                    import sys

                    rows = [
                        ("mv-fake", "case-1", "300"),
                        ("mv-fake", "case-2", "300"),
                        ("mv-fake", "case-3", "300"),
                        ("mv-fake", "case-4", "300"),
                        ("mv-fake", "case-5", "300"),
                    ]

                    workload = None
                    if "--workload" in sys.argv:
                        workload = sys.argv[sys.argv.index("--workload") + 1]

                    for row in rows:
                        if workload and row[0] != workload:
                            continue
                        print("\\t".join(row))
                    """
                ),
                encoding="utf-8",
            )
            bridge.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail

                    command="${1:-}"
                    shift || true

                    case "$command" in
                      exec)
                        workdir="$1"
                        shift || true
                        if [[ "${1:-}" == "--" ]]; then
                          shift
                        fi
                        mkdir -p "$workdir"
                        printf '{"name":"stub-testbed","items":[{"name":"node","details":{"spec":{"replicas":1}}}]}\n' > "$workdir/output"
                        "$@"
                        ;;
                      cleanup)
                        workdir="$1"
                        rm -f "$workdir/output"
                        ;;
                      *)
                        echo "unexpected bridge command: $command" >&2
                        exit 1
                        ;;
                    esac
                    """
                ),
                encoding="utf-8",
            )
            run_and_report.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail

                    workload="${1:-}"
                    for leaked_var in SUITE_OUTPUT_DIR MVIEW_SUITE_OUTPUT_DIR MVIEW_RUNNER_LOG MVIEW_STATUS_TSV MVIEW_CASE_LOG_DIR; do
                      if [[ -n "${!leaked_var:-}" ]]; then
                        echo "unexpected leaked suite var: ${leaked_var}=${!leaked_var}" >&2
                        exit 1
                      fi
                    done
                    IFS= read -r _ || true
                    run_dir="store/Fake ${workload} run-tag ${MVIEW_RUN_TAG} nemesis ${NEMESIS:-none}/20260322T000000"
                    mkdir -p "$run_dir"
                    printf 'stdin isolated for %s\n' "$MVIEW_RUN_TAG"
                    """
                ),
                encoding="utf-8",
            )
            suite_report.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env python3
                    import json
                    import sys

                    if "--json" in sys.argv:
                        print(json.dumps({"stores": sys.argv[sys.argv.index("--json") + 1 :]}))
                    else:
                        print("suite report ok")
                    """
                ),
                encoding="utf-8",
            )
            store_report.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env python3
                    import json
                    import sys

                    if "--json" in sys.argv:
                        print(json.dumps({"store": sys.argv[-1]}))
                    else:
                        print(f"store report ok: {sys.argv[-1]}")
                    """
                ),
                encoding="utf-8",
            )

            catalog.chmod(0o755)
            bridge.chmod(0o755)
            run_and_report.chmod(0o755)

            suite_output = root / "suite-output"
            env = dict(**os.environ)
            env.update(
                {
                    "MAX_PARALLEL": "1",
                    "MVIEW_CATALOG_HELPER": str(catalog),
                    "MVIEW_TESTBED_BRIDGE_SCRIPT": str(bridge),
                    "SUITE_OUTPUT_DIR": str(suite_output),
                    "WORKLOAD_FILTER": "mv-fake",
                    "REPORT_FORMAT": "text",
                }
            )

            proc = subprocess.run(
                [
                    "bash",
                    str(scripts_dir / "mview_parallel_suite_run_and_report.sh"),
                    "branch-validation",
                    "http://example.invalid/tidb.tar.gz",
                ],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

            status_rows = (suite_output / "status.tsv").read_text(encoding="utf-8").splitlines()
            self.assertEqual(5, sum(1 for row in status_rows if "\tpassed\t" in row))

            runner_log = (suite_output / "runner.log").read_text(encoding="utf-8")
            self.assertEqual(5, runner_log.count("case launch workload=mv-fake"))
            self.assertIn("parallel suite end suite=branch-validation exit_code=0 new_store_dirs=5", runner_log)


if __name__ == "__main__":
    unittest.main()
