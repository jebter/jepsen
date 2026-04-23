import os
import shlex
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
BRIDGE = ROOT / "scripts" / "mview_testbed_bridge.sh"


class MViewTestbedBridgeRetryTest(unittest.TestCase):
    def test_create_bridge_file_exports_ssh_tunnel_ports(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workdir = root / "workdir"
            bridge_copy = root / "bridge.sh"

            workdir.mkdir(parents=True)
            bridge_copy.write_text(
                BRIDGE.read_text(encoding="utf-8").replace('main "$@"', ': # main disabled for unit tests\n', 1),
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    "bash",
                    "-lc",
                    (
                        f"source {shlex.quote(str(bridge_copy))}; "
                        f"create_bridge_file {shlex.quote(str(workdir))} stub-testbed "
                        f"/tmp/kubeconfig.yml /tmp/jepsen.pem socks5://proxy.example.com:1080 "
                        f"node-0,node-1 "
                        f"'{{\"node-0\":4000}}' "
                        f"'{{\"node-0\":2200}}'; "
                        f"cat {shlex.quote(str(workdir / 'bridge.env.sh'))}"
                    ),
                ],
                cwd=root,
                text=True,
                capture_output=True,
            )

            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
            self.assertIn("export JEPSEN_SQL_TUNNEL_PORTS='{\"node-0\":4000}'", proc.stdout)
            self.assertIn("export JEPSEN_SSH_TUNNEL_PORTS='{\"node-0\":2200}'", proc.stdout)

    def test_create_tcctl_testbed_retries_quota_limit_errors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bin_dir = root / "bin"
            workdir = root / "workdir"
            attempt_file = root / "attempts.txt"
            bridge_copy = root / "bridge.sh"
            tcctl = bin_dir / "tcctl"

            bin_dir.mkdir(parents=True)
            workdir.mkdir(parents=True)
            bridge_copy.write_text(
                BRIDGE.read_text(encoding="utf-8").replace('main "$@"', ': # main disabled for unit tests\n', 1),
                encoding="utf-8",
            )

            tcctl.write_text(
                textwrap.dedent(
                    f"""\
                    #!/usr/bin/env bash
                    set -euo pipefail

                    attempt_file={shlex.quote(str(attempt_file))}
                    attempts=0
                    if [[ -f "$attempt_file" ]]; then
                      attempts="$(cat "$attempt_file")"
                    fi
                    attempts=$((attempts + 1))
                    printf '%s' "$attempts" > "$attempt_file"

                    if [[ "${{1:-}}" == "testbed" && "${{2:-}}" == "create" ]]; then
                      if [[ "$attempts" -eq 1 ]]; then
                        echo "Error: toomanyrequests: reach your testbed quota limit(4), please check your testbed list and delete them first: tb-a,tb-b" >&2
                        exit 1
                      fi
                      printf '{{"name":"stub-testbed","items":[{{"name":"node","details":{{"spec":{{"replicas":1}}}}}}]}}\\n'
                      exit 0
                    fi

                    echo "unexpected tcctl args: $*" >&2
                    exit 1
                    """
                ),
                encoding="utf-8",
            )
            tcctl.chmod(0o755)

            env = dict(os.environ)
            env["PATH"] = f"{bin_dir}:{env['PATH']}"
            env["CREATE_RETRY_LIMIT"] = "3"
            env["CREATE_RETRY_DELAY_SECONDS"] = "0"
            env["CREATE_QUOTA_RETRY_LIMIT"] = "3"
            env["CREATE_QUOTA_RETRY_DELAY_SECONDS"] = "0"

            proc = subprocess.run(
                [
                    "bash",
                    "-lc",
                    (
                        f"source {shlex.quote(str(bridge_copy))}; "
                        f"create_tcctl_testbed {shlex.quote(str(workdir))} dummy-spec"
                    ),
                ],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
            self.assertEqual("2", attempt_file.read_text(encoding="utf-8"))
            self.assertIn("retrying in 0s", proc.stderr)
            self.assertIn("quota", proc.stderr.lower())

    def test_quota_retry_uses_dedicated_limits(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bin_dir = root / "bin"
            workdir = root / "workdir"
            attempt_file = root / "attempts.txt"
            bridge_copy = root / "bridge.sh"
            tcctl = bin_dir / "tcctl"

            bin_dir.mkdir(parents=True)
            workdir.mkdir(parents=True)
            bridge_copy.write_text(
                BRIDGE.read_text(encoding="utf-8").replace('main "$@"', ': # main disabled for unit tests\n', 1),
                encoding="utf-8",
            )

            tcctl.write_text(
                textwrap.dedent(
                    f"""\
                    #!/usr/bin/env bash
                    set -euo pipefail

                    attempt_file={shlex.quote(str(attempt_file))}
                    attempts=0
                    if [[ -f "$attempt_file" ]]; then
                      attempts="$(cat "$attempt_file")"
                    fi
                    attempts=$((attempts + 1))
                    printf '%s' "$attempts" > "$attempt_file"

                    if [[ "${{1:-}}" == "testbed" && "${{2:-}}" == "create" ]]; then
                      echo "Error: toomanyrequests: reach your testbed quota limit(4), please check your testbed list and delete them first: tb-a,tb-b" >&2
                      exit 1
                    fi

                    echo "unexpected tcctl args: $*" >&2
                    exit 1
                    """
                ),
                encoding="utf-8",
            )
            tcctl.chmod(0o755)

            env = dict(os.environ)
            env["PATH"] = f"{bin_dir}:{env['PATH']}"
            env["CREATE_RETRY_LIMIT"] = "1"
            env["CREATE_RETRY_DELAY_SECONDS"] = "0"
            env["CREATE_QUOTA_RETRY_LIMIT"] = "3"
            env["CREATE_QUOTA_RETRY_DELAY_SECONDS"] = "0"

            proc = subprocess.run(
                [
                    "bash",
                    "-lc",
                    (
                        f"source {shlex.quote(str(bridge_copy))}; "
                        f"create_tcctl_testbed {shlex.quote(str(workdir))} dummy-spec"
                    ),
                ],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(0, proc.returncode, proc.stdout + proc.stderr)
            self.assertEqual("3", attempt_file.read_text(encoding="utf-8"))
            self.assertIn("attempt 2/3", proc.stderr)
            self.assertNotIn("attempt 1/1", proc.stderr)

    def test_cleanup_accepts_tcctl_get_testbed_not_found_after_namespace_timeout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bin_dir = root / "bin"
            workdir = root / "workdir"
            bridge_copy = root / "bridge.sh"
            tcctl = bin_dir / "tcctl"
            kubectl = bin_dir / "kubectl"

            bin_dir.mkdir(parents=True)
            workdir.mkdir(parents=True)
            bridge_copy.write_text(
                BRIDGE.read_text(encoding="utf-8").replace('main "$@"', ': # main disabled for unit tests\n', 1),
                encoding="utf-8",
            )

            (workdir / "output").write_text(
                '{"name":"stub-testbed","items":[{"name":"node","details":{"spec":{"replicas":1}}}]}\n',
                encoding="utf-8",
            )
            (workdir / "kubeconfig.yml").write_text("apiVersion: v1\n", encoding="utf-8")

            tcctl.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail

                    if [[ "${1:-}" == "testbed" && "${2:-}" == "delete" ]]; then
                      echo "Error: stub-testbed not found" >&2
                      exit 1
                    fi

                    if [[ "${1:-}" == "testbed" && "${2:-}" == "get" && "${3:-}" == "stub-testbed" ]]; then
                      echo "Error: stub-testbed not found" >&2
                      exit 1
                    fi

                    if [[ "${1:-}" == "testbed" && "${2:-}" == "list" ]]; then
                      printf 'NAME STATUS AGE\\n'
                      exit 0
                    fi

                    echo "unexpected tcctl args: $*" >&2
                    exit 1
                    """
                ),
                encoding="utf-8",
            )
            tcctl.chmod(0o755)

            kubectl.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail

                    if [[ "${1:-}" == "delete" && "${2:-}" == "namespace" && "${3:-}" == "stub-testbed" ]]; then
                      exit 0
                    fi

                    if [[ "${1:-}" == "get" && "${2:-}" == "namespace" && "${3:-}" == "stub-testbed" ]]; then
                      if [[ "$*" == *"jsonpath"* ]]; then
                        printf 'Terminating'
                        exit 0
                      fi
                      printf 'NAME STATUS AGE\\n'
                      printf 'stub-testbed Terminating 1m\\n'
                      exit 0
                    fi

                    echo "unexpected kubectl args: $*" >&2
                    exit 1
                    """
                ),
                encoding="utf-8",
            )
            kubectl.chmod(0o755)

            env = dict(os.environ)
            env["PATH"] = f"{bin_dir}:{env['PATH']}"
            env["CLEANUP_WAIT_SECONDS"] = "0"

            proc = subprocess.run(
                [
                    "bash",
                    "-lc",
                    (
                        f"source {shlex.quote(str(bridge_copy))}; "
                        f"cleanup_testbed {shlex.quote(str(workdir))}"
                    ),
                ],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
            self.assertIn("cleanup confirmed for testbed stub-testbed via tcctl", proc.stderr)

    def test_create_testbed_cleans_up_when_ssh_tunnel_setup_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workdir = root / "workdir"
            cleanup_marker = root / "cleanup.txt"
            bridge_copy = root / "bridge.sh"

            workdir.mkdir(parents=True)
            bridge_copy.write_text(
                BRIDGE.read_text(encoding="utf-8").replace('main "$@"', ': # main disabled for unit tests\n', 1),
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    "bash",
                    "-lc",
                    textwrap.dedent(
                        f"""\
                        source {shlex.quote(str(bridge_copy))}
                        create_tcctl_testbed() {{
                          local workdir="$1"
                          cat >"$workdir/output" <<'EOF'
                        {{"name":"stub-testbed","items":[{{"name":"node","details":{{"spec":{{"replicas":1}}}}}}]}}
                        EOF
                          cat >"$workdir/.env" <<'EOF'
                        HTTP_PROXY: socks5://proxy.example.com:1080
                        EOF
                        }}
                        extract_private_key() {{
                          printf 'private-key\\n' >"$2"
                          chmod 600 "$2"
                        }}
                        bootstrap_authorized_keys() {{
                          :
                        }}
                        ssh-keygen() {{
                          printf 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDstubkey\\n'
                        }}
                        start_sql_tunnels() {{
                          printf '{{"node-0.node-peer.stub-testbed.svc.cluster.local":4000}}'
                        }}
                        start_ssh_tunnels() {{
                          echo "simulated ssh tunnel failure" >&2
                          return 1
                        }}
                        cleanup_testbed() {{
                          printf '%s\\n' "$1" > {shlex.quote(str(cleanup_marker))}
                          return 0
                        }}
                        create_testbed {shlex.quote(str(workdir))} dummy-spec
                        """
                    ),
                ],
                cwd=root,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(0, proc.returncode, proc.stdout + proc.stderr)
            self.assertIn("simulated ssh tunnel failure", proc.stderr)
            self.assertEqual(str(workdir), cleanup_marker.read_text(encoding="utf-8").strip())


if __name__ == "__main__":
    unittest.main()
