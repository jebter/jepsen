#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFAULT_SPEC="$ROOT_DIR/jepsen-testbed.yaml"
CREATE_RETRY_LIMIT="${CREATE_RETRY_LIMIT:-3}"
CREATE_RETRY_DELAY_SECONDS="${CREATE_RETRY_DELAY_SECONDS:-5}"
CLEANUP_WAIT_SECONDS="${CLEANUP_WAIT_SECONDS:-30}"

usage() {
  cat <<'EOF'
usage:
  scripts/mview_testbed_bridge.sh create [workdir] [spec]
  scripts/mview_testbed_bridge.sh cleanup <workdir>

create:
  Creates a fresh tcctl testbed in workdir, extracts a Jepsen SSH private key
  from kubeconfig.yml, injects the matching public key into every node's
  authorized_keys, starts localhost SQL port-forwards for every node, and
  prints shell assignments for the resulting bridge data.

cleanup:
  Deletes only the testbed described by <workdir>/output and stops only the
  SQL port-forwards recorded under that same workdir. It tries
  "tcctl testbed delete -f output" first. If the namespace is still present
  afterwards, it falls back to deleting that namespace via kubectl.
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

read_testbed_field() {
  local output_file="$1"
  local field="$2"
  python3 - "$output_file" "$field" <<'PY'
import json
import pathlib
import sys

output = pathlib.Path(sys.argv[1]).read_text()
field = sys.argv[2]
data = json.loads(output)

if field == "name":
    print(data["name"])
elif field == "replicas":
    for item in data["items"]:
        if item.get("name") == "node":
            print(item["details"]["spec"]["replicas"])
            break
    else:
        raise SystemExit("node replicas not found in output")
else:
    raise SystemExit(f"unknown field: {field}")
PY
}

read_env_field() {
  local env_file="$1"
  local field="$2"
  python3 - "$env_file" "$field" <<'PY'
import pathlib
import sys

env_file = pathlib.Path(sys.argv[1])
field = sys.argv[2]

for line in env_file.read_text().splitlines():
    if line.startswith("#") or ": " not in line:
        continue
    key, value = line.split(": ", 1)
    if key == field:
        print(value)
        break
else:
    raise SystemExit(f"{field} not found in {env_file}")
PY
}

extract_private_key() {
  local kubeconfig="$1"
  local private_key="$2"
  python3 - "$kubeconfig" "$private_key" <<'PY'
import base64
import pathlib
import re
import sys

kubeconfig = pathlib.Path(sys.argv[1]).read_text()
private_key = pathlib.Path(sys.argv[2])
match = re.search(r"client-key-data:\s*([^\n]+)", kubeconfig)
if not match:
    raise SystemExit("client-key-data not found in kubeconfig")
private_key.write_text(base64.b64decode(match.group(1)).decode())
PY
  chmod 600 "$private_key"
}

node_fqdn_csv() {
  local testbed="$1"
  local replicas="$2"
  python3 - "$testbed" "$replicas" <<'PY'
import sys

testbed = sys.argv[1]
replicas = int(sys.argv[2])
print(",".join(
    f"node-{idx}.node-peer.{testbed}.svc.cluster.local"
    for idx in range(replicas)
))
PY
}

node_fqdn() {
  local testbed="$1"
  local index="$2"
  printf 'node-%s.node-peer.%s.svc.cluster.local\n' "$index" "$testbed"
}

sql_tunnel_pid_file() {
  local workdir="$1"
  printf '%s/sql-port-forward.pids\n' "$workdir"
}

sql_tunnel_log_file() {
  local workdir="$1"
  local index="$2"
  printf '%s/sql-port-forward-node-%s.log\n' "$workdir" "$index"
}

find_free_local_port() {
  python3 - <<'PY'
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

wait_for_port_forward() {
  local pid="$1"
  local port="$2"
  local log_file="$3"
  local attempt=0

  for ((attempt = 0; attempt < 80; attempt++)); do
    if grep -Eq "Forwarding from (127\\.0\\.0\\.1|\\[::1\\]):${port} -> 4000" "$log_file" 2>/dev/null; then
      return 0
    fi

    if ! kill -0 "$pid" >/dev/null 2>&1; then
      [[ -f "$log_file" ]] && cat "$log_file" >&2
      return 1
    fi

    sleep 0.25
  done

  [[ -f "$log_file" ]] && cat "$log_file" >&2
  return 1
}

cleanup_sql_tunnels() {
  local workdir="$1"
  local pid_file=""
  local pid=""

  pid_file="$(sql_tunnel_pid_file "$workdir")"
  [[ -f "$pid_file" ]] || return 0

  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    kill "$pid" >/dev/null 2>&1 || true
  done <"$pid_file"

  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    for ((attempt = 0; attempt < 20; attempt++)); do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        break
      fi
      sleep 0.1
    done
  done <"$pid_file"
}

json_map_from_pairs() {
  python3 - "$@" <<'PY'
import json
import sys

data = {}
for item in sys.argv[1:]:
    key, value = item.split("=", 1)
    data[key] = int(value)
print(json.dumps(data, separators=(",", ":")))
PY
}

start_detached_process() {
  python3 - "$@" <<'PY'
import subprocess
import sys

proc = subprocess.Popen(
    sys.argv[1:],
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    start_new_session=True,
    close_fds=True,
)
print(proc.pid)
PY
}

start_sql_tunnels() {
  local workdir="$1"
  local kubeconfig="$2"
  local namespace="$3"
  local replicas="$4"
  local pid_file=""
  local mappings=()
  local index=0
  local port=""
  local pid=""
  local fqdn=""
  local log_file=""

  pid_file="$(sql_tunnel_pid_file "$workdir")"
  : >"$pid_file"

  for ((index = 0; index < replicas; index++)); do
    fqdn="$(node_fqdn "$namespace" "$index")"
    port="$(find_free_local_port)"
    log_file="$(sql_tunnel_log_file "$workdir" "$index")"
    : >"$log_file"

    pid="$(start_detached_process bash -c '
      set -euo pipefail
      kubeconfig="$1"
      namespace="$2"
      index="$3"
      port="$4"
      log_file="$5"
      child_pid=""

      cleanup() {
        if [[ -n "${child_pid:-}" ]]; then
          kill "$child_pid" >/dev/null 2>&1 || true
          wait "$child_pid" >/dev/null 2>&1 || true
        fi
        exit 0
      }

      trap cleanup TERM INT EXIT

      while true; do
        printf "[restart %s] starting port-forward node-%s %s\n" \
          "$(date +%Y-%m-%dT%H:%M:%S%z)" "$index" "$port" >>"$log_file"
        env KUBECONFIG="$kubeconfig" kubectl port-forward -n "$namespace" "pod/node-$index" \
          "$port:4000" >>"$log_file" 2>&1 &
        child_pid=$!
        wait "$child_pid" >/dev/null 2>&1 || true
        child_pid=""
        printf "[restart %s] port-forward node-%s exited\n" \
          "$(date +%Y-%m-%dT%H:%M:%S%z)" "$index" >>"$log_file"
        sleep 0.2
      done
    ' bash "$kubeconfig" "$namespace" "$index" "$port" "$log_file")"
    printf '%s\n' "$pid" >>"$pid_file"

    if ! wait_for_port_forward "$pid" "$port" "$log_file"; then
      cleanup_sql_tunnels "$workdir"
      echo "failed to start SQL tunnel for $fqdn" >&2
      return 1
    fi

    mappings+=("$fqdn=$port")
  done

  json_map_from_pairs "${mappings[@]}"
}

bootstrap_authorized_keys() {
  local kubeconfig="$1"
  local namespace="$2"
  local replicas="$3"
  local public_key="$4"
  local node=""
  local index=""

  for ((index = 0; index < replicas; index++)); do
    node="node-$index"
    KUBECONFIG="$kubeconfig" kubectl -n "$namespace" exec "$node" -- \
      sh -lc '
        pubkey="$1"
        mkdir -p /root/.ssh
        chmod 700 /root/.ssh
        touch /root/.ssh/authorized_keys
        chmod 600 /root/.ssh/authorized_keys
        grep -qxF "$pubkey" /root/.ssh/authorized_keys || printf "%s\n" "$pubkey" >> /root/.ssh/authorized_keys
      ' sh "$public_key" >/dev/null
  done
}

namespace_exists() {
  local kubeconfig="$1"
  local namespace="$2"
  KUBECONFIG="$kubeconfig" kubectl get namespace "$namespace" >/dev/null 2>&1
}

namespace_phase_raw() {
  local kubeconfig="$1"
  local namespace="$2"
  set +e
  KUBECONFIG="$kubeconfig" kubectl get namespace "$namespace" -o jsonpath='{.status.phase}' 2>&1
  local rc=$?
  set -e
  return "$rc"
}

namespace_phase() {
  local kubeconfig="$1"
  local namespace="$2"
  KUBECONFIG="$kubeconfig" kubectl get namespace "$namespace" -o jsonpath='{.status.phase}' 2>/dev/null || true
}

testbed_state_in_tcctl_list() {
  local testbed="$1"
  local output=""
  local rc=0

  set +e
  output="$(tcctl testbed list 2>/dev/null)"
  rc=$?
  set -e

  if ((rc != 0)); then
    printf 'error'
    return 0
  fi

  if awk -v testbed="$testbed" 'NR > 1 && $1 == testbed { found = 1 } END { exit(found ? 0 : 1) }' <<<"$output"; then
    printf 'present'
    return 0
  fi

  printf 'absent'
}

create_tcctl_testbed() {
  local workdir="$1"
  local spec="$2"
  local attempt=1
  local output=""
  local rc=0

  while ((attempt <= CREATE_RETRY_LIMIT)); do
    set +e
    output="$(cd "$workdir" && tcctl testbed create -f "$spec" --credential kubeconfig.yml --output-spec '{"output":"output"}' 2>&1)"
    rc=$?
    set -e

    printf '%s\n' "$output"

    if ((rc == 0)); then
      return 0
    fi

    if grep -Eq 'resource \[[^]]+/node\] not found|get testbed error: internalerror: resource \[[^]]+/node\] not found' <<<"$output"; then
      if ((attempt < CREATE_RETRY_LIMIT)); then
        echo "transient tcctl create error on attempt $attempt/$CREATE_RETRY_LIMIT, retrying in ${CREATE_RETRY_DELAY_SECONDS}s" >&2
        sleep "$CREATE_RETRY_DELAY_SECONDS"
        ((attempt++))
        continue
      fi
    fi

    return "$rc"
  done
}

wait_for_namespace_gone() {
  local kubeconfig="$1"
  local namespace="$2"
  local timeout_seconds="$3"
  local elapsed=0
  local output=""
  local rc=0
  local list_state=""

  while ((elapsed <= timeout_seconds)); do
    set +e
    output="$(namespace_phase_raw "$kubeconfig" "$namespace")"
    rc=$?
    set -e

    if ((rc == 0)); then
      if [[ -z "$output" ]]; then
        return 0
      fi
      if [[ "$output" != "Terminating" ]]; then
        printf 'phase:%s' "$output"
        return 1
      fi
    else
      if grep -qi 'not found' <<<"$output"; then
        return 0
      fi
      if grep -qi 'forbidden' <<<"$output"; then
        list_state="$(testbed_state_in_tcctl_list "$namespace")"
        if [[ "$list_state" == "absent" ]]; then
          return 0
        fi
        if [[ "$list_state" == "present" ]]; then
          sleep 2
          elapsed=$((elapsed + 2))
          continue
        fi
      fi
      printf 'query-error:%s' "$output"
      return 1
    fi

    sleep 2
    elapsed=$((elapsed + 2))
  done

  printf 'timeout:namespace phase remained Terminating for %ss' "$timeout_seconds"
  return 1
}

create_bridge_file() {
  local workdir="$1"
  local testbed="$2"
  local kubeconfig="$3"
  local private_key="$4"
  local http_proxy="$5"
  local nodes="$6"
  local sql_tunnel_ports="$7"
  cat >"$workdir/bridge.env.sh" <<EOF
export MVIEW_TESTBED_WORKDIR='$workdir'
export TESTBED='$testbed'
export KUBECONFIG='$kubeconfig'
export SSH_PRIVATE_KEY='$private_key'
export JEPSEN_SSH_PRIVATE_KEY='$private_key'
export HTTP_PROXY='$http_proxy'
export ALL_PROXY='$http_proxy'
export JEPSEN_SSH_PROXY='$http_proxy'
export NODES='$nodes'
export JEPSEN_NODES='$nodes'
export JEPSEN_SQL_TUNNEL_PORTS='$sql_tunnel_ports'
EOF
}

create_testbed() {
  local workdir="${1:-}"
  local spec="${2:-$DEFAULT_SPEC}"
  local output_file=""
  local env_file=""
  local kubeconfig=""
  local private_key=""
  local public_key=""
  local testbed=""
  local replicas=""
  local http_proxy=""
  local nodes=""
  local sql_tunnel_ports=""

  if [[ -z "$workdir" ]]; then
    workdir="$(mktemp -d /tmp/mview-testbed-XXXXXX)"
  else
    mkdir -p "$workdir"
  fi

  output_file="$workdir/output"
  env_file="$workdir/.env"
  kubeconfig="$workdir/kubeconfig.yml"
  private_key="$workdir/jepsen.pem"

  create_tcctl_testbed "$workdir" "$spec"

  testbed="$(read_testbed_field "$output_file" name)"
  replicas="$(read_testbed_field "$output_file" replicas)"
  http_proxy="$(read_env_field "$env_file" HTTP_PROXY)"

  extract_private_key "$kubeconfig" "$private_key"
  public_key="$(ssh-keygen -y -f "$private_key")"
  bootstrap_authorized_keys "$kubeconfig" "$testbed" "$replicas" "$public_key"
  nodes="$(node_fqdn_csv "$testbed" "$replicas")"
  sql_tunnel_ports="$(start_sql_tunnels "$workdir" "$kubeconfig" "$testbed" "$replicas")"
  create_bridge_file "$workdir" "$testbed" "$kubeconfig" "$private_key" "$http_proxy" "$nodes" "$sql_tunnel_ports"

  cat <<EOF
MVIEW_TESTBED_WORKDIR=$workdir
TESTBED=$testbed
KUBECONFIG=$kubeconfig
SSH_PRIVATE_KEY=$private_key
JEPSEN_SSH_PRIVATE_KEY=$private_key
HTTP_PROXY=$http_proxy
ALL_PROXY=$http_proxy
JEPSEN_SSH_PROXY=$http_proxy
NODES=$nodes
JEPSEN_NODES=$nodes
JEPSEN_SQL_TUNNEL_PORTS=$sql_tunnel_ports
BRIDGE_ENV=$workdir/bridge.env.sh
EOF
}

cleanup_testbed() {
  local workdir="${1:-}"
  local output_file=""
  local kubeconfig=""
  local testbed=""
  local delete_output=""
  local delete_rc=0
  local phase=""
  local wait_result=""
  local cleanup_cmd="tcctl testbed delete -f output"

  if [[ -z "$workdir" ]]; then
    echo "cleanup requires <workdir>" >&2
    exit 1
  fi

  output_file="$workdir/output"
  kubeconfig="$workdir/kubeconfig.yml"
  if [[ ! -f "$output_file" ]]; then
    echo "missing output file: $output_file" >&2
    exit 1
  fi

  testbed="$(read_testbed_field "$output_file" name)"
  cleanup_sql_tunnels "$workdir"

  set +e
  delete_output="$(cd "$workdir" && tcctl testbed delete -f output 2>&1)"
  delete_rc=$?
  set -e

  if [[ -n "$delete_output" ]]; then
    printf '%s\n' "$delete_output" >&2
  fi

  if namespace_exists "$kubeconfig" "$testbed"; then
    KUBECONFIG="$kubeconfig" kubectl delete namespace "$testbed" --wait=false >/dev/null
  fi

  if namespace_exists "$kubeconfig" "$testbed"; then
    KUBECONFIG="$kubeconfig" kubectl get namespace "$testbed" >&2 || true
  fi

  if ! wait_result="$(wait_for_namespace_gone "$kubeconfig" "$testbed" "$CLEANUP_WAIT_SECONDS")"; then
    phase="$(namespace_phase "$kubeconfig" "$testbed")"
    echo "cleanup blocked for testbed $testbed" >&2
    echo "cleanup command: $cleanup_cmd" >&2
    if [[ -n "$phase" ]]; then
      echo "namespace phase: $phase" >&2
    fi
    echo "blocker: $wait_result" >&2
    return 1
  fi

  if ((delete_rc != 0)); then
    return 0
  fi
}

main() {
  require_cmd tcctl
  require_cmd kubectl
  require_cmd python3
  require_cmd ssh-keygen

  case "${1:-}" in
    create)
      shift
      create_testbed "${1:-}" "${2:-$DEFAULT_SPEC}"
      ;;
    cleanup)
      shift
      cleanup_testbed "${1:-}"
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
