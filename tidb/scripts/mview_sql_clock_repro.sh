#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/mview_common.sh
source "$SCRIPT_DIR/mview_common.sh"

mview_init_root "$SCRIPT_DIR"

TARBALL_URL="${1:-${TARBALL_URL:-}}"
BINARY_URLS="${2:-${BINARY_URLS:-}}"
BOOTSTRAP_WORKLOAD="${BOOTSTRAP_WORKLOAD:-mv-stateful}"
BOOTSTRAP_TIME_LIMIT="${BOOTSTRAP_TIME_LIMIT:-60}"
BOOTSTRAP_CONCURRENCY="${BOOTSTRAP_CONCURRENCY:-1n}"
SAMPLE_NODE_INDEX="${SAMPLE_NODE_INDEX:-${TARGET_NODE_INDEX:-0}}"
TARGET_NODE_INDEXES="${TARGET_NODE_INDEXES:-${TARGET_NODE_INDEX:-0}}"
BUMP_SECONDS="${BUMP_SECONDS:-90}"
BUMP_HOLD_SECONDS="${BUMP_HOLD_SECONDS:-20}"
PRE_RESET_SAMPLES="${PRE_RESET_SAMPLES:-4}"
POST_RESET_SAMPLES="${POST_RESET_SAMPLES:-18}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-5}"
WRITE_LOOP_SECONDS="${WRITE_LOOP_SECONDS:-180}"

mview_init_base_env "mview sql clock repro"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

usage() {
  echo "usage: $0 <tarball-url> [binary-urls]" >&2
  echo "required env from bridge exec: TESTBED KUBECONFIG JEPSEN_SQL_TUNNEL_PORTS MVIEW_TESTBED_WORKDIR" >&2
  echo "optional env: BOOTSTRAP_WORKLOAD BOOTSTRAP_TIME_LIMIT BOOTSTRAP_CONCURRENCY SAMPLE_NODE_INDEX TARGET_NODE_INDEXES BUMP_SECONDS BUMP_HOLD_SECONDS PRE_RESET_SAMPLES POST_RESET_SAMPLES SAMPLE_INTERVAL_SECONDS WRITE_LOOP_SECONDS" >&2
}

if [[ -z "${TARBALL_URL:-}" ]]; then
  usage
  exit 1
fi

require_cmd jq
require_cmd kubectl
require_cmd mysql
require_cmd python3
require_cmd lein

for required_env in TESTBED KUBECONFIG JEPSEN_SQL_TUNNEL_PORTS MVIEW_TESTBED_WORKDIR; do
  if [[ -z "${!required_env:-}" ]]; then
    echo "missing required environment variable: $required_env" >&2
    exit 1
  fi
done

workdir="${MVIEW_TESTBED_WORKDIR}"
mkdir -p "$workdir"
mkdir -p "$workdir/sql-clock-repro"
log_dir="$workdir/sql-clock-repro"
runtime_tsv="$log_dir/runtime.tsv"
history_tsv="$log_dir/history.tsv"
summary_json="$log_dir/summary.json"
bootstrap_run_tag="$(mview_make_run_tag "sql-bootstrap")"
bootstrap_log="$log_dir/bootstrap.log"

sample_node_fqdn="node-${SAMPLE_NODE_INDEX}.node-peer.${TESTBED}.svc.cluster.local"

parse_node_indexes() {
  local raw="${1// /}"
  local -a parsed=()
  local token=""
  IFS=',' read -r -a parsed <<< "$raw"
  if [[ ${#parsed[@]} -eq 0 ]]; then
    echo "TARGET_NODE_INDEXES must not be empty" >&2
    exit 1
  fi
  target_node_indexes=()
  for token in "${parsed[@]}"; do
    if [[ ! "$token" =~ ^[0-9]+$ ]]; then
      echo "invalid node index in TARGET_NODE_INDEXES: $token" >&2
      exit 1
    fi
    target_node_indexes+=("$token")
  done
}

parse_node_indexes "$TARGET_NODE_INDEXES"
target_node_indexes_csv="$(IFS=,; echo "${target_node_indexes[*]}")"
target_node_labels=()
for node_idx in "${target_node_indexes[@]}"; do
  target_node_labels+=("node-$node_idx")
done
target_node_labels_csv="$(IFS=,; echo "${target_node_labels[*]}")"
log_table=""

sql_port_for_node() {
  python3 - "$JEPSEN_SQL_TUNNEL_PORTS" "$1" <<'PY'
import json
import sys

ports = json.loads(sys.argv[1])
node = sys.argv[2]
port = ports.get(node)
if port is None:
    raise SystemExit(f"port mapping missing for {node}")
print(port)
PY
}

target_port="$(sql_port_for_node "$sample_node_fqdn")"

mysql_exec() {
  local sql="$1"
  mysql --protocol tcp -h127.0.0.1 -P"$target_port" -uroot test -Nse "$sql"
}

mysql_force_exec() {
  local sql="$1"
  mysql --force --protocol tcp -h127.0.0.1 -P"$target_port" -uroot test -Nse "$sql"
}

kubectl_exec_node() {
  local node="$1"
  shift
  KUBECONFIG="$KUBECONFIG" kubectl exec -n "$TESTBED" "node-$node" -- "$@"
}

node_epoch_seconds() {
  local node="$1"
  kubectl_exec_node "$node" date +%s
}

set_node_epoch_seconds() {
  local node="$1"
  local epoch="$2"
  local timestamp=""

  timestamp="$(python3 - "$epoch" <<'PY'
import sys
import time

epoch = int(sys.argv[1])
print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(epoch)))
PY
)"

  kubectl_exec_node "$node" sh -lc 'date -u -s "$1" >/dev/null && date +%s' sh "$timestamp"
}

wait_for_sql_ready() {
  local tries=60
  while ((tries > 0)); do
    if mysql_exec 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
    tries=$((tries - 1))
  done
  echo "mysql did not become ready on $sample_node_fqdn via local port $target_port" >&2
  return 1
}

discover_log_table() {
  log_table="$(mysql_exec "SHOW TABLES" | awk '/^\$mlog\$/ {print; exit}')"
  if [[ -z "$log_table" ]]; then
    echo "failed to discover mlog backing table after schema setup" >&2
    exit 1
  fi
}

snapshot_runtime_sample() {
  local phase="$1"
  local sample_idx="$2"
  local host_now=""
  local node_now=""
  local row=""

  host_now="$(date -u +%s)"
  node_now="$(node_epoch_seconds "$SAMPLE_NODE_INDEX")"
  row="$(mysql_exec "
SELECT
  '$phase',
  $sample_idx,
  $host_now,
  $node_now,
  CAST(ROUND(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 1000) AS SIGNED) AS db_now_ms,
  row_i.next_time_ms AS row_next_ms,
  agg_i.next_time_ms AS agg_next_ms,
  purge_i.next_time_ms AS purge_next_ms,
  row_i.last_success_read_tso AS row_tso,
  agg_i.last_success_read_tso AS agg_tso,
  purge_i.last_purged_tso AS purge_tso
FROM
  (SELECT CAST(ROUND(UNIX_TIMESTAMP(i.next_time) * 1000) AS SIGNED) AS next_time_ms,
          i.last_success_read_tso AS last_success_read_tso
   FROM information_schema.tables t
   LEFT JOIN mysql.tidb_mview_refresh_info i ON i.mview_id = t.tidb_table_id
   WHERE t.table_schema = DATABASE() AND t.table_name = 'mv_stateful_row') AS row_i
JOIN
  (SELECT CAST(ROUND(UNIX_TIMESTAMP(i.next_time) * 1000) AS SIGNED) AS next_time_ms,
          i.last_success_read_tso AS last_success_read_tso
   FROM information_schema.tables t
   LEFT JOIN mysql.tidb_mview_refresh_info i ON i.mview_id = t.tidb_table_id
   WHERE t.table_schema = DATABASE() AND t.table_name = 'mv_stateful_agg') AS agg_i
JOIN
  (SELECT CAST(ROUND(UNIX_TIMESTAMP(i.next_time) * 1000) AS SIGNED) AS next_time_ms,
          i.last_purged_tso AS last_purged_tso
   FROM information_schema.tables t
   LEFT JOIN mysql.tidb_mlog_purge_info i ON i.mlog_id = t.tidb_table_id
   WHERE t.table_schema = DATABASE() AND t.table_name = '$log_table') AS purge_i;
")"
  printf '%s\n' "$row" >>"$runtime_tsv"
}

capture_history() {
  mysql_exec "
SELECT 'row-refresh' AS component,
       h.refresh_job_id AS job_id,
       h.refresh_status AS status,
       h.refresh_read_tso AS tso,
       h.refresh_rows AS row_count,
       CAST(ROUND(UNIX_TIMESTAMP(h.refresh_time) * 1000) AS SIGNED) AS start_ms,
       CAST(ROUND(UNIX_TIMESTAMP(h.refresh_endtime) * 1000) AS SIGNED) AS end_ms
FROM information_schema.tables t
JOIN mysql.tidb_mview_refresh_hist h ON h.mview_id = t.tidb_table_id
WHERE t.table_schema = DATABASE() AND t.table_name = 'mv_stateful_row'
ORDER BY h.refresh_job_id DESC
LIMIT 8;

SELECT 'agg-refresh' AS component,
       h.refresh_job_id AS job_id,
       h.refresh_status AS status,
       h.refresh_read_tso AS tso,
       h.refresh_rows AS row_count,
       CAST(ROUND(UNIX_TIMESTAMP(h.refresh_time) * 1000) AS SIGNED) AS start_ms,
       CAST(ROUND(UNIX_TIMESTAMP(h.refresh_endtime) * 1000) AS SIGNED) AS end_ms
FROM information_schema.tables t
JOIN mysql.tidb_mview_refresh_hist h ON h.mview_id = t.tidb_table_id
WHERE t.table_schema = DATABASE() AND t.table_name = 'mv_stateful_agg'
ORDER BY h.refresh_job_id DESC
LIMIT 8;

SELECT 'log-purge' AS component,
       h.purge_job_id AS job_id,
       h.purge_status AS status,
       NULL AS tso,
       h.purge_rows AS row_count,
       CAST(ROUND(UNIX_TIMESTAMP(h.purge_time) * 1000) AS SIGNED) AS start_ms,
       CAST(ROUND(UNIX_TIMESTAMP(h.purge_endtime) * 1000) AS SIGNED) AS end_ms
FROM information_schema.tables t
JOIN mysql.tidb_mlog_purge_hist h ON h.mlog_id = t.tidb_table_id
WHERE t.table_schema = DATABASE() AND t.table_name = '$log_table'
ORDER BY h.purge_job_id DESC
LIMIT 8;
" >"$history_tsv"
}

apply_schema() {
  mysql_force_exec "
DROP MATERIALIZED VIEW mv_stateful_agg;
DROP MATERIALIZED VIEW mv_stateful_row;
DROP MATERIALIZED VIEW LOG ON mv_stateful_base;
DROP TABLE IF EXISTS mv_stateful_base;
"

  mysql_exec "
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE mv_stateful_base (
  id         int          NOT NULL PRIMARY KEY,
  g1         int          NOT NULL,
  v1         bigint       NOT NULL,
  version    bigint       NOT NULL,
  last_token varchar(128) NOT NULL,
  deleted    tinyint      NOT NULL DEFAULT 0,
  pad        varchar(64)  NOT NULL,
  KEY idx_mv_stateful_g1_deleted_v1 (g1, deleted, v1)
);

CREATE MATERIALIZED VIEW LOG ON mv_stateful_base
  (id, g1, v1, version, last_token, deleted)
  PURGE START WITH NOW(0) + INTERVAL 2 SECOND
  NEXT NOW(0) + INTERVAL 11 SECOND;

CREATE MATERIALIZED VIEW mv_stateful_row
  (id, g1, v1, version, last_token, live_cnt)
  COMMENT = 'jepsen:mv-stateful(row)'
  REFRESH FAST START WITH NOW(0) + INTERVAL 2 SECOND
  NEXT NOW(0) + INTERVAL 5 SECOND AS
SELECT id, g1, v1, version, last_token, COUNT(*) AS live_cnt
FROM mv_stateful_base
WHERE deleted = 0
GROUP BY id, g1, v1, version, last_token;

CREATE MATERIALIZED VIEW mv_stateful_agg
  (g1, cnt, sum_v1, min_v1, max_v1)
  COMMENT = 'jepsen:mv-stateful(agg)'
  REFRESH FAST START WITH NOW(0) + INTERVAL 2 SECOND
  NEXT NOW(0) + INTERVAL 7 SECOND AS
SELECT g1,
       COUNT(*) AS cnt,
       SUM(v1)  AS sum_v1,
       MIN(v1)  AS min_v1,
       MAX(v1)  AS max_v1
FROM mv_stateful_base
WHERE deleted = 0
GROUP BY g1;
"

  mysql_exec "
INSERT INTO mv_stateful_base VALUES
  (1, 1, 10, 1, 't1', 0, 'a'),
  (2, 1, 20, 1, 't2', 0, 'b'),
  (3, 2, 30, 1, 't3', 0, 'c'),
  (4, 2, 40, 1, 't4', 0, 'd'),
  (5, 3, 50, 1, 't5', 0, 'e');
"

  discover_log_table
}

start_writer_loop() {
  local writer_log="$log_dir/writer.log"
  : >"$writer_log"

  (
    set +e
    for ((i = 1; i <= WRITE_LOOP_SECONDS; i++)); do
      id=$((1000 + i))
      g1=$(((i % 3) + 1))
      v1=$((i * 100))
      mysql_exec "
INSERT INTO mv_stateful_base
VALUES ($id, $g1, $v1, 1, 'ins_$i', 0, 'pad_$i')
ON DUPLICATE KEY UPDATE
  g1 = VALUES(g1),
  v1 = VALUES(v1),
  version = version + 1,
  last_token = CONCAT(last_token, '_u'),
  deleted = 0,
  pad = VALUES(pad);

UPDATE mv_stateful_base
SET v1 = v1 + 1,
    version = version + 1,
    last_token = CONCAT(last_token, '_u')
WHERE id IN (1, 2, 3);
" >>"$writer_log" 2>&1 || true
      sleep 1
    done
  ) &
  writer_pid=$!
}

stop_writer_loop() {
  if [[ -n "${writer_pid:-}" ]]; then
    kill "$writer_pid" >/dev/null 2>&1 || true
    wait "$writer_pid" >/dev/null 2>&1 || true
  fi
}

set_target_nodes_epoch_seconds() {
  local epoch="$1"
  local node_idx=""
  for node_idx in "${target_node_indexes[@]}"; do
    set_node_epoch_seconds "$node_idx" "$epoch"
  done
}

analyze_runtime() {
  python3 - "$runtime_tsv" "$summary_json" <<'PY'
import csv
import json
import sys
from pathlib import Path

runtime_path = Path(sys.argv[1])
summary_path = Path(sys.argv[2])

rows = []
with runtime_path.open() as fh:
    reader = csv.DictReader(fh, delimiter='\t')
    for row in reader:
        def parse_int(value):
            if value in (None, "", "NULL"):
                return None
            return int(value)

        for key in [
            "sample_idx",
            "host_now_s",
            "node_now_s",
            "db_now_ms",
            "row_next_ms",
            "agg_next_ms",
            "purge_next_ms",
            "row_tso",
            "agg_tso",
            "purge_tso",
        ]:
            row[key] = parse_int(row[key])
        rows.append(row)

post = [r for r in rows if r["phase"] == "post-reset"]
if not post:
    raise SystemExit("missing post-reset samples")

def distinct(values):
    return sorted(set(v for v in values if v is not None))

def first_gap(rows, key):
    value = rows[0][key]
    if value is None:
        return None
    return value - rows[0]["db_now_ms"]

def last_gap(rows, key):
    value = rows[-1][key]
    if value is None:
        return None
    return value - rows[-1]["db_now_ms"]

row_next = distinct(r["row_next_ms"] for r in post)
agg_next = distinct(r["agg_next_ms"] for r in post)
purge_next = distinct(r["purge_next_ms"] for r in post)
row_tso = distinct(r["row_tso"] for r in post)
agg_tso = distinct(r["agg_tso"] for r in post)
purge_tso = distinct(r["purge_tso"] for r in post)

summary = {
    "post_reset_sample_count": len(post),
    "row_next_distinct_count": len(row_next),
    "agg_next_distinct_count": len(agg_next),
    "purge_next_distinct_count": len(purge_next),
    "row_tso_distinct_count": len(row_tso),
    "agg_tso_distinct_count": len(agg_tso),
    "purge_tso_distinct_count": len(purge_tso),
    "row_next_values": row_next,
    "agg_next_values": agg_next,
    "purge_next_values": purge_next,
    "row_tso_values": row_tso,
    "agg_tso_values": agg_tso,
    "purge_tso_values": purge_tso,
    "purge_next_null_count": sum(1 for r in post if r["purge_next_ms"] is None),
    "purge_tso_null_count": sum(1 for r in post if r["purge_tso"] is None),
    "row_future_gap_start_ms": first_gap(post, "row_next_ms"),
    "agg_future_gap_start_ms": first_gap(post, "agg_next_ms"),
    "purge_future_gap_start_ms": first_gap(post, "purge_next_ms"),
    "row_future_gap_end_ms": last_gap(post, "row_next_ms"),
    "agg_future_gap_end_ms": last_gap(post, "agg_next_ms"),
    "purge_future_gap_end_ms": last_gap(post, "purge_next_ms"),
}

summary["reproduced_agg_only_future_pin"] = bool(
    len(row_next) > 1 and
    len(agg_next) == 1 and
    len(row_tso) > 1 and
    len(agg_tso) == 1
)
summary["reproduced_all_components_future_pin"] = bool(
    len(row_next) == 1 and
    len(agg_next) == 1 and
    len(purge_next) == 1 and
    len(row_tso) == 1 and
    len(agg_tso) == 1 and
    len(purge_tso) == 1 and
    summary["row_future_gap_start_ms"] is not None and
    summary["agg_future_gap_start_ms"] is not None and
    summary["purge_future_gap_start_ms"] is not None and
    summary["row_future_gap_end_ms"] is not None and
    summary["agg_future_gap_end_ms"] is not None and
    summary["purge_future_gap_end_ms"] is not None and
    summary["row_future_gap_start_ms"] > 0 and
    summary["agg_future_gap_start_ms"] > 0 and
    summary["purge_future_gap_start_ms"] > 0 and
    summary["row_future_gap_end_ms"] > 0 and
    summary["agg_future_gap_end_ms"] > 0 and
    summary["purge_future_gap_end_ms"] > 0
)
summary["reproduced_core_scheduler_future_pin"] = bool(
    summary["reproduced_agg_only_future_pin"] or
    summary["reproduced_all_components_future_pin"]
)
summary["purge_stalled_with_agg"] = bool(
    len(purge_tso) == 1 and
    len(agg_tso) == 1 and
    purge_tso[0] == agg_tso[0]
)

summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
print(json.dumps(summary, indent=2, sort_keys=True))
PY
}

cleanup() {
  stop_writer_loop
}

trap cleanup EXIT INT TERM

printf 'phase\tsample_idx\thost_now_s\tnode_now_s\tdb_now_ms\trow_next_ms\tagg_next_ms\tpurge_next_ms\trow_tso\tagg_tso\tpurge_tso\n' >"$runtime_tsv"

echo "==> bootstrap cluster on fresh testbed $TESTBED" | tee "$bootstrap_log"
echo "==> sample node: $sample_node_fqdn local mysql port: $target_port" | tee -a "$bootstrap_log"
echo "==> bump/reset nodes: $target_node_labels_csv" | tee -a "$bootstrap_log"

export CONCURRENCY="$BOOTSTRAP_CONCURRENCY"
mview_populate_build_args
mview_run_test "$BOOTSTRAP_WORKLOAD" none "$BOOTSTRAP_TIME_LIMIT" "$bootstrap_run_tag" 2>&1 | tee -a "$bootstrap_log"

echo "==> bootstrap store: ${MVIEW_LAST_STORE_DIR:-unknown}" | tee -a "$bootstrap_log"
wait_for_sql_ready

echo "==> probing manual clock set on $target_node_labels_csv" | tee -a "$bootstrap_log"
probe_now="$(date -u +%s)"
set_target_nodes_epoch_seconds "$probe_now" >/dev/null

echo "==> applying SQL schema" | tee -a "$bootstrap_log"
apply_schema
echo "==> discovered log table: $log_table" | tee -a "$bootstrap_log"
start_writer_loop

for ((i = 1; i <= PRE_RESET_SAMPLES; i++)); do
  snapshot_runtime_sample "pre-reset" "$i"
  sleep "$SAMPLE_INTERVAL_SECONDS"
done

echo "==> bumping $target_node_labels_csv clock forward by ${BUMP_SECONDS}s" | tee -a "$bootstrap_log"
bump_target="$(( $(date -u +%s) + BUMP_SECONDS ))"
set_target_nodes_epoch_seconds "$bump_target" | tee -a "$bootstrap_log"

sleep "$BUMP_HOLD_SECONDS"

echo "==> resetting $target_node_labels_csv clock back to host wall clock" | tee -a "$bootstrap_log"
reset_target="$(date -u +%s)"
set_target_nodes_epoch_seconds "$reset_target" | tee -a "$bootstrap_log"

for ((i = 1; i <= POST_RESET_SAMPLES; i++)); do
  snapshot_runtime_sample "post-reset" "$i"
  sleep "$SAMPLE_INTERVAL_SECONDS"
done

capture_history

echo "==> analyzing post-reset runtime" | tee -a "$bootstrap_log"
analyze_runtime | tee -a "$bootstrap_log"

echo "==> runtime samples: $runtime_tsv"
echo "==> history samples: $history_tsv"
echo "==> summary: $summary_json"
