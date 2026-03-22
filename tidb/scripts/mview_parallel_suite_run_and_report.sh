#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BRIDGE_SCRIPT="${MVIEW_TESTBED_BRIDGE_SCRIPT:-$SCRIPT_DIR/mview_testbed_bridge.sh}"
# shellcheck source=scripts/mview_common.sh
source "$SCRIPT_DIR/mview_common.sh"

mview_init_root "$SCRIPT_DIR"

SUITE="${1:-${SUITE:-}}"
TARBALL_URL="${2:-${TARBALL_URL:-}}"
BINARY_URLS="${3:-${BINARY_URLS:-}}"
REPORT_FORMAT="${REPORT_FORMAT:-text}"
SUITE_OUTPUT_DIR="${SUITE_OUTPUT_DIR:-}"
WORKLOAD_FILTER="${WORKLOAD_FILTER:-}"
MAX_PARALLEL="${MAX_PARALLEL:-3}"
CREATE_QUOTA_RETRY_LIMIT="${CREATE_QUOTA_RETRY_LIMIT:-90}"
CREATE_QUOTA_RETRY_DELAY_SECONDS="${CREATE_QUOTA_RETRY_DELAY_SECONDS:-10}"

mview_init_base_env "parallel mview suite"

if [[ -z "$SUITE" || -z "$TARBALL_URL" ]]; then
  echo "usage: $0 <suite> <tarball-url> [binary-urls]" >&2
  echo "supported suites: branch-validation full-single-fault manual-only" >&2
  echo "optional env: MAX_PARALLEL REPORT_FORMAT SUITE_OUTPUT_DIR WORKLOAD_FILTER BUILD_* FEATURE_FLAGS CONCURRENCY TXN_MODE" >&2
  exit 1
fi

case "$SUITE" in
  branch-validation|full-single-fault|manual-only)
    ;;
  *)
    echo "unsupported suite for parallel runner: $SUITE" >&2
    exit 1
    ;;
esac

if ! [[ "$MAX_PARALLEL" =~ ^[1-9][0-9]*$ ]]; then
  echo "MAX_PARALLEL must be a positive integer" >&2
  exit 1
fi

if [[ -z "$SUITE_OUTPUT_DIR" ]]; then
  timestamp="$(date +%Y%m%dT%H%M%S)"
  SUITE_OUTPUT_DIR="$ROOT_DIR/store/suites/${SUITE}-parallel-${timestamp}-$$-$RANDOM"
fi
export MVIEW_SUITE_OUTPUT_DIR="$SUITE_OUTPUT_DIR"
export MVIEW_RUNNER_LOG="${MVIEW_RUNNER_LOG:-$SUITE_OUTPUT_DIR/runner.log}"
export MVIEW_STATUS_TSV="${MVIEW_STATUS_TSV:-$SUITE_OUTPUT_DIR/status.tsv}"
export MVIEW_CASE_LOG_DIR="${MVIEW_CASE_LOG_DIR:-$SUITE_OUTPUT_DIR/case-logs}"
mview_init_suite_outputs
mview_log_progress "parallel suite start suite=$SUITE max_parallel=$MAX_PARALLEL workload_filter=${WORKLOAD_FILTER:-all} tarball=$TARBALL_URL binary_urls=${BINARY_URLS:-none}"

declare -a ACTIVE_PIDS=()
declare -a ACTIVE_WORKLOADS=()
declare -a ACTIVE_NEMESES=()
declare -a ACTIVE_TIME_LIMITS=()
declare -a ACTIVE_RUN_TAGS=()
declare -a ACTIVE_CASE_LOGS=()
declare -a ACTIVE_WORKDIRS=()
declare -a NEW_STORE_DIRS=()
suite_exit=0

mview_parallel_cleanup_workdir() {
  local workdir="$1"
  local cleanup_rc=0

  [[ -n "$workdir" ]] || return 0

  if [[ ! -d "$workdir" ]]; then
    return 0
  fi

  if [[ -f "$workdir/output" ]]; then
    if "$BRIDGE_SCRIPT" cleanup "$workdir"; then
      mview_log_progress "abort cleanup complete workdir=$workdir"
    else
      cleanup_rc=$?
      mview_log_progress "abort cleanup failed workdir=$workdir exit_code=$cleanup_rc"
    fi
  else
    mview_log_progress "abort cleanup skipped workdir=$workdir reason=missing-output"
  fi

  rm -rf "$workdir"
  return "$cleanup_rc"
}

mview_parallel_abort_active_jobs() {
  local pid=""
  local idx=0
  local workdir=""
  local cleanup_rc=0
  local current_rc=0
  local workload=""
  local nemesis=""
  local time_limit=""
  local run_tag=""
  local case_log=""
  local store_dir=""
  local store_dirs_csv=""
  local -a store_dirs=()

  trap - EXIT INT TERM
  if [[ ${#ACTIVE_PIDS[@]} -eq 0 && ${#ACTIVE_WORKDIRS[@]} -eq 0 ]]; then
    return 0
  fi

  if [[ ${#ACTIVE_PIDS[@]} -gt 0 ]]; then
    for pid in "${ACTIVE_PIDS[@]}"; do
      kill "$pid" 2>/dev/null || true
    done
    for pid in "${ACTIVE_PIDS[@]}"; do
      wait "$pid" 2>/dev/null || true
    done
  fi

  for ((idx = 0; idx < ${#ACTIVE_WORKDIRS[@]}; idx++)); do
    workload="${ACTIVE_WORKLOADS[$idx]}"
    nemesis="${ACTIVE_NEMESES[$idx]}"
    time_limit="${ACTIVE_TIME_LIMITS[$idx]}"
    run_tag="${ACTIVE_RUN_TAGS[$idx]}"
    case_log="${ACTIVE_CASE_LOGS[$idx]}"
    workdir="${ACTIVE_WORKDIRS[$idx]}"
    store_dirs=()
    store_dirs_csv=""
    while IFS= read -r store_dir; do
      [[ -z "$store_dir" ]] && continue
      store_dirs+=("$store_dir")
    done < <(mview_store_dirs_for_run_tag "$run_tag")
    if [[ ${#store_dirs[@]} -gt 0 ]]; then
      store_dirs_csv="$(mview_join_csv "${store_dirs[@]}")"
    fi
    mview_log_progress "case aborted workload=$workload nemesis=$nemesis time_limit=${time_limit}s run_tag=$run_tag store_dirs=${store_dirs_csv:-none} case_log=$case_log"
    mview_append_status "aborted" "$workload" "$nemesis" "$time_limit" "$run_tag" "130" "$store_dirs_csv" "$case_log"

    if mview_parallel_cleanup_workdir "$workdir"; then
      :
    else
      current_rc=$?
      if [[ "$cleanup_rc" -eq 0 ]]; then
        cleanup_rc="$current_rc"
      fi
    fi
  done

  ACTIVE_PIDS=()
  ACTIVE_WORKLOADS=()
  ACTIVE_NEMESES=()
  ACTIVE_TIME_LIMITS=()
  ACTIVE_RUN_TAGS=()
  ACTIVE_CASE_LOGS=()
  ACTIVE_WORKDIRS=()

  return "$cleanup_rc"
}

mview_parallel_abort_signal() {
  mview_parallel_abort_active_jobs || true
  exit 130
}

trap mview_parallel_abort_active_jobs EXIT
trap mview_parallel_abort_signal INT TERM

mview_parallel_inner_command() {
  local workload="$1"
  local command=""
  printf -v command 'cd %q && scripts/mview_run_and_report.sh %q %q' \
    "$ROOT_DIR" \
    "$workload" \
    "$TARBALL_URL"
  if [[ -n "$BINARY_URLS" ]]; then
    local binary_arg=""
    printf -v binary_arg ' %q' "$BINARY_URLS"
    command+="$binary_arg"
  fi
  printf '%s\n' "$command"
}

mview_parallel_has_store_dir() {
  local candidate="$1"
  local existing=""
  if [[ ${#NEW_STORE_DIRS[@]} -eq 0 ]]; then
    return 1
  fi
  for existing in "${NEW_STORE_DIRS[@]}"; do
    [[ "$existing" == "$candidate" ]] && return 0
  done
  return 1
}

mview_parallel_job_running() {
  local pid="$1"
  local running_pid=""
  while IFS= read -r running_pid; do
    [[ "$running_pid" == "$pid" ]] && return 0
  done < <(jobs -pr)
  return 1
}

mview_parallel_remove_active_index() {
  local remove_idx="$1"
  local idx=0
  local -a next_pids=()
  local -a next_workloads=()
  local -a next_nemeses=()
  local -a next_time_limits=()
  local -a next_run_tags=()
  local -a next_case_logs=()
  local -a next_workdirs=()

  for ((idx = 0; idx < ${#ACTIVE_PIDS[@]}; idx++)); do
    if [[ "$idx" -eq "$remove_idx" ]]; then
      continue
    fi
    next_pids+=("${ACTIVE_PIDS[$idx]}")
    next_workloads+=("${ACTIVE_WORKLOADS[$idx]}")
    next_nemeses+=("${ACTIVE_NEMESES[$idx]}")
    next_time_limits+=("${ACTIVE_TIME_LIMITS[$idx]}")
    next_run_tags+=("${ACTIVE_RUN_TAGS[$idx]}")
    next_case_logs+=("${ACTIVE_CASE_LOGS[$idx]}")
    next_workdirs+=("${ACTIVE_WORKDIRS[$idx]}")
  done

  ACTIVE_PIDS=()
  ACTIVE_WORKLOADS=()
  ACTIVE_NEMESES=()
  ACTIVE_TIME_LIMITS=()
  ACTIVE_RUN_TAGS=()
  ACTIVE_CASE_LOGS=()
  ACTIVE_WORKDIRS=()

  if [[ ${#next_pids[@]} -gt 0 ]]; then
    ACTIVE_PIDS=("${next_pids[@]}")
    ACTIVE_WORKLOADS=("${next_workloads[@]}")
    ACTIVE_NEMESES=("${next_nemeses[@]}")
    ACTIVE_TIME_LIMITS=("${next_time_limits[@]}")
    ACTIVE_RUN_TAGS=("${next_run_tags[@]}")
    ACTIVE_CASE_LOGS=("${next_case_logs[@]}")
    ACTIVE_WORKDIRS=("${next_workdirs[@]}")
  fi
}

mview_parallel_finish_case() {
  local idx="$1"
  local pid="${ACTIVE_PIDS[$idx]}"
  local workload="${ACTIVE_WORKLOADS[$idx]}"
  local nemesis="${ACTIVE_NEMESES[$idx]}"
  local time_limit="${ACTIVE_TIME_LIMITS[$idx]}"
  local run_tag="${ACTIVE_RUN_TAGS[$idx]}"
  local case_log="${ACTIVE_CASE_LOGS[$idx]}"
  local workdir="${ACTIVE_WORKDIRS[$idx]}"
  local exit_code=0
  local store_dirs_csv=""
  local store_dir=""
  local missing_store_message=""
  local -a store_dirs=()

  if wait "$pid"; then
    exit_code=0
  else
    exit_code=$?
  fi

  while IFS= read -r store_dir; do
    [[ -z "$store_dir" ]] && continue
    store_dirs+=("$store_dir")
  done < <(mview_store_dirs_for_run_tag "$run_tag")

  if [[ ${#store_dirs[@]} -gt 0 ]]; then
    store_dirs_csv="$(mview_join_csv "${store_dirs[@]}")"
    for store_dir in "${store_dirs[@]}"; do
      if ! mview_parallel_has_store_dir "$store_dir"; then
        NEW_STORE_DIRS+=("$store_dir")
      fi
    done
  elif [[ "$exit_code" -eq 0 ]]; then
    missing_store_message="successful case did not produce a store directory for run_tag=$run_tag"
    printf '%s\n' "$missing_store_message" >> "$case_log"
    exit_code=1
  fi

  if [[ "$exit_code" -eq 0 ]]; then
    mview_log_progress "case passed workload=$workload nemesis=$nemesis time_limit=${time_limit}s run_tag=$run_tag store_dirs=${store_dirs_csv:-none} case_log=$case_log"
    mview_append_status "passed" "$workload" "$nemesis" "$time_limit" "$run_tag" "$exit_code" "$store_dirs_csv" "$case_log"
  else
    mview_log_progress "case failed workload=$workload nemesis=$nemesis time_limit=${time_limit}s run_tag=$run_tag exit_code=$exit_code store_dirs=${store_dirs_csv:-none} case_log=$case_log"
    mview_append_status "failed" "$workload" "$nemesis" "$time_limit" "$run_tag" "$exit_code" "$store_dirs_csv" "$case_log"
    if [[ "$suite_exit" -eq 0 ]]; then
      suite_exit="$exit_code"
    fi
  fi

  rm -rf "$workdir"
  mview_parallel_remove_active_index "$idx"
}

mview_parallel_reap_completed_jobs() {
  local idx=0
  local pid=""
  for ((idx=${#ACTIVE_PIDS[@]}-1; idx>=0; idx--)); do
    pid="${ACTIVE_PIDS[$idx]}"
    if ! mview_parallel_job_running "$pid"; then
      mview_parallel_finish_case "$idx"
    fi
  done
}

mview_parallel_launch_case() {
  local workload="$1"
  local nemesis="$2"
  local time_limit="$3"
  local run_tag="$4"
  local case_slug=""
  local safe_run_tag=""
  local case_log=""
  local workdir=""
  local inner_command=""
  local pid=""

  case_slug="$(mview_safe_name "${workload}__${nemesis}")"
  safe_run_tag="$(mview_safe_name "$run_tag")"
  case_log="$MVIEW_CASE_LOG_DIR/${case_slug}__${safe_run_tag}.log"
  workdir="$(mktemp -d "/tmp/mview-parallel-${case_slug}-XXXXXX")"
  inner_command="$(mview_parallel_inner_command "$workload")"

  : > "$case_log"
  mview_log_progress "case launch workload=$workload nemesis=$nemesis time_limit=${time_limit}s run_tag=$run_tag workdir=$workdir case_log=$case_log"
  mview_append_status "start" "$workload" "$nemesis" "$time_limit" "$run_tag" "" "" "$case_log"

  (
    export NEMESIS="$nemesis"
    export TIME_LIMIT="$time_limit"
    export MVIEW_RUN_TAG="$run_tag"
    export CREATE_QUOTA_RETRY_LIMIT CREATE_QUOTA_RETRY_DELAY_SECONDS
    export BUILD_BRANCH BUILD_COMMIT_SHA BUILD_TIME FEATURE_FLAGS BUILD_NOTES CONCURRENCY TXN_MODE TARBALL_URL BINARY_URLS
    unset MVIEW_SUITE_OUTPUT_DIR MVIEW_RUNNER_LOG MVIEW_STATUS_TSV MVIEW_CASE_LOG_DIR
    "$BRIDGE_SCRIPT" exec "$workdir" -- bash -lc "$inner_command"
  ) >"$case_log" 2>&1 &
  pid=$!

  ACTIVE_PIDS+=("$pid")
  ACTIVE_WORKLOADS+=("$workload")
  ACTIVE_NEMESES+=("$nemesis")
  ACTIVE_TIME_LIMITS+=("$time_limit")
  ACTIVE_RUN_TAGS+=("$run_tag")
  ACTIVE_CASE_LOGS+=("$case_log")
  ACTIVE_WORKDIRS+=("$workdir")
}

while IFS=$'\t' read -r workload nemesis time_limit; do
  [[ -z "$workload" ]] && continue
  while [[ ${#ACTIVE_PIDS[@]} -ge "$MAX_PARALLEL" ]]; do
    mview_parallel_reap_completed_jobs
    if [[ ${#ACTIVE_PIDS[@]} -ge "$MAX_PARALLEL" ]]; then
      sleep 1
    fi
  done
  run_tag="$(mview_make_run_tag "${SUITE}__${workload}__${nemesis}")"
  mview_parallel_launch_case "$workload" "$nemesis" "$time_limit" "$run_tag"
done < <(mview_emit_suite_cases "$SUITE" "$WORKLOAD_FILTER")

while [[ ${#ACTIVE_PIDS[@]} -gt 0 ]]; do
  mview_parallel_reap_completed_jobs
  if [[ ${#ACTIVE_PIDS[@]} -gt 0 ]]; then
    sleep 1
  fi
done

trap - EXIT INT TERM

if [[ ${#NEW_STORE_DIRS[@]} -gt 0 ]]; then
  for store_dir in "${NEW_STORE_DIRS[@]}"; do
    mview_write_store_report "$store_dir"
  done

  printf '%s\n' "${NEW_STORE_DIRS[@]}" > "$SUITE_OUTPUT_DIR/store-dirs.txt"
  python3 scripts/mview_suite_report.py --suite-name "$SUITE" --json "${NEW_STORE_DIRS[@]}" > "$SUITE_OUTPUT_DIR/suite-report.json"
  python3 scripts/mview_suite_report.py --suite-name "$SUITE" "${NEW_STORE_DIRS[@]}" > "$SUITE_OUTPUT_DIR/suite-report.txt"
fi

echo "==> suite report dir: $SUITE_OUTPUT_DIR"
echo "==> suite status: $MVIEW_STATUS_TSV"
echo "==> suite runner log: $MVIEW_RUNNER_LOG"
echo "==> suite case logs: $MVIEW_CASE_LOG_DIR"
if [[ ${#NEW_STORE_DIRS[@]} -gt 0 ]]; then
  echo "==> suite json: $SUITE_OUTPUT_DIR/suite-report.json"
  echo "==> suite text: $SUITE_OUTPUT_DIR/suite-report.txt"
fi

if [[ ${#NEW_STORE_DIRS[@]} -eq 0 ]]; then
  mview_log_progress "parallel suite end suite=$SUITE exit_code=$suite_exit new_store_dirs=0"
  if [[ "$suite_exit" -eq 0 ]]; then
    echo "no new store directories were detected after running $SUITE in parallel" >&2
    exit 1
  fi
  echo "parallel suite failed before any new store directories were detected for $SUITE" >&2
  exit "$suite_exit"
fi

mview_log_progress "parallel suite end suite=$SUITE exit_code=$suite_exit new_store_dirs=${#NEW_STORE_DIRS[@]}"
mview_print_report_format "$REPORT_FORMAT" "$SUITE_OUTPUT_DIR/suite-report.txt" "$SUITE_OUTPUT_DIR/suite-report.json"

if [[ "$suite_exit" -ne 0 ]]; then
  exit "$suite_exit"
fi
