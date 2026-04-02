#!/usr/bin/env bash

mview_default_binary_urls() {
  printf '%s' "tidb:https://fileserver.pingcap.net/download/builds/devbuild/10254/tidb-linux-amd64.tar.gz,tikv:https://fileserver.pingcap.net/download/builds/hotfix/tikv/v8.5.4-20260316-c69cb9b/10004/tikv-patch-linux-amd64.tar.gz"
}

mview_init_binary_urls() {
  if [[ -z "${BINARY_URLS:-}" ]]; then
    BINARY_URLS="$(mview_default_binary_urls)"
  fi
  export BINARY_URLS
}

mview_init_root() {
  local script_dir="$1"
  ROOT_DIR="$(cd "$script_dir/.." && pwd)"
  cd "$ROOT_DIR"
}

mview_init_base_env() {
  local build_notes_default="$1"
  BUILD_BRANCH="${BUILD_BRANCH:-}"
  BUILD_COMMIT_SHA="${BUILD_COMMIT_SHA:-}"
  BUILD_TIME="${BUILD_TIME:-}"
  FEATURE_FLAGS="${FEATURE_FLAGS:-}"
  BUILD_NOTES="${BUILD_NOTES:-$build_notes_default}"
  CONCURRENCY="${CONCURRENCY:-2n}"
  NODES="${NODES:-${JEPSEN_NODES:-}}"
  SSH_PRIVATE_KEY="${SSH_PRIVATE_KEY:-${JEPSEN_SSH_PRIVATE_KEY:-$HOME/.ssh/id_rsa}}"
  JEPSEN_BEST_EFFORT_NET="${JEPSEN_BEST_EFFORT_NET:-1}"
  TXN_MODE="${TXN_MODE:-optimistic}"
  mview_init_binary_urls
  export JEPSEN_BEST_EFFORT_NET
}

mview_init_time_limit() {
  local default_time_limit="$1"
  TIME_LIMIT="${TIME_LIMIT:-$default_time_limit}"
}

mview_require_tarball() {
  local usage="$1"
  local extra_env_help="$2"
  if [[ -z "${TARBALL_URL:-}" ]]; then
    echo "usage: $usage" >&2
    echo "optional env: BUILD_BRANCH BUILD_COMMIT_SHA BUILD_TIME FEATURE_FLAGS BUILD_NOTES CONCURRENCY NODES SSH_PRIVATE_KEY TXN_MODE${extra_env_help}" >&2
    exit 1
  fi
}

mview_populate_build_args() {
  MVIEW_BUILD_ARGS=()
  [[ -n "${BINARY_URLS:-}" ]] && MVIEW_BUILD_ARGS+=(--binary-urls "$BINARY_URLS")
  [[ -n "${BUILD_BRANCH:-}" ]] && MVIEW_BUILD_ARGS+=(--build-branch "$BUILD_BRANCH")
  [[ -n "${BUILD_COMMIT_SHA:-}" ]] && MVIEW_BUILD_ARGS+=(--build-commit-sha "$BUILD_COMMIT_SHA")
  [[ -n "${BUILD_TIME:-}" ]] && MVIEW_BUILD_ARGS+=(--build-time "$BUILD_TIME")
  [[ -n "${FEATURE_FLAGS:-}" ]] && MVIEW_BUILD_ARGS+=(--feature-flags "$FEATURE_FLAGS")
  [[ -n "${BUILD_NOTES:-}" ]] && MVIEW_BUILD_ARGS+=(--build-notes "$BUILD_NOTES")
}

mview_default_nemesis_for() {
  local workload="$1"
  if [[ "$workload" == "mv-autosched-time" ]]; then
    echo "clock-skew"
  else
    echo "none"
  fi
}

mview_warn_if_experimental() {
  local workload="$1"
  if [[ "$workload" == "mv-autosched-time" ]]; then
    echo "warning: mv-autosched-time is still experimental; clock-skew is enabled for manual runs but not in the default gate suite." >&2
  fi
}

mview_catalog_helper() {
  echo "${MVIEW_CATALOG_HELPER:-$ROOT_DIR/mview_case_catalog.py}"
}

mview_emit_suite_cases() {
  local suite="$1"
  local workload_filter="${2:-${WORKLOAD_FILTER:-}}"
  if [[ -n "$workload_filter" ]]; then
    python3 "$(mview_catalog_helper)" suite-cases "$suite" --workload "$workload_filter"
  else
    python3 "$(mview_catalog_helper)" suite-cases "$suite"
  fi
}

mview_timestamp() {
  date '+%Y-%m-%dT%H:%M:%S%z'
}

mview_safe_name() {
  printf '%s' "$1" | tr -c '[:alnum:]._-' '_'
}

mview_tsv_escape() {
  printf '%s' "$1" | tr '\t\r\n' '   '
}

mview_init_suite_outputs() {
  local suite_output_dir="${MVIEW_SUITE_OUTPUT_DIR:-${SUITE_OUTPUT_DIR:-}}"
  if [[ -z "$suite_output_dir" ]]; then
    return 0
  fi

  MVIEW_SUITE_OUTPUT_DIR="$suite_output_dir"
  MVIEW_RUNNER_LOG="${MVIEW_RUNNER_LOG:-$MVIEW_SUITE_OUTPUT_DIR/runner.log}"
  MVIEW_STATUS_TSV="${MVIEW_STATUS_TSV:-$MVIEW_SUITE_OUTPUT_DIR/status.tsv}"
  MVIEW_CASE_LOG_DIR="${MVIEW_CASE_LOG_DIR:-$MVIEW_SUITE_OUTPUT_DIR/case-logs}"
  export MVIEW_SUITE_OUTPUT_DIR MVIEW_RUNNER_LOG MVIEW_STATUS_TSV MVIEW_CASE_LOG_DIR

  mkdir -p "$MVIEW_SUITE_OUTPUT_DIR" "$MVIEW_CASE_LOG_DIR"
  touch "$MVIEW_RUNNER_LOG"
  if [[ ! -f "$MVIEW_STATUS_TSV" ]]; then
    printf 'timestamp\tevent\tworkload\tnemesis\ttime_limit\trun_tag\texit_code\tstore_dirs\tcase_log\ttarball_url\tbinary_urls\n' > "$MVIEW_STATUS_TSV"
  fi
}

mview_log_progress() {
  local timestamp
  timestamp="$(mview_timestamp)"
  printf '[%s] %s\n' "$timestamp" "$*"
  if [[ -n "${MVIEW_RUNNER_LOG:-}" ]]; then
    printf '[%s] %s\n' "$timestamp" "$*" >> "$MVIEW_RUNNER_LOG"
  fi
}

mview_append_status() {
  local event="$1"
  local workload="$2"
  local nemesis="$3"
  local time_limit="$4"
  local run_tag="${5:-}"
  local exit_code="${6:-}"
  local store_dirs="${7:-}"
  local case_log="${8:-}"

  mview_init_suite_outputs
  if [[ -z "${MVIEW_STATUS_TSV:-}" ]]; then
    return 0
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(mview_tsv_escape "$(mview_timestamp)")" \
    "$(mview_tsv_escape "$event")" \
    "$(mview_tsv_escape "$workload")" \
    "$(mview_tsv_escape "$nemesis")" \
    "$(mview_tsv_escape "$time_limit")" \
    "$(mview_tsv_escape "$run_tag")" \
    "$(mview_tsv_escape "$exit_code")" \
    "$(mview_tsv_escape "$store_dirs")" \
    "$(mview_tsv_escape "$case_log")" \
    "$(mview_tsv_escape "${TARBALL_URL:-}")" \
    "$(mview_tsv_escape "${BINARY_URLS:-}")" \
    >> "$MVIEW_STATUS_TSV"
}

mview_join_csv() {
  local IFS=','
  printf '%s' "$*"
}

mview_make_run_tag() {
  local seed="${1:-case}"
  printf '%s-%s-%s-%s\n' \
    "$(mview_safe_name "$seed")" \
    "$(date '+%Y%m%dT%H%M%S')" \
    "$$" \
    "$RANDOM"
}

mview_store_dirs_for_run_tag() {
  local run_tag="$1"
  python3 - "$run_tag" <<'PYSTORETAG'
from pathlib import Path
import sys

run_tag = sys.argv[1]
root = Path('store')
if not root.exists():
    raise SystemExit(0)

paths = []
needle = f" run-tag {run_tag}"
for workload_dir in root.iterdir():
    if workload_dir.name == 'suites':
        continue
    if workload_dir.is_symlink() or not workload_dir.is_dir():
        continue
    idx = workload_dir.name.find(needle)
    if idx == -1:
        continue
    suffix_index = idx + len(needle)
    if suffix_index < len(workload_dir.name) and workload_dir.name[suffix_index] != ' ':
        continue
    for run_dir in workload_dir.iterdir():
        if run_dir.is_symlink() or not run_dir.is_dir():
            continue
        paths.append(str(run_dir.resolve()))

for path in sorted(paths):
    print(path)
PYSTORETAG
}

mview_store_dir_for_run_tag() {
  local run_tag="$1"
  local store_dir=""
  local count=0

  while IFS= read -r candidate; do
    [[ -z "$candidate" ]] && continue
    store_dir="$candidate"
    count=$((count + 1))
  done < <(mview_store_dirs_for_run_tag "$run_tag")

  if [[ "$count" -eq 1 ]]; then
    printf '%s\n' "$store_dir"
    return 0
  fi

  if [[ "$count" -eq 0 ]]; then
    echo "no store directories found for run tag: $run_tag" >&2
  else
    echo "multiple store directories found for run tag: $run_tag" >&2
  fi
  return 1
}

mview_collect_store_dirs_from_status() {
  local status_tsv="$1"
  python3 - "$status_tsv" <<'PYSTATUS'
from pathlib import Path
import csv
import sys

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(0)

seen = set()
with path.open(newline='') as fh:
    reader = csv.DictReader(fh, delimiter='\t')
    for row in reader:
        event = (row.get('event') or '').strip()
        if event not in {'passed', 'failed'}:
            continue
        for store_dir in (row.get('store_dirs') or '').split(','):
            store_dir = store_dir.strip()
            if not store_dir or store_dir in seen:
                continue
            seen.add(store_dir)
            print(store_dir)
PYSTATUS
}

mview_run_suite_cases() {
  local suite="$1"
  local workload_filter="${2:-${WORKLOAD_FILTER:-}}"
  local run_tag=""
  mview_init_suite_outputs
  mview_log_progress "suite cases start suite=$suite workload_filter=${workload_filter:-all}"
  while IFS=$'\t' read -r workload nemesis time_limit; do
    [[ -z "$workload" ]] && continue
    run_tag="$(mview_make_run_tag "${suite}__${workload}__${nemesis}")"
    mview_run_test "$workload" "$nemesis" "$time_limit" "$run_tag"
  done < <(mview_emit_suite_cases "$suite" "$workload_filter")
  mview_log_progress "suite cases done suite=$suite workload_filter=${workload_filter:-all}"
}

mview_run_test() {
  local workload="$1"
  local nemesis="$2"
  local time_limit="$3"
  local run_tag="${4:-${MVIEW_RUN_TAG:-}}"
  local case_log=""
  local store_dirs_csv=""
  local case_slug=""
  local safe_run_tag=""
  local exit_code=0
  local -a store_dirs=()

  if [[ -z "$run_tag" ]]; then
    run_tag="$(mview_make_run_tag "${workload}__${nemesis}")"
  fi

  MVIEW_LAST_RUN_TAG="$run_tag"
  MVIEW_LAST_STORE_DIRS_CSV=""
  MVIEW_LAST_STORE_DIR=""

  local -a cmd=(
    lein run test
    --workload "$workload"
    --nemesis "$nemesis"
    --time-limit "$time_limit"
    --test-count 1
    --concurrency "$CONCURRENCY"
    --auto-retry default
    --auto-retry-limit default
    --txn-mode "$TXN_MODE"
    --tarball-url "$TARBALL_URL"
    --ssh-private-key "$SSH_PRIVATE_KEY"
    --run-tag "$run_tag"
  )

  if [[ -n "${NODES:-}" ]]; then
    cmd+=(--nodes "$NODES")
  fi

  if [[ ${#MVIEW_BUILD_ARGS[@]} -gt 0 ]]; then
    cmd+=("${MVIEW_BUILD_ARGS[@]}")
  fi

  mview_init_suite_outputs
  if [[ -n "${MVIEW_SUITE_OUTPUT_DIR:-}" ]]; then
    case_slug="$(mview_safe_name "${workload}__${nemesis}")"
    safe_run_tag="$(mview_safe_name "$run_tag")"
    case_log="$MVIEW_CASE_LOG_DIR/${case_slug}__${safe_run_tag}.log"
    : > "$case_log"
  fi

  mview_log_progress "case start workload=$workload nemesis=$nemesis time_limit=${time_limit}s run_tag=$run_tag tarball=${TARBALL_URL:-} binary_urls=${BINARY_URLS:-none}"
  mview_append_status "start" "$workload" "$nemesis" "$time_limit" "$run_tag" "" "" "$case_log"

  if [[ -n "$case_log" ]]; then
    if "${cmd[@]}" 2>&1 | tee -a "$case_log"; then
      exit_code=0
    else
      exit_code=$?
    fi
  else
    if "${cmd[@]}"; then
      exit_code=0
    else
      exit_code=$?
    fi
  fi

  while IFS= read -r store_dir; do
    [[ -z "$store_dir" ]] && continue
    store_dirs+=("$store_dir")
  done < <(mview_store_dirs_for_run_tag "$run_tag")

  if [[ ${#store_dirs[@]} -gt 0 ]]; then
    store_dirs_csv="$(mview_join_csv "${store_dirs[@]}")"
    MVIEW_LAST_STORE_DIR="${store_dirs[0]}"
    MVIEW_LAST_STORE_DIRS_CSV="$store_dirs_csv"
  fi

  if [[ "$exit_code" -eq 0 ]]; then
    mview_log_progress "case passed workload=$workload nemesis=$nemesis run_tag=$run_tag store_dirs=${store_dirs_csv:-none} case_log=${case_log:-none}"
    mview_append_status "passed" "$workload" "$nemesis" "$time_limit" "$run_tag" "$exit_code" "$store_dirs_csv" "$case_log"
  else
    mview_log_progress "case failed workload=$workload nemesis=$nemesis run_tag=$run_tag exit_code=$exit_code store_dirs=${store_dirs_csv:-none} case_log=${case_log:-none}"
    mview_append_status "failed" "$workload" "$nemesis" "$time_limit" "$run_tag" "$exit_code" "$store_dirs_csv" "$case_log"
  fi

  return "$exit_code"
}

mview_latest_store_dir() {
  python3 - <<'PYLATEST'
from pathlib import Path
path = Path('store/latest')
if not path.exists():
    raise SystemExit(1)
print(path.resolve())
PYLATEST
}

mview_list_store_dirs() {
  python3 - <<'PYLIST'
from pathlib import Path
root = Path('store')
if not root.exists():
    raise SystemExit(0)
paths = []
for workload_dir in root.iterdir():
    if workload_dir.name == 'suites':
        continue
    if workload_dir.is_symlink() or not workload_dir.is_dir():
        continue
    for run_dir in workload_dir.iterdir():
        if run_dir.is_symlink() or not run_dir.is_dir():
            continue
        paths.append(str(run_dir.resolve()))
for path in sorted(paths):
    print(path)
PYLIST
}

mview_diff_store_dirs() {
  local before_file="$1"
  local after_file="$2"
  python3 - "$before_file" "$after_file" <<'PYDIFF'
from pathlib import Path
import sys
before = set(Path(sys.argv[1]).read_text().splitlines())
after = [line for line in Path(sys.argv[2]).read_text().splitlines() if line and line not in before]
for line in after:
    print(line)
PYDIFF
}

mview_write_store_report() {
  local store_dir="$1"
  local report_json_out="${2:-$store_dir/mview-report.json}"
  local report_text_out="${3:-$store_dir/mview-report.txt}"
  python3 scripts/mview_store_report.py --json "$store_dir" > "$report_json_out"
  python3 scripts/mview_store_report.py "$store_dir" > "$report_text_out"
}

mview_print_report_format() {
  local report_format="$1"
  local text_path="$2"
  local json_path="$3"
  case "$report_format" in
    text)
      cat "$text_path"
      ;;
    json)
      cat "$json_path"
      ;;
    both)
      cat "$text_path"
      echo
      cat "$json_path"
      ;;
    *)
      echo "unsupported REPORT_FORMAT: $report_format (expected text|json|both)" >&2
      exit 1
      ;;
  esac
}
