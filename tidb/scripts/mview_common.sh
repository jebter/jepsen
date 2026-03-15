#!/usr/bin/env bash

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
  SSH_PRIVATE_KEY="${SSH_PRIVATE_KEY:-$HOME/.ssh/id_rsa}"
  TXN_MODE="${TXN_MODE:-optimistic}"
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
    echo "optional env: BUILD_BRANCH BUILD_COMMIT_SHA BUILD_TIME FEATURE_FLAGS BUILD_NOTES CONCURRENCY SSH_PRIVATE_KEY TXN_MODE${extra_env_help}" >&2
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
  echo "$ROOT_DIR/mview_case_catalog.py"
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

mview_run_suite_cases() {
  local suite="$1"
  local workload_filter="${2:-${WORKLOAD_FILTER:-}}"
  while IFS=$'\t' read -r workload nemesis time_limit; do
    [[ -z "$workload" ]] && continue
    mview_run_test "$workload" "$nemesis" "$time_limit"
  done < <(mview_emit_suite_cases "$suite" "$workload_filter")
}

mview_run_test() {
  local workload="$1"
  local nemesis="$2"
  local time_limit="$3"
  echo "==> ${workload} / ${nemesis} / ${time_limit}s"
  lein run test     --workload "$workload"     --nemesis "$nemesis"     --time-limit "$time_limit"     --test-count 1     --concurrency "$CONCURRENCY"     --auto-retry default     --auto-retry-limit default     --txn-mode "$TXN_MODE"     --tarball-url "$TARBALL_URL"     --ssh-private-key "$SSH_PRIVATE_KEY"     "${MVIEW_BUILD_ARGS[@]}"
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
