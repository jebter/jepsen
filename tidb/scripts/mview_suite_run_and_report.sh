#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/mview_common.sh
source "$SCRIPT_DIR/mview_common.sh"

mview_init_root "$SCRIPT_DIR"

SUITE="${1:-${SUITE:-}}"
TARBALL_URL="${2:-${TARBALL_URL:-}}"
BINARY_URLS="${3:-${BINARY_URLS:-}}"
mview_init_binary_urls
REPORT_FORMAT="${REPORT_FORMAT:-text}"
SUITE_OUTPUT_DIR="${SUITE_OUTPUT_DIR:-}"

if [[ -z "$SUITE" || -z "$TARBALL_URL" ]]; then
  echo "usage: $0 <suite> <tarball-url> [binary-urls]" >&2
  echo "supported suites: branch-validation full-single-fault autosched-longrun" >&2
  echo "optional env: REPORT_FORMAT SUITE_OUTPUT_DIR WORKLOAD_FILTER plus the same BUILD_*/FEATURE_FLAGS/NODES/SSH_PRIVATE_KEY/TXN_MODE env vars accepted by the underlying suite script" >&2
  exit 1
fi

case "$SUITE" in
  branch-validation)
    suite_script="scripts/mview_branch_validation.sh"
    ;;
  full-single-fault)
    suite_script="scripts/mview_full_single_fault.sh"
    ;;
  autosched-longrun)
    suite_script="scripts/mview_autosched_longrun.sh"
    ;;
  *)
    echo "unsupported suite: $SUITE" >&2
    exit 1
    ;;
esac

if [[ -z "$SUITE_OUTPUT_DIR" ]]; then
  timestamp="$(date +%Y%m%dT%H%M%S)"
  SUITE_OUTPUT_DIR="$ROOT_DIR/store/suites/${SUITE}-${timestamp}-$$-$RANDOM"
fi
export MVIEW_SUITE_OUTPUT_DIR="$SUITE_OUTPUT_DIR"
export MVIEW_RUNNER_LOG="${MVIEW_RUNNER_LOG:-$SUITE_OUTPUT_DIR/runner.log}"
export MVIEW_STATUS_TSV="${MVIEW_STATUS_TSV:-$SUITE_OUTPUT_DIR/status.tsv}"
export MVIEW_CASE_LOG_DIR="${MVIEW_CASE_LOG_DIR:-$SUITE_OUTPUT_DIR/case-logs}"
mview_init_suite_outputs
mview_log_progress "suite start suite=$SUITE tarball=$TARBALL_URL binary_urls=${BINARY_URLS:-none}"

suite_args=("$TARBALL_URL")
[[ -n "$BINARY_URLS" ]] && suite_args+=("$BINARY_URLS")
suite_exit=0
if "$suite_script" "${suite_args[@]}"; then
  suite_exit=0
else
  suite_exit=$?
fi

new_store_dirs=()
while IFS= read -r store_dir; do
  [[ -z "$store_dir" ]] && continue
  new_store_dirs+=("$store_dir")
done < <(mview_collect_store_dirs_from_status "$MVIEW_STATUS_TSV")

if [[ ${#new_store_dirs[@]} -gt 0 ]]; then
  for store_dir in "${new_store_dirs[@]}"; do
    mview_write_store_report "$store_dir"
  done

  printf '%s\n' "${new_store_dirs[@]}" > "$SUITE_OUTPUT_DIR/store-dirs.txt"
  python3 scripts/mview_suite_report.py --suite-name "$SUITE" --json "${new_store_dirs[@]}" > "$SUITE_OUTPUT_DIR/suite-report.json"
  python3 scripts/mview_suite_report.py --suite-name "$SUITE" "${new_store_dirs[@]}" > "$SUITE_OUTPUT_DIR/suite-report.txt"
fi

echo "==> suite report dir: $SUITE_OUTPUT_DIR"
echo "==> suite status: $MVIEW_STATUS_TSV"
echo "==> suite runner log: $MVIEW_RUNNER_LOG"
echo "==> suite case logs: $MVIEW_CASE_LOG_DIR"
if [[ ${#new_store_dirs[@]} -gt 0 ]]; then
  echo "==> suite json: $SUITE_OUTPUT_DIR/suite-report.json"
  echo "==> suite text: $SUITE_OUTPUT_DIR/suite-report.txt"
fi

if [[ ${#new_store_dirs[@]} -eq 0 ]]; then
  mview_log_progress "suite end suite=$SUITE exit_code=$suite_exit new_store_dirs=0"
  if [[ "$suite_exit" -eq 0 ]]; then
    echo "no new store directories were detected after running $SUITE" >&2
    exit 1
  fi
  echo "suite failed before any new store directories were detected for $SUITE" >&2
  exit "$suite_exit"
fi

mview_log_progress "suite end suite=$SUITE exit_code=$suite_exit new_store_dirs=${#new_store_dirs[@]}"
mview_print_report_format "$REPORT_FORMAT" "$SUITE_OUTPUT_DIR/suite-report.txt" "$SUITE_OUTPUT_DIR/suite-report.json"

if [[ "$suite_exit" -ne 0 ]]; then
  exit "$suite_exit"
fi
