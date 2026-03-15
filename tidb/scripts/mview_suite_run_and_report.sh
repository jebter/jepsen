#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/mview_common.sh
source "$SCRIPT_DIR/mview_common.sh"

mview_init_root "$SCRIPT_DIR"

SUITE="${1:-${SUITE:-}}"
TARBALL_URL="${2:-${TARBALL_URL:-}}"
BINARY_URLS="${3:-${BINARY_URLS:-}}"
REPORT_FORMAT="${REPORT_FORMAT:-text}"
SUITE_OUTPUT_DIR="${SUITE_OUTPUT_DIR:-}"

if [[ -z "$SUITE" || -z "$TARBALL_URL" ]]; then
  echo "usage: $0 <suite> <tarball-url> [binary-urls]" >&2
  echo "supported suites: branch-validation full-single-fault autosched-longrun" >&2
  echo "optional env: REPORT_FORMAT SUITE_OUTPUT_DIR WORKLOAD_FILTER plus the same BUILD_*/FEATURE_FLAGS/TXN_MODE env vars accepted by the underlying suite script" >&2
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

before_file="$(mktemp)"
after_file="$(mktemp)"
trap 'rm -f "$before_file" "$after_file"' EXIT

mview_list_store_dirs > "$before_file"

suite_args=("$TARBALL_URL")
[[ -n "$BINARY_URLS" ]] && suite_args+=("$BINARY_URLS")
"$suite_script" "${suite_args[@]}"

mview_list_store_dirs > "$after_file"
mapfile -t new_store_dirs < <(mview_diff_store_dirs "$before_file" "$after_file")

if [[ ${#new_store_dirs[@]} -eq 0 ]]; then
  echo "no new store directories were detected after running $SUITE" >&2
  exit 1
fi

if [[ -z "$SUITE_OUTPUT_DIR" ]]; then
  timestamp="$(date +%Y%m%dT%H%M%S)"
  SUITE_OUTPUT_DIR="$ROOT_DIR/store/suites/${SUITE}-${timestamp}"
fi
mkdir -p "$SUITE_OUTPUT_DIR"

for store_dir in "${new_store_dirs[@]}"; do
  mview_write_store_report "$store_dir"
done

printf '%s
' "${new_store_dirs[@]}" > "$SUITE_OUTPUT_DIR/store-dirs.txt"
python3 scripts/mview_suite_report.py --suite-name "$SUITE" --json "${new_store_dirs[@]}" > "$SUITE_OUTPUT_DIR/suite-report.json"
python3 scripts/mview_suite_report.py --suite-name "$SUITE" "${new_store_dirs[@]}" > "$SUITE_OUTPUT_DIR/suite-report.txt"

echo "==> suite report dir: $SUITE_OUTPUT_DIR"
echo "==> suite json: $SUITE_OUTPUT_DIR/suite-report.json"
echo "==> suite text: $SUITE_OUTPUT_DIR/suite-report.txt"

mview_print_report_format "$REPORT_FORMAT" "$SUITE_OUTPUT_DIR/suite-report.txt" "$SUITE_OUTPUT_DIR/suite-report.json"
