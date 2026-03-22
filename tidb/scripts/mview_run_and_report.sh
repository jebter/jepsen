#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/mview_common.sh
source "$SCRIPT_DIR/mview_common.sh"

mview_init_root "$SCRIPT_DIR"

WORKLOAD="${1:-${WORKLOAD:-}}"
TARBALL_URL="${2:-${TARBALL_URL:-}}"
BINARY_URLS="${3:-${BINARY_URLS:-}}"
REPORT_FORMAT="${REPORT_FORMAT:-text}"
REPORT_JSON_OUT="${REPORT_JSON_OUT:-}"
REPORT_TEXT_OUT="${REPORT_TEXT_OUT:-}"
mview_init_base_env "mview run-and-report"
mview_init_time_limit 900

if [[ -z "$WORKLOAD" || -z "$TARBALL_URL" ]]; then
  echo "usage: $0 <workload> <tarball-url> [binary-urls]" >&2
  echo "supported workloads: mv-stateful mv-lifecycle mv-autosched mv-autosched-time" >&2
  echo "optional env: NEMESIS BUILD_BRANCH BUILD_COMMIT_SHA BUILD_TIME FEATURE_FLAGS BUILD_NOTES CONCURRENCY NODES SSH_PRIVATE_KEY TXN_MODE TIME_LIMIT REPORT_FORMAT REPORT_JSON_OUT REPORT_TEXT_OUT" >&2
  exit 1
fi

case "$WORKLOAD" in
  mv-stateful|mv-lifecycle|mv-autosched|mv-autosched-time)
    ;;
  *)
    echo "unsupported workload: $WORKLOAD" >&2
    exit 1
    ;;
esac

NEMESIS="${NEMESIS:-$(mview_default_nemesis_for "$WORKLOAD")}"
RUN_TAG="${MVIEW_RUN_TAG:-$(mview_make_run_tag "$WORKLOAD")}"
mview_populate_build_args
mview_warn_if_experimental "$WORKLOAD"
mview_run_test "$WORKLOAD" "$NEMESIS" "$TIME_LIMIT" "$RUN_TAG"

store_dir="${MVIEW_LAST_STORE_DIR:-}"
if [[ -z "$store_dir" ]]; then
  store_dir="$(mview_store_dir_for_run_tag "$RUN_TAG")"
fi
REPORT_JSON_OUT="${REPORT_JSON_OUT:-$store_dir/mview-report.json}"
REPORT_TEXT_OUT="${REPORT_TEXT_OUT:-$store_dir/mview-report.txt}"

mview_write_store_report "$store_dir" "$REPORT_JSON_OUT" "$REPORT_TEXT_OUT"

echo "==> store: $store_dir"
echo "==> run tag: $RUN_TAG"
echo "==> report json: $REPORT_JSON_OUT"
echo "==> report text: $REPORT_TEXT_OUT"

mview_print_report_format "$REPORT_FORMAT" "$REPORT_TEXT_OUT" "$REPORT_JSON_OUT"
